/*
 *     Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::Rng;
use tokio::sync::Notify;
use dragonfly_api::common::v2::{Peer};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_storage::{metadata};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use anyhow::{anyhow, Result};
use crate::resource::piece_collector::{CollectedPiece, CollectedParent, PieceCollector};
use tracing::{error, info};

pub struct PieceSelector {
    config: Arc<Config>,
    host_id: String,
    task_id: String,    
    interested_pieces: Vec<metadata::Piece>,
    parents: Vec<Peer>,
    children: Arc<DashMap<String, Peer>>,
    piece_collectors: Arc<DashMap<String, PieceCollector>>,
    collected_pieces_parents: Arc<DashMap<u32, CollectedPiece>>,
    collected_pieces_children: Arc<DashMap<u32, CollectedPiece>>,

    is_piece_selected: Arc<DashMap<u32, bool>>,
    collector_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<CollectedPiece>>>>,
    consumer_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    cancel: CancellationToken,
    remaining_pieces: Arc<AtomicUsize>,
    piece_notify: Arc<Notify>,
 }

 impl PieceSelector {
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<Peer>,
    ) -> Self {
        let remaining = interested_pieces.len();
        Self {
            config,
            host_id: host_id.to_string(),
            task_id: task_id.to_string(),
            interested_pieces,
            parents,
            children: Arc::new(DashMap::new()),
            piece_collectors: Arc::new(DashMap::new()),
            collected_pieces_parents: Arc::new(DashMap::new()),
            collected_pieces_children: Arc::new(DashMap::new()),
            is_piece_selected: Arc::new(DashMap::new()),
            collector_tx: Arc::new(Mutex::new(None)),
            consumer_handle: Arc::new(Mutex::new(None)),
            cancel: CancellationToken::new(),
            remaining_pieces: Arc::new(AtomicUsize::new(remaining)),
            piece_notify: Arc::new(Notify::new()),
        }
    }

    /// run initializes the selector once and starts parent collectors.
    ///
    /// Notes:
    /// - This method is idempotent: calling it multiple times will not spawn
    ///   duplicate consumer tasks or recreate the central channel.
    /// - The central sender is stored in `collector_tx` so that new collectors
    ///   can be added after `run()` starts.
    pub async fn run(self: Arc<Self>) {
        self.ensure_consumer_started().await;

        // Start collectors for initial parents.
        for peer in self.parents.iter().cloned() {
            if let Err(err) = self.start_collector(peer).await {
                error!("add parent collector failed: {}", err);
            }
        }
    }

    /// ensure_consumer_started initializes the central channel and spawns the consumer task once.
    async fn ensure_consumer_started(self: &Arc<Self>) {
        // Fast path: if already started, do nothing.
        {
            let guard = self.consumer_handle.lock().await;
            if guard.is_some() {
                return;
            }
        }

        // Create the central channel and store the sender.
        let (tx, mut rx) = mpsc::channel::<CollectedPiece>(1024);
        {
            let mut guard = self.collector_tx.lock().await;
            *guard = Some(tx);
        }

        // Spawn the background consumer which owns the receiver.
        let this = Arc::clone(self);
        let cancel = this.cancel.clone();
        let task_id = this.task_id.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        // Selector is shutting down.
                        break;
                    }
                    maybe_piece = rx.recv() => {
                        match maybe_piece {
                            None => {
                                // All senders have been dropped; no more pieces will arrive.
                                break;
                            }
                            Some(piece) => {
                                // Insert/merge piece info into selector state.
                                this.insert_piece(piece).await;
                            }
                        }
                    }
                }
            }

            info!("piece selector consumer exited for task {}", task_id);
        });

        // Store consumer task handle.
        let mut guard = self.consumer_handle.lock().await;
        *guard = Some(handle);
    }

    /// add_parent_collector creates a collector for the given peer and attaches it to the central channel.
    ///
    /// Stability notes:
    /// - If the collector stream ends, the forwarder task ends naturally.
    /// - If the selector is shutting down (central sender dropped), sending fails and forwarder exits.
    pub async fn start_collector(self: &Arc<Self>, peer: Peer) -> Result<()> {
        // Get the central sender.
        let tx = {
            let guard = self.collector_tx.lock().await;
            guard
                .as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("central sender is None"))?
        };

        let peer_id: String = peer.id.clone();

        // Avoid creating duplicate collectors for the same peer id.
        if self.piece_collectors.contains_key(&peer_id) {
            return Ok(());
        }

        let parent = CollectedParent {
            id: peer_id.clone(),
            host: peer.host,
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };

        // Initialize the collector.
        let mut piece_collector = PieceCollector::new(
            self.config.clone(),
            &self.host_id,
            &self.task_id,
            self.interested_pieces.clone(),
            parent,
        )
        .await;

        // Run first, then move collector into DashMap.
        let mut piece_collector_rx = piece_collector.run().await;
        self.piece_collectors.insert(peer_id.clone(), piece_collector);

        // Spawn a forwarder task: collector_rx -> central_tx.
        tokio::spawn(async move {
            while let Some(piece) = piece_collector_rx.recv().await {
                if tx.send(piece).await.is_err() {
                    // Central receiver is gone (selector stopped); exit to avoid leaks.
                    break;
                }
            }
        });

        Ok(())
    }

    /// shutdown stops the selector and all known collectors.
    ///
    /// Behavior:
    /// - Cancels the consumer task.
    /// - Drops the central sender so all forwarders observe send() failure and exit.
    /// - Shuts down all collectors to stop upstream streams promptly.
    pub async fn shutdown(self: &Arc<Self>) {
        // Cancel the consumer loop.
        self.cancel.cancel();

        // Drop the central sender to allow rx to close naturally.
        {
            let mut guard = self.collector_tx.lock().await;
            *guard = None;
        }

        // Stop all collectors (best effort).
        // NOTE: DashMap iteration yields refs; we need mutable access to call shutdown.
        // If PieceCollector::shutdown requires &mut self, we must take ownership.
        // A practical approach is to remove them one-by-one.
        let keys: Vec<String> = self
            .piece_collectors
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for k in keys {
            if let Some((_, mut collector)) = self.piece_collectors.remove(&k) {
                collector.shutdown().await;
            }
        }

        // Abort/join the consumer task.
        if let Some(handle) = self.consumer_handle.lock().await.take() {
            handle.abort();
        }
    }

    /// shutdown_collector stops and removes a specified collector by peer id.
    ///
    /// Returns:
    /// - Ok(true)  if the collector existed and was shut down,
    /// - Ok(false) if the collector did not exist (already removed or never added).
    pub async fn shutdown_collector(self: &Arc<Self>, peer_id: &str) -> Result<bool> {
        // Remove the collector first to take ownership and avoid double-shutdown races.
        let removed = self.piece_collectors.remove(peer_id);
        let Some((_key, mut collector)) = removed else {
            // Collector not found.
            return Ok(false);
        };

        // Stop the collector task. This will eventually close its output receiver,
        // allowing the forwarder task (collector_rx -> central_tx) to exit naturally.
        collector.shutdown().await;

        Ok(true)
    }
    

    /// Inserts a collected piece into the proper map (parents/children) and merges parents if needed.
    ///
    /// Invariant:
    /// - collected_pieces_* maps should only contain pieces that are NOT selected.
    ///
    /// Concurrency notes:
    /// - Selection may happen concurrently with insertion.
    /// - We do a post-check cleanup to ensure selected pieces are removed from maps eventually.
    pub async fn insert_piece(&self, piece: CollectedPiece) {
        let number = piece.number;

        // If the piece is already selected, do nothing.
        if self
            .is_piece_selected
            .get(&number)
            .map(|v| *v.value())
            .unwrap_or(false)
        {
            return;
        }

        // If no parents are attached, nothing to classify/merge.
        let Some(src_parent) = piece.parents.first() else {
            return;
        };
        let src_id = &src_parent.id;

        // Determine whether the source peer belongs to children or parents.
        // Prefer children if it appears in both sets.
        let in_children = self.children.contains_key(src_id);
        let in_parents = self.parents.iter().any(|p| p.id == *src_id);

        let target_is_children = if in_children {
            true
        } else if in_parents {
            false
        } else {
            // If unknown, default to parents (or change to "return" if you prefer dropping it).
            false
        };

        // Upsert + merge under DashMap entry lock (atomic per-key within a shard).
        if target_is_children {
            Self::upsert_and_merge_entry(&self.collected_pieces_children, piece);
            self.piece_notify.notify_one();
        } else {
            Self::upsert_and_merge_entry(&self.collected_pieces_parents, piece);
        }

        // Post-check cleanup:
        // If the piece got selected concurrently, ensure it is removed from maps.
        if self
            .is_piece_selected
            .get(&number)
            .map(|v| *v.value())
            .unwrap_or(false)
        {
            self.collected_pieces_children.remove(&number);
            self.collected_pieces_parents.remove(&number);
        }
    }

    /// Upserts a CollectedPiece entry and merges its parents list with de-duplication.
    fn upsert_and_merge_entry(map: &DashMap<u32, CollectedPiece>, incoming: CollectedPiece) {
        let number = incoming.number;

        match map.entry(number) {
            Entry::Vacant(v) => {
                // Insert a new entry for this piece number.
                v.insert(incoming);
            }
            Entry::Occupied(mut o) => {
                // Merge parents with dedupe by parent id.
                let existing = o.get_mut();
                for p in incoming.parents {
                    if !existing.parents.iter().any(|ep| ep.id == p.id) {
                        existing.parents.push(p);
                    }
                }
            }
        }
    }

    /// Marks a piece as selected and removes it from both collected maps.
    ///
    /// Returns the removed entry if it existed (either from children or parents).
    pub async fn mark_selected_and_remove_piece(&self, number: u32) -> Option<CollectedPiece> {
        // Mark as selected first so future inserts are rejected.
        self.is_piece_selected.insert(number, true);

        // Remove from both maps to keep invariant: only non-selected pieces remain.
        if let Some((_, v)) = self.collected_pieces_children.remove(&number) {
            return Some(v);
        }
        if let Some((_, v)) = self.collected_pieces_parents.remove(&number) {
            return Some(v);
        }
        None
    }

    /// Inserts a child peer and starts a collector for it if newly inserted.
    pub async fn insert_child(self: &Arc<Self>, child: Peer) {
        let child_id = child.id.clone();
        // Insert into children map. If already exists, do nothing.
        let is_new = self.children.insert(child_id.clone(), child.clone()).is_none();
        if !is_new {
            return;
        }

        // Start a collector for this child (best-effort).
        if let Err(err) = self.start_collector(child.clone()).await {
            error!("start child collector failed for {}: {}", child_id, err);
        }
    }

    /// Removes a child peer and shuts down its collector if present.
    pub async fn remove_child(self: &Arc<Self>, child: Peer) {
        let child_id = child.id.clone();

        // Remove from children map first.
        self.children.remove(&child_id);

        // Shut down and remove the corresponding collector (best-effort).
        if let Err(err) = self.shutdown_collector(&child_id).await {
            error!("shutdown child collector failed for {}: {}", child_id, err);
        }

        // Optional cleanup:
        // Remove any collected-but-not-selected entries that only came from this child.
        // This keeps collected_pieces_children minimal.
        //
        // NOTE: This is O(N) over collected_pieces_children.
        let keys: Vec<u32> = self
            .collected_pieces_children
            .iter()
            .filter_map(|entry| {
                let v = entry.value();
                let from_child = v.parents.iter().any(|pp| pp.id == child_id);
                if from_child { Some(*entry.key()) } else { None }
            })
            .collect();

        for k in keys {
            // If it's selected, it should be absent anyway; removing is safe.
            self.collected_pieces_children.remove(&k);
        }
    }
    
    /// Selects one piece randomly from collected_pieces_children.
    ///
    /// Behavior:
    /// - If remaining_pieces == 0, returns None immediately.
    /// - If no child piece is available, waits until insert_piece() notifies.
    /// - On success, removes the piece from collected_pieces_children and decrements remaining_pieces by 1.
    pub async fn select_piece(&self) -> Option<CollectedPiece> {
        loop {
            // If nothing remains to be selected, return immediately.
            if self.remaining_pieces.load(Ordering::Acquire) == 0 {
                return None;
            }

            // Try select once.
            if let Some(piece) = self.try_select_from_children_once() {
                // Decrement remaining pieces (saturating).
                let prev = self.remaining_pieces.fetch_sub(1, Ordering::AcqRel);
                if prev == 0 {
                    self.remaining_pieces.store(0, Ordering::Release);
                }
                return Some(piece);
            }

            // Nothing available; wait for a notification.
            // This can wake spuriously; we will re-check in the loop.
            self.piece_notify.notified().await;
        }
    }

    /// Attempts to select one random piece from collected_pieces_children once.
    /// Returns None if map is empty or if a race removes the chosen entry.
    fn try_select_from_children_once(&self) -> Option<CollectedPiece> {
        // Snapshot keys to allow random selection.
        let keys: Vec<u32> = self
            .collected_pieces_children
            .iter()
            .map(|e| *e.key())
            .collect();

        if keys.is_empty() {
            return None;
        }

        let idx = rand::thread_rng().gen_range(0..keys.len());
        let number = keys[idx];

        // Remove and return the selected piece.
        self.collected_pieces_children.remove(&number).map(|(_, v)| v)
    }
 }



 