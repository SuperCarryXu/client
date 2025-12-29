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

use dragonfly_client_core::{Result};
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use dragonfly_api::common::v2::Peer;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_storage::metadata;
use rand::Rng;
use tokio::sync::{mpsc, Notify, OnceCell};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::resource::piece_collector::{CollectedPiece, CollectedParent, PieceCollector};

/// PieceSelector maintains two independent pipelines:
/// - Parent pipeline: initialized once in run(), only inserts into parent collected map.
/// - Child pipeline: initialized once in run() (pipeline only), supports dynamic add/remove child collectors.
///
/// Selection is performed ONLY from parent collected pieces.
pub struct PieceSelector {
    config: Arc<Config>,
    host_id: String,
    task_id: String,
    interested_pieces: Vec<metadata::Piece>,

    // Fixed parent set (collectors started once in run()).
    parents: Vec<Peer>,

    // Dynamic child set (collectors may be added/removed after run()).
    children: Arc<DashMap<String, Peer>>,

    // Separate collector registries to avoid id collisions and keep semantics clear.
    parent_collectors: Arc<DashMap<String, PieceCollector>>,
    child_collectors: Arc<DashMap<String, PieceCollector>>,

    // Separate collected maps.
    collected_pieces_parents: Arc<DashMap<u32, CollectedPiece>>,
    collected_pieces_children: Arc<DashMap<u32, CollectedPiece>>,

    // Selection bookkeeping (select only from parents).
    remaining_pieces: Arc<AtomicUsize>,
    parent_piece_notify: Arc<Notify>,
    selected_pieces: Arc<DashSet<u32>>,

    // Parent pipeline: created once in run().
    parent_tx: OnceCell<mpsc::Sender<CollectedPiece>>,
    parent_consumer_handle: OnceCell<JoinHandle<()>>,

    // Child pipeline: created once in run(), collectors are dynamic.
    child_tx: OnceCell<mpsc::Sender<CollectedPiece>>,
    child_consumer_handle: OnceCell<JoinHandle<()>>,

    // Global cancellation for both pipelines.
    cancel: CancellationToken,
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

            parent_collectors: Arc::new(DashMap::new()),
            child_collectors: Arc::new(DashMap::new()),

            collected_pieces_parents: Arc::new(DashMap::new()),
            collected_pieces_children: Arc::new(DashMap::new()),

            remaining_pieces: Arc::new(AtomicUsize::new(remaining)),
            parent_piece_notify: Arc::new(Notify::new()),
            selected_pieces: Arc::new(DashSet::new()),

            parent_tx: OnceCell::new(),
            parent_consumer_handle: OnceCell::new(),
            child_tx: OnceCell::new(),
            child_consumer_handle: OnceCell::new(),

            cancel: CancellationToken::new(),
        }
    }

    /// run initializes both pipelines once and starts all parent collectors.
    ///
    /// Notes:
    /// - Parent collectors are started only here.
    /// - Child pipeline is started here, but child collectors can be added/removed later.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.start_parent_pipeline().await?;
        self.start_child_pipeline().await?;

        // Start parent collectors once.
        for peer in self.parents.iter().cloned() {
            self.start_parent_collector(peer)
                .await
                .inspect_err(|e| {
                    error!("failed to start parent collector: {}", e)
                })?;
        }

        Ok(())
    }

    /// start_parent_pipeline creates the parent channel and a consumer task that inserts into parent map.
    async fn start_parent_pipeline(self: &Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<CollectedPiece>(1024);

        if self.parent_tx.set(tx).is_err() {
            error!("parent pipeline already started, skipping");
            return Ok(());
        }

        let this = Arc::clone(self);
        let cancel = this.cancel.clone();
        let task_id = this.task_id.clone();

        // check if parent consumer handle is already set
        if self.parent_consumer_handle.get().is_some() {
            error!("parent consumer handle already set, skipping");
            return Ok(());
        }

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_piece = rx.recv() => {
                        match maybe_piece {
                            None => break,
                            Some(piece) => {
                                this.insert_parent_piece(piece).await;
                            }
                        }
                    }
                }
            }
            info!("parent consumer exited for task {}", task_id);
        });

        if self.parent_consumer_handle.set(handle).is_err() {
            error!("parent consumer handle already set, skipping");
            return Ok(());
        }
        
        info!("parent pipeline started");
        Ok(())
    }

    /// start_child_pipeline creates the child channel and a consumer task that inserts into child map.
    async fn start_child_pipeline(self: &Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<CollectedPiece>(1024);

        if self.child_tx.set(tx).is_err() {
            error!("child pipeline already started, skipping");
            return Ok(());
        }

        let this = Arc::clone(self);
        let cancel = this.cancel.clone();
        let task_id = this.task_id.clone();

        // check if child consumer handle is already set
        if self.child_consumer_handle.get().is_some() {
            error!("child consumer handle already set, skipping");
            return Ok(());
        }

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_piece = rx.recv() => {
                        match maybe_piece {
                            None => break,
                            Some(piece) => {
                                this.insert_child_piece(piece).await;
                            }
                        }
                    }
                }
            }
            info!("child consumer exited for task {}", task_id);
        });

        if self.child_consumer_handle.set(handle).is_err() {
            error!("child consumer handle already set, skipping");
            return Ok(());
        }

        info!("child pipeline started");
        Ok(())
    }

    /// start_parent_collector starts a collector for a parent peer and forwards into parent pipeline.
    pub async fn start_parent_collector(self: &Arc<Self>, peer: Peer) -> Result<()> {
        let tx = match self.parent_tx.get().cloned() {
            Some(tx) => tx,
            None => {
                error!("parent pipeline not started: parent_tx is None");
                return Ok(()); // skip starting this collector
            }
        };

        let peer_id = peer.id.clone();
        if self.parent_collectors.contains_key(&peer_id) {
            return Ok(());
        }

        let parent = CollectedParent {
            id: peer_id.clone(),
            host: peer.host,
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };

        let mut collector = PieceCollector::new(
            self.config.clone(),
            &self.host_id,
            &self.task_id,
            self.interested_pieces.clone(),
            parent,
        )
        .await;

        let mut collector_rx = collector.run().await;
        self.parent_collectors.insert(peer_id.clone(), collector);

        tokio::spawn(async move {
            while let Some(piece) = collector_rx.recv().await {
                if tx.send(piece).await.is_err() {
                    break;
                }
            }
        });

        info!("parent collector started for peer {}", peer_id);
        Ok(())
    }

    /// start_child_collector starts a collector for a child peer and forwards into child pipeline.
    pub async fn start_child_collector(self: &Arc<Self>, peer: Peer) -> Result<()> {
        let tx = match self.child_tx.get().cloned() {
            Some(tx) => tx,
            None => {
                error!("child pipeline not started: child_tx is None");
                return Ok(()); // skip starting this collector
            }
        };

        let peer_id = peer.id.clone();
        if self.child_collectors.contains_key(&peer_id) {
            return Ok(());
        }

        let parent = CollectedParent {
            id: peer_id.clone(),
            host: peer.host,
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };

        let mut collector = PieceCollector::new(
            self.config.clone(),
            &self.host_id,
            &self.task_id,
            self.interested_pieces.clone(),
            parent,
        )
        .await;

        let mut collector_rx = collector.run().await;
        self.child_collectors.insert(peer_id.clone(), collector);

        tokio::spawn(async move {
            while let Some(piece) = collector_rx.recv().await {
                if tx.send(piece).await.is_err() {
                    break;
                }
            }
        });

        info!("child collector started for peer {}", peer_id);
        Ok(())
    }

    /// insert_parent_piece upserts into parent collected map and notifies selector waiters.
    pub async fn insert_parent_piece(&self, piece: CollectedPiece) {
        let number = piece.number;

        // First check: if already selected, drop immediately.
        if self.selected_pieces.contains(&number) {
            return;
        }

        Self::upsert_and_merge_entry(&self.collected_pieces_parents, piece);

        // Second check: handle race where select marked it after our first check.
        if self.selected_pieces.contains(&number) {
            self.collected_pieces_parents.remove(&number);
            return;
        }
        // Notify select_piece() that a parent piece may be available.
        self.parent_piece_notify.notify_one();
    }

    /// insert_child_piece upserts into child collected map.
    pub async fn insert_child_piece(&self, piece: CollectedPiece) {
        Self::upsert_and_merge_entry(&self.collected_pieces_children, piece);
    }

    /// upsert_and_merge_entry upserts a CollectedPiece and merges parents (dedupe by parent id).
    fn upsert_and_merge_entry(map: &DashMap<u32, CollectedPiece>, incoming: CollectedPiece) {
        let number = incoming.number;

        match map.entry(number) {
            Entry::Vacant(v) => {
                debug!("inserting new piece {} into map", number);
                v.insert(incoming);
            }
            Entry::Occupied(mut o) => {
                debug!("merging piece {} into existing map", number);
                let existing = o.get_mut();
                for p in incoming.parents {
                    if !existing.parents.iter().any(|ep| ep.id == p.id) {
                        existing.parents.push(p);
                    }
                }
            }
        }
    }

    /// insert_child registers the child peer and starts its collector.
    pub async fn insert_child(self: &Arc<Self>, child: Peer) {
        let id = child.id.clone();
        let is_new = self.children.insert(id.clone(), child.clone()).is_none();
        info!("inserted child peer {}: {}", id, is_new);
        if !is_new {
            return;
        }

        if let Err(err) = self.start_child_collector(child).await {
            error!("start child collector failed for {}: {}", id, err);
        }
    }

    /// remove_child unregisters the child peer and shuts down its collector if present.
    pub async fn remove_child(self: &Arc<Self>, child: Peer) {
        let id = child.id.clone();
        self.children.remove(&id);
        info!("removed child peer {}", id);

        if let Some((_, mut collector)) = self.child_collectors.remove(&id) {
            collector.shutdown().await;
        }

        // Cleanup pieces contributed by this child.
        self.cleanup_child_pieces(&id);
    }

    /// Removes the given child id from collected_pieces_children.
    /// If a piece ends up with an empty parents list, remove the entry entirely.
    fn cleanup_child_pieces(&self, child_id: &str) {
        // Phase 1: collect affected keys (read-only iteration).
        let keys: Vec<u32> = self
            .collected_pieces_children
            .iter()
            .filter_map(|e| {
                if e.value().parents.iter().any(|p| p.id == child_id) {
                    Some(*e.key())
                } else {
                    None
                }
            })
            .collect();

        // Phase 2: mutate/remove entries using get_mut/remove.
        for number in keys {
            if let Some(mut entry) = self.collected_pieces_children.get_mut(&number) {
                entry.parents.retain(|p| p.id != child_id);

                if entry.parents.is_empty() {
                    // Drop the mutable guard before removing to avoid deadlock.
                    drop(entry);
                    self.collected_pieces_children.remove(&number);
                }
            }
        }
        debug!("cleaned up child pieces for {}", child_id)
    }

    /// shutdown_parent_collector shuts down and removes a parent collector by id.
    pub async fn shutdown_parent_collector(self: &Arc<Self>, peer_id: &str) -> bool {
        let Some((_, mut collector)) = self.parent_collectors.remove(peer_id) else {
            return false;
        };
        collector.shutdown().await;
        info!("parent collector {} shut down", peer_id);
        true
    }

    /// shutdown_child_collector shuts down and removes a child collector by id.
    pub async fn shutdown_child_collector(self: &Arc<Self>, peer_id: &str) -> bool {
        let Some((_, mut collector)) = self.child_collectors.remove(peer_id) else {
            return false;
        };
        collector.shutdown().await;
        info!("child collector {} shut down", peer_id);
        true
    }

    /// select_piece selects one piece randomly from parent collected pieces only.
    ///
    /// Behavior:
    /// - If remaining_pieces == 0, returns None immediately.
    /// - If no parent piece is available, waits until parent insertion notifies.
    /// - On success, removes the selected piece from parent map and decrements remaining by 1.
    pub async fn select_piece(&self) -> Option<CollectedPiece> {
        loop {
            if self.remaining_pieces.load(Ordering::Acquire) == 0 {
                return None;
            }
            
            // Pick & remove one piece from parent map.
            let Some(piece) = self.try_select_from_parents() else {
                self.parent_piece_notify.notified().await;
                continue;
            };

            let number = piece.number;

            // Mark selected first to block future inserts.
            self.selected_pieces.insert(number);

            // Cleanup: in case an insert raced and re-added it after we removed,
            // remove again (idempotent).
            self.collected_pieces_parents.remove(&number);

            // Decrement remaining pieces.
            let prev = self.remaining_pieces.fetch_sub(1, Ordering::AcqRel);
            if prev == 0 {
                self.remaining_pieces.store(0, Ordering::Release);
            }

            return Some(piece);
        }
    }

    /// try_select_from_parents_once_no_alloc randomly chooses one key from the DashMap without allocating.
    ///
    /// Uses reservoir sampling in one pass.
    fn try_select_from_parents(&self) -> Option<CollectedPiece> {
        let mut chosen: Option<u32> = None;
        let mut seen: u32 = 0;
        let mut rng = rand::rng();

        for entry in self.collected_pieces_parents.iter() {
            seen += 1;
            if rng.random_range(0..seen) == 0 {
                chosen = Some(*entry.key());
            }
        }

        let number = chosen?;
        self.collected_pieces_parents.remove(&number).map(|(_, v)| v)
    }

    /// shutdown stops both pipelines and all collectors.
    ///
    /// Behavior:
    /// - Wakes select_piece() waiters.
    /// - Cancels both consumers.
    /// - Shuts down all collectors.
    pub async fn shutdown(self: &Arc<Self>) {
        // Prevent select_piece() from blocking forever.
        self.remaining_pieces.store(0, Ordering::Release);
        self.parent_piece_notify.notify_waiters();

        // Cancel both consumers.
        self.cancel.cancel();

        // Stop all parent collectors.
        let pkeys: Vec<String> = self.parent_collectors.iter().map(|e| e.key().clone()).collect();
        for k in pkeys {
            if let Some((_, mut c)) = self.parent_collectors.remove(&k) {
                c.shutdown().await;
            }
        }

        // Stop all child collectors.
        let ckeys: Vec<String> = self.child_collectors.iter().map(|e| e.key().clone()).collect();
        for k in ckeys {
            if let Some((_, mut c)) = self.child_collectors.remove(&k) {
                c.shutdown().await;
            }
        }

        // Abort consumers (optional; cancellation already requested).
        if let Some(h) = self.parent_consumer_handle.get() {
            h.abort();
        }
        if let Some(h) = self.child_consumer_handle.get() {
            h.abort();
        }
    }

    /// remaining returns how many pieces are still expected to be selected.
    pub fn remaining(&self) -> usize {
        self.remaining_pieces.load(Ordering::Acquire)
    }
}
