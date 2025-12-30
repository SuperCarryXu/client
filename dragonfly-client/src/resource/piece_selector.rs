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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use dragonfly_api::common::v2::Peer;
use dragonfly_api::scheduler::v2::{AnnouncePeerRequest, GetChildPeerRequest};
use dragonfly_api::scheduler::v2::announce_peer_request;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result;
use dragonfly_client_storage::metadata;
use rand::Rng;
use tokio::sync::{Mutex, Notify, OnceCell, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tracing::{debug, error, info};
use tokio::time::{Duration};

use crate::grpc::scheduler::SchedulerClient;
use crate::resource::piece_collector::{CollectedPiece, CollectedParent, PieceCollector};

/// PieceSelector maintains two independent pipelines:
/// - Parent pipeline: initialized once in run(), only inserts into parent collected map.
/// - Child pipeline: initialized once in run() (pipeline only), supports dynamic add/remove child collectors.
///
/// Selection is performed ONLY from parent collected pieces.
pub struct PieceSelector {
    config: Arc<Config>,
    host_id: String,
    peer_id: String,
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

    // Fast selection index:
    // - available_numbers keeps all piece numbers that have ever been inserted into parent map.
    // - available_pos enables O(1) removal by swap_remove.
    available_numbers: Arc<Mutex<Vec<u32>>>,
    available_pos: Arc<DashMap<u32, usize>>,

    // Parent pipeline: created once in run().
    parent_tx: OnceCell<mpsc::Sender<CollectedPiece>>,
    parent_consumer_handle: OnceCell<JoinHandle<()>>,

    // Child pipeline: created once in run(), collectors are dynamic.
    child_tx: OnceCell<mpsc::Sender<CollectedPiece>>,
    child_consumer_handle: OnceCell<JoinHandle<()>>,

    // Scheduler client so selector can talk to scheduler when needed.
    scheduler_client: Arc<SchedulerClient>,

    // Announce stream manager: optional sender (recreated on reconnect) and manager handle.
    announce_tx: Arc<Mutex<Option<mpsc::Sender<AnnouncePeerRequest>>>>,
    announce_manager_handle: OnceCell<JoinHandle<()>>,

    // Global cancellation for both pipelines.
    cancel: CancellationToken,
}

impl PieceSelector {
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        peer_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<Peer>,
        scheduler_client: Arc<SchedulerClient>,
    ) -> Self {
        let remaining = interested_pieces.len();
        Self {
            config,
            host_id: host_id.to_string(),
            peer_id: peer_id.to_string(),
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

            available_numbers: Arc::new(Mutex::new(Vec::new())),
            available_pos: Arc::new(DashMap::new()),

            parent_tx: OnceCell::new(),
            parent_consumer_handle: OnceCell::new(),
            child_tx: OnceCell::new(),
            child_consumer_handle: OnceCell::new(),

            scheduler_client,

            announce_tx: Arc::new(Mutex::new(None)),
            announce_manager_handle: OnceCell::new(),

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

        // Start announce manager once (persistent announce_peer stream with reconnect).
        self.start_announce_manager_once();

        // Start parent collectors once.
        for peer in self.parents.iter().cloned() {
            self.start_parent_collector(peer)
                .await
                .inspect_err(|e| {
                    error!("failed to start parent collector: {}", e);
                })?;
        }

        info!("piece selector started for task {}", self.task_id);
        Ok(())
    }

    /// Starts announce manager once; if already started, do nothing.
    fn start_announce_manager_once(self: &Arc<Self>) {
        if self.announce_manager_handle.get().is_some() {
            return;
        }

        let manager = Arc::clone(self);
        let handle = tokio::spawn(async move {
            // Backoff in seconds.
            let mut backoff = 1u64;

            loop {
                if manager.cancel.is_cancelled() {
                    break;
                }

                // Create a fresh channel per connection attempt.
                let (tx, rx) = mpsc::channel::<AnnouncePeerRequest>(8);

                // Publish sender for this connection.
                {
                    let mut guard = manager.announce_tx.lock().await;
                    *guard = Some(tx.clone());
                }

                // Periodic sender: send GetChildPeerRequest every 1s.
                let sender_manager = Arc::clone(&manager);
                let sender_cancel = sender_manager.cancel.clone();
                let sender_tx = tx.clone();

                let sender_handle = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_secs(1));
                    loop {
                        tokio::select! {
                            _ = sender_cancel.cancelled() => break,
                            _ = ticker.tick() => {
                                let req = AnnouncePeerRequest {
                                    host_id: sender_manager.host_id.clone(),
                                    task_id: sender_manager.task_id.clone(),
                                    peer_id: sender_manager.peer_id.clone(),
                                    request: Some(announce_peer_request::Request::GetChildPeerRequest(
                                        GetChildPeerRequest { description: None },
                                    )),
                                };

                                if sender_tx.send(req).await.is_err() {
                                    // Connection consumer is gone; exit and let manager reconnect.
                                    break;
                                }
                            }
                        }
                    }
                    debug!("announce sender task exited for task {}", sender_manager.task_id);
                });

                // Build request stream.
                let in_stream = ReceiverStream::new(rx);
                let request_stream = Request::new(in_stream);

                // Connect announce stream.
                match manager
                    .scheduler_client
                    .announce_peer(manager.task_id.as_str(), manager.peer_id.as_str(), request_stream)
                    .await
                {
                    Ok(response) => {
                        backoff = 1;
                        info!("announce_peer connected for task {}", manager.task_id);

                        let mut out = response.into_inner();

                        loop {
                            if manager.cancel.is_cancelled() {
                                break;
                            }

                            match tokio::time::timeout(
                                manager.config.scheduler.schedule_timeout,
                                out.try_next(),
                            )
                            .await
                            {
                                Ok(Ok(Some(msg))) => {
                                    if let Some(resp) = msg.response {
                                        if let dragonfly_api::scheduler::v2::announce_peer_response::Response::NormalTaskResponse(nr) = resp {
                                            // Sync children peers.
                                            if let Err(e) = manager.sync_children(nr.candidate_parents).await {
                                                error!("sync_children failed: {:?}", e);
                                            }
                                        }
                                    }
                                }
                                Ok(Ok(None)) => {
                                    // Server closed stream.
                                    info!("announce_peer stream ended for task {}", manager.task_id);
                                    break;
                                }
                                Ok(Err(err)) => {
                                    error!("announce_peer stream error: {:?}", err);
                                    break;
                                }
                                Err(_) => {
                                    // Timeout reading server response; keep the connection and retry.
                                    debug!("announce_peer read timeout for task {}", manager.task_id);
                                    continue;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error!("announce_peer connect failed: {:?}", err);
                    }
                }

                // Cleanup sender.
                {
                    let mut guard = manager.announce_tx.lock().await;
                    *guard = None;
                }
                sender_handle.abort();

                // Backoff before reconnect.
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = std::cmp::min(backoff * 2, 30);
            }

            info!("announce manager exited for task {}", manager.task_id);
        });

        // If set fails (shouldn't in your single-run model), abort new handle to avoid leak.
        if self.announce_manager_handle.set(handle).is_err() {
            if let Some(h) = self.announce_manager_handle.get() {
                h.abort();
            }
        }
    }

    /// start_parent_pipeline creates the parent channel and a consumer task that inserts into parent map.
    async fn start_parent_pipeline(self: &Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<CollectedPiece>(1024);

        if self.parent_tx.set(tx).is_err() {
            error!("parent pipeline already started, skipping");
            return Ok(());
        }

        if self.parent_consumer_handle.get().is_some() {
            error!("parent consumer handle already set, skipping");
            return Ok(());
        }

        let this = Arc::clone(self);
        let cancel = this.cancel.clone();
        let task_id = this.task_id.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_piece = rx.recv() => {
                        match maybe_piece {
                            None => break,
                            Some(piece) => this.insert_parent_piece(piece).await,
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

        info!("parent pipeline started for task {}", self.task_id);
        Ok(())
    }

    /// start_child_pipeline creates the child channel and a consumer task that inserts into child map.
    async fn start_child_pipeline(self: &Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<CollectedPiece>(1024);

        if self.child_tx.set(tx).is_err() {
            error!("child pipeline already started, skipping");
            return Ok(());
        }

        if self.child_consumer_handle.get().is_some() {
            error!("child consumer handle already set, skipping");
            return Ok(());
        }

        let this = Arc::clone(self);
        let cancel = this.cancel.clone();
        let task_id = this.task_id.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe_piece = rx.recv() => {
                        match maybe_piece {
                            None => break,
                            Some(piece) => this.insert_child_piece(piece).await,
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

        info!("child pipeline started for task {}", self.task_id);
        Ok(())
    }

    /// start_parent_collector starts a collector for a parent peer and forwards into parent pipeline.
    pub async fn start_parent_collector(self: &Arc<Self>, peer: Peer) -> Result<()> {
        let tx = match self.parent_tx.get().cloned() {
            Some(tx) => tx,
            None => {
                error!("parent pipeline not started: parent_tx is None");
                return Ok(());
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

        let peer_id_clone = peer_id.clone();
        tokio::spawn(async move {
            while let Some(piece) = collector_rx.recv().await {
                if tx.send(piece).await.is_err() {
                    break;
                }
            }
            debug!("parent collector forwarder exited for peer {}", peer_id_clone);
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
                return Ok(());
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

        let peer_id_clone = peer_id.clone();
        tokio::spawn(async move {
            while let Some(piece) = collector_rx.recv().await {
                if tx.send(piece).await.is_err() {
                    break;
                }
            }
            debug!("child collector forwarder exited for peer {}", peer_id_clone);
        });

        info!("child collector started for peer {}", peer_id);
        Ok(())
    }

    /// insert_parent_piece upserts into parent collected map.
    ///
    /// Performance:
    /// - On first insertion of a new piece number, it is added into the available index.
    /// - Subsequent inserts only merge parents.
    ///
    /// Optimization:
    /// - If the piece is already selected, return early to skip expensive merge/dedupe.
    pub async fn insert_parent_piece(&self, piece: CollectedPiece) {
        let number = piece.number;

        // Fast path: already selected, skip expensive work.
        if self.selected_pieces.contains(&number) {
            debug!("skip insert/merge for selected piece {}", number);
            return;
        }

        let inserted_new = match self.collected_pieces_parents.entry(number) {
            Entry::Vacant(v) => {
                debug!("insert new parent piece {}", number);
                v.insert(piece);
                true
            }
            Entry::Occupied(mut o) => {
                // Re-check under entry lock (best effort).
                if self.selected_pieces.contains(&number) {
                    debug!("skip merge for selected piece {}", number);
                    return;
                }

                debug!("merge parent piece {}", number);
                let existing = o.get_mut();

                // Merge parents with dedupe by parent id.
                for p in piece.parents {
                    if !existing.parents.iter().any(|ep| ep.id == p.id) {
                        existing.parents.push(p);
                    }
                }
                // Update length if needed (keep the latest).
                existing.length = piece.length;
                false
            }
        };

        if inserted_new {
            self.add_to_available_index(number).await;
            self.parent_piece_notify.notify_one();
        }
    }

    /// insert_child_piece upserts into child collected map.
    pub async fn insert_child_piece(&self, piece: CollectedPiece) {
        let number = piece.number;

        match self.collected_pieces_children.entry(number) {
            Entry::Vacant(v) => {
                debug!("insert new child piece {}", number);
                v.insert(piece);
            }
            Entry::Occupied(mut o) => {
                debug!("merge child piece {}", number);
                let existing = o.get_mut();
                for p in piece.parents {
                    if !existing.parents.iter().any(|ep| ep.id == p.id) {
                        existing.parents.push(p);
                    }
                }
                existing.length = piece.length;
            }
        }
    }

    async fn add_to_available_index(&self, number: u32) {
        if self.available_pos.contains_key(&number) {
            return;
        }

        let mut vec = self.available_numbers.lock().await;

        // Double-check under mutex.
        if self.available_pos.contains_key(&number) {
            return;
        }

        let idx = vec.len();
        vec.push(number);
        self.available_pos.insert(number, idx);
    }

    async fn pop_random_available_number(&self) -> Option<u32> {
        let mut vec = self.available_numbers.lock().await;
        if vec.is_empty() {
            return None;
        }

        let idx = rand::rng().random_range(0..vec.len());
        let number = vec.swap_remove(idx);

        self.available_pos.remove(&number);
        if idx < vec.len() {
            let moved = vec[idx];
            self.available_pos.insert(moved, idx);
        }

        Some(number)
    }

    /// sync_children updates dynamic children set by diffing with the provided peers list.
    ///
    /// Optimization:
    /// - Snapshot children as (id, peer) to avoid a second DashMap lookup.
    pub async fn sync_children(self: &Arc<Self>, peers: Vec<Peer>) -> Result<()> {
        // Desired set (id -> peer).
        let mut desired: HashMap<String, Peer> = HashMap::with_capacity(peers.len());
        for p in peers {
            desired.insert(p.id.clone(), p);
        }

        // Snapshot current children as (id, peer).
        let current: Vec<(String, Peer)> = self
            .children
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        // Remove stale.
        for (id, peer) in current {
            if desired.contains_key(&id) {
                continue;
            }
            self.remove_child(peer).await;
            info!("removed stale child {}", id);
        }

        // Add new.
        for (_id, peer) in desired {
            let id = peer.id.clone();
            if self.children.contains_key(&peer.id) {
                continue;
            }
            self.insert_child(peer).await;
            info!("added new child {}", id);
        }
        Ok(())
    }

    /// insert_child registers the child peer and starts its collector.
    pub async fn insert_child(self: &Arc<Self>, child: Peer) {
        let id = child.id.clone();
        let is_new = self.children.insert(id.clone(), child.clone()).is_none();
        if !is_new {
            return;
        }

        info!("child inserted: {}", id);

        if let Err(err) = self.start_child_collector(child).await {
            error!("start child collector failed for {}: {}", id, err);
        }
    }

    /// remove_child unregisters the child peer and shuts down its collector if present.
    pub async fn remove_child(self: &Arc<Self>, child: Peer) {
        let id = child.id.clone();
        self.children.remove(&id);

        info!("child removed: {}", id);

        if let Some((_, mut collector)) = self.child_collectors.remove(&id) {
            collector.shutdown().await;
        }

        self.cleanup_child_pieces(&id);
    }

    /// Removes the given child id from collected_pieces_children.
    /// If a piece ends up with an empty parents list, remove the entry entirely.
    fn cleanup_child_pieces(&self, child_id: &str) {
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

        for number in keys {
            if let Some(mut entry) = self.collected_pieces_children.get_mut(&number) {
                entry.parents.retain(|p| p.id != child_id);

                if entry.parents.is_empty() {
                    drop(entry);
                    self.collected_pieces_children.remove(&number);
                }
            }
        }

        debug!("cleanup child pieces done for {}", child_id);
    }

    /// select_piece selects one piece quickly from parent side only (no DashMap scanning).
    pub async fn select_piece(&self) -> Option<CollectedPiece> {
        loop {
            if self.remaining_pieces.load(Ordering::Acquire) == 0 {
                return None;
            }

            let Some(number) = self.pop_random_available_number().await else {
                self.parent_piece_notify.notified().await;
                continue;
            };

            // If already selected, skip.
            if self.selected_pieces.contains(&number) {
                continue;
            }

            // Mark selected first.
            self.selected_pieces.insert(number);

            // Remove and return.
            let Some((_k, piece)) = self.collected_pieces_parents.remove(&number) else {
                // Missing; keep selected marker monotonic and continue.
                continue;
            };

            // Decrement remaining pieces.
            let prev = self.remaining_pieces.fetch_sub(1, Ordering::AcqRel);
            if prev == 0 {
                self.remaining_pieces.store(0, Ordering::Release);
            }

            info!("selected piece {} (remaining {})", number, self.remaining());
            return Some(piece);
        }
    }

    /// shutdown stops both pipelines and all collectors.
    pub async fn shutdown(self: &Arc<Self>) {
        // Prevent select_piece from blocking forever.
        self.remaining_pieces.store(0, Ordering::Release);
        self.parent_piece_notify.notify_waiters();

        self.cancel.cancel();

        // Stop collectors.
        let pkeys: Vec<String> = self.parent_collectors.iter().map(|e| e.key().clone()).collect();
        for k in pkeys {
            if let Some((_, mut c)) = self.parent_collectors.remove(&k) {
                c.shutdown().await;
            }
        }

        let ckeys: Vec<String> = self.child_collectors.iter().map(|e| e.key().clone()).collect();
        for k in ckeys {
            if let Some((_, mut c)) = self.child_collectors.remove(&k) {
                c.shutdown().await;
            }
        }

        // Abort tasks.
        if let Some(h) = self.parent_consumer_handle.get() {
            h.abort();
        }
        if let Some(h) = self.child_consumer_handle.get() {
            h.abort();
        }
        if let Some(h) = self.announce_manager_handle.get() {
            h.abort();
        }

        info!("piece selector shutdown for task {}", self.task_id);
    }

    pub fn remaining(&self) -> usize {
        self.remaining_pieces.load(Ordering::Acquire)
    }
}
