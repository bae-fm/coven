//! The connections the upload worker's attempts use, lane by lane: each
//! attempt one, and an attempt sending a file's parts together more, while
//! its lane has one free and no upload waits to start in it (§17.9).

use super::file_upload_pool::Lane;
use coven_foundation::locks::lock_unpoisoned;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// What each lane's attempts use, shared by the upload worker and the
/// attempts it runs.
pub(crate) struct UploadConnections {
    lanes: Mutex<[LaneUse; 2]>,
    /// A connection an attempt took beside its own ended: an upload waiting
    /// to start may take it.
    freed: Notify,
}

#[derive(Clone, Copy)]
struct LaneUse {
    /// Attempts in flight, each on a connection of its own.
    attempts: usize,
    /// Connections attempts took beside their own, for parts sent together.
    beside: usize,
    /// An upload of the lane is ready and waits for a connection.
    waiting: bool,
}

/// A connection one attempt took beside its own, for one part; it goes back
/// to the lane when dropped.
pub(crate) struct SpareConnection {
    connections: Arc<UploadConnections>,
    lane: Lane,
}

impl Drop for SpareConnection {
    fn drop(&mut self) {
        lock_unpoisoned(&self.connections.lanes)[self.lane.index()].beside -= 1;
        self.connections.freed.notify_one();
    }
}

impl UploadConnections {
    pub(super) fn new() -> Self {
        let unused = LaneUse { attempts: 0, beside: 0, waiting: false };
        Self { lanes: Mutex::new([unused; 2]), freed: Notify::new() }
    }
    /// The connections `lane`'s attempts took beside their own.
    pub(super) fn beside(&self, lane: Lane) -> usize {
        lock_unpoisoned(&self.lanes)[lane.index()].beside
    }
    /// The worker's count of `lane`'s attempts in flight.
    pub(super) fn set_attempts(&self, lane: Lane, attempts: usize) {
        lock_unpoisoned(&self.lanes)[lane.index()].attempts = attempts;
    }
    /// Whether an upload of `lane` is ready and waits for a connection.
    pub(super) fn set_waiting(&self, lane: Lane, waiting: bool) {
        lock_unpoisoned(&self.lanes)[lane.index()].waiting = waiting;
    }
    /// Whether the worker last found an upload of `lane` waiting for a
    /// connection, for a test.
    #[cfg(test)]
    pub(super) fn waiting(&self, lane: Lane) -> bool {
        lock_unpoisoned(&self.lanes)[lane.index()].waiting
    }
    /// Another connection for an attempt in `lane`, if the lane uses fewer
    /// than `limit` and no upload of it waits to start: an upload waiting
    /// takes a freed connection before a part beside another attempt.
    pub(super) fn spare(self: &Arc<Self>, lane: Lane, limit: usize) -> Option<SpareConnection> {
        let mut lanes = lock_unpoisoned(&self.lanes);
        let used = &mut lanes[lane.index()];
        if used.waiting || used.attempts + used.beside >= limit {
            return None;
        }
        used.beside += 1;
        Some(SpareConnection { connections: self.clone(), lane })
    }
    /// Resolves once a connection taken beside an attempt has ended since
    /// it last resolved.
    pub(super) async fn freed(&self) {
        self.freed.notified().await;
    }
}
