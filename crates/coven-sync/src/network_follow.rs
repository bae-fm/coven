//! What the store does as its watch of the device's network hears a change
//! (§6.7): each usable network that appears, or that the device moves to,
//! is a network report to the store's retry waits, and the network as the
//! watch now says it is is what coven's views show (§4). Each owner that
//! holds transfers for a metered network watches it itself (§17.9).

use crate::RetryWaits;
use coven_database::Database;
use coven_foundation::network_watch::NetworkWatch;
use std::sync::Arc;
use tokio::sync::watch;

/// Follows one open store's network watch from opening to closing: built
/// as the store opens, already showing what the platform says, and closed
/// before the database. It keeps the watch, whose platform registration
/// ends with it.
pub struct NetworkFollow {
    _watch: NetworkWatch,
    stop: watch::Sender<bool>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl NetworkFollow {
    /// Show the network as the watch says it is now, report it if it is
    /// usable, so a wait saved before the store opened ends now, then follow
    /// each change.
    pub fn start(watched: NetworkWatch, waits: Arc<RetryWaits>, database: Database) -> Self {
        let mut network = watched.view();
        let first = network.read();
        database.record_network(first);
        if first.usable == Some(true) {
            waits.report_network();
        }
        let (stop, mut stopping) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut joined = first.joined;
            loop {
                tokio::select! {
                    biased;
                    _ = stopping.changed() => break,
                    () = network.changed() => {}
                }
                let state = network.read();
                database.record_network(state);
                // Coalesced changes count once: each report ends the same
                // waits (§6.7).
                if state.joined != joined {
                    joined = state.joined;
                    waits.report_network();
                }
            }
        });
        Self { _watch: watched, stop, task: tokio::sync::Mutex::new(Some(task)) }
    }

    /// Stop following; nothing reports after this returns.
    pub async fn close(&self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.lock().await.take() {
            crate::files::stopped("the network follower", task).await;
        }
    }
}

#[cfg(test)]
#[path = "network_follow_tests.rs"]
mod tests;
