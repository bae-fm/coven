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
    task: tokio::sync::Mutex<Option<coven_foundation::task::OwnedTask>>,
    /// Each time the follower has reported every change it was told of
    /// and waits.
    #[cfg(any(test, feature = "test-utils"))]
    parks: crate::Parks,
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
        let bugs = database.bugs().clone();
        let parks = crate::Parks::new();
        #[cfg(any(test, feature = "test-utils"))]
        let follower_parks = parks.clone();
        // A panic goes to the store's bug sink, and the network stays as
        // last shown until the store closes.
        let follow = async move {
            let _running = parks.running();
            let mut joined = first.joined;
            loop {
                tokio::select! {
                    biased;
                    never = parks.polling() => match never {},
                    _ = stopping.changed() => break,
                    () = network.changed() => {}
                    never = parks.parked() => match never {},
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
        };
        let task =
            coven_foundation::task::spawn_owned("the network follower", bugs, follow, |_| async {});
        Self {
            _watch: watched,
            stop,
            task: tokio::sync::Mutex::new(Some(task)),
            #[cfg(any(test, feature = "test-utils"))]
            parks: follower_parks,
        }
    }

    /// Each time the follower has reported every network change it was
    /// told of, and waits for the next.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn parked_from_now(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.parks.parked_from_now()
    }

    /// Stop following; nothing reports after this returns.
    pub async fn close(&self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.lock().await.take() {
            task.ended().await;
        }
    }
}

#[cfg(test)]
#[path = "network_follow_tests.rs"]
mod tests;
