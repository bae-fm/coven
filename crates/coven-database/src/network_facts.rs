//! The device's network as this process last heard of it (§4, §6.7, §17.9):
//! whether it has a usable network, and whether that network is metered.
//! Nothing about it is stored: each open store's watch of the network tells
//! it, from the moment the store opens. App SQL reads it through coven's
//! views, `coven_network` and `coven_uploads.metered`, and a live query that
//! reads one runs again as the network changes, as if a write had changed
//! what it read.

use crate::observation::{CommitObserver, RowChange};
use coven_foundation::network_watch::NetworkState;
use rusqlite::functions::FunctionFlags;
use std::sync::{Arc, Mutex};

/// Coven's functions that read the network, which app SQL reaches only
/// through coven's views.
pub(crate) const FUNCTIONS: [&str; 2] = ["_coven_network_usable", "_coven_network_metered"];

/// What a change of the network changes, as live queries read it.
const READ_AS: [(&str, &str); 3] =
    [("coven_network", "usable"), ("coven_network", "metered"), ("coven_uploads", "metered")];

/// What the views show of the network: whether the device has a usable
/// network, `None` where nothing tells, and whether it is metered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct NetworkFact {
    usable: Option<bool>,
    metered: bool,
}

/// The network as one open store last heard of it: one per open store, whose
/// clones share it.
#[derive(Clone)]
pub(crate) struct NetworkFacts {
    state: Arc<Mutex<NetworkFact>>,
    /// Told of each change, as of a commit that changed what the live
    /// queries reading the views read.
    observer: CommitObserver,
}

impl NetworkFacts {
    /// Nothing heard yet: no usable network known, and none metered.
    pub(crate) fn new(observer: CommitObserver) -> Self {
        Self { state: Arc::new(Mutex::new(NetworkFact::default())), observer }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, NetworkFact> {
        self.state.lock().expect("network facts poisoned")
    }

    fn current(&self) -> NetworkFact {
        *self.state()
    }

    /// The store's watch says the network is now `network`; a live query
    /// that read the old one runs again.
    pub(crate) fn set(&self, network: NetworkState) {
        let fact = NetworkFact { usable: network.usable, metered: network.metered };
        let changed = {
            let mut state = self.state();
            let changed = *state != fact;
            *state = fact;
            changed
        };
        if changed {
            self.observer.commit(
                crate::observation::Judged::unjudged(
                    READ_AS
                        .iter()
                        .map(|(table, column)| RowChange {
                            table: (*table).into(),
                            column: (*column).into(),
                            rows: None,
                        })
                        .collect(),
                ),
                None,
            );
        }
    }

    /// Register the functions on a connection to the store's database. None
    /// is deterministic; both are safe in coven's views.
    pub(crate) fn register(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;
        let facts = self.clone();
        connection
            .create_scalar_function(FUNCTIONS[0], 0, flags, move |_| Ok(facts.current().usable))?;
        let facts = self.clone();
        connection
            .create_scalar_function(FUNCTIONS[1], 0, flags, move |_| Ok(facts.current().metered))
    }
}

#[cfg(test)]
#[path = "network_facts_tests.rs"]
mod tests;
