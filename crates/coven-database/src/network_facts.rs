//! The device's network as this process last heard of it (§4, §6.7, §17.9):
//! whether it has a usable network, and whether that network is metered.
//! Nothing about it is stored: each open store's watch of the network tells
//! it, from the moment the store opens. App SQL reads it through coven's
//! views, `coven_network` and `coven_uploads.metered`, as each transaction's
//! snapshot saw it (`memory_facts.rs`), and a live query that reads one runs
//! again as the network changes, as if a write had changed what it read.

use crate::memory_facts::{MemoryFacts, SharedFacts};
use crate::store_failure::StoreHandle;
use coven_foundation::network_watch::NetworkState;
use sqlive::rusqlite::functions::FunctionFlags;
use sqlive::{Setup, SnapshotValue};
use std::sync::Arc;

/// Coven's functions that read the network, which app SQL reaches only
/// through coven's views.
pub(crate) const FUNCTIONS: [&str; 2] = ["_coven_network_usable", "_coven_network_metered"];

/// What a change of the network changes, as live queries read it.
pub(crate) const READ_AS: [(&str, &str); 3] =
    [("coven_network", "usable"), ("coven_network", "metered"), ("coven_uploads", "metered")];

/// What the views show of the network: whether the device has a usable
/// network, `None` where nothing tells, and whether it is metered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct NetworkFact {
    usable: Option<bool>,
    metered: bool,
}

/// The network as one open store last heard of it, which it keeps among
/// the store's facts (`memory_facts.rs`): one per open store, whose clones
/// share it.
#[derive(Clone)]
pub(crate) struct NetworkFacts {
    facts: SharedFacts,
    /// Told of each change, as of a commit that changed what the live
    /// queries reading the views read.
    observer: StoreHandle,
}

impl NetworkFacts {
    /// Nothing heard yet: no usable network known, and none metered.
    pub(crate) fn new(observer: StoreHandle, facts: SharedFacts) -> Self {
        Self { facts, observer }
    }

    /// The store's watch says the network is now `network`; a live query
    /// that read the old one runs again.
    pub(crate) fn set(&self, network: NetworkState) {
        let fact = NetworkFact { usable: network.usable, metered: network.metered };
        let changed = self.facts.change(|facts| {
            let changed = facts.network != fact;
            facts.network = fact;
            changed
        });
        if changed {
            self.observer.changed_without_write(&READ_AS);
        }
    }
}

/// Register the functions on a connection to the store's database, reading
/// the network as each transaction's snapshot saw it. None is
/// deterministic; both are safe in coven's views.
pub(crate) fn register(
    setup: &Setup<'_, crate::app_policy::CovenPolicy>,
    at_snapshot: &SnapshotValue<Arc<MemoryFacts>>,
) -> sqlive::rusqlite::Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;
    let facts = at_snapshot.clone();
    setup.scalar_function(FUNCTIONS[0], 0, flags, move |_| Ok(facts.get().network.usable))?;
    let facts = at_snapshot.clone();
    setup.scalar_function(FUNCTIONS[1], 0, flags, move |_| Ok(facts.get().network.metered))
}

#[cfg(test)]
#[path = "network_facts_tests.rs"]
mod tests;
