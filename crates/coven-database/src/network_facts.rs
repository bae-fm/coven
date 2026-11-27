//! The device's network as this process last heard of it (§4, §6.7, §17.9):
//! whether it has a usable network, and whether that network is metered.
//! Nothing about it is stored in the database: each open store's watch of the
//! network tells it, from the moment the store opens, and it is the one row
//! of `_coven_network` (`memory_facts.rs`). The row exists from the start, so
//! what the watch says only ever changes it, and runs again no live query
//! that reads no column it changed. App SQL reads it through coven's
//! views, `coven_network` and `coven_uploads.metered`, as each transaction's
//! snapshot saw it, and a change runs again each live query that reads what
//! changed.

use coven_foundation::network_watch::NetworkState;
use sqlive::{HostTables, SqlValue};

/// The network as one open store last heard of it: one per open store, whose
/// clones share it.
#[derive(Clone)]
pub(crate) struct NetworkFacts {
    tables: HostTables,
}

impl NetworkFacts {
    /// Nothing heard yet: no usable network known, and none metered.
    pub(crate) fn new(tables: HostTables) -> Self {
        let facts = Self { tables };
        facts.set(NetworkState { usable: None, metered: false, joined: 0 });
        facts
    }

    /// The store's watch says the network is now `network`.
    pub(crate) fn set(&self, network: NetworkState) {
        let usable =
            network.usable.map_or(SqlValue::Null, |usable| SqlValue::Integer(usable.into()));
        self.tables.change(|rows| {
            rows.put(
                "_coven_network",
                vec![SqlValue::Integer(1), usable, SqlValue::Integer(network.metered.into())],
            );
        });
    }
}

#[cfg(test)]
#[path = "network_facts_tests.rs"]
mod tests;
