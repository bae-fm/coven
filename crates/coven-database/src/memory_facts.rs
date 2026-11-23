//! What app SQL reads of this process's memory, which no table holds (§4):
//! the stored objects it is sending, those it is downloading for pins
//! (`transfers_in_flight.rs`), the copies it is downloading to make files
//! device-only, and the device's network as it last heard of
//! it (`network_facts.rs`). All of them are one value, which each change
//! replaces under one lock. Each transaction on a connection to the store's
//! database takes that value as its snapshot is fixed, while no commit runs
//! (sqlive's `snapshot_values.rs`), and coven's functions read what it took:
//! a statement never joins the tables of one moment to facts of another.

use crate::database::file_database::copies::{
    self, CopyDownload, CopyNames, CopyPaths, ReservedCopy,
};
use crate::network_facts::{NetworkFact, NetworkFacts};
use crate::store_failure::StoreHandle;
use crate::transfers_in_flight::TransfersInFlight;
use coven_foundation::id_source::{DeviceId, ObjectId};
use coven_foundation::locks::lock_unpoisoned;
use sqlive::Setup;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// The facts at one moment.
#[derive(Clone, Default)]
pub(crate) struct MemoryFacts {
    /// Each stored object this process is sending, by its uploader.
    pub(crate) sending: BTreeMap<ObjectId, DeviceId>,
    /// Each stored object this process is downloading to keep whole, by its
    /// uploader.
    pub(crate) downloading: BTreeMap<ObjectId, DeviceId>,
    /// The destination paths this process is downloading device-only copies to.
    pub(crate) copy_downloads: BTreeSet<String>,
    /// The paths of copies still reserved to make files device-only.
    pub(crate) reserved_copies: BTreeMap<String, ReservedCopy>,
    /// The device's network.
    pub(crate) network: NetworkFact,
}

/// The facts as they are now. Clones share them.
#[derive(Clone)]
pub(crate) struct SharedFacts(Arc<Mutex<Arc<MemoryFacts>>>);

impl SharedFacts {
    /// No transfer in flight and no network heard of yet.
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(Arc::new(MemoryFacts::default()))))
    }

    /// The facts now, which no later change alters.
    pub(crate) fn now(&self) -> Arc<MemoryFacts> {
        Arc::clone(&lock_unpoisoned(&self.0))
    }

    /// Change the facts with `change`, all at once for every reader.
    pub(crate) fn change<R>(&self, change: impl FnOnce(&mut MemoryFacts) -> R) -> R {
        change(Arc::make_mut(&mut lock_unpoisoned(&self.0)))
    }
}

/// The facts of one open store and what changes them: the transfers in
/// flight and the network. Clones share them.
#[derive(Clone)]
pub(crate) struct ProcessFacts {
    facts: SharedFacts,
    observer: StoreHandle,
    /// The files this process is sending and downloading now.
    transfers: TransfersInFlight,
    /// The device's network as this process last heard of it.
    network: NetworkFacts,
}

impl ProcessFacts {
    /// No transfer in flight and no network heard of yet, telling `observer`
    /// of each change.
    pub(crate) fn new(observer: StoreHandle) -> Self {
        let facts = SharedFacts::new();
        Self {
            transfers: TransfersInFlight::new(observer.clone(), facts.clone()),
            network: NetworkFacts::new(observer.clone(), facts.clone()),
            observer,
            facts,
        }
    }

    /// The files this process is sending, as the upload worker marks them.
    pub(crate) fn uploads(&self) -> crate::InFlight {
        self.transfers.uploads()
    }

    /// The files this process is downloading to keep whole, as each pin's
    /// download marks them.
    pub(crate) fn downloads(&self) -> crate::InFlight {
        self.transfers.downloads()
    }

    /// Mark one device-only copy as downloading for the returned guard's scope.
    pub(crate) fn downloading_copy(&self, path: String) -> Result<CopyDownload, crate::DbError> {
        CopyDownload::start(self.facts.clone(), self.observer.clone(), path)
    }

    /// Reserve the copy paths under the writer that recorded them.
    pub(crate) fn reserve_copies(&self, paths: Vec<String>) -> CopyNames {
        CopyNames::new(self.facts.clone(), self.observer.clone(), paths)
    }

    /// Release the reservations a device-only switch brought into its write.
    pub(crate) fn release_copies(&self, reservations: Vec<CopyPaths>) {
        for paths in reservations {
            paths.release(&self.facts, &self.observer);
        }
    }

    /// The reserved copy paths as they stand now, for a caller under the writer.
    pub(crate) fn with_reserved_copies<R>(
        &self,
        read: impl FnOnce(&BTreeMap<String, ReservedCopy>) -> R,
    ) -> R {
        read(&self.facts.now().reserved_copies)
    }

    /// The store's watch says the network is now `network`.
    pub(crate) fn record_network(&self, network: coven_foundation::network_watch::NetworkState) {
        self.network.set(network);
    }

    /// Register the functions coven's views call to read the facts, on a
    /// connection to the store's database: each reads them as its
    /// transaction's snapshot saw them.
    pub(crate) fn register(
        &self,
        setup: &Setup<'_, crate::app_policy::CovenPolicy>,
    ) -> sqlive::rusqlite::Result<()> {
        let facts = self.facts.clone();
        let at_snapshot = setup.snapshot_value(move || facts.now());
        crate::transfers_in_flight::register(setup, &at_snapshot)?;
        crate::network_facts::register(setup, &at_snapshot)?;
        copies::register_copy_paths(setup, at_snapshot)
    }
}
