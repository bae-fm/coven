//! What app SQL reads of this process's memory, which no table of the
//! database holds (§4): the stored objects it is sending, those it is
//! downloading for pins (`transfers_in_flight.rs`), the device-only copies it
//! is downloading and those it still reserves (`file_database_copies.rs`),
//! and the device's network as it last heard of it (`network_facts.rs`). Each
//! fact is a row of a host table (`coven_schema::MEMORY_TABLES`), keyed by
//! what it is a fact of, which sqlive keeps in memory beside the store's
//! database: coven's views read the tables, each transaction reads them as its
//! snapshot saw them, and a change to a row runs again the live queries that
//! read what its key gives them (sqlive's SPEC.md section 3).

use crate::database::file_database::copies::{
    self, CopyDownload, CopyNames, CopyPaths, ReservedCopies, ReservedCopy,
};
use crate::network_facts::NetworkFacts;
use crate::store_failure::StoreHandle;
use crate::transfers_in_flight::TransfersInFlight;
use sqlive::{HostTables, Setup};
use std::collections::BTreeMap;

/// The facts of one open store and what changes them: the transfers in
/// flight, the device-only copies and the network. Clones share them.
#[derive(Clone)]
pub(crate) struct ProcessFacts {
    /// The facts as app SQL reads them.
    tables: HostTables,
    /// The paths of copies reserved to make files device-only, with the
    /// calls reserving them.
    reserved: ReservedCopies,
    /// The files this process is sending and downloading now.
    transfers: TransfersInFlight,
    /// The device's network as this process last heard of it.
    network: NetworkFacts,
}

impl ProcessFacts {
    /// No transfer in flight, no copy reserved and no network heard of yet,
    /// whose transfers' watches follow `observer`.
    pub(crate) fn new(observer: StoreHandle) -> sqlive::rusqlite::Result<Self> {
        let tables = HostTables::new(&coven_schema::MEMORY_TABLES)?;
        Ok(Self {
            reserved: ReservedCopies::new(tables.clone()),
            transfers: TransfersInFlight::new(observer, tables.clone()),
            network: NetworkFacts::new(tables.clone()),
            tables,
        })
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
        CopyDownload::start(self.tables.clone(), path)
    }

    /// Reserve the copy paths under the writer that recorded them.
    pub(crate) fn reserve_copies(&self, paths: Vec<String>) -> CopyNames {
        CopyNames::new(self.reserved.clone(), paths)
    }

    /// Release the reservations a device-only switch brought into its write.
    pub(crate) fn release_copies(&self, reservations: &[CopyPaths]) {
        copies::release_reservations(&self.reserved, reservations);
    }

    /// The reserved copy paths as they stand now, for a caller under the writer.
    pub(crate) fn with_reserved_copies<R>(
        &self,
        read: impl FnOnce(&BTreeMap<String, ReservedCopy>) -> R,
    ) -> R {
        self.reserved.with(read)
    }

    /// The store's watch says the network is now `network`.
    pub(crate) fn record_network(&self, network: coven_foundation::network_watch::NetworkState) {
        self.network.set(network);
    }

    /// Read the facts' tables on a connection to the store's database.
    pub(crate) fn register(
        &self,
        setup: &Setup<'_, crate::app_policy::CovenPolicy>,
    ) -> sqlive::rusqlite::Result<()> {
        setup.host_tables(&self.tables)
    }
}
