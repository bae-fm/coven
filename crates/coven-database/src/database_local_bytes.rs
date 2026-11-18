//! What the store keeps on this device, read from coven's tables (§17.8,
//! §19.3, Appendix C.3): the counters the cache keeps with its entries, the
//! partial downloads pins left, the sizes of the store's own files and of
//! the archives that resets kept.

use super::{Mode, StoreDatabase};
use crate::sqlite::{unsigned, StoreSql};
use crate::{DbError, LiveQuery};
use coven_foundation::locks::read_unpoisoned;
use coven_foundation::task::CarriesBug;

/// One namespace's cache, in bytes on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceBytes {
    /// The namespace's name as declared.
    pub namespace: String,
    /// Its budget, declared or since set.
    pub budget: u64,
    /// Chunks and headers: what counts toward the budget.
    pub cached: u64,
    /// Pinned whole copies.
    pub pinned: u64,
}

/// The cache's bytes: every declared namespace's, and the partial downloads
/// pins left, which the cache neither counts nor evicts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheBytes {
    /// Each declared namespace's cache, in declaration order.
    pub namespaces: Vec<NamespaceBytes>,
    /// Partial downloads pins left.
    pub pinning: u64,
}

/// What a store keeps on this device, in bytes on disk (§17.8, §19.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalBytes {
    /// Each declared namespace's cache, in declaration order.
    pub namespaces: Vec<NamespaceBytes>,
    /// Partial downloads pins left, kept apart from the cache.
    pub pinning: u64,
    /// The store's own files: its database files as their last checkpoints
    /// left them, coven's copies of app files, the snapshot and floor bodies
    /// this device captured to upload, and a rebuild's database files while
    /// one runs.
    pub library: u64,
    /// The total size of the archives that resets kept, while there is one.
    pub before_reset: Option<u64>,
}

/// Every namespace's budget and counters, by name.
pub(crate) fn budgets(db: &StoreSql) -> Result<Vec<NamespaceBytes>, DbError> {
    db.query(
        "SELECT namespace,budget,cached,pinned FROM _coven_cache_budgets ORDER BY namespace",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
    )?
    .into_iter()
    .map(|(namespace, budget, cached, pinned)| {
        Ok(NamespaceBytes {
            namespace,
            budget: unsigned(budget)?,
            cached: unsigned(cached)?,
            pinned: unsigned(pinned)?,
        })
    })
    .collect()
}

/// The sum of the partial downloads' sizes.
pub(crate) fn partials(db: &StoreSql) -> Result<u64, DbError> {
    unsigned(db.query_row("SELECT coalesce(sum(size),0) FROM _coven_cache_partials", [], |r| {
        r.get::<_, i64>(0)
    })?)
}

/// What the store keeps on this device, from the transaction under way:
/// the namespaces named in `declared`, in that order, the partial
/// downloads, the store's own files and the archives that resets kept.
pub(crate) fn read(db: &StoreSql, declared: &[String]) -> Result<LocalBytes, DbError> {
    let mut kept = budgets(db)?;
    let namespaces = declared
        .iter()
        .filter_map(|name| {
            kept.iter().position(|bytes| &bytes.namespace == name).map(|at| kept.swap_remove(at))
        })
        .collect();
    Ok(LocalBytes {
        namespaces,
        pinning: partials(db)?,
        library: crate::database::library_bytes::library(db)?,
        before_reset: crate::database::archive_bytes::kept(db)?,
    })
}

impl<M: Mode> StoreDatabase<M> {
    /// The names of the namespaces declared at open, in declaration order;
    /// none once the store has closed.
    fn declared_namespaces(&self) -> Vec<String> {
        read_unpoisoned(&self.inner).as_ref().map_or_else(Vec::new, |open| {
            M::access(open).write_schema.namespaces.iter().map(|n| n.name.clone()).collect()
        })
    }

    /// The cache's counters and the partial downloads' sizes, from one
    /// snapshot, for the namespaces declared at open, in declaration order.
    pub async fn cache_bytes(&self) -> Result<CacheBytes, DbError> {
        let declared = self.declared_namespaces();
        let bytes = self.read_committed(move |db| read(db, &declared)).await?;
        Ok(CacheBytes { namespaces: bytes.namespaces, pinning: bytes.pinning })
    }
}

impl StoreDatabase {
    /// What the store keeps on this device, from one snapshot of its tables
    /// (§17.8, §19.3): each declared namespace's cache, the partial
    /// downloads pins left, the store's own files and the archives resets
    /// kept.
    pub async fn local_bytes(&self) -> Result<LocalBytes, DbError> {
        let declared = self.declared_namespaces();
        self.read_committed(move |db| read(db, &declared)).await
    }

    /// `local_bytes` now and after each write that changes what it reads: a
    /// namespace's counters or budget, a partial download, a database
    /// file's size after a checkpoint, a copy kept or released, a captured
    /// body, a rebuild's folder or an archive. A cached read's recency
    /// changes none of them.
    pub fn subscribe_local_bytes<E>(&self) -> LiveQuery<LocalBytes, E>
    where
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        let declared = self.declared_namespaces();
        self.follow(move |db| read(db, &declared).map_err(E::from))
    }
}

#[cfg(test)]
#[path = "database_local_bytes_tests.rs"]
mod tests;
