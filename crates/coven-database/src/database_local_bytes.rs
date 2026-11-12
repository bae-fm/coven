//! The bytes the cache and pins keep on this device, read from the counters
//! the cache keeps with its entries (§17.8, Appendix C.3).

use super::{Database, Mode};
use crate::{sqlite::DatabaseConnection, DbError, LiveQuery};
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
    /// The store's database and its journals, its settings and key files, and
    /// coven's own copies of files: every file in the store's directory but
    /// the cache and the archives.
    pub library: u64,
    /// The archives resets kept, while there is one.
    pub before_reset: Option<u64>,
}

fn unsigned(value: i64) -> Result<u64, DbError> {
    u64::try_from(value).map_err(|_negative| DbError::DamagedDatabase)
}

/// Every namespace's budget and counters, by name.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn budgets(db: &DatabaseConnection) -> Result<Vec<NamespaceBytes>, DbError> {
    db.query(
        "SELECT namespace,bytes,cached,pinned FROM _coven_cache_budgets ORDER BY namespace",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        },
    )?
    .into_iter()
    .map(|(namespace, budget, cached, pinned)| {
        Ok(NamespaceBytes {
            namespace,
            budget: u64::from_be_bytes(budget.try_into().map_err(|_| DbError::DamagedDatabase)?),
            cached: unsigned(cached)?,
            pinned: unsigned(pinned)?,
        })
    })
    .collect()
}

/// The sum of the partial downloads' sizes.
pub(crate) fn partials(db: &DatabaseConnection) -> Result<u64, DbError> {
    unsigned(db.query_row("SELECT coalesce(sum(size),0) FROM _coven_cache_partials", [], |r| {
        r.get::<_, i64>(0)
    })?)
}

impl<M: Mode> Database<M> {
    /// The cache's counters and the partial downloads' sizes, from one
    /// snapshot, for the namespaces declared at open, in declaration order.
    pub async fn cache_bytes(&self) -> Result<CacheBytes, DbError> {
        let declared: Vec<String> = self
            .call(|open| {
                Ok::<_, DbError>(
                    M::access(open)
                        .write_schema
                        .namespaces
                        .iter()
                        .map(|namespace| namespace.name.clone())
                        .collect(),
                )
            })
            .await?;
        let (mut kept, pinning) = self.read(|sql| sql.cache_bytes()).await?;
        let namespaces = declared
            .into_iter()
            .filter_map(|name| {
                kept.iter().position(|bytes| bytes.namespace == name).map(|at| kept.swap_remove(at))
            })
            .collect();
        Ok(CacheBytes { namespaces, pinning })
    }
}

impl Database {
    /// The namespaces' budgets and counters, now and after each change to
    /// either. Reads nothing else, so a cached read's recency or a pin
    /// download's partial growing starts no run.
    pub fn subscribe_cache_budgets<E>(&self) -> LiveQuery<Vec<NamespaceBytes>, E>
    where
        E: From<DbError> + CarriesBug + Send + 'static,
    {
        self.subscribe(|sql| sql.cache_budgets().map_err(E::from))
    }
}

impl Database {
    /// One measurement of what the store keeps on this device: the cache's
    /// counters, budgets and partials from one snapshot, then the store's
    /// directory walked for its own files and the archives resets kept. The
    /// cache's folder is left out of the walk, since its entries count it.
    pub async fn local_bytes(&self) -> Result<LocalBytes, DbError> {
        let cache = self.cache_bytes().await?;
        let directory = self.call(|open| Ok::<_, DbError>(open.access.directory.clone())).await?;
        let walked = super::call::process(&self.bugs, move || {
            directory.local_bytes().map_err(DbError::from)
        })
        .await?;
        Ok(LocalBytes {
            namespaces: cache.namespaces,
            pinning: cache.pinning,
            library: walked.library,
            before_reset: walked.archives,
        })
    }

    /// The first measurement at once, then a new one after each write that
    /// changes a namespace's counters or budget, and after each
    /// `local_bytes_changed`. Partial downloads are measured with each but
    /// start none.
    pub fn subscribe_local_bytes(&self) -> LocalBytesWatch {
        let mut changed = self.writer.local_bytes_changes.subscribe();
        changed.mark_unchanged();
        LocalBytesWatch { database: self.clone(), budgets: self.subscribe_cache_budgets(), changed }
    }

    /// Say that the store's directory changed in a way its database doesn't
    /// report, such as the archives resets kept going.
    pub fn local_bytes_changed(&self) {
        self.writer.local_bytes_changes.send_modify(|count| *count += 1);
    }
}

/// The measurements of what the store keeps on this device, as they change.
pub struct LocalBytesWatch {
    database: Database,
    budgets: LiveQuery<Vec<NamespaceBytes>, DbError>,
    changed: tokio::sync::watch::Receiver<u64>,
}

impl LocalBytesWatch {
    /// The first measurement at once, then the latest one after each change.
    /// A reader slower than the changes gets one measurement of the state
    /// now, not one per change. `None` once the store has closed.
    pub async fn next(&mut self) -> Option<Result<LocalBytes, DbError>> {
        tokio::select! {
            run = self.budgets.next() => match run? {
                Ok(_) => {}
                Err(error) => return Some(Err(error)),
            },
            changed = self.changed.changed() => changed.ok()?,
        }
        Some(self.database.local_bytes().await)
    }

    /// Stop measuring.
    pub fn cancel(self) {}
}
