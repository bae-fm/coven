//! The sizes of the store's own files in `coven_library_bytes` (§17.8):
//! each database file's size as its last checkpoint left it, written as the
//! store opens and again after each checkpoint that changes it, and a
//! rebuild's store's own files while one runs (§18.3). Between a change and
//! the write after it, a row holds the size before it; opening writes each
//! database file's row again.

use crate::database::{finish_blocking, process, start_call, DatabaseInner, StoreDatabase};
use crate::sqlite::StoreSql;
use crate::{next_result, DbError, LiveQuery, SqlReadContext};
use coven_foundation::files::{FileError, StoreDirAccess};
use coven_foundation::task::BugSinkRef;
use sqlive::CheckpointedSize;
use std::sync::{Arc, RwLock, Weak};

/// Write the store's database file's size, `database`, and its input
/// database's, `inputs`, in the transaction under way. A row already
/// holding its size is left alone, so a live query that reads it doesn't
/// run again.
fn record(db: &StoreSql, database: u64, inputs: u64) -> Result<(), DbError> {
    for (part, bytes) in [("database", database), ("inputs", inputs)] {
        db.execute(
            "UPDATE coven_library_bytes SET bytes=?2 WHERE part=?1 AND bytes<>?2",
            (part, crate::sqlite::integer("database file size", bytes)?),
        )?;
    }
    Ok(())
}

/// The `rebuild` row's size, in the transaction under way, while there is
/// one.
fn record_rebuild(db: &StoreSql, bytes: u64) -> Result<(), DbError> {
    db.execute(
        "UPDATE coven_library_bytes SET bytes=?1 WHERE part='rebuild' AND bytes<>?1",
        [crate::sqlite::integer("rebuild store size", bytes)?],
    )?;
    Ok(())
}

/// Delete the `rebuild` row, in the transaction under way.
fn forget_rebuild(db: &StoreSql) -> Result<(), DbError> {
    db.execute("DELETE FROM coven_library_bytes WHERE part='rebuild'", [])?;
    Ok(())
}

/// The sizes of a store's own files together, as its `coven_library_bytes`
/// holds them.
fn own_files(sql: SqlReadContext<'_>) -> Result<u64, DbError> {
    let bytes: i64 =
        sql.query_row("SELECT coalesce(sum(bytes),0) FROM coven_library_bytes", [], |r| r.get(0))?;
    u64::try_from(bytes).map_err(|_negative| DbError::DamagedDatabase)
}

/// Move a folder whose bytes a row counts out of the store's directory with
/// `aside`, then delete its row with `forget`, on `writer`, the store's
/// writer, which the caller holds: one step, so no row names a folder that
/// is partly removed. When `forget` fails, `back` moves the folder back and
/// the failure returns; when that fails too, both failures return. A folder
/// `aside` didn't find has only its row deleted.
pub(crate) fn uncount(
    writer: &mut StoreSql,
    aside: impl FnOnce() -> Result<bool, FileError>,
    back: impl FnOnce() -> Result<(), FileError>,
    forget: impl FnOnce(&StoreSql) -> Result<(), DbError>,
) -> Result<(), DbError> {
    let moved = aside()?;
    match writer.transaction(forget) {
        Ok(()) => Ok(()),
        Err(error) if !moved => Err(error),
        Err(error) => match back() {
            Ok(()) => Err(error),
            Err(returning) => Err(DbError::FileCleanup {
                write: Err(Box::new(error)),
                failures: vec![DbError::Disk(returning)],
            }),
        },
    }
}

/// Run `write` under the writer of the store `store` points to, failing its
/// handle if the write fails. Returns whether the store took the write:
/// false once it has closed or failed.
async fn record_on(
    store: &Weak<RwLock<Option<DatabaseInner>>>,
    bugs: &BugSinkRef,
    operation: &'static str,
    write: impl FnOnce(&StoreSql) -> Result<(), DbError> + Send + 'static,
) -> bool {
    let Some(open) = store.upgrade() else { return false };
    let written = start_call(operation, bugs, open, move |open: &DatabaseInner| {
        open.access.call(|| {
            open.access.with_writer(|writer, _| crate::pending::state_transaction(writer, write))
        })
    })
    .await;
    finish_blocking::<(), DbError>(written).is_ok()
}

/// How a store's two database files report their sizes after each
/// checkpoint: the store's own and its input database's.
pub(crate) struct DatabaseSizes {
    database: CheckpointedSize,
    inputs: CheckpointedSize,
}

impl DatabaseSizes {
    pub(crate) fn new(database: CheckpointedSize, inputs: CheckpointedSize) -> Self {
        Self { database, inputs }
    }

    /// Both sizes now. A later `changed` waits for a size after these.
    fn now(&mut self) -> (u64, u64) {
        (self.database.bytes(), self.inputs.bytes())
    }

    /// Both sizes once a checkpoint changes either, or `None` once the
    /// store has closed.
    async fn changed(&mut self) -> Option<(u64, u64)> {
        tokio::select! {
            size = self.database.changed() => size?,
            size = self.inputs.changed() => size?,
        };
        Some(self.now())
    }

    /// Write both sizes as they are now on `writer`, the opening store's,
    /// failing the handle if the write fails.
    pub(crate) fn record_opened(&mut self, writer: &mut StoreSql) -> Result<(), DbError> {
        let (database, inputs) = self.now();
        crate::pending::state_transaction(writer, |db| record(db, database, inputs))
    }
}

impl StoreDatabase {
    /// Write both of the store's database files' sizes after each checkpoint
    /// that changes one, until the store closes, which ends both views. The
    /// follower holds the store only while it writes, so dropping the store's
    /// last handle still closes it. A write that fails has failed the handle
    /// with its cause, and the follower stops.
    pub(crate) fn follow_database_sizes(&self, mut sizes: DatabaseSizes) {
        let store = Arc::downgrade(&self.inner);
        let bugs = self.bugs.clone();
        let operation = "record the database files' sizes";
        let follow = async move {
            while let Some((database, inputs)) = sizes.changed().await {
                let write = move |db: &StoreSql| record(db, database, inputs);
                if !record_on(&store, &bugs, operation, write).await {
                    break;
                }
            }
        };
        drop(coven_foundation::task::spawn_owned(
            "the database sizes follower",
            self.bugs.clone(),
            follow,
            |_| async {},
        ));
    }
}

impl StoreDatabase {
    /// Make this store's `rebuild` row, at no bytes, as a rebuild is about
    /// to make its folder (§18.3).
    pub(crate) async fn begin_counting_rebuild(&self) -> Result<(), DbError> {
        self.call(|inner| {
            inner.access.with_writer(|writer, _| {
                crate::pending::state_transaction(writer, |db| {
                    db.execute(
                        "INSERT INTO coven_library_bytes(part,bytes) VALUES('rebuild',0)
                         ON CONFLICT(part) DO UPDATE SET bytes=0",
                        [],
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Count `rebuild`, the rebuild's store, just opened, in this store's
    /// `rebuild` row: the sizes of its own files together now, then after
    /// each write that changes them, until it closes. A run of the rebuild's
    /// query that fails leaves the row as it was until the next run.
    pub(crate) async fn count_rebuild(&self, rebuild: &StoreDatabase) -> Result<(), DbError> {
        let (total, first) = next_result(rebuild.subscribe(own_files)).await;
        let bytes = first.ok_or(DbError::StoreClosed)??;
        self.call(move |inner| {
            inner.access.with_writer(|writer, _| {
                crate::pending::state_transaction(writer, |db| record_rebuild(db, bytes))
            })
        })
        .await?;
        self.follow_rebuild(total);
        Ok(())
    }

    /// Write each total `total` gives into the `rebuild` row until the
    /// rebuild's store closes, which ends the query. The follower holds this
    /// store only while it writes, so dropping its last handle still closes
    /// it; a write that fails has failed the handle, and the follower stops.
    fn follow_rebuild(&self, mut total: LiveQuery<u64, DbError>) {
        let store = Arc::downgrade(&self.inner);
        let bugs = self.bugs.clone();
        let operation = "record the rebuild store's size";
        let follow = async move {
            loop {
                let (query, run) = next_result(total).await;
                total = query;
                let bytes = match run {
                    None => break,
                    Some(Ok(bytes)) => bytes,
                    Some(Err(_)) => continue,
                };
                let write = move |db: &StoreSql| record_rebuild(db, bytes);
                if !record_on(&store, &bugs, operation, write).await {
                    break;
                }
            }
        };
        drop(coven_foundation::task::spawn_owned(
            "the rebuild size follower",
            self.bugs.clone(),
            follow,
            |_| async {},
        ));
    }

    /// Remove the rebuild's folder and its row: the folder moves aside and
    /// the row goes in one step, then its files go, on the blocking pool.
    /// What a failure leaves is either still the folder with its row, or in
    /// the folder moved aside, which the next removal removes.
    pub(crate) async fn remove_rebuild(&self) -> Result<(), DbError> {
        let directory = self
            .call(|inner| {
                let directory = &inner.access.directory;
                inner.access.with_writer(|writer, _| {
                    uncount(
                        writer,
                        || directory.retire_rebuild_directory(),
                        || directory.return_rebuild_directory(),
                        forget_rebuild,
                    )
                })?;
                Ok::<_, DbError>(directory.clone())
            })
            .await?;
        process(&self.bugs, move || Ok(directory.remove_retired_rebuild()?)).await
    }
}
