//! The sizes of the store's own files in `coven_library_bytes` (§17.8):
//! each database file's size as its last checkpoint left it, which coven
//! writes as the store opens and again after each checkpoint that changes
//! it, the snapshot and floor bodies coven-sync captured to upload, and a
//! rebuild's store's own files while one runs (§18.3). Between a
//! change and the write after it, a row holds the size before it; opening
//! writes each database file's row again.

use crate::database::{finish_blocking, process, start_call, DatabaseInner, StoreDatabase};
use crate::sqlite::StoreSql;
use crate::store_failure::{StoreFailure, StoreHandle};
use crate::{next_result, DbError, LiveQuery};
use coven_foundation::files::{FileError, StoreDirAccess};
use coven_foundation::task::BugSinkRef;
use sqlive::CheckpointedSize;
use std::sync::{Arc, RwLock, Weak};

/// Write the store's database file's size, `database`, and its input
/// database's, `inputs`, in the transaction under way. The UPDATE skips a
/// row that already holds its size, so a live query that reads it does not
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

/// Write `bytes` into the `rebuild` row, in the transaction under way, when
/// that row exists.
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

/// Count the body coven-sync captured into the file `name`, of `size`
/// bytes, among the store's own files, in the transaction under way: the one
/// that journals the capture's bytes.
pub(crate) fn count_capture(db: &StoreSql, name: &str, size: u64) -> Result<(), DbError> {
    db.execute(
        "INSERT INTO _coven_capture_sizes(name,size) VALUES(?1,?2)",
        (name, crate::sqlite::integer("captured body size", size)?),
    )?;
    Ok(())
}

/// Sum the sizes of the store's own files in `coven_library_bytes`: the
/// value of `LocalBytes::library`.
pub(crate) fn library(db: &StoreSql) -> Result<u64, DbError> {
    crate::sqlite::unsigned(db.query_row(
        "SELECT coalesce(sum(bytes),0) FROM coven_library_bytes",
        [],
        |r| r.get::<_, i64>(0),
    )?)
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
}

/// Write both of `sizes` as they are now on `writer`, the opening store's,
/// failing the handle if the write fails.
pub(crate) fn record_opened(
    writer: &mut StoreSql,
    sizes: &mut DatabaseSizes,
) -> Result<(), DbError> {
    let (database, inputs) = sizes.now();
    crate::pending::state_transaction(writer, |db| record(db, database, inputs))
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
    /// Stop counting the captured body in the file `name` among the store's
    /// own files, before coven-sync deletes the file. A name with no row,
    /// such as a capture's that its journal hasn't recorded, changes nothing.
    pub async fn forget_capture(&self, name: String) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer, _| {
                writer.transaction(|db| {
                    db.execute("DELETE FROM _coven_capture_sizes WHERE name=?1", [&name])?;
                    Ok(())
                })
            })
        })
        .await
    }

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
    /// each write that changes them, until it closes.
    pub(crate) async fn count_rebuild(&self, rebuild: &StoreDatabase) -> Result<(), DbError> {
        let (total, first) = next_result(rebuild.follow(library)).await;
        let bytes = first.ok_or(DbError::StoreClosed)??;
        self.call(move |inner| {
            inner.access.with_writer(|writer, _| {
                crate::pending::state_transaction(writer, |db| record_rebuild(db, bytes))
            })
        })
        .await?;
        self.follow_rebuild(total, rebuild.observer.clone());
        Ok(())
    }

    /// Write each total `total` gives into the `rebuild` row until the
    /// rebuild's store closes, which ends the query. The follower holds this
    /// store only while it writes, so dropping its last handle still closes
    /// it; a write that fails has failed this store's handle, and the
    /// follower stops. A run of the query that fails would leave the row
    /// wrong, so the follower logs the failure, fails `rebuild`, the
    /// rebuild's handle, with it, and stops. The owner's next call on the
    /// rebuild returns the failure, and the owner discards the rebuild,
    /// which deletes the row.
    fn follow_rebuild(&self, mut total: LiveQuery<u64, DbError>, rebuild: StoreHandle) {
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
                    Some(Err(error)) => {
                        tracing::error!(%error, "reading the rebuild store's size failed");
                        rebuild.fail(StoreFailure::State(Arc::new(error)));
                        break;
                    }
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

    /// Delete the rebuild's folder and its row. One step moves the folder
    /// aside and deletes the row; then the method deletes the folder it
    /// moved aside, on the blocking pool. After a failure, either the folder
    /// and its row remain, or the folder moved aside remains and the next
    /// removal deletes it.
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

#[cfg(test)]
#[path = "library_bytes_tests.rs"]
pub(crate) mod tests;
