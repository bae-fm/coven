//! The sizes of the store's database files in `coven_library_bytes`
//! (§17.8): each file's size as its last checkpoint left it, written as the
//! store opens and again after each checkpoint that changes it. Between a
//! checkpoint and the write after it, the row holds the size before it;
//! opening writes each row again.

use crate::database::{finish_blocking, start_call, DatabaseInner, StoreDatabase};
use crate::sqlite::StoreSql;
use crate::DbError;
use sqlive::CheckpointedSize;
use std::sync::Arc;

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
                let Some(open) = store.upgrade() else { break };
                let written = start_call(operation, &bugs, open, move |open: &DatabaseInner| {
                    open.access.call(|| {
                        open.access.with_writer(|writer, _| {
                            crate::pending::state_transaction(writer, |db| {
                                record(db, database, inputs)
                            })
                        })
                    })
                })
                .await;
                if finish_blocking::<(), DbError>(written).is_err() {
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
