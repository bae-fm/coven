use std::sync::Arc;

use coven_database::{CloudOutboxSnapshot, CommittedChanges, StoreDatabase};

const OUTBOX_TABLES: &[&str] = &["cloud_outbox", "blob_make_remote_intents"];

/// A committed view of coven's durable cloud work.
///
/// The initial snapshot is returned immediately. Later calls wait for a
/// transaction that changes the upload queue or a make-remote intent, then read
/// both tables in one database operation. Transfer byte callbacks do not write
/// here; hosts combine their in-memory progress with this durable lower bound.
///
/// [`next`](Self::next) is cancel-safe: a snapshot stays owed until a read of
/// it completes, so dropping a `next()` future mid-read (a `select!` branch
/// losing) leaves the next call to deliver it.
pub struct CloudOutboxLiveQuery {
    database: StoreDatabase,
    changes: tokio::sync::broadcast::Receiver<Arc<CommittedChanges>>,
    /// The initial snapshot, or a relevant change taken from `changes`, has
    /// not been delivered yet.
    snapshot_owed: bool,
}

impl CloudOutboxLiveQuery {
    pub(crate) fn new(database: StoreDatabase) -> Self {
        let changes = database.subscribe_committed_changes();
        Self {
            database,
            changes,
            snapshot_owed: true,
        }
    }

    /// Return the initial snapshot, or wait for the next relevant committed
    /// change and return the resulting snapshot. A read error is delivered in
    /// place of that snapshot and does not end the subscription.
    pub async fn next(&mut self) -> Result<CloudOutboxSnapshot, crate::DbError> {
        while !self.snapshot_owed {
            match self.changes.recv().await {
                Ok(changes) if changes.affects_any_table(OUTBOX_TABLES) => {
                    self.snapshot_owed = true;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    self.snapshot_owed = true;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("the cloud outbox live query retains its database")
                }
            }
        }
        let snapshot = self.database.cloud_outbox_snapshot().await;
        self.snapshot_owed = false;
        snapshot
    }
}
