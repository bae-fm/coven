use std::sync::Arc;

use coven_database::{CloudOutboxSnapshot, CommittedChanges, StoreDatabase};

const OUTBOX_TABLES: &[&str] = &["cloud_outbox", "blob_make_remote_intents"];

/// A committed view of coven's durable cloud work.
///
/// The initial snapshot is returned immediately. Later calls wait for a
/// transaction that changes the upload queue or a make-remote intent, then read
/// both tables in one database operation. Transfer byte callbacks do not write
/// here; hosts combine their in-memory progress with this durable lower bound.
pub struct CloudOutboxLiveQuery {
    database: StoreDatabase,
    changes: tokio::sync::broadcast::Receiver<Arc<CommittedChanges>>,
    initial: bool,
}

impl CloudOutboxLiveQuery {
    pub(crate) fn new(database: StoreDatabase) -> Self {
        let changes = database.subscribe_committed_changes();
        Self {
            database,
            changes,
            initial: true,
        }
    }

    /// Return the initial snapshot, or wait for the next relevant committed
    /// change and return the resulting snapshot.
    pub async fn next(&mut self) -> Result<CloudOutboxSnapshot, crate::DbError> {
        if self.initial {
            self.initial = false;
        } else {
            loop {
                match self.changes.recv().await {
                    Ok(changes) if changes.affects_any_table(OUTBOX_TABLES) => break,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("the cloud outbox live query retains its database")
                    }
                }
            }
        }
        self.database.cloud_outbox_snapshot().await
    }
}
