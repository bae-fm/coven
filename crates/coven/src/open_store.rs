//! What holds a store's directory open while its handle is open.

use std::sync::Arc;

use coven_database::store::StoreReads;
use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreOpenGuard;

/// The writer connection, the application read connections, and the
/// directory lock of one open store: every file the handle keeps open in the
/// store directory between calls.
#[derive(Clone)]
pub(crate) struct OpenStore {
    database: StoreDatabase,
    reads: StoreReads,
    open_guard: Arc<StoreOpenGuard>,
}

impl OpenStore {
    pub(crate) fn new(
        database: StoreDatabase,
        reads: StoreReads,
        open_guard: Arc<StoreOpenGuard>,
    ) -> Self {
        Self {
            database,
            reads,
            open_guard,
        }
    }

    /// Close both connections, waiting until each is closed, then release
    /// the lock: the lock is free only once nothing else of the store is open.
    pub(crate) async fn close(&self) {
        self.database.close().await;
        self.reads.close().await;
        self.open_guard.release();
    }
}
