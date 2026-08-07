use super::*;
use std::sync::Arc;
mod local_store_writer;
pub(crate) mod operation;

pub(crate) use local_store_writer::LocalStoreWriter;
pub(crate) use local_store_writer::LocalWriterKeyrings;
use local_store_writer::StoreOperationSigningContext;

pub(crate) use operation::operations;
pub(crate) use operation::prepare_partition_blob_locator;
pub use operation::StoreWriterAuthorizationError;

#[derive(Clone, Copy)]
pub(crate) struct SnapshotHistoryConstruction;

pub struct AuthorizedWriterOperation<'storage> {
    database: StoreDatabase,
    history: AuthorizedStoreHistory<'storage>,
    storage: &'storage Arc<dyn SyncStorage>,
    store_dir: &'storage StoreDir,
    membership: coven_protocol::membership::MembershipChain,
    writer: Arc<LocalStoreWriter>,
    keyrings: LocalWriterKeyrings<'storage>,
}

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(super) fn from_parts(
        database: StoreDatabase,
        history: AuthorizedStoreHistory<'storage>,
        storage: &'storage Arc<dyn SyncStorage>,
        store_dir: &'storage StoreDir,
        membership: coven_protocol::membership::MembershipChain,
        writer: Arc<LocalStoreWriter>,
        keyrings: LocalWriterKeyrings<'storage>,
    ) -> Self {
        Self {
            database,
            history,
            storage,
            store_dir,
            membership,
            writer,
            keyrings,
        }
    }
}
