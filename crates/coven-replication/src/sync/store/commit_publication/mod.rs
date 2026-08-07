use super::*;
use crate::sync::store::owner::authorized_history::AuthorizedStoreHistory;
use crate::sync::store::pull;
use std::sync::Arc;
pub(crate) mod operation;
mod signing;

pub(crate) use signing::LocalStoreWriter;
pub(crate) use signing::LocalWriterKeyrings;
use signing::StoreOperationSigningContext;

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
