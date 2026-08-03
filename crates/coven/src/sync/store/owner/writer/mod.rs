use super::*;
use std::sync::Arc;

mod operation;

pub(crate) use operation::acknowledgements::StoreAckError;
pub(super) use operation::membership_mutation::{
    validate_prepared_publication, validate_prepared_transition,
};
pub(super) use operation::membership_mutation_journal::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
pub(crate) use operation::AuthorizedWriterOperation;
pub(super) use operation::StoreWriterAuthorizationError;
pub(crate) use operation::{blob_preparation, operations, reclaim, snapshot};

#[derive(Clone, Copy)]
pub(super) struct SnapshotHistoryConstruction;

#[allow(clippy::too_many_arguments)]
pub(super) fn authorize<'storage>(
    database: StoreDatabase,
    history: super::history::AuthorizedStoreHistory<'storage>,
    storage: &'storage Arc<dyn SyncStorage>,
    membership: crate::protocol::membership::MembershipChain,
    identity: &'storage UserKeypair,
    registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
    device_signer: UserKeypair,
) -> AuthorizedWriterOperation<'storage> {
    operation::AuthorizedWriterOperation::from_parts(
        database,
        history,
        storage,
        membership,
        identity,
        registration,
        device_signer,
    )
}
