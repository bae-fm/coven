use super::*;

mod abandonment;
mod acknowledgements;
mod blob_lifecycle;
pub(crate) mod blob_preparation;
mod membership_mutation;
mod operation;
pub(crate) mod operations;
mod preparation;
mod publication;
pub(crate) mod reclaim;
pub(crate) mod snapshot;

pub(crate) use acknowledgements::StoreAckError;
pub(super) use membership_mutation::{
    validate_prepared_publication, validate_prepared_transition, PreparedMembershipPublication,
    PreparedMembershipTransition,
};
pub(crate) use operation::AuthorizedWriterOperation;
pub(super) use operation::StoreWriterAuthorizationError;

#[derive(Clone, Copy)]
pub(super) struct SnapshotHistoryConstruction;

impl SnapshotHistoryConstruction {
    fn authorize_history(self) -> super::history::HistoryConstructionAuthority {
        super::history::HistoryConstructionAuthority::snapshot(self)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn authorize<'storage>(
    database: StoreDatabase,
    history: super::history::AuthorizedStoreHistory<'storage>,
    storage: &'storage Arc<dyn SyncStorage>,
    membership: crate::protocol::membership::MembershipChain,
    identity: &'storage UserKeypair,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
    device_signer: UserKeypair,
) -> AuthorizedWriterOperation<'storage> {
    operation::AuthorizedWriterOperation::from_parts(
        database,
        history,
        storage,
        membership,
        identity,
        registration_ref,
        registration,
        device_signer,
    )
}
