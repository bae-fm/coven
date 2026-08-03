use super::*;
use std::sync::Arc;
mod operation;

pub(crate) use operation::acknowledgements::StoreAckError;
pub(super) use operation::membership_mutation_journal::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
pub(super) use operation::prepare_partition_blob_locator;
pub(super) use operation::StoreWriterAuthorizationError;
pub(crate) use operation::{operations, reclaim, snapshot};

#[derive(Clone, Copy)]
pub(super) struct SnapshotHistoryConstruction;

pub(super) struct LocalStoreWriter<'store> {
    identity: &'store UserKeypair,
    registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
    device_signer: UserKeypair,
}

impl<'store> LocalStoreWriter<'store> {
    fn from_verified_parts(
        identity: &'store UserKeypair,
        registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> Self {
        Self {
            identity,
            registration,
            device_signer,
        }
    }

    fn registration_ref(&self) -> &crate::protocol::store_commit::StoreDeviceRegistrationRef {
        self.registration.reference()
    }

    fn registration(&self) -> &crate::protocol::store_commit::StoreDeviceRegistration {
        self.registration.value()
    }

    fn referenced_registration(
        &self,
    ) -> &crate::protocol::store_commit::ReferencedStoreDeviceRegistration {
        &self.registration
    }
}

pub(crate) struct AuthorizedWriterOperation<'storage> {
    database: StoreDatabase,
    history: AuthorizedStoreHistory<'storage>,
    storage: &'storage Arc<dyn SyncStorage>,
    membership: crate::protocol::membership::MembershipChain,
    writer: LocalStoreWriter<'storage>,
}

impl<'storage> AuthorizedWriterOperation<'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_parts(
        database: StoreDatabase,
        history: AuthorizedStoreHistory<'storage>,
        storage: &'storage Arc<dyn SyncStorage>,
        membership: crate::protocol::membership::MembershipChain,
        identity: &'storage UserKeypair,
        registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> Self {
        Self {
            database,
            history,
            storage,
            membership,
            writer: LocalStoreWriter::from_verified_parts(identity, registration, device_signer),
        }
    }
}
