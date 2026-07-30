//! Store-owned Owner promotion workflow.

mod acceptance;
mod authority;
mod error;
mod finalization;
mod journal;
mod request;

pub(crate) use error::OwnerPromotionError;
pub(crate) use journal::{OwnerPromotionJournal, OwnerPromotionJournalTransition};

pub(super) struct AuthorizedOwnerPromotion<'operation, 'storage> {
    writer: &'operation mut super::AuthorizedWriterOperation<'storage>,
    database: crate::database::StoreDatabase,
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    root: crate::protocol::store_commit::StoreRootRef,
    membership: crate::protocol::membership::MembershipChain,
    identity: crate::keys::UserKeypair,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
}

impl<'operation, 'storage> AuthorizedOwnerPromotion<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        writer: &'operation mut super::AuthorizedWriterOperation<'storage>,
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
        root: crate::protocol::store_commit::StoreRootRef,
        membership: crate::protocol::membership::MembershipChain,
        identity: crate::keys::UserKeypair,
        registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            membership,
            identity,
            registration_ref,
            registration,
        }
    }
}

#[cfg(test)]
mod tests;
