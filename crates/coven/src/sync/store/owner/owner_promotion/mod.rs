//! Store-owned Owner promotion workflow.

mod acceptance;
mod authority;
mod error;
mod finalization;
mod journal;
mod request;

pub(crate) use error::OwnerPromotionError;
pub(crate) use journal::{OwnerPromotionJournal, OwnerPromotionJournalTransition};

pub(crate) struct AuthorizedOwnerPromotion<'operation, 'storage> {
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

    fn exact_member_grant(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::protocol::membership::MembershipGrantId, OwnerPromotionError> {
        let grants = self.membership.active_grant_ids(member_pubkey);
        let Some(grant) = grants.iter().next() else {
            return Err(OwnerPromotionError::Protocol(
                "promotion target has no active Member grant".to_string(),
            ));
        };
        if grants.len() != 1
            || self.membership.active_grant(grant).is_none_or(|record| {
                record.role != crate::protocol::membership::StoreMembershipRoleGrant::Member
            })
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion target does not have exactly one active Member grant".to_string(),
            ));
        }
        Ok(grant.clone())
    }
}

#[cfg(test)]
mod tests;
