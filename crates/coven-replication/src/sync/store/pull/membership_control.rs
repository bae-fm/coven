use super::*;

pub(crate) fn membership_authorizes(
    membership: &MembershipChain,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> bool {
    match &commit.body {
        store_commit::StoreCommitBody::Operations(_) => commit
            .membership_authority
            .as_ref()
            .is_some_and(|authority| {
                membership.authorizes_write_authority(authority, &author.author_pubkey)
            }),
        store_commit::StoreCommitBody::OwnerPromotionRequest { request } => {
            membership
                .active_owner_grant(&author.author_pubkey)
                .as_ref()
                == Some(&request.promoter_owner_grant)
                && membership
                    .active_grant(&request.promoter_owner_grant)
                    .is_some_and(|grant| {
                        commit.membership_authority.as_ref() == Some(&grant.creation_authority)
                    })
        }
        store_commit::StoreCommitBody::ReclaimAuthorization { .. }
        | store_commit::StoreCommitBody::ReclaimCompletion { .. }
        | store_commit::StoreCommitBody::AbandonCandidates { .. } => true,
    }
}
