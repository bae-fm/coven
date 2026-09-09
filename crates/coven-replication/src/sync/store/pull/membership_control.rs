use super::*;

pub(crate) fn membership_authorizes(
    membership: Option<&MembershipChain>,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> bool {
    match &commit.body {
        store_commit::StoreCommitBody::Operations(_) => membership.is_some_and(|chain| {
            commit
                .membership_authority
                .as_ref()
                .is_some_and(|authority| {
                    chain.authorizes_write_authority(authority, &author.author_pubkey)
                })
        }),
        store_commit::StoreCommitBody::OwnerPromotionRequest { request } => {
            membership.is_some_and(|chain| {
                chain.active_owner_grant(&author.author_pubkey).as_ref()
                    == Some(&request.promoter_owner_grant)
                    && chain
                        .active_grant(&request.promoter_owner_grant)
                        .is_some_and(|grant| {
                            commit.membership_authority.as_ref() == Some(&grant.creation_authority)
                        })
            })
        }
        store_commit::StoreCommitBody::ReclaimAuthorization { .. }
        | store_commit::StoreCommitBody::ReclaimReceipt { .. }
        | store_commit::StoreCommitBody::AbandonCandidates { .. } => true,
    }
}
