use super::*;

pub(crate) fn membership_authorizes(
    membership: Option<&MembershipChain>,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> bool {
    if commit.operations().is_none() {
        return true;
    }
    let Some(chain) = membership else {
        return false;
    };
    commit
        .membership_authority
        .as_ref()
        .is_some_and(|authority| chain.authorizes_write_authority(authority, &author.author_pubkey))
}
