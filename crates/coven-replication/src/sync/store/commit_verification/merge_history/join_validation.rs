use super::registration::*;
use super::*;

/// The checks a commit that opens a device-join attempt has to pass.
///
/// There is nothing to compare the commit against any more. The attempt used to
/// be restated in a signed file naming the owner, the membership state and the
/// bootstrap cut, and every one of those was checked against this same commit —
/// signed by this same device. What decides the attempt is the commit: its
/// author is an active Owner at its predecessor, and its own order and
/// membership state are the history the joining device installs from.
pub(crate) fn validate_commit_join_attempts(
    _commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&MembershipChain>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join attempt activation has no exact predecessor membership authority"
                .to_string(),
        )
    })?;
    if !predecessor.is_owner_now(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join attempt activation author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    Ok(())
}
