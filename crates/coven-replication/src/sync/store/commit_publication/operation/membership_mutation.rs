//! Store membership operations authorized by a retained local writer.

mod removal;

use crate::sync::store::membership::MembershipMutationError;

pub(super) use removal::AuthorizedMembershipRevocation;

use super::{
    decode_membership_mutation, exact_owned_remote, MembershipMutationPlan,
    MembershipMutationProgress, ReplacementWrappedKey, RevokeMembershipPublication,
    RevokeMutationPlan,
};

pub(super) fn validate_revoke_rotation_adoption(
    row: coven_database::DurableMembershipMutation,
    adopted_generation: u64,
) -> Result<coven_protocol::store_commit::ObjectHash, MembershipMutationError> {
    let intent_hash = row.intent_hash;
    let (plan, progress) = decode_membership_mutation(row)?;
    let MembershipMutationPlan::Revoke(plan) = plan else {
        return Err(MembershipMutationError::InvalidDurableMutation(
            "key adoption found another membership mutation".to_string(),
        ));
    };
    if !matches!(progress, MembershipMutationProgress::RevokeActivated { .. }) {
        return Err(MembershipMutationError::InvalidDurableMutation(
            "key adoption found a removal that is not activated".to_string(),
        ));
    }
    let planned_generation =
        coven_keys::encryption::EncryptionService::from_keyring_payload(plan.keyring_payload)
            .map_err(MembershipMutationError::Encryption)?
            .current_generation();
    if planned_generation != adopted_generation {
        return Err(MembershipMutationError::InvalidDurableMutation(format!(
            "adopted key generation {adopted_generation} differs from the activated removal generation {planned_generation}"
        )));
    }
    Ok(intent_hash)
}

#[cfg(test)]
mod tests;
