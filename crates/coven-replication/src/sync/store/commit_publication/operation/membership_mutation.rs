//! Store membership operations authorized by a retained local writer.

mod admission;
mod removal;

use crate::sync::store::membership::MembershipMutationError;

pub(super) use removal::AuthorizedMembershipRevocation;

pub(super) enum MembershipRevocation {
    Activated(coven_keys::encryption::EncryptionService),
    AlreadyRemoved(coven_keys::encryption::EncryptionService),
}

use super::{
    decode_membership_mutation, MembershipMutationPlan, MembershipMutationProgress,
    RevokeMutationPlan,
};

pub(super) fn publication_predecessor_changed(
    error: &MembershipMutationError,
    expected: &coven_protocol::membership::MembershipCoord,
) -> bool {
    if let MembershipMutationError::Membership(
        coven_protocol::membership::MembershipError::PublicationPredecessorChanged { coord },
    ) = error
    {
        return coord.as_ref() == expected;
    }
    let MembershipMutationError::Store(error) = error else {
        return false;
    };
    let crate::sync::store::StoreError::Pull(error) = error.as_ref() else {
        return false;
    };
    let crate::sync::store::pull::StorePullError::MembershipChain(
        crate::sync::store::membership::AnchoredChainError::Membership(
            coven_protocol::membership::MembershipError::PublicationPredecessorChanged { coord },
        ),
    ) = error
    else {
        return false;
    };
    coord.as_ref() == expected
}

/// The removal whose rotation `adopted_generation` adopts, taken from the
/// journal row and the gate together: the row names the plan, and the gate's
/// committed local rotation is the removal's activation.
pub(super) fn validate_revoke_rotation_adoption(
    row: coven_database::DurableMembershipMutation,
    gate: Option<coven_protocol::objects::RotationGate>,
    adopted_generation: u64,
) -> Result<coven_protocol::store_commit::ObjectHash, MembershipMutationError> {
    let intent_hash = row.intent_hash;
    let (plan, _) = decode_membership_mutation(row)?;
    let MembershipMutationPlan::Revoke(plan) = plan else {
        return Err(MembershipMutationError::InvalidDurableMutation(
            "key adoption found another membership mutation".to_string(),
        ));
    };
    if !matches!(
        gate.and_then(|gate| gate.local()),
        Some(coven_protocol::objects::LocalRotation::Committed {
            generation,
            mutation,
        }) if generation.get() == adopted_generation && mutation == intent_hash
    ) {
        return Err(MembershipMutationError::InvalidDurableMutation(
            "key adoption found a removal whose rotation is not committed".to_string(),
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

#[cfg(test)]
mod authority_upload_tests;
