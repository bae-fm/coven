//! Store membership operations authorized by a retained local writer.

mod invitation;
mod journal;
mod publication;
mod removal;
mod resolution;

use crate::sync::store::membership::InviteError;

pub(super) use invitation::create_invitation_with_encryption_durable;
pub(crate) use publication::AuthorizedMembershipPublication;
pub(crate) use publication::{validate_prepared_publication, validate_prepared_transition};
pub(super) use removal::revoke_member_durable;
pub(super) use resolution::resolve_membership_conflict;

use journal::{
    decode_membership_mutation, encode_membership_mutation, encode_membership_progress,
    exact_owned_remote, InviteMutationPlan, MembershipMutationPlan, MembershipMutationProgress,
    MutationPersistence, ReplacementWrappedKey, ResolveMutationPlan, RevokeMembershipPublication,
    RevokeMutationPlan,
};
pub(crate) use journal::{PreparedMembershipPublication, PreparedMembershipTransition};
use publication::chain_with_exact_entry;

pub(super) fn validate_revoke_rotation_adoption(
    row: crate::database::DurableMembershipMutation,
    adopted_generation: u64,
) -> Result<crate::protocol::store_commit::ObjectHash, InviteError> {
    let intent_hash = row.intent_hash;
    let (plan, progress) = decode_membership_mutation(row)?;
    let MembershipMutationPlan::Revoke(plan) = plan else {
        return Err(InviteError::InvalidDurableMutation(
            "key adoption found another membership mutation".to_string(),
        ));
    };
    if !matches!(progress, MembershipMutationProgress::RevokeActivated { .. }) {
        return Err(InviteError::InvalidDurableMutation(
            "key adoption found a removal that is not activated".to_string(),
        ));
    }
    let planned_generation =
        crate::encryption::EncryptionService::from_keyring_payload(plan.keyring_payload)
            .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
            .current_generation();
    if planned_generation != adopted_generation {
        return Err(InviteError::InvalidDurableMutation(format!(
            "adopted key generation {adopted_generation} differs from the activated removal generation {planned_generation}"
        )));
    }
    Ok(intent_hash)
}

#[cfg(test)]
mod tests;
