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
pub(super) use removal::{complete_revoke_rotation_adoption, revoke_member_durable};
pub(super) use resolution::resolve_membership_conflict;

use journal::{
    decode_membership_mutation, encode_membership_mutation, encode_membership_progress,
    exact_owned_remote, select_mutation_author_stream, InviteMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence, ReplacementWrappedKey, ResolveMutationPlan,
    RevokeMembershipPublication, RevokeMutationPlan,
};
pub(crate) use journal::{PreparedMembershipPublication, PreparedMembershipTransition};
use publication::chain_with_exact_entry;

#[cfg(test)]
mod tests;
