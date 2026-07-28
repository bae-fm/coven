//! Durable Store membership mutations.

mod error;
mod invitation;
mod journal;
mod keyring;
mod publication;
mod removal;
mod resolution;

pub use error::InviteError;
pub use keyring::unwrap_store_keyring;

pub(crate) use invitation::create_invitation_with_encryption_durable;
#[cfg(test)]
pub(crate) use keyring::signed_wrapped_keyring_for_test;
pub(crate) use keyring::{
    ed25519_hex_to_x25519, load_authorized_owner_keyring, signed_wrapped_key,
    unwrap_store_keyring_for_refs,
};
pub(crate) use publication::{
    finish_membership_transition, prepare_membership_transition,
    publish_prepared_merge_membership_activation_with_history,
    publish_prepared_merge_membership_authority, validate_prepared_publication,
    validate_prepared_transition,
};
#[cfg(test)]
pub(crate) use removal::revoke_member_durable;
pub(crate) use removal::{complete_revoke_rotation_adoption, revoke_member_durable_with_history};
pub(crate) use resolution::resolve_membership_conflict_with_history;

use journal::{
    decode_membership_mutation, encode_membership_mutation, encode_membership_progress,
    exact_owned_remote, select_mutation_author_stream, InviteMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence, ReplacementWrappedKey, ResolveMutationPlan,
    RevokeMembershipPublication, RevokeMutationPlan,
};
pub(crate) use journal::{PreparedMembershipPublication, PreparedMembershipTransition};
use publication::{chain_with_exact_entry, prepare_membership_publication};

#[cfg(test)]
mod tests;
