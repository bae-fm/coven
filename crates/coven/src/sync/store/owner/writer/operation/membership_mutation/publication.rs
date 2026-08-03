use crate::protocol::membership;
use crate::protocol::store_commit::{
    membership_entry_semantic_prefix, membership_head_slot_prefix,
};

use super::{InviteError, PreparedMembershipPublication, PreparedMembershipTransition};

pub(crate) fn validate_prepared_publication(
    publication: &PreparedMembershipPublication,
) -> Result<(), InviteError> {
    validate_prepared_transition(&PreparedMembershipTransition {
        entry: publication.entry.clone(),
        entry_ref: publication.entry_ref.clone(),
        entry_object: publication.entry_object.clone(),
        transition: membership::MergeMembershipHeadTransition {
            body: publication.head.body.clone(),
            head_slot: publication.head_ref.object.slot().clone(),
        },
    })?;
    let coord = publication.entry.coord();
    if publication.entry_ref.coord != coord
        || publication.entry_ref.object != *publication.entry_object.reference()
        || publication.head.body.entry != publication.entry_ref
        || publication.head.entry_coord() != coord
        || publication.head_ref.coord != coord
        || publication.head_ref.head_hash != publication.head.head_hash()
        || publication.head_ref.object != *publication.head_object.reference()
        || publication.head_object.stored_bytes()
            != serde_json::to_vec(&publication.head)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership publication does not bind one exact entry and head".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_prepared_transition(
    transition: &PreparedMembershipTransition,
) -> Result<(), InviteError> {
    let coord = transition.entry.coord();
    let entry_bytes = serde_json::to_vec(&transition.entry)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
        InviteError::InvalidDurableMutation("membership sequence is exhausted".to_string())
    })?;
    let entry_key = format!(
        "{}.json",
        membership_entry_semantic_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.entry_hash,
        )
    );
    let head_key = format!(
        "{}.json",
        membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        )
    );
    let successor_key = format!(
        "{}.json",
        membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            next_sequence,
        )
    );
    if transition.entry_ref.coord != transition.entry.coord()
        || transition.entry_ref.object != *transition.entry_object.reference()
        || transition.entry_object.stored_bytes() != entry_bytes
        || transition.entry_ref.object.slot().logical_key() != entry_key
        || transition.transition.body.entry != transition.entry_ref
        || transition.transition.body.resolutions != transition.entry.resolution_dependencies
        || transition.transition.head_slot.logical_key() != head_key
        || transition.transition.body.successor.next_slot.logical_key() != successor_key
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership transition does not bind its exact entry".to_string(),
        ));
    }
    Ok(())
}
