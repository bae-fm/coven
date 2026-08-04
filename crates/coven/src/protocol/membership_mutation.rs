//! Prepared membership mutations: the exact entry, head, and objects one
//! membership publication or transition binds, validated as a unit before
//! anything durable records them.

use crate::protocol::membership::{
    self, AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    MergeMembershipHeadTransition,
};
use crate::protocol::objects::{ExactObjectRef, PreparedExactObject};
use crate::protocol::store_commit::ObjectHash;

/// A prepared membership mutation whose parts do not bind one exact entry and
/// head. Workflow errors wrap it at the operation boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid prepared membership mutation: {0}")]
pub(crate) struct MembershipPreparationError(pub(crate) String);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMembershipPublication {
    pub(crate) entry: MembershipEntry,
    pub(crate) entry_ref: MembershipEntryRef,
    pub(crate) entry_object: PreparedExactObject,
    pub(crate) head: AuthorHead,
    pub(crate) head_ref: MembershipHeadRef,
    pub(crate) head_object: PreparedExactObject,
}

impl PreparedMembershipPublication {
    pub(crate) fn validate(&self) -> Result<(), MembershipPreparationError> {
        PreparedMembershipTransition {
            entry: self.entry.clone(),
            entry_ref: self.entry_ref.clone(),
            entry_object: self.entry_object.clone(),
            transition: membership::MergeMembershipHeadTransition {
                body: self.head.body.clone(),
                head_slot: self.head_ref.object.slot().clone(),
            },
        }
        .validate()?;
        let coord = self.entry.coord();
        if self.entry_ref.coord != coord
            || self.entry_ref.object != *self.entry_object.reference()
            || self.head.body.entry != self.entry_ref
            || self.head.entry_coord() != coord
            || self.head_ref.coord != coord
            || self.head_ref.head_hash != self.head.head_hash()
            || self.head_ref.object != *self.head_object.reference()
            || self.head_object.stored_bytes()
                != serde_json::to_vec(&self.head)
                    .map_err(|error| MembershipPreparationError(error.to_string()))?
        {
            return Err(MembershipPreparationError(
                "prepared membership publication does not bind one exact entry and head"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMembershipTransition {
    pub(crate) entry: MembershipEntry,
    pub(crate) entry_ref: MembershipEntryRef,
    pub(crate) entry_object: PreparedExactObject,
    pub(crate) transition: MergeMembershipHeadTransition,
}

impl PreparedMembershipTransition {
    pub(crate) fn validate(&self) -> Result<(), MembershipPreparationError> {
        let coord = self.entry.coord();
        let entry_bytes = serde_json::to_vec(&self.entry)
            .map_err(|error| MembershipPreparationError(error.to_string()))?;
        let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
            MembershipPreparationError("membership sequence is exhausted".to_string())
        })?;
        let entry_key = format!(
            "{}.json",
            crate::protocol::store_commit::membership_entry_semantic_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
                coord.entry_hash,
            )
        );
        let head_key = format!(
            "{}.json",
            crate::protocol::store_commit::membership_head_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
            )
        );
        let successor_key = format!(
            "{}.json",
            crate::protocol::store_commit::membership_head_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                next_sequence,
            )
        );
        if self.entry_ref.coord != self.entry.coord()
            || self.entry_ref.object != *self.entry_object.reference()
            || self.entry_object.stored_bytes() != entry_bytes
            || self.entry_ref.object.slot().logical_key() != entry_key
            || self.transition.body.entry != self.entry_ref
            || self.transition.body.resolutions != self.entry.resolution_dependencies
            || self.transition.head_slot.logical_key() != head_key
            || self.transition.body.successor.next_slot.logical_key() != successor_key
        {
            return Err(MembershipPreparationError(
                "prepared membership transition does not bind its exact entry".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) enum StoreMembershipJournalCompletion {
    Mutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<crate::protocol::remote_object::RemoteObjectRecord>,
    },
    RotationMutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
        remote_objects: Vec<crate::protocol::remote_object::RemoteObjectRecord>,
    },
    OwnerPromotion {
        transition: crate::protocol::owner_promotion_journal::OwnerPromotionJournalTransition,
        remote_objects: Vec<crate::protocol::remote_object::RemoteObjectRecord>,
    },
}

impl StoreMembershipJournalCompletion {
    pub(crate) fn object_refs(&self) -> Vec<ExactObjectRef> {
        let remote_objects = match self {
            Self::Mutation { remote_objects, .. }
            | Self::RotationMutation { remote_objects, .. }
            | Self::OwnerPromotion { remote_objects, .. } => remote_objects,
        };
        remote_objects
            .iter()
            .map(|remote| remote.object().clone())
            .collect()
    }

    pub(crate) fn remote_object(
        &self,
        object: &ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::RemoteObjectRecord, MembershipPreparationError>
    {
        let remote_objects = match self {
            Self::Mutation { remote_objects, .. }
            | Self::RotationMutation { remote_objects, .. }
            | Self::OwnerPromotion { remote_objects, .. } => remote_objects,
        };
        remote_objects
            .iter()
            .find(|remote| remote.object() == object)
            .cloned()
            .ok_or_else(|| {
                MembershipPreparationError(
                    "membership completion omits an exact activated object".to_string(),
                )
            })
    }
}
