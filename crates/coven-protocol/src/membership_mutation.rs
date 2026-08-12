//! Prepared membership mutations: the exact entry, head, and objects one
//! membership publication or transition binds, validated as a unit before
//! anything durable records them.

use crate::membership::{
    self, AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    MergeMembershipHeadTransition,
};
use crate::objects::{ExactObjectRef, PreparedExactObject};
use crate::store_commit::ObjectHash;

/// One value prepared for upload: its canonical bytes under the exact object its
/// reference names.
///
/// The bytes are not carried beside the value — they are what the value
/// serializes to — so both the validators and the upload paths re-derive them
/// here. [`PreparedExactObject::new`] checks them against the reference, so a
/// value that does not serialize to what its reference names fails at this call
/// rather than reaching storage.
pub fn prepare_exact_object(
    object: &ExactObjectRef,
    value: &impl serde::Serialize,
) -> Result<PreparedExactObject, MembershipPreparationError> {
    let bytes = serde_json::to_vec(value).map_err(MembershipPreparationError::Json)?;
    PreparedExactObject::new(object.clone(), bytes).map_err(MembershipPreparationError::ExactObject)
}

fn binds_exact_object(object: &ExactObjectRef, value: &impl serde::Serialize) -> bool {
    prepare_exact_object(object, value).is_ok()
}

/// A prepared membership mutation whose parts do not bind one exact entry and
/// head. Workflow errors wrap it at the operation boundary.
#[derive(Debug, thiserror::Error)]
pub enum MembershipPreparationError {
    #[error("invalid prepared membership mutation: {0}")]
    Invariant(String),
    #[error("serialize prepared membership mutation: {0}")]
    Json(#[source] serde_json::Error),
    #[error("prepared membership exact object: {0}")]
    ExactObject(#[source] crate::objects::StorageError),
}

/// One membership entry and the head that publishes it, each named by its exact
/// reference.
///
/// The entry and head values are here, and `entry_ref.object` / `head_ref.object`
/// name the objects they serialize to, so nothing carries their bytes a second
/// time: the upload rebuilds them from the value and the reference re-checks
/// them on the way out.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedMembershipPublication {
    pub entry: MembershipEntry,
    pub entry_ref: MembershipEntryRef,
    pub head: AuthorHead,
    pub head_ref: MembershipHeadRef,
}

impl PreparedMembershipPublication {
    pub fn validate(&self) -> Result<(), MembershipPreparationError> {
        PreparedMembershipTransition {
            entry: self.entry.clone(),
            entry_ref: self.entry_ref.clone(),
            transition: membership::MergeMembershipHeadTransition {
                body: self.head.body.clone(),
                head_slot: self.head_ref.object.slot().clone(),
            },
        }
        .validate()?;
        let coord = self.entry.coord();
        if self.entry_ref.coord != coord
            || self.head.body.entry != self.entry_ref
            || self.head.entry_coord() != coord
            || self.head_ref.coord != coord
            || self.head_ref.head_hash != self.head.head_hash()
            || !binds_exact_object(&self.head_ref.object, &self.head)
        {
            return Err(MembershipPreparationError::Invariant(
                "prepared membership publication does not bind one exact entry and head"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// The entry object this publication uploads.
    pub fn prepared_entry(&self) -> Result<PreparedExactObject, MembershipPreparationError> {
        prepare_exact_object(&self.entry_ref.object, &self.entry)
    }

    /// The head object this publication uploads.
    pub fn prepared_head(&self) -> Result<PreparedExactObject, MembershipPreparationError> {
        prepare_exact_object(&self.head_ref.object, &self.head)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedMembershipTransition {
    pub entry: MembershipEntry,
    pub entry_ref: MembershipEntryRef,
    pub transition: MergeMembershipHeadTransition,
}

impl PreparedMembershipTransition {
    pub fn validate(&self) -> Result<(), MembershipPreparationError> {
        let coord = self.entry.coord();
        let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
            MembershipPreparationError::Invariant("membership sequence is exhausted".to_string())
        })?;
        let entry_key = format!(
            "{}.json",
            crate::store_commit::membership_entry_semantic_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
                coord.entry_hash,
            )
        );
        let head_key = format!(
            "{}.json",
            crate::store_commit::membership_head_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
            )
        );
        let successor_key = format!(
            "{}.json",
            crate::store_commit::membership_head_slot_prefix(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                next_sequence,
            )
        );
        if self.entry_ref.coord != self.entry.coord()
            || !binds_exact_object(&self.entry_ref.object, &self.entry)
            || self.entry_ref.object.slot().logical_key() != entry_key
            || self.transition.body.entry != self.entry_ref
            || self.transition.body.resolutions != self.entry.resolution_dependencies
            || self.transition.head_slot.logical_key() != head_key
            || self.transition.body.successor.next_slot.logical_key() != successor_key
        {
            return Err(MembershipPreparationError::Invariant(
                "prepared membership transition does not bind its exact entry".to_string(),
            ));
        }
        Ok(())
    }

    /// The entry object this transition uploads.
    pub fn prepared_entry(&self) -> Result<PreparedExactObject, MembershipPreparationError> {
        prepare_exact_object(&self.entry_ref.object, &self.entry)
    }
}

pub enum StoreMembershipJournalCompletion {
    Mutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<crate::remote_object::RemoteObjectRecord>,
    },
    RotationMutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
        remote_objects: Vec<crate::remote_object::RemoteObjectRecord>,
    },
    OwnerPromotion {
        transition: crate::owner_promotion_journal::OwnerPromotionJournalTransition,
        remote_objects: Vec<crate::remote_object::RemoteObjectRecord>,
    },
}

impl StoreMembershipJournalCompletion {
    pub fn object_refs(&self) -> Vec<ExactObjectRef> {
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

    pub fn remote_object(
        &self,
        object: &ExactObjectRef,
    ) -> Result<crate::remote_object::RemoteObjectRecord, MembershipPreparationError> {
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
                MembershipPreparationError::Invariant(
                    "membership completion omits an exact activated object".to_string(),
                )
            })
    }
}
