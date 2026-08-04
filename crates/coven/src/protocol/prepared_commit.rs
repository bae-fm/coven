//! A signed Store operation commit prepared for publication: the exact commit
//! bytes, their reference, and the remote-object records a candidate or
//! activation derives from them.

use crate::keys::UserKeypair;
use crate::protocol::membership_mutation::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
use crate::protocol::objects::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError,
};
use crate::protocol::store_commit::head_slot_prefix;
use crate::protocol::store_commit::{
    ActivatedStoreDeviceRegistration, StoreBatchCommit, StoreBatchCommitRef, StoreControl,
    StoreDeviceHead, StoreDeviceHeadRef,
};

/// A prepared commit whose parts contradict each other or cannot form valid
/// remote-object records. Workflow errors wrap it at the operation boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid prepared Store operation: {0}")]
pub(crate) struct PreparedCommitError(pub(crate) String);

impl From<crate::protocol::objects::StorageError> for PreparedCommitError {
    fn from(error: crate::protocol::objects::StorageError) -> Self {
        PreparedCommitError(error.to_string())
    }
}

impl From<StoreObjectError> for PreparedCommitError {
    fn from(error: StoreObjectError) -> Self {
        PreparedCommitError(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedStoreOperationCommon {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) prepared: PreparedExactObject,
    pub(crate) reference: StoreBatchCommitRef,
    pub(crate) registration_activation: Option<ActivatedStoreDeviceRegistration>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedStoreOperationCommit {
    pub(crate) common: PreparedStoreOperationCommon,
    pub(crate) head: StoreDeviceHead,
    pub(crate) prepared_head: PreparedExactObject,
    pub(crate) history_summary: super::store_commit::RetainedVerifiedMergeHistorySummary,
}

impl std::ops::Deref for PreparedStoreOperationCommit {
    type Target = PreparedStoreOperationCommon;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl std::ops::DerefMut for PreparedStoreOperationCommit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}

impl PreparedStoreOperationCommit {
    fn candidate_remote_objects(
        &self,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        let mut objects = vec![
            crate::protocol::remote_object::RemoteObjectRecord::candidate_commit(
                self.reference.clone(),
                self.commit.to_bytes(),
                self.prepared.stored_bytes().to_vec(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?,
        ];
        objects.push(
            crate::protocol::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                super::store_commit::StoreDeviceHeadRef {
                    head_hash: self.head.head_hash(),
                    object: self.prepared_head.reference().clone(),
                },
                self.head.to_bytes(),
                self.prepared_head.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?,
        );
        Ok(objects)
    }

    pub(crate) fn merge_membership_activation_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        self.validate_closed_shape().map_err(PreparedCommitError)?;
        transition
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        publication
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        if self.commit.control()
            != Some(&StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.reference
            )
        {
            return Err(PreparedCommitError(
                "Merge membership authority graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        let expected_wraps = match &transition.entry.change {
            super::membership::MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
            _ => {
                return Err(PreparedCommitError(
                    "Merge membership removal graph contains another change".to_string(),
                ))
            }
        };
        if expected_wraps.len() != wraps.len()
            || expected_wraps
                .iter()
                .zip(wraps)
                .any(|(reference, prepared)| reference != &prepared.reference)
        {
            return Err(PreparedCommitError(
                "Merge membership removal wraps differ from its exact entry".to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(transition, publication, wraps, Vec::new())
    }

    pub(crate) fn merge_membership_resolution_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        resolution: &super::membership::StoreMembershipConflictResolution,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
        prepared: &PreparedExactObject,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        self.validate_closed_shape().map_err(PreparedCommitError)?;
        transition
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        publication
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        if self.commit.control()
            != Some(&StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.reference
            )
            || !matches!(
                &transition.entry.change,
                super::membership::MembershipChange::ResolutionActivation {
                    resolution: introduced,
                } if introduced == reference
            )
            || reference.object != *prepared.reference()
            || reference.resolution_hash != resolution.resolution_hash()
            || reference.conflict_hash != resolution.conflict_hash
            || reference.resolver_pubkey != resolution.resolver_pubkey
            || prepared.stored_bytes()
                != serde_json::to_vec(resolution).map_err(|error| {
                    PreparedCommitError(format!("serialize Store membership resolution: {error}"))
                })?
        {
            return Err(PreparedCommitError(
                "Merge membership resolution graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        let authority =
            crate::protocol::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                reference.clone(),
                serde_json::to_vec(resolution).map_err(|error| {
                    PreparedCommitError(format!(
                        "serialize Store membership resolution: {error}"
                    ))
                })?,
                prepared.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        self.close_merge_membership_remote_objects(transition, publication, &[], vec![authority])
    }

    pub(crate) fn merge_owner_promotion_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        wrapped_key: &super::wrapped_store_key::PreparedWrappedStoreKey,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        self.validate_closed_shape().map_err(PreparedCommitError)?;
        transition
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        publication
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        if self.commit.control()
            != Some(&StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.reference
            )
            || !matches!(
                &transition.entry.change,
                super::membership::MembershipChange::SetMember { wrapped_key: expected, role: super::membership::StoreMembershipRoleGrant::Owner { .. }, .. }
                    if expected == &wrapped_key.reference
            )
        {
            return Err(PreparedCommitError(
                "Merge Owner-promotion graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(
            transition,
            publication,
            std::slice::from_ref(wrapped_key),
            Vec::new(),
        )
    }

    fn close_merge_membership_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
        authorities: Vec<crate::protocol::remote_object::RemoteObjectRecord>,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        let family = self.commit.candidate_family();
        let mut objects = self.candidate_remote_objects()?;
        let entry_bytes = serde_json::to_vec(&transition.entry).map_err(|error| {
            PreparedCommitError(format!(
                "serialize Merge membership candidate entry: {error}"
            ))
        })?;
        objects.push(
            crate::protocol::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
                family,
                transition.entry_ref.clone(),
                entry_bytes,
                transition.entry_object.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?,
        );
        objects.push(
            crate::protocol::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
                family,
                publication.head_ref.clone(),
                serde_json::to_vec(&publication.head).map_err(|error| {
                    PreparedCommitError(format!(
                        "serialize Merge membership candidate head: {error}"
                    ))
                })?,
                publication.head_object.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?,
        );
        for prepared in wraps {
            let value = prepared.validate().map_err(PreparedCommitError::from)?;
            let canonical = serde_json::to_vec(&value).map_err(|error| {
                PreparedCommitError(format!(
                    "serialize Merge membership candidate wrap: {error}"
                ))
            })?;
            objects.push(
                crate::protocol::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_wrapped_store_key(
                    family,
                    prepared.reference.clone(),
                    canonical,
                    prepared.object.stored_bytes().to_vec(),
                    self.reference.clone(),
                )
                .map_err(|error| PreparedCommitError(error.to_string()))?,
            );
        }
        objects.extend(authorities);
        let mut unique = std::collections::BTreeSet::new();
        if objects
            .iter()
            .any(|object| !unique.insert(object.object_id()))
        {
            return Err(PreparedCommitError(
                "Merge membership authority graph repeats an exact object".to_string(),
            ));
        }
        Ok(objects)
    }

    pub(crate) fn validate_closed_shape(&self) -> Result<(), String> {
        self.reference
            .verify_commit(&self.commit)
            .map_err(|error| error.to_string())?;
        if self.prepared.reference() != &self.reference.object
            || self.prepared.stored_bytes() != self.commit.to_bytes()
        {
            return Err(
                "prepared Store operation does not bind its exact commit bytes".to_string(),
            );
        }
        if self.head.commit != self.reference
            || self.prepared_head.stored_bytes() != self.head.to_bytes()
            || self.head.history_summary != self.history_summary.digest()
            || self.history_summary.causal_cut.get(&self.reference.coord) != Some(&self.reference)
            || self.history_summary.validate_shape().is_err()
            || self.commit.control().is_some()
                != self
                    .history_summary
                    .membership_proofs
                    .contains_key(&self.reference)
        {
            return Err(
                "prepared Store operation does not bind its exact activation head".to_string(),
            );
        }
        Ok(())
    }

    pub(crate) fn has_same_durable_activation_as(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.commit.to_bytes() == other.commit.to_bytes()
            && self.prepared.reference() == other.prepared.reference()
            && self.registration_activation == other.registration_activation
            && self.head.to_bytes() == other.head.to_bytes()
            && self.prepared_head.reference() == other.prepared_head.reference()
            && self.history_summary == other.history_summary
    }

    pub(crate) fn publication(&self) -> (&StoreDeviceHead, &PreparedExactObject) {
        (&self.head, &self.prepared_head)
    }

    pub(crate) fn head_ref(&self) -> StoreDeviceHeadRef {
        StoreDeviceHeadRef {
            head_hash: self.head.head_hash(),
            object: self.prepared_head.reference().clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn publication_for_test(&self) -> (&StoreDeviceHead, &PreparedExactObject) {
        self.publication()
    }

    pub(crate) fn acknowledgement_remote_objects(
        &self,
        acknowledgement: &crate::database::ExactProtocolObject<super::store_commit::StoreAck>,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        let reference = self.commit.acknowledgement().ok_or_else(|| {
            PreparedCommitError(
                "prepared acknowledgement operation has no exact acknowledgement ref".to_string(),
            )
        })?;
        if reference.object != acknowledgement.object
            || reference.ack_hash != acknowledgement.value.ack_hash()
            || acknowledgement.value.to_bytes() != acknowledgement.bytes
        {
            return Err(PreparedCommitError(
                "prepared acknowledgement operation differs from its exact acknowledgement object"
                    .to_string(),
            ));
        }
        let authority =
            crate::protocol::remote_object::RemoteObjectRecord::candidate_activated_store_acknowledgement(
                reference.clone(),
                acknowledgement.bytes.clone(),
                acknowledgement.prepared.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        self.retained_authority_remote_objects(vec![authority])
    }

    pub(crate) fn circle_acknowledgement_remote_objects(
        &self,
        acknowledgement: &crate::database::ExactProtocolObject<super::store_commit::CircleAck>,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        let reference = self
            .commit
            .circle_acknowledgements()
            .iter()
            .find(|reference| reference.object == acknowledgement.object)
            .ok_or_else(|| {
                PreparedCommitError(
                    "prepared activation does not name its Circle acknowledgement object"
                        .to_string(),
                )
            })?;
        if reference.circle_id != acknowledgement.value.circle_id
            || reference.ack_hash != acknowledgement.value.ack_hash()
            || acknowledgement.value.to_bytes() != acknowledgement.bytes
        {
            return Err(PreparedCommitError(
                "prepared Circle acknowledgement differs from its exact acknowledgement object"
                    .to_string(),
            ));
        }
        let authority =
            crate::protocol::remote_object::RemoteObjectRecord::candidate_activated_circle_acknowledgement(
                reference.clone(),
                acknowledgement.bytes.clone(),
                acknowledgement.prepared.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        self.retained_authority_remote_objects(vec![authority])
    }

    pub(crate) fn retained_authority_remote_objects(
        &self,
        authorities: Vec<crate::protocol::remote_object::RemoteObjectRecord>,
    ) -> Result<Vec<crate::protocol::remote_object::RemoteObjectRecord>, PreparedCommitError> {
        if authorities.is_empty() {
            return Err(PreparedCommitError(
                "Store operation has no retained authority objects".to_string(),
            ));
        }
        let mut authority_ids = std::collections::BTreeSet::new();
        for authority in &authorities {
            authority
                .validate()
                .map_err(|error| PreparedCommitError(error.to_string()))?;
            if !matches!(authority, crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(&record.state, crate::protocol::remote_object::RetainedAuthorityObjectState::Prepared { ownership }
                    if ownership.pending == std::collections::BTreeSet::from([self.reference.clone()])))
            {
                return Err(PreparedCommitError(
                    "Store operation retained authority has different candidate ownership"
                        .to_string(),
                ));
            }
            if !authority_ids.insert(authority.object_id()) {
                return Err(PreparedCommitError(
                    "Store operation repeats a retained authority object".to_string(),
                ));
            }
        }
        let mut objects = self.candidate_remote_objects()?;
        objects.extend(authorities);
        Ok(objects)
    }

    pub(crate) fn adopt_merge_head(
        &mut self,
        winner: StoreDeviceHead,
        prepared: PreparedExactObject,
    ) -> Result<(), PreparedCommitError> {
        let current = &mut self.head;
        let current_prepared = &mut self.prepared_head;
        let history_summary = &self.history_summary;
        if winner.commit != self.common.reference
            || prepared.reference().slot() != current_prepared.reference().slot()
            || prepared.reference() == current_prepared.reference()
            || winner.author_registration != current.author_registration
            || winner.successor.activation != current.successor.activation
            || winner.successor.predecessor != current.successor.predecessor
            || winner.history_summary != history_summary.digest()
        {
            return Err(PreparedCommitError(
                "alternate Merge head differs from the prepared activation point".to_string(),
            ));
        }
        *current = winner;
        *current_prepared = prepared;
        Ok(())
    }

    pub(crate) fn attach_merge_membership_proof_with(
        &mut self,
        publication: &PreparedMembershipPublication,
        resolution_value: Option<&super::membership::StoreMembershipConflictResolution>,
        identity_signer: &UserKeypair,
        prepare_head: impl FnOnce(
            &ProtocolObjectContext,
            crate::protocol::objects::ObjectSlot,
            &str,
            Vec<u8>,
        ) -> Result<PreparedExactObject, StoreObjectError>,
    ) -> Result<(), PreparedCommitError> {
        publication
            .validate()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        let reference = self.common.reference.clone();
        let commit = self.common.commit.clone();
        let head = &mut self.head;
        let prepared = &mut self.prepared_head;
        let history_summary = &mut self.history_summary;
        let Some(StoreControl { transition }) = commit.control() else {
            return Err(PreparedCommitError(
                "Merge membership proof accompanies another Store control".to_string(),
            ));
        };
        if !transition.matches_head(&publication.head, &publication.head_ref)
            || publication.entry_ref != transition.body.entry
        {
            return Err(PreparedCommitError(
                "Merge membership proof differs from its signed Store transition".to_string(),
            ));
        }
        let resolution = match &publication.entry.change {
            super::membership::MembershipChange::ResolutionActivation { resolution } => {
                let value = resolution_value.ok_or_else(|| {
                    PreparedCommitError(
                        "Merge resolution activation lacks its exact resolution proof".to_string(),
                    )
                })?;
                if value.resolution_ref(resolution.object.clone()) != *resolution {
                    return Err(PreparedCommitError(
                        "Merge resolution proof differs from its exact reference".to_string(),
                    ));
                }
                (Some(resolution.clone()), Some(value.clone()))
            }
            _ if resolution_value.is_none() => (None, None),
            _ => {
                return Err(PreparedCommitError(
                    "non-resolution membership proof carries a resolution".to_string(),
                ))
            }
        };
        history_summary.membership_proofs.insert(
            reference.clone(),
            super::store_commit::RetainedMergeMembershipProof {
                commit: reference,
                commit_value: commit,
                announcement: None,
                entry: publication.entry_ref.clone(),
                entry_value: publication.entry.clone(),
                head: publication.head_ref.clone(),
                head_value: publication.head.clone(),
                resolution: resolution.0,
                resolution_value: resolution.1,
            },
        );
        history_summary
            .membership_floor
            .advance(
                publication.entry_ref.coord.clone(),
                &publication.head.body.resolutions,
            )
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        history_summary
            .validate_shape()
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        let author = history_summary
            .registrations
            .get(&head.author_registration.device_id)
            .filter(|registration| registration.reference() == &head.author_registration)
            .ok_or_else(|| {
                PreparedCommitError(
                    "Merge membership proof lacks its exact author registration".to_string(),
                )
            })?;
        let device_signer = author
            .value()
            .device_signer(identity_signer)
            .map_err(|error| PreparedCommitError(error.to_string()))?;
        let replacement = StoreDeviceHead::signed(
            head.store_root_hash,
            head.author_registration.clone(),
            head.commit.clone(),
            history_summary.digest(),
            head.successor.clone(),
            &device_signer,
        )
        .map_err(|error| PreparedCommitError(error.to_string()))?;
        let context = ProtocolObjectContext::signed_plaintext(
            head.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let prefix = head_slot_prefix(
            &head.author_registration.device_id.to_string(),
            head.commit.coord.sequence(),
        );
        *prepared = prepare_head(
            &context,
            prepared.reference().slot().clone(),
            &prefix,
            replacement.to_bytes(),
        )?;
        *head = replacement;
        self.validate_closed_shape().map_err(PreparedCommitError)?;
        Ok(())
    }
}

/// One Circle acknowledgement object riding an activating Store commit: its
/// exact reference (named in the signed commit body) and the exact object the
/// commit uploads and takes ownership of.
#[derive(Debug, Clone)]
pub(crate) struct CircleAckActivation {
    pub reference: crate::protocol::store_commit::CircleAckRef,
    pub ack:
        crate::protocol::objects::ExactProtocolObject<crate::protocol::store_commit::CircleAck>,
}
