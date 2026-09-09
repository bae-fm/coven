//! A signed Store operation commit prepared for publication: the exact commit
//! bytes, their reference, and the remote-object records a candidate or
//! activation derives from them.

use crate::membership_mutation::{PreparedMembershipPublication, PreparedMembershipTransition};
use crate::objects::{ExactObjectRef, ExactObjectVersion, PreparedExactObject, StoreObjectError};
use crate::store_commit::{
    ActivatedStoreDeviceRegistration, SnapshotMeta, StoreBatchCommit, StoreBatchCommitRef,
    StoreControl, StoreCurrentPublicationRecord, StorePublicationEntry, StorePublicationPayload,
    StorePublicationRef, StoreSnapshotRef,
};

/// A prepared commit whose parts contradict each other or cannot form valid
/// remote-object records. Workflow errors wrap it at the operation boundary.
#[derive(Debug, thiserror::Error)]
pub enum PreparedCommitError {
    #[error("invalid prepared Store operation: {0}")]
    Invariant(String),
    #[error("prepared Store operation storage: {0}")]
    Storage(#[from] crate::objects::StorageError),
    #[error("prepared Store operation object: {0}")]
    StoreObject(#[from] StoreObjectError),
    #[error("prepared Store protocol: {0}")]
    Protocol(#[from] crate::store_commit::StoreProtocolError),
    #[error("prepared membership transition: {0}")]
    Membership(#[from] crate::membership_mutation::MembershipPreparationError),
    #[error("{operation}: {source}")]
    Json {
        operation: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("prepared Store remote object: {0}")]
    RemoteObject(#[from] crate::remote_object::RemoteObjectRecordError),
}

/// A signed commit and the exact object it is published as.
///
/// `reference.object` names that object; the commit's bytes are what `commit`
/// serializes to, so they are not carried beside it. Whoever uploads rebuilds
/// them through [`PreparedExactObject::new`], which re-checks them against the
/// reference on the way out.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStoreOperationCommon {
    pub commit: StoreBatchCommit,
    pub reference: StoreBatchCommitRef,
    pub registration_activation: Option<ActivatedStoreDeviceRegistration>,
}

/// One immutable Store publication entry and the conditional replacement that
/// can accept it. The observed record and provider version remain together so
/// a retry cannot apply the replacement against another boundary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStorePublication {
    pub previous: StoreCurrentPublicationRecord,
    pub previous_version: ExactObjectVersion,
    pub entry: StorePublicationEntry,
    pub entry_object: ExactObjectRef,
    pub replacement: StoreCurrentPublicationRecord,
}

impl PreparedStorePublication {
    pub fn reference(&self) -> Result<StorePublicationRef, PreparedCommitError> {
        StorePublicationRef::from_entry(&self.entry, self.entry_object.clone())
            .map_err(PreparedCommitError::from)
    }

    pub fn prepared_entry(&self) -> Result<PreparedExactObject, PreparedCommitError> {
        PreparedExactObject::new(self.entry_object.clone(), self.entry.to_bytes())
            .map_err(PreparedCommitError::from)
    }

    pub fn verify_commit(
        &self,
        commit: &crate::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<(), PreparedCommitError> {
        let reference = self.reference()?;
        let signing_pubkey = &commit.author().device_signing_pubkey;
        StorePublicationEntry::parse_at(
            &self.entry.to_bytes(),
            commit.store_root_hash(),
            &reference,
            signing_pubkey,
        )?;
        self.replacement.verify_commit_transition(
            &self.previous,
            &self.entry,
            &reference,
            commit,
            signing_pubkey,
        )?;
        Ok(())
    }

    pub fn validate_commit_shape(
        &self,
        commit: &StoreBatchCommit,
        reference: &StoreBatchCommitRef,
    ) -> Result<(), PreparedCommitError> {
        let publication = self.reference()?;
        if self.entry.predecessor.as_ref() != self.previous.accepted()
            || self.entry.previous_state_hash != self.previous.state_hash()
            || self.replacement.accepted() != Some(&publication)
            || self.replacement.store_root_hash != self.previous.store_root_hash
            || self.entry.store_root_hash != commit.store_root_hash
            || self.entry.author_registration != commit.author_registration
            || !matches!(&self.entry.payload, StorePublicationPayload::Commit(published) if published == reference)
        {
            return Err(PreparedCommitError::Invariant(
                "prepared Store publication differs from its commit or predecessor".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_snapshot_shape(
        &self,
        snapshot: &SnapshotMeta,
        reference: &StoreSnapshotRef,
    ) -> Result<(), PreparedCommitError> {
        let publication = self.reference()?;
        if self.entry.predecessor.as_ref() != self.previous.accepted()
            || self.entry.previous_state_hash != self.previous.state_hash()
            || self.replacement.accepted() != Some(&publication)
            || self.replacement.store_root_hash != self.previous.store_root_hash
            || snapshot.publication_predecessor != self.previous
            || self.entry.store_root_hash != snapshot.store_root_hash
            || self.entry.author_registration != snapshot.author_registration
            || !matches!(&self.entry.payload, StorePublicationPayload::Snapshot(published) if published == reference)
        {
            return Err(PreparedCommitError::Invariant(
                "prepared Store publication differs from its snapshot or predecessor".to_string(),
            ));
        }
        Ok(())
    }
}

impl PreparedStoreOperationCommon {
    /// The commit prepared for upload: its canonical bytes, re-derived from the
    /// value, under the exact reference the operation names.
    pub fn prepared_commit(&self) -> Result<PreparedExactObject, PreparedCommitError> {
        PreparedExactObject::new(self.reference.object.clone(), self.commit.to_bytes())
            .map_err(PreparedCommitError::from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedStoreOperationCommit {
    pub common: PreparedStoreOperationCommon,
    pub publication: PreparedStorePublication,
    pub history_evidence: super::store_commit::RetainedMergeCommitEvidence,
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
    pub(crate) fn candidate_remote_object(
        &self,
    ) -> Result<crate::remote_object::ClosedRemoteObject, PreparedCommitError> {
        let commit_bytes = self.commit.to_bytes();
        // Store commits are signed plaintext: their canonical bytes are also
        // their stored bytes. The publication attempt owns its own entry.
        crate::remote_object::RemoteObjectRecord::candidate_commit(
            self.reference.clone(),
            &commit_bytes,
            &commit_bytes,
        )
        .map_err(PreparedCommitError::from)
    }

    /// Validate the frame every Merge membership-activation candidate shares:
    /// a closed commit, valid transition and publication, the commit's control
    /// naming the transition, and the published head activating this candidate.
    fn validate_merge_membership_activation(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
    ) -> Result<(), PreparedCommitError> {
        self.validate_closed_shape()?;
        transition.validate().map_err(PreparedCommitError::from)?;
        publication.validate().map_err(PreparedCommitError::from)?;
        if self.commit.control()
            != Some(&StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit, .. }
                    if commit == &self.reference
            )
        {
            return Err(PreparedCommitError::Invariant(
                "Merge membership authority graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub fn prepared_membership_publication(
        &self,
    ) -> Result<PreparedMembershipPublication, PreparedCommitError> {
        let proof = self
            .history_evidence
            .membership_proof
            .as_ref()
            .ok_or_else(|| {
                PreparedCommitError::Invariant(
                    "Store control lacks its prepared authority proof".into(),
                )
            })?;
        let publication = PreparedMembershipPublication {
            entry: proof.entry_value.clone(),
            entry_ref: proof.entry.clone(),
            head: proof.head_value.clone(),
            head_ref: proof.head.clone(),
        };
        self.validate_merge_membership_activation(&publication.transition(), &publication)?;
        Ok(publication)
    }

    pub(crate) fn retained_control_remote_objects(
        &self,
        authorities: Vec<crate::remote_object::ClosedRemoteObject>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        let publication = self.prepared_membership_publication()?;
        self.close_merge_membership_remote_objects(&publication, &[], authorities)
    }

    pub fn merge_membership_activation_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        self.validate_merge_membership_activation(transition, publication)?;
        let expected_wraps: &[super::wrapped_store_key::WrappedStoreKeyRef] =
            match &transition.entry.change {
                super::membership::StoreAuthorityChange::RemoveMember { wrapped_keys, .. } => {
                    wrapped_keys
                }
                super::membership::StoreAuthorityChange::SetMember {
                    role:
                        super::membership::StoreMembershipRoleGrant::Member
                        | super::membership::StoreMembershipRoleGrant::Follower,
                    wrapped_key,
                    ..
                } => std::slice::from_ref(wrapped_key),
                _ => {
                    return Err(PreparedCommitError::Invariant(
                        "Merge membership mutation graph contains another change".to_string(),
                    ))
                }
            };
        if expected_wraps.len() != wraps.len()
            || expected_wraps
                .iter()
                .zip(wraps)
                .any(|(reference, prepared)| reference != &prepared.reference)
        {
            return Err(PreparedCommitError::Invariant(
                "Merge membership mutation wraps differ from its exact entry".to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(publication, wraps, Vec::new())
    }

    pub fn merge_membership_resolution_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        resolution: &super::membership::StoreMembershipConflictResolution,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        self.validate_merge_membership_activation(transition, publication)?;
        let resolution_bytes =
            serde_json::to_vec(resolution).map_err(|source| PreparedCommitError::Json {
                operation: "serialize Store membership resolution",
                source,
            })?;
        if !matches!(
            &transition.entry.change,
            super::membership::StoreAuthorityChange::ResolutionActivation {
                resolution: introduced,
            } if introduced == reference
        ) || reference.object.verify(&resolution_bytes).is_err()
            || reference.resolution_hash != resolution.resolution_hash()
            || reference.conflict_hash != resolution.conflict_hash
            || reference.resolver_pubkey != resolution.resolver_pubkey
        {
            return Err(PreparedCommitError::Invariant(
                "Merge membership resolution graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        let authority =
            crate::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                reference.clone(),
                &resolution_bytes,
                &resolution_bytes,
                self.reference.clone(),
            )
            .map_err(PreparedCommitError::from)?;
        self.close_merge_membership_remote_objects(publication, &[], vec![authority])
    }

    pub fn merge_owner_promotion_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        wrapped_key: &super::wrapped_store_key::PreparedWrappedStoreKey,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        self.validate_merge_membership_activation(transition, publication)?;
        if !matches!(
            &transition.entry.change,
            super::membership::StoreAuthorityChange::SetMember { wrapped_key: expected, role: super::membership::StoreMembershipRoleGrant::Owner { .. }, .. }
                if expected == &wrapped_key.reference
        ) {
            return Err(PreparedCommitError::Invariant(
                "Merge Owner-promotion graph differs from its activating Store candidate"
                    .to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(
            publication,
            std::slice::from_ref(wrapped_key),
            Vec::new(),
        )
    }

    fn close_merge_membership_remote_objects(
        &self,
        publication: &PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
        authorities: Vec<crate::remote_object::ClosedRemoteObject>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        let family = self.commit.candidate_family();
        let mut objects = vec![self.candidate_remote_object()?];
        objects.extend(publication.candidate_remote_objects(&self.commit, &self.reference)?);
        for prepared in wraps {
            let value = prepared.validate().map_err(PreparedCommitError::from)?;
            let canonical =
                serde_json::to_vec(&value).map_err(|source| PreparedCommitError::Json {
                    operation: "serialize Merge membership candidate wrap",
                    source,
                })?;
            objects.push(
                crate::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_wrapped_store_key(
                    family,
                    prepared.reference.clone(),
                    &canonical,
                    prepared.object.stored_bytes(),
                    self.reference.clone(),
                )
                .map_err(PreparedCommitError::from)?,
            );
        }
        objects.extend(authorities);
        let mut unique = std::collections::BTreeSet::new();
        if objects
            .iter()
            .any(|object| !unique.insert(object.record().object_id()))
        {
            return Err(PreparedCommitError::Invariant(
                "Merge membership authority graph repeats an exact object".to_string(),
            ));
        }
        Ok(objects)
    }

    pub fn validate_closed_shape(&self) -> Result<(), PreparedCommitError> {
        self.reference.verify_commit(&self.commit)?;
        self.reference.object.verify(&self.commit.to_bytes())?;
        self.publication
            .validate_commit_shape(&self.commit, &self.reference)?;
        self.history_evidence
            .validate_for(&self.reference, &self.commit)?;
        Ok(())
    }

    pub(crate) fn has_same_durable_activation_as(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.commit.to_bytes() == other.commit.to_bytes()
            && self.registration_activation == other.registration_activation
            && self.publication == other.publication
            && self.history_evidence == other.history_evidence
    }

    pub fn acknowledgement_remote_objects(
        &self,
        acknowledgement: &crate::objects::ExactProtocolObject<super::store_commit::StoreAck>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        let reference = self.commit.acknowledgement().ok_or_else(|| {
            PreparedCommitError::Invariant(
                "prepared acknowledgement operation has no exact acknowledgement ref".to_string(),
            )
        })?;
        if &reference.object != acknowledgement.prepared.reference()
            || reference.ack_hash != acknowledgement.value.ack_hash()
            || acknowledgement.value.to_bytes() != acknowledgement.bytes
        {
            return Err(PreparedCommitError::Invariant(
                "prepared acknowledgement operation differs from its exact acknowledgement object"
                    .to_string(),
            ));
        }
        let authority =
            crate::remote_object::RemoteObjectRecord::candidate_activated_store_acknowledgement(
                reference.clone(),
                &acknowledgement.bytes,
                acknowledgement.prepared.stored_bytes(),
                self.reference.clone(),
            )
            .map_err(PreparedCommitError::from)?;
        let mut authorities = vec![authority];
        let retained = self
            .history_evidence
            .acknowledgement
            .as_ref()
            .ok_or_else(|| {
                PreparedCommitError::Invariant(
                    "prepared acknowledgement omits its retained proof".into(),
                )
            })?;
        retained.validate_predecessors()?;
        for (reference, value) in &retained.predecessors {
            let bytes = value.to_bytes();
            authorities.push(
                crate::remote_object::RemoteObjectRecord::candidate_activated_store_acknowledgement(
                    reference.clone(), &bytes, &bytes, self.reference.clone(),
                )?,
            );
        }
        self.retained_authority_remote_objects(authorities)
    }

    pub fn circle_acknowledgement_remote_objects(
        &self,
        acknowledgement: &crate::objects::ExactProtocolObject<super::store_commit::CircleAck>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        let reference = self
            .commit
            .circle_acknowledgements()
            .iter()
            .find(|reference| &reference.object == acknowledgement.prepared.reference())
            .ok_or_else(|| {
                PreparedCommitError::Invariant(
                    "prepared activation does not name its Circle acknowledgement object"
                        .to_string(),
                )
            })?;
        if reference.circle_id != acknowledgement.value.circle_id
            || reference.ack_hash != acknowledgement.value.ack_hash()
            || acknowledgement.value.to_bytes() != acknowledgement.bytes
        {
            return Err(PreparedCommitError::Invariant(
                "prepared Circle acknowledgement differs from its exact acknowledgement object"
                    .to_string(),
            ));
        }
        let authority =
            crate::remote_object::RemoteObjectRecord::candidate_activated_circle_acknowledgement(
                reference.clone(),
                &acknowledgement.bytes,
                acknowledgement.prepared.stored_bytes(),
                self.reference.clone(),
            )
            .map_err(PreparedCommitError::from)?;
        self.retained_authority_remote_objects(vec![authority])
    }

    pub fn retained_authority_remote_objects(
        &self,
        authorities: Vec<crate::remote_object::ClosedRemoteObject>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        if authorities.is_empty() {
            return Err(PreparedCommitError::Invariant(
                "Store operation has no retained authority objects".to_string(),
            ));
        }
        let mut authority_ids = std::collections::BTreeSet::new();
        for authority in &authorities {
            if !matches!(authority.record(), crate::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(&record.state, crate::remote_object::RetainedAuthorityObjectState::Prepared { ownership }
                    if ownership.pending == std::collections::BTreeSet::from([self.reference.clone()])))
            {
                return Err(PreparedCommitError::Invariant(
                    "Store operation retained authority has different candidate ownership"
                        .to_string(),
                ));
            }
            if !authority_ids.insert(authority.record().object_id()) {
                return Err(PreparedCommitError::Invariant(
                    "Store operation repeats a retained authority object".to_string(),
                ));
            }
        }
        let mut objects = vec![self.candidate_remote_object()?];
        objects.extend(authorities);
        Ok(objects)
    }

    pub fn attach_merge_membership_proof_with(
        &mut self,
        publication: &PreparedMembershipPublication,
        resolution_value: Option<&super::membership::StoreMembershipConflictResolution>,
    ) -> Result<(), PreparedCommitError> {
        publication.validate().map_err(PreparedCommitError::from)?;
        let reference = self.common.reference.clone();
        let commit = self.common.commit.clone();
        let Some(StoreControl { transition }) = commit.control() else {
            return Err(PreparedCommitError::Invariant(
                "Merge membership proof accompanies another Store control".to_string(),
            ));
        };
        if !transition.matches_head(&publication.head, &publication.head_ref)
            || publication.entry_ref != transition.body.entry
        {
            return Err(PreparedCommitError::Invariant(
                "Merge membership proof differs from its signed Store transition".to_string(),
            ));
        }
        let resolution = match &publication.entry.change {
            super::membership::StoreAuthorityChange::ResolutionActivation { resolution } => {
                let value = resolution_value.ok_or_else(|| {
                    PreparedCommitError::Invariant(
                        "Merge resolution activation lacks its exact resolution proof".to_string(),
                    )
                })?;
                if value.resolution_ref(resolution.object.clone()) != *resolution {
                    return Err(PreparedCommitError::Invariant(
                        "Merge resolution proof differs from its exact reference".to_string(),
                    ));
                }
                (Some(resolution.clone()), Some(value.clone()))
            }
            _ if resolution_value.is_none() => (None, None),
            _ => {
                return Err(PreparedCommitError::Invariant(
                    "non-resolution membership proof carries a resolution".to_string(),
                ))
            }
        };
        self.history_evidence.membership_proof = Some(Box::new(
            super::store_commit::RetainedMergeMembershipProof {
                commit: reference,
                commit_value: commit,
                entry: publication.entry_ref.clone(),
                entry_value: publication.entry.clone(),
                head: publication.head_ref.clone(),
                head_value: publication.head.clone(),
                resolution: resolution.0,
                resolution_value: resolution.1,
            },
        ));
        self.validate_closed_shape()?;
        Ok(())
    }
}

/// One Circle acknowledgement object riding an activating Store commit: its
/// exact reference (named in the signed commit body) and the exact object the
/// commit uploads and takes ownership of.
#[derive(Debug, Clone)]
pub struct CircleAckActivation {
    pub reference: crate::store_commit::CircleAckRef,
    pub ack: crate::objects::ExactProtocolObject<crate::store_commit::CircleAck>,
}
