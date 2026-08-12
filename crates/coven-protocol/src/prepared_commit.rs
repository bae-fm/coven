//! A signed Store operation commit prepared for publication: the exact commit
//! bytes, their reference, and the remote-object records a candidate or
//! activation derives from them.

use crate::membership_mutation::{PreparedMembershipPublication, PreparedMembershipTransition};
use crate::objects::{ExactObjectRef, PreparedExactObject, StoreObjectError};
use crate::store_commit::{
    ActivatedStoreDeviceRegistration, StoreBatchCommit, StoreBatchCommitRef, StoreControl,
    StoreDeviceHead, StoreDeviceHeadRef,
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
    pub head: StoreDeviceHead,
    pub head_object: ExactObjectRef,
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
    fn candidate_remote_objects(
        &self,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        let commit_bytes = self.commit.to_bytes();
        let head_bytes = self.head.to_bytes();
        // A Store commit and a Store head are signed plaintext: what goes to
        // storage is the canonical value, so both arguments are the same bytes.
        let mut objects = vec![crate::remote_object::RemoteObjectRecord::candidate_commit(
            self.reference.clone(),
            &commit_bytes,
            &commit_bytes,
        )
        .map_err(PreparedCommitError::from)?];
        objects.push(
            crate::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                self.head_ref(),
                &head_bytes,
                &head_bytes,
                self.reference.clone(),
            )
            .map_err(PreparedCommitError::from)?,
        );
        Ok(objects)
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
                super::membership::MembershipHeadActivation::StoreCommit { commit }
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

    pub fn merge_membership_activation_remote_objects(
        &self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        self.validate_merge_membership_activation(transition, publication)?;
        let expected_wraps = match &transition.entry.change {
            super::membership::MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
            _ => {
                return Err(PreparedCommitError::Invariant(
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
            return Err(PreparedCommitError::Invariant(
                "Merge membership removal wraps differ from its exact entry".to_string(),
            ));
        }
        self.close_merge_membership_remote_objects(transition, publication, wraps, Vec::new())
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
            super::membership::MembershipChange::ResolutionActivation {
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
        self.close_merge_membership_remote_objects(transition, publication, &[], vec![authority])
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
            super::membership::MembershipChange::SetMember { wrapped_key: expected, role: super::membership::StoreMembershipRoleGrant::Owner { .. }, .. }
                if expected == &wrapped_key.reference
        ) {
            return Err(PreparedCommitError::Invariant(
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
        authorities: Vec<crate::remote_object::ClosedRemoteObject>,
    ) -> Result<Vec<crate::remote_object::ClosedRemoteObject>, PreparedCommitError> {
        let family = self.commit.candidate_family();
        let mut objects = self.candidate_remote_objects()?;
        let entry_bytes =
            serde_json::to_vec(&transition.entry).map_err(|source| PreparedCommitError::Json {
                operation: "serialize Merge membership candidate entry",
                source,
            })?;
        let head_bytes =
            serde_json::to_vec(&publication.head).map_err(|source| PreparedCommitError::Json {
                operation: "serialize Merge membership candidate head",
                source,
            })?;
        // Membership entries and heads are signed plaintext, so the canonical
        // value is also what goes to storage.
        objects.push(
            crate::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
                family,
                transition.entry_ref.clone(),
                &entry_bytes,
                &entry_bytes,
                self.reference.clone(),
            )
            .map_err(PreparedCommitError::from)?,
        );
        objects.push(
            crate::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
                family,
                publication.head_ref.clone(),
                &head_bytes,
                &head_bytes,
                self.reference.clone(),
            )
            .map_err(PreparedCommitError::from)?,
        );
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
        if self.head.commit != self.reference {
            return Err(PreparedCommitError::Invariant(
                "prepared Store operation head names another commit".to_string(),
            ));
        }
        self.head_object.verify(&self.head.to_bytes())?;
        self.history_evidence
            .validate_for(&self.reference, &self.commit)?;
        Ok(())
    }

    pub(crate) fn has_same_durable_activation_as(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.commit.to_bytes() == other.commit.to_bytes()
            && self.registration_activation == other.registration_activation
            && self.head.to_bytes() == other.head.to_bytes()
            && self.head_object == other.head_object
            && self.history_evidence == other.history_evidence
    }

    /// The activation head prepared for upload: its canonical bytes, re-derived
    /// from the value, under the exact object the operation names.
    pub fn prepared_head(&self) -> Result<PreparedExactObject, PreparedCommitError> {
        PreparedExactObject::new(self.head_object.clone(), self.head.to_bytes())
            .map_err(PreparedCommitError::from)
    }

    pub fn publication(&self) -> (&StoreDeviceHead, &ExactObjectRef) {
        (&self.head, &self.head_object)
    }

    pub fn head_ref(&self) -> StoreDeviceHeadRef {
        StoreDeviceHeadRef {
            head_hash: self.head.head_hash(),
            object: self.head_object.clone(),
        }
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
        self.retained_authority_remote_objects(vec![authority])
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
        let mut objects = self.candidate_remote_objects()?;
        objects.extend(authorities);
        Ok(objects)
    }

    pub fn adopt_merge_head(
        &mut self,
        winner: StoreDeviceHead,
        object: ExactObjectRef,
    ) -> Result<(), PreparedCommitError> {
        let current = &mut self.head;
        let current_object = &mut self.head_object;
        if winner.commit != self.common.reference
            || object.slot() != current_object.slot()
            || object == *current_object
            || winner.author_registration != current.author_registration
            || winner.successor.activation != current.successor.activation
            || winner.successor.predecessor != current.successor.predecessor
        {
            return Err(PreparedCommitError::Invariant(
                "alternate Merge head differs from the prepared activation point".to_string(),
            ));
        }
        *current = winner;
        *current_object = object;
        Ok(())
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
            super::membership::MembershipChange::ResolutionActivation { resolution } => {
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
                announcement: None,
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
