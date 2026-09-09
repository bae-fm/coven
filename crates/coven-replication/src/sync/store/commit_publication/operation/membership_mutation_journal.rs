use coven_database::DurableMembershipMutation;
use coven_database::StoreDatabase;
use coven_keys::encryption::EncryptionService;
use coven_protocol::membership::{
    self, MemberRole, MembershipEntry, StoreAuthorityChange, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
use coven_protocol::membership_mutation::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject};
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::remote_object::{
    CandidateNonactivation, ClosedRemoteObject, RemoteObjectRecord,
};
use coven_protocol::store_commit::{self, ObjectHash, StoreBatchCommitRef};
use coven_protocol::wrapped_store_key::PreparedWrappedStoreKey;
use coven_storage::cloud::{CloudAccessOutcome, CloudAccessState, CloudHomeJoinInfo};
use coven_storage::CloudSyncObjectStorage;

use crate::sync::store::membership::MembershipMutationError;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    content = "plan",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum MembershipMutationPlan {
    Admission(AdmissionMutationPlan),
    Revoke(RevokeMutationPlan),
    Resolve(ResolveMutationPlan),
}

impl MembershipMutationPlan {
    pub(super) fn encode(&self) -> Result<Vec<u8>, MembershipMutationError> {
        serde_json::to_vec(self).map_err(MembershipMutationError::Json)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdmissionMutationPlan {
    pub(super) activation: PreparedMembershipActivation,
    pub(super) member_pubkey: String,
    pub(super) member_email: Option<String>,
    pub(super) role: MemberRole,
    pub(super) desired_access: CloudAccessState,
    pub(super) wrapped_key: PreparedWrappedStoreKey,
}

impl AdmissionMutationPlan {
    pub(super) fn matches_request(
        &self,
        owner_pubkey: &str,
        member_pubkey: &str,
        member_email: Option<&str>,
        role: &MemberRole,
        store_id: &str,
    ) -> bool {
        self.activation.publication.entry.author_pubkey == owner_pubkey
            && self.activation.publication.entry.store_id == store_id
            && self.member_pubkey == member_pubkey
            && self.member_email.as_deref() == member_email
            && &self.role == role
            && self.desired_access
                == (CloudAccessState::Present {
                    member_pubkey: member_pubkey.to_string(),
                    provider_account_email: member_email.map(str::to_string),
                })
            && matches!(
                &self.activation.publication.entry.change,
                StoreAuthorityChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role: entry_role,
                    ..
                } if user_pubkey == member_pubkey
                    && provider_account_email.as_deref() == member_email
                    && entry_role.role() == *role
            )
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevokeMutationPlan {
    pub(super) publication: PreparedMembershipActivation,
    pub(super) revokee_pubkey: String,
    pub(super) desired_access: CloudAccessState,
    pub(super) prior_access: CloudAccessState,
    pub(super) wraps: Vec<ReplacementWrappedKey>,
    pub(super) keyring_payload: Vec<u8>,
}

impl RevokeMutationPlan {
    pub(super) fn matches_request(
        &self,
        owner_pubkey: &str,
        revokee_pubkey: &str,
        store_id: &str,
    ) -> bool {
        self.publication.entry().author_pubkey == owner_pubkey
            && self.publication.entry().store_id == store_id
            && self.revokee_pubkey == revokee_pubkey
            && matches!(
                &self.publication.entry().change,
                StoreAuthorityChange::RemoveMember { user_pubkey, .. }
                    if user_pubkey == revokee_pubkey
            )
            && matches!(
                &self.desired_access,
                CloudAccessState::Absent { member_pubkey, .. }
                    if member_pubkey == revokee_pubkey
            )
            && matches!(
                &self.prior_access,
                CloudAccessState::Present { member_pubkey, .. }
                    if member_pubkey == revokee_pubkey
            )
    }

    pub(super) fn validate_closed_shape(&self) -> Result<(), MembershipMutationError> {
        let publication = &self.publication.publication;
        publication.validate()?;
        let (desired_member, desired_email) = match &self.desired_access {
            CloudAccessState::Absent {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Present { .. } => {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "membership removal requests present provider access".to_string(),
                ));
            }
        };
        let (prior_member, prior_email) = match &self.prior_access {
            CloudAccessState::Present {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Absent { .. } => {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "membership removal compensation requests absent provider access".to_string(),
                ));
            }
        };
        if desired_member != &self.revokee_pubkey
            || prior_member != &self.revokee_pubkey
            || desired_email != prior_email
        {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership removal access and compensation intents disagree".to_string(),
            ));
        }
        let StoreAuthorityChange::RemoveMember {
            user_pubkey,
            wrapped_keys,
            retirement_device_state,
            retirement_barriers,
            ..
        } = &publication.entry.change
        else {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership removal plan contains another change".to_string(),
            ));
        };
        let planned_wraps = self
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect::<Vec<_>>();
        if user_pubkey != &self.revokee_pubkey || wrapped_keys != &planned_wraps {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership removal plan differs from its exact entry".to_string(),
            ));
        }
        let activation = &self.publication;
        activation.validate()?;
        let retires_owner = retirement_barriers.values().any(|barrier| {
            matches!(
                barrier,
                membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
            )
        });
        if retires_owner != retirement_device_state.is_some()
            || retirement_device_state
                .as_ref()
                .is_some_and(|state| state != &activation.candidate.commit.device_state)
        {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership retirement differs from its exact Store device state".into(),
            ));
        }
        self.candidate_remote_objects()?;
        Ok(())
    }

    pub(super) fn candidate_remote_objects(
        &self,
    ) -> Result<Vec<ClosedRemoteObject>, MembershipMutationError> {
        self.publication.remote_objects(
            &self
                .wraps
                .iter()
                .map(|wrap| wrap.prepared.clone())
                .collect::<Vec<_>>(),
        )
    }

    pub(super) fn candidate_cleanup_objects(&self) -> Vec<ExactObjectRef> {
        std::iter::once(self.publication.candidate.reference.object.clone())
            .chain(std::iter::once(
                self.publication.transition.entry_ref.object.clone(),
            ))
            .chain(std::iter::once(
                self.publication.publication.head_ref.object.clone(),
            ))
            .chain(
                self.wraps
                    .iter()
                    .map(|wrap| wrap.prepared.reference.object.clone()),
            )
            .collect()
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResolveMutationPlan {
    pub(super) resolution: StoreMembershipConflictResolution,
    pub(super) reference: StoreMembershipConflictResolutionRef,
    pub(super) transition: Box<PreparedMembershipTransition>,
    pub(super) candidate: Box<PreparedStoreOperationCommit>,
    pub(super) publication: Box<PreparedMembershipPublication>,
}

impl ResolveMutationPlan {
    /// The resolution object this plan uploads: the resolution's canonical bytes
    /// under the exact object its reference names.
    pub(super) fn prepared_resolution(
        &self,
    ) -> Result<PreparedExactObject, MembershipMutationError> {
        coven_protocol::membership_mutation::prepare_exact_object(
            &self.reference.object,
            &self.resolution,
        )
        .map_err(MembershipMutationError::from)
    }

    pub(super) fn candidate_cleanup_objects(&self) -> (Vec<ExactObjectRef>, Vec<ExactObjectRef>) {
        (
            vec![
                self.candidate.reference.object.clone(),
                self.transition.entry_ref.object.clone(),
                self.publication.head_ref.object.clone(),
            ],
            vec![self.reference.object.clone()],
        )
    }

    pub(super) fn remote_objects(
        &self,
    ) -> Result<Vec<ClosedRemoteObject>, MembershipMutationError> {
        self.candidate
            .merge_membership_resolution_remote_objects(
                &self.transition,
                &self.publication,
                &self.resolution,
                &self.reference,
            )
            .map_err(MembershipMutationError::from)
    }

    pub(super) fn validate_closed_shape(&self) -> Result<(), MembershipMutationError> {
        self.transition.validate()?;
        self.publication.validate()?;
        if !self.resolution.verify_signature()
            || self.reference
                != self
                    .resolution
                    .resolution_ref(self.reference.object.clone())
            || self.prepared_resolution().is_err()
            || self.transition.entry != self.publication.entry
            || self.transition.entry_ref != self.publication.entry_ref
            || self.candidate.commit.control()
                != Some(&store_commit::StoreControl {
                    transition: self.transition.transition.clone(),
                })
            || !matches!(
                &self.publication.entry.change,
                StoreAuthorityChange::ResolutionActivation { resolution }
                    if resolution == &self.reference
            )
            || !matches!(
                &self.publication.head.activation,
                membership::MembershipHeadActivation::StoreCommit { commit, .. }
                    if commit == &self.candidate.reference
            )
        {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership resolution plan violates its exact activation graph".to_string(),
            ));
        }
        self.remote_objects()?;
        Ok(())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedMembershipActivation {
    pub(super) transition: Box<PreparedMembershipTransition>,
    pub(super) candidate: Box<PreparedStoreOperationCommit>,
    pub(super) publication: Box<PreparedMembershipPublication>,
}

impl PreparedMembershipActivation {
    pub(super) fn entry(&self) -> &MembershipEntry {
        &self.publication.entry
    }

    pub(super) fn validate(&self) -> Result<(), MembershipMutationError> {
        self.transition.validate()?;
        self.publication.validate()?;
        self.candidate
            .validate_closed_shape()
            .map_err(MembershipMutationError::PreparedCommit)?;
        if self.transition.entry != self.publication.entry
            || self.transition.entry_ref != self.publication.entry_ref
            || self.candidate.commit.control()
                != Some(&store_commit::StoreControl {
                    transition: self.transition.transition.clone(),
                })
            || !self
                .transition
                .transition
                .matches_head(&self.publication.head, &self.publication.head_ref)
            || !matches!(&self.publication.head.activation, membership::MembershipHeadActivation::StoreCommit { commit, .. } if commit == &self.candidate.reference)
        {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership change differs from its exact Store activation graph".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn remote_objects(
        &self,
        wraps: &[PreparedWrappedStoreKey],
    ) -> Result<Vec<ClosedRemoteObject>, MembershipMutationError> {
        self.candidate
            .merge_membership_activation_remote_objects(&self.transition, &self.publication, wraps)
            .map_err(MembershipMutationError::from)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplacementWrappedKey {
    pub(super) prepared: PreparedWrappedStoreKey,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum MembershipMutationProgress {
    Pending,
    AdmissionGranted {
        join_info: CloudHomeJoinInfo,
    },
    AdmissionActivated {
        join_info: CloudHomeJoinInfo,
    },
    RevokeAccessRemoved,
    RevokeCandidateNonactivating {
        nonactivation: CandidateNonactivation,
    },
    ResolutionCandidateNonactivating {
        nonactivation: CandidateNonactivation,
    },
    RevokeActivated {
        candidate: StoreBatchCommitRef,
    },
    ResolutionActivated {
        candidate: StoreBatchCommitRef,
    },
}

impl MembershipMutationProgress {
    pub(super) fn encode(&self) -> Result<Vec<u8>, MembershipMutationError> {
        serde_json::to_vec(self).map_err(MembershipMutationError::Json)
    }
}

pub(super) struct MutationPersistence {
    database: StoreDatabase,
    storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
    intent_hash: ObjectHash,
}

impl MutationPersistence {
    pub(super) fn new(
        database: StoreDatabase,
        storage: std::sync::Arc<dyn CloudSyncObjectStorage>,
        intent_hash: ObjectHash,
    ) -> MutationPersistence {
        MutationPersistence {
            database,
            storage,
            intent_hash,
        }
    }

    pub(super) async fn record_progress(
        &self,
        progress: &MembershipMutationProgress,
    ) -> Result<(), MembershipMutationError> {
        let bytes = serde_json::to_vec(progress).map_err(MembershipMutationError::Json)?;
        self.database
            .update_membership_mutation_progress(self.intent_hash, bytes)
            .await?;
        Ok(())
    }

    pub(super) fn intent_hash(&self) -> ObjectHash {
        self.intent_hash
    }

    pub(super) async fn mark_remote_object_uploaded(
        &self,
        remote: RemoteObjectRecord,
    ) -> Result<(), MembershipMutationError> {
        self.database.mark_remote_object_uploaded(remote).await?;
        Ok(())
    }

    pub(super) async fn complete(&self) -> Result<(), MembershipMutationError> {
        self.database
            .complete_membership_mutation(self.intent_hash)
            .await?;
        Ok(())
    }

    pub(super) async fn finish_nonactivating_revoke(
        &self,
        plan: &RevokeMutationPlan,
    ) -> Result<(), MembershipMutationError> {
        let candidate = &plan.publication.candidate;
        let candidate_objects = plan.candidate_cleanup_objects();
        let cleanup = self
            .database
            .membership_candidate_cleanup_targets(
                self.intent_hash,
                candidate.reference.clone(),
                candidate_objects,
            )
            .await?;
        self.finish_nonactivating_revoke_with_targets(plan, cleanup)
            .await
    }

    async fn finish_nonactivating_revoke_with_targets(
        &self,
        plan: &RevokeMutationPlan,
        cleanup: Vec<coven_database::CandidateCleanupObject>,
    ) -> Result<(), MembershipMutationError> {
        let candidate = &plan.publication.candidate;
        match self
            .storage
            .set_member_access(plan.prior_access.clone())
            .await?
        {
            CloudAccessOutcome::Present(_) => {}
            CloudAccessOutcome::Absent(_) => {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "provider returned absent while restoring a nonactivated removal".to_string(),
                ));
            }
        }
        crate::sync::store::authorization::delete_candidate_cleanup_targets::<
            MembershipMutationError,
        >(self.storage.as_ref(), cleanup)
        .await?;
        let candidate_objects = plan.candidate_cleanup_objects();
        self.database
            .complete_nonactivating_membership_candidate_mutation(
                self.intent_hash,
                candidate.reference.clone(),
                candidate_objects,
                Vec::new(),
                Some(
                    EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                        .map_err(MembershipMutationError::Encryption)?
                        .current_generation(),
                ),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn finish_nonactivating_resolution(
        &self,
        plan: &ResolveMutationPlan,
    ) -> Result<(), MembershipMutationError> {
        let (candidate_objects, retained) = plan.candidate_cleanup_objects();
        let cleanup = self
            .database
            .membership_candidate_cleanup_targets(
                self.intent_hash,
                plan.candidate.reference.clone(),
                candidate_objects.iter().chain(&retained).cloned().collect(),
            )
            .await?;
        self.finish_nonactivating_resolution_with_targets(plan, cleanup)
            .await
    }

    async fn finish_nonactivating_resolution_with_targets(
        &self,
        plan: &ResolveMutationPlan,
        cleanup: Vec<coven_database::CandidateCleanupObject>,
    ) -> Result<(), MembershipMutationError> {
        let (candidate_objects, retained) = plan.candidate_cleanup_objects();
        crate::sync::store::authorization::delete_candidate_cleanup_targets::<
            MembershipMutationError,
        >(self.storage.as_ref(), cleanup)
        .await?;
        self.database
            .complete_nonactivating_membership_candidate_mutation(
                self.intent_hash,
                plan.candidate.reference.clone(),
                candidate_objects,
                retained,
                None,
            )
            .await?;
        Ok(())
    }
}

pub(super) fn decode_membership_mutation(
    row: DurableMembershipMutation,
) -> Result<(MembershipMutationPlan, MembershipMutationProgress), MembershipMutationError> {
    let plan = serde_json::from_slice(&row.plan_bytes).map_err(MembershipMutationError::Json)?;
    let progress =
        serde_json::from_slice(&row.progress_bytes).map_err(MembershipMutationError::Json)?;
    Ok((plan, progress))
}

pub(super) fn exact_owned_remote(
    remotes: &[ClosedRemoteObject],
    object: &ExactObjectRef,
) -> Result<ClosedRemoteObject, MembershipMutationError> {
    let mut matching = remotes.iter().filter(|remote| remote.object() == object);
    let remote = matching.next().cloned().ok_or_else(|| {
        MembershipMutationError::InvalidDurableMutation(format!(
            "membership candidate does not own exact object {}",
            object.slot().logical_key()
        ))
    })?;
    if matching.next().is_some() {
        return Err(MembershipMutationError::InvalidDurableMutation(format!(
            "membership candidate repeats exact object {}",
            object.slot().logical_key()
        )));
    }
    Ok(remote)
}
