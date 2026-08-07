use coven_database::DurableMembershipMutation;
use coven_database::StoreDatabase;
use coven_keys::encryption::EncryptionService;
use coven_protocol::membership::{
    self, MemberRole, MembershipChange, MembershipEntry, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
use coven_protocol::membership_mutation::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject};
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::remote_object::{CandidateNonactivation, RemoteObjectRecord};
use coven_protocol::store_commit::{self, ObjectHash, StoreBatchCommitRef};
use coven_protocol::wrapped_store_key::PreparedWrappedStoreKey;
use coven_storage::cloud::{CloudAccessOutcome, CloudAccessState, CloudHomeJoinInfo};
use coven_storage::SyncStorage;

use crate::sync::store::membership::InviteError;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    content = "plan",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum MembershipMutationPlan {
    Invite(InviteMutationPlan),
    Revoke(RevokeMutationPlan),
    Resolve(ResolveMutationPlan),
}

impl MembershipMutationPlan {
    pub(super) fn encode(&self) -> Result<Vec<u8>, InviteError> {
        serde_json::to_vec(self).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize plan: {error}"))
        })
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InviteMutationPlan {
    pub(super) publication: PreparedMembershipPublication,
    pub(super) invitee_pubkey: String,
    pub(super) invitee_email: Option<String>,
    pub(super) role: MemberRole,
    pub(super) desired_access: CloudAccessState,
    pub(super) wrapped_key: PreparedWrappedStoreKey,
}

impl InviteMutationPlan {
    pub(super) fn matches_request(
        &self,
        owner_pubkey: &str,
        invitee_pubkey: &str,
        invitee_email: Option<&str>,
        role: &MemberRole,
        store_id: &str,
    ) -> bool {
        self.publication.entry.author_pubkey == owner_pubkey
            && self.publication.entry.store_id == store_id
            && self.invitee_pubkey == invitee_pubkey
            && self.invitee_email.as_deref() == invitee_email
            && &self.role == role
            && self.desired_access
                == (CloudAccessState::Present {
                    member_pubkey: invitee_pubkey.to_string(),
                    provider_account_email: invitee_email.map(str::to_string),
                })
            && matches!(
                &self.publication.entry.change,
                MembershipChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role: entry_role,
                    ..
                } if user_pubkey == invitee_pubkey
                    && provider_account_email.as_deref() == invitee_email
                    && entry_role.role() == *role
            )
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevokeMutationPlan {
    pub(super) publication: RevokeMembershipPublication,
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
                MembershipChange::RemoveMember { user_pubkey, .. }
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

    pub(super) fn validate_closed_shape(&self) -> Result<(), InviteError> {
        let publication = self.publication.publication();
        publication.validate()?;
        let (desired_member, desired_email) = match &self.desired_access {
            CloudAccessState::Absent {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Present { .. } => {
                return Err(InviteError::InvalidDurableMutation(
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
                return Err(InviteError::InvalidDurableMutation(
                    "membership removal compensation requests absent provider access".to_string(),
                ));
            }
        };
        if desired_member != &self.revokee_pubkey
            || prior_member != &self.revokee_pubkey
            || desired_email != prior_email
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal access and compensation intents disagree".to_string(),
            ));
        }
        let MembershipChange::RemoveMember {
            user_pubkey,
            wrapped_keys,
            retirement_device_state,
            retirement_barriers,
            ..
        } = &publication.entry.change
        else {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal plan contains another change".to_string(),
            ));
        };
        let planned_wraps = self
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect::<Vec<_>>();
        if user_pubkey != &self.revokee_pubkey || wrapped_keys != &planned_wraps {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal plan differs from its exact entry".to_string(),
            ));
        }
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => {
                if retirement_device_state.is_some()
                    || retirement_barriers.values().any(|barrier| {
                        matches!(
                            barrier,
                            membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                        )
                    })
                    || !matches!(
                        publication.head.activation,
                        membership::MembershipHeadActivation::Direct
                    )
                {
                    return Err(InviteError::InvalidDurableMutation(
                        "direct membership removal carries Owner retirement authority".to_string(),
                    ));
                }
            }
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                ..
            } => {
                transition.validate()?;
                candidate
                    .validate_closed_shape()
                    .map_err(InviteError::InvalidDurableMutation)?;
                if transition.entry != publication.entry
                    || transition.entry_ref != publication.entry_ref
                    || transition.entry_object != publication.entry_object
                    || retirement_device_state.as_ref() != Some(&candidate.commit.device_state)
                    || !retirement_barriers.values().any(|barrier| {
                        matches!(
                            barrier,
                            membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                        )
                    })
                    || candidate.commit.control()
                        != Some(&store_commit::StoreControl {
                            transition: transition.transition.clone(),
                        })
                    || !transition
                        .transition
                        .matches_head(&publication.head, &publication.head_ref)
                    || !matches!(
                        &publication.head.activation,
                        membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
                {
                    return Err(InviteError::InvalidDurableMutation(
                        "Owner retirement differs from its exact Store activation graph"
                            .to_string(),
                    ));
                }
                candidate
                    .merge_membership_activation_remote_objects(
                        transition,
                        publication,
                        &self
                            .wraps
                            .iter()
                            .map(|wrap| wrap.prepared.clone())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            }
        }
        Ok(())
    }

    pub(super) fn candidate_remote_objects(
        &self,
    ) -> Result<Option<Vec<RemoteObjectRecord>>, InviteError> {
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => Ok(None),
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                publication,
            } => candidate
                .merge_membership_activation_remote_objects(
                    transition,
                    publication,
                    &self
                        .wraps
                        .iter()
                        .map(|wrap| wrap.prepared.clone())
                        .collect::<Vec<_>>(),
                )
                .map(Some)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string())),
        }
    }

    pub(super) fn candidate_cleanup_objects(&self) -> (Vec<ExactObjectRef>, Vec<ExactObjectRef>) {
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => (Vec::new(), Vec::new()),
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                publication,
            } => {
                let candidate_objects = std::iter::once(candidate.reference.object.clone())
                    .chain(std::iter::once(transition.entry_ref.object.clone()))
                    .chain(std::iter::once(publication.head_ref.object.clone()))
                    .chain(
                        self.wraps
                            .iter()
                            .map(|wrap| wrap.prepared.reference.object.clone()),
                    )
                    .collect();
                let retained = vec![candidate.head_ref().object];
                (candidate_objects, retained)
            }
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResolveMutationPlan {
    pub(super) resolution: StoreMembershipConflictResolution,
    pub(super) reference: StoreMembershipConflictResolutionRef,
    pub(super) resolution_object: PreparedExactObject,
    pub(super) transition: Box<PreparedMembershipTransition>,
    pub(super) candidate: Box<PreparedStoreOperationCommit>,
    pub(super) publication: Box<PreparedMembershipPublication>,
}

impl ResolveMutationPlan {
    pub(super) fn candidate_cleanup_objects(&self) -> (Vec<ExactObjectRef>, Vec<ExactObjectRef>) {
        (
            vec![
                self.candidate.reference.object.clone(),
                self.transition.entry_ref.object.clone(),
                self.publication.head_ref.object.clone(),
            ],
            vec![
                self.reference.object.clone(),
                self.candidate.head_ref().object,
            ],
        )
    }

    pub(super) fn remote_objects(&self) -> Result<Vec<RemoteObjectRecord>, InviteError> {
        self.candidate
            .merge_membership_resolution_remote_objects(
                &self.transition,
                &self.publication,
                &self.resolution,
                &self.reference,
                &self.resolution_object,
            )
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
    }

    pub(super) fn validate_closed_shape(&self) -> Result<(), InviteError> {
        self.transition.validate()?;
        self.publication.validate()?;
        if !self.resolution.verify_signature()
            || self.reference
                != self
                    .resolution
                    .resolution_ref(self.resolution_object.reference().clone())
            || self.resolution_object.stored_bytes()
                != serde_json::to_vec(&self.resolution).map_err(|error| {
                    InviteError::InvalidDurableMutation(format!(
                        "serialize membership resolution: {error}"
                    ))
                })?
            || self.transition.entry != self.publication.entry
            || self.transition.entry_ref != self.publication.entry_ref
            || self.transition.entry_object != self.publication.entry_object
            || self.candidate.commit.control()
                != Some(&store_commit::StoreControl {
                    transition: self.transition.transition.clone(),
                })
            || !matches!(
                &self.publication.entry.change,
                MembershipChange::ResolutionActivation { resolution }
                    if resolution == &self.reference
            )
            || !matches!(
                &self.publication.head.activation,
                membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.candidate.reference
            )
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution plan violates its exact activation graph".to_string(),
            ));
        }
        self.remote_objects()?;
        Ok(())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "activation", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RevokeMembershipPublication {
    Direct {
        publication: Box<PreparedMembershipPublication>,
    },
    StoreActivated {
        transition: Box<PreparedMembershipTransition>,
        candidate: Box<PreparedStoreOperationCommit>,
        publication: Box<PreparedMembershipPublication>,
    },
}

impl RevokeMembershipPublication {
    pub(super) fn publication(&self) -> &PreparedMembershipPublication {
        match self {
            Self::Direct { publication } | Self::StoreActivated { publication, .. } => publication,
        }
    }

    pub(super) fn entry(&self) -> &MembershipEntry {
        &self.publication().entry
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
    InviteGranted {
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
        candidate: Option<StoreBatchCommitRef>,
    },
    ResolutionActivated {
        candidate: StoreBatchCommitRef,
    },
}

impl MembershipMutationProgress {
    pub(super) fn encode(&self) -> Result<Vec<u8>, InviteError> {
        serde_json::to_vec(self).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize progress: {error}"))
        })
    }
}

pub(super) struct MutationPersistence {
    database: StoreDatabase,
    storage: std::sync::Arc<dyn SyncStorage>,
    intent_hash: ObjectHash,
}

impl MutationPersistence {
    pub(super) fn new(
        database: StoreDatabase,
        storage: std::sync::Arc<dyn SyncStorage>,
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
    ) -> Result<(), InviteError> {
        let bytes = serde_json::to_vec(progress).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize progress: {error}"))
        })?;
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
    ) -> Result<(), InviteError> {
        self.database.mark_remote_object_uploaded(remote).await?;
        Ok(())
    }

    pub(super) async fn record_direct_revoke_activation(
        &self,
        generation: u64,
    ) -> Result<(), InviteError> {
        self.database
            .record_direct_revoke_activation(
                self.intent_hash,
                MembershipMutationProgress::RevokeActivated { candidate: None }.encode()?,
                generation,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn adopt_candidate_head(
        &mut self,
        plan_bytes: Vec<u8>,
        previous: RemoteObjectRecord,
        replacement: RemoteObjectRecord,
        rotation_generation: Option<u64>,
    ) -> Result<(ObjectHash, ObjectHash), InviteError> {
        let previous_intent = self.intent_hash;
        let replacement_intent = self
            .database
            .adopt_merge_membership_candidate_head(
                previous_intent,
                plan_bytes,
                previous,
                replacement,
                rotation_generation,
            )
            .await?;
        self.intent_hash = replacement_intent;
        Ok((previous_intent, replacement_intent))
    }

    pub(super) async fn complete(&self) -> Result<(), InviteError> {
        self.database
            .complete_membership_mutation(self.intent_hash)
            .await?;
        Ok(())
    }

    pub(super) async fn finish_nonactivating_revoke(
        &self,
        plan: &RevokeMutationPlan,
    ) -> Result<(), InviteError> {
        let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication
        else {
            return Err(InviteError::InvalidDurableMutation(
                "direct membership removal has no candidate cleanup".to_string(),
            ));
        };
        let (candidate_objects, _) = plan.candidate_cleanup_objects();
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

    pub(super) async fn begin_nonactivating_revoke(
        &self,
        plan: &RevokeMutationPlan,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), InviteError> {
        let (candidate_objects, retained) = plan.candidate_cleanup_objects();
        let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication
        else {
            return Err(InviteError::InvalidDurableMutation(
                "direct membership removal has no candidate nonactivation".to_string(),
            ));
        };
        let progress = MembershipMutationProgress::RevokeCandidateNonactivating {
            nonactivation: nonactivation.clone().into_durable(),
        };
        let cleanup = self
            .database
            .begin_membership_candidate_nonactivation(
                self.intent_hash,
                candidate.reference.clone(),
                candidate_objects,
                retained,
                progress.encode()?,
                nonactivation,
            )
            .await?;
        self.finish_nonactivating_revoke_with_targets(plan, cleanup)
            .await
    }

    async fn finish_nonactivating_revoke_with_targets(
        &self,
        plan: &RevokeMutationPlan,
        cleanup: Vec<coven_database::CandidateCleanupObject>,
    ) -> Result<(), InviteError> {
        let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication
        else {
            return Err(InviteError::InvalidDurableMutation(
                "direct membership removal has no candidate terminalization".to_string(),
            ));
        };
        match self
            .storage
            .set_member_access(plan.prior_access.clone())
            .await?
        {
            CloudAccessOutcome::Present(_) => {}
            CloudAccessOutcome::Absent(_) => {
                return Err(InviteError::InvalidDurableMutation(
                    "provider returned absent while restoring a nonactivated removal".to_string(),
                ));
            }
        }
        crate::sync::store::authorization::delete_candidate_cleanup_targets::<InviteError>(
            self.storage.as_ref(),
            &self.database,
            cleanup,
        )
        .await?;
        let (candidate_objects, retained) = plan.candidate_cleanup_objects();
        self.database
            .complete_nonactivating_membership_candidate_mutation(
                self.intent_hash,
                candidate.reference.clone(),
                candidate_objects,
                retained,
                Some(
                    EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                        .map_err(|error| {
                            InviteError::Crypto(format!("parse rotated keyring: {error}"))
                        })?
                        .current_generation(),
                ),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn finish_nonactivating_resolution(
        &self,
        plan: &ResolveMutationPlan,
    ) -> Result<(), InviteError> {
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

    pub(super) async fn begin_nonactivating_resolution(
        &self,
        plan: &ResolveMutationPlan,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), InviteError> {
        let (candidate_objects, retained) = plan.candidate_cleanup_objects();
        let progress = MembershipMutationProgress::ResolutionCandidateNonactivating {
            nonactivation: nonactivation.clone().into_durable(),
        };
        let cleanup = self
            .database
            .begin_membership_candidate_nonactivation(
                self.intent_hash,
                plan.candidate.reference.clone(),
                candidate_objects,
                retained,
                progress.encode()?,
                nonactivation,
            )
            .await?;
        self.finish_nonactivating_resolution_with_targets(plan, cleanup)
            .await
    }

    async fn finish_nonactivating_resolution_with_targets(
        &self,
        plan: &ResolveMutationPlan,
        cleanup: Vec<coven_database::CandidateCleanupObject>,
    ) -> Result<(), InviteError> {
        let (candidate_objects, retained) = plan.candidate_cleanup_objects();
        crate::sync::store::authorization::delete_candidate_cleanup_targets::<InviteError>(
            self.storage.as_ref(),
            &self.database,
            cleanup,
        )
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
) -> Result<(MembershipMutationPlan, MembershipMutationProgress), InviteError> {
    let plan = serde_json::from_slice(&row.plan_bytes)
        .map_err(|error| InviteError::InvalidDurableMutation(format!("parse plan: {error}")))?;
    let progress = serde_json::from_slice(&row.progress_bytes)
        .map_err(|error| InviteError::InvalidDurableMutation(format!("parse progress: {error}")))?;
    Ok((plan, progress))
}

pub(super) fn exact_owned_remote(
    remotes: &[RemoteObjectRecord],
    object: &ExactObjectRef,
) -> Result<RemoteObjectRecord, InviteError> {
    let mut matching = remotes.iter().filter(|remote| remote.object() == object);
    let remote = matching.next().cloned().ok_or_else(|| {
        InviteError::InvalidDurableMutation(format!(
            "membership candidate does not own exact object {}",
            object.slot().logical_key()
        ))
    })?;
    if matching.next().is_some() {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership candidate repeats exact object {}",
            object.slot().logical_key()
        )));
    }
    Ok(remote)
}
