use crate::database::DurableMembershipMutation;
use crate::database::StoreDatabase;
use crate::keys::{self, UserKeypair};
use crate::protocol::membership::{
    self, AuthorHead, AuthorStreamId, MemberRole, MembershipChain, MembershipChange,
    MembershipEntry, MembershipEntryRef, MembershipError, MembershipHeadRef,
    MergeMembershipHeadTransition, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
use crate::protocol::remote_object::{CandidateNonactivation, RemoteObjectRecord};
use crate::protocol::store_commit::{self, ObjectHash, StoreBatchCommitRef};
use crate::protocol::wrapped_store_key::PreparedWrappedStoreKey;
use crate::storage::cloud::{CloudAccessState, CloudHomeJoinInfo};
use crate::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::store::operations::PreparedStoreOperationCommit;

use super::{validate_prepared_publication, validate_prepared_transition, InviteError};

/// Select the exact author stream without overwriting its committed prefix.
/// Streams are persisted per database, so independently restored devices use
/// different streams; copied state that reuses one exposes an immutable fork.
pub(super) async fn select_mutation_author_stream(
    database: &StoreDatabase,
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> Result<AuthorStreamId, InviteError> {
    let author = keys::public_key_hex(signer);
    let grant = chain
        .active_owner_grant(&author)
        .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
    let mut reusable = chain.reusable_author_streams(&author, &grant);
    if let Some(anchored) = chain.membership_stream_id(&grant) {
        reusable.insert(anchored);
    }
    Ok(database
        .select_membership_author_stream(&author, &grant, reusable)
        .await?)
}

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
        owner_keypair: &UserKeypair,
        invitee_pubkey: &str,
        invitee_email: Option<&str>,
        role: &MemberRole,
        store_id: &str,
    ) -> bool {
        self.publication.entry.author_pubkey == hex::encode(owner_keypair.public_key())
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
        owner_keypair: &UserKeypair,
        revokee_pubkey: &str,
        store_id: &str,
    ) -> bool {
        self.publication.entry().author_pubkey == hex::encode(owner_keypair.public_key())
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
        validate_prepared_publication(publication)?;
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
                validate_prepared_transition(transition)?;
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMembershipTransition {
    pub(crate) entry: MembershipEntry,
    pub(crate) entry_ref: MembershipEntryRef,
    pub(crate) entry_object: PreparedExactObject,
    pub(crate) transition: MergeMembershipHeadTransition,
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

pub(super) struct MutationPersistence<'a> {
    pub(super) database: &'a StoreDatabase,
    pub(super) intent_hash: ObjectHash,
}

impl MutationPersistence<'_> {
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

    pub(super) async fn complete(&self) -> Result<(), InviteError> {
        self.database
            .complete_membership_mutation(self.intent_hash)
            .await?;
        Ok(())
    }
}

pub(super) fn encode_membership_mutation(
    plan: &MembershipMutationPlan,
) -> Result<Vec<u8>, InviteError> {
    serde_json::to_vec(plan)
        .map_err(|error| InviteError::InvalidDurableMutation(format!("serialize plan: {error}")))
}

pub(super) fn encode_membership_progress(
    progress: &MembershipMutationProgress,
) -> Result<Vec<u8>, InviteError> {
    serde_json::to_vec(progress).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("serialize progress: {error}"))
    })
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
