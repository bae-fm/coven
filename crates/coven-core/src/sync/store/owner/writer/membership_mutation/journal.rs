use crate::database::DurableMembershipMutation;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{CloudAccessState, CloudHomeJoinInfo};
use crate::sync::membership::{
    AuthorHead, AuthorStreamId, MemberRole, MembershipChain, MembershipEntry, MembershipEntryRef,
    MembershipError, MembershipHeadRef, MergeMembershipHeadTransition,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use crate::sync::remote_object::{CandidateNonactivation, RemoteObjectRecord};
use crate::sync::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::operations::PreparedStoreOperationCommit;
use crate::sync::store_commit::{ObjectHash, StoreBatchCommitRef};
use crate::sync::wrapped_store_key::PreparedWrappedStoreKey;

use super::InviteError;

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
