use coven_database::DurableMembershipMutation;
use coven_database::StoreDatabase;
use coven_protocol::membership::{self, MemberRole, StoreAuthorityChange};
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::remote_object::{ClosedRemoteObject, RemoteObjectRecord};
use coven_protocol::store_commit::{ObjectHash, StoreBatchCommitRef};
use coven_protocol::wrapped_store_key::PreparedWrappedStoreKey;
use coven_storage::cloud::{CloudAccessState, CloudHomeJoinInfo};

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
    Resolve(Box<PreparedStoreOperationCommit>),
}

impl MembershipMutationPlan {
    pub(super) fn encode(&self) -> Result<Vec<u8>, MembershipMutationError> {
        serde_json::to_vec(self).map_err(MembershipMutationError::Json)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdmissionMutationPlan {
    pub(super) candidate: Box<PreparedStoreOperationCommit>,
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
    ) -> Result<bool, MembershipMutationError> {
        let publication = self.candidate.prepared_membership_publication()?;
        Ok(publication.entry.author_pubkey == owner_pubkey
            && publication.entry.store_id == store_id
            && self.member_pubkey == member_pubkey
            && self.member_email.as_deref() == member_email
            && &self.role == role
            && self.desired_access
                == (CloudAccessState::Present {
                    member_pubkey: member_pubkey.to_string(),
                    provider_account_email: member_email.map(str::to_string),
                })
            && matches!(
                &publication.entry.change,
                StoreAuthorityChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role: entry_role,
                    ..
                } if user_pubkey == member_pubkey
                    && provider_account_email.as_deref() == member_email
                    && entry_role.role() == *role
            ))
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevokeMutationPlan {
    pub(super) candidate: Box<PreparedStoreOperationCommit>,
    pub(super) revokee_pubkey: String,
    pub(super) desired_access: CloudAccessState,
    pub(super) wraps: Vec<ReplacementWrappedKey>,
    pub(super) keyring_payload: Vec<u8>,
}

impl RevokeMutationPlan {
    pub(super) fn matches_request(
        &self,
        owner_pubkey: &str,
        revokee_pubkey: &str,
        store_id: &str,
    ) -> Result<bool, MembershipMutationError> {
        let publication = self.candidate.prepared_membership_publication()?;
        Ok(publication.entry.author_pubkey == owner_pubkey
            && publication.entry.store_id == store_id
            && self.revokee_pubkey == revokee_pubkey
            && matches!(
                &publication.entry.change,
                StoreAuthorityChange::RemoveMember { user_pubkey, .. }
                    if user_pubkey == revokee_pubkey
            )
            && matches!(
                &self.desired_access,
                CloudAccessState::Absent { member_pubkey, .. }
                    if member_pubkey == revokee_pubkey
            ))
    }

    pub(super) fn validate_closed_shape(&self) -> Result<(), MembershipMutationError> {
        let publication = self.candidate.prepared_membership_publication()?;
        if !matches!(
            &self.desired_access,
            CloudAccessState::Absent { member_pubkey, .. }
                if member_pubkey == &self.revokee_pubkey
        ) {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership removal access differs from its requested member".to_string(),
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
        let retires_owner = retirement_barriers.values().any(|barrier| {
            matches!(
                barrier,
                membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
            )
        });
        if retires_owner != retirement_device_state.is_some()
            || retirement_device_state
                .as_ref()
                .is_some_and(|state| state != &self.candidate.commit.device_state)
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
        Ok(self.candidate.merge_membership_activation_remote_objects(
            &self
                .wraps
                .iter()
                .map(|wrap| wrap.prepared.clone())
                .collect::<Vec<_>>(),
        )?)
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
    AdmissionGranted { join_info: CloudHomeJoinInfo },
    AdmissionActivated { join_info: CloudHomeJoinInfo },
    RevokeAccessRemoved,
    RevokeActivated { candidate: StoreBatchCommitRef },
    ResolutionActivated { candidate: StoreBatchCommitRef },
}

impl MembershipMutationProgress {
    pub(super) fn encode(&self) -> Result<Vec<u8>, MembershipMutationError> {
        serde_json::to_vec(self).map_err(MembershipMutationError::Json)
    }
}

pub(super) struct MutationPersistence {
    database: StoreDatabase,
    intent_hash: ObjectHash,
}

impl MutationPersistence {
    pub(super) fn new(database: StoreDatabase, intent_hash: ObjectHash) -> MutationPersistence {
        MutationPersistence {
            database,
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
