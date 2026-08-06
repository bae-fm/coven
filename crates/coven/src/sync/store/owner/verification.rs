use super::pull::*;
use super::verified_history::predecessor_verifies_owner;
use super::verified_history::registration::{
    device_state_has_active_registration, device_state_has_pending_proposal,
    registration_attempt_error, RegistrationLoadError,
};
use crate::database::{activated_merge_membership_remote_objects, MembershipAuthorityBytes};
use crate::storage::run_blocking_object_verification;
use crate::storage::SyncStorage;
use crate::sync::store::StoreError;
use coven_protocol::membership::{MembershipChain, MembershipChange, MembershipHeadRef};
use coven_protocol::objects::{
    decode_protocol_object, verify_store_root, StoreObjectError, VerifiedObject,
};
use coven_protocol::objects::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use coven_protocol::reclaim::{
    reclaim_authorization_semantic_prefix, reclaim_evidence_semantic_prefix,
    reclaim_receipt_semantic_prefix, ReclaimAuthorization, ReclaimAuthorizationRef,
    ReclaimEvidence, ReclaimReceipt, ReclaimReceiptRef,
};
use coven_protocol::remote_object;
use coven_protocol::store_commit::*;
use coven_protocol::store_commit::{
    ack_slot_prefix, device_exclusion_outcome_semantic_prefix,
    device_exclusion_proposal_semantic_prefix, device_join_attempt_semantic_prefix,
    device_join_outcome_semantic_prefix, founder_registration_semantic_prefix,
    package_semantic_prefix, provider_access_grant_semantic_prefix, registration_semantic_prefix,
    snapshot_slot_prefix, DeviceJoinAttemptRef, DeviceJoinOutcome, DeviceJoinOutcomeRef,
    SnapshotMeta, StoreAck, StoreAckRef, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposal, StoreDeviceExclusionProposalRef, StoreDeviceHeadRef,
    StoreSnapshotRef,
};
use std::collections::{BTreeMap, BTreeSet};

mod membership;

mod acknowledgements_snapshots;
mod announcements;
mod commits;
mod device_lifecycle;
mod registrations;
pub(crate) use membership::StoreMembershipObjectVerifier;

pub(super) enum DeviceStateResolver<'a> {
    Database(&'a crate::database::StoreDatabase),
    Loaded {
        genesis: &'a ResolvedStoreDeviceState,
        states: &'a BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    },
}

impl DeviceStateResolver<'_> {
    async fn resolve(
        &self,
        reference: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
        let state = match self {
            DeviceStateResolver::Database(database) => {
                return database
                    .resolved_store_device_state(reference)
                    .await
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()));
            }
            DeviceStateResolver::Loaded { genesis, states } => {
                let frontier = &reference.frontier().0;
                if frontier.is_empty() {
                    (*genesis).clone()
                } else {
                    ResolvedStoreDeviceState::merge(
                        frontier
                            .values()
                            .map(|commit| {
                                states.get(commit).cloned().ok_or_else(|| {
                                    RegistrationLoadError::Invalid(
                                        "device state references an unloaded predecessor snapshot"
                                            .to_string(),
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?
                }
            }
        };
        if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
            return Err(RegistrationLoadError::Invalid(
                "device state differs from its exact predecessor snapshots".to_string(),
            ));
        }
        Ok(state)
    }
}

pub(crate) struct StoreCommitVerifier<'a> {
    storage: &'a dyn SyncStorage,
    root: super::VerifiedStoreRoot,
    commits: BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit>,
}

pub(crate) struct VerifiedMergeMembershipClosure {
    objects: crate::database::VerifiedMergeMembershipObjects,
    remote_objects: Vec<remote_object::RemoteObjectRecord>,
    pub(crate) proof: RetainedMergeMembershipProof,
}

impl VerifiedMergeMembershipClosure {
    pub(super) fn objects(&self) -> &crate::database::VerifiedMergeMembershipObjects {
        &self.objects
    }

    pub(super) fn into_remote_objects(self) -> Vec<remote_object::RemoteObjectRecord> {
        self.remote_objects
    }
}

pub(crate) struct ExactAnnouncementPath {
    next_slot: coven_protocol::objects::ObjectSlot,
    accepted_head: Option<StoreDeviceHeadRef>,
    commits: Vec<StoreBatchCommitRef>,
}

#[derive(Debug)]
pub(crate) struct VerifiedReclaimAuthorization {
    pub(crate) authorization: VerifiedObject<ReclaimAuthorization>,
    pub(crate) evidence: VerifiedObject<ReclaimEvidence>,
}

#[derive(Debug)]
pub(crate) struct VerifiedReclaimReceipt {
    pub(crate) receipt: VerifiedObject<ReclaimReceipt>,
    pub(crate) executor: StoreDeviceRegistration,
}

impl<'a> StoreCommitVerifier<'a> {
    pub(super) fn store_root_hash(&self) -> ObjectHash {
        self.root.reference().store_root_hash
    }

    pub(crate) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'a> {
        StoreMembershipObjectVerifier::new(self)
    }

    pub(crate) async fn verified_merge_membership_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        let Some(StoreControl { transition }) = commit.control() else {
            return Ok(None);
        };
        let entry = self
            .membership_objects()
            .load_entry(&transition.body.entry)
            .await
            .map_err(StorePullError::Object)?;
        let coord = &transition.body.entry.coord;
        let loaded_head = self
            .membership_objects()
            .load_head_at_slot(
                &transition.head_slot,
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
            )
            .await
            .map_err(StorePullError::Object)?;
        let head_bytes = loaded_head.bytes;
        let head_object = loaded_head.object;
        let head = loaded_head.value;
        let head_ref = MembershipHeadRef {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
            object: head_object,
        };
        let objects = crate::database::VerifiedMergeMembershipObjects::verify(
            commit,
            commit_ref,
            &entry.value,
            &head,
            head_ref.clone(),
        )
        .map_err(StorePullError::Database)?;
        let family = commit.candidate_family();
        let resolution = match &entry.value.change {
            MembershipChange::ResolutionActivation { resolution } => Some(resolution.clone()),
            _ => None,
        };
        let resolution_loaded = if let Some(resolution) = &resolution {
            let loaded = self
                .membership_objects()
                .load_resolution(resolution)
                .await
                .map_err(StorePullError::Object)?;
            Some((loaded.bytes, loaded.value))
        } else {
            None
        };
        let remote_objects = activated_merge_membership_remote_objects(
            family,
            &objects,
            MembershipAuthorityBytes::new(entry.bytes.clone(), entry.bytes),
            MembershipAuthorityBytes::new(head_bytes.clone(), head_bytes),
            resolution_loaded
                .as_ref()
                .map(|(bytes, _)| MembershipAuthorityBytes::new(bytes.clone(), bytes.clone())),
            commit_ref,
        )
        .map_err(StorePullError::RemoteObject)?;
        let resolution_value = resolution_loaded.map(|(_, value)| value);
        let proof = RetainedMergeMembershipProof {
            commit: commit_ref.clone(),
            commit_value: commit.clone(),
            announcement: None,
            entry: transition.body.entry.clone(),
            entry_value: entry.value,
            head: head_ref,
            head_value: head,
            resolution,
            resolution_value,
        };
        Ok(Some(VerifiedMergeMembershipClosure {
            objects,
            remote_objects,
            proof,
        }))
    }

    pub(super) fn from_verified_root(
        _authority: super::HistoryConstructionAuthority,
        storage: &'a dyn SyncStorage,
        root: super::VerifiedStoreRoot,
    ) -> Self {
        Self {
            storage,
            root,
            commits: BTreeMap::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitCoverageError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
}

pub(crate) struct LoadedDeviceJoinAttemptEvidence {
    pub(crate) attempt: VerifiedObject<DeviceJoinAttempt>,
}
