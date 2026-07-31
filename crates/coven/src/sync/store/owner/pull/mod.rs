use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::verified_history::registration::*;
use super::verified_history::*;
use super::*;

mod authorized;
use crate::blob::decl::BlobDecls;
use crate::changeset::RowChange;
use crate::database::{BlobActivation, Database, DbError, VerifiedMergeMaterialization};
use crate::protocol::audience_package::{AudiencePackage, PackageAudience};
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{
    head_slot_prefix, CirclePackageRef, CommitFrontier, DeviceStreamAnchor, ObjectHash,
    OwnerRecoveryCursor, OwnerRecoveryPosition, ResolvedStoreDeviceState,
    RetainedStoreDeviceExclusionOutcome, RetainedStoreDeviceExclusionProposal,
    RetainedStoreDeviceOperations, RetainedVerifiedMergeHistorySummary,
    RetainedVerifiedRegistration, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceExclusionOutcome, StoreDeviceExclusionProof, StoreDeviceHead,
    StoreDeviceProposalAck, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};
use crate::protocol::{circle, membership, remote_object, store_commit};
use crate::storage::StoreObjectError;
use crate::storage::{
    BlobSpoolProtection, ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use crate::store_dir::StoreDir;
use crate::sync::apply::{resolve_and_apply_changeset_with_policy_on, ValidatedChangeset};
use crate::sync::conflict::{IncomingTimestampPolicy, TableSchema};
use crate::sync::session::SyncedTable;
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use crate::sync::store::retained_replay;
use crate::sync::store::StoreError;
use crate::sync::{gate, hlc};

mod device_lifecycle_state;
mod device_operations;
mod discovery;
mod join_activation;
mod local_device_operations;
mod materialization;
mod membership_control;
mod model;
mod owner_promotion;
mod replay;
mod root_validation;
mod snapshot_evidence;
mod support;

#[derive(Clone)]
pub(super) struct MergeCandidate {
    pub(super) candidate: Candidate,
    pub(super) activation_head: StoreDeviceHead,
    pub(super) activation_head_object: ExactObjectRef,
    pub(super) predecessor_membership: MembershipChain,
    pub(super) device_operations: VerifiedStoreDeviceOperations,
    pub(super) membership_control: Option<VerifiedCircleActivations>,
    pub(super) membership_prefix: VerifiedMergeMembershipPrefix,
}

pub(super) struct VerifiedPullCandidate {
    pub(super) verified: VerifiedStoreBatchCommit,
    pub(super) predecessor_membership: MembershipChain,
    pub(super) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub(super) operations: VerifiedStoreDeviceOperations,
    pub(super) membership_control: Option<VerifiedCircleActivations>,
}

pub(super) struct LoadedMergePredecessorMemberships {
    pub(super) by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
}

impl LoadedMergePredecessorMemberships {
    pub(super) fn membership_for(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<&MembershipChain, StorePullError> {
        self.by_commit.get(reference).ok_or_else(|| {
            StorePullError::Database(format!(
                "retained Merge commit {reference:?} has no loaded predecessor membership"
            ))
        })
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub(crate) struct StorePullExecution {
    pub result: StorePullResult,
    pub membership: MembershipChain,
}

pub(super) async fn execute(
    history: &mut AuthorizedStoreHistory<'_>,
    store_dir: &StoreDir,
    membership: &MembershipChain,
    identity: Option<&UserKeypair>,
    routing_encryption: Option<&crate::encryption::EncryptionService>,
) -> Result<StorePullExecution, StorePullError> {
    authorized::AuthorizedPull::load(history, store_dir, membership, identity, routing_encryption)
        .await?
        .execute()
        .await
}

pub(crate) use super::verification::{CommitCoverageError, LoadedDeviceJoinAttemptEvidence};
pub(crate) use device_lifecycle_state::*;
pub(crate) use device_operations::*;
pub(crate) use discovery::*;
pub(crate) use join_activation::*;
pub(crate) use local_device_operations::{
    derive_local_post_device_state, load_local_commit_device_operations,
};
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub(crate) use model::{
    commit_stream_id, Candidate, HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason,
    LoadedCirclePackage, LocalStoreMembership, StorePullError, StorePullFuture,
    StorePullMembershipError, StorePullResult, VerifiedStoreDeviceHead,
};
pub(crate) use owner_promotion::verify_merge_owner_promotion_acceptance_with_history;
pub(crate) use replay::{install_circle_bootstrap_image_on, replay_retained_merge_projection_on};
pub(crate) use root_validation::*;
pub(crate) use snapshot_evidence::*;
pub(crate) use support::{BlobDownloadFailure, BlobDownloadFailures, PullError};

#[cfg(test)]
mod tests;
