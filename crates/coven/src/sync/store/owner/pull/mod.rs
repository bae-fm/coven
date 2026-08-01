use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::verified_history::registration::*;
use super::verified_history::*;
use super::*;

mod authorized;
use crate::changeset::RowChange;
use crate::database::BlobDecls;
use crate::database::{
    BlobActivation, Database, DbError, MergeMaterializationTransaction, ValidatedChangeset,
    VerifiedMergeMaterialization,
};
use crate::database::{IncomingTimestampPolicy, TableSchema};
use crate::protocol::audience_package::{AudiencePackage, PackageAudience};
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{
    CirclePackageRef, CommitFrontier, ObjectHash, OwnerRecoveryCursor, OwnerRecoveryPosition,
    ResolvedStoreDeviceState, RetainedVerifiedMergeHistorySummary, RetainedVerifiedRegistration,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationActivation, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut, StoreProtocolError,
    StoreRootRef, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::protocol::{circle, membership, remote_object, store_commit};
use crate::storage::StoreObjectError;
use crate::storage::{BlobSpoolProtection, ExactObjectRef, StorageError};
use crate::sync::hlc;
use crate::sync::session::SyncedTable;
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use crate::sync::store::StoreError;
pub(super) use authorized::AuthorizedPull;

mod device_lifecycle_state;
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

pub(super) enum MaterializedCheck {
    Yes,
    Missing,
    Held(HeldStorePositionReason),
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

use super::verification::DeviceStateResolver;
pub(crate) use super::verification::{CommitCoverageError, LoadedDeviceJoinAttemptEvidence};
pub(crate) use device_lifecycle_state::*;
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
