use std::collections::BTreeMap;

use super::verification::VerifiedMergeMembershipClosure;
use super::verified_history::registration::*;
use super::verified_history::*;
use super::*;

mod authorized;
use crate::changeset::RowChange;
use crate::database::{DbError, ValidatedChangeset};
use crate::protocol::audience_package::{AudiencePackage, PackageAudience};
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CirclePackageRef, CommitFrontier, ObjectHash,
    ResolvedStoreDeviceState, RetainedVerifiedMergeHistorySummary, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut, StoreProtocolError,
    StoreRootRef, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::protocol::{circle, remote_object, store_commit};
use crate::storage::StoreObjectError;
use crate::storage::{BlobSpoolProtection, ExactObjectRef, StorageError};
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
pub(super) use authorized::AuthorizedPull;

mod device_lifecycle_state;
mod discovery;
mod join_activation;
mod materialization;
mod membership_control;
mod model;
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
    pub(super) registrations: Vec<ActivatedStoreDeviceRegistration>,
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

pub(crate) use super::verification::{CommitCoverageError, LoadedDeviceJoinAttemptEvidence};
pub(crate) use device_lifecycle_state::*;
pub(crate) use discovery::*;
pub(crate) use join_activation::*;
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub(crate) use model::{
    commit_stream_id, Candidate, HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason,
    LoadedCirclePackage, LocalStoreMembership, StorePullError, StorePullMembershipError,
    StorePullResult, VerifiedStoreDeviceHead,
};
pub(crate) use root_validation::*;
pub(crate) use snapshot_evidence::*;
pub(crate) use support::{BlobDownloadFailure, BlobDownloadFailures, PullError};

#[cfg(test)]
mod tests;
