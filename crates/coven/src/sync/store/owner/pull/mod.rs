use std::collections::BTreeMap;

use super::circles::CirclePackageReadError;
use super::verification::VerifiedMergeMembershipClosure;
use super::verified_history::registration::*;
use super::verified_history::*;
use super::*;

mod authorized;
use crate::database::DbError;
use crate::protocol::audience_package::{AudiencePackage, PackageAudience};
use crate::protocol::membership::MembershipChain;
use crate::protocol::objects::StoreObjectError;
use crate::protocol::objects::{BlobSpoolProtection, ExactObjectRef, StorageError};
use crate::protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CirclePackageRef, CommitFrontier, ObjectHash,
    ResolvedStoreDeviceState, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistration, StoreDeviceStateRef, StoreHistoryCut,
    StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::protocol::{circle, store_commit};
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
pub(super) use authorized::AuthorizedPull;
use coven_foundation::changeset::RowChange;

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

pub(crate) struct VerifiedPullCandidate {
    pub(super) verified: VerifiedStoreBatchCommit,
    pub(super) predecessor_membership: MembershipChain,
    pub(super) registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub(super) operations: VerifiedStoreDeviceOperations,
    pub(super) membership_control: Option<VerifiedCircleActivations>,
}

pub(crate) struct LoadedMergePredecessorMemberships {
    pub(super) by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
}

pub(crate) enum MaterializedCheck {
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
            StorePullError::InvalidState(format!(
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
pub(crate) use crate::protocol::membership::{HeldStorePositionReason, LocalStoreMembership};
pub(crate) use device_lifecycle_state::*;
pub(crate) use discovery::*;
pub(crate) use join_activation::*;
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub(crate) use model::{
    commit_stream_id, Candidate, HeldStoreCoordinate, HeldStorePosition, LoadedCirclePackage,
    StorePullError, StorePullMembershipError, StorePullResult, VerifiedStoreDeviceHead,
};
pub(crate) use snapshot_evidence::*;
pub(crate) use support::{BlobDownloadFailure, BlobDownloadFailures, PullError};

#[cfg(test)]
mod tests;
