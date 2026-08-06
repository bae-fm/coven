use std::collections::BTreeMap;

use super::circles::CirclePackageReadError;
use super::verification::VerifiedMergeMembershipClosure;
use super::verified_history::registration::*;
use super::verified_history::*;
use super::*;

mod authorized;
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
pub(super) use authorized::AuthorizedPull;
use coven_database::DbError;
use coven_foundation::changeset::RowChange;
use coven_protocol::audience_package::{AudiencePackage, PackageAudience};
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{BlobSpoolProtection, ExactObjectRef, StorageError};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CirclePackageRef, CommitFrontier, ObjectHash,
    ResolvedStoreDeviceState, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistration, StoreDeviceStateRef, StoreHistoryCut,
    StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use coven_protocol::{circle, store_commit};

mod device_lifecycle_state;
mod discovery;
mod join_activation;
mod materialization;
mod membership_control;
mod model;
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
pub(crate) use coven_protocol::membership::{HeldStorePositionReason, LocalStoreMembership};
pub(crate) use device_lifecycle_state::*;
pub(crate) use discovery::*;
pub(crate) use join_activation::*;
pub use materialization::Readiness;
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub use model::LoadedCirclePackage;
pub(crate) use model::{
    commit_stream_id, Candidate, HeldStoreCoordinate, HeldStorePosition, StorePullMembershipError,
    VerifiedStoreDeviceHead,
};
pub use model::{StorePullError, StorePullResult};
pub(crate) use snapshot_evidence::*;
pub use support::PullError;
pub(crate) use support::{BlobDownloadFailure, BlobDownloadFailures};

#[cfg(test)]
mod tests;
