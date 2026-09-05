use std::collections::BTreeMap;

use super::circles::CirclePackageReadError;
use super::*;
use crate::sync::store::commit_verification::commit::VerifiedMergeMembershipClosure;
use crate::sync::store::commit_verification::merge_history::*;

mod authorized;
mod history;
pub(crate) use authorized::AuthorizedPull;
use coven_database::DbError;
use coven_foundation::changeset::RowChange;
use coven_protocol::audience_package::{AudiencePackage, PackageAudience};
use coven_protocol::circle_activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ExactObjectRef, StorageError};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CirclePackageRef, CommitFrontier, ObjectHash,
    ResolvedStoreDeviceState, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistration, StoreDeviceStateRef, StoreHistoryCut,
    StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use coven_protocol::{circle, store_commit};
pub(crate) use history::PullHistory;

mod device_join_bootstrap;
mod device_lifecycle_state;
mod discovery;
mod join_activation;
mod materialization;
mod membership_control;
mod model;
mod snapshot_evidence;
mod support;

#[derive(Clone)]
struct MergeCandidate {
    candidate: Candidate,
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    predecessor_membership: MembershipChain,
    device_operations: VerifiedStoreDeviceOperations,
    membership_control: Option<VerifiedCircleActivations>,
    membership_prefix: VerifiedMergeMembershipPrefix,
}

pub(crate) struct VerifiedPullCandidate {
    pub(crate) verified: VerifiedStoreBatchCommit,
    pub(crate) predecessor_membership: MembershipChain,
    pub(crate) registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub(crate) operations: VerifiedStoreDeviceOperations,
    pub(crate) membership_control: Option<VerifiedCircleActivations>,
}

pub(crate) struct LoadedMergePredecessorMemberships {
    by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
}

pub(crate) enum MaterializedCheck {
    Yes,
    Missing,
    Held(HeldStorePositionReason),
}

/// Whether a commit reference is the one currently materialized at its
/// position, held behind something, or absent from accepted history.
pub(crate) async fn materialized_reference_status(
    database: &coven_database::StoreDatabase,
    history: &mut MergeHistoryVerifier<'_>,
    coverage: &CommitFrontier,
    stream_id: &str,
    reference: &StoreBatchCommitRef,
) -> Result<MaterializedCheck, StorePullError> {
    if commit_stream_id(&reference.coord) != stream_id {
        return Ok(MaterializedCheck::Held(HeldStorePositionReason::WrongSlot(
            format!(
                "commit reference stream {} differs from dependency stream {stream_id}",
                commit_stream_id(&reference.coord)
            ),
        )));
    }
    if let Some(actual) = database
        .exact_materialized_ref(stream_id, reference.coord.sequence())
        .await?
    {
        if actual != *reference {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.to_string(),
                    referenced_commit: reference.clone(),
                    materialized_hash: actual.commit_hash,
                },
            ));
        }
        return Ok(MaterializedCheck::Yes);
    }
    Ok(history
        .covered_reference_status(coverage, stream_id, reference)
        .await)
}

impl LoadedMergePredecessorMemberships {
    fn membership_for(
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

pub(crate) use crate::sync::store::commit_verification::commit::CommitCoverageError;
pub(crate) use coven_protocol::membership::LocalStoreMembership;
pub(crate) use device_lifecycle_state::*;
pub(crate) use discovery::*;
pub(crate) use join_activation::*;
pub use materialization::Readiness;
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub use model::LoadedCirclePackage;
pub(crate) use model::{
    commit_stream_id, ApplyOutcome, Candidate, StorePullMembershipError, VerifiedStoreDeviceHead,
};
pub use model::{HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason};
pub use model::{StorePullError, StorePullResult};
pub(crate) use snapshot_evidence::*;
pub use support::PullError;

#[cfg(test)]
mod tests;
