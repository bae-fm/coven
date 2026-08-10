use crate::*;

mod circle_coverage;
mod materialization_io;
mod retained_objects;
mod snapshot_retention;

use crate::{RetainedReplayAuthority, RetainedReplayBaseline};
use coven_protocol::audience_package::AudiencePackage;
use coven_protocol::blob::locator::{RemoteAudience, StoredBlobRef};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::membership::{AuthorHead, MembershipEntry};
use coven_protocol::remote_object::{
    remote_object_id, RemoteObjectRecord, RetainedReplayOwner, SharedLiveSetObjectDomain,
};
use coven_protocol::store_commit::{
    CommitFrontier, ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistrationRef,
};
use coven_protocol::write::{WriteId, WriteStatus};
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};

use super::candidate_records::PreparedMergeCandidate;
use super::materialization_models::{
    MergeRetractionCleanupInput, RetainedAudiencePackage, RetainedMergeMaterializationInput,
};
use super::store_device_state::load_store_device_snapshot_on;
use super::verified_store_authority::{VerifiedRegistrationLookup, VerifiedStoreLookup};
use super::*;
use crate::store::candidate_records::{
    load_author_exclusion_activation_locator_on, parse_prepared_merge_candidate_parts_on,
    verify_prepared_merge_candidate_parts,
};

pub(super) enum RetainedCommitAuthority<'a> {
    StoredBytes,
    Operation(&'a coven_protocol::store_commit::VerifiedStoreBatchCommit),
}

pub struct CircleReplayEpochIndex {
    pub control_epochs: BTreeMap<
        (
            coven_protocol::circle::CircleId,
            coven_protocol::circle::CircleControlCoord,
        ),
        coven_protocol::circle::CircleEpochId,
    >,
    pub cutoffs: BTreeMap<
        (
            coven_protocol::circle::CircleId,
            coven_protocol::circle::CircleEpochId,
        ),
        CommitFrontier,
    >,
}

pub struct CircleRestoreSelectionIndex {
    pub circles: Vec<(
        coven_protocol::circle::CircleId,
        Vec<coven_protocol::circle::CircleControlCoord>,
    )>,
    pub preserved_bootstraps: Vec<coven_protocol::circle::CircleBootstrapCoverageRef>,
}

impl CircleReplayEpochIndex {
    pub fn record_control(
        &mut self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::PreparedCircleControl,
    ) -> Result<(), DbError> {
        let control_key = (circle_id, control.coord.clone());
        match self.control_epochs.entry(control_key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(control.value.epoch_id());
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if *entry.get() == control.value.epoch_id() => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(DbError::Message(format!(
                    "Circle replay index maps one control for {circle_id} to conflicting epochs"
                )));
            }
        }
        let coven_protocol::circle::CircleEpochOrigin::Closed {
            closed_epoch_id,
            cutoff,
            ..
        } = &control.value.active_common().origin
        else {
            return Ok(());
        };
        match self.cutoffs.entry((circle_id, *closed_epoch_id)) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(cutoff.clone());
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == cutoff => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(DbError::Message(format!(
                    "Circle {circle_id} has conflicting cutoffs for epoch {closed_epoch_id}"
                )));
            }
        }
        Ok(())
    }

    pub fn include_verified_activations(
        &mut self,
        activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
    ) -> Result<(), DbError> {
        for activation in activations {
            self.record_control(activation.circle_id, &activation.control)?;
        }
        Ok(())
    }

    pub fn permits(
        &self,
        commit_ref: &StoreBatchCommitRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<bool, DbError> {
        let epoch_id = self
            .control_epochs
            .get(&(circle_id, control.clone()))
            .ok_or_else(|| {
                DbError::Message(format!(
                    "Circle package {} names an unretained control",
                    circle_id
                ))
            })?;
        let Some(cutoff) = self.cutoffs.get(&(circle_id, *epoch_id)) else {
            return Ok(true);
        };
        if cutoff.covers_commit(commit_ref) {
            Ok(true)
        } else if cutoff
            .0
            .get(&commit_ref.coord.stream_id)
            .is_some_and(|accepted| accepted.coord.sequence() == commit_ref.coord.sequence())
        {
            Err(DbError::Message(format!(
                "Circle package {} conflicts with its accepted epoch cutoff",
                circle_id
            )))
        } else {
            Ok(false)
        }
    }
}

impl StoreDatabase {}

#[cfg(test)]
mod circle_epoch_cutoff_tests;
