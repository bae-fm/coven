use crate::database::*;

mod cache;
mod circle_coverage;
mod materialization_io;
mod retained_objects;
mod snapshot_retention;

use crate::database::{
    activated_merge_membership_remote_objects, MembershipAuthorityBytes,
    PreparedMergeMaterialization, PreparedMergeMaterializationPackage,
};
use crate::database::{RetainedReplayAuthority, RetainedReplayBaseline};
use crate::protocol::audience_package::AudiencePackage;
use crate::protocol::blob::locator::{RemoteAudience, StoredBlobRef};
use crate::protocol::circle_activation::VerifiedCircleActivations;
use crate::protocol::membership::{ApplyOutcome, HeldStorePositionReason, LocalStoreMembership};
use crate::protocol::membership::{AuthorHead, MembershipEntry};
use crate::protocol::objects::{ExactObjectRef, PreparedExactObject};
use crate::protocol::remote_object::{
    remote_object_id, RemoteObjectRecord, RetainedReplayOwner, SharedLiveSetObjectDomain,
};
use crate::protocol::store_commit::{
    CommitFrontier, ObjectHash, RetainedStoreDeviceRegistrationActivations, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
    StoreRootRef,
};
use crate::write::{WriteId, WriteStatus};
pub(crate) use cache::{
    CircleReplayEpochIndex, CircleRestoreSelectionIndex, RetainedMergeMaterializationCache,
};
use rusqlite::{Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::candidate_records::PreparedMergeCandidate;
use super::materialization_models::{
    MergeRetractionCleanupInput, RetainedAudiencePackage, RetainedCommitActivationInput,
    RetainedMergeMaterializationInput,
};
use super::store_device_state::load_store_device_snapshot_on;
use super::*;
use crate::database::store::candidate_records::{
    load_author_exclusion_activation_locator_on, parse_prepared_merge_candidate_parts_on,
};

impl StoreDatabase {}

#[cfg(test)]
mod circle_epoch_cutoff_tests;
