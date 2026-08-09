use crate::*;

mod cache;
mod circle_coverage;
mod materialization_io;
mod retained_objects;
mod snapshot_retention;

use crate::{
    activated_merge_membership_remote_objects, MembershipAuthorityBytes,
    PreparedMergeMaterialization, PreparedMergeMaterializationPackage,
};
use crate::{RetainedReplayAuthority, RetainedReplayBaseline};
pub use cache::RetainedReplayCache;
pub(crate) use cache::{CircleReplayEpochIndex, CircleRestoreSelectionIndex};
use coven_protocol::audience_package::AudiencePackage;
use coven_protocol::blob::locator::{RemoteAudience, StoredBlobRef};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::membership::{ApplyOutcome, HeldStorePositionReason, LocalStoreMembership};
use coven_protocol::membership::{AuthorHead, MembershipEntry};
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject};
use coven_protocol::remote_object::{
    remote_object_id, RemoteObjectRecord, RetainedReplayOwner, SharedLiveSetObjectDomain,
};
use coven_protocol::store_commit::{
    CommitFrontier, ObjectHash, RetainedStoreDeviceRegistrationActivations, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
    StoreRootRef,
};
use coven_protocol::write::{WriteId, WriteStatus};
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
use crate::store::candidate_records::{
    load_author_exclusion_activation_locator_on, parse_prepared_merge_candidate_parts_on,
};

impl StoreDatabase {}

#[cfg(test)]
mod circle_epoch_cutoff_tests;
