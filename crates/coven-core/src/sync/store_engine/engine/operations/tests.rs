use std::sync::{Arc, RwLock};

use super::*;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::store_engine::engine::abandonment::{
    abandon_merge_candidate, observe_excluded_candidate_head, prepare_merge_candidate_abandonment,
    ExcludedCandidateHeadObservation, MergeCandidateAbandonment,
};
use crate::sync::store_engine::engine::preparation::prepare_store_write as prepare_merge_store_write;
use crate::sync::store_engine::engine::publication::drain_store_writes;
use crate::sync::test_helpers::{
    create_exact_test_store, host_exec, install_active_device_fixture, open_test_db,
    promote_active_member_fixture, pubkey_hex, temp_store_dir, TestCustody, TestStore,
};

#[path = "tests/common.rs"]
mod common;
use common::*;

#[path = "tests/merge_fixture.rs"]
mod merge_fixture;
use merge_fixture::*;

#[path = "tests/authorization.rs"]
mod authorization;
#[path = "tests/candidate_nonactivation.rs"]
mod candidate_nonactivation;
#[path = "tests/merge_publication.rs"]
mod merge_publication;
