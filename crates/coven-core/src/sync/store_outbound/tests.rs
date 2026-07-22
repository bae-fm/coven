use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use super::*;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::storage::{CreateHeadError, ReplaceHeadError, VersionToken, VersionedObject};
use crate::sync::store_commit::serial_head_key;
use crate::sync::store_engine::merge::abandonment::{
    abandon_merge_candidate, observe_excluded_candidate_head, prepare_merge_candidate_abandonment,
    ExcludedCandidateHeadObservation, MergeCandidateAbandonment,
};
use crate::sync::store_engine::merge::preparation::prepare_store_write as prepare_merge_store_write;
use crate::sync::store_engine::merge::publication::drain_store_writes;
use crate::sync::store_engine::serial::abandonment::{
    abandon_serial_branch, prepare_serial_candidate_abandonment, SerialBranchAbandonment,
};
use crate::sync::store_engine::serial::publication::{
    current_serial_authorization, current_serial_head_ref,
    drain_store_writes as drain_serial_store_writes,
    prepare_serial_store_branch as prepare_serial_store_write,
};
use crate::sync::test_helpers::{
    create_exact_test_store, host_exec, install_active_device_fixture, open_serial_test_db,
    open_test_db, promote_active_member_fixture, pubkey_hex, temp_store_dir, test_migrations,
    test_synced_tables, TestCustody, TestStore,
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
#[path = "tests/serial.rs"]
mod serial;
use serial::{competing_head, serial_fixture};
