use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use super::*;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::storage::VersionedObject;
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
