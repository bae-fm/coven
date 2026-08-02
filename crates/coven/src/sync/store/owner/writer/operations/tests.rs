use std::sync::Arc;

use super::*;
use crate::database::Database;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::store::owner::history::abandonment::{
    ExcludedCandidateHeadObservation, MergeCandidateAbandonment,
};
use crate::sync::test_helpers::{open_test_db, pubkey_hex, temp_store_dir, TestCustody, TestStore};

#[test]
fn store_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
    assert!(matches!(
        successor_store_sequence(u64::MAX),
        Err(StoreError::SequenceExhausted { current: u64::MAX })
    ));
}

#[path = "tests/merge_fixture.rs"]
mod merge_fixture;
use merge_fixture::*;

#[path = "tests/authorization.rs"]
mod authorization;
#[path = "tests/candidate_nonactivation.rs"]
mod candidate_nonactivation;
#[path = "tests/merge_publication.rs"]
mod merge_publication;
