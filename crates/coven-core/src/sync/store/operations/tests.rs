use std::sync::{Arc, RwLock};

use super::*;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::store::abandonment::{
    abandon_merge_candidate as store_abandon_merge_candidate,
    observe_excluded_candidate_head as store_observe_excluded_candidate_head,
    prepare_merge_candidate_abandonment as store_prepare_merge_candidate_abandonment,
    ExcludedCandidateHeadObservation, MergeCandidateAbandonment,
};
use crate::sync::store::preparation::prepare_store_write as store_prepare_merge_store_write;
use crate::sync::store::publication::drain_store_writes as store_drain_store_writes;
use crate::sync::test_helpers::{
    create_exact_test_store, host_exec, install_active_device_fixture, open_test_db,
    promote_active_member_fixture, pubkey_hex, temp_store_dir, TestCustody, TestStore,
};

async fn prepare_plan(
    db: &Database,
    storage: &dyn SyncStorage,
    candidate_membership: &crate::sync::membership::MembershipChain,
    device_id: &str,
    keypair: &UserKeypair,
) -> Result<StoreOperationCommitPlan, StoreError> {
    super::prepare_plan(
        &StoreDatabase::new(db),
        storage,
        candidate_membership,
        device_id,
        keypair,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_merge_store_write(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: &crate::sync::membership::MembershipChain,
) -> Result<bool, StoreError> {
    store_prepare_merge_store_write(
        &StoreDatabase::new(db),
        storage,
        device_id,
        timestamp,
        keypair,
        store_dir,
        membership,
    )
    .await
}

async fn drain_store_writes(db: &Database, storage: &dyn SyncStorage) -> Result<u64, StoreError> {
    store_drain_store_writes(&StoreDatabase::new(db), storage).await
}

async fn load_local_store_authority(
    db: &Database,
    expected_device_id: &str,
    identity_signer: &UserKeypair,
) -> Result<
    (
        StoreRootRef,
        StoreDeviceRegistrationRef,
        StoreDeviceRegistration,
        UserKeypair,
    ),
    StoreError,
> {
    super::load_local_store_authority(&StoreDatabase::new(db), expected_device_id, identity_signer)
        .await
}

async fn prepare_merge_conflict_resolution_commit(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    keypair: &UserKeypair,
    candidate_membership_heads: &[crate::sync::membership::MembershipHeadRef],
) -> Result<MergeConflictResolutionCommitPlan, StoreError> {
    super::prepare_merge_conflict_resolution_commit(
        &StoreDatabase::new(db),
        storage,
        device_id,
        keypair,
        candidate_membership_heads,
    )
    .await
}

async fn observe_excluded_candidate_head(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    candidate: &StoreDeviceHead,
    candidate_commit: &StoreBatchCommit,
    candidate_object: &ExactObjectRef,
) -> Result<ExcludedCandidateHeadObservation, StoreError> {
    let database = StoreDatabase::new(db);
    let root = database
        .local_store_root_ref()
        .await?
        .ok_or_else(|| StoreError::InvalidOutbound("test Store root is absent".to_string()))?;
    if root.store_root_hash != store_root_hash {
        return Err(StoreError::InvalidOutbound(
            "test candidate names another Store root".to_string(),
        ));
    }
    let mut commit_verifier =
        crate::sync::store::pull::StoreCommitVerifier::new(storage, &root).await?;
    let verified_commit = commit_verifier
        .authenticate_bytes(&candidate.commit, &candidate_commit.to_bytes())
        .await?;
    store_observe_excluded_candidate_head(
        &database,
        storage,
        &mut commit_verifier,
        candidate,
        &verified_commit,
        candidate_object,
    )
    .await
}

async fn prepare_merge_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<bool, StoreError> {
    store_prepare_merge_candidate_abandonment(
        &StoreDatabase::new(db),
        storage,
        device_id,
        identity_signer,
        write_id,
    )
    .await
}

async fn abandon_merge_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    store_abandon_merge_candidate(
        &StoreDatabase::new(db),
        storage,
        device_id,
        identity_signer,
        write_id,
    )
    .await
}

async fn publish_prepared_remote_objects(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: &crate::WriteId,
    store_root_hash: ObjectHash,
) -> Result<(), StoreError> {
    super::publish_prepared_remote_objects(
        &StoreDatabase::new(db),
        storage,
        write_id,
        store_root_hash,
    )
    .await
}

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
