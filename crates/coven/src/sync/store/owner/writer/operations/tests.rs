use std::sync::Arc;

use super::*;
use crate::database::Database;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::store::owner::history::abandonment::{
    ExcludedCandidateHeadObservation, MergeCandidateAbandonment,
};
use crate::sync::test_helpers::{
    create_exact_test_store, host_exec, open_test_db, promote_active_member_fixture, pubkey_hex,
    temp_store_dir, TestCustody, TestStore,
};

async fn prepare_plan(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    keypair: &UserKeypair,
) -> Result<StoreOperationCommitPlan, StoreError> {
    let store =
        crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone(), keypair.clone())
            .await?;
    store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        .prepare_plan()
        .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_merge_store_write(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
) -> Result<bool, StoreError> {
    let store =
        crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone(), keypair.clone())
            .await?;
    let mut writer = store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    writer.prepare_pending_store_write(store_dir).await
}

async fn drain_store_writes(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    keypair: &UserKeypair,
) -> Result<u64, StoreError> {
    let store =
        crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone(), keypair.clone())
            .await?;
    let mut writer = store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    writer.drain_store_writes().await
}

async fn prepare_merge_conflict_resolution_commit(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    keypair: &UserKeypair,
    candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
) -> Result<super::super::MergeConflictResolutionCommitPlan, StoreError> {
    let store =
        crate::sync::store::Store::load(StoreDatabase::new(db), storage.clone(), keypair.clone())
            .await?;
    let mut writer = store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    writer
        .prepare_conflict_resolution_plan(candidate_membership_heads)
        .await
}

async fn prepare_merge_candidate_abandonment(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<bool, StoreError> {
    let store = crate::sync::store::Store::load(
        StoreDatabase::new(db),
        storage.clone(),
        identity_signer.clone(),
    )
    .await?;
    store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        .prepare_merge_candidate_abandonment(write_id)
        .await
}

async fn abandon_merge_candidate(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    let store = crate::sync::store::Store::load(
        StoreDatabase::new(db),
        storage.clone(),
        identity_signer.clone(),
    )
    .await?;
    store.abandon_merge_candidate(write_id).await
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
