//! Durable construction and ordered publication of local Store commits.

use super::membership::MembershipChain;
use super::storage::SyncStorage;
use super::store_commit::{
    commit_semantic_prefix, head_semantic_prefix, ObjectHash, StoreBatchCommit, StoreDeviceHead,
};
use super::store_objects::{append_and_verify, StoreObjectError};
use crate::database::{
    Database, PreparedStoreWrite, PreparedStoreWriteCommit, StoreBlobManifest,
    StoreWritePreparation,
};
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

#[derive(Debug, thiserror::Error)]
pub enum StoreOutboundError {
    #[error("database: {0}")]
    Database(String),
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("Store protocol state {key:?} is absent")]
    MissingState { key: &'static str },
    #[error("Store protocol state {key:?} is invalid: {reason}")]
    InvalidState { key: &'static str, reason: String },
    #[error("outbound Store row is invalid: {0}")]
    InvalidOutbound(String),
    #[error("outbound Store preparation failed: {0}")]
    Preparation(#[source] super::service::SyncCycleError),
    #[error("outbound blob {namespace}/{id} is local and cannot be published")]
    LocalUserBlob { namespace: String, id: String },
    #[error("outbound blob {namespace}/{id} is absent from storage")]
    MissingBlob { namespace: String, id: String },
    #[error("checking outbound blob {namespace}/{id}: {source}")]
    BlobStorage {
        namespace: String,
        id: String,
        source: super::storage::StorageError,
    },
}

impl From<crate::database::DbError> for StoreOutboundError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.0)
    }
}

/// Prepare the oldest pending write as exact signed bytes. A blocked or already
/// prepared oldest write holds later writes behind it.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_pending_store_write(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
    cancel: Option<&super::service::HostUploadCloud<'_>>,
) -> Result<bool, StoreOutboundError> {
    let Some(PreparedStoreWrite {
        write_id,
        changeset,
        dependencies,
        blob_facts,
    }) = db.prepare_store_write().await?
    else {
        return Ok(false);
    };
    let preparation = async {
        let payload = super::service::prepare_store_payload(
            db,
            storage,
            &blob_facts,
            keypair,
            store_dir,
            membership,
            cancel,
        )
        .await
        .map_err(StoreOutboundError::Preparation)?;
        let store_root_hash = store_root_hash(db).await?;
        let previous = db.latest_local_store_position().await?;
        let seq = previous
            .as_ref()
            .map_or(1, |position| position.seq.saturating_add(1));
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            write_id.clone(),
            device_id.to_string(),
            seq,
            previous.map(|position| position.commit_hash),
            dependencies,
            payload.membership_grant,
            db.schema_version(),
            &changeset,
            keypair,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let head = StoreDeviceHead::signed(
            store_root_hash,
            device_id.to_string(),
            Some(commit.position()),
            timestamp.to_string(),
            keypair,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        Ok::<_, StoreOutboundError>(StoreWritePreparation {
            write_id: write_id.clone(),
            package_bytes: changeset,
            commit,
            head,
            blob_manifest: payload.blob_manifest,
            local_cleanup: payload.local_cleanup,
            completion: payload.completion,
        })
    }
    .await;
    let preparation = match preparation {
        Ok(preparation) => preparation,
        Err(error) => {
            record_preparation_failure(db, &write_id, &error).await?;
            return Err(error);
        }
    };
    db.prepare_store_write_commit(preparation).await?;
    Ok(true)
}

/// Publish prepared writes in sequence order. Each attempt appends fresh physical
/// copies of the package, commit, and head; only a verified head allows the local
/// write's published position and completion bookkeeping to commit.
pub async fn drain_store_writes(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreOutboundError> {
    let mut published = 0_u64;
    while let Some(batch) = db.oldest_prepared_store_write().await? {
        let write_id = batch.commit.value.write_id.clone();
        db.set_write_status(&write_id, crate::WriteStatus::Publishing)
            .await?;
        let attempt = async {
            let store_root_hash = store_root_hash(db).await?;
            let device_id = db
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?
                .ok_or(StoreOutboundError::MissingState {
                    key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
                })?;
            validate_manifest(storage, &batch.blob_manifest).await?;
            validate_outbound(&batch, store_root_hash, &device_id)?;
            let commit = &batch.commit.value;
            let head = &batch.head.value;
            append_and_verify(
                storage,
                &commit.package.object_key,
                ".pkg",
                &batch.package_bytes,
            )
            .await?;
            append_and_verify(
                storage,
                &commit_semantic_prefix(&device_id, commit.seq, commit.commit_hash()),
                ".json",
                &batch.commit.bytes,
            )
            .await?;
            append_and_verify(
                storage,
                &head_semantic_prefix(&device_id, commit.seq, head.head_hash()),
                ".json",
                &batch.head.bytes,
            )
            .await?;
            db.complete_prepared_store_write(commit.position()).await?;
            Ok::<(), StoreOutboundError>(())
        }
        .await;
        if let Err(error) = attempt {
            let status = match blocked_status(&error) {
                Some(block) => crate::WriteStatus::Blocked(block),
                None => crate::WriteStatus::Pending,
            };
            db.set_write_status(&write_id, status).await?;
            return Err(error);
        }
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreOutboundError::Database("publish count exceeded u64".into()))?;
    }
    Ok(published)
}

fn blocked_status(error: &StoreOutboundError) -> Option<crate::WriteBlock> {
    match error {
        StoreOutboundError::Database(_) | StoreOutboundError::BlobStorage { .. } => None,
        StoreOutboundError::Object(StoreObjectError::Storage(_))
        | StoreOutboundError::Object(StoreObjectError::CandidateUnreadable { .. })
        | StoreOutboundError::Object(StoreObjectError::AppendReadbackMismatch { .. }) => None,
        StoreOutboundError::MissingBlob { namespace, id } => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::LocalUserBlob { namespace, id } => {
            Some(crate::WriteBlock::LocalUserBlob {
                namespace: namespace.clone(),
                id: id.clone(),
            })
        }
        StoreOutboundError::MissingState { key } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: format!("Store protocol state {key:?} is absent"),
        }),
        StoreOutboundError::InvalidState { key, reason } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: format!("Store protocol state {key:?} is invalid: {reason}"),
            })
        }
        StoreOutboundError::InvalidOutbound(_) | StoreOutboundError::Object(_) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Preparation(super::service::SyncCycleError::LocalUserBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::LocalUserBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::MissingBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::Gate(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::AssetScan(_)) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Preparation(super::service::SyncCycleError::AssetUpload(_)) => None,
    }
}

async fn record_preparation_failure(
    db: &Database,
    write_id: &crate::WriteId,
    error: &StoreOutboundError,
) -> Result<(), StoreOutboundError> {
    let Some(block) = blocked_status(error) else {
        return Ok(());
    };
    db.set_write_status(write_id, crate::WriteStatus::Blocked(block))
        .await
        .map_err(|status_error| {
            StoreOutboundError::Database(format!(
                "record blocked status for write {write_id} after {error}: {status_error}"
            ))
        })
}

fn validate_outbound(
    batch: &PreparedStoreWriteCommit,
    store_root_hash: ObjectHash,
    device_id: &str,
) -> Result<(), StoreOutboundError> {
    let commit = StoreBatchCommit::parse_at(
        &batch.commit.bytes,
        store_root_hash,
        device_id,
        batch.commit.value.seq,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if commit != batch.commit.value {
        return Err(StoreOutboundError::InvalidOutbound(
            "stored commit differs from its exact signed bytes".to_string(),
        ));
    }
    commit
        .verify_package(&batch.package_bytes)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreDeviceHead::parse_at(&batch.head.bytes, store_root_hash, device_id, commit.seq)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if head != batch.head.value || head.position.as_ref() != Some(&commit.position()) {
        return Err(StoreOutboundError::InvalidOutbound(
            "stored head differs from its exact signed bytes".to_string(),
        ));
    }
    Ok(())
}

async fn validate_manifest(
    storage: &dyn SyncStorage,
    manifest: &StoreBlobManifest,
) -> Result<(), StoreOutboundError> {
    for blob in &manifest.blobs {
        let exists = storage
            .blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
            .await
            .map_err(|source| StoreOutboundError::BlobStorage {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
                source,
            })?;
        if !exists {
            return Err(StoreOutboundError::MissingBlob {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
            });
        }
    }
    Ok(())
}

async fn store_root_hash(db: &Database) -> Result<ObjectHash, StoreOutboundError> {
    let raw = db
        .get_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: crate::database::STORE_ROOT_HASH_STATE_KEY,
        })?;
    raw.parse::<ObjectHash>()
        .map_err(|error| StoreOutboundError::InvalidState {
            key: crate::database::STORE_ROOT_HASH_STATE_KEY,
            reason: error.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::test_helpers::{
        host_exec, open_test_db, publish_test_store_protocol_root, temp_store_dir,
    };

    struct PreparedWriteFixture {
        home: InMemoryCloudHome,
        storage: CloudSyncStorage,
        db: Database,
        write_id: crate::WriteId,
        position_hash: ObjectHash,
    }

    async fn prepared_write_fixture(copy_source: &str) -> PreparedWriteFixture {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "outbound-crash-test",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)));
        let db = open_test_db();
        publish_test_store_protocol_root(
            &db,
            &storage,
            "outbound-crash-test",
            "dev-writer",
            &keypair,
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'outbound', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("prepare outbound write"));
        let batch = db
            .oldest_prepared_store_write()
            .await
            .expect("read prepared write")
            .expect("prepared write exists");
        PreparedWriteFixture {
            home,
            storage,
            db,
            write_id: batch.commit.value.write_id.clone(),
            position_hash: batch.commit.value.commit_hash(),
        }
    }

    fn count_prefix(home: &InMemoryCloudHome, prefix: &str) -> usize {
        home.appended_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .count()
    }

    #[tokio::test]
    async fn failures_before_package_commit_and_head_keep_the_exact_prepared_write_retryable() {
        for failed_call in 1..=3 {
            let fixture = prepared_write_fixture(&format!("before-{failed_call}")).await;
            fixture.home.fail_append_before_call(failed_call);
            let first = drain_store_writes(&fixture.db, &fixture.storage).await;
            assert!(first.is_err(), "append call {failed_call} fails");
            assert_eq!(
                fixture.db.write_status(&fixture.write_id).await.unwrap(),
                crate::WriteStatus::Pending,
                "transport failure returns the write to Pending",
            );
            assert!(
                fixture
                    .db
                    .oldest_prepared_store_write()
                    .await
                    .unwrap()
                    .is_some(),
                "the exact prepared write remains after append call {failed_call}",
            );
            assert_eq!(
                fixture
                    .db
                    .exact_materialized_hash("dev-writer", 1)
                    .await
                    .unwrap(),
                None,
                "local position cannot advance before a verified head",
            );
            assert_eq!(
                count_prefix(&fixture.home, "store-v1/packages/dev-writer/1/"),
                usize::from(failed_call > 1),
            );
            assert_eq!(
                count_prefix(&fixture.home, "store-v1/commits/dev-writer/1/"),
                usize::from(failed_call > 2),
            );
            assert_eq!(
                count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
                0,
            );

            assert_eq!(
                drain_store_writes(&fixture.db, &fixture.storage)
                    .await
                    .expect("retry exact outbound batch"),
                1,
            );
            assert!(fixture
                .db
                .oldest_prepared_store_write()
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                fixture
                    .db
                    .exact_materialized_hash("dev-writer", 1)
                    .await
                    .unwrap(),
                Some(fixture.position_hash),
            );
            assert!(matches!(
                fixture.db.write_status(&fixture.write_id).await.unwrap(),
                crate::WriteStatus::Published(position)
                    if position.device_id == "dev-writer"
                        && position.position.seq == 1
                        && position.position.commit_hash == fixture.position_hash
            ));
        }
    }

    #[tokio::test]
    async fn append_readback_mismatch_returns_the_prepared_write_to_pending() {
        let fixture = prepared_write_fixture("readback-mismatch").await;
        fixture.home.corrupt_append_readback_on_call(1);

        let result = drain_store_writes(&fixture.db, &fixture.storage).await;

        assert!(matches!(
            result,
            Err(StoreOutboundError::Object(
                StoreObjectError::AppendReadbackMismatch { .. }
            ))
        ));
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Pending,
            "a provider readback mismatch can be retried from the owned exact bytes",
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn ambiguous_failure_after_head_leaves_visible_head_and_retries_identical_bytes() {
        let fixture = prepared_write_fixture("after-head").await;
        fixture.home.fail_append_after_call(3);
        let first = drain_store_writes(&fixture.db, &fixture.storage).await;
        assert!(first.is_err());
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Pending,
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/packages/dev-writer/1/"),
            1
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/commits/dev-writer/1/"),
            1
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
            1
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            fixture
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            None
        );

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("retry ambiguous head append"),
            1
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/packages/dev-writer/1/"),
            2
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/commits/dev-writer/1/"),
            2
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
            2
        );
        let loaded = super::super::store_objects::load_commit_slot(
            &fixture.storage,
            store_root_hash(&fixture.db).await.unwrap(),
            "dev-writer",
            1,
        )
        .await
        .expect("coalesce retry copies")
        .expect("commit exists");
        assert_eq!(loaded.copies.len(), 2);
        assert_eq!(loaded.semantic_hash, fixture.position_hash);
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if position.position.seq == 1
                    && position.position.commit_hash == fixture.position_hash
        ));
    }

    #[tokio::test]
    async fn local_completion_failure_rolls_back_position_and_retries_after_visible_head() {
        let fixture = prepared_write_fixture("completion").await;
        fixture
            .db
            .call(|conn| {
                conn.execute_batch(
                    "CREATE TEMP TRIGGER fail_outbound_completion \
                     BEFORE UPDATE OF prepared ON store_writes \
                     WHEN OLD.prepared IS NOT NULL AND NEW.prepared IS NULL \
                     BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("install completion fault");
        let first = drain_store_writes(&fixture.db, &fixture.storage).await;
        assert!(matches!(first, Err(StoreOutboundError::Database(_))));
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Pending,
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
            1
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            fixture
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            None,
            "position and prepared-state clearing share the failed transaction",
        );

        fixture
            .db
            .call(|conn| {
                conn.execute_batch("DROP TRIGGER fail_outbound_completion")
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("remove completion fault");
        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("retry local completion"),
            1
        );
        assert_eq!(
            fixture
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            Some(fixture.position_hash),
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if position.position.seq == 1
                    && position.position.commit_hash == fixture.position_hash
        ));
    }

    #[tokio::test]
    async fn restart_blocks_a_prepared_write_when_its_store_root_is_unusable() {
        for invalid_root in [None, Some("not-an-object-hash")] {
            let temp = tempfile::tempdir().expect("temp dir");
            let path = temp.path().join("store.sqlite3");
            let open = || {
                Database::open(
                    &path,
                    crate::sync::test_helpers::test_synced_tables(),
                    crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                    crate::blob::TransferLimits::serial(),
                    "dev-writer".to_string(),
                    &crate::sync::test_helpers::test_migrations(),
                )
                .expect("open test database")
                .0
            };
            let home = InMemoryCloudHome::new();
            let keypair = UserKeypair::generate();
            let storage = CloudSyncStorage::new(
                Arc::new(home),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "prepared-root-status",
                keypair.clone(),
            )
            .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("root-status")));
            let db = open();
            publish_test_store_protocol_root(
                &db,
                &storage,
                "prepared-root-status",
                "dev-writer",
                &keypair,
            )
            .await;
            host_exec(
                &db,
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('root-status', 'outbound', NULL, 1, \
                         '0000000001000-0000-writer', '2026-01-01')",
            )
            .await;
            let (_store_temp, store_dir) = temp_store_dir();
            assert!(prepare_pending_store_write(
                &db,
                &storage,
                "dev-writer",
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                None,
                None,
            )
            .await
            .expect("prepare write"));
            let write_id = db
                .oldest_prepared_store_write()
                .await
                .expect("load prepared write")
                .expect("prepared write exists")
                .commit
                .value
                .write_id;
            db.call(move |conn| {
                match invalid_root {
                    Some(value) => conn.execute(
                        "UPDATE protocol_state SET value = ?2 WHERE key = ?1",
                        [crate::database::STORE_ROOT_HASH_STATE_KEY, value],
                    ),
                    None => conn.execute(
                        "DELETE FROM protocol_state WHERE key = ?1",
                        [crate::database::STORE_ROOT_HASH_STATE_KEY],
                    ),
                }
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("make root unusable");
            drop(db);

            let reopened = open();
            let result = drain_store_writes(&reopened, &storage).await;
            assert!(matches!(
                result,
                Err(StoreOutboundError::MissingState { .. })
                    | Err(StoreOutboundError::InvalidState { .. })
            ));
            assert!(matches!(
                reopened.write_status(&write_id).await.expect("write status"),
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { reason })
                    if reason.contains(crate::database::STORE_ROOT_HASH_STATE_KEY)
            ));
        }
    }
}
