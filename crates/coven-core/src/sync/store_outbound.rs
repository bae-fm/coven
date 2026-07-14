//! Durable construction and ordered publication of local Store commits.

use super::membership::MembershipChain;
use super::storage::SyncStorage;
use super::store_commit::{
    commit_semantic_prefix, head_semantic_prefix, ObjectHash, StoreBatchCommit, StoreDeviceHead,
};
use super::store_objects::{append_and_verify, StoreObjectError};
use crate::blob::{BlobScope, CacheFill, Provenance};
use crate::database::{
    Database, OutboundStoreBatch, PreparedStoreBatch, StoreBatchStage, StoreBlobManifest,
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

/// Turn the pending journal into exact signed bytes and move it atomically into
/// the ordered outbound queue. Empty journals leave no queue row.
#[allow(clippy::too_many_arguments)]
pub async fn stage_pending_store_batch(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
    cancel: Option<&super::service::HostUploadCloud<'_>>,
) -> Result<bool, StoreOutboundError> {
    let prepared = db.prepare_store_batch().await?;
    let PreparedStoreBatch::Prepared {
        max_pending_id,
        changeset,
        dependencies,
    } = prepared
    else {
        return Ok(false);
    };
    let payload = super::service::prepare_store_payload(
        db, storage, &changeset, keypair, store_dir, membership, cancel,
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let store_root_hash = store_root_hash(db).await?;
    let previous = db.latest_local_store_position().await?;
    let seq = previous
        .as_ref()
        .map_or(1, |position| position.seq.saturating_add(1));
    let commit = StoreBatchCommit::signed(
        store_root_hash,
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
    db.stage_store_batch(StoreBatchStage {
        max_pending_id,
        package_bytes: changeset,
        commit,
        head,
        blob_manifest: payload.blob_manifest,
        local_cleanup: payload.local_cleanup,
        completion: payload.completion,
    })
    .await?;
    Ok(true)
}

/// Publish queued batches in sequence order. Each attempt appends fresh physical
/// copies of the package, commit, and head; only a verified head allows the local
/// queue row and its completion bookkeeping to commit.
pub async fn drain_outbound_store_batches(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreOutboundError> {
    let store_root_hash = store_root_hash(db).await?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        })?;
    let mut published = 0_u64;
    while let Some(batch) = db.oldest_outbound_store_batch().await? {
        validate_manifest(db, storage, &batch.blob_manifest).await?;
        let (commit, head) = validate_outbound(&batch, store_root_hash, &device_id)?;
        append_and_verify(
            storage,
            &commit.package.object_key,
            ".pkg",
            &batch.package_bytes,
        )
        .await?;
        append_and_verify(
            storage,
            &commit_semantic_prefix(&device_id, batch.seq, batch.commit_hash),
            ".json",
            &batch.commit_bytes,
        )
        .await?;
        append_and_verify(
            storage,
            &head_semantic_prefix(&device_id, batch.seq, batch.head_hash),
            ".json",
            &batch.head_bytes,
        )
        .await?;
        db.complete_outbound_store_batch(batch.seq, batch.commit_hash)
            .await?;
        let _ = head;
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreOutboundError::Database("publish count exceeded u64".into()))?;
    }
    Ok(published)
}

fn validate_outbound(
    batch: &OutboundStoreBatch,
    store_root_hash: ObjectHash,
    device_id: &str,
) -> Result<(StoreBatchCommit, StoreDeviceHead), StoreOutboundError> {
    let commit =
        StoreBatchCommit::parse_at(&batch.commit_bytes, store_root_hash, device_id, batch.seq)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if commit.commit_hash() != batch.commit_hash
        || commit.previous_commit_hash != batch.previous_commit_hash
        || commit.dependencies != batch.dependencies
        || commit.package.content_hash != batch.package_hash
    {
        return Err(StoreOutboundError::InvalidOutbound(
            "stored commit columns differ from its exact signed bytes".to_string(),
        ));
    }
    commit
        .verify_package(&batch.package_bytes)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreDeviceHead::parse_at(&batch.head_bytes, store_root_hash, device_id, batch.seq)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if head.head_hash() != batch.head_hash || head.position.as_ref() != Some(&commit.position()) {
        return Err(StoreOutboundError::InvalidOutbound(
            "stored head columns differ from its exact signed bytes".to_string(),
        ));
    }
    Ok((commit, head))
}

async fn validate_manifest(
    db: &Database,
    storage: &dyn SyncStorage,
    manifest: &StoreBlobManifest,
) -> Result<(), StoreOutboundError> {
    for entry in &manifest.blobs {
        let scope = BlobScope::from_outbox_str(&entry.scope).ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(format!(
                "blob {}/{} has invalid scope {:?}",
                entry.namespace, entry.id, entry.scope
            ))
        })?;
        let provenance = match entry.provenance.as_str() {
            "user_provided" => Provenance::UserProvided,
            "host_provided" => Provenance::HostProvided,
            other => {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "blob {}/{} has invalid provenance {other:?}",
                    entry.namespace, entry.id
                )))
            }
        };
        let fill = match entry.fill.as_str() {
            "cache_eager" => CacheFill::CacheEager,
            "cache_lazy" => CacheFill::CacheLazy,
            other => {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "blob {}/{} has invalid fill {other:?}",
                    entry.namespace, entry.id
                )))
            }
        };
        if provenance == Provenance::UserProvided && db.external_blob(&entry.id).await?.is_some() {
            return Err(StoreOutboundError::LocalUserBlob {
                namespace: entry.namespace.clone(),
                id: entry.id.clone(),
            });
        }
        let blob = crate::blob::BlobRef {
            namespace: entry.namespace.clone(),
            id: entry.id.clone(),
            scope,
            cloud_path: entry.cloud_path.clone(),
            provenance,
            fill,
        };
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
                namespace: blob.namespace,
                id: blob.id,
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

    struct StagedBatch {
        home: InMemoryCloudHome,
        storage: CloudSyncStorage,
        db: Database,
        position_hash: ObjectHash,
    }

    async fn staged_batch(copy_source: &str) -> StagedBatch {
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
        assert!(stage_pending_store_batch(
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
        .expect("stage outbound batch"));
        let batch = db
            .oldest_outbound_store_batch()
            .await
            .expect("read staged batch")
            .expect("staged batch exists");
        StagedBatch {
            home,
            storage,
            db,
            position_hash: batch.commit_hash,
        }
    }

    fn count_prefix(home: &InMemoryCloudHome, prefix: &str) -> usize {
        home.appended_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .count()
    }

    #[tokio::test]
    async fn failures_before_package_commit_and_head_keep_the_exact_outbound_batch_retryable() {
        for failed_call in 1..=3 {
            let staged = staged_batch(&format!("before-{failed_call}")).await;
            staged.home.fail_append_before_call(failed_call);
            let first = drain_outbound_store_batches(&staged.db, &staged.storage).await;
            assert!(first.is_err(), "append call {failed_call} fails");
            assert!(
                staged
                    .db
                    .oldest_outbound_store_batch()
                    .await
                    .unwrap()
                    .is_some(),
                "the exact staged batch remains after append call {failed_call}",
            );
            assert_eq!(
                staged
                    .db
                    .exact_materialized_hash("dev-writer", 1)
                    .await
                    .unwrap(),
                None,
                "local position cannot advance before a verified head",
            );
            assert_eq!(
                count_prefix(&staged.home, "store-v1/packages/dev-writer/1/"),
                usize::from(failed_call > 1),
            );
            assert_eq!(
                count_prefix(&staged.home, "store-v1/commits/dev-writer/1/"),
                usize::from(failed_call > 2),
            );
            assert_eq!(
                count_prefix(&staged.home, "store-v1/heads/dev-writer/1/"),
                0,
            );

            assert_eq!(
                drain_outbound_store_batches(&staged.db, &staged.storage)
                    .await
                    .expect("retry exact outbound batch"),
                1,
            );
            assert!(staged
                .db
                .oldest_outbound_store_batch()
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                staged
                    .db
                    .exact_materialized_hash("dev-writer", 1)
                    .await
                    .unwrap(),
                Some(staged.position_hash),
            );
        }
    }

    #[tokio::test]
    async fn ambiguous_failure_after_head_leaves_visible_head_and_retries_identical_bytes() {
        let staged = staged_batch("after-head").await;
        staged.home.fail_append_after_call(3);
        let first = drain_outbound_store_batches(&staged.db, &staged.storage).await;
        assert!(first.is_err());
        assert_eq!(
            count_prefix(&staged.home, "store-v1/packages/dev-writer/1/"),
            1
        );
        assert_eq!(
            count_prefix(&staged.home, "store-v1/commits/dev-writer/1/"),
            1
        );
        assert_eq!(
            count_prefix(&staged.home, "store-v1/heads/dev-writer/1/"),
            1
        );
        assert!(staged
            .db
            .oldest_outbound_store_batch()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            staged
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            None
        );

        assert_eq!(
            drain_outbound_store_batches(&staged.db, &staged.storage)
                .await
                .expect("retry ambiguous head append"),
            1
        );
        assert_eq!(
            count_prefix(&staged.home, "store-v1/packages/dev-writer/1/"),
            2
        );
        assert_eq!(
            count_prefix(&staged.home, "store-v1/commits/dev-writer/1/"),
            2
        );
        assert_eq!(
            count_prefix(&staged.home, "store-v1/heads/dev-writer/1/"),
            2
        );
        let loaded = super::super::store_objects::load_commit_slot(
            &staged.storage,
            store_root_hash(&staged.db).await.unwrap(),
            "dev-writer",
            1,
        )
        .await
        .expect("coalesce retry copies")
        .expect("commit exists");
        assert_eq!(loaded.copies.len(), 2);
        assert_eq!(loaded.semantic_hash, staged.position_hash);
    }

    #[tokio::test]
    async fn local_completion_failure_rolls_back_position_and_retries_after_visible_head() {
        let staged = staged_batch("completion").await;
        staged
            .db
            .call(|conn| {
                conn.execute_batch(
                    "CREATE TEMP TRIGGER fail_outbound_completion \
                     BEFORE DELETE ON outbound_store_batches \
                     BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("install completion fault");
        let first = drain_outbound_store_batches(&staged.db, &staged.storage).await;
        assert!(matches!(first, Err(StoreOutboundError::Database(_))));
        assert_eq!(
            count_prefix(&staged.home, "store-v1/heads/dev-writer/1/"),
            1
        );
        assert!(staged
            .db
            .oldest_outbound_store_batch()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            staged
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            None,
            "position and queue deletion share the failed transaction",
        );

        staged
            .db
            .call(|conn| {
                conn.execute_batch("DROP TRIGGER fail_outbound_completion")
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("remove completion fault");
        assert_eq!(
            drain_outbound_store_batches(&staged.db, &staged.storage)
                .await
                .expect("retry local completion"),
            1
        );
        assert_eq!(
            staged
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            Some(staged.position_hash),
        );
    }
}
