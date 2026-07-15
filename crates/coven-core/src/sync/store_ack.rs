//! Durable append-only Store acknowledgement publication.

use super::storage::SyncStorage;
use super::store_commit::{ack_semantic_prefix, CommitFrontier, ObjectHash, StoreAck};
use super::store_objects::{append_and_verify, StoreObjectError};
use crate::database::Database;
use crate::keys::UserKeypair;

#[derive(Debug, thiserror::Error)]
pub enum StoreAckError {
    #[error("database: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store acknowledgement protocol state {0:?} is absent")]
    MissingState(&'static str),
    #[error("Store acknowledgement protocol state {key:?} is invalid: {reason}")]
    InvalidState { key: &'static str, reason: String },
    #[error("outbound Store acknowledgement is invalid: {0}")]
    InvalidOutbound(String),
}

impl From<crate::database::DbError> for StoreAckError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

pub async fn stage_store_ack(
    db: &Database,
    frontier: CommitFrontier,
    last_sync: String,
    signer: &UserKeypair,
) -> Result<StoreAck, StoreAckError> {
    if db.oldest_outbound_store_ack().await?.is_some() {
        return Err(StoreAckError::InvalidOutbound(
            "a prior acknowledgement remains queued".to_string(),
        ));
    }
    let store_root_hash = required_store_root_hash(db).await?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or(StoreAckError::MissingState(
            crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        ))?;
    let previous = db.latest_local_store_ack().await?;
    let revision = previous.as_ref().map_or(1, |(revision, _)| revision + 1);
    if frontier.policy() != db.write_policy() {
        return Err(StoreAckError::InvalidOutbound(format!(
            "acknowledgement frontier uses {:?}, database uses {:?}",
            frontier.policy(),
            db.write_policy()
        )));
    }
    let ack = StoreAck::signed(
        store_root_hash,
        device_id,
        revision,
        previous.map(|(_, hash)| hash),
        frontier,
        last_sync,
        signer,
    )
    .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
    db.stage_store_ack(ack.clone()).await?;
    Ok(ack)
}

pub async fn drain_outbound_store_acks(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreAckError> {
    let store_root_hash = required_store_root_hash(db).await?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or(StoreAckError::MissingState(
            crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        ))?;
    let mut published = 0_u64;
    while let Some(outbound) = db.oldest_outbound_store_ack().await? {
        let ack = StoreAck::parse_at(
            &outbound.ack_bytes,
            store_root_hash,
            &device_id,
            outbound.revision,
        )
        .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
        if ack.ack_hash() != outbound.ack_hash
            || ack.previous_ack_hash != outbound.previous_ack_hash
        {
            return Err(StoreAckError::InvalidOutbound(
                "stored acknowledgement columns differ from its exact signed bytes".to_string(),
            ));
        }
        append_and_verify(
            storage,
            &super::storage::ProtocolObjectContext::store(
                store_root_hash,
                super::storage::ProtocolObjectDomain::StoreAck,
            ),
            &ack_semantic_prefix(&device_id, outbound.revision, outbound.ack_hash),
            ".json",
            &outbound.ack_bytes,
        )
        .await?;
        db.complete_outbound_store_ack(outbound.revision, outbound.ack_hash)
            .await?;
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreAckError::Database("ack publish count exceeded u64".into()))?;
    }
    Ok(published)
}

async fn required_store_root_hash(db: &Database) -> Result<ObjectHash, StoreAckError> {
    db.required_store_root_hash_mapped(
        || StoreAckError::MissingState(crate::database::STORE_ROOT_HASH_STATE_KEY),
        |reason| StoreAckError::InvalidState {
            key: crate::database::STORE_ROOT_HASH_STATE_KEY,
            reason,
        },
        StoreAckError::from,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::CommitPosition;
    use crate::sync::store_objects::{list_latest_ack_chains, load_ack_slot};
    use crate::sync::test_helpers::{
        open_serial_test_db, open_test_db, publish_test_serial_store_protocol_root,
        publish_test_store_protocol_root,
    };

    async fn initialized(
        copy_source: &str,
    ) -> (
        InMemoryCloudHome,
        CloudSyncStorage,
        Database,
        UserKeypair,
        ObjectHash,
    ) {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "ack-store-test",
            signer.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)));
        let db = open_test_db();
        let store_root_hash = publish_test_store_protocol_root(
            &db,
            &storage,
            "ack-store-test",
            "dev-reader",
            &signer,
        )
        .await;
        (home, storage, db, signer, store_root_hash)
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_ack_error_variants() {
        let db = open_test_db();
        let signer = UserKeypair::generate();
        let frontier = CommitFrontier::MergeConcurrent(BTreeMap::new());

        assert!(matches!(
            stage_store_ack(
                &db,
                frontier.clone(),
                "2026-01-01T00:00:00Z".to_string(),
                &signer,
            )
            .await,
            Err(StoreAckError::MissingState(key))
                if key == crate::database::STORE_ROOT_HASH_STATE_KEY
        ));

        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            "not-an-object-hash",
        )
        .await
        .expect("write malformed Store root");
        assert!(matches!(
            stage_store_ack(
                &db,
                frontier,
                "2026-01-01T00:00:01Z".to_string(),
                &signer,
            )
            .await,
            Err(StoreAckError::InvalidState { key, reason })
                if key == crate::database::STORE_ROOT_HASH_STATE_KEY && !reason.is_empty()
        ));
    }

    #[tokio::test]
    async fn durable_ack_revisions_form_an_exact_append_only_chain() {
        let (_home, storage, db, signer, store_root_hash) = initialized("ack-chain").await;
        let first = stage_store_ack(
            &db,
            CommitFrontier::MergeConcurrent(BTreeMap::new()),
            "2026-01-01T00:00:00Z".to_string(),
            &signer,
        )
        .await
        .unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(first.previous_ack_hash, None);
        assert_eq!(drain_outbound_store_acks(&db, &storage).await.unwrap(), 1);

        let mut frontier = BTreeMap::new();
        frontier.insert(
            "dev-writer".to_string(),
            CommitPosition {
                seq: 3,
                commit_hash: ObjectHash::digest(b"writer-three"),
            },
        );
        let second = stage_store_ack(
            &db,
            CommitFrontier::MergeConcurrent(frontier.clone()),
            "2026-01-02T00:00:00Z".to_string(),
            &signer,
        )
        .await
        .unwrap();
        assert_eq!(second.revision, 2);
        assert_eq!(second.previous_ack_hash, Some(first.ack_hash()));
        assert_eq!(drain_outbound_store_acks(&db, &storage).await.unwrap(), 1);

        let chains = list_latest_ack_chains(&storage, store_root_hash)
            .await
            .expect("verify acknowledgement chains");
        let latest = &chains.latest_by_device["dev-reader"];
        assert_eq!(latest.value, second);
        assert_eq!(
            latest.value.frontier,
            CommitFrontier::MergeConcurrent(frontier)
        );
        assert_eq!(
            load_ack_slot(&storage, store_root_hash, "dev-reader", 1)
                .await
                .unwrap()
                .unwrap()
                .value,
            first,
        );
    }

    #[tokio::test]
    async fn serial_acknowledgement_carries_the_exact_global_position() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "serial-ack-store",
            signer.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("serial-ack")));
        let db = open_serial_test_db();
        let store_root_hash = publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "serial-ack-store",
            "serial-reader",
            &signer,
        )
        .await;
        let position = CommitPosition {
            seq: 7,
            commit_hash: ObjectHash::digest(b"serial-seven"),
        };

        let ack = stage_store_ack(
            &db,
            CommitFrontier::Serial(Some(position.clone())),
            "2026-07-14T00:00:00Z".to_string(),
            &signer,
        )
        .await
        .expect("stage Serial acknowledgement");
        assert_eq!(ack.frontier, CommitFrontier::Serial(Some(position.clone())));
        assert_eq!(drain_outbound_store_acks(&db, &storage).await.unwrap(), 1);
        let loaded = load_ack_slot(&storage, store_root_hash, "serial-reader", 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded.value.frontier,
            CommitFrontier::Serial(Some(position))
        );

        assert!(matches!(
            stage_store_ack(
                &db,
                CommitFrontier::MergeConcurrent(BTreeMap::new()),
                "2026-07-14T00:00:01Z".to_string(),
                &signer,
            )
            .await,
            Err(StoreAckError::InvalidOutbound(_))
        ));
    }

    #[tokio::test]
    async fn acknowledgement_append_failures_preserve_exact_outbox_for_retry() {
        for after_visibility in [false, true] {
            let (home, storage, db, signer, store_root_hash) = initialized(if after_visibility {
                "ack-after"
            } else {
                "ack-before"
            })
            .await;
            let ack = stage_store_ack(
                &db,
                CommitFrontier::MergeConcurrent(BTreeMap::new()),
                "2026-01-01T00:00:00Z".to_string(),
                &signer,
            )
            .await
            .unwrap();
            if after_visibility {
                home.fail_append_after_call(1);
            } else {
                home.fail_append_before_call(1);
            }
            assert!(drain_outbound_store_acks(&db, &storage).await.is_err());
            assert!(db.oldest_outbound_store_ack().await.unwrap().is_some());
            assert_eq!(
                db.latest_local_store_ack().await.unwrap(),
                Some((1, ack.ack_hash()))
            );
            assert_eq!(
                home.appended_keys()
                    .into_iter()
                    .filter(|key| key.starts_with("store-v1/acks/dev-reader/1/"))
                    .count(),
                usize::from(after_visibility),
            );

            assert_eq!(drain_outbound_store_acks(&db, &storage).await.unwrap(), 1);
            assert!(db.oldest_outbound_store_ack().await.unwrap().is_none());
            let loaded = load_ack_slot(&storage, store_root_hash, "dev-reader", 1)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.value, ack);
            assert_eq!(loaded.copies.len(), if after_visibility { 2 } else { 1 });
        }
    }

    #[tokio::test]
    async fn acknowledgement_local_completion_failure_retries_after_visible_copy() {
        let (home, storage, db, signer, store_root_hash) = initialized("ack-completion").await;
        let ack = stage_store_ack(
            &db,
            CommitFrontier::MergeConcurrent(BTreeMap::new()),
            "2026-01-01T00:00:00Z".to_string(),
            &signer,
        )
        .await
        .unwrap();
        db.call(|conn| {
            conn.execute_batch(
                "CREATE TEMP TRIGGER fail_ack_completion \
                 BEFORE DELETE ON outbound_store_acks \
                 BEGIN SELECT RAISE(ABORT, 'injected ack completion failure'); END;",
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
        assert!(matches!(
            drain_outbound_store_acks(&db, &storage).await,
            Err(StoreAckError::Database(_))
        ));
        assert_eq!(
            load_ack_slot(&storage, store_root_hash, "dev-reader", 1)
                .await
                .unwrap()
                .unwrap()
                .value,
            ack
        );
        assert!(db.oldest_outbound_store_ack().await.unwrap().is_some());

        db.call(|conn| {
            conn.execute_batch("DROP TRIGGER fail_ack_completion")
                .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
        assert_eq!(drain_outbound_store_acks(&db, &storage).await.unwrap(), 1);
        assert!(db.oldest_outbound_store_ack().await.unwrap().is_none());
        assert_eq!(
            home.appended_keys()
                .into_iter()
                .filter(|key| key.starts_with("store-v1/acks/dev-reader/1/"))
                .count(),
            2
        );
    }
}
