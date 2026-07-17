//! Durable exact Store acknowledgement publication.

use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    ack_slot_prefix, CommitFrontier, DeviceStreamAnchor, StoreAck, StoreHistoryCut,
    StoreSerialPredecessor, StreamActivationId, SuccessorLink,
};
use super::store_objects::StoreObjectError;
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
    #[error("exact Store root authority is absent")]
    ExactRootAuthorityMissing,
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
    storage: &dyn SyncStorage,
    frontier: CommitFrontier,
    last_sync: String,
    signer: &UserKeypair,
) -> Result<StoreAck, StoreAckError> {
    if db.oldest_outbound_store_ack().await?.is_some() {
        return Err(StoreAckError::InvalidOutbound(
            "a prior acknowledgement remains queued".to_string(),
        ));
    }
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or(StoreAckError::MissingState(
            crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        ))?;
    let (root, registration_ref, registration, device_signer) =
        super::store_outbound::load_local_store_authority(db, &device_id, signer)
            .await
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
    if frontier.policy() != db.write_policy() {
        return Err(StoreAckError::InvalidOutbound(format!(
            "acknowledgement frontier uses {:?}, database uses {:?}",
            frontier.policy(),
            db.write_policy()
        )));
    }
    let history_cut = match frontier {
        CommitFrontier::MergeConcurrent(commits) => StoreHistoryCut::merge_concurrent(commits),
        CommitFrontier::Serial(Some(commit)) => {
            StoreHistoryCut::serial(StoreSerialPredecessor::Commit(commit))
        }
        CommitFrontier::Serial(None) => {
            if !matches!(
                registration.origin,
                super::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
            ) {
                return Err(StoreAckError::InvalidOutbound(
                    "only the exact founder registration can acknowledge Serial genesis"
                        .to_string(),
                ));
            }
            StoreHistoryCut::serial(StoreSerialPredecessor::Genesis {
                root: root.clone(),
                founder_registration: registration_ref.clone(),
            })
        }
    };
    let previous = db.latest_local_store_ack().await?;
    let (revision, predecessor, current_slot) = match previous {
        Some(previous) => (
            previous.reference.revision.checked_add(1).ok_or_else(|| {
                StoreAckError::InvalidOutbound(
                    "Store acknowledgement revision overflow".to_string(),
                )
            })?,
            Some(previous.reference),
            previous.successor_slot,
        ),
        None => (1, None, acknowledgement_first_slot(&registration)?.clone()),
    };
    let context =
        ProtocolObjectContext::store(root.store_root_hash, ProtocolObjectDomain::StoreAck);
    let semantic_prefix = ack_slot_prefix(&device_id, revision);
    let next_slot = storage
        .allocate_protocol_slot(
            &context,
            &ack_slot_prefix(
                &device_id,
                revision.checked_add(1).ok_or_else(|| {
                    StoreAckError::InvalidOutbound(
                        "Store acknowledgement revision overflow".to_string(),
                    )
                })?,
            ),
            ".json",
        )
        .await
        .map_err(StoreObjectError::from)?;
    let ack = StoreAck::signed(
        root.store_root_hash,
        registration_ref.clone(),
        revision,
        predecessor.clone(),
        history_cut,
        last_sync,
        SuccessorLink {
            activation: StreamActivationId::store_acknowledgements(&root, &registration_ref),
            predecessor: predecessor.map(|reference| reference.object),
            next_slot,
        },
        &device_signer,
    )
    .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
    let prepared = storage
        .prepare_protocol_object(&context, current_slot, &semantic_prefix, ack.to_bytes())
        .map_err(StoreObjectError::from)?;
    db.stage_store_ack(ack.clone(), prepared).await?;
    Ok(ack)
}

pub async fn drain_outbound_store_acks(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreAckError> {
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(StoreAckError::ExactRootAuthorityMissing)?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or(StoreAckError::MissingState(
            crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        ))?;
    let context =
        ProtocolObjectContext::store(root.store_root_hash, ProtocolObjectDomain::StoreAck);
    let mut published = 0_u64;
    while let Some(outbound) = db.oldest_outbound_store_ack().await? {
        storage
            .create_protocol_object(&outbound.ack.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let opened = storage
            .read_protocol_object(
                &context,
                &outbound.reference.object,
                &ack_slot_prefix(&device_id, outbound.reference.revision),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if opened != outbound.ack.bytes {
            return Err(StoreAckError::InvalidOutbound(
                "Store acknowledgement exact readback differs from prepared bytes".to_string(),
            ));
        }
        db.complete_outbound_store_ack(outbound.reference).await?;
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreAckError::Database("ack publish count exceeded u64".into()))?;
    }
    Ok(published)
}

fn acknowledgement_first_slot(
    registration: &super::store_commit::StoreDeviceRegistration,
) -> Result<&crate::storage::cloud::ObjectSlot, StoreAckError> {
    match &registration.acknowledgements {
        DeviceStreamAnchor::StoreAcknowledgements { first_slot } => Ok(first_slot),
        _ => Err(StoreAckError::InvalidOutbound(
            "local Store registration has no acknowledgement stream anchor".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};

    fn open(path: &Path, device_id: &str) -> Database {
        Database::open(
            path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::MergeConcurrent,
            device_id.to_string(),
            &crate::sync::test_helpers::test_migrations(),
        )
        .expect("open acknowledgement test database")
        .0
    }

    fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "ack-exact-store",
            signer.clone(),
        )
        .expect("construct acknowledgement test storage")
    }

    async fn initialize(db: &Database, storage: &CloudSyncStorage, signer: &UserKeypair) {
        super::super::store_protocol_root::create_store(db, storage, "ack-exact-store", signer)
            .await
            .expect("create acknowledgement test Store");
        super::super::store_registration::ensure_active_registration(db, storage, signer)
            .await
            .expect("activate acknowledgement test registration");
    }

    async fn stage(db: &Database, storage: &CloudSyncStorage, signer: &UserKeypair) -> StoreAck {
        stage_store_ack(
            db,
            storage,
            CommitFrontier::MergeConcurrent(BTreeMap::new()),
            "2026-07-16T00:00:00Z".to_string(),
            signer,
        )
        .await
        .expect("stage exact acknowledgement")
    }

    #[tokio::test]
    async fn staged_acknowledgement_reuses_its_exact_object_after_restart_and_lost_response() {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let path = directory.path().join("store.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(&path, "ack-test-device");
        initialize(&db, &storage, &signer).await;
        let founder_ack = db
            .latest_local_store_ack()
            .await
            .expect("read founder acknowledgement")
            .expect("Store creation publishes its founder acknowledgement");
        let ack = stage(&db, &storage, &signer).await;
        assert_eq!(ack.revision, founder_ack.reference.revision + 1);
        assert_eq!(ack.predecessor, Some(founder_ack.reference));
        let staged = db
            .oldest_outbound_store_ack()
            .await
            .expect("read acknowledgement outbox")
            .expect("staged acknowledgement exists");
        drop(db);

        let reopened = open(&path, "ack-test-device");
        home.fail_exact_create_after_call(1);
        assert_eq!(
            drain_outbound_store_acks(&reopened, &storage)
                .await
                .expect("resolve lost exact-create response"),
            1
        );
        let published = reopened
            .latest_local_store_ack()
            .await
            .expect("read published acknowledgement")
            .expect("published acknowledgement exists");
        assert_eq!(published.reference, staged.reference);
        assert_eq!(published.reference.ack_hash, ack.ack_hash());
        assert!(reopened
            .oldest_outbound_store_ack()
            .await
            .expect("read drained acknowledgement outbox")
            .is_none());
        assert_eq!(home.exact_create_count(), 1);
    }

    #[tokio::test]
    async fn occupied_acknowledgement_slot_is_never_replaced_or_completed() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "ack-test-device");
        initialize(&db, &storage, &signer).await;
        let founder_ack = db
            .latest_local_store_ack()
            .await
            .expect("read founder acknowledgement")
            .expect("Store creation publishes its founder acknowledgement");
        stage(&db, &storage, &signer).await;
        let pending = db
            .oldest_outbound_store_ack()
            .await
            .expect("read acknowledgement outbox")
            .expect("staged acknowledgement exists");
        let slot = pending.reference.object.slot().clone();
        home.insert_exact_object(slot.logical_key(), b"competing bytes".to_vec());

        assert!(drain_outbound_store_acks(&db, &storage).await.is_err());
        assert_eq!(
            home.get(slot.logical_key()),
            Some(b"competing bytes".to_vec())
        );
        assert!(db
            .oldest_outbound_store_ack()
            .await
            .expect("read retained acknowledgement outbox")
            .is_some());
        assert_eq!(
            db.latest_local_store_ack()
                .await
                .expect("read unchanged published acknowledgement state")
                .expect("founder acknowledgement remains published")
                .reference,
            founder_ack.reference
        );
    }

    #[tokio::test]
    async fn acknowledgement_predecessor_and_reserved_successor_form_one_exact_chain() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "ack-test-device");
        initialize(&db, &storage, &signer).await;
        let first = stage(&db, &storage, &signer).await;
        drain_outbound_store_acks(&db, &storage)
            .await
            .expect("publish first acknowledgement");
        let first_published = db
            .latest_local_store_ack()
            .await
            .expect("read first acknowledgement")
            .expect("first acknowledgement exists");
        let second = stage(&db, &storage, &signer).await;
        let second_pending = db
            .oldest_outbound_store_ack()
            .await
            .expect("read second acknowledgement")
            .expect("second acknowledgement exists");

        assert_eq!(second.predecessor, Some(first_published.reference.clone()));
        assert_eq!(
            second.successor.predecessor,
            Some(first_published.reference.object.clone())
        );
        assert_eq!(
            second_pending.reference.object.slot(),
            &first.successor.next_slot
        );
        assert_eq!(second.revision, first.revision + 1);
    }
}
