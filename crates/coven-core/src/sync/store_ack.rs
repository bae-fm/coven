//! Durable exact Store acknowledgement publication.

use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    ack_slot_prefix, DeviceStreamAnchor, StoreAck, StoreAckExclusionState, StoreHistoryCut,
    SuccessorLink,
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
    #[error("outbound Store acknowledgement is invalid: {0}")]
    InvalidOutbound(String),
    #[error("Store acknowledgement activation: {0}")]
    Outbound(#[from] super::store_outbound::StoreOutboundError),
    #[error("Store acknowledgement snapshot: {0}")]
    Snapshot(#[from] super::snapshot::SnapshotError),
}

impl From<crate::database::DbError> for StoreAckError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

pub(crate) struct ResolvedStoreAckPlan {
    pub(crate) root: super::store_commit::StoreRootRef,
    pub(crate) registration_ref: super::store_commit::StoreDeviceRegistrationRef,
    pub(crate) registration: super::store_commit::StoreDeviceRegistration,
    pub(crate) device_signer: UserKeypair,
    pub(crate) device_id: String,
    pub(crate) history_cut: StoreHistoryCut,
    pub(crate) device_state: super::store_commit::StoreDeviceStateRef,
    pub(crate) snapshot: Option<super::store_commit::StoreSnapshotLocator>,
    pub(crate) exclusions: StoreAckExclusionState,
    pub(crate) last_sync: String,
}

pub(crate) async fn stage_resolved_store_ack(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: ResolvedStoreAckPlan,
) -> Result<StoreAck, StoreAckError> {
    if db.oldest_outbound_store_ack().await?.is_some() {
        return Err(StoreAckError::InvalidOutbound(
            "a prior acknowledgement remains queued".to_string(),
        ));
    }
    let previous = db.latest_local_store_ack().await?;
    let (sequence, predecessor, current_slot) = match previous {
        Some(previous) => (
            previous.reference.sequence.checked_add(1).ok_or_else(|| {
                StoreAckError::InvalidOutbound(
                    "Store acknowledgement sequence overflow".to_string(),
                )
            })?,
            Some(previous.reference.object),
            previous.successor_slot,
        ),
        None => (
            1,
            None,
            acknowledgement_first_slot(&plan.registration)?.clone(),
        ),
    };
    let context = ProtocolObjectContext::signed_plaintext(
        plan.root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let semantic_prefix = ack_slot_prefix(&plan.device_id, sequence);
    let next_slot = storage
        .allocate_protocol_slot(
            &context,
            &ack_slot_prefix(
                &plan.device_id,
                sequence.checked_add(1).ok_or_else(|| {
                    StoreAckError::InvalidOutbound(
                        "Store acknowledgement sequence overflow".to_string(),
                    )
                })?,
            ),
            ".json",
        )
        .await
        .map_err(StoreObjectError::from)?;
    let activation = plan
        .registration
        .store_acknowledgement_activation(&plan.registration_ref)
        .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?
        .activation_id();
    let acknowledgement = StoreAck::signed(
        plan.root.store_root_hash,
        plan.registration_ref,
        sequence,
        plan.history_cut,
        plan.device_state,
        plan.snapshot,
        plan.exclusions,
        plan.last_sync,
        SuccessorLink {
            activation,
            predecessor,
            next_slot,
        },
        &plan.device_signer,
    )
    .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
    let prepared = storage
        .prepare_protocol_object(
            &context,
            current_slot,
            &semantic_prefix,
            acknowledgement.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    db.stage_store_ack(acknowledgement.clone(), prepared)
        .await?;
    Ok(acknowledgement)
}

pub(crate) async fn publish_acknowledgement_object(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    outbound: &crate::database::OutboundStoreAck,
    candidate: &super::store_outbound::PreparedStoreOperationCommit,
) -> Result<bool, StoreAckError> {
    let context = ProtocolObjectContext::signed_plaintext(
        outbound.ack.value.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    if let Err(error) = storage.create_protocol_object(&outbound.ack.prepared).await {
        if !matches!(error, super::storage::StorageError::SlotCollision(_)) {
            return Err(StoreObjectError::from(error).into());
        }
        let semantic_prefix = ack_slot_prefix(device_id, outbound.reference.sequence);
        let (winner_bytes, winner_prepared) = storage
            .read_prepared_protocol_slot(
                &context,
                outbound.reference.object.slot(),
                &semantic_prefix,
            )
            .await
            .map_err(StoreObjectError::from)?;
        db.adopt_outbound_store_ack_slot_winner(
            outbound.reference.clone(),
            winner_bytes,
            winner_prepared,
        )
        .await?;
        return Ok(false);
    }
    let opened = storage
        .read_protocol_object(
            &context,
            &outbound.reference.object,
            &ack_slot_prefix(device_id, outbound.reference.sequence),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if opened != outbound.ack.bytes {
        return Err(StoreAckError::InvalidOutbound(
            "Store acknowledgement exact readback differs from prepared bytes".to_string(),
        ));
    }
    let acknowledgement_remote = candidate
        .acknowledgement_remote_objects(&outbound.ack)?
        .into_iter()
        .find(|remote| remote.object() == &outbound.reference.object)
        .ok_or_else(|| {
            StoreAckError::InvalidOutbound(
                "prepared activation does not own its acknowledgement object".to_string(),
            )
        })?;
    db.mark_remote_object_uploaded(acknowledgement_remote)
        .await?;
    Ok(true)
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
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::CommitFrontier;

    fn open(path: &Path, device_id: &str) -> Database {
        Database::open(
            path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
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
        super::super::store_registration::ensure_active_registration(db, storage)
            .await
            .expect("activate acknowledgement test registration");
    }

    async fn stage(db: &Database, storage: &CloudSyncStorage, signer: &UserKeypair) -> StoreAck {
        stage_at(db, storage, signer, "2026-07-16T00:00:00Z").await
    }

    async fn stage_at(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
        last_sync: &str,
    ) -> StoreAck {
        let frontier = CommitFrontier::from_refs(
            db.materialized_frontier()
                .await
                .expect("read acknowledgement frontier"),
        )
        .expect("shape acknowledgement frontier");
        super::super::store_engine::stage_merge_acknowledgement_for_test(
            db,
            storage,
            frontier,
            last_sync.to_string(),
            signer,
        )
        .await
        .expect("stage exact acknowledgement")
    }

    fn drain<'a>(
        db: &'a Database,
        storage: &'a CloudSyncStorage,
        signer: &'a UserKeypair,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, StoreAckError>> + 'a>> {
        Box::pin(async move {
            super::super::store_engine::drain_merge_acknowledgements_for_test(db, storage, signer)
                .await
        })
    }

    async fn persist_candidate(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
        outbound: &crate::database::OutboundStoreAck,
    ) -> super::super::store_outbound::PreparedStoreOperationCommit {
        let membership = super::super::pull::load_cycle_membership(storage, db)
            .await
            .expect("load acknowledgement test membership");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("local Store device id");
        let plan = super::super::store_engine::engine::operations::prepare_plan(
            db,
            storage,
            membership
                .chain
                .as_ref()
                .expect("resolved Merge membership"),
            &device_id,
            signer,
        )
        .await
        .expect("prepare acknowledgement activation");
        plan.common()
            .validate_acknowledgement(&outbound.ack.value)
            .expect("acknowledgement matches activation predecessor");
        let candidate = super::super::store_engine::engine::operations::prepare_candidate(
            db,
            storage,
            plan,
            super::super::store_outbound::StoreOperationBatch::Acknowledgement {
                reference: outbound.reference.clone(),
                value: outbound.ack.value.clone(),
            },
        )
        .await
        .expect("prepare acknowledgement candidate");
        super::super::store_engine::prepare_merge_acknowledgement_activation_for_test(
            db,
            outbound.reference.clone(),
            candidate.clone(),
        )
        .await
        .expect("persist acknowledgement candidate");
        candidate
    }

    struct LosingMergeAckFixture {
        home: InMemoryCloudHome,
        signer: UserKeypair,
        storage: CloudSyncStorage,
        db: Database,
        outbound: crate::database::OutboundStoreAck,
        losing: super::super::store_outbound::PreparedStoreOperationCommit,
    }

    async fn losing_merge_ack_fixture(path: &Path) -> LosingMergeAckFixture {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(path, "ack-merge-loser-device");
        Box::pin(initialize(&db, &storage, &signer)).await;
        Box::pin(stage(&db, &storage, &signer)).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged acknowledgement exists");
        let losing = Box::pin(persist_candidate(&db, &storage, &signer, &outbound)).await;
        let membership = Box::pin(super::super::pull::load_cycle_membership(&storage, &db))
            .await
            .expect("load acknowledgement test membership");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("local Store device id");
        let competing_plan = Box::pin(
            super::super::store_engine::engine::operations::prepare_plan(
                &db,
                &storage,
                membership
                    .chain
                    .as_ref()
                    .expect("resolved Merge membership"),
                &device_id,
                &signer,
            ),
        )
        .await
        .expect("prepare competing Store operation");
        let grant_id = super::super::provider::ProviderAccessGrantId::from_random_bytes([91; 32]);
        let grant_prefix =
            super::super::store_commit::provider_access_grant_semantic_prefix(&grant_id);
        let grant_bytes = b"competing provider grant";
        let grant = super::super::provider::StoreMemberProviderAccessGrantRef {
            grant_id,
            grant_hash: super::super::store_commit::ObjectHash::digest(grant_bytes),
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(format!("{grant_prefix}.json"))
                    .expect("valid provider grant slot"),
                grant_bytes.len() as u64,
                super::super::store_commit::ObjectHash::digest(grant_bytes),
            ),
        };
        let competing = Box::pin(
            super::super::store_engine::engine::operations::prepare_candidate(
                &db,
                &storage,
                competing_plan,
                super::super::store_outbound::StoreOperationBatch::ProviderAccessGrant(grant),
            ),
        )
        .await
        .expect("prepare competing candidate");
        assert_ne!(competing.reference, losing.reference);
        let (_, competing_head) = competing.publication_for_test();
        storage
            .create_protocol_object(&competing.prepared)
            .await
            .expect("publish competing commit");
        storage
            .create_protocol_object(competing_head)
            .await
            .expect("publish competing head");
        LosingMergeAckFixture {
            home,
            signer,
            storage,
            db,
            outbound,
            losing,
        }
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
        assert_eq!(ack.sequence, founder_ack.reference.sequence + 1);
        assert_eq!(
            ack.successor.predecessor,
            Some(founder_ack.reference.object)
        );
        let staged = db
            .oldest_outbound_store_ack()
            .await
            .expect("read acknowledgement outbox")
            .expect("staged acknowledgement exists");
        drop(db);

        let reopened = open(&path, "ack-test-device");
        home.fail_exact_create_after_call(1);
        assert_eq!(
            drain(&reopened, &storage, &signer)
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
        assert_eq!(home.exact_create_count(), 3);
    }

    #[tokio::test]
    async fn invalid_acknowledgement_slot_bytes_are_never_replaced_or_completed() {
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

        assert!(drain(&db, &storage, &signer).await.is_err());
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

    async fn assert_valid_acknowledgement_slot_winner_is_adopted_and_activated() {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let seed_path = directory.path().join("seed.sqlite3");
        let winner_path = directory.path().join("winner.sqlite3");
        let loser_path = directory.path().join("loser.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let seed = open(&seed_path, "ack-slot-race-device");
        initialize(&seed, &storage, &signer).await;
        for destination in [&winner_path, &loser_path] {
            let destination = destination
                .to_str()
                .expect("temporary database path is UTF-8")
                .to_string();
            seed.call(move |connection| {
                connection
                    .execute("VACUUM INTO ?1", [destination])
                    .map(|_| ())
                    .map_err(crate::DbError::from)
            })
            .await
            .expect("copy acknowledgement database");
        }
        drop(seed);

        let winner_db = open(&winner_path, "ack-slot-race-device");
        stage_at(&winner_db, &storage, &signer, "2026-07-16T00:00:01Z").await;
        let winner = winner_db
            .oldest_outbound_store_ack()
            .await
            .expect("read winner acknowledgement")
            .expect("winner acknowledgement exists");
        storage
            .create_protocol_object(&winner.ack.prepared)
            .await
            .expect("publish winner acknowledgement");

        let loser_db = open(&loser_path, "ack-slot-race-device");
        stage_at(&loser_db, &storage, &signer, "2026-07-16T00:00:02Z").await;
        let loser = loser_db
            .oldest_outbound_store_ack()
            .await
            .expect("read losing acknowledgement")
            .expect("losing acknowledgement exists");
        assert_eq!(
            winner.reference.object.slot(),
            loser.reference.object.slot()
        );
        assert_ne!(winner.reference, loser.reference);
        let losing_candidate = persist_candidate(&loser_db, &storage, &signer, &loser).await;
        let losing_object_ids = losing_candidate
            .acknowledgement_remote_objects(&loser.ack)
            .expect("load losing acknowledgement candidate graph")
            .into_iter()
            .map(|remote| remote.object_id())
            .collect::<Vec<_>>();

        let result = drain(&loser_db, &storage, &signer).await;
        assert_eq!(
            result.expect("adopt and activate acknowledgement slot winner"),
            1
        );
        assert_eq!(
            loser_db
                .latest_local_store_ack()
                .await
                .expect("read adopted acknowledgement")
                .expect("adopted acknowledgement is published")
                .reference,
            winner.reference
        );
        assert!(loser_db
            .oldest_outbound_store_ack()
            .await
            .expect("read drained acknowledgement outbox")
            .is_none());
        assert!(loser_db
            .protocol_inert_object(loser.reference.object)
            .await
            .expect("read losing acknowledgement inert state")
            .is_none());
        for object_id in losing_object_ids {
            let exists = loser_db
                .call(move |connection| {
                    connection
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                            [object_id.to_string()],
                            |row| row.get::<_, bool>(0),
                        )
                        .map_err(crate::DbError::from)
                })
                .await
                .expect("read losing acknowledgement candidate ownership");
            assert!(!exists);
        }
    }

    #[tokio::test]
    async fn valid_merge_acknowledgement_slot_winner_is_adopted_and_activated() {
        Box::pin(assert_valid_acknowledgement_slot_winner_is_adopted_and_activated()).await;
    }

    #[tokio::test]
    async fn acknowledgement_predecessor_and_reserved_successor_form_one_exact_chain() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "ack-test-device");
        initialize(&db, &storage, &signer).await;
        let first = stage(&db, &storage, &signer).await;
        drain(&db, &storage, &signer)
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

        assert_eq!(
            second.successor.predecessor,
            Some(first_published.reference.object)
        );
        assert_eq!(
            second_pending.reference.object.slot(),
            &first.successor.next_slot
        );
        assert_eq!(second.sequence, first.sequence + 1);
        drain(&db, &storage, &signer)
            .await
            .expect("publish successor acknowledgement after an activated predecessor");
        assert_eq!(
            db.latest_local_store_ack()
                .await
                .unwrap()
                .expect("successor acknowledgement exists")
                .reference,
            second_pending.reference
        );
    }

    #[tokio::test]
    async fn activated_acknowledgement_completes_its_outbox_after_restart_without_another_commit() {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let path = directory.path().join("store.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(&path, "ack-test-device");
        Box::pin(initialize(&db, &storage, &signer)).await;
        Box::pin(stage(&db, &storage, &signer)).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged acknowledgement exists");
        storage
            .create_protocol_object(&outbound.ack.prepared)
            .await
            .expect("publish acknowledgement object");
        let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
        let acknowledgement_remote = candidate
            .acknowledgement_remote_objects(&outbound.ack)
            .expect("candidate owns acknowledgement")
            .into_iter()
            .find(|remote| remote.object() == &outbound.reference.object)
            .expect("acknowledgement remote object");
        db.mark_remote_object_uploaded(acknowledgement_remote)
            .await
            .expect("record acknowledgement upload");
        super::super::store_engine::engine::operations::publish_prepared(
            &db,
            &storage,
            Box::new(candidate),
            None,
            None,
        )
        .await
        .expect("activate acknowledgement commit");
        let activated_position = db.latest_local_store_position().await.unwrap();
        drop(db);

        let reopened = open(&path, "ack-test-device");
        assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
        assert_eq!(
            reopened.latest_local_store_position().await.unwrap(),
            activated_position
        );
        assert!(reopened
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn prepared_activation_candidate_resumes_exactly_after_restart() {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let path = directory.path().join("store.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(&path, "ack-test-device");
        initialize(&db, &storage, &signer).await;
        stage(&db, &storage, &signer).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged acknowledgement exists");
        let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
        let expected = candidate.reference.clone();
        drop(db);

        let reopened = open(&path, "ack-test-device");
        let resumed = reopened
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("prepared acknowledgement exists after restart");
        assert!(matches!(
            resumed.activation,
            crate::database::OutboundStoreAckActivation::Prepared(ref prepared)
                if prepared.reference == expected
        ));
        assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
        assert_eq!(
            reopened.latest_local_store_position().await.unwrap(),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn uploaded_acknowledgement_accepts_its_sole_candidate_nonactivation() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "ack-nonactivation-device");
        initialize(&db, &storage, &signer).await;
        stage(&db, &storage, &signer).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged acknowledgement exists");
        let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
        let mut acknowledgement = candidate
            .acknowledgement_remote_objects(&outbound.ack)
            .expect("candidate owns acknowledgement")
            .into_iter()
            .find(|remote| remote.object() == &outbound.reference.object)
            .expect("acknowledgement ownership record");
        acknowledgement
            .mark_uploaded_verified()
            .expect("acknowledgement upload is durable");
        let winner_bytes = b"different valid winner head";
        let winner_object = crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(
                "store-v1/heads/ack-nonactivation-winner.json".to_string(),
            )
            .expect("valid winner slot"),
            winner_bytes.len() as u64,
            super::super::store_commit::ObjectHash::digest(winner_bytes),
        );
        let nonactivation =
            super::super::remote_object::CandidateNonactivation::unverified_for_test(
                super::super::store_commit::StoreBatchCommitDeletionTarget {
                    coord: candidate.reference.coord.clone(),
                    object: candidate.reference.object.clone(),
                    canonical_signed_bytes: candidate.commit.to_bytes(),
                },
                super::super::remote_object::CandidateNonactivationProof::MergeWinner {
                    winner_head: super::super::store_commit::StoreDeviceHeadRef {
                        head_hash: super::super::store_commit::ObjectHash::digest(winner_bytes),
                        object: winner_object,
                    },
                },
            );

        assert!(acknowledgement
            .begin_candidate_nonactivation(nonactivation)
            .expect("uploaded acknowledgement accepts candidate loss")
            .is_some());
    }

    #[tokio::test]
    async fn losing_merge_activation_inerts_the_uploaded_acknowledgement() {
        let LosingMergeAckFixture {
            home,
            signer,
            storage,
            db,
            outbound,
            losing,
        } = Box::pin(losing_merge_ack_fixture(Path::new(":memory:"))).await;

        assert_eq!(
            drain(&db, &storage, &signer)
                .await
                .expect("settle losing acknowledgement activation"),
            1
        );
        assert!(db.oldest_outbound_store_ack().await.unwrap().is_none());
        assert_eq!(
            db.latest_local_store_ack()
                .await
                .unwrap()
                .expect("inert acknowledgement still advances its physical stream")
                .reference,
            outbound.reference
        );
        let inert = db
            .protocol_inert_object(outbound.reference.object.clone())
            .await
            .unwrap()
            .expect("losing acknowledgement is retained outside reducer state");
        assert!(matches!(
            inert.identity.domain,
            super::super::remote_object::RetainedAuthorityObjectDomain::Acknowledgement {
                ref reference
            } if reference == &outbound.reference
        ));
        assert!(inert
            .candidate_nonactivation_proof(&losing.reference)
            .expect("inert acknowledgement proof is valid")
            .is_some());
        assert!(home
            .get(losing.reference.object.slot().logical_key())
            .is_none());
        assert_ne!(
            db.activated_store_ack(&outbound.reference.registration)
                .await
                .unwrap(),
            Some(outbound.reference.clone())
        );
    }

    #[tokio::test]
    async fn merge_acknowledgement_nonactivation_resumes_after_delete_failure_and_restart() {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let path = directory.path().join("store.sqlite3");
        let LosingMergeAckFixture {
            home,
            signer,
            storage,
            db,
            outbound,
            losing,
        } = Box::pin(losing_merge_ack_fixture(&path)).await;
        home.fail_exact_delete_on_call(1);

        assert!(drain(&db, &storage, &signer).await.is_err());
        assert!(matches!(
            db.oldest_outbound_store_ack()
                .await
                .unwrap()
                .expect("nonactivating acknowledgement remains durable")
                .activation,
            crate::database::OutboundStoreAckActivation::Nonactivating(ref candidate)
                if candidate.reference == losing.reference
        ));
        assert!(db
            .protocol_inert_object(outbound.reference.object.clone())
            .await
            .unwrap()
            .is_some());
        drop(db);

        let reopened = open(&path, "ack-merge-loser-device");
        assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
        assert!(reopened
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .is_none());
        assert!(home
            .get(losing.reference.object.slot().logical_key())
            .is_none());
    }

    #[tokio::test]
    async fn merge_acknowledgement_completion_rejects_mismatched_durable_loss_proofs() {
        Box::pin(run_merge_acknowledgement_completion_rejects_mismatched_durable_loss_proofs())
            .await;
    }

    async fn run_merge_acknowledgement_completion_rejects_mismatched_durable_loss_proofs() {
        let LosingMergeAckFixture {
            home,
            signer,
            storage,
            db,
            outbound,
            losing,
        } = Box::pin(losing_merge_ack_fixture(Path::new(":memory:"))).await;
        home.fail_exact_delete_on_call(1);
        assert!(Box::pin(drain(&db, &storage, &signer)).await.is_err());

        let head = super::super::store_commit::StoreDeviceHeadRef {
            head_hash: losing.head.head_hash(),
            object: losing.prepared_head.reference().clone(),
        };
        db.call(move |conn| {
            let object_id = super::super::remote_object::remote_object_id(&head.object);
            let encoded: String = conn
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(crate::DbError::from)?;
            let mut remote: super::super::remote_object::RemoteObjectRecord =
                serde_json::from_str(&encoded).map_err(|error| {
                    crate::DbError::Message(format!("parse test head ownership: {error}"))
                })?;
            let super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record) =
                &mut remote
            else {
                return Err(crate::DbError::Message(
                    "test head is not retained authority".to_string(),
                ));
            };
            let super::super::remote_object::RetainedAuthorityObjectState::UncreatedVerified {
                former_candidates,
            } = &mut record.state
            else {
                return Err(crate::DbError::Message(
                    "test head is not proven uncreated".to_string(),
                ));
            };
            let Some(nonactivation) = former_candidates.first_mut() else {
                return Err(crate::DbError::Message(
                    "test head has no loss proof".to_string(),
                ));
            };
            let super::super::remote_object::CandidateNonactivationProof::MergeWinner {
                winner_head,
            } = nonactivation.proof_mut_for_test()
            else {
                return Err(crate::DbError::Message(
                    "test head has a non-Merge loss proof".to_string(),
                ));
            };
            let tampered_bytes = b"different winner at the same head slot";
            winner_head.object = crate::sync::storage::ExactObjectRef::new(
                winner_head.object.slot().clone(),
                tampered_bytes.len() as u64,
                super::super::store_commit::ObjectHash::digest(tampered_bytes),
            );
            remote.validate().map_err(|error| {
                crate::DbError::Message(format!("validate test head ownership: {error}"))
            })?;
            conn.execute(
                "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                (
                    object_id.to_string(),
                    serde_json::to_string(&remote).map_err(|error| {
                        crate::DbError::Message(format!("serialize test head ownership: {error}"))
                    })?,
                ),
            )
            .map_err(crate::DbError::from)?;
            Ok(())
        })
        .await
        .expect("install mismatched durable head proof");

        assert!(Box::pin(drain(&db, &storage, &signer)).await.is_err());
        assert_eq!(
            db.oldest_outbound_store_ack()
                .await
                .unwrap()
                .expect("mismatched proof keeps acknowledgement pending")
                .reference,
            outbound.reference
        );
    }

    #[tokio::test]
    async fn alternate_merge_head_for_the_same_ack_candidate_is_adopted() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "ack-alternate-head-device");
        initialize(&db, &storage, &signer).await;
        stage(&db, &storage, &signer).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged acknowledgement exists");
        let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
        let expected_head = candidate.head.clone();
        let expected_prepared = candidate.prepared_head.clone();
        let (root, registration_ref, _, device_signer) =
            super::super::store_outbound::load_local_store_authority(
                &db,
                &outbound.reference.registration.device_id.to_string(),
                &signer,
            )
            .await
            .expect("load local device signing authority");
        let alternate_next = crate::storage::cloud::ObjectSlot::opaque(
            expected_head.successor.next_slot.logical_key().to_string(),
            "alternate-next-slot".to_string(),
        )
        .expect("valid alternate successor slot");
        let alternate_head = super::super::store_commit::StoreDeviceHead::signed(
            root.store_root_hash,
            registration_ref,
            candidate.reference.clone(),
            expected_head.history_summary,
            super::super::store_commit::SuccessorLink {
                activation: expected_head.successor.activation,
                predecessor: expected_head.successor.predecessor.clone(),
                next_slot: alternate_next,
            },
            &device_signer,
        )
        .expect("sign alternate head");
        let head_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = super::super::store_commit::head_slot_prefix(
            &outbound.reference.registration.device_id.to_string(),
            candidate.commit.seq(),
        );
        let alternate_prepared = storage
            .prepare_protocol_object(
                &head_context,
                expected_prepared.reference().slot().clone(),
                &head_prefix,
                alternate_head.to_bytes(),
            )
            .expect("prepare alternate head at the same slot");
        assert_ne!(
            alternate_prepared.reference(),
            expected_prepared.reference()
        );
        storage
            .create_protocol_object(&alternate_prepared)
            .await
            .expect("publish alternate head");

        assert_eq!(drain(&db, &storage, &signer).await.unwrap(), 1);
        assert_eq!(
            db.activated_store_ack(&outbound.reference.registration)
                .await
                .unwrap(),
            Some(outbound.reference)
        );
    }
}
