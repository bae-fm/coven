//! Durable exact Store acknowledgement publication.

use super::membership::MembershipChain;
use super::storage::{
    CoordinationStorage, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, CommitFrontier, DeviceStreamAnchor, StoreAck, StoreAckExclusionState,
    StoreHistoryCut, StoreSerialPredecessor, StreamActivationId, SuccessorLink,
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
    #[error("Store acknowledgement activation: {0}")]
    Outbound(#[from] super::store_outbound::StoreOutboundError),
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
    let history_cut = match &frontier {
        CommitFrontier::MergeConcurrent(commits) => {
            StoreHistoryCut::merge_concurrent(commits.clone())
        }
        CommitFrontier::Serial(Some(commit)) => {
            StoreHistoryCut::serial(StoreSerialPredecessor::Commit(commit.clone()))
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
        None => (1, None, acknowledgement_first_slot(&registration)?.clone()),
    };
    let (device_state, _) = db.store_device_state_for_history_cut(&history_cut).await?;
    let snapshot = match db.latest_local_store_snapshot().await? {
        Some(snapshot) if frontier.covers(&snapshot.meta.coverage) => Some(snapshot.reference),
        Some(_) => {
            return Err(StoreAckError::InvalidOutbound(
                "latest Store snapshot is outside the acknowledgement frontier".to_string(),
            ))
        }
        None => None,
    };
    let exclusions = match frontier {
        CommitFrontier::MergeConcurrent(_) => StoreAckExclusionState::MergeConcurrent {
            proposal_freezes: Vec::new(),
        },
        CommitFrontier::Serial(_) => StoreAckExclusionState::Serial,
    };
    let context =
        ProtocolObjectContext::store(root.store_root_hash, ProtocolObjectDomain::StoreAck);
    let semantic_prefix = ack_slot_prefix(&device_id, sequence);
    let next_slot = storage
        .allocate_protocol_slot(
            &context,
            &ack_slot_prefix(
                &device_id,
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
    let ack = StoreAck::signed(
        root.store_root_hash,
        registration_ref.clone(),
        sequence,
        history_cut,
        device_state,
        snapshot,
        exclusions,
        last_sync,
        SuccessorLink {
            activation: StreamActivationId::store_acknowledgements(&root, &registration_ref),
            predecessor,
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
    coordination: Option<&dyn CoordinationStorage>,
    signer: &UserKeypair,
    membership: Option<&MembershipChain>,
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
        if let Some(activated) = db
            .activated_store_ack(&outbound.reference.registration)
            .await?
        {
            if activated == outbound.reference {
                db.complete_outbound_store_ack(outbound.reference).await?;
                published = published.checked_add(1).ok_or_else(|| {
                    StoreAckError::Database("ack publish count exceeded u64".into())
                })?;
                continue;
            }
            if activated.sequence >= outbound.reference.sequence {
                return Err(StoreAckError::InvalidOutbound(
                    "queued Store acknowledgement differs from the activated exact ref".to_string(),
                ));
            }
        }
        let candidate = match outbound.activation.clone() {
            crate::database::OutboundStoreAckActivation::AwaitingCandidate => {
                let plan = super::store_outbound::prepare_store_operation_commit(
                    db,
                    storage,
                    coordination,
                    &device_id,
                    signer,
                    membership,
                )
                .await?;
                plan.validate_acknowledgement(&outbound.ack.value)?;
                let candidate = super::store_outbound::prepare_store_operation_candidate(
                    db,
                    storage,
                    plan,
                    super::store_outbound::StoreOperationBatch::Acknowledgement(
                        outbound.reference.clone(),
                    ),
                )
                .await?;
                db.prepare_outbound_store_ack_activation(outbound.reference.clone(), candidate)
                    .await?;
                continue;
            }
            crate::database::OutboundStoreAckActivation::Prepared(candidate) => candidate,
        };
        storage
            .create_protocol_object(&outbound.ack.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let opened = storage
            .read_protocol_object(
                &context,
                &outbound.reference.object,
                &ack_slot_prefix(&device_id, outbound.reference.sequence),
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
        super::store_outbound::publish_prepared_store_operation(
            db,
            storage,
            coordination,
            candidate,
        )
        .await?;
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
        let frontier = CommitFrontier::from_refs(
            crate::WritePolicy::MergeConcurrent,
            db.materialized_frontier()
                .await
                .expect("read acknowledgement frontier"),
        )
        .expect("shape Merge acknowledgement frontier");
        stage_store_ack(
            db,
            storage,
            frontier,
            "2026-07-16T00:00:00Z".to_string(),
            signer,
        )
        .await
        .expect("stage exact acknowledgement")
    }

    async fn drain(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
    ) -> Result<u64, StoreAckError> {
        let membership = super::super::pull::load_cycle_membership(storage, db)
            .await
            .expect("load acknowledgement test membership");
        drain_outbound_store_acks(db, storage, None, signer, membership.chain.as_ref()).await
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
        let plan = super::super::store_outbound::prepare_store_operation_commit(
            db,
            storage,
            None,
            &device_id,
            signer,
            membership.chain.as_ref(),
        )
        .await
        .expect("prepare acknowledgement activation");
        plan.validate_acknowledgement(&outbound.ack.value)
            .expect("acknowledgement matches activation predecessor");
        let candidate = super::super::store_outbound::prepare_store_operation_candidate(
            db,
            storage,
            plan,
            super::super::store_outbound::StoreOperationBatch::Acknowledgement(
                outbound.reference.clone(),
            ),
        )
        .await
        .expect("prepare acknowledgement candidate");
        db.prepare_outbound_store_ack_activation(outbound.reference.clone(), candidate.clone())
            .await
            .expect("persist acknowledgement candidate");
        candidate
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
        initialize(&db, &storage, &signer).await;
        stage(&db, &storage, &signer).await;
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
        super::super::store_outbound::publish_prepared_store_operation(
            &db, &storage, None, candidate,
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
}
