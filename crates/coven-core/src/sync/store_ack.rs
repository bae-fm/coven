//! Durable exact Store acknowledgement publication.

use super::membership::MembershipChain;
use super::storage::{
    CoordinationStorage, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, CommitFrontier, DeviceStreamAnchor, StoreAck, StoreAckExclusionState,
    StoreHistoryCut, StoreSerialPredecessor, SuccessorLink,
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
    let proposal_freezes = db.store_device_exclusion_freezes().await?;
    let exclusions = match frontier {
        CommitFrontier::MergeConcurrent(_) => {
            StoreAckExclusionState::MergeConcurrent { proposal_freezes }
        }
        CommitFrontier::Serial(_) if proposal_freezes.is_empty() => StoreAckExclusionState::Serial,
        CommitFrontier::Serial(_) => {
            return Err(StoreAckError::InvalidOutbound(
                "Serial Store contains Merge device exclusion freezes".to_string(),
            ))
        }
    };
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
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
    let activation = registration
        .store_acknowledgement_activation(&registration_ref)
        .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?
        .activation_id();
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
            activation,
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
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
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
            crate::database::OutboundStoreAckActivation::Nonactivating(_) => {
                super::store_outbound::finish_nonactivating_store_ack(
                    db,
                    storage,
                    outbound.reference,
                )
                .await?;
                published = published.checked_add(1).ok_or_else(|| {
                    StoreAckError::Database("ack publish count exceeded u64".into())
                })?;
                continue;
            }
        };
        if let Err(error) = storage.create_protocol_object(&outbound.ack.prepared).await {
            if !matches!(error, super::storage::StorageError::SlotCollision(_)) {
                return Err(StoreObjectError::from(error).into());
            }
            let semantic_prefix = ack_slot_prefix(&device_id, outbound.reference.sequence);
            let (winner_bytes, winner_prepared) = storage
                .read_prepared_protocol_slot(
                    &context,
                    outbound.reference.object.slot(),
                    &semantic_prefix,
                )
                .await
                .map_err(StoreObjectError::from)?;
            db.adopt_outbound_store_ack_slot_winner(
                outbound.reference,
                winner_bytes,
                winner_prepared,
            )
            .await?;
            continue;
        }
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
        let outcome = super::store_outbound::publish_prepared_store_operation(
            db,
            storage,
            coordination,
            Box::new(candidate),
        )
        .await?;
        match outcome {
            super::store_outbound::StoreOperationPublicationOutcome::Activated(_) => {
                db.complete_outbound_store_ack(outbound.reference).await?;
            }
            super::store_outbound::StoreOperationPublicationOutcome::Nonactivated(_) => {}
            super::store_outbound::StoreOperationPublicationOutcome::Reprepared => continue,
            super::store_outbound::StoreOperationPublicationOutcome::RepreparedCandidate(_)
            | super::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate {
                ..
            } => {
                return Err(StoreAckError::InvalidOutbound(
                    "acknowledgement publication returned non-acknowledgement conflict state"
                        .to_string(),
                ));
            }
        }
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
        open_with_policy(path, device_id, crate::WritePolicy::MergeConcurrent)
    }

    fn open_with_policy(
        path: &Path,
        device_id: &str,
        write_policy: crate::WritePolicy,
    ) -> Database {
        Database::open(
            path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            write_policy,
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
        stage_at(db, storage, signer, "2026-07-16T00:00:00Z").await
    }

    async fn stage_at(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
        last_sync: &str,
    ) -> StoreAck {
        let frontier = CommitFrontier::from_refs(
            db.write_policy(),
            db.materialized_frontier()
                .await
                .expect("read acknowledgement frontier"),
        )
        .expect("shape acknowledgement frontier");
        stage_store_ack(db, storage, frontier, last_sync.to_string(), signer)
            .await
            .expect("stage exact acknowledgement")
    }

    fn drain<'a>(
        db: &'a Database,
        storage: &'a CloudSyncStorage,
        signer: &'a UserKeypair,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, StoreAckError>> + 'a>> {
        Box::pin(async move {
            let membership = super::super::pull::load_cycle_membership(storage, db)
                .await
                .expect("load acknowledgement test membership");
            drain_outbound_store_acks(db, storage, None, signer, membership.chain.as_ref()).await
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

    async fn persist_serial_candidate(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
        outbound: &crate::database::OutboundStoreAck,
    ) -> super::super::store_outbound::PreparedStoreOperationCommit {
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("local Store device id");
        let plan = super::super::store_outbound::prepare_store_operation_commit(
            db,
            storage,
            Some(storage),
            &device_id,
            signer,
            None,
        )
        .await
        .expect("prepare Serial acknowledgement activation");
        plan.validate_acknowledgement(&outbound.ack.value)
            .expect("Serial acknowledgement matches activation predecessor");
        let candidate = super::super::store_outbound::prepare_store_operation_candidate(
            db,
            storage,
            plan,
            super::super::store_outbound::StoreOperationBatch::Acknowledgement(
                outbound.reference.clone(),
            ),
        )
        .await
        .expect("prepare Serial acknowledgement candidate");
        db.prepare_outbound_store_ack_activation(outbound.reference.clone(), candidate.clone())
            .await
            .expect("persist Serial acknowledgement candidate");
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
            super::super::store_outbound::prepare_store_operation_commit(
                &db,
                &storage,
                None,
                &device_id,
                &signer,
                membership.chain.as_ref(),
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
            super::super::store_outbound::prepare_store_operation_candidate(
                &db,
                &storage,
                competing_plan,
                super::super::store_outbound::StoreOperationBatch::ProviderAccessGrant(grant),
            ),
        )
        .await
        .expect("prepare competing candidate");
        assert_ne!(competing.reference, losing.reference);
        let (_, competing_head) = competing
            .merge_publication_for_test()
            .expect("Merge operation has a device head");
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

    struct LosingSerialAckFixture {
        home: InMemoryCloudHome,
        signer: UserKeypair,
        storage: CloudSyncStorage,
        db: Database,
        outbound: crate::database::OutboundStoreAck,
        losing: super::super::store_outbound::PreparedStoreOperationCommit,
    }

    async fn losing_serial_ack_fixture(path: &Path) -> LosingSerialAckFixture {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer).with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_with_policy(path, "ack-serial-loser-device", crate::WritePolicy::Serial);
        Box::pin(initialize(&db, &storage, &signer)).await;
        Box::pin(stage(&db, &storage, &signer)).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged Serial acknowledgement exists");
        let losing = Box::pin(persist_serial_candidate(&db, &storage, &signer, &outbound)).await;
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("local Store device id");
        let competing_plan = Box::pin(
            super::super::store_outbound::prepare_store_operation_commit(
                &db,
                &storage,
                Some(&storage),
                &device_id,
                &signer,
                None,
            ),
        )
        .await
        .expect("prepare competing Serial operation");
        let grant_id = super::super::provider::ProviderAccessGrantId::from_random_bytes([92; 32]);
        let grant_prefix =
            super::super::store_commit::provider_access_grant_semantic_prefix(&grant_id);
        let grant_bytes = b"competing Serial provider grant";
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
            super::super::store_outbound::prepare_store_operation_candidate(
                &db,
                &storage,
                competing_plan,
                super::super::store_outbound::StoreOperationBatch::ProviderAccessGrant(grant),
            ),
        )
        .await
        .expect("prepare competing Serial candidate");
        assert!(matches!(
            Box::pin(
                super::super::store_outbound::publish_prepared_store_operation(
                    &db,
                    &storage,
                    Some(&storage),
                    Box::new(competing),
                )
            )
            .await
            .expect("activate competing Serial candidate"),
            super::super::store_outbound::StoreOperationPublicationOutcome::Activated(_)
        ));
        LosingSerialAckFixture {
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

    async fn assert_valid_acknowledgement_slot_winner_is_adopted_and_activated(
        write_policy: crate::WritePolicy,
    ) {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let seed_path = directory.path().join("seed.sqlite3");
        let winner_path = directory.path().join("winner.sqlite3");
        let loser_path = directory.path().join("loser.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = match write_policy {
            crate::WritePolicy::MergeConcurrent => storage(&home, &signer),
            crate::WritePolicy::Serial => {
                storage(&home, &signer).with_test_serial_coordination(Arc::new(home.clone()))
            }
        };
        let seed = open_with_policy(&seed_path, "ack-slot-race-device", write_policy);
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

        let winner_db = open_with_policy(&winner_path, "ack-slot-race-device", write_policy);
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

        let loser_db = open_with_policy(&loser_path, "ack-slot-race-device", write_policy);
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
        let losing_candidate = match write_policy {
            crate::WritePolicy::MergeConcurrent => {
                persist_candidate(&loser_db, &storage, &signer, &loser).await
            }
            crate::WritePolicy::Serial => {
                persist_serial_candidate(&loser_db, &storage, &signer, &loser).await
            }
        };
        let losing_object_ids = losing_candidate
            .acknowledgement_remote_objects(&loser.ack)
            .expect("load losing acknowledgement candidate graph")
            .into_iter()
            .map(|remote| remote.object_id())
            .collect::<Vec<_>>();

        let result = match write_policy {
            crate::WritePolicy::MergeConcurrent => drain(&loser_db, &storage, &signer).await,
            crate::WritePolicy::Serial => {
                drain_outbound_store_acks(&loser_db, &storage, Some(&storage), &signer, None).await
            }
        };
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
        Box::pin(
            assert_valid_acknowledgement_slot_winner_is_adopted_and_activated(
                crate::WritePolicy::MergeConcurrent,
            ),
        )
        .await;
    }

    #[tokio::test]
    async fn valid_serial_acknowledgement_slot_winner_is_adopted_and_activated() {
        Box::pin(
            assert_valid_acknowledgement_slot_winner_is_adopted_and_activated(
                crate::WritePolicy::Serial,
            ),
        )
        .await;
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
        super::super::store_outbound::publish_prepared_store_operation(
            &db,
            &storage,
            None,
            Box::new(candidate),
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
        let nonactivation = super::super::remote_object::CandidateNonactivation {
            candidate: super::super::store_commit::StoreBatchCommitDeletionTarget {
                coord: candidate.reference.coord.clone(),
                object: candidate.reference.object.clone(),
                canonical_signed_bytes: candidate.commit.to_bytes(),
            },
            proof: super::super::remote_object::CandidateNonactivationProof::MergeWinner {
                winner_head: super::super::store_commit::StoreDeviceHeadRef {
                    head_hash: super::super::store_commit::ObjectHash::digest(winner_bytes),
                    object: winner_object,
                },
            },
        };

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

        let head = losing
            .merge_head_ref()
            .expect("Merge acknowledgement candidate has a head");
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
            } = &mut nonactivation.proof
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
        let (expected_head, expected_prepared) = candidate
            .merge_publication_for_test()
            .expect("Merge acknowledgement has a device head");
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

    #[tokio::test]
    async fn losing_serial_activation_inerts_the_uploaded_acknowledgement() {
        let LosingSerialAckFixture {
            home,
            signer,
            storage,
            db,
            outbound,
            losing,
        } = Box::pin(losing_serial_ack_fixture(Path::new(":memory:"))).await;

        assert_eq!(
            Box::pin(drain_outbound_store_acks(
                &db,
                &storage,
                Some(&storage),
                &signer,
                None,
            ))
            .await
            .expect("settle losing Serial acknowledgement activation"),
            1
        );
        assert!(db.oldest_outbound_store_ack().await.unwrap().is_none());
        let inert = db
            .protocol_inert_object(outbound.reference.object.clone())
            .await
            .unwrap()
            .expect("losing Serial acknowledgement is protocol-inert");
        assert!(inert
            .candidate_nonactivation_proof(&losing.reference)
            .expect("Serial acknowledgement loss proof is valid")
            .is_some());
        assert!(home
            .get(losing.reference.object.slot().logical_key())
            .is_none());
        assert_ne!(
            db.activated_store_ack(&outbound.reference.registration)
                .await
                .unwrap(),
            Some(outbound.reference)
        );
    }

    #[tokio::test]
    async fn serial_acknowledgement_nonactivation_resumes_after_delete_failure_and_restart() {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let path = directory.path().join("store.sqlite3");
        let LosingSerialAckFixture {
            home,
            signer,
            storage,
            db,
            outbound,
            losing,
        } = Box::pin(losing_serial_ack_fixture(&path)).await;
        storage
            .create_protocol_object(&losing.prepared)
            .await
            .expect("publish losing Serial commit before the competing activation is observed");
        db.mark_candidate_commit_uploaded(losing.reference.clone())
            .await
            .expect("record uploaded losing Serial commit");
        home.fail_exact_delete_on_call(1);

        assert!(Box::pin(drain_outbound_store_acks(
            &db,
            &storage,
            Some(&storage),
            &signer,
            None,
        ))
        .await
        .is_err());
        assert!(matches!(
            db.oldest_outbound_store_ack()
                .await
                .unwrap()
                .expect("nonactivating Serial acknowledgement remains durable")
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

        let reopened =
            open_with_policy(&path, "ack-serial-loser-device", crate::WritePolicy::Serial);
        assert_eq!(
            Box::pin(drain_outbound_store_acks(
                &reopened,
                &storage,
                Some(&storage),
                &signer,
                None,
            ))
            .await
            .expect("resume Serial acknowledgement cleanup"),
            1
        );
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
    async fn serial_acknowledgement_retries_after_a_version_only_head_change() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer).with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_with_policy(
            Path::new(":memory:"),
            "ack-serial-version-device",
            crate::WritePolicy::Serial,
        );
        initialize(&db, &storage, &signer).await;
        stage(&db, &storage, &signer).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged Serial acknowledgement exists");
        Box::pin(persist_serial_candidate(&db, &storage, &signer, &outbound)).await;

        let head_key = super::super::store_commit::serial_head_key();
        let original = CoordinationStorage::read_head(&storage, head_key)
            .await
            .expect("read Serial genesis head");
        CoordinationStorage::replace_head(&storage, head_key, &original.version, &original.bytes)
            .await
            .expect("replace Serial head with the same signed bytes");

        assert_eq!(
            Box::pin(drain_outbound_store_acks(
                &db,
                &storage,
                Some(&storage),
                &signer,
                None,
            ))
            .await
            .expect("retry acknowledgement from the newer provider receipt"),
            1
        );
        assert_eq!(
            db.activated_store_ack(&outbound.reference.registration)
                .await
                .unwrap(),
            Some(outbound.reference.clone())
        );
        assert!(db
            .protocol_inert_object(outbound.reference.object)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn serial_acknowledgement_activates_when_its_candidate_is_an_accepted_ancestor() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer).with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_with_policy(
            Path::new(":memory:"),
            "ack-serial-ancestor-device",
            crate::WritePolicy::Serial,
        );
        Box::pin(initialize(&db, &storage, &signer)).await;
        Box::pin(stage(&db, &storage, &signer)).await;
        let outbound = db
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("staged Serial acknowledgement exists");
        let candidate = Box::pin(persist_serial_candidate(&db, &storage, &signer, &outbound)).await;
        let (base_head, candidate_head) = candidate
            .serial_publication_for_test()
            .expect("Serial acknowledgement has a coordination head");
        storage
            .create_protocol_object(&outbound.ack.prepared)
            .await
            .expect("publish acknowledgement object");
        storage
            .create_protocol_object(&candidate.prepared)
            .await
            .expect("publish acknowledgement candidate commit");
        let accepted_candidate_head = CoordinationStorage::replace_head(
            &storage,
            super::super::store_commit::serial_head_key(),
            &base_head.version,
            &candidate_head.to_bytes(),
        )
        .await
        .expect("activate acknowledgement candidate without completing local state");

        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("local Store device id");
        let successor_plan = Box::pin(
            super::super::store_outbound::serial_successor_plan_for_test(
                &db,
                &device_id,
                &signer,
                &candidate,
                accepted_candidate_head,
            ),
        )
        .await
        .expect("prepare accepted Serial successor");
        let grant_id = super::super::provider::ProviderAccessGrantId::from_random_bytes([93; 32]);
        let grant_prefix =
            super::super::store_commit::provider_access_grant_semantic_prefix(&grant_id);
        let grant_bytes = b"accepted Serial successor provider grant";
        let successor = Box::pin(
            super::super::store_outbound::prepare_store_operation_candidate(
                &db,
                &storage,
                successor_plan,
                super::super::store_outbound::StoreOperationBatch::ProviderAccessGrant(
                    super::super::provider::StoreMemberProviderAccessGrantRef {
                        grant_id,
                        grant_hash: super::super::store_commit::ObjectHash::digest(grant_bytes),
                        object: crate::sync::storage::ExactObjectRef::new(
                            crate::storage::cloud::ObjectSlot::logical(format!(
                                "{grant_prefix}.json"
                            ))
                            .expect("valid provider grant slot"),
                            grant_bytes.len() as u64,
                            super::super::store_commit::ObjectHash::digest(grant_bytes),
                        ),
                    },
                ),
            ),
        )
        .await
        .expect("prepare accepted Serial successor candidate");
        let successor_materialization = Box::pin(
            super::super::store_outbound::publish_prepared_store_operation(
                &db,
                &storage,
                Some(&storage),
                Box::new(successor),
            ),
        )
        .await
        .expect_err("fixture database has not materialized the accepted predecessor");
        assert!(successor_materialization
            .to_string()
            .contains("durable predecessor is None"));

        assert_eq!(
            Box::pin(drain_outbound_store_acks(
                &db,
                &storage,
                Some(&storage),
                &signer,
                None,
            ))
            .await
            .expect("complete accepted acknowledgement ancestor"),
            1
        );
        assert_eq!(
            db.activated_store_ack(&outbound.reference.registration)
                .await
                .unwrap(),
            Some(outbound.reference.clone())
        );
        assert!(db
            .protocol_inert_object(outbound.reference.object)
            .await
            .unwrap()
            .is_none());
    }
}
