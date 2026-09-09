use super::store_database;
use crate::sync::test_helpers::TestStore;
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ExactObjectRef, ObjectSlot};
use coven_protocol::store_commit::ObjectHash;

struct MemberRemovalHistory {
    db: coven_database::Database,
    store: std::sync::Arc<TestStore>,
    device: crate::sync::test_helpers::TestDevice,
    removal: coven_database::OwnedVerifiedMergeMaterialization,
}

impl MemberRemovalHistory {
    async fn create() -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let member_pubkey = crate::sync::test_helpers::pubkey_hex(&member);
        let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let store = TestStore::create(
            &db,
            db_store_dir.clone(),
            "retained-removal-proof",
            owner.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create removal-proof Store");
        store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &member_pubkey,
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Retained removal proof",
            )
            .await
            .expect("admit removable member");
        let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
        store
            .activate_joined_device(
                &db,
                db_store_dir.clone(),
                &member_db,
                member_db_store_dir.clone(),
                &member,
                "2026-07-21T00:00:00Z",
            )
            .await
            .expect("activate removable member device");
        store
            .promote_active_member_fixture(
                &db,
                db_store_dir.clone(),
                &member_db,
                member_db_store_dir.clone(),
                &owner,
                &member,
                &encryption,
            )
            .await
            .expect("promote removable member to Owner");
        let custody = crate::sync::test_helpers::TestCustody::default();
        store
            .remove_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &member_pubkey,
                &encryption,
                &custody,
            )
            .await
            .expect("remove retained member");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &owner)
            .await
            .expect("bind retained-removal Store");
        let retained = device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("load retained removal history");
        let removal = retained
            .into_iter()
            .find(|materialization| {
                materialization
                    .history_evidence()
                    .membership_proof
                    .as_ref()
                    .is_some_and(|proof| {
                        matches!(
                            proof.entry_value.change,
                            coven_protocol::membership::StoreAuthorityChange::RemoveMember { .. }
                        )
                    })
            })
            .expect("removal activation is retained");
        Self {
            db,
            store,
            device,
            removal,
        }
    }

    async fn publish_snapshot(
        &self,
    ) -> (
        coven_protocol::store_commit::SnapshotMeta,
        coven_database::PublishedStoreSnapshot,
    ) {
        let directory = tempfile::tempdir().expect("create snapshot image directory");
        let database = store_database(&self.db);
        let image = database
            .capture_snapshot_image_for_test(
                self.store.root().clone(),
                directory.path().to_path_buf(),
                None,
            )
            .await
            .expect("create checkpoint snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("load snapshot coverage"),
        )
        .expect("derive snapshot coverage");
        let meta = self
            .device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish checkpoint snapshot");
        let published = database
            .latest_local_store_snapshot()
            .await
            .expect("load published snapshot")
            .expect("published snapshot is recorded");
        (meta, published)
    }
}

#[tokio::test]
async fn retained_commit_evidence_rejects_an_omitted_membership_removal() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let mut omitted = fixture.removal.history_evidence().clone();
    omitted.membership_proof = None;
    assert!(omitted
        .validate_for(fixture.removal.commit_ref(), fixture.removal.commit())
        .is_err());
}

#[tokio::test]
async fn membership_checkpoint_floor_includes_the_activating_control() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let (meta, _) = fixture.publish_snapshot().await;
    let control = meta
        .history_summary
        .membership_proofs
        .values()
        .find(|proof| {
            matches!(
                proof.entry_value.change,
                coven_protocol::membership::StoreAuthorityChange::RemoveMember { .. }
            )
        })
        .expect("retained history contains the removal control proof");
    assert!(meta
        .history_summary
        .membership_floor
        .effective_coordinates
        .contains(&control.entry.coord));
}

#[tokio::test]
async fn retained_membership_proof_rejects_an_incomplete_resolution_authority() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let mut evidence = fixture.removal.history_evidence().clone();
    let proof = evidence
        .membership_proof
        .as_mut()
        .expect("retained history contains a membership proof");
    let bytes = b"incomplete retained resolution authority";
    proof.resolution = Some(
        coven_protocol::membership::StoreMembershipConflictResolutionRef {
            conflict_hash: ObjectHash::digest(b"retained resolution conflict"),
            resolver_pubkey: "retained-resolution-resolver".to_string(),
            resolution_hash: ObjectHash::digest(bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical(
                    "store-v1/tests/incomplete-retained-resolution.json".to_string(),
                )
                .expect("valid retained resolution slot"),
                bytes.len() as u64,
                ObjectHash::digest(bytes),
            ),
        },
    );
    assert!(
        evidence
            .validate_for(fixture.removal.commit_ref(), fixture.removal.commit())
            .is_err(),
        "retained membership proof accepted a resolution reference without its signed value",
    );
}

#[tokio::test]
async fn signed_snapshot_rejects_an_omitted_pre_snapshot_membership_control() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let (meta, published) = fixture.publish_snapshot().await;
    let mut forged = meta;
    let summary = &mut forged.body_mut().history_summary;
    let removal = summary
        .membership_proofs
        .iter()
        .find_map(|(reference, proof)| {
            matches!(
                proof.entry_value.change,
                coven_protocol::membership::StoreAuthorityChange::RemoveMember { .. }
            )
            .then(|| reference.clone())
        })
        .expect("snapshot retains pre-snapshot removal control");
    summary.membership_proofs.remove(&removal);
    let forged = fixture
        .device
        .resign_snapshot_meta_for_test(forged)
        .await
        .expect("re-sign internally valid snapshot through Store authority");
    let forged_bytes = forged.to_bytes();
    let forged_reference = coven_protocol::store_commit::StoreSnapshotRef {
        snapshot_hash: forged.snapshot_hash(),
        object: ExactObjectRef::new(
            published.reference.object.slot().clone(),
            forged_bytes.len() as u64,
            ObjectHash::digest(&forged_bytes),
        ),
    };
    assert_eq!(
        fixture
            .device
            .parse_local_snapshot_meta_for_test(&forged_bytes, &forged_reference)
            .await
            .expect("re-signed omitted summary is internally valid"),
        forged,
    );
    let forged = coven_database::PublishedStoreSnapshot {
        reference: forged_reference,
        meta: forged,
    };
    assert!(
        fixture
            .device
            .verify_installable_snapshots_for_test(std::slice::from_ref(&forged))
            .await
            .is_err(),
        "snapshot authority accepted a signed summary that omitted exact cut history",
    );
}
