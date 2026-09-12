#[path = "baseline_tests.rs"]
mod baseline_tests;
#[path = "completion_tests.rs"]
mod completion_tests;
#[path = "offline_peer_tests.rs"]
mod offline_peer_tests;

use super::*;
use coven_keys::keys::{self, UserKeypair};
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::objects::ObjectSlot;
use coven_protocol::store_commit::{
    StoreCommitCoord, StoreDeviceRegistrationRef, StoreProtocolError,
};
use coven_storage::CloudSyncObjectStorage;
use std::collections::BTreeMap;

fn proof_object(path: &str) -> ExactObjectRef {
    let bytes = path.as_bytes();
    ExactObjectRef::new(
        ObjectSlot::logical(path.to_string()).expect("valid proof slot"),
        u64::try_from(bytes.len()).expect("proof length fits u64"),
        ObjectHash::digest(bytes),
    )
}

pub(super) async fn publish_current_snapshot(device: &crate::sync::test_helpers::TestDevice) {
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize reclaim snapshot writer");
    let mut snapshots = writer.snapshots();
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let cut = snapshots
        .capture_snapshot_cut(Some(&encryption))
        .await
        .expect("capture the accepted reclaim snapshot");
    snapshots
        .push_snapshot_cut(cut, "2026-07-16T00:00:00Z".to_string())
        .await
        .expect("publish reclaim snapshot");
}

/// An owner Store whose founder stream carries two acknowledged, snapshot-covered
/// Store packages, released from replay retention so both are reclaim-eligible.
struct ReclaimJourneyFixture {
    db: coven_database::Database,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    device: crate::sync::test_helpers::TestDevice,
    packages: Vec<StorePackageReclaimTarget>,
}

impl ReclaimJourneyFixture {
    async fn build(store_id: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) = crate::sync::test_helpers::TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            store_id,
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create Store");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind reclaim Store");

        let mut activations = Vec::new();
        for (sequence, row) in [
            (
                1,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                     VALUES ('reclaim-journey-1', 'first', NULL, \
                     '0000000001000-0000-reclaim-journey', '2026-01-01')",
            ),
            (
                2,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                     VALUES ('reclaim-journey-2', 'second', NULL, \
                     '0000000002000-0000-reclaim-journey', '2026-01-01')",
            ),
        ] {
            let changeset = crate::sync::test_helpers::open_test_db(
                crate::sync::test_helpers::test_store_dir(),
            )
            .capture_test_changeset(&[row])
            .await;
            let activation = store
                .publish_changeset("founder", sequence, &changeset, db.schema_version())
                .await
                .expect("publish package activation");
            activations.push(activation);
        }
        // A real captured image, not a placeholder: reclaim adopts the snapshot
        // it proves acknowledged as this device's replay baseline, and an image
        // that is not a database cannot serve a rewind. Publishing it the way
        // production does is also what releases the replay pins on the two
        // packages below — the fixture used to reach past that with a test-only
        // ownership release, which is the thing this suite is now checking.
        device
            .ensure_device_join_snapshot_for_test()
            .await
            .expect("publish and acknowledge a covering snapshot");

        let mut packages = Vec::new();
        for activation in activations {
            let commit = device
                .load_commit_for_test(&activation)
                .await
                .expect("load package activation");
            let package = commit
                .value()
                .store_package()
                .expect("activation carries a Store package")
                .clone();
            packages.push(StorePackageReclaimTarget {
                package,
                activation,
            });
        }

        Self {
            db,
            store,
            storage,
            home,
            device,
            packages,
        }
    }

    async fn stuck_operations(&self) -> Vec<coven_database::StuckReclaimOperation> {
        coven_database::StoreDatabase::new(&self.db)
            .stuck_reclaim_operations()
            .await
            .expect("read the stuck reclaim operations")
    }

    async fn retry_stuck(&self, operation_id: ObjectHash) -> Result<(), coven_database::DbError> {
        coven_database::StoreDatabase::new(&self.db)
            .retry_stuck_reclaim_operation(operation_id)
            .await
    }

    async fn reclaim(&self) -> Result<StoreReclaimResult, StoreReclaimError> {
        self.device
            .authorize_writer()
            .await
            .map_err(StoreReclaimError::from)?
            .reclaim_packages(&crate::sync::store::SettledCycle::default())
            .await
    }

    async fn materialized_frontier(&self) -> coven_protocol::store_commit::CommitFrontier {
        coven_protocol::store_commit::CommitFrontier::from_refs(
            self.device
                .materialized_frontier()
                .await
                .expect("read materialized frontier"),
        )
        .expect("shape materialized frontier")
    }

    async fn replay_note_count(&self) -> Result<i64, coven_database::DbError> {
        self.device.replay_row_count_for_test("notes").await
    }

    async fn package_is_present(&self, target: &StorePackageReclaimTarget) -> bool {
        let stream_id = target.activation.coord.stream_id.to_string();
        let prefix = coven_protocol::store_commit::package_semantic_prefix(
            target.package.candidate_family,
            &stream_id,
            target.activation.coord.sequence(),
            target.package.content_hash,
        );
        let context = ProtocolObjectContext::store_encrypted(
            self.store.root().store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        match self
            .storage
            .read_protocol_object(&context, &target.package.object, &prefix)
            .await
        {
            Ok(_) => true,
            Err(StorageError::NotFound(_)) => false,
            Err(error) => panic!("read reclaim package object: {error}"),
        }
    }

    fn package_deletes(&self, target: &StorePackageReclaimTarget) -> usize {
        let key = target.package.object.slot().logical_key();
        self.home
            .deletes_seen()
            .into_iter()
            // Opaque exact slots record as `<logical_key>#exact#<provider_id>`;
            // compare the logical part so a re-created object's new provider id
            // still counts as a delete of the same package.
            .filter(|deleted| deleted.split("#exact#").next() == Some(key))
            .count()
    }
}

#[tokio::test]
async fn reclaim_selects_the_latest_accepted_snapshot_without_acknowledgements() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        db_store_dir.clone(),
        "reclaim-stable-snapshot-selection",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind reclaim Store");
    let first_changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('stable-snapshot-row', 'stable', NULL, \
                 '0000000001000-0000-stable-snapshot', '2026-01-01')",
            ])
            .await;
    let first_commit = store
        .publish_changeset("founder", 1, &first_changeset, db.schema_version())
        .await
        .expect("publish first Store position");
    let StoreCommitCoord { stream_id, .. } = first_commit.coord;
    let first_coverage = CommitFrontier(BTreeMap::from([(stream_id, first_commit.clone())]));
    publish_current_snapshot(&device).await;
    device
        .publish_acknowledgement(first_coverage)
        .await
        .expect("acknowledge stable snapshot");
    let stable = coven_database::StoreDatabase::new(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load stable snapshot")
        .expect("stable snapshot exists");

    let second_changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('unstable-snapshot-row', 'unstable', NULL, \
                 '0000000002000-0000-unstable-snapshot', '2026-01-01')",
            ])
            .await;
    store
        .publish_changeset("founder", 3, &second_changeset, db.schema_version())
        .await
        .expect("publish second Store position");
    publish_current_snapshot(&device).await;
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize reclaim writer");
    let selected = writer
        .reclaim()
        .choose_snapshot()
        .await
        .expect("select the stable reclaim snapshot");

    assert_ne!(selected.snapshot().reference, stable.reference);
    assert_eq!(
        selected.reference(),
        coven_database::StoreDatabase::new(&db)
            .store_current_publication()
            .await
            .expect("read current publication")
            .record()
            .latest_snapshot()
            .expect("accepted snapshot exists")
            .clone(),
    );
}

#[tokio::test]
async fn signed_reclaim_authority_rejects_relocated_objects_and_unproven_deletion() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let (store, cloud_storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "signed-reclaim-authority",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim', '2026-01-01')",
            ])
            .await;
    let activation = store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish package activation");
    let founder_authority = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let loaded = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("load reclaim Store");
    let activated = loaded
        .load_commit_for_test(&activation)
        .await
        .expect("load package activation");
    assert_eq!(activated.author(), founder_authority.registration());
    let package = activated
        .store_package()
        .expect("activation carries Store package")
        .clone();
    let evidence = ReclaimEvidence::signed(
        store.root().store_root_hash,
        ReclaimClaim::StorePackage(StorePackageReclaimClaim {
            target: StorePackageReclaimTarget {
                package: package.clone(),
                activation: activation.clone(),
            },
        }),
        &signer,
    )
    .expect("sign reclaim evidence");
    let evidence_context = ProtocolObjectContext::store_encrypted(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreReclaimEvidence,
    );
    let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
    let evidence_slot = cloud_storage
        .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
        .await
        .expect("allocate evidence slot");
    let prepared_evidence = cloud_storage
        .prepare_protocol_object(
            &evidence_context,
            evidence_slot,
            &evidence_prefix,
            evidence.to_bytes(),
        )
        .expect("prepare evidence");
    cloud_storage
        .create_protocol_object(&prepared_evidence)
        .await
        .expect("create evidence");
    let evidence_ref =
        ReclaimEvidenceRef::from_evidence(&evidence, prepared_evidence.reference().clone());
    let authorization = ReclaimAuthorization::signed(
        store.root().store_root_hash,
        ReclaimTarget::StorePackage(StorePackageReclaimTarget {
            package,
            activation,
        }),
        evidence_ref,
        StoreReclaimAuthority {
            membership: activated.membership_state.clone(),
            owner_grant: loaded
                .protocol_root_for_test()
                .descriptor
                .founder_grant
                .clone(),
        },
        &signer,
    );
    let authorization_context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreReclaimAuthorization,
    );
    let authorization_prefix =
        reclaim_authorization_semantic_prefix(authorization.authorization_hash());
    let authorization_slot = cloud_storage
        .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
        .await
        .expect("allocate authorization slot");
    let prepared_authorization = cloud_storage
        .prepare_protocol_object(
            &authorization_context,
            authorization_slot,
            &authorization_prefix,
            authorization.to_bytes(),
        )
        .expect("prepare authorization");
    cloud_storage
        .create_protocol_object(&prepared_authorization)
        .await
        .expect("create authorization");
    let authorization_ref = ReclaimAuthorizationRef::from_authorization(
        &authorization,
        prepared_authorization.reference().clone(),
    );

    let mut relocated = authorization.clone();
    let ReclaimTarget::StorePackage(relocated_target) = &mut relocated.body_mut().target else {
        unreachable!("Store package reclaim target");
    };
    relocated_target.package.object =
        proof_object("store-v1/candidates/family/packages/device/1/another-package.pkg");
    assert!(authorization.verify(&keys::public_key_hex(&signer)).is_ok());
    assert!(matches!(
        relocated.verify(&keys::public_key_hex(&signer)),
        Err(StoreProtocolError::InvalidSignature)
    ));

    db.release_retained_replay_ownership_for_test()
        .await
        .expect("release retained replay package ownership");
    let target = evidence.claim.target();
    let super::ReclaimActivation::Commit(target_activation) = target.activation() else {
        panic!("a Store package reclaim target is activated by a Store commit");
    };
    let mut authorization_activation = target_activation.clone();
    authorization_activation.coord = StoreCommitCoord {
        stream_id: authorization_activation.coord.stream_id,
        sequence: authorization_activation.coord.sequence() + 1,
    };
    authorization_activation.commit_hash = ObjectHash::digest(b"reclaim authorization commit");
    authorization_activation.object = proof_object("store-v1/commits/reclaim-authorization.json");
    let operation = DurableStoreReclaimOperation::Authorized {
        authorization: authorization_ref.clone(),
        activation: authorization_activation,
    };
    let mut writer = loaded
        .authorize_writer()
        .await
        .expect("authorize reclaim writer");
    let deletion = writer.reclaim().execute_delete(operation).await;
    assert!(
        deletion.is_err(),
        "nonexistent snapshot and acknowledgement refs must not authorize deletion"
    );
    let resolved_target = evidence.claim.target();
    let ReclaimTarget::StorePackage(target) = &resolved_target else {
        unreachable!("Store package reclaim target");
    };
    let StoreCommitCoord { stream_id, .. } = target.activation.coord;
    cloud_storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root().store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            &target.package.object,
            &coven_protocol::store_commit::package_semantic_prefix(
                target.package.candidate_family,
                &stream_id.to_string(),
                target.activation.coord.sequence(),
                target.package.content_hash,
            ),
        )
        .await
        .expect("unverified reclaim proof must leave its target readable");
}

#[tokio::test]
async fn missing_or_retracted_merge_activation_blocks_reclaim_deletion() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let (store, cloud_storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "reclaim-activation-head",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-head-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim-head', '2026-01-01')",
            ])
            .await;
    let target_activation = store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish target package activation");
    let loaded = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("load reclaim Store");
    let target_commit = loaded
        .load_commit_for_test(&target_activation)
        .await
        .expect("load target activation");
    let target_package = target_commit
        .value()
        .store_package()
        .expect("target activation carries a Store package")
        .clone();
    let StoreCommitCoord { stream_id, .. } = target_activation.coord;
    let coverage = CommitFrontier(BTreeMap::from([(stream_id, target_activation.clone())]));
    publish_current_snapshot(&loaded).await;
    loaded
        .publish_acknowledgement(coverage)
        .await
        .expect("publish covering acknowledgement");
    db.release_retained_replay_ownership_for_test()
        .await
        .expect("release target retained replay ownership");
    let mut writer = loaded
        .authorize_writer()
        .await
        .expect("authorize reclaim writer");
    let mut reclaim = writer.reclaim();
    reclaim
        .prepare_authorization(ReclaimClaim::StorePackage(StorePackageReclaimClaim {
            target: StorePackageReclaimTarget {
                package: target_package.clone(),
                activation: target_activation.clone(),
            },
        }))
        .await
        .expect("prepare reclaim authorization");
    let candidate = coven_database::StoreDatabase::new(&db)
        .store_reclaim_operations()
        .await
        .expect("load reclaim candidate")
        .into_iter()
        .next()
        .expect("reclaim candidate exists");
    let prepared_candidate = candidate
        .candidate()
        .expect("reclaim operation has a candidate");
    let activation_publication = prepared_candidate
        .publication
        .reference()
        .expect("reference reclaim activation publication");
    let activation_publication_prepared = prepared_candidate
        .publication
        .prepared_entry()
        .expect("prepare reclaim activation publication");
    reclaim
        .drive_candidate(candidate)
        .await
        .expect("activate reclaim authorization");
    cloud_storage
        .delete_protocol_object(&activation_publication.object)
        .await
        .expect("remove reclaim activation publication");
    let authorized = coven_database::StoreDatabase::new(&db)
        .store_reclaim_operations()
        .await
        .expect("load activated reclaim")
        .into_iter()
        .next()
        .expect("activated reclaim exists");

    let deletion = reclaim.execute_delete(authorized.clone()).await;

    assert!(
        deletion.is_err(),
        "a reclaim authorization without its exact Store publication must not delete"
    );
    cloud_storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root().store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            &target_package.object,
            &coven_protocol::store_commit::package_semantic_prefix(
                target_package.candidate_family,
                &stream_id.to_string(),
                target_activation.coord.sequence(),
                target_package.content_hash,
            ),
        )
        .await
        .expect("missing activation authority leaves target readable");

    cloud_storage
        .create_protocol_object(&activation_publication_prepared)
        .await
        .expect("restore exact reclaim activation publication");
    let activation_commit = match &authorized {
        DurableStoreReclaimOperation::Authorized { activation, .. } => activation.clone(),
        _ => unreachable!("fixture has an activated reclaim"),
    };
    db.delete_exact_materialized_commit_for_test(activation_commit)
        .await
        .expect("retract reclaim activation materialization");

    assert!(
        reclaim.execute_delete(authorized).await.is_err(),
        "a retracted Merge reclaim activation must not delete"
    );
    cloud_storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root().store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            &target_package.object,
            &coven_protocol::store_commit::package_semantic_prefix(
                target_package.candidate_family,
                &stream_id.to_string(),
                target_activation.coord.sequence(),
                target_package.content_hash,
            ),
        )
        .await
        .expect("retracted activation authority leaves target readable");
}

/// A delete failure between authorization activation and object deletion leaves
/// the already-deleted package gone and the failing one still present; a restart
/// deletes exactly the remaining package and never re-issues the completed delete.
#[tokio::test]
async fn interrupted_reclaim_deletes_only_the_remaining_package_on_restart() {
    let fixture = ReclaimJourneyFixture::build("reclaim-crash-resume").await;
    for target in &fixture.packages {
        assert!(
            fixture.package_is_present(target).await,
            "every covered package is present before reclamation",
        );
    }

    // The two package deletions are the last exact deletes of a reclaim run and
    // arrive in an order fixed by the (per-run random) authorization identities.
    // Fail the second one whichever it is: the first package deletes durably, the
    // second's delete fails, and the run surfaces the error to its initiator.
    let package_slots: Vec<&ObjectSlot> = fixture
        .packages
        .iter()
        .map(|target| target.package.object.slot())
        .collect();
    fixture.home.fail_nth_exact_delete_of(&package_slots, 2);

    let interrupted = fixture.reclaim().await;
    assert!(
        interrupted.is_err(),
        "the delete failure fails the reclaim to its initiator: {interrupted:?}",
    );
    assert!(
        fixture.stuck_operations().await.is_empty(),
        "a transport failure says nothing about the operation, so it stays runnable",
    );

    let present: Vec<&StorePackageReclaimTarget> = {
        let mut present = Vec::new();
        for target in &fixture.packages {
            if fixture.package_is_present(target).await {
                present.push(target);
            }
        }
        present
    };
    assert_eq!(
        present.len(),
        1,
        "exactly one package survives the interrupted deletion",
    );
    let survivor = present[0];
    for target in &fixture.packages {
        assert_eq!(
            fixture.package_deletes(target),
            usize::from(!std::ptr::eq(target, survivor)),
            "only the already-deleted package has a recorded delete",
        );
    }

    let resumed = fixture
        .reclaim()
        .await
        .expect("restart resumes reclamation");
    assert_eq!(
        (resumed.packages_deleted, resumed.physical_copies_deleted),
        (1, 1),
        "the restart reclaims exactly the one remaining package",
    );
    for target in &fixture.packages {
        assert!(
            !fixture.package_is_present(target).await,
            "every covered package is deleted after the restart",
        );
        assert_eq!(
            fixture.package_deletes(target),
            1,
            "each package is deleted exactly once across the interrupted and resumed runs",
        );
    }
}

/// A reclaim operation whose delete the provider refuses for good is stuck
/// after that one failure: the pass finishes the operations behind it, later
/// passes spend nothing on it, and it runs again only when the host asks.
///
/// The old shape failed the whole cycle on that refusal, so one operation the
/// provider would never accept held every operation behind it — and the loop
/// paid for the same refusal every cycle, forever.
#[tokio::test]
async fn a_refused_reclaim_delete_leaves_one_operation_stuck_and_finishes_the_rest() {
    let fixture = ReclaimJourneyFixture::build("reclaim-stuck-operation").await;
    let package_slots: Vec<&ObjectSlot> = fixture
        .packages
        .iter()
        .map(|target| target.package.object.slot())
        .collect();
    // Refuse the first package delete permanently, whichever of the two it is.
    fixture
        .home
        .fail_nth_exact_delete_of_permanently(&package_slots, 1);

    let refused = fixture
        .reclaim()
        .await
        .expect("a deterministic refusal does not fail the pass");
    assert_eq!(
        (refused.packages_deleted, refused.stuck),
        (1, 1),
        "the pass deletes the package behind the refused one and reports what it left stuck",
    );
    let stuck = fixture.stuck_operations().await;
    let [stuck_operation] = stuck.as_slice() else {
        panic!("exactly one operation is stuck: {stuck:?}");
    };
    assert!(
        stuck_operation.error.contains("refused to delete"),
        "the mark carries the provider's refusal: {}",
        stuck_operation.error,
    );
    let refused_target = fixture
        .packages
        .iter()
        .find(|target| target.package.object == *stuck_operation.target.object())
        .expect("the stuck operation names one of the covered packages");
    assert!(
        fixture.package_is_present(refused_target).await,
        "the refused package is still at the provider",
    );

    let deletes_before = fixture.home.exact_delete_count();
    let skipped = fixture
        .reclaim()
        .await
        .expect("a later pass runs with the operation still stuck");
    assert_eq!(
        (skipped.packages_deleted, skipped.stuck),
        (0, 1),
        "a later pass reports the operation as still waiting on a person",
    );
    assert_eq!(
        fixture.home.exact_delete_count(),
        deletes_before,
        "a stuck operation costs the provider nothing",
    );

    fixture
        .retry_stuck(stuck_operation.operation_id)
        .await
        .expect("the host asks for the operation back");
    assert!(
        fixture
            .retry_stuck(stuck_operation.operation_id)
            .await
            .is_err(),
        "an operation that is not stuck has no retry to accept",
    );

    let retried = fixture
        .reclaim()
        .await
        .expect("the cleared operation runs again");
    assert_eq!(
        (retried.packages_deleted, retried.stuck),
        (1, 0),
        "the retry deletes the package the refusal left behind",
    );
    for target in &fixture.packages {
        assert!(
            !fixture.package_is_present(target).await,
            "every covered package is deleted once the refusal is cleared",
        );
    }
}

/// The whole reclaim journal runs end to end: both acknowledged, snapshot-covered
/// packages are proof-gated, deleted, and completed in one uninterrupted pass.
#[tokio::test]
async fn reclaim_journal_deletes_every_covered_package_in_one_pass() {
    let fixture = ReclaimJourneyFixture::build("reclaim-journal-full-pass").await;
    let snapshot = fixture
        .device
        .latest_local_store_snapshot_for_test()
        .await
        .expect("read accepted coverage")
        .expect("fixture published a snapshot");
    let publications = &snapshot.meta.history_summary.reclaim.publications;
    assert_eq!(
        publications.len(),
        2,
        "coverage retires both package publications"
    );
    for publication in publications.values() {
        assert!(fixture.home.contains_exact_object(&publication.object));
    }
    let result = fixture.reclaim().await.expect("reclaim covered packages");
    assert_eq!(
        (result.packages_deleted, result.physical_copies_deleted),
        (2, 4),
        "reclaim deletes two packages and their two retired publication entries",
    );
    for publication in publications.values() {
        assert!(!fixture.home.contains_exact_object(&publication.object));
    }
    for target in &fixture.packages {
        assert!(
            !fixture.package_is_present(target).await,
            "every covered package is deleted",
        );
        assert_eq!(
            fixture.package_deletes(target),
            1,
            "each package is deleted exactly once",
        );
    }

    let idempotent = fixture
        .reclaim()
        .await
        .expect("a second reclaim over the same coverage is a no-op");
    assert_eq!(
        (
            idempotent.packages_deleted,
            idempotent.physical_copies_deleted
        ),
        (0, 0),
        "the recorded reclaim operations are not repeated",
    );
    assert_eq!(
        idempotent.store_packages.authorized, 0,
        "a second pass signs no fresh authorization",
    );
    assert_eq!(
        idempotent.store_packages.already_authorized, 2,
        "it reports both targets as already journalled rather than as nothing to do",
    );
}

/// A run that deletes nothing says which step declined, not just that it
/// deleted nothing.
///
/// This is the shape that cost a live store a night: the reclaim stage ran
/// every cycle, spent seconds, deleted nothing, and emitted no line about what
/// it had considered — so "declining" and "nothing to do" were the same
/// observation from outside. The two commonest declines are deliberately
/// turned into an empty target list so Store trouble cannot block Circle
/// reclaim, which is what swallowed the reason along with the error. The
/// report carries it instead.
#[tokio::test]
async fn a_reclaim_that_deletes_nothing_reports_the_step_that_declined() {
    let fixture = ReclaimJourneyFixture::build("reclaim-decline-visibility").await;

    let result = fixture.reclaim().await.expect("reclaim covered packages");

    assert_eq!(
        result.store_packages.coverage,
        super::StorePackageReclaimCoverage::Snapshot {
            snapshot: coven_database::StoreDatabase::new(&fixture.db)
                .store_current_publication()
                .await
                .expect("read accepted publication")
                .record()
                .latest_snapshot()
                .expect("accepted snapshot exists")
                .clone(),
        },
        "reclaim reports the exact accepted snapshot that licenses retirement",
    );
    assert_eq!(
        result.store_packages.targets_considered, 2,
        "the report counts the package-bearing commits behind the coverage",
    );
    assert_eq!(
        result.store_packages.authorized, 2,
        "and how many of them this run signed an authorization for",
    );
    assert_eq!(
        result.store_packages.retained_for_replay, 0,
        "none of them were pinned by a retained materialization",
    );
    assert_eq!(
        result.store_packages.targets_considered,
        result.store_packages.retained_for_replay
            + result.store_packages.already_authorized
            + result.store_packages.authorized,
        "every considered target is accounted for by exactly one outcome",
    );
}

/// A single-stream commit frontier at `sequence`, deterministic in `stream`.
fn frontier_at(stream: &str, sequence: u64) -> coven_protocol::store_commit::CommitFrontier {
    let stream_id = coven_protocol::causal_grants::AuthorStreamId::from_digest(ObjectHash::digest(
        stream.as_bytes(),
    ));
    let commit = coven_protocol::store_commit::StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{stream}:{sequence}").as_bytes()),
        object: proof_object(&format!(
            "store-v1/candidates/f/commits/{stream}/{sequence}/hash"
        )),
    };
    coven_protocol::store_commit::CommitFrontier(std::collections::BTreeMap::from([(
        stream_id, commit,
    )]))
}

/// The bootstrap-reclaim strict-domination guard: a stable snapshot supersedes a
/// seed only when its cut covers the seed AND is not equal to it. The equal-cut
/// boundary (a snapshot at the recipient's exact bootstrap cut) must not reclaim —
/// dropping the strict inequality flips this case and reclaims a live seed.
#[test]
fn snapshot_supersedes_seed_requires_strict_domination() {
    let seed = frontier_at("owner", 4);
    assert!(
        !super::candidates::snapshot_supersedes_seed(&frontier_at("owner", 4), &seed),
        "a snapshot whose cut equals the seed exactly does not supersede it"
    );
    assert!(
        super::candidates::snapshot_supersedes_seed(&frontier_at("owner", 5), &seed),
        "a snapshot strictly past the seed on its stream supersedes it"
    );
    assert!(
        !super::candidates::snapshot_supersedes_seed(&frontier_at("owner", 3), &seed),
        "a snapshot behind the seed does not cover it and cannot supersede it"
    );
}

/// A superseded snapshot generation's membership rollup is reclaimed; the
/// newest generation's stays.
///
/// A rollup is reachable only through the generation that names it, and only the
/// newest generation's is ever read — a joining device takes the newest listed
/// snapshot and follows its `membership_rollup`. Nothing lists
/// `store-v1/membership-rollups/`, so a superseded generation's rollup is not
/// merely unused: it is unfindable, and before this it stayed at the provider
/// forever, one per distinct membership frontier the store ever published over.
///
/// The rollup is owned by the generation that published it rather than deleted
/// with it, because the object is content-addressed over the membership
/// frontier: a generation published while membership stood still names the
/// object an earlier one already owns, and it has to survive until neither does.
#[tokio::test]
async fn superseded_membership_rollups_are_reclaimed_and_the_newest_stays() {
    let fixture = ReclaimJourneyFixture::build("membership-rollup-reclaim").await;
    let first = fixture
        .device
        .latest_local_store_snapshot_for_test()
        .await
        .expect("read the first snapshot")
        .expect("the fixture published one");

    // A second generation over a strictly later cut, acknowledged the way the
    // first one was.
    let changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('rollup-reclaim-3', 'third', NULL, \
                 '0000000003000-0000-rollup-reclaim', '2026-01-01')",
            ])
            .await;
    fixture
        .store
        .publish_changeset("founder", 4, &changeset, fixture.device.schema_version())
        .await
        .expect("publish a third package activation");
    publish_current_snapshot(&fixture.device).await;
    let second = fixture
        .device
        .latest_local_store_snapshot_for_test()
        .await
        .expect("read successor snapshot")
        .expect("successor snapshot exists");
    assert_ne!(
        first.meta.membership_rollup.object, second.meta.membership_rollup.object,
        "the two generations must name different rollups for this to test anything",
    );
    assert!(
        fixture
            .store
            .contains_membership_rollup(&first.meta.membership_rollup)
            .await
            .expect("read the superseded rollup"),
        "the superseded generation's rollup is at the provider before reclaim",
    );

    fixture.reclaim().await.expect("run reclaim");

    assert!(
        !fixture
            .store
            .contains_membership_rollup(&first.meta.membership_rollup)
            .await
            .expect("read the superseded rollup after reclaim"),
        "the superseded generation's rollup is deleted",
    );
    assert!(
        fixture
            .store
            .contains_membership_rollup(&second.meta.membership_rollup)
            .await
            .expect("read the newest rollup after reclaim"),
        "the newest generation's rollup stays: it supersedes nothing, and it is \
         the one a joining device reads",
    );
}
