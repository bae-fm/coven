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

/// An owner Store whose founder stream carries two acknowledged, snapshot-covered
/// Store packages, released from replay retention so both are reclaim-eligible.
struct ReclaimJourneyFixture {
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
            store,
            storage,
            home,
            device,
            packages,
        }
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
async fn reclaim_selects_an_older_stable_snapshot_over_a_newer_unacknowledged_snapshot() {
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
    device
        .publish_snapshot(b"stable reclaim snapshot".to_vec(), first_coverage.clone())
        .await
        .expect("publish stable snapshot");
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
    let second_commit = store
        .publish_changeset("founder", 3, &second_changeset, db.schema_version())
        .await
        .expect("publish second Store position");
    device
        .publish_snapshot(
            b"unacknowledged reclaim snapshot".to_vec(),
            CommitFrontier(BTreeMap::from([(stream_id, second_commit)])),
        )
        .await
        .expect("publish unacknowledged snapshot");
    let registrations = coven_database::StoreDatabase::new(&db)
        .activated_store_device_registration_records()
        .await
        .expect("load active registrations");

    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize reclaim writer");
    let selected = writer
        .reclaim()
        .choose_snapshot(&registrations)
        .await
        .expect("select the stable reclaim snapshot");

    assert_eq!(selected.snapshot.reference, stable.reference);
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
            covering_snapshot: StoreSnapshotLocator {
                author_registration: founder_authority.registration_ref().clone(),
                snapshot: coven_protocol::store_commit::StoreSnapshotRef {
                    generation: 0,
                    snapshot_hash: ObjectHash::digest(b"covering snapshot"),
                    object: proof_object("store-v1/snapshots/founder/covering"),
                },
            },
            acknowledgements: vec![StoreAckRef {
                registration: founder_authority.registration_ref().clone(),
                sequence: 1,
                ack_hash: ObjectHash::digest(b"acknowledgement"),
                object: proof_object("store-v1/acks/founder/1.json"),
            }],
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

    let receipt = founder_authority
        .sign_reclaim_receipt_for_test(
            store.root().store_root_hash,
            authorization_ref,
            authorization.authority.membership.clone(),
            loaded
                .protocol_root_for_test()
                .descriptor
                .founder_provider_admin
                .grant_id
                .clone(),
        )
        .expect("sign reclaim receipt");
    let mut reassigned = receipt.clone();
    reassigned.body_mut().provider_admin_grant =
        coven_protocol::provider::ProviderAdminGrantId(ObjectHash::digest(b"another admin"));
    assert!(matches!(
        reassigned.verify(founder_authority.registration()),
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
        authorization: receipt.authorization.clone(),
        activation: ReclaimCommitActivation::new(
            authorization_activation,
            coven_protocol::store_commit::StoreDeviceHeadRef {
                head_hash: ObjectHash::digest(b"reclaim authorization head"),
                object: proof_object("store-v1/heads/reclaim-authorization.json"),
            },
        )
        .expect("valid reclaim activation"),
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
    loaded
        .publish_snapshot(b"reclaim activation snapshot".to_vec(), coverage.clone())
        .await
        .expect("publish covering snapshot");
    loaded
        .publish_acknowledgement(coverage)
        .await
        .expect("publish covering acknowledgement");
    let snapshot = coven_database::StoreDatabase::new(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load covering snapshot")
        .expect("covering snapshot exists");
    let acknowledgement = coven_database::StoreDatabase::new(&db)
        .latest_local_store_ack()
        .await
        .expect("load covering acknowledgement")
        .expect("covering acknowledgement exists")
        .reference;
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
            covering_snapshot: StoreSnapshotLocator {
                author_registration: snapshot.meta.author_registration.clone(),
                snapshot: snapshot.reference.clone(),
            },
            acknowledgements: vec![acknowledgement],
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
    let activation_head = prepared_candidate.head_ref();
    let activation_head_prepared = prepared_candidate
        .prepared_head()
        .expect("prepare reclaim activation head");
    reclaim
        .drive_candidate(candidate)
        .await
        .expect("activate reclaim authorization");
    cloud_storage
        .delete_protocol_object(&activation_head.object)
        .await
        .expect("remove reclaim activation head");
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
        "a reclaim authorization without its exact Merge activation head must not delete"
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
        .create_protocol_object(&activation_head_prepared)
        .await
        .expect("restore exact reclaim activation head");
    let activation_commit = match &authorized {
        DurableStoreReclaimOperation::Authorized { activation, .. } => activation.commit().clone(),
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

/// The whole reclaim journal runs end to end: both acknowledged, snapshot-covered
/// packages are proof-gated, deleted, and receipted in one uninterrupted pass.
#[tokio::test]
async fn reclaim_journal_deletes_every_covered_package_in_one_pass() {
    let fixture = ReclaimJourneyFixture::build("reclaim-journal-full-pass").await;
    let result = fixture.reclaim().await.expect("reclaim covered packages");
    assert_eq!(
        (result.packages_deleted, result.physical_copies_deleted),
        (2, 2),
    );
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
        super::StorePackageReclaimCoverage::Snapshot { generation: 0 },
        "a run that found coverage names the generation it deleted behind \
         (the fixture's covering snapshot is its first, so generation zero)",
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

/// A standing device advances its own replay baseline over the snapshot it
/// acknowledges, and that is what releases the packages behind it.
///
/// This is the shape that cost a live store 87MB and every package it ever
/// wrote. A device that joins gets a baseline at the snapshot it installs, but
/// one that has been in the store since the beginning never moved its own: its
/// baseline stayed at genesis, so its retained-replay closure spanned all of
/// history and pinned every package forever. Reclaim selected the right
/// snapshot, found the targets behind it, and declined every one of them as
/// retained for replay — for as long as the store existed.
///
/// The fixture acknowledges its snapshot while it builds, so the advance has
/// already happened by the time reclaim runs; what reclaim shows is the
/// consequence — nothing left pinned, every target authorized.
#[tokio::test]
async fn a_standing_device_advances_its_baseline_and_releases_what_it_pinned() {
    let fixture = ReclaimJourneyFixture::build("reclaim-baseline-advance").await;

    let result = fixture.reclaim().await.expect("reclaim covered packages");

    assert_eq!(
        result.store_packages.retained_for_replay, 0,
        "the acknowledgement's advance left no target pinned for replay",
    );
    assert_eq!(
        result.store_packages.authorized, result.store_packages.targets_considered,
        "and every considered target is authorized instead of declined",
    );
    assert!(
        result.store_packages.targets_considered > 0,
        "the run had targets to consider in the first place",
    );
}

/// Acknowledging a snapshot is what moves the baseline, and it moves once.
///
/// Rebuilding the baseline image replays the whole retained history, so a
/// device whose baseline already stands at the snapshot it is acknowledging
/// must decline before paying for it.
#[tokio::test]
async fn a_baseline_already_at_the_coverage_does_not_advance_again() {
    let fixture = ReclaimJourneyFixture::build("reclaim-baseline-settled").await;
    let frontier = fixture.materialized_frontier().await;

    let again = fixture
        .device
        .advance_baseline_by_acknowledging(frontier)
        .await
        .expect("acknowledge the snapshot a second time");

    assert!(
        again.is_none(),
        "the baseline already stands at the acknowledged snapshot, so nothing is rebuilt",
    );
}

/// Replay still reconstructs the store after the baseline moves.
///
/// The advance retires the retained rows the new cut covers, which is only safe
/// because the baseline image restates what they replayed. If it did not, this
/// count would come back short by the retired commits' rows.
#[tokio::test]
async fn replay_reconstructs_the_store_after_the_baseline_advances() {
    let fixture = ReclaimJourneyFixture::build("reclaim-baseline-replay").await;

    let after = fixture
        .replay_note_count()
        .await
        .expect("replay after advancing");
    assert_eq!(
        after, 2,
        "replay from the advanced baseline reproduces both published notes",
    );
}

/// The acknowledgement a device has already published is the licence, and it
/// goes on licensing without being restated.
///
/// A quiet device says nothing new: its standing acknowledgement still asserts
/// everything true, so the cycle stages no acknowledgement at all. If moving
/// the baseline rode on staging one, a device that acknowledged a snapshot on a
/// build without the advance would stay on its old baseline for as long as it
/// had nothing to say — pinning every package behind that snapshot, which is
/// exactly the state this whole change exists to end.
#[tokio::test]
async fn a_standing_acknowledgement_advances_a_baseline_that_never_moved() {
    let fixture = StandingAcknowledgementFixture::build("standing-ack-advance").await;

    let advanced = fixture.stand_on_acknowledged_snapshot().await;

    assert!(
        advanced.retired_commits > 0,
        "advancing retires the retained materializations the acknowledged cut covers, retired {}",
        advanced.retired_commits,
    );
    assert!(
        advanced.released_pins > 0,
        "and releases the replay pins those materializations held, released {}",
        advanced.released_pins,
    );
}

/// The live shape: the licence is in history, not in the latest word.
///
/// An acknowledgement names a snapshot only while that snapshot still describes
/// the Store's devices. Register a device and every acknowledgement after it
/// names nothing — the standing one included — because there is no published
/// snapshot left to name. The device has still said it holds the older one, in
/// an acknowledgement its own retained history carries, and that statement is
/// what licenses the advance. A store in this state sat on a genesis baseline
/// with two hundred retained rows, reporting every reclaim target as retained
/// for replay, cycle after cycle.
#[tokio::test]
async fn a_baseline_advances_over_a_snapshot_only_history_remembers_acknowledging() {
    let fixture = StandingAcknowledgementFixture::build("standing-ack-overtaken").await;
    fixture.overtake_the_acknowledged_device_state().await;
    fixture.acknowledge_naming_no_snapshot().await;

    assert!(
        fixture.standing_acknowledgement_names_no_snapshot().await,
        "the fixture reproduces the live shape: the latest word names no snapshot",
    );

    let advanced = fixture.stand_on_acknowledged_snapshot().await;

    assert!(
        advanced.retired_commits > 0,
        "the acknowledgement history carries licenses the advance, retired {}",
        advanced.retired_commits,
    );
}

/// Once a device has caught up, the stage says so and reads nothing.
#[tokio::test]
async fn a_baseline_at_the_acknowledged_coverage_declines_and_says_why() {
    let fixture = StandingAcknowledgementFixture::build("standing-ack-settled").await;
    fixture.stand_on_acknowledged_snapshot().await;

    let outcome = fixture
        .device
        .stand_on_acknowledged_snapshot()
        .await
        .expect("stand on the acknowledged snapshot again");

    assert_eq!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::BaselineAtCoverage { generation: 0 },
        ),
        "the second pass reports the steady state rather than a silent nothing",
    );
}

/// A device that has acknowledged no snapshot says that, rather than nothing.
#[tokio::test]
async fn a_device_that_acknowledged_no_snapshot_declines_and_says_why() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "no-acknowledged-snapshot",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Store");

    let outcome = device
        .stand_on_acknowledged_snapshot()
        .await
        .expect("stand on nothing");

    assert_eq!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::NoAcknowledgedSnapshot,
        ),
    );
}

/// Two changesets, a published snapshot, and an acknowledgement of it made the
/// way a build without the advance made one: the statement is published and the
/// baseline never moved.
struct StandingAcknowledgementFixture {
    db: coven_database::Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    device: crate::sync::test_helpers::TestDevice,
    signer: UserKeypair,
}

impl StandingAcknowledgementFixture {
    async fn build(store_id: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            store_id,
            signer.clone(),
            home,
        )
        .await
        .expect("create Store");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind Store");
        for (sequence, row) in [
            (
                1,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('standing-1', 'first', NULL, \
                 '0000000001000-0000-standing', '2026-01-01')",
            ),
            (
                2,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('standing-2', 'second', NULL, \
                 '0000000002000-0000-standing', '2026-01-01')",
            ),
        ] {
            let changeset = crate::sync::test_helpers::open_test_db(
                crate::sync::test_helpers::test_store_dir(),
            )
            .capture_test_changeset(&[row])
            .await;
            store
                .publish_changeset("founder", sequence, &changeset, db.schema_version())
                .await
                .expect("publish package activation");
        }
        let image_dir = tempfile::tempdir().expect("snapshot image dir");
        let image = coven_database::StoreDatabase::new(&db)
            .capture_snapshot_image_for_test(
                store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture a real snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&db)
                .materialized_frontier()
                .await
                .expect("materialized frontier"),
        )
        .expect("frontier");
        device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish the snapshot");
        device
            .publish_acknowledgement_without_advancing(coverage)
            .await
            .expect("acknowledge it the way a build without the advance did");
        Self {
            db,
            db_store_dir,
            store,
            device,
            signer,
        }
    }

    async fn frontier(&self) -> coven_protocol::store_commit::CommitFrontier {
        coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&self.db)
                .materialized_frontier()
                .await
                .expect("read materialized frontier"),
        )
        .expect("shape materialized frontier")
    }

    async fn stand_on_acknowledged_snapshot(&self) -> coven_database::AdvancedReplayBaseline {
        match self
            .device
            .stand_on_acknowledged_snapshot()
            .await
            .expect("stand on the acknowledged snapshot")
        {
            crate::sync::store::ReplayBaselineAdvance::Advanced(advanced) => advanced,
            crate::sync::store::ReplayBaselineAdvance::Declined(decline) => {
                panic!("declined to advance: {}", decline.as_str())
            }
        }
    }

    /// Publish an acknowledgement now that nothing is acknowledgeable, so the
    /// latest word names no snapshot.
    async fn acknowledge_naming_no_snapshot(&self) {
        self.device
            .publish_acknowledgement_without_advancing(self.frontier().await)
            .await
            .expect("publish an acknowledgement that names no snapshot");
    }

    async fn standing_acknowledgement_names_no_snapshot(&self) -> bool {
        coven_database::StoreDatabase::new(&self.db)
            .latest_local_store_ack()
            .await
            .expect("read the standing acknowledgement")
            .and_then(|published| published.standing)
            .expect("the device has published an acknowledgement")
            .assertion
            .snapshot
            .is_none()
    }

    /// Register a second device, so no published snapshot describes this
    /// Store's devices any more and nothing is acknowledgeable.
    async fn overtake_the_acknowledged_device_state(&self) {
        let joining_store_dir = crate::sync::test_helpers::test_store_dir();
        self.store
            .activate_joined_device_from_snapshot(
                &self.db,
                self.db_store_dir.clone(),
                joining_store_dir,
                &self.signer,
                "2026-07-16T00:00:04Z",
                crate::sync::test_helpers::test_synced_tables(),
                crate::sync::test_helpers::test_migrations(),
                self.db.schema_version(),
            )
            .await
            .expect("activate a second device");
    }
}

/// Publishing a snapshot does not move the publisher's baseline; acknowledging
/// it does.
///
/// The baseline is what replay rewinds to, and advancing it retires the rows
/// that served that rewind. What licenses that is this device's own
/// acknowledgement: the signed statement that it holds everything the snapshot
/// covers. Until it says so, it keeps replaying from where it was — and reclaim,
/// which needs every device to have said it, has no coverage to work from
/// either.
#[tokio::test]
async fn an_unacknowledged_snapshot_does_not_advance_the_baseline() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "reclaim-baseline-unacknowledged",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind reclaim Store");

    let changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('unacknowledged-1', 'first', NULL, \
             '0000000001000-0000-unacknowledged', '2026-01-01')",
            ])
            .await;
    store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish package activation");

    // Published the way production publishes it, and deliberately never
    // acknowledged: the image is real, so nothing about its shape is what
    // stops the advance.
    let image_dir = tempfile::tempdir().expect("snapshot image dir");
    let image = coven_database::StoreDatabase::new(&db)
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture a real snapshot image");
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        coven_database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("frontier");
    device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish the unacknowledged snapshot");

    let result = device
        .reclaim_packages()
        .await
        .expect("reclaim runs even with nothing it may delete behind");
    assert_ne!(
        result.store_packages.coverage,
        super::StorePackageReclaimCoverage::Snapshot { generation: 0 },
        "the leg reports that it had no acknowledged coverage to work from",
    );

    let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
        coven_database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("frontier");
    let advanced = device
        .advance_baseline_by_acknowledging(frontier)
        .await
        .expect("acknowledge the published snapshot")
        .expect("acknowledging it is what licenses the advance");
    assert!(
        advanced.retired_commits > 0,
        "advancing retires the retained materializations the new cut covers, retired {}",
        advanced.retired_commits,
    );
    assert!(
        advanced.released_pins > 0,
        "and releases the replay pins those materializations held, released {}",
        advanced.released_pins,
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

/// A two-device owner Store with a snapshot whose coverage the peer's join is
/// under, and only the owner's acknowledgement of it.
///
/// # What a fixture here has to get right
///
/// Four things about the harness decide whether a test like this measures the
/// eligible set or something else entirely. Each of them produced a test that
/// passed for the wrong reason before it was understood, so they are written
/// down rather than rediscovered.
///
/// **Reclaim always has the generation-zero snapshot to fall back on.** Its
/// coverage is empty, so the founder is the only device at it and the owner's
/// own acknowledgement settles it. "Blocked" therefore never surfaces as an
/// error — `choose_snapshot` still returns `Ok`, just with an older snapshot.
/// Asserting on an error, or on whether any packages were deleted, measures the
/// fallback rather than the rule. These tests assert *which* snapshot is chosen,
/// which is also what decides how much history a reclaim may delete.
///
/// **A join publishes commits but no snapshot of its own, and acknowledges the
/// one that already exists.** So a freshly joined device is not idle with
/// respect to that snapshot, and a snapshot published before the join cannot
/// have the peer in its coverage-time state. The snapshot under test has to be
/// taken *after* the join.
///
/// **Its coverage has to be the owner's position after the join, not the join
/// snapshot's own coverage.** The latter is generation zero's, which sits below
/// the join's commits: reusing it yields a snapshot the peer is absent from and
/// which does not strictly dominate the seed, so reclaim rejects it for a reason
/// that has nothing to do with acknowledgements.
///
/// **The test producer shares the founder's stream**, so anything published
/// before the packages shifts their expected sequence numbers, and a
/// freshly activated peer has no local Store position of its own to read.
///
/// **Who the peer belongs to decides which questions the fixture can ask.** A
/// second device of the owner's own identity settles everything about device
/// status, but its author is the owner, so it cannot be removed as a member
/// while the store still has an owner. Asking what a *member* removal does to
/// the set needs a peer with a keypair of its own — see [`PeerPrincipal`].
struct UnanimityFixture {
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    owner_db: coven_database::Database,
    owner_dir: coven_foundation::store_dir::StoreDir,
    owner: UserKeypair,
    /// The peer's own identity, for a [`PeerPrincipal::SeparateMember`] peer.
    /// A same-principal peer has none: it writes under the owner's key.
    member: Option<UserKeypair>,
    owner_device: crate::sync::test_helpers::TestDevice,
    /// Read once while the fixture's database is open. Reclaim uses these only
    /// to enumerate snapshot streams; which devices must acknowledge comes from
    /// the verified device states, not from this list.
    registrations: Vec<coven_protocol::store_commit::ReferencedStoreDeviceRegistration>,
    /// The snapshot whose eligible set is under test.
    covering: coven_protocol::store_commit::StoreSnapshotRef,
    peer: Option<StoreDeviceRegistrationRef>,
}

/// When the second device joins, relative to the snapshot's coverage.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PeerJoin {
    BeforeCoverage,
    AfterCoverage,
}

/// Whose identity the second device registers under.
///
/// The two are not interchangeable, because ending a device and ending a member
/// are different acts with different reach. Excluding a device marks that device
/// Inactive and leaves its owner a member. Removing a member ends that member's
/// grants and rotates the store key, and touches no device status at all — so
/// only a peer with an identity of its own can pose that second question.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PeerPrincipal {
    /// A second device of the owner's own identity.
    SamePrincipal,
    /// A distinct member, admitted to the Store, with its own keypair.
    SeparateMember,
}

impl UnanimityFixture {
    async fn build(store_id: &str, join: PeerJoin, principal: PeerPrincipal) -> Self {
        let signer = UserKeypair::generate();
        let member = match principal {
            PeerPrincipal::SamePrincipal => None,
            PeerPrincipal::SeparateMember => Some(UserKeypair::generate()),
        };
        let owner_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_dir.clone());
        let store = Box::pin(crate::sync::test_helpers::TestStore::create(
            &owner_db,
            owner_dir.clone(),
            store_id,
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        ))
        .await
        .expect("create two-device reclaim Store");
        let owner_device = Box::pin(store.open_into(&owner_db, owner_dir.clone()))
            .await
            .expect("open owner Store device");

        let changeset =
            crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
                .capture_test_changeset(&[
                    "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                     VALUES ('unanimity-row', 'unanimity', NULL, \
                     '0000000001000-0000-unanimity', '2026-01-01')",
                ])
                .await;
        let commit = store
            .publish_changeset("founder", 1, &changeset, owner_db.schema_version())
            .await
            .expect("publish Store history to snapshot");

        // A join publishes its own snapshot, and its coverage spans what the
        // activation touched — so that snapshot's device state is the one the
        // peer is in. Joining before the coverage means letting it be the
        // snapshot under test; joining after means the owner takes one first,
        // from a state the peer is absent from.
        let latest_snapshot = || async {
            coven_database::StoreDatabase::new(&owner_db)
                .latest_local_store_snapshot()
                .await
                .expect("load the latest snapshot")
                .expect("a snapshot exists")
        };
        let mut covering = None;
        if join == PeerJoin::AfterCoverage {
            let StoreCommitCoord { stream_id, .. } = commit.coord;
            owner_device
                .publish_snapshot(
                    b"unanimity snapshot".to_vec(),
                    CommitFrontier(BTreeMap::from([(stream_id, commit)])),
                )
                .await
                .expect("publish covering snapshot");
            covering = Some(latest_snapshot().await.reference);
        }
        let peer_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
        match &member {
            None => {
                Box::pin(store.activate_joined_device(
                    &owner_db,
                    owner_dir.clone(),
                    &peer_db,
                    peer_dir,
                    &signer,
                    "2026-07-18T00:00:00Z",
                ))
                .await
                .expect("activate peer Store device");
            }
            Some(member) => {
                // Admitting first is what makes this peer a member in its own
                // right; the activation that follows is the same join the
                // same-principal peer does, so the choreography above still
                // holds and only the author of the registration differs.
                Box::pin(store.admit_and_activate_peer(
                    &owner_db,
                    owner_dir.clone(),
                    &peer_db,
                    peer_dir,
                    member,
                ))
                .await
                .expect("admit and activate a second member's device");
            }
        }
        let covering = match covering {
            Some(reference) => reference,
            None => {
                // A join publishes a snapshot and acknowledges it, so the peer
                // is not idle with respect to that one. The snapshot under test
                // is a fresh one the owner takes afterwards, over the join
                // snapshot's own coverage — the frontier that spans the streams
                // the activation touched, and so the one whose device state has
                // the peer in it. Acknowledgements match a snapshot by exact
                // reference, so the peer's earlier one does not carry over.
                // A join publishes commits but no snapshot of its own, and
                // it acknowledges the one that already exists — so the peer is
                // not idle with respect to that one. The snapshot under test is
                // one the owner takes now, over its position *after* the join:
                // that frontier is above the join's commits, so the device state
                // resolved at it has the peer in it, and it strictly dominates
                // the seed. Acknowledgements match a snapshot by exact
                // reference, so the peer's earlier one does not carry over.
                let after_join = owner_device
                    .latest_local_store_position()
                    .await
                    .expect("read the owner's Store position after the join")
                    .expect("the join published Store history");
                let StoreCommitCoord { stream_id, .. } = after_join.coord;
                owner_device
                    .publish_snapshot(
                        b"unanimity snapshot".to_vec(),
                        CommitFrontier(BTreeMap::from([(stream_id, after_join)])),
                    )
                    .await
                    .expect("publish covering snapshot above the join");
                latest_snapshot().await.reference
            }
        };

        let acknowledged_at = owner_device
            .latest_local_store_position()
            .await
            .expect("read the owner's Store position")
            .expect("the Store has published history");
        let StoreCommitCoord { stream_id, .. } = acknowledged_at.coord;
        owner_device
            .publish_acknowledgement(CommitFrontier(BTreeMap::from([(
                stream_id,
                acknowledged_at,
            )])))
            .await
            .expect("owner acknowledges the covering snapshot");

        let local_device_id = owner_device.device_id().clone();
        let registrations = coven_database::StoreDatabase::new(&owner_db)
            .activated_store_device_registration_records()
            .await
            .expect("list active Store registrations");
        let peer = registrations
            .iter()
            .map(|registration| registration.reference().clone())
            .find(|reference| reference.device_id.to_string() != local_device_id);

        Self {
            store,
            owner_db,
            owner_dir,
            owner: signer,
            member,
            owner_device,
            registrations,
            covering,
            peer,
        }
    }

    /// Removes the peer's member from the Store: its grants end and the store
    /// key rotates. The devices that member registered keep the status they
    /// had, which is the whole point of the case this serves.
    async fn remove_peer_member(&self) {
        let member = self
            .member
            .as_ref()
            .expect("only a separate member can be removed as one");
        self.store
            .remove_member(
                &self.owner_db,
                self.owner_dir.clone(),
                &self.owner,
                &crate::sync::test_helpers::pubkey_hex(member),
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                &crate::sync::test_helpers::TestCustody::default(),
            )
            .await
            .expect("remove the peer's member");
    }

    async fn chosen_snapshot(&self) -> coven_protocol::store_commit::StoreSnapshotRef {
        let mut writer = self
            .owner_device
            .authorize_writer()
            .await
            .expect("authorize reclaim writer");
        writer
            .reclaim()
            .choose_snapshot(&self.registrations)
            .await
            .expect("reclaim selects some snapshot")
            .snapshot
            .reference
    }
}

/// A device active at the coverage and still active gets no relaxation, whether
/// or not it has done anything since. It is a current member, so history behind
/// that snapshot is history it could still ask for, and reclaim declines the
/// snapshot rather than delete it.
#[tokio::test]
async fn an_idle_device_active_at_the_coverage_still_blocks_reclaim() {
    Box::pin(async {
        let fixture = UnanimityFixture::build(
            "reclaim-unanimity-idle",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SamePrincipal,
        )
        .await;
        assert!(
            fixture.peer.is_some(),
            "the peer joined before the coverage"
        );

        let chosen = fixture.chosen_snapshot().await;
        assert!(
            chosen.generation < fixture.covering.generation,
            "a current member that has not acknowledged the snapshot blocks it, so reclaim \
             falls back below it: chose generation {} against {}",
            chosen.generation,
            fixture.covering.generation,
        );
    })
    .await;
}

/// A device excluded after the coverage was active there, so the coverage-time
/// state alone would demand a signature it can never publish — and one snapshot
/// stuck that way takes every earlier one with it. It is not a member, cannot
/// pull, and re-enters only through a join that bootstraps at or past the
/// snapshot, so there is nothing behind it left to need.
#[tokio::test]
async fn a_device_excluded_after_the_coverage_does_not_block_reclaim() {
    Box::pin(async {
        let fixture = UnanimityFixture::build(
            "reclaim-unanimity-excluded",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SamePrincipal,
        )
        .await;
        let peer = fixture.peer.clone().expect("the peer joined");
        fixture.owner_device.finalize_peer_exclusion(&peer).await;

        // At or past, not equal: excluding a device publishes history of its own,
        // which can produce a newer snapshot that is also selectable. What
        // matters is that reclaim is no longer held below the one the excluded
        // device was blocking.
        let chosen = fixture.chosen_snapshot().await;
        assert!(
            chosen.generation >= fixture.covering.generation,
            "an excluded device is excused, so reclaim reaches its snapshot: chose \
             generation {} against {}",
            chosen.generation,
            fixture.covering.generation,
        );
    })
    .await;
}

/// A device that joined after the coverage is absent from the coverage-time
/// state, so it never enters the set. Stated as its own case because the reason
/// is not that it is new: a join installs a snapshot image and materializes only
/// what is past it, so the device already stands where an acknowledgement would
/// have put it.
#[tokio::test]
async fn a_device_that_joined_after_the_coverage_does_not_block_reclaim() {
    Box::pin(async {
        let fixture = UnanimityFixture::build(
            "reclaim-unanimity-joined-after",
            PeerJoin::AfterCoverage,
            PeerPrincipal::SamePrincipal,
        )
        .await;
        assert!(fixture.peer.is_some(), "the peer joined after the coverage");

        let chosen = fixture.chosen_snapshot().await;
        assert!(
            chosen.generation >= fixture.covering.generation,
            "a device that joined after the coverage is excused, so reclaim reaches its \
             snapshot: chose generation {} against {}",
            chosen.generation,
            fixture.covering.generation,
        );
    })
    .await;
}

/// A removed member's device stops blocking reclaim — the case device status
/// alone can never notice.
///
/// This is the shape the live store was stuck on. Removing a member ends its
/// grants and rotates the store key; it does not mark the devices that member
/// registered Inactive, because device status tracks a device's own lifecycle,
/// not its owner's standing. A rule that asked only "is this device still
/// Active" therefore kept demanding an acknowledgement from every removed
/// member's device — devices that cannot pull, will never publish again, and
/// had every snapshot behind them pinned unreclaimable for good.
///
/// Asserted as a before and an after over one fixture, so what moves reclaim is
/// the removal and not the generation-zero fallback every one of these tests
/// can otherwise land on.
#[tokio::test]
async fn a_removed_members_device_does_not_block_reclaim() {
    Box::pin(async {
        let fixture = UnanimityFixture::build(
            "reclaim-unanimity-removed-member",
            PeerJoin::BeforeCoverage,
            PeerPrincipal::SeparateMember,
        )
        .await;
        assert!(
            fixture.peer.is_some(),
            "the second member's device joined before the coverage"
        );

        let before = fixture.chosen_snapshot().await;
        assert!(
            before.generation < fixture.covering.generation,
            "while it is still a member, its device blocks the snapshot: chose generation {} \
             against {}",
            before.generation,
            fixture.covering.generation,
        );

        fixture.remove_peer_member().await;

        // At or past, not equal: removing a member publishes history of its own,
        // which can produce a newer snapshot that is also selectable. What
        // matters is that reclaim is no longer held below the one the removed
        // member's device was blocking.
        let after = fixture.chosen_snapshot().await;
        assert!(
            after.generation >= fixture.covering.generation,
            "a removed member's device is excused, so reclaim reaches its snapshot: chose \
             generation {} against {}",
            after.generation,
            fixture.covering.generation,
        );
    })
    .await;
}
