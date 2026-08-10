use super::*;
use coven_keys::keys::{self, UserKeypair};
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::objects::ObjectSlot;
use coven_protocol::store_commit::{StoreCommitCoord, StoreProtocolError};
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
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    device: crate::sync::test_helpers::TestDevice,
    packages: Vec<StorePackageReclaimTarget>,
}

impl ReclaimJourneyFixture {
    async fn build(store_id: &str) -> Self {
        let db = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            store_id,
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create Store");
        let device = store
            .bind_device(&db, &signer)
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
            let changeset = crate::sync::test_helpers::open_test_db()
                .database
                .capture_test_changeset(&[row])
                .await;
            let activation = store
                .publish_changeset(
                    "founder",
                    sequence,
                    &changeset,
                    db.database.schema_version(),
                )
                .await
                .expect("publish package activation");
            activations.push(activation);
        }
        let tip = activations.last().expect("published two packages").clone();
        let StoreCommitCoord { stream_id, .. } = tip.coord;
        let coverage = CommitFrontier(BTreeMap::from([(stream_id, tip)]));

        device
            .publish_snapshot(b"reclaim journey snapshot".to_vec(), coverage.clone())
            .await
            .expect("publish covering snapshot");
        device
            .publish_acknowledgement(coverage)
            .await
            .expect("acknowledge covering snapshot");
        db.database
            .test_sql(|database| {
                database.transaction(|transaction| {
                    transaction.remove_retained_replay_ownership_from_snapshot()
                })
            })
            .await
            .expect("release retained replay ownership");

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
            home,
            device,
            packages,
        }
    }

    async fn reclaim(&self) -> Result<StoreReclaimResult, StoreReclaimError> {
        self.device
            .authorize_writer()
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
            .reclaim_packages()
            .await
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
            self.store.root.store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        match self
            .store
            .storage()
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
    let db = crate::sync::test_helpers::open_test_db();
    let signer = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "reclaim-stable-snapshot-selection",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device(&db, &signer)
        .await
        .expect("bind reclaim Store");
    let first_changeset = crate::sync::test_helpers::open_test_db()
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('stable-snapshot-row', 'stable', NULL, \
                 '0000000001000-0000-stable-snapshot', '2026-01-01')",
        ])
        .await;
    let first_commit = store
        .publish_changeset("founder", 1, &first_changeset, db.database.schema_version())
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
    let stable = coven_database::StoreDatabase::new(&db.database)
        .latest_local_store_snapshot()
        .await
        .expect("load stable snapshot")
        .expect("stable snapshot exists");

    let second_changeset = crate::sync::test_helpers::open_test_db()
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('unstable-snapshot-row', 'unstable', NULL, \
                 '0000000002000-0000-unstable-snapshot', '2026-01-01')",
        ])
        .await;
    let second_commit = store
        .publish_changeset(
            "founder",
            3,
            &second_changeset,
            db.database.schema_version(),
        )
        .await
        .expect("publish second Store position");
    device
        .publish_snapshot(
            b"unacknowledged reclaim snapshot".to_vec(),
            CommitFrontier(BTreeMap::from([(stream_id, second_commit)])),
        )
        .await
        .expect("publish unacknowledged snapshot");
    let registrations = coven_database::StoreDatabase::new(&db.database)
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
    let db = crate::sync::test_helpers::open_test_db();
    let signer = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "signed-reclaim-authority",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let changeset = crate::sync::test_helpers::open_test_db()
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim', '2026-01-01')",
        ])
        .await;
    let activation = store
        .publish_changeset("founder", 1, &changeset, db.database.schema_version())
        .await
        .expect("publish package activation");
    let founder_authority = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let loaded = store
        .bind_device(&db, &signer)
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
        store.root.store_root_hash,
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
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimEvidence,
    );
    let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
    let evidence_slot = store
        .storage()
        .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
        .await
        .expect("allocate evidence slot");
    let prepared_evidence = store
        .storage()
        .prepare_protocol_object(
            &evidence_context,
            evidence_slot,
            &evidence_prefix,
            evidence.to_bytes(),
        )
        .expect("prepare evidence");
    store
        .storage()
        .create_protocol_object(&prepared_evidence)
        .await
        .expect("create evidence");
    let evidence_ref =
        ReclaimEvidenceRef::from_evidence(&evidence, prepared_evidence.reference().clone());
    let authorization = ReclaimAuthorization::signed(
        store.root.store_root_hash,
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
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimAuthorization,
    );
    let authorization_prefix =
        reclaim_authorization_semantic_prefix(authorization.authorization_hash());
    let authorization_slot = store
        .storage()
        .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
        .await
        .expect("allocate authorization slot");
    let prepared_authorization = store
        .storage()
        .prepare_protocol_object(
            &authorization_context,
            authorization_slot,
            &authorization_prefix,
            authorization.to_bytes(),
        )
        .expect("prepare authorization");
    store
        .storage()
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
            store.root.store_root_hash,
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

    db.database
        .test_sql(|database| {
            database.transaction(|transaction| {
                transaction.remove_retained_replay_ownership_from_snapshot()
            })
        })
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
    store
        .storage()
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
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
    let db = crate::sync::test_helpers::open_test_db();
    let signer = UserKeypair::generate();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "reclaim-activation-head",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let changeset = crate::sync::test_helpers::open_test_db()
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-head-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim-head', '2026-01-01')",
        ])
        .await;
    let target_activation = store
        .publish_changeset("founder", 1, &changeset, db.database.schema_version())
        .await
        .expect("publish target package activation");
    let loaded = store
        .bind_device(&db, &signer)
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
    let snapshot = coven_database::StoreDatabase::new(&db.database)
        .latest_local_store_snapshot()
        .await
        .expect("load covering snapshot")
        .expect("covering snapshot exists");
    let acknowledgement = coven_database::StoreDatabase::new(&db.database)
        .latest_local_store_ack()
        .await
        .expect("load covering acknowledgement")
        .expect("covering acknowledgement exists")
        .reference;
    db.database
        .test_sql(|database| {
            database.transaction(|transaction| {
                transaction.remove_retained_replay_ownership_from_snapshot()
            })
        })
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
    let candidate = coven_database::StoreDatabase::new(&db.database)
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
    store
        .storage()
        .delete_protocol_object(&activation_head.object)
        .await
        .expect("remove reclaim activation head");
    let authorized = coven_database::StoreDatabase::new(&db.database)
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
    store
        .storage()
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
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

    store
        .storage()
        .create_protocol_object(&activation_head_prepared)
        .await
        .expect("restore exact reclaim activation head");
    let activation_commit = match &authorized {
        DurableStoreReclaimOperation::Authorized { activation, .. } => activation.commit().clone(),
        _ => unreachable!("fixture has an activated reclaim"),
    };
    db.database
        .test_sql(move |database| database.delete_exact_materialized_commit(&activation_commit))
        .await
        .expect("retract reclaim activation materialization");

    assert!(
        reclaim.execute_delete(authorized).await.is_err(),
        "a retracted Merge reclaim activation must not delete"
    );
    store
        .storage()
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
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
        resumed,
        StoreReclaimResult {
            packages_deleted: 1,
            physical_copies_deleted: 1,
        },
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
        result,
        StoreReclaimResult {
            packages_deleted: 2,
            physical_copies_deleted: 2,
        },
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
        idempotent,
        StoreReclaimResult {
            packages_deleted: 0,
            physical_copies_deleted: 0,
        },
        "the recorded reclaim operations are not repeated",
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
