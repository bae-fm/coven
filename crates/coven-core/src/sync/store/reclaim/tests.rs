use super::*;
use crate::storage::cloud::ObjectSlot;
use crate::sync::store_commit::StoreCommitCoord;

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
    db: crate::database::Database,
    store: crate::sync::test_helpers::TestStore,
    signer: UserKeypair,
    device_id: String,
    membership: MembershipChain,
    packages: Vec<StorePackageReclaimTarget>,
}

impl ReclaimJourneyFixture {
    async fn build(store_id: &str) -> Self {
        let db = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(&db, store_id, signer.clone())
            .await
            .expect("create Store");

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
            let changeset = crate::sync::test_helpers::capture_bytes(
                &crate::sync::test_helpers::open_test_db(),
                &[row],
            )
            .await;
            let activation = store
                .publish_changeset("founder", sequence, &changeset, db.schema_version())
                .await
                .expect("publish package activation");
            activations.push(activation);
        }
        let tip = activations.last().expect("published two packages").clone();
        let StoreCommitCoord { stream_id, .. } = tip.coord;
        let coverage = CommitFrontier(BTreeMap::from([(stream_id, tip)]));

        let membership = crate::sync::store::pull::load_cycle_membership(
            &store.storage,
            &StoreDatabase::new(&db),
        )
        .await
        .expect("load Store membership");
        let chain = membership.clone();
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            b"reclaim journey snapshot".to_vec(),
            coverage.clone(),
            &signer,
            &chain,
            &db,
        )
        .await
        .expect("publish covering snapshot");
        crate::sync::test_helpers::publish_store_ack_fixture(
            &db,
            &store.storage,
            coverage,
            &signer,
        )
        .await
        .expect("acknowledge covering snapshot");
        db.call(|connection| {
            let transaction = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            StoreDatabase::remove_retained_replay_ownership_from_snapshot_on(&transaction)?;
            transaction.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("release retained replay ownership");

        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load local device id")
            .expect("local device id exists");

        let mut commit_verifier =
            crate::sync::store::pull::StoreCommitVerifier::new(&store.storage, &store.root)
                .await
                .expect("create Store commit verifier");
        let mut packages = Vec::new();
        for activation in activations {
            let commit = commit_verifier
                .load_ref(&activation)
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
            signer,
            device_id,
            membership: chain,
            packages,
        }
    }

    async fn reclaim(&self) -> Result<StoreReclaimResult, StoreReclaimError> {
        reclaim_store_packages(
            &StoreDatabase::new(&self.db),
            &self.store.storage,
            &self.device_id,
            &self.signer,
            self.store.root.store_root_hash,
            &self.membership,
        )
        .await
    }

    async fn package_is_present(&self, target: &StorePackageReclaimTarget) -> bool {
        let stream_id = target.activation.coord.stream_id.to_string();
        let prefix = crate::sync::store_commit::package_semantic_prefix(
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
        self.store
            .home
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
    )
    .await
    .expect("create Store");
    let first_changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('stable-snapshot-row', 'stable', NULL, \
                 '0000000001000-0000-stable-snapshot', '2026-01-01')",
        ],
    )
    .await;
    let first_commit = store
        .publish_changeset("founder", 1, &first_changeset, db.schema_version())
        .await
        .expect("publish first Store position");
    let StoreCommitCoord { stream_id, .. } = first_commit.coord;
    let first_coverage = CommitFrontier(BTreeMap::from([(stream_id, first_commit.clone())]));
    let membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(&db),
    )
    .await
    .expect("load Store membership");
    let chain = &membership;
    crate::sync::test_helpers::publish_snapshot_fixture(
        &store.storage,
        &store.root,
        b"stable reclaim snapshot".to_vec(),
        first_coverage.clone(),
        &signer,
        chain,
        &db,
    )
    .await
    .expect("publish stable snapshot");
    crate::sync::test_helpers::publish_store_ack_fixture(
        &db,
        &store.storage,
        first_coverage,
        &signer,
    )
    .await
    .expect("acknowledge stable snapshot");
    let stable = crate::sync::store::database::StoreDatabase::new(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load stable snapshot")
        .expect("stable snapshot exists");

    let second_changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('unstable-snapshot-row', 'unstable', NULL, \
                 '0000000002000-0000-unstable-snapshot', '2026-01-01')",
        ],
    )
    .await;
    let second_commit = store
        .publish_changeset("founder", 3, &second_changeset, db.schema_version())
        .await
        .expect("publish second Store position");
    crate::sync::test_helpers::publish_snapshot_fixture(
        &store.storage,
        &store.root,
        b"unacknowledged reclaim snapshot".to_vec(),
        CommitFrontier(BTreeMap::from([(stream_id, second_commit)])),
        &signer,
        chain,
        &db,
    )
    .await
    .expect("publish unacknowledged snapshot");
    let registrations = crate::sync::store::database::StoreDatabase::new(&db)
        .activated_store_device_registration_records()
        .await
        .expect("load active registrations");

    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(&store.storage, &store.root)
            .await
            .expect("open reclaim snapshot history");
    let selected = choose_snapshot(&mut history_verifier, &registrations)
        .await
        .expect("select the stable reclaim snapshot");

    assert_eq!(selected.snapshot.reference, stable.reference);
}

#[tokio::test]
async fn exact_reclaim_receipt_opens_its_authorization_and_encrypted_evidence() {
    let db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "signed-reclaim-authority",
        UserKeypair::generate(),
    )
    .await
    .expect("create Store");
    let changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim', '2026-01-01')",
        ],
    )
    .await;
    let activation = store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish package activation");
    let (founder_ref, founder, device_signer) = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let mut commit_verifier =
        crate::sync::store::pull::StoreCommitVerifier::new(&store.storage, &store.root)
            .await
            .expect("open Store commit verifier");
    let activated = commit_verifier
        .load_ref(&activation)
        .await
        .expect("load package activation");
    assert_eq!(activated.author(), &founder);
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
                author_registration: founder_ref.clone(),
                snapshot: crate::sync::store_commit::StoreSnapshotRef {
                    generation: 0,
                    snapshot_hash: ObjectHash::digest(b"covering snapshot"),
                    object: proof_object("store-v1/snapshots/founder/covering"),
                },
            },
            acknowledgements: vec![StoreAckRef {
                registration: founder_ref.clone(),
                sequence: 1,
                ack_hash: ObjectHash::digest(b"acknowledgement"),
                object: proof_object("store-v1/acks/founder/1.json"),
            }],
        }),
        &store.signer,
    )
    .expect("sign reclaim evidence");
    let evidence_context = ProtocolObjectContext::store_encrypted(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimEvidence,
    );
    let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
    let evidence_slot = store
        .storage
        .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
        .await
        .expect("allocate evidence slot");
    let prepared_evidence = store
        .storage
        .prepare_protocol_object(
            &evidence_context,
            evidence_slot,
            &evidence_prefix,
            evidence.to_bytes(),
        )
        .expect("prepare evidence");
    store
        .storage
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
            owner_grant: store.protocol_root.descriptor.founder_grant.clone(),
        },
        &store.signer,
    );
    let authorization_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimAuthorization,
    );
    let authorization_prefix =
        reclaim_authorization_semantic_prefix(authorization.authorization_hash());
    let authorization_slot = store
        .storage
        .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
        .await
        .expect("allocate authorization slot");
    let prepared_authorization = store
        .storage
        .prepare_protocol_object(
            &authorization_context,
            authorization_slot,
            &authorization_prefix,
            authorization.to_bytes(),
        )
        .expect("prepare authorization");
    store
        .storage
        .create_protocol_object(&prepared_authorization)
        .await
        .expect("create authorization");
    let authorization_ref = ReclaimAuthorizationRef::from_authorization(
        &authorization,
        prepared_authorization.reference().clone(),
    );

    let opened = crate::sync::store_objects::load_reclaim_authorization_ref(
        &store.storage,
        &store.root,
        &authorization_ref,
    )
    .await
    .expect("open exact reclaim authority graph");

    assert_eq!(opened.authorization.value, authorization);
    assert_eq!(opened.evidence.value, evidence);
    let mut relocated = authorization.clone();
    let ReclaimTarget::StorePackage(relocated_target) = &mut relocated.target else {
        unreachable!("Store package reclaim target");
    };
    relocated_target.package.object =
        proof_object("store-v1/candidates/family/packages/device/1/another-package.pkg");
    assert!(authorization
        .verify(&keys::public_key_hex(&store.signer))
        .is_ok());
    assert!(matches!(
        relocated.verify(&keys::public_key_hex(&store.signer)),
        Err(StoreProtocolError::InvalidSignature)
    ));

    let receipt = ReclaimReceipt::signed(
        store.root.store_root_hash,
        authorization_ref,
        authorization.authority.membership.clone(),
        store
            .protocol_root
            .descriptor
            .founder_provider_admin
            .grant_id
            .clone(),
        founder_ref,
        &founder,
        &device_signer,
    )
    .expect("sign reclaim receipt");
    let receipt_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimReceipt,
    );
    let receipt_prefix = reclaim_receipt_semantic_prefix(receipt.receipt_hash());
    let receipt_slot = store
        .storage
        .allocate_protocol_slot(&receipt_context, &receipt_prefix, ".json")
        .await
        .expect("allocate receipt slot");
    let prepared_receipt = store
        .storage
        .prepare_protocol_object(
            &receipt_context,
            receipt_slot,
            &receipt_prefix,
            receipt.to_bytes(),
        )
        .expect("prepare receipt");
    store
        .storage
        .create_protocol_object(&prepared_receipt)
        .await
        .expect("create receipt");
    let receipt_ref =
        ReclaimReceiptRef::from_receipt(&receipt, prepared_receipt.reference().clone());

    let opened_receipt = crate::sync::store_objects::load_reclaim_receipt_ref(
        &store.storage,
        &store.root,
        &receipt_ref,
    )
    .await
    .expect("open exact reclaim receipt graph");

    assert_eq!(opened_receipt.receipt.value, receipt);
    assert_eq!(
        opened_receipt.authorization.authorization.value,
        authorization
    );
    assert_eq!(opened_receipt.authorization.evidence.value, evidence);
    let mut reassigned = receipt.clone();
    reassigned.provider_admin_grant =
        crate::sync::provider::ProviderAdminGrantId(ObjectHash::digest(b"another admin"));
    assert!(matches!(
        reassigned.verify(&founder),
        Err(StoreProtocolError::InvalidSignature)
    ));

    db.call(|connection| {
        let transaction = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        crate::sync::store::database::StoreDatabase::remove_retained_replay_ownership_from_snapshot_on(&transaction)?;
        transaction.commit().map_err(crate::database::DbError::from)
    })
    .await
    .expect("release retained replay package ownership");
    let target = opened.evidence.value.claim.target();
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
    let operation = super::journal::DurableStoreReclaimOperation::Authorized {
        authorization: receipt.authorization.clone(),
        activation: super::journal::ReclaimCommitActivation::new(
            authorization_activation,
            crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: ObjectHash::digest(b"reclaim authorization head"),
                object: proof_object("store-v1/heads/reclaim-authorization.json"),
            },
        )
        .expect("valid reclaim activation"),
    };
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(&store.storage, &store.root)
            .await
            .expect("create Store history verifier");
    let deletion =
        execute_reclaim_delete(&StoreDatabase::new(&db), &mut history_verifier, operation).await;
    assert!(
        deletion.is_err(),
        "nonexistent snapshot and acknowledgement refs must not authorize deletion"
    );
    let resolved_target = opened.evidence.value.claim.target();
    let ReclaimTarget::StorePackage(target) = &resolved_target else {
        unreachable!("Store package reclaim target");
    };
    let StoreCommitCoord { stream_id, .. } = target.activation.coord;
    store
        .storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            &target.package.object,
            &crate::sync::store_commit::package_semantic_prefix(
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
    )
    .await
    .expect("create Store");
    let changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-head-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim-head', '2026-01-01')",
        ],
    )
    .await;
    let target_activation = store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish target package activation");
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(&store.storage, &store.root)
            .await
            .expect("create Store history verifier");
    let target_commit = history_verifier
        .commit_verifier()
        .load_ref(&target_activation)
        .await
        .expect("load target activation");
    let target_package = target_commit
        .value()
        .store_package()
        .expect("target activation carries a Store package")
        .clone();
    let StoreCommitCoord { stream_id, .. } = target_activation.coord;
    let coverage = CommitFrontier(BTreeMap::from([(stream_id, target_activation.clone())]));
    let membership = crate::sync::store::pull::load_cycle_membership(
        &store.storage,
        &crate::sync::store::database::StoreDatabase::new(&db),
    )
    .await
    .expect("load reclaim membership");
    let chain = &membership;
    crate::sync::test_helpers::publish_snapshot_fixture(
        &store.storage,
        &store.root,
        b"reclaim activation snapshot".to_vec(),
        coverage.clone(),
        &signer,
        chain,
        &db,
    )
    .await
    .expect("publish covering snapshot");
    crate::sync::test_helpers::publish_store_ack_fixture(&db, &store.storage, coverage, &signer)
        .await
        .expect("publish covering acknowledgement");
    let snapshot = crate::sync::store::database::StoreDatabase::new(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load covering snapshot")
        .expect("covering snapshot exists");
    let acknowledgement = crate::sync::store::database::StoreDatabase::new(&db)
        .latest_local_store_ack()
        .await
        .expect("load covering acknowledgement")
        .expect("covering acknowledgement exists")
        .reference;
    db.call(|connection| {
        let transaction = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        crate::sync::store::database::StoreDatabase::remove_retained_replay_ownership_from_snapshot_on(&transaction)?;
        transaction.commit().map_err(crate::database::DbError::from)
    })
    .await
    .expect("release target retained replay ownership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load local device id")
        .expect("local device id exists");
    let reclaim_membership = chain;
    prepare_reclaim_authorization(
        &StoreDatabase::new(&db),
        &device_id,
        &signer,
        reclaim_membership,
        &mut history_verifier,
        ReclaimClaim::StorePackage(StorePackageReclaimClaim {
            target: StorePackageReclaimTarget {
                package: target_package.clone(),
                activation: target_activation.clone(),
            },
            covering_snapshot: StoreSnapshotLocator {
                author_registration: snapshot.meta.author_registration.clone(),
                snapshot: snapshot.reference.clone(),
            },
            acknowledgements: vec![acknowledgement],
        }),
    )
    .await
    .expect("prepare reclaim authorization");
    let candidate = StoreDatabase::new(&db)
        .store_reclaim_operations()
        .await
        .expect("load reclaim candidate")
        .into_iter()
        .next()
        .expect("reclaim candidate exists");
    let prepared_candidate = candidate
        .candidate()
        .expect("reclaim operation has a candidate");
    let activation_head = crate::sync::store_commit::StoreDeviceHeadRef {
        head_hash: prepared_candidate.head.head_hash(),
        object: prepared_candidate.prepared_head.reference().clone(),
    };
    let activation_head_prepared = prepared_candidate.prepared_head.clone();
    drive_reclaim_candidate(
        &StoreDatabase::new(&db),
        &mut history_verifier,
        &device_id,
        &signer,
        reclaim_membership,
        candidate,
    )
    .await
    .expect("activate reclaim authorization");
    store
        .storage
        .delete_protocol_object(&activation_head.object)
        .await
        .expect("remove reclaim activation head");
    let authorized = StoreDatabase::new(&db)
        .store_reclaim_operations()
        .await
        .expect("load activated reclaim")
        .into_iter()
        .next()
        .expect("activated reclaim exists");

    let deletion = execute_reclaim_delete(
        &StoreDatabase::new(&db),
        &mut history_verifier,
        authorized.clone(),
    )
    .await;

    assert!(
        deletion.is_err(),
        "a reclaim authorization without its exact Merge activation head must not delete"
    );
    store
        .storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            &target_package.object,
            &crate::sync::store_commit::package_semantic_prefix(
                target_package.candidate_family,
                &stream_id.to_string(),
                target_activation.coord.sequence(),
                target_package.content_hash,
            ),
        )
        .await
        .expect("missing activation authority leaves target readable");

    store
        .storage
        .create_protocol_object(&activation_head_prepared)
        .await
        .expect("restore exact reclaim activation head");
    let activation_commit = match &authorized {
        super::journal::DurableStoreReclaimOperation::Authorized { activation, .. } => {
            activation.commit().clone()
        }
        _ => unreachable!("fixture has an activated reclaim"),
    };
    let StoreCommitCoord {
        stream_id: activation_stream,
        sequence: activation_sequence,
    } = activation_commit.coord;
    db.call(move |connection| {
        let removed = connection
            .execute(
                "DELETE FROM materialized_commits \
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                rusqlite::params![
                    activation_stream.to_string(),
                    i64::try_from(activation_sequence).expect("activation sequence fits i64"),
                    serde_json::to_string(&activation_commit).expect("serialize activation"),
                ],
            )
            .map_err(crate::database::DbError::from)?;
        if removed != 1 {
            return Err(crate::database::DbError::Message(
                "reclaim activation materialization was not removed".to_string(),
            ));
        }
        Ok(())
    })
    .await
    .expect("retract reclaim activation materialization");

    assert!(
        execute_reclaim_delete(&StoreDatabase::new(&db), &mut history_verifier, authorized,)
            .await
            .is_err(),
        "a retracted Merge reclaim activation must not delete"
    );
    store
        .storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                store.root.store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            &target_package.object,
            &crate::sync::store_commit::package_semantic_prefix(
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
    fixture
        .store
        .home
        .fail_nth_exact_delete_of(&package_slots, 2);

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
fn frontier_at(stream: &str, sequence: u64) -> crate::sync::store_commit::CommitFrontier {
    let stream_id = crate::sync::causal_grants::AuthorStreamId::from_digest(ObjectHash::digest(
        stream.as_bytes(),
    ));
    let commit = crate::sync::store_commit::StoreBatchCommitRef {
        coord: StoreCommitCoord {
            stream_id,
            sequence,
        },
        commit_hash: ObjectHash::digest(format!("{stream}:{sequence}").as_bytes()),
        object: proof_object(&format!(
            "store-v1/candidates/f/commits/{stream}/{sequence}/hash"
        )),
    };
    crate::sync::store_commit::CommitFrontier(std::collections::BTreeMap::from([(
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
        !super::snapshot_supersedes_seed(&frontier_at("owner", 4), &seed),
        "a snapshot whose cut equals the seed exactly does not supersede it"
    );
    assert!(
        super::snapshot_supersedes_seed(&frontier_at("owner", 5), &seed),
        "a snapshot strictly past the seed on its stream supersedes it"
    );
    assert!(
        !super::snapshot_supersedes_seed(&frontier_at("owner", 3), &seed),
        "a snapshot behind the seed does not cover it and cannot supersede it"
    );
}

/// A deterministic device registration reference for claim-shape tests.
fn claim_registration(label: &str) -> crate::sync::store_commit::StoreDeviceRegistrationRef {
    crate::sync::store_commit::StoreDeviceRegistrationRef {
        device_id: ObjectHash::digest(label.as_bytes())
            .to_string()
            .parse()
            .expect("a digest is a valid device id"),
        registration_hash: ObjectHash::digest(format!("{label}:registration").as_bytes()),
        object: proof_object(&format!("store-v1/devices/{label}/registration.json")),
    }
}

/// A deterministic Circle control coordinate for claim-shape tests.
fn claim_control(label: &str, seq: u64) -> CircleControlCoord {
    CircleControlCoord {
        device_id: label.to_string(),
        stream_id: crate::sync::causal_grants::AuthorStreamId::from_digest(ObjectHash::digest(
            label.as_bytes(),
        )),
        author_pubkey: format!("{label}-pubkey"),
        author_owner_grant: crate::sync::test_helpers::test_membership_grant_id(label),
        seq,
        control_hash: ObjectHash::digest(format!("{label}:{seq}").as_bytes()),
    }
}

fn claim_image(label: &str) -> crate::sync::store_commit::SnapshotImageRef {
    crate::sync::store_commit::SnapshotImageRef {
        image_hash: ObjectHash::digest(format!("{label}:image").as_bytes()),
        object: proof_object(&format!("circles/{label}/snapshot-images/{label}/image.db")),
    }
}

fn claim_ack(circle_id: CircleId, label: &str) -> CircleAckRef {
    CircleAckRef {
        registration: claim_registration(label),
        circle_id,
        sequence: 1,
        ack_hash: ObjectHash::digest(format!("{label}:ack").as_bytes()),
        object: proof_object(&format!("circles/{circle_id}/acks/{label}/1.json")),
    }
}

fn claim_coverage(circle_id: CircleId, label: &str) -> CircleBootstrapCoverageRef {
    CircleBootstrapCoverageRef {
        circle_id,
        control: claim_control(label, 1),
        activation_commit: frontier_at(label, 4)
            .0
            .values()
            .next()
            .expect("single-stream frontier names a commit")
            .clone(),
        bootstrap: crate::sync::circle::CircleBootstrapRef {
            coverage: frontier_at(label, 4),
            schema_version: 1,
            sync_routing_hash: ObjectHash::digest(b"routing"),
            image: claim_image(label),
            blobs: Vec::new(),
        },
    }
}

/// A Circle bootstrap reclaim claim must be refused when its proof acknowledgement
/// belongs to a different Circle than the coverage it authorizes deleting. Reclaim
/// evidence is Store-visible, so this shape has to be caught on the claim itself
/// rather than only where a Circle member can read the acknowledgement: without
/// the check, an acknowledgement from a Circle the Owner does hold access to would
/// travel as evidence over another Circle's image.
#[test]
fn circle_bootstrap_reclaim_refuses_an_acknowledgement_from_another_circle() {
    let target = CircleId::from_bytes([1; 16]);
    let other = CircleId::from_bytes([2; 16]);
    let claim = |acknowledged: CircleId| {
        ReclaimClaim::CircleBootstrapImage(CircleBootstrapImageReclaimClaim {
            target: CircleBootstrapImageReclaimTarget {
                coverage: claim_coverage(target, "recipient"),
            },
            proof: CircleBootstrapReclaimProof::RecipientCoverage {
                acknowledgement: claim_ack(acknowledged, "recipient"),
            },
        })
    };
    claim(target)
        .validate()
        .expect("an acknowledgement from the target Circle is well-formed");
    let error = claim(other)
        .validate()
        .expect_err("an acknowledgement from another Circle is refused");
    assert!(
        error
            .to_string()
            .contains("acknowledgement names another Circle"),
        "{error}"
    );
}

/// Every reclaim target names the object it deletes and, separately, the signed
/// statement that authorizes deleting it. A claim whose target IS its own
/// authority would have the Owner delete the evidence its verification rests on,
/// so each kind refuses that aliasing on the claim itself.
#[test]
fn reclaim_claims_refuse_a_target_that_aliases_its_own_authority() {
    let circle_id = CircleId::from_bytes([3; 16]);

    let mut coverage = claim_coverage(circle_id, "aliased");
    coverage.bootstrap.image.object = coverage.activation_commit.object.clone();
    let error = ReclaimClaim::CircleBootstrapImage(CircleBootstrapImageReclaimClaim {
        target: CircleBootstrapImageReclaimTarget { coverage },
        proof: CircleBootstrapReclaimProof::RecipientCoverage {
            acknowledgement: claim_ack(circle_id, "aliased"),
        },
    })
    .validate()
    .expect_err("a bootstrap image that is its own activation commit is refused");
    assert!(
        error.to_string().contains("aliases proof authority"),
        "{error}"
    );

    let snapshot = CircleSnapshotRef {
        generation: 0,
        snapshot_hash: ObjectHash::digest(b"generation zero"),
        object: proof_object("circles/aliased/snapshots/device/0.json"),
    };
    let error = ReclaimClaim::CircleSnapshotImage(CircleSnapshotImageReclaimClaim {
        target: CircleSnapshotImageReclaimTarget {
            circle_id,
            snapshot_author: claim_registration("aliased"),
            control: claim_control("aliased", 1),
            snapshot: snapshot.clone(),
            image: crate::sync::store_commit::SnapshotImageRef {
                image_hash: ObjectHash::digest(b"aliased image"),
                object: snapshot.object.clone(),
            },
        },
        superseding: CircleSnapshotRef {
            generation: 1,
            snapshot_hash: ObjectHash::digest(b"generation one"),
            object: proof_object("circles/aliased/snapshots/device/1.json"),
        },
    })
    .validate()
    .expect_err("a snapshot image that is its own metadata object is refused");
    assert!(
        error.to_string().contains("aliases proof authority"),
        "{error}"
    );
}

/// A superseded snapshot generation is only superseded by a LATER one. A claim
/// naming an equal or earlier generation as its superseding evidence is refused on
/// the claim, before any stream walk: the generation ordering is part of what the
/// claim asserts, and nothing later in verification re-establishes it if the
/// claim's own numbering is inverted.
#[test]
fn circle_snapshot_reclaim_refuses_a_superseding_generation_that_is_not_later() {
    let circle_id = CircleId::from_bytes([4; 16]);
    let claim = |target_generation: u64, superseding_generation: u64| {
        ReclaimClaim::CircleSnapshotImage(CircleSnapshotImageReclaimClaim {
            target: CircleSnapshotImageReclaimTarget {
                circle_id,
                snapshot_author: claim_registration("ordering"),
                control: claim_control("ordering", 1),
                snapshot: CircleSnapshotRef {
                    generation: target_generation,
                    snapshot_hash: ObjectHash::digest(b"target generation"),
                    object: proof_object("circles/ordering/snapshots/device/target.json"),
                },
                image: claim_image("ordering"),
            },
            superseding: CircleSnapshotRef {
                generation: superseding_generation,
                snapshot_hash: ObjectHash::digest(b"superseding generation"),
                object: proof_object("circles/ordering/snapshots/device/superseding.json"),
            },
        })
    };
    claim(0, 1)
        .validate()
        .expect("a strictly later generation is well-formed");
    for (target_generation, superseding_generation) in [(1, 1), (2, 1)] {
        let error = claim(target_generation, superseding_generation)
            .validate()
            .expect_err("a superseding generation that is not later is refused");
        assert!(
            error
                .to_string()
                .contains("superseding generation that is not later"),
            "{error}"
        );
    }
}
