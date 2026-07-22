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
    let membership = crate::sync::pull::load_cycle_membership(&store.storage, &db)
        .await
        .expect("load Store membership");
    let chain = membership
        .chain
        .as_ref()
        .expect("initialized Store has membership");
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
    let stable = db
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
    let registrations = db
        .activated_store_device_registration_records()
        .await
        .expect("load active registrations");

    let selected = choose_snapshot(&store.storage, &store.root, &registrations)
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
    let activated = crate::sync::store_objects::load_commit_ref(
        &store.storage,
        store.root.store_root_hash,
        &activation,
        &founder,
    )
    .await
    .expect("load package activation")
    .value;
    let package = activated
        .store_package()
        .expect("activation carries Store package")
        .clone();
    let evidence = ReclaimEvidence::signed(
        store.root.store_root_hash,
        StorePackageReclaimClaim {
            target: StorePackageReclaimTarget {
                package: package.clone(),
                activation,
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
        },
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
        package,
        evidence_ref,
        StoreReclaimAuthority {
            membership: activated.membership_state,
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
    relocated.target.object =
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
        crate::database::Database::remove_retained_replay_ownership_from_snapshot_on(&transaction)?;
        transaction.commit().map_err(crate::database::DbError::from)
    })
    .await
    .expect("release retained replay package ownership");
    let mut authorization_activation = opened.evidence.value.claim.target.activation.clone();
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
    let deletion = execute_reclaim_delete(&db, &store.storage, operation).await;
    assert!(
        deletion.is_err(),
        "nonexistent snapshot and acknowledgement refs must not authorize deletion"
    );
    let target = &opened.evidence.value.claim.target;
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
    let (target_commit, _) = crate::sync::store::pull::load_commit_with_author(
        &store.storage,
        &store.root,
        &target_activation,
    )
    .await
    .expect("load target activation");
    let target_package = target_commit
        .store_package()
        .expect("target activation carries a Store package")
        .clone();
    let StoreCommitCoord { stream_id, .. } = target_activation.coord;
    let coverage = CommitFrontier(BTreeMap::from([(stream_id, target_activation.clone())]));
    let membership = crate::sync::pull::load_cycle_membership(&store.storage, &db)
        .await
        .expect("load reclaim membership");
    let chain = membership
        .chain
        .as_ref()
        .expect("initialized Store has membership");
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
    let snapshot = db
        .latest_local_store_snapshot()
        .await
        .expect("load covering snapshot")
        .expect("covering snapshot exists");
    let acknowledgement = db
        .latest_local_store_ack()
        .await
        .expect("load covering acknowledgement")
        .expect("covering acknowledgement exists")
        .reference;
    db.call(|connection| {
        let transaction = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        crate::database::Database::remove_retained_replay_ownership_from_snapshot_on(&transaction)?;
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
        &db,
        &store.storage,
        &device_id,
        &signer,
        reclaim_membership,
        &store.root,
        StorePackageReclaimClaim {
            target: StorePackageReclaimTarget {
                package: target_package.clone(),
                activation: target_activation.clone(),
            },
            covering_snapshot: StoreSnapshotLocator {
                author_registration: snapshot.meta.author_registration.clone(),
                snapshot: snapshot.reference.clone(),
            },
            acknowledgements: vec![acknowledgement],
        },
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
        &db,
        &store.storage,
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

    let deletion = execute_reclaim_delete(&db, &store.storage, authorized.clone()).await;

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
        execute_reclaim_delete(&db, &store.storage, authorized)
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
