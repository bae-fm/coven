use crate::snapshot_objects::validate_snapshot_object_owner_records_on;

use crate::*;

use super::fixtures::*;

/// A solely-owned, activated remote record for a Store package, and the package
/// reference a reclaim of it names. The record's object is the package's own, so
/// the reclaim closure keyed on that package finds exactly this record.
fn reclaim_target_package(
    label: &str,
    owner: &StoreBatchCommitRef,
) -> (
    coven_protocol::store_commit::StorePackageRef,
    coven_protocol::remote_object::ClosedRemoteObject,
) {
    let package = AudiencePackage::store(
        ObjectHash::digest(format!("{label} Store root").as_bytes()),
        test_candidate_family(),
        coven_protocol::write::WriteId::from_generated(format!("{label}-write")),
        owner.coord.clone(),
        1,
        format!("{label} changeset").into_bytes(),
        Vec::new(),
    )
    .expect("build reclaim target package");
    let semantic = package.to_bytes();
    let stored = format!("{label} stored package").into_bytes();
    let object = ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(format!(
            "{}.pkg",
            coven_protocol::store_commit::package_semantic_prefix(
                test_candidate_family(),
                &owner.coord.stream_id.to_string(),
                owner.coord.sequence(),
                ObjectHash::digest(&semantic),
            )
        ))
        .expect("valid reclaim package slot"),
        stored.len() as u64,
        ObjectHash::digest(&stored),
    );
    let reference = coven_protocol::store_commit::StorePackageRef {
        candidate_family: package.candidate_family(),
        content_hash: ObjectHash::digest(&semantic),
        schema_version: package.schema_version(),
        changeset_size: semantic.len() as u64,
        object: object.clone(),
    };
    let prepared = coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(
        coven_protocol::remote_object::CandidateObjectRecord {
            identity: coven_protocol::remote_object::CandidateExclusiveTarget {
                family: package.candidate_family(),
                domain:
                    coven_protocol::remote_object::CandidateExclusiveObjectDomain::StorePackage {
                        reference: reference.clone(),
                    },
                semantic_hash: ObjectHash::digest(&semantic),
                object: object.clone(),
            },
            payloads: coven_protocol::remote_object::RemoteObjectPayloads::SpooledInline,
            state: coven_protocol::remote_object::CandidateObjectState::Prepared {
                ownership: coven_protocol::remote_object::PendingCandidateOwnership {
                    pending: std::collections::BTreeSet::from([owner.clone()]),
                    nonactivated: Vec::new(),
                },
            },
        },
    );
    let mut uploaded = prepared;
    uploaded
        .mark_uploaded_verified()
        .expect("mark reclaim package uploaded");
    let activated = uploaded
        .into_activated(owner)
        .expect("activate reclaim package owner");
    let payloads = std::collections::BTreeMap::from([
        (ObjectHash::digest(&semantic), semantic),
        (ObjectHash::digest(&stored), stored),
    ]);
    let closed =
        coven_protocol::remote_object::ClosedRemoteObject::with_payloads(activated, payloads)
            .expect("close reclaim package with its payloads");
    (reference, closed)
}

/// A Store commit reference at `sequence`, distinct per `label`.
fn reclaim_commit(label: &str, sequence: u64) -> StoreBatchCommitRef {
    let coord = StoreCommitCoord {
        stream_id: coven_protocol::membership::AuthorStreamId::from_bytes([11; 32]),
        sequence,
    };
    let commit_hash = ObjectHash::digest(format!("{label} commit").as_bytes());
    StoreBatchCommitRef {
        coord: coord.clone(),
        commit_hash,
        object: reclaim_test_object(&format!(
            "{}.json",
            coven_protocol::store_commit::commit_semantic_prefix(
                test_candidate_family(),
                &coord.stream_id.to_string(),
                sequence,
                commit_hash,
            )
        )),
    }
}

#[tokio::test]
async fn reclaimed_store_package_cannot_return_to_remote_ownership() {
    let target_activation = reclaim_commit("closed-reclaimed-package/target", 1);
    let (target, package_remote) =
        reclaim_target_package("closed-reclaimed-package", &target_activation);
    let authorization_activation = reclaim_commit("closed-reclaimed-package/authority", 2);
    let receipt_activation = reclaim_commit("closed-reclaimed-package/receipt", 3);
    let authorization = coven_protocol::reclaim::ReclaimAuthorizationRef {
        authorization_hash: ObjectHash::digest(b"closed reclaim authorization"),
        evidence: coven_protocol::reclaim::ReclaimEvidenceRef {
            evidence_hash: ObjectHash::digest(b"closed reclaim evidence"),
            target: Box::new(coven_protocol::reclaim::ReclaimTarget::StorePackage(
                coven_protocol::reclaim::StorePackageReclaimTarget {
                    package: target,
                    activation: target_activation,
                },
            )),
            object: reclaim_test_object("store-v1/reclaim/evidence/closed.json"),
        },
        object: reclaim_test_object("store-v1/reclaim/authorizations/closed.json"),
    };
    let authorization_activation =
        reclaim_test_activation(authorization_activation, "authorization");
    let receipt_activation = reclaim_test_activation(receipt_activation, "receipt");
    let operation = DurableStoreReclaimOperation::Authorized {
        authorization: authorization.clone(),
        activation: authorization_activation.clone(),
    };
    let absence = ReclaimedStorePackage::absent_verified(
        authorization.clone(),
        authorization_activation.clone(),
    )
    .expect("valid absence closure");
    let object_id = absence.object_id();
    let closure_db = crate::synthetic_store::open_test_db();
    let saved_remote_for_insert = package_remote.clone();
    closure_db
        .database
        .test_sql(move |conn| {
            conn.persist_exact_remote_object(
                &saved_remote_for_insert,
                "reclaim closure fixture package",
            )
        })
        .await
        .expect("install solely-owned reclaim closure fixture");
    let operation_for_insert = operation.clone();
    let absence_for_insert = absence.clone();
    closure_db
        .database
        .test_sql(move |conn| {
            conn.transaction(|tx| {
                tx.insert_store_reclaim_operation(&operation_for_insert)?;
                tx.record_reclaimed_store_package(&absence_for_insert)
            })
        })
        .await
        .expect("close reclaimed package");

    let saved_remote_for_revival = package_remote.clone();
    closure_db
        .database
        .test_sql(move |conn| {
            assert!(conn.load_remote_object(object_id).is_err());
            assert_eq!(conn.load_reclaimed_store_package(object_id)?, Some(absence));
            assert!(conn
                .persist_exact_remote_object(&saved_remote_for_revival, "revived Store package",)
                .is_err());
            Ok(())
        })
        .await
        .expect("verify irreversible absence closure");

    let receipt = coven_protocol::reclaim::ReclaimReceiptRef {
        receipt_hash: ObjectHash::digest(b"closed reclaim receipt"),
        authorization: authorization.clone(),
        object: reclaim_test_object("store-v1/reclaim/receipts/closed.json"),
    };
    let receipted = ReclaimedStorePackage::receipted(
        authorization,
        authorization_activation,
        receipt,
        receipt_activation,
    )
    .expect("valid receipt closure");
    let receipted_for_insert = receipted.clone();
    closure_db
        .database
        .test_sql(move |conn| conn.record_reclaimed_store_package(&receipted_for_insert))
        .await
        .expect("attach receipt to reclaimed package");
    assert_eq!(
        closure_db
            .database
            .test_sql(move |conn| conn.load_reclaimed_store_package(object_id))
            .await
            .expect("load receipted closure"),
        Some(receipted)
    );
}

#[test]
fn snapshot_blob_owner_rejects_other_activation_and_later_generation() {
    let conn = Connection::open_in_memory().expect("open snapshot owner database");
    apply_coven_schema(&conn).expect("apply snapshot owner schema");
    let binding = exact_blob_binding(
        "snapshot-owner-photo",
        "0000000001000-0000-owner",
        b"snapshot owner bytes",
    );
    let expected = coven_protocol::remote_object::SnapshotObjectOwner {
        activation: snapshot_activation("verified"),
        generation: 7,
    };
    let install = |owner: coven_protocol::remote_object::SnapshotObjectOwner| {
        conn.execute("DELETE FROM remote_objects", [])
            .expect("clear snapshot owner");
        let remote = RemoteObjectRecord::snapshot_activated_blob(binding.blob(), owner)
            .expect("build snapshot-owned blob")
            .into_record();
        conn.execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
            rusqlite::params![
                remote.object_id().to_string(),
                serde_json::to_string(&remote).expect("serialize snapshot-owned blob"),
            ],
        )
        .expect("install snapshot-owned blob");
    };

    install(expected.clone());
    validate_snapshot_object_owner_records_on(&conn, &expected)
        .expect("verified snapshot owner matches");

    install(coven_protocol::remote_object::SnapshotObjectOwner {
        activation: expected.activation,
        generation: expected.generation - 1,
    });
    validate_snapshot_object_owner_records_on(&conn, &expected)
        .expect("an earlier generation remains valid snapshot ownership");

    install(coven_protocol::remote_object::SnapshotObjectOwner {
        activation: snapshot_activation("other"),
        generation: expected.generation,
    });
    assert!(validate_snapshot_object_owner_records_on(&conn, &expected).is_err());

    install(coven_protocol::remote_object::SnapshotObjectOwner {
        activation: expected.activation,
        generation: expected.generation + 1,
    });
    assert!(validate_snapshot_object_owner_records_on(&conn, &expected).is_err());
}

#[tokio::test]
async fn upload_retry_preserves_prepared_object_handoff() {
    let db = open_outbox_database("prepared-upload-retry");
    let row = local_row_blob("photo", "0000000001000-0000-a", b"photo bytes");
    let initial_row = row.clone();
    db.test_sql(move |conn| {
        conn.enqueue_blob_upload(
            "photos",
            "photo",
            &initial_row,
            Path::new("/source/first"),
            false,
            "2026-07-16T10:00:00Z",
        )
    })
    .await
    .expect("enqueue upload");

    let entry = crate::StoreDatabase::new(&db)
        .pending_blob_uploads()
        .await
        .expect("read upload")
        .pop()
        .expect("upload entry");
    let stored = exact_blob_binding("photo", "0000000001000-0000-a", b"photo bytes")
        .blob()
        .clone();
    let spool_path = PathBuf::from("/spool/prepared");
    crate::StoreDatabase::new(&db)
        .mark_blob_upload_prepared(
            &entry,
            coven_protocol::audience_package::PackageAudience::Store,
            stored.clone(),
            spool_path.clone(),
        )
        .await
        .expect("record prepared object");

    db.test_sql(move |conn| {
        conn.enqueue_blob_upload(
            "photos",
            "photo",
            &row,
            Path::new("/source/retried"),
            true,
            "2026-07-16T10:01:00Z",
        )
    })
    .await
    .expect("retry upload command");

    let entries = crate::StoreDatabase::new(&db)
        .pending_blob_uploads()
        .await
        .expect("read retried upload");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].operation,
        OutboxOperation::Upload {
            root_table: "photos".to_string(),
            root_id: "photo".to_string(),
            row: local_row_blob("photo", "0000000001000-0000-a", b"photo bytes"),
            source_path: PathBuf::from("/source/retried"),
            retain_pinned: true,
            state: OutboxUploadState::Prepared {
                authority: coven_protocol::audience_package::PackageAudience::Store,
                stored,
                spool_path,
            },
        }
    );
    crate::StoreDatabase::new(&db)
        .mark_blob_upload_created(&entries[0])
        .await
        .expect("record exact cloud creation");
    let created = crate::StoreDatabase::new(&db)
        .pending_blob_uploads()
        .await
        .expect("read Created upload");
    let OutboxOperation::Upload { state, .. } = &created[0].operation else {
        panic!("upload query returned a non-upload operation");
    };
    assert!(matches!(
        state,
        OutboxUploadState::Created {
            authority: coven_protocol::audience_package::PackageAudience::Store,
            ..
        }
    ));
}

#[tokio::test]
async fn repeated_exact_delete_resets_retry_state_without_duplication() {
    let db = open_outbox_database("repeat-delete");
    let stored = exact_blob_binding("photo", "0000000001000-0000-a", b"photo bytes")
        .blob()
        .clone();
    let delete_stored = stored.clone();
    db.test_sql(move |conn| conn.enqueue_blob_delete(&delete_stored, "2026-07-16T10:00:00Z"))
        .await
        .expect("enqueue delete");
    let failed = crate::StoreDatabase::new(&db)
        .pending_blob_deletes()
        .await
        .expect("read pending delete")
        .pop()
        .expect("delete entry");
    crate::StoreDatabase::new(&db)
        .record_outbox_failure(&failed, "provider unavailable", "2026-07-16T10:00:30Z")
        .await
        .expect("record delete failure");
    let retry_stored = stored.clone();
    db.test_sql(move |conn| conn.enqueue_blob_delete(&retry_stored, "2026-07-16T10:01:00Z"))
        .await
        .expect("repeat exact delete");

    let deletes = crate::StoreDatabase::new(&db)
        .pending_blob_deletes()
        .await
        .expect("read deletes");
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].attempt_count, 0);
    assert_eq!(deletes[0].last_attempt_at, None);
    assert_eq!(deletes[0].operation, OutboxOperation::Delete { stored });
}

#[tokio::test]
async fn exact_deletes_for_distinct_objects_remain_distinct() {
    let db = open_outbox_database("distinct-deletes");
    let first = exact_blob_binding("photo-a", "0000000001000-0000-a", b"photo a")
        .blob()
        .clone();
    let second = exact_blob_binding("photo-b", "0000000001000-0000-a", b"photo b")
        .blob()
        .clone();
    db.test_sql(move |conn| conn.enqueue_blob_delete(&first, "2026-07-16T10:00:00Z"))
        .await
        .expect("enqueue first delete");
    db.test_sql(move |conn| conn.enqueue_blob_delete(&second, "2026-07-16T10:01:00Z"))
        .await
        .expect("enqueue second delete");

    let deletes = crate::StoreDatabase::new(&db)
        .pending_blob_deletes()
        .await
        .expect("read deletes");
    assert_eq!(deletes.len(), 2);
    assert_ne!(deletes[0].operation, deletes[1].operation);
}

#[test]
fn blob_bindings_install_only_for_exact_winning_row_stamps() {
    let (_spool, store_dir) = coven_foundation::store_dir::temp_store_dir();
    let mut conn = Connection::open_in_memory().expect("open");
    apply_coven_schema(&conn).expect("apply coven schema");
    conn.execute_batch(
        "CREATE TABLE photos (
            id TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            hash TEXT NOT NULL,
            cloud_path TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )
    .expect("create photos");
    let table = blob_binding_table();
    let tables = vec![table];
    let gates = Gates::from_tables(&conn, &tables).expect("build gates");
    DatabaseTestSql::new(&conn)
        .insert_blob_row("winner", "0000000001000-0000-a", b"winner bytes")
        .expect("insert winning blob row");
    DatabaseTestSql::new(&conn)
        .insert_blob_row("loser", "0000000002000-0000-b", b"loser bytes")
        .expect("insert losing blob row");

    let package = AudiencePackage::store(
        ObjectHash::digest(b"root"),
        test_candidate_family(),
        WriteId::from_generated("write-1".to_string()),
        test_commit_coord(),
        1,
        Vec::new(),
        vec![
            exact_blob_binding("winner", "0000000001000-0000-a", b"winner bytes"),
            exact_blob_binding("loser", "0000000001000-0000-b", b"loser bytes"),
        ],
    )
    .expect("build package");
    let activation = BlobActivation {
        coord: test_commit_coord(),
    };

    let tx = conn.transaction().expect("begin");
    Database::install_pulled_blob_activations_on(&tx, &package, &test_commit_ref())
        .expect("install pulled blob activation");
    let winning_rows = [crate::WinningRow {
        table: "photos".to_string(),
        row_id: "winner".to_string(),
        row_stamp: Some("0000000001000-0000-a".to_string()),
    }];
    assert_eq!(
        crate::store::test_install_winning_blob_bindings(
            &tx,
            &store_dir,
            &gates,
            &tables,
            &package,
            &activation,
            &winning_rows,
        )
        .expect("install winning binding"),
        1
    );
    tx.commit().expect("commit");

    assert_eq!(
        conn.query_row("SELECT count(*) FROM blob_locators", [], |row| row
            .get::<_, i64>(0))
            .expect("count locators"),
        1
    );
    assert_eq!(
        conn.query_row("SELECT row_id FROM row_blob_locators", [], |row| row
            .get::<_, String>(0))
            .expect("read binding"),
        "winner"
    );
    let resolved = Database::row_blob_ref_on(&conn, &gates, &tables[0], "winner")
        .expect("resolve exact row blob reference");
    assert_eq!(resolved.row_stamp(), "0000000001000-0000-a");
    assert_eq!(
        resolved.plaintext_hash(),
        ObjectHash::digest(b"winner bytes")
    );
    assert_eq!(
        resolved
            .stored()
            .expect("remote row carries exact locator")
            .locator()
            .blob_id(),
        "winner"
    );
}

#[test]
fn mismatched_blob_values_roll_back_locator_installation_with_rows() {
    let (_spool, store_dir) = coven_foundation::store_dir::temp_store_dir();
    let mut conn = Connection::open_in_memory().expect("open");
    apply_coven_schema(&conn).expect("apply coven schema");
    conn.execute_batch(
        "CREATE TABLE photos (
            id TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            hash TEXT NOT NULL,
            cloud_path TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )
    .expect("create photos");
    let tables = vec![blob_binding_table()];
    let gates = Gates::from_tables(&conn, &tables).expect("build gates");
    DatabaseTestSql::new(&conn)
        .insert_blob_row("photo", "0000000001000-0000-a", b"actual bytes")
        .expect("insert blob row");
    let package = AudiencePackage::store(
        ObjectHash::digest(b"root"),
        test_candidate_family(),
        WriteId::from_generated("write-1".to_string()),
        test_commit_coord(),
        1,
        Vec::new(),
        vec![exact_blob_binding(
            "photo",
            "0000000001000-0000-a",
            b"different bytes",
        )],
    )
    .expect("build package");
    let activation = BlobActivation {
        coord: test_commit_coord(),
    };

    let tx = conn.transaction().expect("begin");
    Database::install_pulled_blob_activations_on(&tx, &package, &test_commit_ref())
        .expect("install pulled blob activation");
    let winning_rows = [crate::WinningRow {
        table: "photos".to_string(),
        row_id: "photo".to_string(),
        row_stamp: Some("0000000001000-0000-a".to_string()),
    }];
    let error = crate::store::test_install_winning_blob_bindings(
        &tx,
        &store_dir,
        &gates,
        &tables,
        &package,
        &activation,
        &winning_rows,
    )
    .expect_err("mismatched locator must fail");
    assert!(error
        .to_string()
        .contains("does not match winning row values"));
    tx.rollback().expect("roll back");
    assert_eq!(
        conn.query_row("SELECT count(*) FROM blob_locators", [], |row| row
            .get::<_, i64>(0))
            .expect("count locators"),
        0
    );
}
