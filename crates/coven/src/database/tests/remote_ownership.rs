use crate::database::remote_object_records::load_reclaimed_store_package_on;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;
use crate::database::remote_object_records::record_reclaimed_store_package_on;
use crate::database::snapshot_objects::validate_snapshot_object_owner_records_on;
use crate::database::store_reclaim_records::insert_store_reclaim_operation_on;

use super::super::*;

use super::fixtures::*;

#[tokio::test]
async fn reclaimed_store_package_cannot_return_to_remote_ownership() {
    let db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "closed-reclaimed-package",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create Store");
    let first_changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('reclaim-target', 'target', NULL, \
           '0000000001000-0000-reclaim', '2026-01-01')",
        ],
    )
    .await;
    let target_activation = store
        .publish_changeset("founder", 1, &first_changeset, db.schema_version())
        .await
        .expect("publish target package");
    let authority_changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('reclaim-authority', 'authority', NULL, \
           '0000000002000-0000-reclaim', '2026-01-01')",
        ],
    )
    .await;
    let authorization_activation = store
        .publish_changeset("founder", 2, &authority_changeset, db.schema_version())
        .await
        .expect("publish later Store position");
    let receipt_changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('reclaim-receipt', 'receipt', NULL, \
           '0000000003000-0000-reclaim', '2026-01-01')",
        ],
    )
    .await;
    let receipt_activation = store
        .publish_changeset("founder", 3, &receipt_changeset, db.schema_version())
        .await
        .expect("publish receipt Store position");
    let (_, founder, _) = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let target_commit = store
        .founder_device()
        .await
        .expect("load founder Store")
        .load_commit_for_test(&target_activation)
        .await
        .expect("load target commit");
    assert_eq!(target_commit.author(), &founder);
    let target = target_commit
        .store_package()
        .expect("target commit has a package")
        .clone();
    let authorization = crate::sync::store::ReclaimAuthorizationRef {
        authorization_hash: ObjectHash::digest(b"closed reclaim authorization"),
        evidence: crate::sync::store::ReclaimEvidenceRef {
            evidence_hash: ObjectHash::digest(b"closed reclaim evidence"),
            target: Box::new(crate::sync::store::ReclaimTarget::StorePackage(
                crate::sync::store::StorePackageReclaimTarget {
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
    let mut saved_remote = db
        .call(move |conn| load_remote_object_on(conn, object_id))
        .await
        .expect("load activated package ownership");
    saved_remote
        .remove_all_retained_replay_owners()
        .expect("remove unrelated replay retention from reclaim closure fixture");
    let closure_db = crate::sync::test_helpers::open_test_db();
    let saved_remote_for_insert = saved_remote.clone();
    closure_db
        .call(move |conn| {
            persist_exact_remote_object_on(
                conn,
                &saved_remote_for_insert,
                "reclaim closure fixture package",
            )
        })
        .await
        .expect("install solely-owned reclaim closure fixture");
    let operation_for_insert = operation.clone();
    let absence_for_insert = absence.clone();
    closure_db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            insert_store_reclaim_operation_on(&tx, &operation_for_insert)?;
            record_reclaimed_store_package_on(&tx, &absence_for_insert)?;
            tx.commit().map_err(DbError::from)
        })
        .await
        .expect("close reclaimed package");

    let saved_remote_for_revival = saved_remote.clone();
    closure_db
        .call(move |conn| {
            assert!(load_remote_object_on(conn, object_id).is_err());
            assert_eq!(
                load_reclaimed_store_package_on(conn, object_id)?,
                Some(absence)
            );
            assert!(persist_exact_remote_object_on(
                conn,
                &saved_remote_for_revival,
                "revived Store package",
            )
            .is_err());
            Ok(())
        })
        .await
        .expect("verify irreversible absence closure");

    let receipt = crate::sync::store::ReclaimReceiptRef {
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
        .call(move |conn| record_reclaimed_store_package_on(conn, &receipted_for_insert))
        .await
        .expect("attach receipt to reclaimed package");
    assert_eq!(
        closure_db
            .call(move |conn| load_reclaimed_store_package_on(conn, object_id))
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
    let expected = crate::protocol::remote_object::SnapshotObjectOwner {
        activation: snapshot_activation("verified"),
        generation: 7,
    };
    let install = |owner: crate::protocol::remote_object::SnapshotObjectOwner| {
        conn.execute("DELETE FROM remote_objects", [])
            .expect("clear snapshot owner");
        let remote = RemoteObjectRecord::snapshot_activated_blob(binding.blob(), owner)
            .expect("build snapshot-owned blob");
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

    install(crate::protocol::remote_object::SnapshotObjectOwner {
        activation: expected.activation,
        generation: expected.generation - 1,
    });
    validate_snapshot_object_owner_records_on(&conn, &expected)
        .expect("an earlier generation remains valid snapshot ownership");

    install(crate::protocol::remote_object::SnapshotObjectOwner {
        activation: snapshot_activation("other"),
        generation: expected.generation,
    });
    assert!(validate_snapshot_object_owner_records_on(&conn, &expected).is_err());

    install(crate::protocol::remote_object::SnapshotObjectOwner {
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
    db.call(move |conn| {
        CloudOutboxRecords::new(conn).enqueue_upload(
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

    let entry = db
        .get_pending_cloud_uploads()
        .await
        .expect("read upload")
        .pop()
        .expect("upload entry");
    let stored = exact_blob_binding("photo", "0000000001000-0000-a", b"photo bytes")
        .blob()
        .clone();
    let spool_path = PathBuf::from("/spool/prepared");
    db.mark_cloud_upload_prepared(
        &entry,
        crate::protocol::audience_package::PackageAudience::Store,
        stored.clone(),
        spool_path.clone(),
    )
    .await
    .expect("record prepared object");

    db.call(move |conn| {
        CloudOutboxRecords::new(conn).enqueue_upload(
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

    let entries = db
        .get_pending_cloud_uploads()
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
                authority: crate::protocol::audience_package::PackageAudience::Store,
                stored,
                spool_path,
            },
        }
    );
    db.mark_cloud_upload_created(&entries[0])
        .await
        .expect("record exact cloud creation");
    let created = db
        .get_pending_cloud_uploads()
        .await
        .expect("read Created upload");
    let OutboxOperation::Upload { state, .. } = &created[0].operation else {
        panic!("upload query returned a non-upload operation");
    };
    assert!(matches!(
        state,
        OutboxUploadState::Created {
            authority: crate::protocol::audience_package::PackageAudience::Store,
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
    db.call(move |conn| {
        CloudOutboxRecords::new(conn).enqueue_delete(&delete_stored, "2026-07-16T10:00:00Z")
    })
    .await
    .expect("enqueue delete");
    let failed = db
        .get_pending_cloud_deletes()
        .await
        .expect("read pending delete")
        .pop()
        .expect("delete entry");
    db.record_cloud_outbox_failure(&failed, "provider unavailable", "2026-07-16T10:00:30Z")
        .await
        .expect("record delete failure");
    let retry_stored = stored.clone();
    db.call(move |conn| {
        CloudOutboxRecords::new(conn).enqueue_delete(&retry_stored, "2026-07-16T10:01:00Z")
    })
    .await
    .expect("repeat exact delete");

    let deletes = db.get_pending_cloud_deletes().await.expect("read deletes");
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
    db.call(move |conn| {
        CloudOutboxRecords::new(conn).enqueue_delete(&first, "2026-07-16T10:00:00Z")
    })
    .await
    .expect("enqueue first delete");
    db.call(move |conn| {
        CloudOutboxRecords::new(conn).enqueue_delete(&second, "2026-07-16T10:01:00Z")
    })
    .await
    .expect("enqueue second delete");

    let deletes = db.get_pending_cloud_deletes().await.expect("read deletes");
    assert_eq!(deletes.len(), 2);
    assert_ne!(deletes[0].operation, deletes[1].operation);
}

#[test]
fn blob_bindings_install_only_for_exact_winning_row_stamps() {
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
    insert_blob_row(&conn, "winner", "0000000001000-0000-a", b"winner bytes");
    insert_blob_row(&conn, "loser", "0000000002000-0000-b", b"loser bytes");

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
    let winning_rows = [crate::database::WinningRow {
        table: "photos".to_string(),
        row_id: "winner".to_string(),
        row_stamp: Some("0000000001000-0000-a".to_string()),
    }];
    assert_eq!(
        Database::install_winning_blob_bindings_on(
            &tx,
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
    insert_blob_row(&conn, "photo", "0000000001000-0000-a", b"actual bytes");
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
    let winning_rows = [crate::database::WinningRow {
        table: "photos".to_string(),
        row_id: "photo".to_string(),
        row_stamp: Some("0000000001000-0000-a".to_string()),
    }];
    let error = Database::install_winning_blob_bindings_on(
        &tx,
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
