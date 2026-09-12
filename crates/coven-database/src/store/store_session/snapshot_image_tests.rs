use super::*;

#[test]
fn snapshot_blob_settlement_does_not_restore_obsolete_row_bindings() {
    use crate::tests::fixtures::exact_blob_binding;
    use coven_protocol::remote_object::{RemoteObjectRecord, SnapshotObjectOwner};

    let live = Connection::open_in_memory().expect("open live database");
    crate::apply_coven_schema(&live).expect("apply live schema");
    let owner = SnapshotObjectOwner::Store {
        metadata_slot: coven_protocol::objects::ObjectSlot::logical(
            "store-v1/snapshots/accepted.json".to_string(),
        )
        .expect("snapshot metadata slot"),
    };
    let binding = exact_blob_binding("photo", "0000000001000-0000-owner", b"accepted photo");
    let remote = RemoteObjectRecord::snapshot_activated_blob(binding.blob(), owner.clone())
        .expect("snapshot ownership")
        .into_record();
    let object_id = remote.object_id();
    let plan = crate::PreparedSnapshotBlob {
        bindings: vec![binding],
        authority: coven_protocol::audience_package::PackageAudience::Store,
        remote,
    };
    crate::install_snapshot_blob_plan_on(&live, &plan).expect("install accepted row binding");
    live.execute("DELETE FROM row_blob_locators", [])
        .expect("unpublished row deletion removes its live binding");

    let transaction = live
        .unchecked_transaction()
        .expect("begin snapshot settlement");
    crate::install_snapshot_blob_plans_on(&transaction, &[plan])
        .expect("retain accepted snapshot payload");
    transaction.commit().expect("commit snapshot settlement");

    assert_eq!(
        live.query_row("SELECT COUNT(*) FROM row_blob_locators", [], |row| row
            .get::<_, i64>(0))
            .expect("count live row bindings"),
        0,
        "snapshot settlement must not recreate a binding removed by unpublished work"
    );
    let retained = crate::remote_object_records::load_remote_object_on(&live, object_id)
        .expect("read retained snapshot payload");
    assert_eq!(retained.snapshot_owners().collect::<Vec<_>>(), [&owner]);
}

#[test]
fn staged_database_cleanup_reports_a_target_that_remains() {
    let directory = tempfile::tempdir().expect("snapshot cleanup directory");
    let path = directory.path().join("snapshot.db");
    std::fs::create_dir(&path).expect("create unremovable snapshot target");

    let error = SnapshotDatabaseImage::prepare(path.clone())
        .expect_err("an unremovable staged database must fail");

    assert!(
        matches!(
            error,
            SnapshotImageError::Cleanup {
                path: ref failed_path,
                ..
            } if *failed_path == path
        ),
        "{error}"
    );
    std::fs::remove_dir(path).expect("remove cleanup obstruction");
}

#[test]
fn staged_database_cleanup_preserves_the_operation_failure() {
    let directory = tempfile::tempdir().expect("snapshot cleanup directory");
    let path = directory.path().join("snapshot.db");
    let staged =
        SnapshotDatabaseImage::prepare(path.clone()).expect("prepare staged database image");
    std::fs::create_dir(&path).expect("create cleanup obstruction");

    let error = staged
        .finish::<()>(Err(SnapshotImageError::Projection(
            "injected operation failure".to_string(),
        )))
        .expect_err("operation and cleanup failures must both surface");

    assert!(
        matches!(
            error,
            SnapshotImageError::CleanupAfterFailure {
                path: ref failed_path,
                ref cause,
                ..
            } if *failed_path == path
                && matches!(
                    cause.as_ref(),
                    SnapshotImageError::Projection(message)
                        if message == "injected operation failure"
                )
        ),
        "{error}"
    );
    std::fs::remove_dir(path).expect("remove cleanup obstruction");
}

#[test]
fn staged_database_creation_refuses_an_existing_target() {
    let directory = tempfile::tempdir().expect("snapshot creation directory");
    let path = directory.path().join("snapshot.db");
    std::fs::write(&path, b"existing database").expect("write existing database");

    let result = SnapshotDatabaseImage::create(path.clone(), b"replacement database");

    assert!(result.is_err(), "creation must refuse an existing database");
    assert_eq!(
        std::fs::read(path).expect("read preserved database"),
        b"existing database"
    );
}

#[test]
fn blob_graph_installation_does_not_require_sqlite_sidecar_paths() {
    let source = Connection::open_in_memory().expect("open source database");
    crate::apply_coven_schema(&source).expect("apply source schema");
    source
        .execute_batch("CREATE TABLE marker (value TEXT NOT NULL) STRICT;")
        .expect("create source schema");
    let bytes =
        crate::connection_io::serialize_database_image(&source).expect("serialize source database");
    let directory = tempfile::tempdir().expect("snapshot directory");
    let path = directory.path().join("snapshot.db");
    let image = SnapshotDatabaseImage::create(path.clone(), &bytes).expect("create staged image");
    let journal_path = PathBuf::from(format!("{}-journal", path.display()));
    std::fs::create_dir(&journal_path).expect("reserve SQLite journal path");

    let owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: coven_protocol::objects::ObjectSlot::logical(
            "store-v1/snapshots/sidecar.json".to_string(),
        )
        .expect("snapshot slot"),
    };
    let result = image.install_blob_graph(&owner, &[], &std::collections::BTreeSet::new());

    std::fs::remove_dir(journal_path).expect("remove journal-path reservation");
    let image = result.expect("install without opening the staged image as a disk database");
    let installed = image.read_and_discard().expect("read installed image");
    let mut connection = Connection::open_in_memory().expect("open installed image connection");
    crate::connection_io::deserialize_database_image_into(&mut connection, &installed)
        .expect("deserialize installed image");
    let table_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'marker')",
            [],
            |row| row.get(0),
        )
        .expect("read installed schema");
    assert!(table_exists);
}

#[test]
fn exported_orphan_inventory_releases_snapshot_owners_without_releasing_commit_or_replay_owners() {
    use crate::tests::fixtures::{exact_blob_binding, test_commit_ref};
    use crate::RetainedReplayOwner;
    use coven_protocol::remote_object::{RemoteObjectRecord, SnapshotObjectOwner};

    let source = Connection::open_in_memory().expect("open live database");
    crate::apply_coven_schema(&source).expect("apply live schema");
    let owner_a = SnapshotObjectOwner::Store {
        metadata_slot: coven_protocol::objects::ObjectSlot::opaque(
            "store-v1/snapshots/a.json".to_string(),
            "provider-a".to_string(),
        )
        .expect("first metadata slot"),
    };
    let owner_b = SnapshotObjectOwner::Store {
        metadata_slot: coven_protocol::objects::ObjectSlot::opaque(
            "store-v1/snapshots/b.json".to_string(),
            "provider-b".to_string(),
        )
        .expect("second metadata slot"),
    };
    let binding = exact_blob_binding("retained", "0000000001000-0000-a", b"retained bytes");
    let mut blob = RemoteObjectRecord::snapshot_activated_blob(binding.blob(), owner_a.clone())
        .expect("first live blob owner")
        .into_record();
    blob.merge_snapshot_owner(binding.blob(), owner_b.clone())
        .expect("second live blob owner");
    blob.merge_blob_activation(binding.blob(), &test_commit_ref())
        .expect("retain exact original commit provenance");
    let replay = RetainedReplayOwner {
        commit: test_commit_ref(),
        input_hash: ObjectHash::digest(b"retained replay input"),
    };
    source
        .execute(
            "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
            rusqlite::params![
                blob.object_id().to_string(),
                serde_json::to_string(&blob).expect("encode blob")
            ],
        )
        .expect("install retained blob fixture");
    let transaction = source
        .unchecked_transaction()
        .expect("begin retained owner index");
    let RetainedReplayOwner { commit, input_hash } = &replay;
    transaction
        .execute(
            "INSERT INTO retained_merge_materializations
         (device_id, seq, commit_ref, input_hash, canonical_input)
         VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                commit.coord.stream_id.to_string(),
                i64::try_from(commit.coord.sequence()).expect("test sequence fits SQLite"),
                serde_json::to_string(commit).expect("encode retained commit"),
                input_hash.to_string(),
                b"retained replay input",
            ],
        )
        .expect("retain the indexed input");
    crate::remote_object_records::index_retained_replay_owner_on(
        &transaction,
        blob.object_id(),
        &replay,
    )
    .expect("index retained replay owner");
    transaction.commit().expect("commit retained owner index");

    let before =
        crate::connection_io::serialize_database_image(&source).expect("serialize live state");
    let directory = tempfile::tempdir().expect("snapshot directory");
    let image = SnapshotDatabaseImage::create(directory.path().join("snapshot.db"), &before)
        .expect("create image copy")
        .install_blob_graph(&owner_b, &[], &std::collections::BTreeSet::new())
        .expect("rewrite inherited owners even without new blob plans")
        .read_and_discard()
        .expect("read exported image");
    let mut exported = Connection::open_in_memory().expect("open exported image");
    crate::connection_io::deserialize_database_image_into(&mut exported, &image)
        .expect("deserialize exported image");
    let object_id = blob.object_id();
    let record = crate::remote_object_records::load_remote_object_on(&exported, object_id)
        .expect("read exported ownership");
    assert!(record.snapshot_owners().next().is_none());
    assert_eq!(
        record.stored_blob_commit_owners(),
        blob.stored_blob_commit_owners()
    );
    assert_eq!(
        record.object(),
        blob.object(),
        "export preserves exact object identity"
    );
    assert_eq!(
        crate::remote_object_records::indexed_retained_replay_owners_on(&exported, object_id)
            .expect("read exported replay pins"),
        std::collections::BTreeSet::from([replay]),
        "the exported image keeps the replay pin in its index"
    );
    assert_eq!(
        crate::remote_object_records::load_remote_object_on(&source, object_id)
            .expect("read preserved live ownership"),
        blob
    );
    assert_eq!(
        crate::connection_io::serialize_database_image(&source)
            .expect("serialize preserved live state"),
        before
    );
}
