//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them, exercising the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use libsqlite3_sys as ffi;

use crate::blob::BlobPlan;
use crate::library_dir::LibraryDir;
use crate::sync::pull::pull_changes;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::session::SyncSession;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// Capture a changeset's bytes after running `stmts`.
unsafe fn capture_bytes(db: *mut ffi::sqlite3, stmts: &[&str]) -> Vec<u8> {
    let session = SyncSession::start(db).expect("start session");
    for s in stmts {
        exec(db, s);
    }
    session
        .changeset()
        .expect("changeset")
        .expect("non-empty")
        .as_bytes()
        .to_vec()
}

fn temp_library_dir() -> (tempfile::TempDir, LibraryDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = LibraryDir::new(tmp.path());
    (tmp, dir)
}

#[tokio::test]
async fn pull_applies_remote_changeset_and_surfaces_row_changes() {
    unsafe {
        init_synced_tables();
        let storage = MockSyncStorage::new();

        // Source device records a note as changeset seq 1.
        let db1 = open_memory_db();
        create_synced_schema(db1);
        let cs = capture_bytes(
            db1,
            &["INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')"],
        );
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        // Second device pulls.
        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (_tmp, ld) = temp_library_dir();
        let (updated, result) = pull_changes(
            db2,
            &storage,
            "dev2",
            &HashMap::new(),
            None,
            &ld,
            &NoopBlobPlan,
        )
        .await
        .expect("pull");

        assert_eq!(result.changesets_applied, 1);
        assert_eq!(updated.get("dev1"), Some(&1));
        assert_eq!(
            query_text(db2, "SELECT title FROM notes WHERE id = 'n1'"),
            "First"
        );
        assert!(result
            .row_changes
            .iter()
            .any(|c| c.table == "notes" && c.pk() == Some("n1")));

        ffi::sqlite3_close(db1);
        ffi::sqlite3_close(db2);
    }
}

#[tokio::test]
async fn pull_skips_changeset_from_newer_schema() {
    unsafe {
        init_synced_tables();
        let storage = MockSyncStorage::new();

        let db1 = open_memory_db();
        create_synced_schema(db1);
        let cs = capture_bytes(
            db1,
            &["INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'Future', NULL, '0000000001000-0000-dev1', '2026-01-01')"],
        );
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION + 1);

        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (updated, result) = pull_changes(
            db2,
            &storage,
            "dev2",
            &HashMap::new(),
            None,
            &temp_library_dir().1,
            &NoopBlobPlan,
        )
        .await
        .expect("pull");

        assert_eq!(result.changesets_applied, 0);
        assert_eq!(result.skipped_schema, 1);
        // Cursor still advances past the skipped seq so we don't re-fetch it.
        assert_eq!(updated.get("dev1"), Some(&1));
        assert!(!row_exists(db2, "SELECT 1 FROM notes WHERE id = 'n1'"));

        ffi::sqlite3_close(db1);
        ffi::sqlite3_close(db2);
    }
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    unsafe {
        init_synced_tables();
        let storage = MockSyncStorage::new();

        // Source: a note + a cover photo, with the photo file present locally.
        let src_photos = tempfile::tempdir().expect("src photos");
        std::fs::write(src_photos.path().join("p1"), b"PHOTOBYTES").expect("write photo");
        let src_plan = PhotoBlobPlan {
            dir: src_photos.path().to_path_buf(),
        };

        let db1 = open_memory_db();
        create_synced_schema(db1);
        let cs = capture_bytes(
            db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );

        // Upload the blobs the changeset references, then publish the changeset.
        let changes = crate::changeset::walk(&cs).expect("walk");
        for b in src_plan.blobs_to_push(&changes) {
            let data = std::fs::read(&b.local_path).expect("read photo");
            storage
                .put_blob(&b.namespace, &b.id, b.scope.clone(), data)
                .await
                .expect("put_blob");
        }
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        // Destination pulls; its plan points photos at its own directory.
        let dst_photos = tempfile::tempdir().expect("dst photos");
        let dst_plan = PhotoBlobPlan {
            dir: dst_photos.path().to_path_buf(),
        };
        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (_t, ld) = temp_library_dir();
        let (_updated, result) = pull_changes(
            db2,
            &storage,
            "dev2",
            &HashMap::new(),
            None,
            &ld,
            &dst_plan,
        )
        .await
        .expect("pull");

        assert_eq!(result.changesets_applied, 1);
        assert!(!result.asset_downloads_failed);
        let downloaded = std::fs::read(dst_photos.path().join("p1")).expect("downloaded photo");
        assert_eq!(downloaded, b"PHOTOBYTES");

        ffi::sqlite3_close(db1);
        ffi::sqlite3_close(db2);
    }
}
