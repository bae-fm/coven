//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them through a real [`crate::database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use crate::blob::BlobPlan;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cycle;
use crate::sync::encrypted_storage::EncryptedSyncStorage;
use crate::sync::pull::pull_changes;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::service::SyncService;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// Pull into `db` (suspending its capture session first, as the protocol requires).
async fn pull_into(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &crate::library_dir::LibraryDir,
    blob_plan: &dyn BlobPlan,
) -> (HashMap<String, u64>, crate::sync::pull::PullResult) {
    db.take_changeset_and_suspend()
        .await
        .expect("suspend before pull");
    let r = pull_changes(
        db,
        &test_synced_tables(),
        storage,
        device_id,
        cursors,
        library_dir,
        blob_plan,
    )
    .await
    .expect("pull");
    db.resume_session().await.expect("resume after pull");
    r
}

#[tokio::test]
async fn pull_applies_remote_changeset_and_surfaces_row_changes() {
    let storage = MockSyncStorage::new();

    // Source device records a note as changeset seq 1.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Second device pulls.
    let db2 = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let (updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &NoopBlobPlan).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "First"
    );
    assert!(result
        .row_changes
        .iter()
        .any(|c| c.table == "notes" && c.pk() == Some("n1")));
}

#[tokio::test]
async fn pull_skips_changeset_from_newer_schema() {
    let storage = MockSyncStorage::new();

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Future', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION + 1);

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.skipped_schema, 1);
    // The cursor must NOT advance past a genuine newer-schema changeset: it
    // becomes applicable once this app updates, and an already-running device
    // never re-bootstraps, so advancing would strand its rows forever. Leaving
    // the cursor put re-fetches seq 1 after the upgrade.
    assert_eq!(updated.get("dev1"), None);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

#[tokio::test]
async fn pull_does_not_advance_cursor_past_a_blob_failed_changeset() {
    let storage = MockSyncStorage::new();

    // Source dev1: seq 1 references a photo blob; seq 2 is a plain note.
    let db1 = open_test_db();
    let cs1 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'One', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('ph1', 'n1', 'attach', '0000000001001-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs1, SCHEMA_VERSION);
    // The photo blob is intentionally never uploaded, so seq 1's blob download
    // fails on the puller (a transient cloud unavailability, in the real world).
    let cs2 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n2', 'Two', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 2, &cs2, SCHEMA_VERSION);

    // The puller resolves note_photos to blobs, so seq 1's missing blob fails
    // while seq 2 (no blob) would succeed.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let plan = PhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &plan,
    )
    .await;

    assert!(
        result.asset_downloads_failed,
        "seq 1's blob download must fail"
    );
    // The cursor must NOT jump to 2 past the blob-failed seq 1 — otherwise seq 1's
    // blob would never be re-fetched. It stays before seq 1 so the next cycle
    // resumes there.
    assert_ne!(
        updated.get("dev1"),
        Some(&2),
        "cursor must not advance past the blob-failed seq",
    );
    assert_eq!(
        updated.get("dev1"),
        None,
        "cursor stays before the blob-failed seq 1",
    );
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let storage = MockSyncStorage::new();

    // Source: a note + a cover photo, with the photo file present locally.
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1"), b"PHOTOBYTES").expect("write photo");
    let src_plan = PhotoBlobPlan {
        dir: src_photos.path().to_path_buf(),
    };

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    // Upload the blobs the changeset references, then publish the changeset.
    // Resolve the public scope to the internal key scope exactly as the push
    // loop does — `PhotoBlobPlan` only emits Derived/Master, which pass through.
    let changes = crate::changeset::walk(&cs).expect("walk");
    for b in src_plan.blobs_to_push(&changes) {
        let data = std::fs::read(&b.local_path).expect("read photo");
        let resolved = db1
            .resolve_blob_scope(b.scope.clone())
            .await
            .expect("resolve scope");
        storage
            .put_blob(&b.namespace, &b.id, resolved, data)
            .await
            .expect("put_blob");
    }
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Destination pulls; its plan points photos at its own directory.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let dst_plan = PhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_t, ld) = temp_library_dir();
    let (_updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &dst_plan).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let downloaded = std::fs::read(dst_photos.path().join("p1")).expect("downloaded photo");
    assert_eq!(downloaded, b"PHOTOBYTES");
}

/// Full encrypted blob round-trip through `EncryptedSyncStorage` over a shared
/// `CloudHome`. Device A publishes a note plus its cover photo via the real
/// `SyncService::sync`; the blob lands ciphertext at rest. Device B — a fresh DB
/// with its own asset directory but the same library key — pulls, downloads the
/// blob, decrypts it, and recovers the original bytes byte-for-byte.
#[tokio::test]
async fn encrypted_blob_round_trips_and_second_device_decrypts() {
    // One cloud and one library key, shared by both devices.
    let storage = EncryptedSyncStorage::new(
        Box::new(InMemoryCloudHome::new()),
        EncryptionService::new_with_key(&[7u8; 32]),
    );

    // Device A: a note and its cover photo, the file present locally.
    let plaintext = b"COVER-ART-BYTES";
    let src_photos = tempfile::tempdir().expect("src photos");
    std::fs::write(src_photos.path().join("p1cover"), plaintext).expect("write photo");
    let src_plan = PhotoBlobPlan {
        dir: src_photos.path().to_path_buf(),
    };

    let db1 = open_test_db();
    // Write the changes, capture+suspend, then drive sync() with the captured
    // bytes — it gates, uploads blobs, and builds the envelope. sync() does not
    // put_changeset itself; the caller pushes the returned envelope.
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1cover', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    let outgoing = db1
        .take_changeset_and_suspend()
        .await
        .expect("capture outgoing");

    let service = SyncService::new("dev1".to_string());
    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_library_dir();
    let result = service
        .sync(
            &db1,
            &test_synced_tables(),
            outgoing,
            0,
            &HashMap::new(),
            &storage,
            "2026-01-01T00:00:00Z",
            "",
            &keypair,
            &ld1,
            &src_plan,
        )
        .await
        .expect("sync");
    db1.resume_session().await.expect("resume");
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        None,
        &HashMap::new(),
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");

    // At rest the cover photo is ciphertext, not the source bytes.
    let blob_key = EncryptedSyncStorage::blob_key("photos", "p1cover");
    let at_rest = storage
        .cloud_home()
        .read(&blob_key)
        .await
        .expect("blob present in cloud");
    assert_ne!(
        at_rest, plaintext,
        "blob must be encrypted at rest in the cloud"
    );

    // Device B: a fresh DB and its own asset directory, same cloud + key.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let dst_plan = PhotoBlobPlan {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_t, ld) = temp_library_dir();
    let (updated, result) =
        pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &dst_plan).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithPhoto"
    );
    let downloaded =
        std::fs::read(dst_photos.path().join("p1cover")).expect("device B downloaded photo");
    assert_eq!(
        downloaded, plaintext,
        "device B must recover the source bytes after decrypting with the shared key"
    );
}

#[tokio::test]
async fn pull_rejects_unsigned_changeset_when_chain_exists() {
    let storage = MockSyncStorage::new();

    // A membership chain exists. Coven always signs its changesets, so an
    // unsigned one here is forged; a chained library must reject it.
    let founder = UserKeypair::generate();
    let entry = founder_entry(&founder, "2026-03-01T00:00:00Z");
    let entry_bytes = serde_json::to_vec(&entry).expect("serialize founder");
    storage
        .put_membership_entry(&hex::encode(founder.public_key), 1, entry_bytes)
        .await
        .expect("put founder entry");

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db();
    let (updated, result) = pull_into(
        &db2,
        &storage,
        "dev2",
        &HashMap::new(),
        &temp_library_dir().1,
        &NoopBlobPlan,
    )
    .await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // The cursor still advances past the rejected seq so it isn't refetched.
    assert_eq!(updated.get("dev1"), Some(&1));
}

mod schema_version_too_old_display {
    use crate::sync::pull::PullError;

    #[test]
    fn names_app_and_versions_and_recovery() {
        let err = PullError::SchemaVersionTooOld {
            local_version: 3,
            min_version: 5,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("Update the app"),
            "missing recovery verb: {msg}"
        );
        assert!(msg.contains("v5"), "missing required version: {msg}");
        assert!(msg.contains("v3"), "missing current version: {msg}");
    }
}
