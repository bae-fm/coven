//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them, exercising the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use libsqlite3_sys as ffi;

use crate::blob::BlobPlan;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cycle;
use crate::sync::encrypted_storage::EncryptedSyncStorage;
use crate::sync::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipEntry,
};
use crate::sync::pull::{pull_changes, SendDbPtr};
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::service::SyncService;
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

/// A signed founder (first) membership entry for `kp`.
fn founder_entry(kp: &UserKeypair, timestamp: &str) -> MembershipEntry {
    let pk_hex = hex::encode(kp.public_key);
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: pk_hex.clone(),
        role: MemberRole::Owner,
        timestamp: timestamp.to_string(),
        author_pubkey: pk_hex,
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, kp);
    entry
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
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        // Second device pulls.
        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (_tmp, ld) = temp_library_dir();
        let (updated, result) = pull_changes(
            SendDbPtr(db2),
            &storage,
            "dev2",
            &HashMap::new(),
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
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'Future', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION + 1);

        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (updated, result) = pull_changes(
            SendDbPtr(db2),
            &storage,
            "dev2",
            &HashMap::new(),
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
            SendDbPtr(db2),
            &storage,
            "dev2",
            &HashMap::new(),
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

/// Full encrypted blob round-trip through `EncryptedSyncStorage` over a shared
/// `CloudHome`. Device A publishes a note plus its cover photo; the blob lands
/// ciphertext at rest (asserted by reading the raw `CloudHome` bytes directly).
/// Device B — a fresh DB with its own asset directory but the same library
/// key — pulls, downloads the blob, decrypts it, and recovers the original
/// bytes byte-for-byte.
#[tokio::test]
async fn encrypted_blob_round_trips_and_second_device_decrypts() {
    unsafe {
        init_synced_tables();

        // One cloud and one library key, shared by both devices (device B holds
        // the same key a joined device would). The storage owns the cloud; raw
        // reads through `cloud_home()` prove the bytes land ciphertext.
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

        let db1 = open_memory_db();
        create_synced_schema(db1);
        // Start a session, write the changes, then drive sync() — it captures
        // the changeset, uploads blobs, and builds the envelope. sync() does
        // not put_changeset itself; the caller pushes the returned envelope
        // via cycle::push_changeset.
        let session = SyncSession::start(db1).expect("start session");
        exec(
            db1,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            db1,
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('p1cover', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        );

        let service = SyncService::new("dev1".to_string());
        let keypair = UserKeypair::generate();
        let (_t1, ld1) = temp_library_dir();
        let result = service
            .sync(
                db1,
                session,
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
        let outgoing = result.outgoing.expect("outgoing changeset");
        cycle::push_changeset(
            &storage,
            "dev1",
            outgoing.seq,
            outgoing.packed,
            None,
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
        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (_t, ld) = temp_library_dir();
        let (updated, result) = pull_changes(
            SendDbPtr(db2),
            &storage,
            "dev2",
            &HashMap::new(),
            &ld,
            &dst_plan,
        )
        .await
        .expect("pull");

        assert_eq!(result.changesets_applied, 1);
        assert!(!result.asset_downloads_failed);
        assert_eq!(updated.get("dev1"), Some(&1));
        assert_eq!(
            query_text(db2, "SELECT title FROM notes WHERE id = 'n1'"),
            "WithPhoto"
        );
        let downloaded =
            std::fs::read(dst_photos.path().join("p1cover")).expect("device B downloaded photo");
        assert_eq!(
            downloaded, plaintext,
            "device B must recover the source bytes after decrypting with the shared key"
        );

        ffi::sqlite3_close(db1);
        ffi::sqlite3_close(db2);
    }
}

#[tokio::test]
async fn pull_rejects_unsigned_changeset_when_chain_exists() {
    unsafe {
        init_synced_tables();
        let storage = MockSyncStorage::new();

        // A membership chain exists, founded after the changeset's timestamp.
        // `store_changeset` stamps changesets at 2026-02-10, so this unsigned
        // changeset predates the chain -- the case the old grandfathering path
        // admitted. Coven always signs its changesets, so an unsigned one here
        // is forged; a chained library must reject it rather than apply it.
        let founder = UserKeypair::generate();
        let entry = founder_entry(&founder, "2026-03-01T00:00:00Z");
        let entry_bytes = serde_json::to_vec(&entry).expect("serialize founder");
        storage
            .put_membership_entry(&hex::encode(founder.public_key), 1, entry_bytes)
            .await
            .expect("put founder entry");

        let db1 = open_memory_db();
        create_synced_schema(db1);
        let cs = capture_bytes(
            db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'Forged', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            ],
        );
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        let db2 = open_memory_db();
        create_synced_schema(db2);
        let (updated, result) = pull_changes(
            SendDbPtr(db2),
            &storage,
            "dev2",
            &HashMap::new(),
            &temp_library_dir().1,
            &NoopBlobPlan,
        )
        .await
        .expect("pull");

        assert_eq!(result.changesets_applied, 0);
        assert!(!row_exists(db2, "SELECT 1 FROM notes WHERE id = 'n1'"));
        // The cursor still advances past the rejected seq so it isn't refetched.
        assert_eq!(updated.get("dev1"), Some(&1));

        ffi::sqlite3_close(db1);
        ffi::sqlite3_close(db2);
    }
}

mod schema_version_too_old_display {
    use crate::sync::pull::PullError;

    #[test]
    fn names_bae_and_versions_and_recovery() {
        let err = PullError::SchemaVersionTooOld {
            local_version: 3,
            min_version: 5,
        };
        let msg = err.to_string();
        assert!(msg.contains("Update bae"), "missing recovery verb: {msg}");
        assert!(msg.contains("v5"), "missing required version: {msg}");
        assert!(msg.contains("v3"), "missing current version: {msg}");
    }
}
