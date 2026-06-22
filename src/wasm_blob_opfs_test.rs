//! Headless wasm proof that coven's device-local blob storage works on the
//! browser's Origin Private File System.
//!
//! Two layers are covered:
//!
//! 1. [`local_blob`](crate::local_blob) directly — write/read/exists/overwrite
//!    against OPFS, the unit that replaces `std::fs` on wasm.
//! 2. The real sync cycle end to end — device A writes a photo blob to OPFS and
//!    pushes a changeset referencing it (`SyncService` reads the file through
//!    `local_blob` and uploads it); device B pulls the changeset and
//!    `download_blobs` writes the photo to *its* OPFS. So a blob crosses two
//!    devices through capture → upload → download → OPFS, all on wasm.
//!
//! OPFS sync access handles exist only on a dedicated Worker, so the whole binary
//! runs there. `wasm-pack test --headless --firefox` gives each run a fresh
//! browser profile (so OPFS starts empty); within one run, tests use disjoint
//! path prefixes so they don't collide.

use std::path::Path;
use std::sync::RwLock;

use rusqlite::OptionalExtension;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::clock::SystemClock;
use crate::database::{Database, DbError};
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::test_helpers::{create_synced_schema, test_synced_tables, PhotoBlobSource};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Open a `Database` over a fresh `:memory:` connection with the synthetic synced
/// schema (`notes` / `note_tags` / `note_photos`) and `device_id`. Each call is an
/// independent local database — the two devices share only the cloud.
fn open_device(device_id: &str) -> Database {
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        device_id.to_string(),
        create_synced_schema,
    )
    .expect("open in-memory Database");
    db
}

/// Run one full sync cycle for `device_id` over `storage`, with a `PhotoBlobSource`
/// rooted at `blob_dir` so `note_photos` rows map to blobs there. Plaintext at
/// rest, no live cloud home (changeset + blob I/O go through `storage`).
async fn run_cycle(
    storage: &CloudSyncStorage,
    db: &Database,
    device_id: &str,
    library_dir: &LibraryDir,
    blob_dir: &Path,
) {
    let cipher = RwLock::new(CloudCipher::Plaintext);
    let keypair = UserKeypair::generate();
    let hlc = Hlc::new(device_id.to_string());
    let blob_plan = PhotoBlobSource {
        dir: blob_dir.to_path_buf(),
    };

    run_single_sync_cycle(
        storage,
        "test-lib",
        device_id,
        &hlc,
        &SystemClock,
        db,
        &cipher,
        &keypair,
        library_dir,
        None,
        &blob_plan,
        None,
    )
    .await
    .expect("sync cycle");
}

/// `local_blob` over OPFS: write creates parent directories, read returns the
/// bytes, exists reflects presence, and an overwrite with shorter content leaves
/// no stale tail. A path nothing ever writes stays absent.
#[wasm_bindgen_test]
async fn local_blob_round_trips_through_opfs() {
    console_error_panic_hook::set_once();

    let path = Path::new("/coven-blob-test/unit/ab/cd/blob-1");

    // A path that no test writes is absent (a definite `Ok(false)`, not an error),
    // and reading it is an error (not empty bytes) — the upload paths rely on both.
    let absent = Path::new("/coven-blob-test/unit/definitely/absent");
    assert_eq!(
        crate::local_blob::exists(absent).await,
        Ok(false),
        "an unwritten path is reported absent, not as an error",
    );
    assert!(
        crate::local_blob::read(absent).await.is_err(),
        "reading a missing OPFS file is an error, not empty bytes",
    );

    // Write creates the nested directories and the file.
    let payload = b"the quick brown fox jumps over the lazy dog".to_vec();
    crate::local_blob::write(path, &payload)
        .await
        .expect("write blob to OPFS");
    assert_eq!(
        crate::local_blob::exists(path).await,
        Ok(true),
        "a written path exists",
    );
    assert_eq!(
        crate::local_blob::read(path).await.expect("read back"),
        payload,
        "OPFS read returns exactly what was written",
    );

    // Overwriting with shorter content must not leave any tail of the old bytes.
    let shorter = b"short".to_vec();
    crate::local_blob::write(path, &shorter)
        .await
        .expect("overwrite blob");
    assert_eq!(
        crate::local_blob::read(path)
            .await
            .expect("read after overwrite"),
        shorter,
        "overwrite truncates — no stale tail from the longer previous value",
    );

    // Empty content round-trips too.
    crate::local_blob::write(path, b"")
        .await
        .expect("write empty");
    assert!(
        crate::local_blob::read(path)
            .await
            .expect("read empty")
            .is_empty(),
        "an empty blob reads back empty",
    );
}

/// A photo blob crosses two devices through the real cycle on wasm: A writes it to
/// OPFS and pushes a changeset that references it; B pulls and the blob lands in
/// B's OPFS with the original bytes.
#[wasm_bindgen_test]
async fn photo_blob_syncs_across_devices_through_opfs() {
    console_error_panic_hook::set_once();

    let cloud = InMemoryCloudHome::new();
    // Each device's upload-source dir (where A's photo plaintext lives, the push's
    // source) and its own library dir (where B's pull writes the blob into the
    // pinned cache, `storage/pinned/<id>` — coven-owned, distinct from the source).
    let dir_a = Path::new("/coven-blob-test/device-a");
    let dir_b = Path::new("/coven-blob-test/device-b");
    let lib_a = LibraryDir::new(std::path::Path::new("/coven-blob-test/lib-a"));
    let lib_b = LibraryDir::new(std::path::Path::new("/coven-blob-test/lib-b"));

    let db_a = open_device("device-a");
    let db_b = open_device("device-b");

    // Device A: a shared note plus a `note_photos` row (the blob-bearing child,
    // which inherits the note's `shared` gate through its foreign key).
    db_a.call(|conn| {
        conn.execute_batch(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('note-1', 'has a photo', 'body', 1, '0000000001000-0000-device-a', '2026-01-01');\
             INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('photo-1', 'note-1', 'thumb', '0000000001001-0000-device-a', '2026-01-01');",
        )
        .map_err(DbError::from)
    })
    .await
    .expect("device A insert");

    // The photo's plaintext lives on A's OPFS at the path `PhotoBlobSource` derives
    // (`dir_a/photo-1`). The push reads it through `local_blob` and uploads it.
    let photo_bytes = b"\x89PNG\r\n\x1a\n fake image bytes for photo-1".to_vec();
    crate::local_blob::write(&dir_a.join("photo-1"), &photo_bytes)
        .await
        .expect("write photo to A's OPFS");

    let storage_a = CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Hashed,
        UserKeypair::generate(),
    );
    run_cycle(&storage_a, &db_a, "device-a", &lib_a, dir_a).await;

    // B has not pulled, so the blob is not in B's pinned cache yet.
    let b_pinned = lib_b.pinned_blob_path("photo-1").expect("pinned blob path");
    assert_eq!(
        crate::local_blob::exists(&b_pinned).await,
        Ok(false),
        "the photo must not be in B's cache before B syncs",
    );

    // Device B: one cycle pulls A's changeset, applies the note + photo rows, and
    // `download_blobs` writes the photo (a Mirrored blob) into B's pinned cache.
    let storage_b = CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Hashed,
        UserKeypair::generate(),
    );
    run_cycle(&storage_b, &db_b, "device-b", &lib_b, dir_b).await;

    // The row crossed...
    let has_photo_row = db_b
        .call(|conn| {
            conn.query_row("SELECT 1 FROM note_photos WHERE id = 'photo-1'", [], |_| {
                Ok(())
            })
            .optional()
            .map(|o| o.is_some())
            .map_err(DbError::from)
        })
        .await
        .expect("query note_photos on B");
    assert!(has_photo_row, "device B applied the note_photos row");

    // ...and so did the blob bytes, written into B's pinned cache by the pull.
    assert_eq!(
        crate::local_blob::read(&b_pinned)
            .await
            .expect("read photo from B's OPFS"),
        photo_bytes,
        "the photo blob reached B's pinned cache with its original bytes",
    );
}
