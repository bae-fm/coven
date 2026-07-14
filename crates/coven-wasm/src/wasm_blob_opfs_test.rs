//! Headless wasm proof that coven's device-local blob storage works on the
//! browser's Origin Private File System.
//!
//! Two layers are covered:
//!
//! 1. [`local_blob`](coven_core::local_blob) directly — write/read/exists/overwrite
//!    through the registered OPFS backend.
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

use rusqlite::OptionalExtension;
use std::path::Path;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::blob::{local_files, BlobRef, BlobScope, CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::session::BlobDecl;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{test_migrations, test_synced_tables_with_blob};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Open a `Database` over a fresh `:memory:` connection with the synthetic synced
/// schema (`notes` / `note_tags` / `note_photos`) and `device_id`. Each call is an
/// independent local database — the two devices share only the cloud.
fn open_device(device_id: &str) -> Database {
    crate::install_platform();
    let (db, _stamper) = Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        )),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        device_id.to_string(),
        &test_migrations(),
    )
    .expect("open in-memory Database");
    db
}

/// Run one full sync cycle for `device_id` over `storage`. coven derives
/// `note_photos`'s blob from the declaration on the synced set. Changeset and
/// blob I/O use the supplied storage, including its fixed at-rest cipher.
async fn run_cycle(storage: CloudSyncStorage, db: &Database, store_dir: &StoreDir) {
    db.set_sync_state("snapshot_seq", "0")
        .await
        .expect("seed snapshot floor for blob wasm test");

    let components = crate::sync::cycle::init_sync_over_storage(db, storage)
        .await
        .expect("initialize sync session");
    components
        .run_cycle(&SystemClock, None, store_dir, None)
        .await
        .expect("sync cycle");
}

/// `local_blob` over OPFS: write creates parent directories, read returns the
/// bytes, exists reflects presence, and an overwrite with shorter content leaves
/// no stale tail. A path nothing ever writes stays absent.
#[wasm_bindgen_test]
async fn local_blob_round_trips_through_opfs() {
    console_error_panic_hook::set_once();
    crate::install_platform();

    let path = Path::new("/coven-blob-test/unit/ab/cd/blob-1");

    // A path that no test writes is absent (a definite `Ok(false)`, not an error),
    // and reading it is an error (not empty bytes) — the upload paths rely on both.
    let absent = Path::new("/coven-blob-test/unit/definitely/absent");
    assert_eq!(
        coven_core::local_blob::exists(absent).await,
        Ok(false),
        "an unwritten path is reported absent, not as an error",
    );
    assert!(
        coven_core::local_blob::read(absent).await.is_err(),
        "reading a missing OPFS file is an error, not empty bytes",
    );

    // Write creates the nested directories and the file.
    let payload = b"the quick brown fox jumps over the lazy dog".to_vec();
    coven_core::local_blob::write_atomic(path, &payload)
        .await
        .expect("write blob to OPFS");
    assert_eq!(
        coven_core::local_blob::exists(path).await,
        Ok(true),
        "a written path exists",
    );
    assert_eq!(
        coven_core::local_blob::read(path).await.expect("read back"),
        payload,
        "OPFS read returns exactly what was written",
    );

    // Overwriting with shorter content must not leave any tail of the old bytes.
    let shorter = b"short".to_vec();
    coven_core::local_blob::write_atomic(path, &shorter)
        .await
        .expect("overwrite blob");
    assert_eq!(
        coven_core::local_blob::read(path)
            .await
            .expect("read after overwrite"),
        shorter,
        "overwrite truncates — no stale tail from the longer previous value",
    );

    // Empty content round-trips too.
    coven_core::local_blob::write_atomic(path, b"")
        .await
        .expect("write empty");
    assert!(
        coven_core::local_blob::read(path)
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
    // Each device's own store dir; B's pull writes the blob into its pinned cache
    // (`storage/pinned/<id>`), which coven owns and builds from the validated id.
    let lib_a = StoreDir::new(std::path::Path::new("/coven-blob-test/lib-a"));
    let lib_b = StoreDir::new(std::path::Path::new("/coven-blob-test/lib-b"));

    let db_a = open_device("device-a");
    let db_b = open_device("device-b");
    let identity = UserKeypair::generate();
    let photo_bytes = b"\x89PNG\r\n\x1a\n fake image bytes for photo-1".to_vec();

    // Device A: a shared note plus a `note_photos` row (the blob-bearing child,
    // which inherits the note's `shared` gate through its foreign key). Written
    // through coven's journaled write path so A's cycle pushes it.
    crate::wasm_test_support::journaled_exec(
        &db_a,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('note-1', 'has a photo', 'body', 1, '0000000001000-0000-device-a', '2026-01-01');\
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo-1', 'note-1', 'thumb', 37, '{}', '0000000001001-0000-device-a', '2026-01-01');",
            crate::blob::content_hash(&photo_bytes),
        ),
    )
    .await
    .expect("device A insert");

    // The host stores the cover (host-provided) in A's local store — its Local home,
    // which the inline push reads to upload. On wasm this exercises the local-store
    // write over OPFS.
    local_files::store(&lib_a, "photos", "photo-1", &photo_bytes)
        .await
        .expect("store photo in A's local store");

    let storage_a = CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([8u8; 32])),
        BlobPathScheme::Hashed,
        "wasm-blob-opfs-test",
        identity.clone(),
    );
    run_cycle(storage_a, &db_a, &lib_a).await;

    // B has not pulled, so the blob is not in B's cache yet.
    let b_cache = lib_b
        .cache_blob_path("photos", "photo-1")
        .expect("cache blob path");
    assert_eq!(
        coven_core::local_blob::exists(&b_cache).await,
        Ok(false),
        "the photo must not be in B's cache before B syncs",
    );

    // Device B: one cycle pulls A's changeset, applies the note + photo rows, and
    // `download_blobs` writes the photo (a CacheEager blob) into B's evictable cache.
    let storage_b = CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key([8u8; 32])),
        BlobPathScheme::Hashed,
        "wasm-blob-opfs-test",
        identity,
    );
    run_cycle(storage_b, &db_b, &lib_b).await;

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

    // ...and so did the blob bytes, written into B's evictable cache by the pull.
    assert_eq!(
        coven_core::local_blob::read(&b_cache)
            .await
            .expect("read photo from B's OPFS"),
        photo_bytes,
        "the photo blob reached B's evictable cache with its original bytes",
    );
}

#[wasm_bindgen_test]
async fn truncated_cached_blob_refetches_instead_of_serving_short_bytes() {
    console_error_panic_hook::set_once();

    let cloud = InMemoryCloudHome::new();
    let lib_b = StoreDir::new(std::path::Path::new("/coven-blob-torn/lib-b"));
    let db_b = open_device("device-b-torn");
    let photo_bytes = b"complete opfs cache payload".to_vec();

    let torn_hash = crate::blob::content_hash(&photo_bytes);
    db_b.call({
        let torn_hash = torn_hash.clone();
        move |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('note-torn', 'has a photo', 'body', 1, '0000000001000-0000-device-b-torn', '2026-01-01');\
                 INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('photo-torn', 'note-torn', 'thumb', 27, '{torn_hash}', '0000000001001-0000-device-b-torn', '2026-01-01');",
            ))
            .map_err(DbError::from)
        }
    })
    .await
    .expect("device B insert");

    let storage_b = CloudSyncStorage::new(
        std::sync::Arc::new(cloud.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Hashed,
        "wasm-blob-torn",
        UserKeypair::generate(),
    );
    let uploader = storage_b
        .own_uploader()
        .expect("hashed home has an uploader");
    let location = crate::sync::test_helpers::test_blob_location(&uploader, 1000);
    storage_b
        .put_blob(
            "photos",
            &location,
            "photo-torn",
            BlobScope::Master,
            None,
            photo_bytes.clone(),
        )
        .await
        .expect("seed cloud blob");

    db_b.record_blob_location("photos", "photo-torn", &location)
        .await
        .expect("record exact cloud location");

    let cache_path = lib_b
        .cache_blob_path("photos", "photo-torn")
        .expect("cache blob path");
    coven_core::local_blob::write_atomic(&cache_path, &photo_bytes[..8])
        .await
        .expect("simulate a torn OPFS cache file");

    let blob = BlobRef {
        namespace: "photos".to_string(),
        id: "photo-torn".to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheEager,
    };
    let read = crate::blob::cache::read_blob(&db_b, &lib_b, Some(&storage_b), &blob)
        .await
        .expect("short OPFS cache file is a miss");
    assert_eq!(read, photo_bytes);
    assert_eq!(
        coven_core::local_blob::read(&cache_path)
            .await
            .expect("cache was rewritten"),
        photo_bytes,
        "the OPFS cache is restored to the full blob",
    );
}

/// The directory primitives the blob cache's eviction / pin / clear paths drive on
/// OPFS: `walk_files` enumerates a sharded tree yielding each leaf file's size,
/// `rename` moves a file across directories (copy-then-delete, since OPFS has no
/// native rename), `remove_file` drops one file, and `remove_dir_all` drops a whole
/// subtree. Absence is reported as `Ok(false)` / an empty list, never an error —
/// the contract `evict_to_budget` and `clear_cache` rely on. Disjoint path prefix
/// from the other tests so a shared OPFS within one run doesn't collide.
#[wasm_bindgen_test]
async fn local_blob_dir_ops_round_trip_through_opfs() {
    console_error_panic_hook::set_once();
    crate::install_platform();

    let root = Path::new("/coven-blob-dirops");
    // Two files under a two-level shard, mirroring the cache's `{ab}/{cd}/<id>`.
    let a = root.join("ab/cd/blob-a");
    let b = root.join("ef/01/blob-b");
    coven_core::local_blob::write_atomic(&a, b"aaaa")
        .await
        .expect("write blob-a");
    coven_core::local_blob::write_atomic(&b, b"bbbbbb")
        .await
        .expect("write blob-b");

    // walk_files descends both shards and returns every leaf file with its size.
    let listed = coven_core::local_blob::walk_files(root)
        .await
        .expect("walk");
    assert_eq!(listed.len(), 2, "walk_files found both leaf files");
    assert!(
        listed.iter().any(|(p, _, size)| p == &a && *size == 4),
        "blob-a enumerated with its 4-byte size",
    );
    assert!(
        listed.iter().any(|(p, _, size)| p == &b && *size == 6),
        "blob-b enumerated with its 6-byte size",
    );

    // rename moves blob-a into a fresh shard: the destination has the bytes (its
    // parent dirs created along the way), the source is gone.
    let a_moved = root.join("99/88/blob-a");
    coven_core::local_blob::rename(&a, &a_moved)
        .await
        .expect("rename blob-a");
    assert_eq!(
        coven_core::local_blob::exists(&a).await,
        Ok(false),
        "the rename source is gone (copy-then-delete completed)",
    );
    assert_eq!(
        coven_core::local_blob::read(&a_moved)
            .await
            .expect("read the moved blob"),
        b"aaaa",
        "the moved file holds the original bytes",
    );

    // remove_file drops a present file (Ok(true)); a second remove is Ok(false), not
    // an error — the already-gone case eviction tolerates.
    assert_eq!(
        coven_core::local_blob::remove_file(&a_moved).await,
        Ok(true),
        "removing a present file reports it was there",
    );
    assert_eq!(
        coven_core::local_blob::remove_file(&a_moved).await,
        Ok(false),
        "removing an already-absent file is Ok(false), not an error",
    );

    // remove_dir_all drops the whole subtree (blob-b and its shards with it); a
    // second sweep over the now-absent root is Ok(false), the empty-cache case.
    assert_eq!(
        coven_core::local_blob::remove_dir_all(root).await,
        Ok(true),
        "removing a present tree reports it was there",
    );
    assert_eq!(
        coven_core::local_blob::exists(&b).await,
        Ok(false),
        "a file under the removed subtree is gone",
    );
    assert_eq!(
        coven_core::local_blob::remove_dir_all(root).await,
        Ok(false),
        "removing an already-absent tree is Ok(false), not an error",
    );

    // walk_files over an absent tree is an empty list, not an error — the
    // nothing-cached-yet case `evict_to_budget` returns early on.
    assert!(
        coven_core::local_blob::walk_files(root)
            .await
            .expect("walk an absent tree")
            .is_empty(),
        "an absent tree enumerates to nothing",
    );
}
