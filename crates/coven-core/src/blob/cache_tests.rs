//! Tests for the device-local blob cache.
//!
//! These drive the real cache free functions over a real [`Database`] and a real
//! temp store directory, with a [`MockSyncStorage`] standing in for the cloud, so
//! a hit/miss and a folder move are exercised against actual files on disk. The
//! load-bearing properties: presence is the file (no table), pinned-ness is which
//! folder (`storage/pinned/` vs `storage/cache/`), and a read serves a local copy
//! without a cloud round-trip.

use std::collections::HashMap;

use super::cache::{
    clear_cache, evict_to_budget, fetch_from_cloud, open_blob_stream, pin, read_blob, unpin,
    write_blob, BlobCacheError,
};
use crate::blob::{BlobRef, BlobScope, CacheFill, Provenance};
use crate::database::Database;
use crate::sync::session::BlobDecl;
use crate::sync::session::SyncedTable;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    capture_bytes, open_test_db, open_test_db_schema, open_test_db_with_blob,
    open_test_db_with_user_and_host_blobs, plant_blob_row, pull_into, read_test_db,
    set_blob_remote, temp_store_dir, test_migrations, MockSyncStorage,
};

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

/// A `BlobRef` keyed by `id` in `namespace`, master-scoped, no `cloud_path`, of
/// cache `fill` and `Provenance::UserProvided`. Blob ids are ≥4 chars so they form
/// the `{ab}/{cd}` partition shard. Provenance is fixed because these tests read
/// from the cache/cloud, where provenance doesn't change the path; the host-provided
/// local-store read has its own helper ([`host_blob_ref`]).
fn blob_ref(id: &str, namespace: &str, fill: CacheFill) -> BlobRef {
    BlobRef {
        namespace: namespace.to_string(),
        id: id.to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::UserProvided,
        fill,
    }
}

/// A host-provided `BlobRef` (its Local home is coven's local store, not a user
/// path), for the read-resolution test that exercises the local-store step.
fn host_blob_ref(id: &str, namespace: &str, fill: CacheFill) -> BlobRef {
    BlobRef {
        namespace: namespace.to_string(),
        id: id.to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::HostProvided,
        fill,
    }
}

/// The `note_photos` declaration for the cache tests: namespace `"photos"`, master
/// scope, host-provided · `CacheEager` (fetched into the cache on pull).
fn photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
}

fn remote_root_db(decl: BlobDecl) -> Database {
    open_test_db_schema(
        vec![
            SyncedTable::new("notes").remote_root(),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos").carries_blob(decl),
        ],
        test_migrations(),
    )
}

fn plain_blob_db(decl: BlobDecl) -> Database {
    open_test_db_schema(
        vec![
            SyncedTable::new("notes"),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos").carries_blob(decl),
        ],
        test_migrations(),
    )
}

/// Put `bytes` into the mock cloud under the flat `{namespace}/{id}` key the mock's
/// `get_blob` reads back (master scope, no cloud_path), so a cache miss can fetch it.
async fn put_cloud_blob(storage: &MockSyncStorage, id: &str, namespace: &str, bytes: &[u8]) {
    storage
        .put_blob(namespace, id, BlobScope::Master, None, bytes.to_vec())
        .await
        .expect("put blob in mock cloud");
}

/// A second read is a local hit: the first read populates `cache/<id>` from the
/// cloud, and after the cloud copy is deleted the second read still returns the
/// bytes — served from disk, no fetch.
#[tokio::test]
async fn second_read_is_a_local_hit() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-aaaa", "audio", CacheFill::CacheLazy);
    let bytes = b"THE-BLOB-BYTES".to_vec();
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    // First read misses, fetches from the cloud, and populates the evictable cache.
    let first = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("first read fetches from cloud");
    assert_eq!(first, bytes);
    assert!(
        ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "the first read populates storage/cache/<id>",
    );

    // Delete the cloud copy so a second fetch would fail: the read must be served
    // from the local file, proving the cache hit, not a re-download.
    storage.delete_blob_object("audio", &blob.id).await;
    let second = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("second read is served from the local cache");
    assert_eq!(
        second, bytes,
        "the second read returns the cached bytes without touching the cloud",
    );
}

#[tokio::test]
async fn short_cached_remote_blob_is_a_miss_and_refetches() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-torn", "audio", CacheFill::CacheLazy);
    let bytes = b"complete remote blob bytes".to_vec();
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("first read populates cache");
    let cache_path = ld.cache_blob_path(&blob.namespace, &blob.id).unwrap();
    crate::local_blob::write_atomic(&cache_path, &bytes[..8])
        .await
        .expect("simulate a torn cache file");

    let refetched = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("short cache file refetches from cloud");
    assert_eq!(refetched, bytes);
    assert_eq!(
        crate::local_blob::read(&cache_path)
            .await
            .expect("cache was rewritten"),
        bytes,
        "the cache contains the complete bytes after refetch",
    );
}

/// A cloud object whose plaintext length disagrees with the row's declared size is
/// rejected on read: the whole-blob Remote fetch verifies `bytes.len()` against the
/// declared size before it caches or returns anything. A mismatch is a typed error
/// and writes nothing to `cache/`, so a wrong-length cloud object cannot poison the
/// cache into a warn-refetch-repoison loop — every read fails loud instead.
#[tokio::test]
async fn short_cloud_object_errors_and_caches_nothing() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-shrt", "audio", CacheFill::CacheLazy);
    let declared = b"the full declared blob bytes".to_vec();
    let short = declared[..8].to_vec();
    plant_blob_row(&db, &blob.id, true, declared.len() as u64).await;
    // The cloud object is shorter than the row's declared size.
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &short).await;

    let err = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect_err("a cloud object shorter than the declared size must error");
    assert!(
        matches!(
            err,
            BlobCacheError::CloudSizeMismatch { expected, actual, .. }
                if expected == declared.len() as u64 && actual == short.len() as u64
        ),
        "a short cloud object maps to CloudSizeMismatch naming expected/actual: {err:?}",
    );
    assert!(
        !ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "a length mismatch writes nothing to storage/cache/<id>",
    );

    // A subsequent read keeps erroring loudly — no cached wrong-length file to serve,
    // no refetch loop that returns the short bytes.
    let again = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect_err("subsequent reads keep erroring, never serving the short bytes");
    assert!(
        matches!(again, BlobCacheError::CloudSizeMismatch { .. }),
        "the second read errors the same way, not a wrong-length hit: {again:?}",
    );
}

#[tokio::test]
async fn blob_reads_reuse_schema_models_built_at_open() {
    crate::sync::gate::reset_from_tables_call_count();
    crate::blob::decl::reset_from_tables_call_count();

    let db = read_test_db("audio");
    assert_eq!(
        crate::sync::gate::from_tables_call_count(),
        1,
        "database open builds the gate model once",
    );
    assert_eq!(
        crate::blob::decl::from_tables_call_count(),
        1,
        "database open builds the blob declaration model once",
    );

    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();
    let blob = blob_ref("blob-reuse", "audio", CacheFill::CacheLazy);
    let bytes = ramp(4096);

    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    assert_eq!(
        read_blob(&db, &ld, Some(&storage), &blob)
            .await
            .expect("first read fetches from cloud"),
        bytes,
    );
    assert_eq!(
        read_blob(&db, &ld, Some(&storage), &blob)
            .await
            .expect("second read serves from cache"),
        bytes,
    );
    let offset = 1024;
    let len = 512;
    assert_eq!(
        open_blob_stream(
            &db,
            &ld,
            Some(&storage),
            &blob,
            bytes.len() as u64,
            offset,
            len,
        )
        .await
        .expect("ranged read serves through the cache path"),
        bytes[offset as usize..(offset + len) as usize],
    );

    assert_eq!(
        crate::sync::gate::from_tables_call_count(),
        1,
        "blob reads reuse the database's gate model",
    );
    assert_eq!(
        crate::blob::decl::from_tables_call_count(),
        1,
        "blob reads reuse the database's blob declaration model",
    );
}

/// A CacheEager blob pulled in a changeset lands in the EVICTABLE CACHE: its file is
/// in `storage/cache/<id>`, not the kept `storage/pinned/<id>`. On a peer the release
/// is Remote, so the blob's local copy is a cache copy (evictable + re-fetchable, not
/// pinned). (Driven through the real pull, which routes CacheEager blobs to
/// `download_blobs` → `cache/`.)
#[tokio::test]
async fn cache_eager_lands_in_cache_on_pull() {
    let storage = MockSyncStorage::new();

    // Source dev1 records a note + a (non-cover, so master-scoped) photo row.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('ph01abcd', 'n1', 'attach', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    put_cloud_blob(&storage, "ph01abcd", "photos", b"COVERBYTES").await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // The puller declares the photo a CacheEager blob; the pull writes it into the
    // store dir's evictable cache tree.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert!(
        ld.cache_blob_path("photos", "ph01abcd").unwrap().exists(),
        "a CacheEager blob lands in the evictable cache on pull: storage/cache/<namespace>/<id>",
    );
    assert!(
        !ld.pinned_blob_path("photos", "ph01abcd").unwrap().exists(),
        "a CacheEager blob does NOT land system-pinned in storage/pinned/<namespace>/<id>",
    );
}

/// Pin promotes a cached blob to `pinned/` and that survives a cache sweep; unpin
/// demotes it back to `cache/` where a sweep then drops it; and unpinning a
/// never-pinned CacheEager blob is a no-op (no system pin to reject anymore — a
/// CacheEager blob lands evictable in the cache on pull).
#[tokio::test]
async fn pin_survives_clear_cache_and_unpin_demotes() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("ond-aaaa", "audio", CacheFill::CacheLazy);
    let bytes = b"ON-DEMAND-AUDIO".to_vec();
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    // Read it on demand → it lands in the evictable cache.
    read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("read populates the cache");
    assert!(ld
        .cache_blob_path(&blob.namespace, &blob.id)
        .unwrap()
        .exists());
    assert!(!ld
        .pinned_blob_path(&blob.namespace, &blob.id)
        .unwrap()
        .exists());

    // Pin it → moves cache/ → pinned/.
    pin(&db, &ld, Some(&storage), std::slice::from_ref(&blob))
        .await
        .expect("pin promotes the cached blob");
    assert!(
        ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "pin moves the blob into storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "pin leaves nothing behind in storage/cache/<id>",
    );

    // Clear the cache → the pinned blob is untouched.
    clear_cache(&ld).await.expect("clear cache");
    assert!(
        ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "a pinned blob survives a cache sweep",
    );

    // Unpin it → moves pinned/ → cache/ (the file stays, now evictable).
    unpin(&ld, std::slice::from_ref(&blob))
        .await
        .expect("unpin demotes the blob");
    assert!(
        ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "unpin moves the blob back into storage/cache/<id>",
    );
    assert!(
        !ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "unpin leaves nothing behind in storage/pinned/<id>",
    );

    // Clear the cache again → the now-unpinned blob is gone.
    clear_cache(&ld).await.expect("clear cache");
    assert!(
        !ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "an unpinned blob is dropped by a cache sweep",
    );

    // Unpinning a never-pinned CacheEager blob is a harmless no-op: it lands evictable
    // in the cache on pull (not system-pinned), so there is nothing in `pinned/` to
    // demote.
    let eager = blob_ref("mir-aaaa", "images", CacheFill::CacheEager);
    unpin(&ld, std::slice::from_ref(&eager))
        .await
        .expect("unpinning a never-pinned CacheEager blob is a no-op");
    assert!(
        !ld.pinned_blob_path(&eager.namespace, &eager.id)
            .unwrap()
            .exists()
            && !ld
                .cache_blob_path(&eager.namespace, &eager.id)
                .unwrap()
                .exists(),
        "the never-pinned blob is in neither folder",
    );
}

#[tokio::test]
async fn pin_downloads_remote_blob_straight_to_pinned_file() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("pin0aaaa", "audio", CacheFill::CacheLazy);
    let bytes: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    pin(&db, &ld, Some(&storage), std::slice::from_ref(&blob))
        .await
        .expect("pin downloads remote blob");

    assert_eq!(
        storage.blob_read_to_file_count(),
        1,
        "pin writes a cache miss through the file download path",
    );
    assert_eq!(
        std::fs::read(ld.pinned_blob_path(&blob.namespace, &blob.id).unwrap()).unwrap(),
        bytes,
        "pin writes the remote bytes into the pinned file",
    );
}

/// A CacheLazy blob is NOT downloaded on pull (no file in either folder afterward),
/// and a later read fetches it into the cache on first access.
#[tokio::test]
async fn cache_lazy_fetches_on_first_read() {
    let storage = MockSyncStorage::new();

    // Source dev1: a note + a photo row the puller's source treats as CacheLazy.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            // shared = 1: the note is Remote, so its blob resolves Remote (cache/cloud)
            // on the peer that pulls it.
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            // size = 13 matches the `AUDIO-PAYLOAD` bytes put in the cloud below; the
            // whole-blob read verifies the fetched length against this declared size.
            "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
             VALUES ('aud01234', 'n1', 'audio', 13, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    // The row applied, but the CacheLazy blob is in neither folder — pull skipped it.
    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert!(
        !ld.pinned_blob_path("audio", "aud01234").unwrap().exists()
            && !ld.cache_blob_path("audio", "aud01234").unwrap().exists(),
        "a CacheLazy blob is not fetched on pull — neither folder holds it",
    );

    // Now put it in the cloud and read it: the first read fetches into the cache.
    let bytes = b"AUDIO-PAYLOAD".to_vec();
    put_cloud_blob(&storage, "aud01234", "audio", &bytes).await;
    let blob = blob_ref("aud01234", "audio", CacheFill::CacheLazy);
    let got = read_blob(&db2, &ld, Some(&storage), &blob)
        .await
        .expect("first read fetches the CacheLazy blob");
    assert_eq!(got, bytes);
    assert!(
        ld.cache_blob_path("audio", "aud01234").unwrap().exists(),
        "the on-demand fetch populates storage/cache/<namespace>/<id>",
    );
}

/// `write_blob` writes host bytes straight into the cache (`cache/<id>`), and a
/// later `pin` promotes them by renaming — with NO cloud fetch. The cloud copy is
/// deleted first, so a pin that tried to fetch would fail: it must not.
#[tokio::test]
async fn write_blob_writes_to_cache_and_pin_needs_no_cloud_fetch() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("stg-aaaa", "audio", CacheFill::CacheLazy);
    let bytes = b"CACHED-BYTES".to_vec();
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;

    // Write the bytes into the cache.
    write_blob(&db, &ld, &blob.namespace, &blob.id, &bytes)
        .await
        .expect("write_blob writes into the cache");
    assert!(
        ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "write_blob writes to storage/cache/<id>",
    );
    assert!(
        !ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "write_blob does not pin — nothing in storage/pinned/<id>",
    );

    // The blob is NOT in the cloud (nothing was ever put there). A pin that fetched
    // would fail; instead it must promote the staged file by renaming it.
    pin(&db, &ld, Some(&storage), std::slice::from_ref(&blob))
        .await
        .expect("pin promotes the staged file without a cloud fetch");
    assert!(
        ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "pin renames the staged blob into storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "pin moves the staged blob out of storage/cache/<id>",
    );
    // Read it back to confirm the bytes survived the staging + rename intact.
    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("the pinned staged blob reads back");
    assert_eq!(got, bytes, "the staged bytes survive write_blob → pin");
}

/// A multi-byte blob payload `0,1,2,…` mod 251, long enough to slice a mid-file
/// window out of. The byte at index `i` is `(i % 251) as u8`, so a returned slice
/// is trivially checkable against `&full[offset..offset+len]`.
fn ramp(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Write `bytes` to an external user-owned file under `base/external/<name>` and
/// return its absolute path — a file outside coven's `storage/` cache folders,
/// the source a `local_blob_refs` row points at.
fn write_external_file(base: &std::path::Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let dir = base.join("external");
    std::fs::create_dir_all(&dir).expect("create external dir");
    let path = dir.join(name);
    std::fs::write(&path, bytes).expect("write external file");
    path
}

/// A ranged read of a CACHED blob is served from the local plaintext file: after a
/// whole-file read populates `cache/<id>`, the cloud copy is deleted so any cloud
/// fallback would fail, and ranged reads (a mid-file window and an `offset > 0`
/// tail) still return the correct slices — proving they came from disk, not a
/// re-fetch.
#[tokio::test]
async fn ranged_read_of_a_cached_blob_serves_from_the_local_file() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-aaaa", "audio", CacheFill::CacheLazy);
    let full = ramp(5000);
    plant_blob_row(&db, &blob.id, true, full.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &full).await;

    // Populate the cache with the whole file, then remove the cloud copy so a
    // ranged read that tried to fetch would fail.
    read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("whole-file read populates the cache");
    assert!(ld
        .cache_blob_path(&blob.namespace, &blob.id)
        .unwrap()
        .exists());
    storage.delete_blob_object("audio", &blob.id).await;

    // A window from the middle of the file.
    let (offset, len) = (1234u64, 1000u64);
    let mid = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        full.len() as u64,
        offset,
        len,
    )
    .await
    .expect("mid-file ranged read served from the local file");
    assert_eq!(
        mid,
        &full[offset as usize..(offset + len) as usize],
        "the mid-file window matches the plaintext slice",
    );

    // A tail starting at offset > 0 running to the end of the file.
    let tail_off = 4000u64;
    let tail_len = full.len() as u64 - tail_off;
    let tail = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        full.len() as u64,
        tail_off,
        tail_len,
    )
    .await
    .expect("tail ranged read served from the local file");
    assert_eq!(
        tail,
        &full[tail_off as usize..],
        "the tail window matches the plaintext slice",
    );
}

/// A ranged read of a NON-cached blob fetches and decrypts just the requested
/// range from the cloud AND leaves the cache empty: neither `pinned/<id>` nor
/// `cache/<id>` exists afterward. A ranged read must never write a truncated cache
/// file — only the whole-file `read_blob` populates.
#[tokio::test]
async fn ranged_read_of_a_non_cached_blob_fetches_range_and_writes_no_cache_file() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-bbbb", "audio", CacheFill::CacheLazy);
    let full = ramp(5000);
    plant_blob_row(&db, &blob.id, true, full.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &full).await;

    // The blob is in neither folder before the read.
    assert!(!ld
        .pinned_blob_path(&blob.namespace, &blob.id)
        .unwrap()
        .exists());
    assert!(!ld
        .cache_blob_path(&blob.namespace, &blob.id)
        .unwrap()
        .exists());

    let (offset, len) = (2000u64, 1500u64);
    let got = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        full.len() as u64,
        offset,
        len,
    )
    .await
    .expect("ranged read fetches the range from the cloud");
    assert_eq!(
        got,
        &full[offset as usize..(offset + len) as usize],
        "the fetched range matches the plaintext slice",
    );

    // The defining guarantee: a ranged miss populated NOTHING. A later whole-file
    // read would have to fetch fresh, and `read_blob`'s presence check is never
    // fooled by a truncated file.
    assert!(
        !ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "a ranged miss must not write storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "a ranged miss must not write storage/cache/<id> (no truncated cache file)",
    );
}

/// A full `read_blob` still populates the evictable cache (Phase 2 behavior intact,
/// unchanged by adding the ranged path): after one whole-file read, `cache/<id>`
/// exists and a second read is served from it even with the cloud copy gone.
#[tokio::test]
async fn full_read_blob_still_populates_the_cache() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-cccc", "audio", CacheFill::CacheLazy);
    let bytes = b"WHOLE-FILE-PAYLOAD".to_vec();
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    let first = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("first whole-file read fetches from the cloud");
    assert_eq!(first, bytes);
    assert!(
        ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "a whole-file read populates storage/cache/<id>",
    );

    // Cloud copy gone → the second whole-file read must be a local hit.
    storage.delete_blob_object("audio", &blob.id).await;
    let second = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("second whole-file read is served from the cache");
    assert_eq!(second, bytes);
}

/// The ranged contract is pinned and identical on both serving paths: an
/// `offset + len` past the blob's plaintext size is an error (never a short read),
/// and a zero-length read is an empty result (never an error) — checked for a
/// cached blob (served from disk) and a non-cached one (served from the cloud).
#[tokio::test]
async fn ranged_read_out_of_range_errors_and_zero_len_is_empty_on_both_paths() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let full = ramp(1000);

    // Non-cached blob: contract enforced before/within the cloud path.
    let remote = blob_ref("blob-dddd", "audio", CacheFill::CacheLazy);
    plant_blob_row(&db, &remote.id, true, full.len() as u64).await;
    put_cloud_blob(&storage, &remote.id, &remote.namespace, &full).await;
    assert!(
        open_blob_stream(
            &db,
            &ld,
            Some(&storage),
            &remote,
            full.len() as u64,
            900,
            200
        )
        .await
        .is_err(),
        "a range past the blob size must error on the cloud path",
    );
    assert!(
        open_blob_stream(&db, &ld, Some(&storage), &remote, full.len() as u64, 500, 0)
            .await
            .expect("zero-length read is not an error")
            .is_empty(),
        "a zero-length read is an empty result on the cloud path",
    );

    // Cached blob: same contract on the local-file path. Populate the cache, drop
    // the cloud copy so only the local path can serve.
    let cached = blob_ref("blob-eeee", "audio", CacheFill::CacheLazy);
    plant_blob_row(&db, &cached.id, true, full.len() as u64).await;
    put_cloud_blob(&storage, &cached.id, &cached.namespace, &full).await;
    read_blob(&db, &ld, Some(&storage), &cached)
        .await
        .expect("populate the cache");
    storage.delete_blob_object("audio", &cached.id).await;
    assert!(
        open_blob_stream(
            &db,
            &ld,
            Some(&storage),
            &cached,
            full.len() as u64,
            900,
            200
        )
        .await
        .is_err(),
        "a range past the blob size must error on the local-file path too",
    );
    assert!(
        open_blob_stream(&db, &ld, Some(&storage), &cached, full.len() as u64, 500, 0)
            .await
            .expect("zero-length read is not an error")
            .is_empty(),
        "a zero-length read is an empty result on the local-file path too",
    );
}

// ---- External refs (local_blob_refs, locality-aware read) ----

/// A Local + user-provided blob serves the user's own file: `read_blob` returns the
/// whole file and `open_blob_stream` returns a correct slice of it, both with nothing
/// in the cloud — a Local user-provided blob's bytes only ever live at the user's
/// path. An external read also populates neither cache folder (it owns no cache copy).
#[tokio::test]
async fn external_ref_read_serves_the_user_file_without_the_cloud() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (tmp, ld) = temp_store_dir();

    let blob = blob_ref("extr-aaaa", "audio", CacheFill::CacheLazy);
    // Local + user-provided: the gate dispatches to the external file.
    let full = ramp(5000);
    plant_blob_row(&db, &blob.id, false, full.len() as u64).await;
    let path = write_external_file(tmp.path(), "song.flac", &full);

    db.register_external_blob(&blob.id, &blob.namespace, &path, full.len() as u64)
        .await
        .expect("register external ref");

    let whole = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("read serves the external file (no cloud copy exists)");
    assert_eq!(
        whole, full,
        "the whole read returns the external file's bytes"
    );

    let (offset, len) = (1234u64, 1000u64);
    let mid = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        full.len() as u64,
        offset,
        len,
    )
    .await
    .expect("ranged read off the external file");
    assert_eq!(
        mid,
        &full[offset as usize..(offset + len) as usize],
        "the ranged read returns the correct slice of the external file",
    );

    assert!(
        !ld.pinned_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists()
            && !ld
                .cache_blob_path(&blob.namespace, &blob.id)
                .unwrap()
                .exists(),
        "an external read populates neither cache folder",
    );
}

/// A missing external file is [`BlobCacheError::ExternalMissing`] and a present
/// file whose length differs from the registered size is
/// [`BlobCacheError::ExternalSizeMismatch`] — both terminal. A cloud copy exists
/// under the same id in each case, so a store-probing read would serve those bytes:
/// the gate-first dispatch must not, proving the Local + user-provided arm never
/// reaches the cloud.
#[tokio::test]
async fn external_missing_and_size_mismatch_error_with_no_cloud_fallback() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (tmp, ld) = temp_store_dir();

    let cloud_bytes = b"CLOUD-FALLBACK-BYTES".to_vec();

    // Missing file: a ref pointing at a path that does not exist.
    let missing = blob_ref("extm-aaaa", "audio", CacheFill::CacheLazy);
    plant_blob_row(&db, &missing.id, false, 1234).await;
    put_cloud_blob(&storage, &missing.id, &missing.namespace, &cloud_bytes).await;
    let missing_path = tmp.path().join("external").join("gone.flac");
    db.register_external_blob(&missing.id, &missing.namespace, &missing_path, 1234)
        .await
        .expect("register missing external ref");
    let err = read_blob(&db, &ld, Some(&storage), &missing)
        .await
        .expect_err("a missing external file is terminal, never a cloud fetch");
    assert!(
        matches!(err, BlobCacheError::ExternalMissing { .. }),
        "a missing external file maps to ExternalMissing: {err:?}",
    );

    // Present file, wrong length: register a size one byte off the real file.
    let mism = blob_ref("exts-aaaa", "audio", CacheFill::CacheLazy);
    put_cloud_blob(&storage, &mism.id, &mism.namespace, &cloud_bytes).await;
    let actual = ramp(2000);
    plant_blob_row(&db, &mism.id, false, actual.len() as u64 + 1).await;
    let mism_path = write_external_file(tmp.path(), "wrong-size.flac", &actual);
    db.register_external_blob(
        &mism.id,
        &mism.namespace,
        &mism_path,
        actual.len() as u64 + 1,
    )
    .await
    .expect("register size-mismatched external ref");
    let err = read_blob(&db, &ld, Some(&storage), &mism)
        .await
        .expect_err("a size-mismatched external file is terminal, never a cloud fetch");
    assert!(
        matches!(err, BlobCacheError::ExternalSizeMismatch { .. }),
        "a length != registered size maps to ExternalSizeMismatch: {err:?}",
    );
}

/// The gate, not the external ref, decides whether a read serves the user's file or
/// the cloud. While the blob is Local + user-provided (ref registered) the read serves
/// the external file; after a make_remote's end state — the gate flips Remote AND the
/// external ref is cleared, together — the read resolves Remote and serves the cloud,
/// populating the cache.
#[tokio::test]
async fn gate_flip_to_remote_routes_the_read_from_the_external_file_to_the_cloud() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (tmp, ld) = temp_store_dir();

    let blob = blob_ref("extc-aaaa", "audio", CacheFill::CacheLazy);
    // Local + user-provided: serves the user's external file.
    let ext_bytes = ramp(1500);
    plant_blob_row(&db, &blob.id, false, ext_bytes.len() as u64).await;
    let path = write_external_file(tmp.path(), "owned.flac", &ext_bytes);
    db.register_external_blob(&blob.id, &blob.namespace, &path, ext_bytes.len() as u64)
        .await
        .expect("register external ref");

    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("Local + user-provided read serves the external file");
    assert_eq!(
        got, ext_bytes,
        "the registered ref serves the external file"
    );

    // make_remote's end state: the gate flips Remote AND the external ref is cleared,
    // together. After it, the blob resolves Remote and reads from the cloud — the
    // (now-cleared) ref is not consulted for a Remote blob.
    set_blob_remote(&db, &blob.id, true).await;
    db.clear_external_blob(&blob.id)
        .await
        .expect("clear external ref");
    // The cloud object's length must match the row's declared size (1500) — the
    // whole-blob read verifies it before serving.
    let cloud_bytes = ramp(1500);
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &cloud_bytes).await;
    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("after going Remote, the read fetches from the cloud");
    assert_eq!(
        got, cloud_bytes,
        "once Remote the blob resolves through cache/cloud",
    );
    assert!(
        ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "the cloud fetch populated the evictable cache",
    );
}

/// A **Local + user-provided** blob reads its external file — and only that. Gate-first
/// dispatch picks the external source from the blob's provenance, so a same-id cache
/// file and a stray same-id local-store file are both ignored (whole and ranged).
#[tokio::test]
async fn local_user_provided_blob_reads_its_external_file_ignoring_decoys() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (tmp, ld) = temp_store_dir();

    // Local + user-provided.
    let blob = blob_ref("extp-aaaa", "audio", CacheFill::CacheLazy);
    let ext_bytes = ramp(2048);
    plant_blob_row(&db, &blob.id, false, ext_bytes.len() as u64).await;

    // Decoys at the other on-device stores — both must be ignored.
    write_blob(&db, &ld, &blob.namespace, &blob.id, b"OWNED-CACHE-BYTES")
        .await
        .expect("write a same-id cache decoy");
    crate::blob::local_files::store(&ld, &blob.namespace, &blob.id, b"STALE-LOCAL-STORE")
        .await
        .expect("write a same-id local-store decoy");

    // The real bytes: the user's external file.
    let path = write_external_file(tmp.path(), "precedence.flac", &ext_bytes);
    db.register_external_blob(&blob.id, &blob.namespace, &path, ext_bytes.len() as u64)
        .await
        .expect("register external ref");

    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("Local + user-provided read");
    assert_eq!(
        got, ext_bytes,
        "the read serves the external file, not the cache or local-store decoys",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        ext_bytes.len() as u64,
        offset,
        len,
    )
    .await
    .expect("ranged Local + user-provided read");
    assert_eq!(
        mid,
        &ext_bytes[offset as usize..(offset + len) as usize],
        "the ranged read also serves the external file",
    );
}

/// A **Local + host-provided** blob reads the local store — and only that. Gate-first
/// dispatch picks the local store from provenance, so a same-id external ref, a same-id
/// cache file, and a cloud copy are all ignored (whole and ranged).
#[tokio::test]
async fn local_host_provided_blob_reads_the_local_store_ignoring_decoys() {
    let db = read_test_db("photos");
    let storage = MockSyncStorage::new();
    let (tmp, ld) = temp_store_dir();

    // Local + host-provided: its bytes live in the local store, never the cloud.
    let blob = host_blob_ref("res0aaaa", "photos", CacheFill::CacheEager);
    let store_bytes = ramp(2048);
    plant_blob_row(&db, &blob.id, false, store_bytes.len() as u64).await;

    // Decoys at every other store — all must be ignored.
    put_cloud_blob(&storage, &blob.id, &blob.namespace, b"FROM-CLOUD").await;
    write_blob(&db, &ld, &blob.namespace, &blob.id, b"FROM-CACHE")
        .await
        .expect("write a same-id cache decoy");
    let ext_bytes = b"FROM-EXTERNAL".to_vec();
    let ext_path = write_external_file(tmp.path(), "res.bin", &ext_bytes);
    db.register_external_blob(&blob.id, &blob.namespace, &ext_path, ext_bytes.len() as u64)
        .await
        .expect("register a same-id external-ref decoy");

    // The real bytes: the host-provided local store.
    crate::blob::local_files::store(&ld, &blob.namespace, &blob.id, &store_bytes)
        .await
        .expect("store the host-provided local copy");

    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("Local + host-provided read");
    assert_eq!(
        got, store_bytes,
        "the read serves the local store, not the external/cache/cloud decoys",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        store_bytes.len() as u64,
        offset,
        len,
    )
    .await
    .expect("ranged Local + host-provided read");
    assert_eq!(
        mid,
        &store_bytes[offset as usize..(offset + len) as usize],
        "the ranged read also serves the local store",
    );
}

/// A **Remote + user-provided** blob reads the cache/cloud — and never the local
/// store. A stale same-id local-store file sits on disk plus a cloud copy; the read
/// ignores the local store, fetches from the cloud, and populates the cache (whole
/// and ranged). Upload staging is only part of Remote + host-provided reads.
#[tokio::test]
async fn remote_user_provided_blob_reads_cache_cloud_ignoring_a_stale_local_store_file() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("rem0cccc", "audio", CacheFill::CacheLazy);
    let cloud_bytes = ramp(2048);
    plant_blob_row(&db, &blob.id, true, cloud_bytes.len() as u64).await;

    // A stale local-store file (a Remote + user-provided read must NOT serve it) and
    // the real cloud copy with distinct bytes.
    crate::blob::local_files::store(&ld, &blob.namespace, &blob.id, b"STALE-LOCAL-STORE")
        .await
        .expect("write a stale local-store file");
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &cloud_bytes).await;

    let whole = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("Remote read fetches from the cloud");
    assert_eq!(
        whole, cloud_bytes,
        "a Remote + user-provided read serves the cloud copy, not the stale local-store file",
    );
    assert!(
        ld.cache_blob_path(&blob.namespace, &blob.id)
            .unwrap()
            .exists(),
        "the Remote read populated the evictable cache",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        cloud_bytes.len() as u64,
        offset,
        len,
    )
    .await
    .expect("ranged Remote read");
    assert_eq!(
        mid,
        &cloud_bytes[offset as usize..(offset + len) as usize],
        "the ranged Remote + user-provided read also ignores the local-store file",
    );
}

#[tokio::test]
async fn remote_user_provided_blob_with_only_a_stale_local_store_file_needs_cloud() {
    let db = read_test_db("audio");
    let (_tmp, ld) = temp_store_dir();
    let blob = blob_ref("rem0dddd", "audio", CacheFill::CacheLazy);

    plant_blob_row(&db, &blob.id, true, 17).await;
    crate::blob::local_files::store(&ld, &blob.namespace, &blob.id, b"STALE-LOCAL-STORE")
        .await
        .expect("write a stale local-store file");

    let err = read_blob(&db, &ld, None, &blob)
        .await
        .expect_err("Remote + user-provided read needs cache or cloud");
    assert!(
        matches!(err, BlobCacheError::NoCloudHome),
        "a stale local-store file does not satisfy a Remote + user-provided read: {err:?}",
    );

    let range_err = open_blob_stream(&db, &ld, None, &blob, 17, 0, 5)
        .await
        .expect_err("Remote + user-provided range read needs cache or cloud");
    assert!(
        matches!(range_err, BlobCacheError::NoCloudHome),
        "a stale local-store file does not satisfy a ranged Remote + user-provided read: {range_err:?}",
    );
}

#[tokio::test]
async fn remote_root_blob_reads_cache_cloud_even_without_a_gate_column() {
    let db = remote_root_db(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();
    let blob = host_blob_ref("rrrr0001", "photos", CacheFill::CacheEager);

    let cloud_bytes = b"REMOTE-ROOT-CLOUD";
    plant_blob_row(&db, &blob.id, false, cloud_bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, cloud_bytes).await;

    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("remote-root blob reads through cache/cloud");
    assert_eq!(
        got, b"REMOTE-ROOT-CLOUD",
        "a remote-root blob is Remote even when the table has no gate column"
    );
}

#[tokio::test]
async fn remote_root_cache_lazy_host_blob_pulls_row_then_reads_on_demand() {
    let storage = MockSyncStorage::new();
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'RemoteRoot', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
             VALUES ('lazy0001', 'n1', 'cover', 16, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);
    put_cloud_blob(&storage, "lazy0001", "photos", b"LAZY-REMOTE-ROOT").await;

    let db2 = remote_root_db(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    ));
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(
        !ld.cache_blob_path("photos", "lazy0001").unwrap().exists()
            && !ld.pinned_blob_path("photos", "lazy0001").unwrap().exists()
            && !ld.local_blob_path("photos", "lazy0001").unwrap().exists(),
        "CacheLazy host-provided blobs under a remote root are not downloaded on pull"
    );

    let blob = host_blob_ref("lazy0001", "photos", CacheFill::CacheLazy);
    let got = read_blob(&db2, &ld, Some(&storage), &blob)
        .await
        .expect("CacheLazy remote-root blob reads on demand");
    assert_eq!(got, b"LAZY-REMOTE-ROOT");
    assert!(
        ld.cache_blob_path("photos", "lazy0001").unwrap().exists(),
        "the on-demand read populates the evictable cache"
    );
}

#[tokio::test]
async fn plain_blob_table_without_gate_or_remote_root_fails_locality_resolution() {
    let db = plain_blob_db(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();
    let blob = host_blob_ref("plain001", "photos", CacheFill::CacheEager);

    let cloud_bytes = b"PLAIN-CLOUD";
    plant_blob_row(&db, &blob.id, true, cloud_bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, cloud_bytes).await;

    let err = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect_err("plain blob table has no authoritative locality");
    assert!(
        matches!(err, BlobCacheError::LocalityUnresolved { .. }),
        "plain blob table maps to LocalityUnresolved: {err:?}"
    );
}

/// A blob resolved **Local + user-provided** whose external ref is missing is fail-loud
/// [`BlobCacheError::NoExternalRef`], not a fall-through. The user's file is the only
/// copy and the ref is how coven finds it; its absence is corruption, surfaced loud.
#[tokio::test]
async fn local_user_provided_blob_without_an_external_ref_errors() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("extn-aaaa", "audio", CacheFill::CacheLazy);
    // Gate Local + the BlobRef's user-provided provenance ⇒ the external arm, but no
    // ref is registered. A cloud copy exists as a decoy a store-probing read would serve.
    plant_blob_row(&db, &blob.id, false, 11).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, b"CLOUD-DECOY").await;

    let err = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect_err("a Local user-provided blob with no external ref is terminal");
    assert!(
        matches!(err, BlobCacheError::NoExternalRef { .. }),
        "a missing external ref maps to NoExternalRef: {err:?}",
    );
    let range_err = open_blob_stream(&db, &ld, Some(&storage), &blob, 11, 0, 5)
        .await
        .expect_err("the ranged read is fail-loud on a missing external ref too");
    assert!(
        matches!(range_err, BlobCacheError::NoExternalRef { .. }),
        "the ranged read also maps to NoExternalRef: {range_err:?}",
    );
}

/// A **Local + host-provided** blob whose local store is empty is fail-loud corruption:
/// [`read_blob`] (and [`open_blob_stream`]) return [`BlobCacheError::NoLocalCopy`]
/// rather than reaching the cloud. A cloud copy exists under the same id — a read that
/// probed every store would serve it — so this proves the host-provided Local arm
/// refuses to: a Local blob has no cloud copy, and a missing local file is broken state
/// to surface, not a cache miss to refetch.
#[tokio::test]
async fn local_blob_absent_from_local_store_errors_instead_of_hitting_the_cloud() {
    let db = read_test_db("photos");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    let blob = host_blob_ref("res1bbbb", "photos", CacheFill::CacheEager);
    plant_blob_row(&db, &blob.id, false, 11).await;

    // A cloud copy under the same id: a read that probed every store would serve these
    // bytes. No external ref, no local-store file, no cache file.
    put_cloud_blob(&storage, &blob.id, &blob.namespace, b"CLOUD-DECOY").await;

    let err = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect_err("a Local blob missing from the local store is terminal, never a cloud fetch");
    assert!(
        matches!(err, BlobCacheError::NoLocalCopy { .. }),
        "a missing Local copy maps to NoLocalCopy: {err:?}",
    );

    let range_err = open_blob_stream(&db, &ld, Some(&storage), &blob, 11, 0, 5)
        .await
        .expect_err("the ranged read is fail-loud on a missing Local copy too");
    assert!(
        matches!(range_err, BlobCacheError::NoLocalCopy { .. }),
        "the ranged read also maps to NoLocalCopy: {range_err:?}",
    );
}

// ---- Drift receipts: gate-first beats external-first, namespace-scoped gate ----

/// A Remote user-provided blob that STILL carries a registered external ref reads the
/// cache/cloud, never the stale ref. Manufactured drift: the gate is Remote yet a
/// `local_blob_refs` row points at a (distinct-bytes) file, plus a cloud copy. The old
/// external-first read would have served the stale ref; gate-first serves the cloud
/// (whole and ranged).
#[tokio::test]
async fn remote_user_provided_blob_ignores_a_stale_external_ref() {
    let db = read_test_db("audio");
    let storage = MockSyncStorage::new();
    let (tmp, ld) = temp_store_dir();

    // Remote + user-provided, with a stale external ref still registered.
    let blob = blob_ref("drft0aaa", "audio", CacheFill::CacheLazy);
    let stale_ext = ramp(1500);
    let path = write_external_file(tmp.path(), "stale.flac", &stale_ext);
    db.register_external_blob(&blob.id, &blob.namespace, &path, stale_ext.len() as u64)
        .await
        .expect("register the stale external ref");
    let cloud_bytes = ramp(2048);
    plant_blob_row(&db, &blob.id, true, cloud_bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &cloud_bytes).await;

    let whole = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("a Remote blob reads the cloud");
    assert_eq!(
        whole, cloud_bytes,
        "a Remote blob serves the cloud, ignoring the stale external ref",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = open_blob_stream(
        &db,
        &ld,
        Some(&storage),
        &blob,
        cloud_bytes.len() as u64,
        offset,
        len,
    )
    .await
    .expect("ranged Remote read");
    assert_eq!(
        mid,
        &cloud_bytes[offset as usize..(offset + len) as usize],
        "the ranged read also ignores the stale external ref",
    );
}

/// The same blob id carried by a row in two different namespaces' tables, under
/// different gates, resolves the gate of the blob's OWN namespace — not whichever
/// table a scan-all would have matched first. `note_photos` (namespace `ns_local`)
/// sits under a Local note; `note_covers` (namespace `ns_remote`) under a Remote note;
/// both rows share one id. Reading the `ns_remote` blob serves its cloud copy (Remote
/// gate) and the `ns_local` blob serves its local store (Local gate) — distinct bytes
/// proving each read its own namespace's gate.
#[tokio::test]
async fn read_resolves_the_blobs_own_namespace_gate_not_a_colliding_id() {
    let db = open_test_db_with_user_and_host_blobs(
        BlobDecl::new("ns_local", Provenance::HostProvided, CacheFill::CacheEager),
        BlobDecl::new("ns_remote", Provenance::HostProvided, CacheFill::CacheEager),
    );
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    // One id carried by a row in each table, under different gates: note_photos
    // (ns_local) below a Local note, note_covers (ns_remote) below a Remote note.
    let id = "dup0aaaa";
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('note-local', 'x', 0, '0000000001000-0000-dev1', '2026-01-01'), \
                    ('note-remote', 'x', 1, '0000000001000-0000-dev1', '2026-01-01')",
            [],
        )
        .map_err(crate::database::DbError::from)?;
        conn.execute(
            "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
             VALUES (?1, 'note-local', 'attach', 17, '0000000001000-0000-dev1', '2026-01-01')",
            [id],
        )
        .map_err(crate::database::DbError::from)?;
        conn.execute(
            "INSERT INTO note_covers (id, note_id, size, _updated_at, created_at) \
             VALUES (?1, 'note-remote', 18, '0000000001000-0000-dev1', '2026-01-01')",
            [id],
        )
        .map_err(crate::database::DbError::from)?;
        Ok(())
    })
    .await
    .expect("plant the colliding-id rows");

    // Distinct bytes per source so the result reveals which gate was read.
    crate::blob::local_files::store(&ld, "ns_local", id, b"LOCAL-STORE-BYTES")
        .await
        .expect("store the Local blob's local copy");
    put_cloud_blob(&storage, id, "ns_remote", b"REMOTE-CLOUD-BYTES").await;

    // The ns_remote blob resolves note_covers' gate (Remote) → its cloud copy.
    let remote_blob = host_blob_ref(id, "ns_remote", CacheFill::CacheEager);
    assert_eq!(
        read_blob(&db, &ld, Some(&storage), &remote_blob)
            .await
            .unwrap(),
        b"REMOTE-CLOUD-BYTES",
        "the ns_remote blob resolves its own table's Remote gate, not the colliding Local row",
    );

    // The ns_local blob resolves note_photos' gate (Local) → its local store.
    let local_blob = host_blob_ref(id, "ns_local", CacheFill::CacheEager);
    assert_eq!(
        read_blob(&db, &ld, Some(&storage), &local_blob)
            .await
            .unwrap(),
        b"LOCAL-STORE-BYTES",
        "the ns_local blob resolves its own table's Local gate",
    );
}

// ---- Eviction (per-namespace budgets, folder-model) ----

/// Sum the sizes of every file under `storage/cache/<namespace>/` — the same total
/// `evict_to_budget` measures for that namespace, recomputed from the test side to
/// assert the budget is respected. Reuses the production tree walker
/// ([`crate::local_blob::walk_files`]) over that namespace's cache subtree (which
/// ignores `pinned/` and every other namespace), so the test measures the cache the
/// same way eviction does. An absent subtree walks to nothing (0).
async fn cache_total_bytes(ld: &crate::store_dir::StoreDir, namespace: &str) -> u64 {
    let dir = ld
        .cache_namespace_dir(namespace)
        .expect("valid namespace token");
    crate::local_blob::walk_files(&dir)
        .await
        .expect("walk the namespace cache subtree")
        .iter()
        .map(|(_, _, size)| *size)
        .sum()
}

/// Pin a cache file's modification time to a fixed instant so eviction order is
/// deterministic (the cache evicts oldest-mtime first). `secs` is an offset from
/// the unix epoch — smaller is older.
fn set_cache_mtime(ld: &crate::store_dir::StoreDir, namespace: &str, id: &str, secs: u64) {
    let path = ld.cache_blob_path(namespace, id).expect("cache path");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open cache file to set mtime");
    file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
        .expect("set cache file mtime");
}

/// Stage `bytes` into `cache/<namespace>/<id>` with a fixed modification time, with
/// NO budget set so the stage itself never evicts. Builds the over-budget cache a
/// later eviction test then trims.
async fn stage_with_mtime(
    db: &Database,
    ld: &crate::store_dir::StoreDir,
    namespace: &str,
    id: &str,
    bytes: &[u8],
    mtime_secs: u64,
) {
    let blob = blob_ref(id, namespace, CacheFill::CacheLazy);
    write_blob(db, ld, &blob.namespace, &blob.id, bytes)
        .await
        .expect("stage blob into cache");
    set_cache_mtime(ld, namespace, id, mtime_secs);
}

/// Over budget, eviction deletes the OLDEST files (by mtime) in that namespace first
/// and stops once the total is back under the namespace's budget: the oldest go, the
/// newest stay, and the summed `cache/<namespace>/` size ends `<=` the budget. Driven
/// by staging files with distinct mtimes (no budget), then setting the budget and
/// running a sweep.
#[tokio::test]
async fn eviction_drops_oldest_cache_files_until_under_budget() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    // Four 100-byte files, oldest → newest by mtime. No budget yet, so staging does
    // not evict; the cache holds all 400 bytes.
    stage_with_mtime(&db, &ld, "release_files", "old1aaaa", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "release_files", "old2bbbb", &[2u8; 100], 2000).await;
    stage_with_mtime(&db, &ld, "release_files", "new3cccc", &[3u8; 100], 3000).await;
    stage_with_mtime(&db, &ld, "release_files", "new4dddd", &[4u8; 100], 4000).await;
    assert_eq!(
        cache_total_bytes(&ld, "release_files").await,
        400,
        "all four files are cached"
    );

    // Budget of 250 bytes: the two oldest (200 bytes) must go to bring the total
    // (then 200) under budget; the two newest stay. A bare sweep (`None`) — no file
    // is being protected as just-written here.
    db.set_cache_budget("release_files", 250)
        .await
        .expect("set budget");
    evict_to_budget(&db, &ld, "release_files", None)
        .await
        .expect("evict to budget");

    assert!(
        !ld.cache_blob_path("release_files", "old1aaaa")
            .unwrap()
            .exists(),
        "the oldest file is evicted first",
    );
    assert!(
        !ld.cache_blob_path("release_files", "old2bbbb")
            .unwrap()
            .exists(),
        "the second-oldest file is evicted next",
    );
    assert!(
        ld.cache_blob_path("release_files", "new3cccc")
            .unwrap()
            .exists(),
        "a newer file survives once the total is back under budget",
    );
    assert!(
        ld.cache_blob_path("release_files", "new4dddd")
            .unwrap()
            .exists(),
        "the newest file survives",
    );
    assert!(
        cache_total_bytes(&ld, "release_files").await <= 250,
        "the cache is back within budget after eviction",
    );
}

/// Per-namespace isolation: filling `release_files` past its budget evicts that
/// namespace's oldest audio files but leaves a `covers` blob in another namespace
/// untouched — the sweep walks only `cache/release_files/`, never `cache/covers/`.
#[tokio::test]
async fn release_files_eviction_leaves_covers_intact() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    // A cover in the small `covers` namespace, plus four audio files in the big
    // `release_files` namespace. No budgets yet.
    stage_with_mtime(&db, &ld, "covers", "cov00aaa", &[9u8; 500], 1000).await;
    stage_with_mtime(&db, &ld, "release_files", "aud01aaa", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "release_files", "aud02bbb", &[2u8; 100], 2000).await;
    stage_with_mtime(&db, &ld, "release_files", "aud03ccc", &[3u8; 100], 3000).await;
    stage_with_mtime(&db, &ld, "release_files", "aud04ddd", &[4u8; 100], 4000).await;

    // Only `release_files` is budgeted and swept. `covers` is never touched.
    db.set_cache_budget("release_files", 250)
        .await
        .expect("set release_files budget");
    evict_to_budget(&db, &ld, "release_files", None)
        .await
        .expect("evict release_files to budget");

    assert!(
        !ld.cache_blob_path("release_files", "aud01aaa")
            .unwrap()
            .exists(),
        "the oldest audio file is evicted under release_files pressure",
    );
    assert!(
        ld.cache_blob_path("release_files", "aud04ddd")
            .unwrap()
            .exists(),
        "the newest audio file survives within release_files' budget",
    );
    assert!(
        cache_total_bytes(&ld, "release_files").await <= 250,
        "release_files is back within its budget",
    );
    assert!(
        ld.cache_blob_path("covers", "cov00aaa").unwrap().exists(),
        "the cover in another namespace is untouched by release_files eviction",
    );
    assert_eq!(
        cache_total_bytes(&ld, "covers").await,
        500,
        "covers' cache is whole — its namespace was never walked",
    );
}

/// A `covers` namespace with a small budget evicts its own oldest cover when over
/// budget, and a later read of the evicted cover re-fetches it from the cloud back
/// into `cache/covers/` (covers are not pinned — an evicted one re-materializes on
/// the next read).
#[tokio::test]
async fn covers_eviction_drops_oldest_cover_and_a_read_refetches() {
    let db = read_test_db("covers");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    // Two 200-byte covers in the cloud, both `CacheEager`. The `covers` budget holds
    // one at a time (250 bytes).
    let cov1 = blob_ref("cov01aaa", "covers", CacheFill::CacheEager);
    let cov2 = blob_ref("cov02bbb", "covers", CacheFill::CacheEager);
    let cov1_bytes = [1u8; 200];
    let cov2_bytes = [2u8; 200];
    plant_blob_row(&db, &cov1.id, true, cov1_bytes.len() as u64).await;
    plant_blob_row(&db, &cov2.id, true, cov2_bytes.len() as u64).await;
    put_cloud_blob(&storage, &cov1.id, &cov1.namespace, &cov1_bytes).await;
    put_cloud_blob(&storage, &cov2.id, &cov2.namespace, &cov2_bytes).await;
    db.set_cache_budget("covers", 250)
        .await
        .expect("set covers budget");

    // Read the first cover into the cache, then age it so it is the oldest.
    read_blob(&db, &ld, Some(&storage), &cov1)
        .await
        .expect("first cover read populates the cache");
    set_cache_mtime(&ld, "covers", &cov1.id, 1000);

    // Read the second cover: populating it pushes `covers` to 400 (> 250), so the
    // read's own sweep evicts the oldest (the first cover), leaving only the second.
    read_blob(&db, &ld, Some(&storage), &cov2)
        .await
        .expect("second cover read populates and evicts to budget");
    assert!(
        !ld.cache_blob_path("covers", &cov1.id).unwrap().exists(),
        "the oldest cover is evicted when covers goes over its small budget",
    );
    assert!(
        ld.cache_blob_path("covers", &cov2.id).unwrap().exists(),
        "the just-read cover stays",
    );

    // A read of the evicted cover re-fetches it from the cloud back into the cache.
    let refetched = read_blob(&db, &ld, Some(&storage), &cov1)
        .await
        .expect("a read of the evicted cover re-fetches it");
    assert_eq!(refetched, vec![1u8; 200], "the re-fetch returns the bytes");
    assert!(
        ld.cache_blob_path("covers", &cov1.id).unwrap().exists(),
        "the re-fetched cover is back in cache/covers/",
    );
}

/// A pinned blob is structurally exempt regardless of its namespace's budget: it
/// lives in `pinned/<namespace>/`, which the budget never walks, so it is never
/// evicted no matter how far over budget that namespace's cache is. Here a
/// `CacheEager` blob (namespace `photos`) pulled and pinned, plus a user-pinned
/// `CacheLazy` blob (namespace `audio`), both survive a tiny budget on their OWN
/// namespace with that namespace's cache flooded.
#[tokio::test]
async fn a_pinned_blob_is_never_evicted_even_far_over_budget() {
    let storage = MockSyncStorage::new();

    // dev1 records a note + a (master-scoped) photo row; pull on dev2 lands the
    // CacheEager blob in the evictable cache, then a pin promotes it to `pinned/`.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('mir0aaaa', 'n1', 'attach', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    put_cloud_blob(&storage, "mir0aaaa", "photos", &[9u8; 500]).await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db_with_blob(photo_decl());
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld).await;
    assert_eq!(result.changesets_applied, 1);
    assert!(
        ld.cache_blob_path("photos", "mir0aaaa").unwrap().exists(),
        "the CacheEager blob lands in the evictable cache on pull",
    );
    let eager = blob_ref("mir0aaaa", "photos", CacheFill::CacheEager);
    pin(&db2, &ld, Some(&storage), std::slice::from_ref(&eager))
        .await
        .expect("pin the eager blob into pinned/");
    assert!(ld.pinned_blob_path("photos", "mir0aaaa").unwrap().exists());

    // Also user-pin a CacheLazy blob (a different namespace) into pinned/.
    let lazy = blob_ref("usr0bbbb", "audio", CacheFill::CacheLazy);
    write_blob(&db2, &ld, &lazy.namespace, &lazy.id, &[7u8; 500])
        .await
        .expect("write the lazy blob into the cache");
    pin(&db2, &ld, Some(&storage), std::slice::from_ref(&lazy))
        .await
        .expect("user-pin the lazy blob");
    assert!(ld.pinned_blob_path("audio", "usr0bbbb").unwrap().exists());

    // Flood BOTH namespaces' evictable caches, set a tiny budget on EACH, and sweep
    // each. The pinned files live in pinned/ — the per-namespace sweep never touches
    // them, whichever namespace's budget runs.
    stage_with_mtime(&db2, &ld, "photos", "junkp1cc", &[1u8; 1000], 1000).await;
    stage_with_mtime(&db2, &ld, "audio", "junka1cc", &[2u8; 1000], 2000).await;
    db2.set_cache_budget("photos", 10)
        .await
        .expect("set tiny photos budget");
    db2.set_cache_budget("audio", 10)
        .await
        .expect("set tiny audio budget");
    evict_to_budget(&db2, &ld, "photos", None)
        .await
        .expect("evict photos to budget");
    evict_to_budget(&db2, &ld, "audio", None)
        .await
        .expect("evict audio to budget");

    assert!(
        ld.pinned_blob_path("photos", "mir0aaaa").unwrap().exists(),
        "a pinned CacheEager blob survives its namespace's eviction (it is in pinned/)",
    );
    assert!(
        ld.pinned_blob_path("audio", "usr0bbbb").unwrap().exists(),
        "a user-pinned CacheLazy blob survives its namespace's eviction (it is in pinned/)",
    );
    assert!(
        cache_total_bytes(&ld, "photos").await <= 10,
        "the photos evictable cache is trimmed to budget, ignoring pinned/",
    );
    assert!(
        cache_total_bytes(&ld, "audio").await <= 10,
        "the audio evictable cache is trimmed to budget, ignoring pinned/",
    );
}

/// With no budget set for a namespace its cache is unlimited: even a large cache and
/// an explicit eviction sweep leave every file in place. The host opts a namespace
/// into a budget; until then nothing in it is evicted.
#[tokio::test]
async fn unset_namespace_budget_never_evicts() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    stage_with_mtime(&db, &ld, "release_files", "keep1aaa", &[1u8; 5000], 1000).await;
    stage_with_mtime(&db, &ld, "release_files", "keep2bbb", &[2u8; 5000], 2000).await;
    stage_with_mtime(&db, &ld, "release_files", "keep3ccc", &[3u8; 5000], 3000).await;
    assert_eq!(cache_total_bytes(&ld, "release_files").await, 15000);

    // No budget set for this namespace — an explicit sweep is a no-op.
    evict_to_budget(&db, &ld, "release_files", None)
        .await
        .expect("evict is a no-op with no budget");
    assert_eq!(
        cache_total_bytes(&ld, "release_files").await,
        15000,
        "a big cache stays whole when no budget is set",
    );
    for id in ["keep1aaa", "keep2bbb", "keep3ccc"] {
        assert!(
            ld.cache_blob_path("release_files", id).unwrap().exists(),
            "{id} survives with no budget",
        );
    }
}

/// The blob a read just populated is the newest, so a single over-budget eviction
/// triggered by that read does not evict it: an older file goes first, bringing the
/// total under budget before the new file is reached. The triggering read still
/// returns the fetched bytes, and the new file stays on disk.
#[tokio::test]
async fn just_populated_blob_survives_the_read_that_triggers_eviction() {
    let db = read_test_db("release_files");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    // An older cached file (100 bytes, old mtime), no budget yet.
    stage_with_mtime(&db, &ld, "release_files", "older1aa", &[1u8; 100], 1000).await;

    // Budget of 150 bytes: holds either file alone, not both. Now a read fetches a
    // second 100-byte blob; populating it pushes the total to 200 (> 150), so the
    // read's own eviction must drop the older file (the newest — the one just
    // read — survives).
    db.set_cache_budget("release_files", 150)
        .await
        .expect("set budget");
    let blob = blob_ref("newer2bb", "release_files", CacheFill::CacheLazy);
    let bytes = vec![2u8; 100];
    plant_blob_row(&db, &blob.id, true, bytes.len() as u64).await;
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    let got = read_blob(&db, &ld, Some(&storage), &blob)
        .await
        .expect("read fetches and populates, then evicts to budget");
    assert_eq!(got, bytes, "the triggering read still returns its bytes");
    assert!(
        ld.cache_blob_path("release_files", "newer2bb")
            .unwrap()
            .exists(),
        "the just-populated (newest) blob survives its own over-budget eviction",
    );
    assert!(
        !ld.cache_blob_path("release_files", "older1aa")
            .unwrap()
            .exists(),
        "the older blob is the one evicted",
    );
    assert!(
        cache_total_bytes(&ld, "release_files").await <= 150,
        "the cache is back within budget after the read-triggered eviction",
    );
}

/// The budget never drifts over: after a sequence of over-budget populates (each a
/// `read_blob` miss that fetches a new blob), the summed `cache/<namespace>/` size is
/// `<=` the namespace budget every time, because each populate's own sweep trims back
/// to it.
#[tokio::test]
async fn budget_never_drifts_over_across_repeated_populates() {
    let db = read_test_db("release_files");
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_store_dir();

    // Budget of 250 bytes; each blob is 100 bytes, so at most two fit.
    db.set_cache_budget("release_files", 250)
        .await
        .expect("set budget");

    for i in 0..6u8 {
        let id = format!("seqr{i:04}"); // ≥4 chars, distinct per i, for the shard
        let bytes = vec![i; 100];
        plant_blob_row(&db, &id, true, bytes.len() as u64).await;
        put_cloud_blob(&storage, &id, "release_files", &bytes).await;
        let blob = blob_ref(&id, "release_files", CacheFill::CacheLazy);

        let got = read_blob(&db, &ld, Some(&storage), &blob)
            .await
            .expect("each read populates then evicts to budget");
        assert_eq!(got, bytes, "each read returns its freshly-fetched bytes");

        // The cache is within budget after every populate, never drifting over as
        // new blobs arrive.
        assert!(
            cache_total_bytes(&ld, "release_files").await <= 250,
            "after populate {i} the cache is within the 250-byte budget",
        );
    }

    // Eviction ran rather than the budget never being reached: with a 250-byte
    // budget and 100-byte blobs, at most two fit, so after six reads the earliest
    // blobs must have been evicted and the just-read last blob must still be present.
    assert!(
        cache_total_bytes(&ld, "release_files").await <= 200,
        "at most two 100-byte blobs remain under the 250-byte budget",
    );
    assert!(
        !ld.cache_blob_path("release_files", "seqr0000")
            .unwrap()
            .exists(),
        "the first blob read was evicted by later populates",
    );
    assert!(
        ld.cache_blob_path("release_files", "seqr0005")
            .unwrap()
            .exists(),
        "the most-recently read blob is still cached",
    );
}

/// The `protect` file is never evicted even when it is NOT the newest by mtime: a
/// populate's survival is structural (the just-written path is excluded outright),
/// not a bet on mtime ordering. Here the protected file is the OLDER of the two —
/// so a pure oldest-first sweep would evict it first — yet it survives and the
/// newer, unprotected file is the one dropped. This is the case mtime granularity
/// can't be trusted for (two writes within one filesystem mtime tick are
/// unordered): the protection makes it deterministic regardless.
#[tokio::test]
async fn the_protected_file_survives_even_when_it_is_not_the_newest() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    // Two 100-byte files. The protected one is OLDER (mtime 1000); the other is
    // NEWER (mtime 2000). A naive oldest-first sweep would evict the protected one.
    stage_with_mtime(&db, &ld, "release_files", "prot0aaa", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "release_files", "othr0bbb", &[2u8; 100], 2000).await;

    // Budget of 100 bytes: exactly one file fits, so one must be evicted.
    db.set_cache_budget("release_files", 100)
        .await
        .expect("set budget");
    let protected = ld.cache_blob_path("release_files", "prot0aaa").unwrap();
    evict_to_budget(&db, &ld, "release_files", Some(&protected))
        .await
        .expect("evict to budget, protecting the older file");

    assert!(
        ld.cache_blob_path("release_files", "prot0aaa")
            .unwrap()
            .exists(),
        "the protected file survives even though it is the older by mtime",
    );
    assert!(
        !ld.cache_blob_path("release_files", "othr0bbb")
            .unwrap()
            .exists(),
        "the newer, unprotected file is the one evicted instead",
    );
    assert!(
        cache_total_bytes(&ld, "release_files").await <= 100,
        "the cache is within budget after the protected eviction",
    );
}

/// When the protected in-use file alone exceeds the budget, eviction cannot reach
/// the budget: it deletes every other candidate and then leaves the cache holding
/// exactly that file, over budget by that much. The call still returns `Ok(())` —
/// the file being served can't be evicted — rather than failing the populate that
/// triggered it. (The over-budget condition is logged, not asserted here.)
#[tokio::test]
async fn protected_file_larger_than_budget_leaves_cache_over_budget_but_ok() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    // A 100-byte protected file plus a 100-byte evictable one. The protected file
    // alone (100 bytes) is larger than the 50-byte budget.
    stage_with_mtime(&db, &ld, "release_files", "biginuse", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "release_files", "othr0bbb", &[2u8; 100], 2000).await;

    db.set_cache_budget("release_files", 50)
        .await
        .expect("set budget");
    let protected = ld.cache_blob_path("release_files", "biginuse").unwrap();
    evict_to_budget(&db, &ld, "release_files", Some(&protected))
        .await
        .expect("eviction returns Ok even when the in-use file alone exceeds budget");

    assert!(
        ld.cache_blob_path("release_files", "biginuse")
            .unwrap()
            .exists(),
        "the protected in-use file is kept even though it alone exceeds the budget",
    );
    assert!(
        !ld.cache_blob_path("release_files", "othr0bbb")
            .unwrap()
            .exists(),
        "every other candidate is still evicted",
    );
    assert_eq!(
        cache_total_bytes(&ld, "release_files").await,
        100,
        "the cache is left over budget, holding exactly the in-use file",
    );
}

/// Eviction never deletes a `.tmp.<uuid>` sibling. While a populate is mid-write, its
/// atomic-write temp lives in the same namespace cache subtree the sweep walks;
/// treating it as an eviction candidate would let the sweep delete it and fail that
/// populate's finishing rename. The temp is skipped outright: it survives an
/// over-budget sweep whose oldest-first order would otherwise take it first, its bytes
/// never count toward the budget, and the rename that turns it into a committed blob
/// still succeeds.
#[tokio::test]
async fn eviction_skips_a_concurrent_populates_temp_file() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    // A committed 100-byte cache file (newer by mtime), already at the budget.
    stage_with_mtime(&db, &ld, "release_files", "keep0aaa", &[1u8; 100], 2000).await;

    // A concurrent populate's in-flight temp: a `.tmp.<uuid>` sibling in the shard dir
    // where its committed blob will land, aged OLDER than the committed file so a sweep
    // that treated it as a candidate would evict it first.
    let dest = ld.cache_blob_path("release_files", "new0bbbb").unwrap();
    let shard = dest.parent().unwrap();
    std::fs::create_dir_all(shard).expect("create shard dir");
    let temp_path = shard.join(format!(
        "{}{}",
        crate::local_blob::TEMP_BLOB_PREFIX,
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&temp_path, [9u8; 100]).expect("write concurrent populate temp");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&temp_path)
        .expect("open temp to age it")
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000))
        .expect("age the temp so a candidate sweep would evict it first");

    // Budget of 100 bytes: exactly the committed file fits. A sweep that counted the
    // temp would see 200 bytes and evict the oldest (the temp) first.
    db.set_cache_budget("release_files", 100)
        .await
        .expect("set budget");
    evict_to_budget(&db, &ld, "release_files", None)
        .await
        .expect("evict to budget");

    assert!(
        temp_path.exists(),
        "eviction leaves a concurrent populate's temp in place",
    );
    assert!(
        ld.cache_blob_path("release_files", "keep0aaa")
            .unwrap()
            .exists(),
        "the committed cache file survives — the temp's bytes never counted toward the budget",
    );

    // The populate's finishing rename still finds its temp and commits the blob intact.
    crate::local_blob::rename(&temp_path, &dest)
        .await
        .expect("the populate's rename succeeds after the sweep");
    assert_eq!(
        std::fs::read(&dest).expect("committed blob readable"),
        [9u8; 100],
        "the renamed temp became the committed blob intact",
    );
}

/// When the uploader recorded for a blob no longer holds it (its copy was
/// legitimately reclaimed) but another member's copy of the same `(namespace, id)`
/// survives, the read must not fail forever on the stale record. `fetch_from_cloud`
/// treats the recorded prefix's NotFound as evidence the record is stale: it
/// forgets the record, rescans for a surviving copy, records it, and retries.
#[tokio::test]
async fn stale_uploader_record_falls_back_to_a_surviving_copy() {
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::CloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use std::sync::Arc;

    // A hashed home storing bytes verbatim, so a copy can be planted under any
    // uploader prefix and read back without encryption setup.
    let home = Arc::new(InMemoryCloudHome::new());
    let storage = CloudSyncStorage::new(
        home.clone(),
        CloudCipher::Plaintext,
        BlobPathScheme::Hashed,
        "test-lib",
        UserKeypair::generate(),
    );
    let db = open_test_db();
    let blob = blob_ref("blobxxxx", "photos", CacheFill::CacheLazy);

    let uploader_a = hex::encode(UserKeypair::generate().public_key());
    let uploader_b = hex::encode(UserKeypair::generate().public_key());

    // Only uploader B's copy exists on the cloud, but the index (staleness) records
    // uploader A — A's copy has been reclaimed.
    let b_key = CloudSyncStorage::blob_key(
        BlobPathScheme::Hashed,
        "photos",
        Some(&uploader_b),
        "blobxxxx",
        None,
    )
    .expect("hashed key");
    home.put_object(&b_key, b"BLOB-BYTES".to_vec())
        .await
        .expect("plant B's copy");
    db.record_blob_uploader("photos", "blobxxxx", &uploader_a)
        .await
        .expect("record stale uploader");

    // The read finds A's prefix empty, forgets A, rescans, finds and records B, and
    // serves B's bytes.
    let bytes = fetch_from_cloud(&db, &storage, &blob)
        .await
        .expect("read falls back to the surviving copy");
    assert_eq!(bytes, b"BLOB-BYTES");
    assert_eq!(
        db.blob_uploader("photos", "blobxxxx")
            .await
            .expect("read uploader index"),
        Some(uploader_b),
        "the index now points at the surviving copy's uploader",
    );
}
