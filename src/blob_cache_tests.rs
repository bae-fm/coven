//! Tests for the device-local blob cache.
//!
//! These drive the real cache free functions over a real [`Database`] and a real
//! temp library directory, with a [`MockSyncStorage`] standing in for the cloud, so
//! a hit/miss and a folder move are exercised against actual files on disk. The
//! load-bearing properties: presence is the file (no table), pinned-ness is which
//! folder (`storage/pinned/` vs `storage/cache/`), and a read serves a local copy
//! without a cloud round-trip.

use std::collections::HashMap;

use crate::blob::{BlobRef, BlobScope, BlobSource, BlobSync, ResolvedScope};
use crate::blob_cache::{clear_cache, open_blob_stream, pin, read_blob, unpin, write_blob};
use crate::changeset::RowChange;
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::{
    capture_bytes, note_photos_refs, note_photos_refs_from_db, open_test_db, pull_into,
    temp_library_dir, MockSyncStorage, PhotoBlobSource,
};

/// A `BlobRef` keyed by `id` in `namespace`, master-scoped, no `cloud_path`, of
/// retention class `sync`. `local_path` is set to a throwaway path the cache never
/// reads (the cache owns the on-disk destination); it is here only to fill the
/// field. Blob ids are ≥4 chars so they form the `{ab}/{cd}` partition shard.
fn blob_ref(id: &str, namespace: &str, sync: BlobSync) -> BlobRef {
    BlobRef {
        namespace: namespace.to_string(),
        id: id.to_string(),
        local_path: std::path::PathBuf::from("/unused/upload/source"),
        scope: BlobScope::Master,
        cloud_path: None,
        sync,
    }
}

/// Put `bytes` into the mock cloud under the flat `{namespace}/{id}` key the mock's
/// `get_blob` reads back (master scope, no cloud_path), so a cache miss can fetch it.
async fn put_cloud_blob(storage: &MockSyncStorage, id: &str, namespace: &str, bytes: &[u8]) {
    storage
        .put_blob(namespace, id, ResolvedScope::Master, None, bytes.to_vec())
        .await
        .expect("put blob in mock cloud");
}

/// A blob source mapping each `note_photos` row to an `OnDemand` blob (master
/// scope) under `dir` in the `audio` namespace — the class that is uploaded on push
/// but NOT downloaded on pull (fetched on first read instead). The OnDemand mirror
/// of [`PhotoBlobSource`], for driving the real pull with an OnDemand blob.
struct OnDemandSource {
    dir: std::path::PathBuf,
}

impl BlobSource for OnDemandSource {
    fn blobs_for_change(&self, change: &RowChange) -> Vec<BlobRef> {
        note_photos_refs(
            std::slice::from_ref(change),
            &self.dir,
            "audio",
            &|_kind, _note_id| BlobScope::Master,
            BlobSync::OnDemand,
        )
    }
    fn blobs_in_db(&self, conn: &rusqlite::Connection) -> rusqlite::Result<Vec<BlobRef>> {
        note_photos_refs_from_db(
            conn,
            &self.dir,
            "audio",
            &|_kind, _note_id| BlobScope::Master,
            BlobSync::OnDemand,
        )
    }
}

/// A second read is a local hit: the first read populates `cache/<id>` from the
/// cloud, and after the cloud copy is deleted the second read still returns the
/// bytes — served from disk, no fetch.
#[tokio::test]
async fn second_read_is_a_local_hit() {
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let blob = blob_ref("blob-aaaa", "audio", BlobSync::OnDemand);
    let bytes = b"THE-BLOB-BYTES".to_vec();
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    // First read misses, fetches from the cloud, and populates the evictable cache.
    let first = read_blob(&db, &ld, &storage, &blob)
        .await
        .expect("first read fetches from cloud");
    assert_eq!(first, bytes);
    assert!(
        ld.cache_blob_path(&blob.id).unwrap().exists(),
        "the first read populates storage/cache/<id>",
    );

    // Delete the cloud copy so a second fetch would fail: the read must be served
    // from the local file, proving the cache hit, not a re-download.
    storage.delete_blob_object("audio", &blob.id).await;
    let second = read_blob(&db, &ld, &storage, &blob)
        .await
        .expect("second read is served from the local cache");
    assert_eq!(
        second, bytes,
        "the second read returns the cached bytes without touching the cloud",
    );
}

/// A Mirrored blob pulled in a changeset lands SYSTEM-PINNED: its file is in
/// `storage/pinned/<id>`, not the evictable `storage/cache/<id>`. (Driven through
/// the real pull, which routes Mirrored blobs to `download_blobs` → `pinned/`.)
#[tokio::test]
async fn mirrored_lands_in_pinned_on_pull() {
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

    // The puller's blob source maps the photo to a Mirrored blob; the pull writes
    // it under the library dir's pinned tree.
    let dst_photos = tempfile::tempdir().expect("dst photos");
    let plan = PhotoBlobSource {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &plan).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert!(
        ld.pinned_blob_path("ph01abcd").unwrap().exists(),
        "a Mirrored blob is system-pinned on pull: it lands in storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path("ph01abcd").unwrap().exists(),
        "a Mirrored blob does NOT land in the evictable storage/cache/<id>",
    );
}

/// Pin promotes a cached blob to `pinned/` and that survives a cache sweep; unpin
/// demotes it back to `cache/` where a sweep then drops it; and unpinning a Mirrored
/// blob is rejected (its system pin is not user-removable).
#[tokio::test]
async fn pin_survives_clear_cache_unpin_demotes_and_mirrored_unpin_is_rejected() {
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let blob = blob_ref("ond-aaaa", "audio", BlobSync::OnDemand);
    let bytes = b"ON-DEMAND-AUDIO".to_vec();
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    // Read it on demand → it lands in the evictable cache.
    read_blob(&db, &ld, &storage, &blob)
        .await
        .expect("read populates the cache");
    assert!(ld.cache_blob_path(&blob.id).unwrap().exists());
    assert!(!ld.pinned_blob_path(&blob.id).unwrap().exists());

    // Pin it → moves cache/ → pinned/.
    pin(&db, &ld, &storage, std::slice::from_ref(&blob))
        .await
        .expect("pin promotes the cached blob");
    assert!(
        ld.pinned_blob_path(&blob.id).unwrap().exists(),
        "pin moves the blob into storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path(&blob.id).unwrap().exists(),
        "pin leaves nothing behind in storage/cache/<id>",
    );

    // Clear the cache → the pinned blob is untouched.
    clear_cache(&ld).await.expect("clear cache");
    assert!(
        ld.pinned_blob_path(&blob.id).unwrap().exists(),
        "a pinned blob survives a cache sweep",
    );

    // Unpin it → moves pinned/ → cache/ (the file stays, now evictable).
    unpin(&ld, std::slice::from_ref(&blob))
        .await
        .expect("unpin demotes the blob");
    assert!(
        ld.cache_blob_path(&blob.id).unwrap().exists(),
        "unpin moves the blob back into storage/cache/<id>",
    );
    assert!(
        !ld.pinned_blob_path(&blob.id).unwrap().exists(),
        "unpin leaves nothing behind in storage/pinned/<id>",
    );

    // Clear the cache again → the now-unpinned blob is gone.
    clear_cache(&ld).await.expect("clear cache");
    assert!(
        !ld.cache_blob_path(&blob.id).unwrap().exists(),
        "an unpinned blob is dropped by a cache sweep",
    );

    // Unpinning a Mirrored blob is rejected: its system pin isn't user-removable.
    let mirrored = blob_ref("mir-aaaa", "images", BlobSync::Mirrored);
    let err = unpin(&ld, std::slice::from_ref(&mirrored))
        .await
        .expect_err("unpinning a Mirrored blob must be rejected");
    assert!(
        err.to_string().contains("Mirrored"),
        "the rejection names the Mirrored class: {err}",
    );
}

/// An OnDemand blob is NOT downloaded on pull (no file in either folder afterward),
/// and a later read fetches it into the cache on first access.
#[tokio::test]
async fn on_demand_fetches_on_first_read() {
    let storage = MockSyncStorage::new();

    // Source dev1: a note + a photo row the puller's source treats as OnDemand.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('aud01234', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let dst = tempfile::tempdir().expect("dst");
    let plan = OnDemandSource {
        dir: dst.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &plan).await;

    // The row applied, but the OnDemand blob is in neither folder — pull skipped it.
    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert!(
        !ld.pinned_blob_path("aud01234").unwrap().exists()
            && !ld.cache_blob_path("aud01234").unwrap().exists(),
        "an OnDemand blob is not fetched on pull — neither folder holds it",
    );

    // Now put it in the cloud and read it: the first read fetches into the cache.
    let bytes = b"AUDIO-PAYLOAD".to_vec();
    put_cloud_blob(&storage, "aud01234", "audio", &bytes).await;
    let blob = blob_ref("aud01234", "audio", BlobSync::OnDemand);
    let got = read_blob(&db2, &ld, &storage, &blob)
        .await
        .expect("first read fetches the OnDemand blob");
    assert_eq!(got, bytes);
    assert!(
        ld.cache_blob_path("aud01234").unwrap().exists(),
        "the on-demand fetch populates storage/cache/<id>",
    );
}

/// `write_blob` stages host bytes straight into the cache (`cache/<id>`), and a
/// later `pin` promotes them by renaming — with NO cloud fetch. The cloud copy is
/// deleted first, so a pin that tried to fetch would fail: it must not.
#[tokio::test]
async fn write_blob_stages_to_cache_and_pin_needs_no_cloud_fetch() {
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let blob = blob_ref("stg-aaaa", "audio", BlobSync::OnDemand);
    let bytes = b"STAGED-BYTES".to_vec();

    // Stage the bytes into the cache.
    write_blob(&ld, &blob, &bytes)
        .await
        .expect("write_blob stages into the cache");
    assert!(
        ld.cache_blob_path(&blob.id).unwrap().exists(),
        "write_blob writes to storage/cache/<id>",
    );
    assert!(
        !ld.pinned_blob_path(&blob.id).unwrap().exists(),
        "write_blob does not pin — nothing in storage/pinned/<id>",
    );

    // The blob is NOT in the cloud (nothing was ever put there). A pin that fetched
    // would fail; instead it must promote the staged file by renaming it.
    pin(&db, &ld, &storage, std::slice::from_ref(&blob))
        .await
        .expect("pin promotes the staged file without a cloud fetch");
    assert!(
        ld.pinned_blob_path(&blob.id).unwrap().exists(),
        "pin renames the staged blob into storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path(&blob.id).unwrap().exists(),
        "pin moves the staged blob out of storage/cache/<id>",
    );
    // Read it back to confirm the bytes survived the staging + rename intact.
    let got = read_blob(&db, &ld, &storage, &blob)
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

/// A ranged read of a CACHED blob is served from the local plaintext file: after a
/// whole-file read populates `cache/<id>`, the cloud copy is deleted so any cloud
/// fallback would fail, and ranged reads (a mid-file window and an `offset > 0`
/// tail) still return the correct slices — proving they came from disk, not a
/// re-fetch.
#[tokio::test]
async fn ranged_read_of_a_cached_blob_serves_from_the_local_file() {
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let blob = blob_ref("blob-aaaa", "audio", BlobSync::OnDemand);
    let full = ramp(5000);
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &full).await;

    // Populate the cache with the whole file, then remove the cloud copy so a
    // ranged read that tried to fetch would fail.
    read_blob(&db, &ld, &storage, &blob)
        .await
        .expect("whole-file read populates the cache");
    assert!(ld.cache_blob_path(&blob.id).unwrap().exists());
    storage.delete_blob_object("audio", &blob.id).await;

    // A window from the middle of the file.
    let (offset, len) = (1234u64, 1000u64);
    let mid = open_blob_stream(&db, &ld, &storage, &blob, full.len() as u64, offset, len)
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
        &storage,
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
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let blob = blob_ref("blob-bbbb", "audio", BlobSync::OnDemand);
    let full = ramp(5000);
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &full).await;

    // The blob is in neither folder before the read.
    assert!(!ld.pinned_blob_path(&blob.id).unwrap().exists());
    assert!(!ld.cache_blob_path(&blob.id).unwrap().exists());

    let (offset, len) = (2000u64, 1500u64);
    let got = open_blob_stream(&db, &ld, &storage, &blob, full.len() as u64, offset, len)
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
        !ld.pinned_blob_path(&blob.id).unwrap().exists(),
        "a ranged miss must not write storage/pinned/<id>",
    );
    assert!(
        !ld.cache_blob_path(&blob.id).unwrap().exists(),
        "a ranged miss must not write storage/cache/<id> (no truncated cache file)",
    );
}

/// A full `read_blob` still populates the evictable cache (Phase 2 behavior intact,
/// unchanged by adding the ranged path): after one whole-file read, `cache/<id>`
/// exists and a second read is served from it even with the cloud copy gone.
#[tokio::test]
async fn full_read_blob_still_populates_the_cache() {
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let blob = blob_ref("blob-cccc", "audio", BlobSync::OnDemand);
    let bytes = b"WHOLE-FILE-PAYLOAD".to_vec();
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    let first = read_blob(&db, &ld, &storage, &blob)
        .await
        .expect("first whole-file read fetches from the cloud");
    assert_eq!(first, bytes);
    assert!(
        ld.cache_blob_path(&blob.id).unwrap().exists(),
        "a whole-file read populates storage/cache/<id>",
    );

    // Cloud copy gone → the second whole-file read must be a local hit.
    storage.delete_blob_object("audio", &blob.id).await;
    let second = read_blob(&db, &ld, &storage, &blob)
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
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    let full = ramp(1000);

    // Non-cached blob: contract enforced before/within the cloud path.
    let remote = blob_ref("blob-dddd", "audio", BlobSync::OnDemand);
    put_cloud_blob(&storage, &remote.id, &remote.namespace, &full).await;
    assert!(
        open_blob_stream(&db, &ld, &storage, &remote, full.len() as u64, 900, 200)
            .await
            .is_err(),
        "a range past the blob size must error on the cloud path",
    );
    assert!(
        open_blob_stream(&db, &ld, &storage, &remote, full.len() as u64, 500, 0)
            .await
            .expect("zero-length read is not an error")
            .is_empty(),
        "a zero-length read is an empty result on the cloud path",
    );

    // Cached blob: same contract on the local-file path. Populate the cache, drop
    // the cloud copy so only the local path can serve.
    let cached = blob_ref("blob-eeee", "audio", BlobSync::OnDemand);
    put_cloud_blob(&storage, &cached.id, &cached.namespace, &full).await;
    read_blob(&db, &ld, &storage, &cached)
        .await
        .expect("populate the cache");
    storage.delete_blob_object("audio", &cached.id).await;
    assert!(
        open_blob_stream(&db, &ld, &storage, &cached, full.len() as u64, 900, 200)
            .await
            .is_err(),
        "a range past the blob size must error on the local-file path too",
    );
    assert!(
        open_blob_stream(&db, &ld, &storage, &cached, full.len() as u64, 500, 0)
            .await
            .expect("zero-length read is not an error")
            .is_empty(),
        "a zero-length read is an empty result on the local-file path too",
    );
}
