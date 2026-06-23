//! Tests for the device-local blob cache.
//!
//! These drive the real cache free functions over a real [`Database`] and a real
//! temp library directory, with a [`MockSyncStorage`] standing in for the cloud, so
//! a hit/miss and a folder move are exercised against actual files on disk. The
//! load-bearing properties: presence is the file (no table), pinned-ness is which
//! folder (`storage/pinned/` vs `storage/cache/`), and a read serves a local copy
//! without a cloud round-trip.

use std::collections::HashMap;

use super::cache::{
    clear_cache, evict_to_budget, open_blob_stream, pin, read_blob, unpin, write_blob,
};
use crate::blob::{BlobRef, BlobScope, BlobSource, BlobSync, ResolvedScope};
use crate::changeset::RowChange;
use crate::database::Database;
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
    write_blob(&db, &ld, &blob, &bytes)
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

// ---- Eviction (max_cache_size, folder-model) ----

/// Sum the sizes of every file under `storage/cache/` — the same total
/// `evict_to_budget` measures, recomputed from the test side to assert the budget
/// is respected. Walks the shard tree (`cache/{ab}/{cd}/<id>`) and ignores
/// `pinned/` (a sibling root the cache budget never sees).
fn cache_total_bytes(ld: &crate::library_dir::LibraryDir) -> u64 {
    fn sum(dir: &std::path::Path) -> u64 {
        let mut total = 0;
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            // Only an absent cache dir reads as empty; any other read failure is a
            // real fault that must not under-count into a spuriously-passing budget
            // assertion, so it panics rather than returning 0.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
            Err(e) => panic!("read cache dir {}: {e}", dir.display()),
        };
        for entry in entries {
            let entry = entry.expect("read cache dir entry");
            let meta = entry.metadata().expect("stat cache entry");
            if meta.is_dir() {
                total += sum(&entry.path());
            } else {
                total += meta.len();
            }
        }
        total
    }
    sum(&ld.cache_dir())
}

/// Pin a cache file's modification time to a fixed instant so eviction order is
/// deterministic (the cache evicts oldest-mtime first). `secs` is an offset from
/// the unix epoch — smaller is older.
fn set_cache_mtime(ld: &crate::library_dir::LibraryDir, id: &str, secs: u64) {
    let path = ld.cache_blob_path(id).expect("cache path");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open cache file to set mtime");
    file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
        .expect("set cache file mtime");
}

/// Stage `bytes` into `cache/<id>` with a fixed modification time, with NO budget
/// set so the stage itself never evicts. Builds the over-budget cache a later
/// eviction test then trims.
async fn stage_with_mtime(
    db: &Database,
    ld: &crate::library_dir::LibraryDir,
    id: &str,
    bytes: &[u8],
    mtime_secs: u64,
) {
    let blob = blob_ref(id, "audio", BlobSync::OnDemand);
    write_blob(db, ld, &blob, bytes)
        .await
        .expect("stage blob into cache");
    set_cache_mtime(ld, id, mtime_secs);
}

/// Over budget, eviction deletes the OLDEST `cache/` files (by mtime) first and
/// stops once the total is back under the budget: the oldest go, the newest stay,
/// and the summed `cache/` size ends `<= max_cache_size`. Driven by staging files
/// with distinct mtimes (no budget), then setting the budget and running a sweep.
#[tokio::test]
async fn eviction_drops_oldest_cache_files_until_under_budget() {
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();

    // Four 100-byte files, oldest → newest by mtime. No budget yet, so staging does
    // not evict; the cache holds all 400 bytes.
    stage_with_mtime(&db, &ld, "old1aaaa", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "old2bbbb", &[2u8; 100], 2000).await;
    stage_with_mtime(&db, &ld, "new3cccc", &[3u8; 100], 3000).await;
    stage_with_mtime(&db, &ld, "new4dddd", &[4u8; 100], 4000).await;
    assert_eq!(cache_total_bytes(&ld), 400, "all four files are cached");

    // Budget of 250 bytes: the two oldest (200 bytes) must go to bring the total
    // (then 200) under budget; the two newest stay. A bare sweep (`None`) — no file
    // is being protected as just-written here.
    db.set_max_cache_size(250).await.expect("set budget");
    evict_to_budget(&db, &ld, None)
        .await
        .expect("evict to budget");

    assert!(
        !ld.cache_blob_path("old1aaaa").unwrap().exists(),
        "the oldest file is evicted first",
    );
    assert!(
        !ld.cache_blob_path("old2bbbb").unwrap().exists(),
        "the second-oldest file is evicted next",
    );
    assert!(
        ld.cache_blob_path("new3cccc").unwrap().exists(),
        "a newer file survives once the total is back under budget",
    );
    assert!(
        ld.cache_blob_path("new4dddd").unwrap().exists(),
        "the newest file survives",
    );
    assert!(
        cache_total_bytes(&ld) <= 250,
        "the cache is back within budget after eviction",
    );
}

/// A pinned blob is structurally exempt: it lives in `pinned/`, which the budget
/// never walks, so it is never evicted no matter how far over budget the cache is.
/// Here a system-pinned `Mirrored` blob (landed in `pinned/` by a real pull) and a
/// user-pinned `OnDemand` blob both survive a tiny budget with the cache flooded.
#[tokio::test]
async fn a_pinned_blob_is_never_evicted_even_far_over_budget() {
    let storage = MockSyncStorage::new();

    // dev1 records a note + a (master-scoped) photo row; pull on dev2 system-pins
    // the Mirrored blob into `pinned/`.
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

    let dst_photos = tempfile::tempdir().expect("dst photos");
    let plan = PhotoBlobSource {
        dir: dst_photos.path().to_path_buf(),
    };
    let db2 = open_test_db();
    let (_tmp, ld) = temp_library_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &HashMap::new(), &ld, &plan).await;
    assert_eq!(result.changesets_applied, 1);
    assert!(
        ld.pinned_blob_path("mir0aaaa").unwrap().exists(),
        "the Mirrored blob is system-pinned in pinned/",
    );

    // Also user-pin an OnDemand blob into pinned/ (via write_blob → pin).
    let on_demand = blob_ref("usr0bbbb", "audio", BlobSync::OnDemand);
    write_blob(&db2, &ld, &on_demand, &[7u8; 500])
        .await
        .expect("stage on-demand blob");
    pin(&db2, &ld, &storage, std::slice::from_ref(&on_demand))
        .await
        .expect("user-pin the on-demand blob");
    assert!(ld.pinned_blob_path("usr0bbbb").unwrap().exists());

    // Flood the evictable cache, then evict to a tiny budget. The pinned files live
    // in pinned/ — the sweep never touches them.
    stage_with_mtime(&db2, &ld, "junk1ccc", &[1u8; 1000], 1000).await;
    stage_with_mtime(&db2, &ld, "junk2ddd", &[2u8; 1000], 2000).await;
    db2.set_max_cache_size(10).await.expect("set tiny budget");
    evict_to_budget(&db2, &ld, None)
        .await
        .expect("evict to budget");

    assert!(
        ld.pinned_blob_path("mir0aaaa").unwrap().exists(),
        "a system-pinned Mirrored blob survives eviction (it is in pinned/)",
    );
    assert!(
        ld.pinned_blob_path("usr0bbbb").unwrap().exists(),
        "a user-pinned OnDemand blob survives eviction (it is in pinned/)",
    );
    assert!(
        cache_total_bytes(&ld) <= 10,
        "the evictable cache is trimmed to budget, ignoring pinned/",
    );
}

/// With no `max_cache_size` set the cache is unlimited: even a large cache and an
/// explicit eviction sweep leave every file in place. The host opts into a budget;
/// until then nothing is evicted.
#[tokio::test]
async fn unset_max_cache_size_never_evicts() {
    let db = open_test_db();
    let (_tmp, ld) = temp_library_dir();

    stage_with_mtime(&db, &ld, "keep1aaa", &[1u8; 5000], 1000).await;
    stage_with_mtime(&db, &ld, "keep2bbb", &[2u8; 5000], 2000).await;
    stage_with_mtime(&db, &ld, "keep3ccc", &[3u8; 5000], 3000).await;
    assert_eq!(cache_total_bytes(&ld), 15000);

    // No budget set anywhere — an explicit sweep is a no-op.
    evict_to_budget(&db, &ld, None)
        .await
        .expect("evict is a no-op with no budget");
    assert_eq!(
        cache_total_bytes(&ld),
        15000,
        "a big cache stays whole when no budget is set",
    );
    for id in ["keep1aaa", "keep2bbb", "keep3ccc"] {
        assert!(
            ld.cache_blob_path(id).unwrap().exists(),
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
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    // An older cached file (100 bytes, old mtime), no budget yet.
    stage_with_mtime(&db, &ld, "older1aa", &[1u8; 100], 1000).await;

    // Budget of 150 bytes: holds either file alone, not both. Now a read fetches a
    // second 100-byte blob; populating it pushes the total to 200 (> 150), so the
    // read's own eviction must drop the older file (the newest — the one just
    // read — survives).
    db.set_max_cache_size(150).await.expect("set budget");
    let blob = blob_ref("newer2bb", "audio", BlobSync::OnDemand);
    let bytes = vec![2u8; 100];
    put_cloud_blob(&storage, &blob.id, &blob.namespace, &bytes).await;

    let got = read_blob(&db, &ld, &storage, &blob)
        .await
        .expect("read fetches and populates, then evicts to budget");
    assert_eq!(got, bytes, "the triggering read still returns its bytes");
    assert!(
        ld.cache_blob_path("newer2bb").unwrap().exists(),
        "the just-populated (newest) blob survives its own over-budget eviction",
    );
    assert!(
        !ld.cache_blob_path("older1aa").unwrap().exists(),
        "the older blob is the one evicted",
    );
    assert!(
        cache_total_bytes(&ld) <= 150,
        "the cache is back within budget after the read-triggered eviction",
    );
}

/// The budget never drifts over: after a sequence of over-budget populates (each a
/// `read_blob` miss that fetches a new blob), the summed `cache/` size is `<=
/// max_cache_size` every time, because each populate's own sweep trims back to it.
#[tokio::test]
async fn budget_never_drifts_over_across_repeated_populates() {
    let db = open_test_db();
    let storage = MockSyncStorage::new();
    let (_tmp, ld) = temp_library_dir();

    // Budget of 250 bytes; each blob is 100 bytes, so at most two fit.
    db.set_max_cache_size(250).await.expect("set budget");

    for i in 0..6u8 {
        let id = format!("seqr{i:04}"); // ≥4 chars, distinct per i, for the shard
        let bytes = vec![i; 100];
        put_cloud_blob(&storage, &id, "audio", &bytes).await;
        let blob = blob_ref(&id, "audio", BlobSync::OnDemand);

        let got = read_blob(&db, &ld, &storage, &blob)
            .await
            .expect("each read populates then evicts to budget");
        assert_eq!(got, bytes, "each read returns its freshly-fetched bytes");

        // The cache is within budget after every populate, never drifting over as
        // new blobs arrive.
        assert!(
            cache_total_bytes(&ld) <= 250,
            "after populate {i} the cache is within the 250-byte budget",
        );
    }

    // Eviction ran rather than the budget never being reached: with a 250-byte
    // budget and 100-byte blobs, at most two fit, so after six reads the earliest
    // blobs must have been evicted and the just-read last blob must still be present.
    assert!(
        cache_total_bytes(&ld) <= 200,
        "at most two 100-byte blobs remain under the 250-byte budget",
    );
    assert!(
        !ld.cache_blob_path("seqr0000").unwrap().exists(),
        "the first blob read was evicted by later populates",
    );
    assert!(
        ld.cache_blob_path("seqr0005").unwrap().exists(),
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
    let (_tmp, ld) = temp_library_dir();

    // Two 100-byte files. The protected one is OLDER (mtime 1000); the other is
    // NEWER (mtime 2000). A naive oldest-first sweep would evict the protected one.
    stage_with_mtime(&db, &ld, "prot0aaa", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "othr0bbb", &[2u8; 100], 2000).await;

    // Budget of 100 bytes: exactly one file fits, so one must be evicted.
    db.set_max_cache_size(100).await.expect("set budget");
    let protected = ld.cache_blob_path("prot0aaa").unwrap();
    evict_to_budget(&db, &ld, Some(&protected))
        .await
        .expect("evict to budget, protecting the older file");

    assert!(
        ld.cache_blob_path("prot0aaa").unwrap().exists(),
        "the protected file survives even though it is the older by mtime",
    );
    assert!(
        !ld.cache_blob_path("othr0bbb").unwrap().exists(),
        "the newer, unprotected file is the one evicted instead",
    );
    assert!(
        cache_total_bytes(&ld) <= 100,
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
    let (_tmp, ld) = temp_library_dir();

    // A 100-byte protected file plus a 100-byte evictable one. The protected file
    // alone (100 bytes) is larger than the 50-byte budget.
    stage_with_mtime(&db, &ld, "biginuse", &[1u8; 100], 1000).await;
    stage_with_mtime(&db, &ld, "othr0bbb", &[2u8; 100], 2000).await;

    db.set_max_cache_size(50).await.expect("set budget");
    let protected = ld.cache_blob_path("biginuse").unwrap();
    evict_to_budget(&db, &ld, Some(&protected))
        .await
        .expect("eviction returns Ok even when the in-use file alone exceeds budget");

    assert!(
        ld.cache_blob_path("biginuse").unwrap().exists(),
        "the protected in-use file is kept even though it alone exceeds the budget",
    );
    assert!(
        !ld.cache_blob_path("othr0bbb").unwrap().exists(),
        "every other candidate is still evicted",
    );
    assert_eq!(
        cache_total_bytes(&ld),
        100,
        "the cache is left over budget, holding exactly the in-use file",
    );
}
