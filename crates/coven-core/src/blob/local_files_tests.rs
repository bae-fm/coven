//! Tests for the local store ([`super::local_files`]): coven's own copy of a
//! host-provided Local blob. Driven over a real temp store directory so the
//! files-on-disk model is exercised directly.

use super::local_files;
use crate::blob::cache::{evict_to_budget, write_blob};
use crate::blob::{BlobRef, BlobScope, CacheFill, Provenance};
use crate::sync::test_helpers::{open_test_db, temp_store_dir};

/// Store a host-provided blob, read it back whole and ranged, then drop it.
#[tokio::test]
async fn store_read_drop_round_trip() {
    let (_tmp, ld) = temp_store_dir();
    let bytes: Vec<u8> = (0..2000).map(|i| (i % 251) as u8).collect();

    local_files::store(&ld, "covers", "cov0aaaa", &bytes)
        .await
        .expect("store");
    assert!(
        ld.local_blob_path("covers", "cov0aaaa").unwrap().exists(),
        "the bytes land at storage/local/<namespace>/<id>",
    );

    let read = local_files::read(&ld, "covers", "cov0aaaa", bytes.len() as u64)
        .await
        .expect("read");
    assert_eq!(read, Some(bytes.clone()), "the whole blob round-trips");

    let (offset, len) = (500u64, 300u64);
    let ranged =
        local_files::read_range(&ld, "covers", "cov0aaaa", bytes.len() as u64, offset, len)
            .await
            .expect("ranged read");
    assert_eq!(
        ranged,
        Some(bytes[offset as usize..(offset + len) as usize].to_vec()),
        "a ranged read returns the right slice",
    );

    // A blob that isn't stored reads back as None (no error), so a caller falls
    // through to the cache/cloud path.
    assert_eq!(
        local_files::read(&ld, "covers", "absent00", bytes.len() as u64)
            .await
            .unwrap(),
        None,
        "an unstored blob reads back as None",
    );

    let removed = local_files::drop_blob(&ld, "covers", "cov0aaaa")
        .await
        .expect("drop");
    assert!(removed, "drop reports the file was there");
    assert_eq!(
        local_files::read(&ld, "covers", "cov0aaaa", bytes.len() as u64)
            .await
            .unwrap(),
        None,
        "after the drop the blob is gone",
    );
    assert!(
        !local_files::drop_blob(&ld, "covers", "cov0aaaa")
            .await
            .unwrap(),
        "dropping an already-absent blob reports false, not an error",
    );
}

/// A host-provided Local blob survives an `evict_to_budget` sweep no matter how far
/// over budget the cache is: the sweep walks only `storage/cache/`, never the local
/// store under `storage/local/`.
#[tokio::test]
async fn local_store_blob_survives_an_evict_to_budget_sweep() {
    let db = open_test_db();
    let (_tmp, ld) = temp_store_dir();

    // A host-provided blob in the local store.
    let store_bytes = vec![7u8; 5000];
    local_files::store(&ld, "covers", "keep0aaa", &store_bytes)
        .await
        .expect("store the local blob");

    // Flood the evictable cache far past a tiny budget.
    let cache_blob = BlobRef {
        namespace: "audio".to_string(),
        id: "junk0bbb".to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::UserProvided,
        fill: CacheFill::CacheLazy,
    };
    write_blob(
        &db,
        &ld,
        &cache_blob.namespace,
        crate::sync::test_helpers::test_cache_locator_hash(&cache_blob.id),
        &vec![1u8; 4000],
    )
    .await
    .expect("write a cache file");
    db.set_cache_budget("audio", 10)
        .await
        .expect("set a tiny budget");
    evict_to_budget(&db, &ld, "audio", None)
        .await
        .expect("evict");

    assert!(
        ld.local_blob_path("covers", "keep0aaa").unwrap().exists(),
        "the local-store blob survives — the budget sweep never walks storage/local/",
    );
    assert_eq!(
        local_files::read(&ld, "covers", "keep0aaa", store_bytes.len() as u64)
            .await
            .unwrap(),
        Some(store_bytes),
        "and its bytes are intact",
    );
}
