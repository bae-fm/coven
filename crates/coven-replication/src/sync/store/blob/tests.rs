//! Tests for the device-local blob cache.
//!
//! These drive the real cache free functions over a real [`SyntheticStoreFixture`] and a real
//! temp store directory, with a [`TestStore`] standing in for the cloud, so
//! a hit/miss and a folder move are exercised against actual files on disk. The
//! load-bearing properties: presence is the file (no table), pinned-ness is which
//! folder (`storage/pinned/` vs `storage/cache/`), and a read serves a local copy
//! without a cloud round-trip.

use super::cache::BlobCacheError;
use super::StoreBlobCache;
use crate::sync::test_helpers::{
    open_test_db, open_test_db_schema, open_test_db_with_blob,
    open_test_db_with_user_and_host_blobs, photo_decl, read_test_db,
    read_test_db_with_download_limit, remote_root_db, temp_store_dir, test_migrations, TestStore,
    TestStoreFixture,
};
use coven_database::{StoreDatabase, SyntheticStoreFixture};
use coven_protocol::blob::{BlobRef, BlobScope, CacheFill, Provenance};
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::synced_schema::BlobDecl;
use coven_protocol::synced_schema::SyncedTable;
use coven_storage::CloudSyncObjectStorage;

/// The synthetic test db opens with a single migration, so its
/// [`coven_database::Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

trait BlobTestStoreDirOps {
    async fn cache_total_bytes(&self, namespace: &str) -> u64;
}

impl BlobTestStoreDirOps for coven_foundation::store_dir::StoreDir {
    async fn cache_total_bytes(&self, namespace: &str) -> u64 {
        self.cached_blob_files(namespace)
            .await
            .expect("walk the namespace cache subtree")
            .iter()
            .map(|file| file.size())
            .sum()
    }
}

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

fn locator_hash(reference: &coven_protocol::blob::RowBlobRef) -> ObjectHash {
    reference
        .stored()
        .expect("Remote row blob reference has exact storage")
        .locator()
        .locator_hash()
}

fn cache_path(
    store_dir: &coven_foundation::store_dir::StoreDir,
    reference: &coven_protocol::blob::RowBlobRef,
) -> std::path::PathBuf {
    store_dir
        .cache_blob_path(&reference.blob().namespace, locator_hash(reference))
        .expect("build exact cache path")
}

fn pinned_path(
    store_dir: &coven_foundation::store_dir::StoreDir,
    reference: &coven_protocol::blob::RowBlobRef,
) -> std::path::PathBuf {
    store_dir
        .pinned_blob_path(&reference.blob().namespace, locator_hash(reference))
        .expect("build exact pinned path")
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
fn plain_blob_db(decl: BlobDecl) -> SyntheticStoreFixture {
    open_test_db_schema(
        vec![
            SyncedTable::new(
                "notes",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "note_tags",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "note_photos",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            )
            .carries_blob(decl),
        ],
        test_migrations(),
    )
}

async fn create_store(
    db: &SyntheticStoreFixture,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
) -> TestStoreFixture {
    TestStoreFixture::create(
        db,
        "test-store",
        coven_keys::keys::UserKeypair::generate(),
        home,
    )
    .await
    .expect("create exact test Store for the test database")
}

struct ExactRemoteBlobFixture<'a> {
    database: &'a SyntheticStoreFixture,
    store: &'a TestStore,
}

impl<'a> ExactRemoteBlobFixture<'a> {
    fn new(database: &'a SyntheticStoreFixture, store: &'a TestStore) -> Self {
        Self { database, store }
    }

    async fn install(
        &self,
        id: &str,
        namespace: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::RowBlobRef {
        self.install_for_row("note_photos", id, namespace, bytes)
            .await
    }

    async fn install_for_row(
        &self,
        table: &str,
        id: &str,
        namespace: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::RowBlobRef {
        self.bind_for_row(table, id, namespace, bytes).await;
        coven_database::StoreDatabase::new(&self.database.database)
            .row_blob_ref(table, id)
            .await
            .expect("load exact row blob reference")
    }

    async fn bind_for_row(&self, table: &str, id: &str, namespace: &str, bytes: &[u8]) {
        let source_db = open_test_db();
        let changeset = source_db
            .database
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
              VALUES ('cache-owner', 'owner', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            ])
            .await;
        let device = self
            .store
            .founder_device()
            .await
            .expect("retain exact test producer");
        let sequence = device
            .latest_local_store_position()
            .await
            .expect("load exact test producer position")
            .map_or(1, |reference| reference.coord.sequence() + 1);
        let owner = self
            .store
            .publish_changeset("founder", sequence, &changeset, SCHEMA_VERSION)
            .await
            .expect("publish exact blob owner commit");
        self.bind_for_row_with_owner(table, id, namespace, bytes, owner)
            .await;
    }

    async fn bind_for_row_with_owner(
        &self,
        table: &str,
        id: &str,
        namespace: &str,
        bytes: &[u8],
        owner: coven_protocol::store_commit::StoreBatchCommitRef,
    ) {
        let stored = self
            .store
            .create_exact_opaque_blob(namespace, id, bytes)
            .await;
        self.database
            .database
            .bind_stored_blob_to_row_for_test(&stored, table, id, owner)
            .await
            .expect("install exact remote blob binding");
    }

    async fn install_many(
        &self,
        count: usize,
    ) -> (Vec<coven_protocol::blob::RowBlobRef>, Vec<Vec<u8>>) {
        let mut blobs = Vec::new();
        let mut all_bytes = Vec::new();
        for i in 0..count {
            let blob = blob_ref(&format!("pinc{i:04}"), "audio", CacheFill::CacheLazy);
            let bytes: Vec<u8> = (0..1000u32).map(|x| ((x + i as u32) % 251) as u8).collect();
            self.database
                .database
                .plant_blob_row_for_test(&blob.id, true, &bytes)
                .await;
            blobs.push(self.install(&blob.id, &blob.namespace, &bytes).await);
            all_bytes.push(bytes);
        }
        (blobs, all_bytes)
    }
}

#[tokio::test]
async fn materialize_row_blob_publishes_the_exact_locator_without_replacement() {
    let db = remote_root_db(photo_decl());
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, store_dir) = temp_store_dir();
    let bytes = b"exact materialized plaintext";
    db.database
        .plant_blob_row_for_test("materialized-row", true, bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install("materialized-row", "photos", bytes)
        .await;
    let locator_hash = reference
        .stored()
        .expect("remote row has an exact locator")
        .locator()
        .locator_hash();
    let destination = store_dir
        .cache_blob_path("photos", locator_hash)
        .expect("exact cache path");

    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .materialize_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("materialize exact remote row blob");
    assert_eq!(
        std::fs::read(&destination).expect("read materialization"),
        bytes
    );

    let corrupt = vec![b'!'; bytes.len()];
    std::fs::write(&destination, &corrupt).expect("replace fixture with same-length corruption");
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .materialize_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("occupied corrupt materialization must fail");
    assert_eq!(
        std::fs::read(&destination).expect("read occupied destination"),
        corrupt,
        "materialization never replaces an occupied exact path",
    );
}

#[tokio::test]
async fn materialize_row_blob_rejects_same_length_corruption_in_pinned_cache() {
    let db = remote_root_db(photo_decl());
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, store_dir) = temp_store_dir();
    let bytes = b"exact pinned plaintext";
    db.database
        .plant_blob_row_for_test("pinned-corruption", true, bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install("pinned-corruption", "photos", bytes)
        .await;

    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .pin_blobs(
        Some(cloud_storage.clone()),
        std::slice::from_ref(&reference),
    )
    .await
    .expect("pin the exact locator");
    let destination = pinned_path(&store_dir, &reference);
    let corrupt = vec![b'!'; bytes.len()];
    std::fs::write(&destination, &corrupt).expect("write same-length pinned corruption");

    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            store_dir.clone()
        )
        .materialize_blob(Some(cloud_storage.clone()), &reference)
        .await,
        Err(BlobCacheError::LocalIntegrity { .. })
    ));
    assert_eq!(
        std::fs::read(&destination).expect("read occupied pinned destination"),
        corrupt,
        "materialization never replaces an occupied pinned path",
    );
}

#[tokio::test]
async fn materialize_row_blob_rejects_a_stale_reference_without_publishing() {
    let db = remote_root_db(photo_decl());
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, store_dir) = temp_store_dir();
    db.database
        .plant_blob_row_for_test("stale-materialized-row", true, b"stale source")
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install("stale-materialized-row", "photos", b"stale source")
        .await;
    let destination = store_dir
        .cache_blob_path(
            "photos",
            reference
                .stored()
                .expect("remote row has an exact locator")
                .locator()
                .locator_hash(),
        )
        .expect("exact cache path");
    StoreDatabase::new(&db.database)
        .replace_blob_row_stamp_for_test(
            "note_photos",
            "stale-materialized-row",
            "0000000002000-0000-dev1",
        )
        .await
        .expect("replace row stamp");

    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .materialize_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("stale row reference must fail");
    assert!(
        !destination.exists(),
        "stale materialization publishes no file"
    );
}

#[tokio::test]
async fn materialize_row_blob_rejects_same_length_corruption_in_local_sources() {
    let (tmp, store_dir) = temp_store_dir();
    let external_db = read_test_db("audio");
    let external_bytes = b"external exact bytes";
    external_db
        .database
        .plant_blob_row_for_test("external-corrupt", false, external_bytes)
        .await;
    let external_path = write_external_file(tmp.path(), "external-corrupt", external_bytes);
    coven_database::StoreDatabase::new(&external_db.database)
        .register_external_blob_for_test("note_photos", "external-corrupt", &external_path)
        .await;
    std::fs::write(&external_path, vec![b'!'; external_bytes.len()])
        .expect("write same-length external corruption");
    let external = external_db
        .database
        .row_blob_ref("note_photos", "external-corrupt")
        .await
        .expect("load external row reference");
    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&external_db.database),
            store_dir.clone()
        )
        .materialize_blob(None, &external)
        .await,
        Err(BlobCacheError::LocalIntegrity { .. })
    ));

    let host_db = open_test_db_with_blob(photo_decl());
    let host_bytes = b"host local exact bytes";
    host_db
        .database
        .plant_blob_row_for_test("host-corrupt", false, host_bytes)
        .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &store_dir,
        "photos",
        "host-corrupt",
        host_bytes,
    )
    .await
    .expect("store host-local source");
    let host_path = store_dir
        .local_blob_path("photos", "host-corrupt")
        .expect("host-local path");
    std::fs::write(&host_path, vec![b'?'; host_bytes.len()])
        .expect("write same-length host-local corruption");
    let host = host_db
        .database
        .row_blob_ref("note_photos", "host-corrupt")
        .await
        .expect("load host-local row reference");
    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&host_db.database),
            store_dir.clone()
        )
        .materialize_blob(None, &host)
        .await,
        Err(BlobCacheError::LocalIntegrity { .. })
    ));
}

#[tokio::test]
async fn two_locators_for_one_logical_id_keep_independent_cache_state() {
    let db = remote_root_db(photo_decl());
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, store_dir) = temp_store_dir();
    let id = "same-logical-id";
    let first_bytes = b"first exact version";
    db.database
        .plant_blob_row_for_test(id, true, first_bytes)
        .await;
    let first = ExactRemoteBlobFixture::new(&db, &storage)
        .install(id, "photos", first_bytes)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .materialize_blob(Some(cloud_storage.clone()), &first)
    .await
    .expect("materialize first exact locator");
    let first_path = cache_path(&store_dir, &first);

    let second_bytes = b"second exact version with different bytes";
    let second_hash = coven_protocol::blob::content_hash(second_bytes);
    let second_size = second_bytes.len() as i64;
    let id_for_update = id.to_string();
    StoreDatabase::new(&db.database)
        .replace_blob_row_facts_for_test(
            "note_photos",
            &id_for_update,
            second_size,
            &second_hash,
            "0000000002000-0000-dev1",
        )
        .await
        .expect("replace logical row with a second exact version");
    let second = ExactRemoteBlobFixture::new(&db, &storage)
        .install(id, "photos", second_bytes)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .materialize_blob(Some(cloud_storage.clone()), &second)
    .await
    .expect("materialize second exact locator");
    let second_cache = cache_path(&store_dir, &second);

    assert_ne!(first_path, second_cache);
    assert_eq!(std::fs::read(&first_path).unwrap(), first_bytes);
    assert_eq!(std::fs::read(&second_cache).unwrap(), second_bytes);
    let store_cache =
        crate::sync::store::blob::StoreBlobCache::new(store_database.clone(), store_dir.clone());
    assert!(store_cache
        .all_pinned(std::slice::from_ref(&first))
        .await
        .is_err());

    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .pin_blobs(Some(cloud_storage.clone()), std::slice::from_ref(&second))
    .await
    .expect("pin only the current exact locator");
    assert!(first_path.exists());
    assert!(pinned_path(&store_dir, &second).exists());

    StoreBlobCache::new(store_database.clone(), store_dir.clone())
        .unpin(std::slice::from_ref(&second))
        .await
        .expect("unpin only the current exact locator");
    assert!(first_path.exists());
    assert!(second_cache.exists());

    store_cache
        .evict(&second)
        .await
        .expect("evict only the current exact locator");
    assert!(first_path.exists());
    assert!(!second_cache.exists());
}

/// A second read is a local hit: the first read populates the exact locator cache from the
/// cloud, and after the cloud copy is deleted the second read still returns the
/// bytes — served from disk, no fetch.
#[tokio::test]
async fn second_read_is_a_local_hit() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-aaaa", "audio", CacheFill::CacheLazy);
    let bytes = b"THE-BLOB-BYTES".to_vec();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;

    // First read misses, fetches from the cloud, and populates the evictable cache.
    let first = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("first read fetches from cloud");
    assert_eq!(first, bytes);
    assert!(
        cache_path(&ld, &reference).exists(),
        "the first read populates the exact locator cache path",
    );

    // Delete the cloud copy so a second fetch would fail: the read must be served
    // from the local file, proving the cache hit, not a re-download.
    cloud_storage
        .clone()
        .delete_blob_object(reference.stored().expect("remote blob has exact storage"))
        .await
        .expect("delete exact remote blob");
    let second = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("second read is served from the local cache");
    assert_eq!(
        second, bytes,
        "the second read returns the cached bytes without touching the cloud",
    );
}

#[tokio::test]
async fn remote_cache_miss_surfaces_invalid_cache_budget_after_population() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, store_dir) = temp_store_dir();
    let id = "budget01";
    let bytes = b"REMOTE-BLOB-WITH-INVALID-BUDGET";
    db.database.plant_blob_row_for_test(id, true, bytes).await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(id, "audio", bytes)
        .await;
    StoreDatabase::new(&db.database)
        .set_invalid_cache_budget_for_test("audio", "invalid")
        .await
        .expect("store invalid cache budget metadata");

    let error = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        store_dir.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("cache budget metadata failure must fail the read");
    assert!(
        matches!(error, BlobCacheError::Metadata(_)),
        "invalid cache budget metadata must remain visible: {error:?}",
    );
    assert!(
        cache_path(&store_dir, &reference).exists(),
        "the verified remote bytes were populated before eviction read its budget",
    );
}

#[tokio::test]
async fn corrupt_cached_remote_blob_fails_without_replacement() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-torn", "audio", CacheFill::CacheLazy);
    let bytes = b"complete remote blob bytes".to_vec();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;

    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("first read populates cache");
    let path = cache_path(&ld, &reference);
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&path, &bytes[..8])
        .await
        .expect("simulate a torn cache file");

    let error = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("corrupt cache file must fail loud");
    assert!(matches!(error, BlobCacheError::LocalIntegrity { .. }));
    assert_eq!(
        tokio::fs::read(&path)
            .await
            .expect("read unchanged corrupt cache"),
        bytes[..8],
        "the occupied cache file is never replaced",
    );
}

/// A cloud object whose stored bytes no longer match its exact reference is rejected
/// before opening or caching it. Every later read keeps failing against the same
/// exact reference, and the tampered object never poisons the plaintext cache.
#[tokio::test]
async fn tampered_exact_cloud_object_errors_and_caches_nothing() {
    let db = read_test_db("audio");
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, home.clone()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-shrt", "audio", CacheFill::CacheLazy);
    let declared = b"the full declared blob bytes".to_vec();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &declared)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &declared)
        .await;
    home.replace_exact_object(
        reference
            .stored()
            .expect("remote blob has exact storage")
            .object()
            .slot(),
        declared[..8].to_vec(),
    );

    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("tampered exact cloud bytes must error");
    assert!(
        matches!(err, BlobCacheError::Storage(_)),
        "exact stored-byte verification remains visible: {err:?}",
    );
    assert!(
        !cache_path(&ld, &reference).exists(),
        "tampered stored bytes write nothing to the exact locator cache path",
    );

    let again = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("subsequent reads keep rejecting the tampered bytes");
    assert!(
        matches!(again, BlobCacheError::Storage(_)),
        "the second read errors the same way, not a cache hit: {again:?}",
    );
}

#[tokio::test]
async fn blob_reads_reuse_schema_models_built_at_open() {
    coven_database::reset_gate_from_tables_call_count();
    coven_database::reset_from_tables_call_count();

    let db = read_test_db("audio");
    assert_eq!(
        coven_database::gate_from_tables_call_count(),
        1,
        "database open builds the gate model once",
    );
    assert_eq!(
        coven_database::from_tables_call_count(),
        1,
        "database open builds the blob declaration model once",
    );

    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    let blob = blob_ref("blob-reuse", "audio", CacheFill::CacheLazy);
    let bytes = ramp(4096);

    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;
    let gate_models_before_reads = coven_database::gate_from_tables_call_count();
    let blob_models_before_reads = coven_database::from_tables_call_count();

    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &reference)
        .await
        .expect("first read fetches from cloud"),
        bytes,
    );
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &reference)
        .await
        .expect("second read serves from cache"),
        bytes,
    );
    let offset = 1024;
    let len = 512;
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len,)
        .await
        .expect("ranged read serves through the cache path"),
        bytes[offset as usize..(offset + len) as usize],
    );

    assert_eq!(
        coven_database::gate_from_tables_call_count(),
        gate_models_before_reads,
        "blob reads reuse the database's gate model",
    );
    assert_eq!(
        coven_database::from_tables_call_count(),
        blob_models_before_reads,
        "blob reads reuse the database's blob declaration model",
    );
}

/// A CacheEager blob pulled in a changeset lands in the EVICTABLE CACHE: its file is
/// in the evictable locator cache, not the kept locator cache. On a peer the release
/// is Remote, so the blob's local copy is a cache copy (evictable + re-fetchable, not
/// pinned). (Driven through the real pull, which routes CacheEager blobs to
/// `download_blobs` → `cache/`.)
#[tokio::test]
async fn cache_eager_lands_in_cache_on_pull() {
    // Source dev1 records a note + a (non-cover, so master-scoped) photo row.
    let db1 = open_test_db_with_blob(photo_decl());
    let TestStoreFixture {
        store: storage,
        storage: _,
    } = create_store(&db1, crate::sync::test_helpers::test_cloud_home()).await;
    storage
        .open_into(&db1)
        .await
        .expect("open source into exact test Store");
    let (_source_tmp, source_store_dir) = temp_store_dir();
    let cover = b"COVERBYTES";
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &source_store_dir,
        "photos",
        "ph01abcd",
        cover,
    )
    .await
    .expect("store source blob");
    db1.database.execute_test_host_write(&format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('ph01abcd', 'n1', 'attach', {}, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            cover.len(),
            coven_protocol::blob::content_hash(cover),
        ),
    )
    .await;
    assert!(
        storage
            .publish_pending(&db1, &source_store_dir)
            .await
            .expect("publish exact source write"),
        "source write publishes a Store commit",
    );

    // The puller declares the photo a CacheEager blob; the pull writes it into the
    // store dir's evictable cache tree.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let reference = db2
        .database
        .row_blob_ref("note_photos", "ph01abcd")
        .await
        .expect("load pulled exact row blob reference");
    assert!(
        cache_path(&ld, &reference).exists(),
        "a CacheEager blob lands in the exact locator cache path on pull",
    );
    assert!(
        !pinned_path(&ld, &reference).exists(),
        "a CacheEager blob does not land pinned",
    );
}

/// Pin promotes a cached blob to `pinned/` and that survives a cache sweep; unpin
/// demotes it back to `cache/` where a sweep then drops it; and unpinning a
/// never-pinned CacheEager blob is a no-op (no system pin to reject anymore — a
/// CacheEager blob lands evictable in the cache on pull).
#[tokio::test]
async fn pin_survives_clear_cache_and_unpin_demotes() {
    let db = read_test_db("audio");
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("ond-aaaa", "audio", CacheFill::CacheLazy);
    let bytes = b"ON-DEMAND-AUDIO".to_vec();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;

    // Read it on demand → it lands in the evictable cache.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("read populates the cache");
    assert!(cache_path(&ld, &reference).exists());
    assert!(!pinned_path(&ld, &reference).exists());

    // Pin it → moves cache/ → pinned/.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .pin_blobs(
        Some(cloud_storage.clone()),
        std::slice::from_ref(&reference),
    )
    .await
    .expect("pin promotes the cached blob");
    assert!(
        pinned_path(&ld, &reference).exists(),
        "pin moves the blob into its exact locator pinned path",
    );
    assert!(
        !cache_path(&ld, &reference).exists(),
        "pin leaves nothing behind in its exact locator cache path",
    );

    // Clear the cache → the pinned blob is untouched.
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .clear_for_test()
        .await
        .expect("clear cache");
    assert!(
        pinned_path(&ld, &reference).exists(),
        "a pinned blob survives a cache sweep",
    );

    // Unpin it → moves pinned/ → cache/ (the file stays, now evictable).
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .unpin(std::slice::from_ref(&reference))
        .await
        .expect("unpin demotes the blob");
    assert!(
        cache_path(&ld, &reference).exists(),
        "unpin moves the blob back into its exact locator cache path",
    );
    assert!(
        !pinned_path(&ld, &reference).exists(),
        "unpin leaves nothing behind in its exact locator pinned path",
    );

    // Clear the cache again → the now-unpinned blob is gone.
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .clear_for_test()
        .await
        .expect("clear cache");
    assert!(
        !cache_path(&ld, &reference).exists(),
        "an unpinned blob is dropped by a cache sweep",
    );

    // Unpinning a never-pinned CacheEager blob is a harmless no-op: it lands evictable
    // in the cache on pull (not system-pinned), so there is nothing in `pinned/` to
    // demote.
    let eager = blob_ref("mir-aaaa", "audio", CacheFill::CacheEager);
    let eager_bytes = b"NEVER-PINNED";
    db.database
        .plant_blob_row_for_test(&eager.id, true, eager_bytes)
        .await;
    let eager_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&eager.id, &eager.namespace, eager_bytes)
        .await;
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .unpin(std::slice::from_ref(&eager_reference))
        .await
        .expect("unpinning a never-pinned CacheEager blob is a no-op");
    assert!(
        !pinned_path(&ld, &eager_reference).exists() && !cache_path(&ld, &eager_reference).exists(),
        "the never-pinned blob is in neither folder",
    );
}

#[tokio::test]
async fn pin_downloads_remote_blob_straight_to_pinned_file() {
    let db = read_test_db("audio");
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, home.clone()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("pin0aaaa", "audio", CacheFill::CacheLazy);
    let bytes: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;

    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .pin_blobs(
        Some(cloud_storage.clone()),
        std::slice::from_ref(&reference),
    )
    .await
    .expect("pin downloads remote blob");

    assert_eq!(
        home.exact_stream_read_count(),
        1,
        "pin writes a cache miss through the file download path",
    );
    assert_eq!(
        std::fs::read(pinned_path(&ld, &reference)).unwrap(),
        bytes,
        "pin writes the remote bytes into the pinned file",
    );
}

// --- pin: bounded concurrency ------------------------------------------------

/// Plant `n` distinct Remote blobs in namespace `audio` (a gated root + child row
/// each) and put their bytes in the mock cloud, returning the refs and the bytes so a
/// pin test can drive the download loop and verify what landed. None are cached, so a
/// pin fetches each through `read_blob_to_file`.
/// At limit 1 the pin loop runs one at a time: every blob is fetched (one at a time) and
/// lands in `pinned/` with its bytes.
#[tokio::test]
async fn pin_at_limit_one_pins_every_blob() {
    let db = read_test_db_with_download_limit("audio", 1);
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, home.clone()).await;
    let (_tmp, ld) = temp_store_dir();

    let (blobs, bytes) = ExactRemoteBlobFixture::new(&db, &storage)
        .install_many(4)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .pin_blobs(Some(cloud_storage.clone()), &blobs)
    .await
    .expect("pin every blob serially");

    assert_eq!(home.exact_stream_read_count(), 4, "every blob is fetched");
    for (reference, want) in blobs.iter().zip(&bytes) {
        assert_eq!(
            std::fs::read(pinned_path(&ld, reference)).unwrap(),
            *want,
            "the blob landed pinned with its bytes",
        );
    }
}

/// At limit 2 the pin loop runs two downloads at once and no more: a barrier that
/// only releases when two `read_blob_to_file` calls gather proves both the
/// concurrency and the bound (a limit of 1 would deadlock it), and every blob lands
/// pinned with its bytes.
#[tokio::test]
async fn pin_runs_downloads_concurrently_up_to_the_limit() {
    let db = read_test_db_with_download_limit("audio", 2);
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, home.clone()).await;
    home.arm_exact_stream_read_concurrency_probe(2);
    let (_tmp, ld) = temp_store_dir();

    // Four blobs over a barrier of two: waves of two must gather to proceed.
    let (blobs, bytes) = ExactRemoteBlobFixture::new(&db, &storage)
        .install_many(4)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .pin_blobs(Some(cloud_storage.clone()), &blobs)
    .await
    .expect("pin runs downloads concurrently");

    assert_eq!(
        home.exact_stream_read_max_inflight(),
        2,
        "exactly two downloads ran at once",
    );
    for (reference, want) in blobs.iter().zip(&bytes) {
        assert_eq!(
            std::fs::read(pinned_path(&ld, reference)).unwrap(),
            *want,
            "every concurrently-fetched blob landed pinned with its bytes",
        );
    }
}

/// A blob whose bytes are missing from the cloud fails the whole pin (the one-at-a-time
/// abort semantics: `pin` returns on the first error), even under concurrency.
#[tokio::test]
async fn pin_mid_batch_failure_surfaces_the_error() {
    let db = read_test_db_with_download_limit("audio", 2);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let (mut blobs, _bytes) = ExactRemoteBlobFixture::new(&db, &storage)
        .install_many(3)
        .await;
    // A blob whose row resolves Remote but whose bytes were never uploaded: no
    // uploader is recorded and no cloud object exists, so its fetch fails.
    let missing = blob_ref("miss0aaa", "audio", CacheFill::CacheLazy);
    db.database
        .plant_blob_row_for_test(&missing.id, true, b"never-uploaded")
        .await;
    let missing_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&missing.id, &missing.namespace, b"never-uploaded")
        .await;
    cloud_storage
        .clone()
        .delete_blob_object(
            missing_reference
                .stored()
                .expect("remote blob has exact storage"),
        )
        .await
        .expect("delete missing blob body");
    blobs.push(missing_reference);

    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .pin_blobs(Some(cloud_storage.clone()), &blobs)
    .await
    .expect_err("a missing blob fails the pin");
    assert!(
        matches!(err, BlobCacheError::Storage(_)),
        "the failure surfaces rather than being pinned around, got {err:?}",
    );
}

/// A CacheLazy blob is NOT downloaded on pull (no file in either folder afterward),
/// and a later read fetches it into the cache on first access.
#[tokio::test]
async fn cache_lazy_fetches_on_first_read() {
    // Source dev1: a note + a photo row the puller's source treats as CacheLazy.
    let declaration = BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy);
    let db1 = open_test_db_with_blob(declaration.clone());
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db1, crate::sync::test_helpers::test_cloud_home()).await;
    storage
        .open_into(&db1)
        .await
        .expect("open source into exact test Store");
    let source_tmp = tempfile::tempdir().expect("create external source directory");
    let bytes = b"AUDIO-PAYLOAD".to_vec();
    let external_path = write_external_file(source_tmp.path(), "audio.bin", &bytes);
    db1.database
        .execute_test_host_write(&format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('aud01234', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            coven_protocol::blob::content_hash(&bytes),
        ))
        .await;
    coven_database::StoreDatabase::new(&db1.database)
        .register_external_blob_for_test("note_photos", "aud01234", &external_path)
        .await;
    storage
        .execute_unscoped_host_sql_for_test(
            "UPDATE notes
         SET shared = 1, _updated_at = '0000000002000-0000-dev1'
         WHERE id = 'n1'",
        )
        .await
        .expect("move the source row into the Store audience");
    assert!(
        storage
            .publish_pending(&db1, &db1.store_dir)
            .await
            .expect("publish exact source write"),
        "source write publishes a Store commit",
    );

    let db2 = open_test_db_with_blob(declaration);
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = storage.pull_into(&db2, &ld).await;

    // The row applied, but the CacheLazy blob is in neither folder — pull skipped it.
    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let reference = db2
        .database
        .row_blob_ref("note_photos", "aud01234")
        .await
        .expect("load pulled exact row blob reference");
    assert!(
        !pinned_path(&ld, &reference).exists() && !cache_path(&ld, &reference).exists(),
        "a CacheLazy blob is not fetched on pull — neither folder holds it",
    );

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db2.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("first read fetches the CacheLazy blob");
    assert_eq!(got, bytes);
    assert!(
        cache_path(&ld, &reference).exists(),
        "the on-demand fetch populates the exact locator cache path",
    );
}

/// `write_blob` writes host bytes straight into the synthetic locator cache, and a
/// later `pin` promotes them by renaming — with NO cloud fetch. The cloud copy is
/// deleted first, so a pin that tried to fetch would fail: it must not.
#[tokio::test]
async fn write_blob_writes_to_cache_and_pin_needs_no_cloud_fetch() {
    let db = read_test_db("audio");
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("stg-aaaa", "audio", CacheFill::CacheLazy);
    let bytes = b"CACHED-BYTES".to_vec();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;
    cloud_storage
        .clone()
        .delete_blob_object(reference.stored().expect("remote blob has exact storage"))
        .await
        .expect("delete exact remote blob");

    // Write the bytes into the cache.
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .populate_bytes_for_test(&blob.namespace, locator_hash(&reference), &bytes)
        .await
        .expect("write_blob writes into the cache");
    assert!(
        cache_path(&ld, &reference).exists(),
        "write_blob writes to the exact locator cache path",
    );
    assert!(
        !pinned_path(&ld, &reference).exists(),
        "write_blob does not pin",
    );

    // The blob is NOT in the cloud (nothing was ever put there). A pin that fetched
    // would fail; instead it must promote the staged file by renaming it.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .pin_blobs(
        Some(cloud_storage.clone()),
        std::slice::from_ref(&reference),
    )
    .await
    .expect("pin promotes the staged file without a cloud fetch");
    assert!(
        pinned_path(&ld, &reference).exists(),
        "pin promotes the staged blob into its exact locator pinned path",
    );
    assert!(
        !cache_path(&ld, &reference).exists(),
        "pin removes the staged blob from its exact locator cache path",
    );
    // Read it back to confirm the bytes survived the staging + rename intact.
    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
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
/// whole-file read populates the exact locator cache, the cloud copy is deleted so any cloud
/// fallback would fail, and ranged reads (a mid-file window and an `offset > 0`
/// tail) still return the correct slices — proving they came from disk, not a
/// re-fetch.
#[tokio::test]
async fn ranged_read_of_a_cached_blob_serves_from_the_local_file() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-aaaa", "audio", CacheFill::CacheLazy);
    let full = ramp(5000);
    db.database
        .plant_blob_row_for_test(&blob.id, true, &full)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &full)
        .await;

    // Populate the cache with the whole file, then remove the cloud copy so a
    // ranged read that tried to fetch would fail.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("whole-file read populates the cache");
    assert!(cache_path(&ld, &reference).exists());
    cloud_storage
        .clone()
        .delete_blob_object(reference.stored().expect("remote blob has exact storage"))
        .await
        .expect("delete exact remote blob");

    // A window from the middle of the file.
    let (offset, len) = (1234u64, 1000u64);
    let mid = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
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
    let tail = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, tail_off, tail_len)
    .await
    .expect("tail ranged read served from the local file");
    assert_eq!(
        tail,
        &full[tail_off as usize..],
        "the tail window matches the plaintext slice",
    );
}

/// A host holds one stream for as long as it is reading that blob, which means
/// across await points and, for a media host filling a buffer, across tasks.
#[test]
fn a_blob_stream_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<crate::sync::BlobStream>();
}

/// The cost contract of the stream, measured at the cloud: ranges off an
/// uncached Remote stream fetch only the chunks they cover — the object is never
/// downloaded, not per range and not once per stream. A media host probing a
/// codec header issues ~20 small ranges to start one track, so anything that
/// reads the whole object makes starting playback cost the blob's size.
///
/// The counter is the discriminator. Asserting only that the bytes are correct
/// passes either way — a whole-object fetch serves correct bytes too.
#[tokio::test]
async fn ranges_off_an_uncached_stream_never_download_the_object() {
    let db = read_test_db("audio");
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, home.clone()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-once", "audio", CacheFill::CacheLazy);
    let full = ramp(5000);
    db.database
        .plant_blob_row_for_test(&blob.id, true, &full)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &full)
        .await;

    // The blob is in neither cache folder, so the stream reads the cloud object.
    assert!(!pinned_path(&ld, &reference).exists());
    assert!(!cache_path(&ld, &reference).exists());

    let whole_reads_before = (home.exact_stream_read_count(), home.exact_full_read_count());
    let stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .open_blob_stream(Some(cloud_storage.clone()), &reference)
    .await
    .expect("open a stream over a non-cached Remote blob");
    assert_eq!(
        stream.plaintext_size(),
        full.len() as u64,
        "the stream reports the blob's plaintext length",
    );

    // Every range a codec probe would ask for, including a 1-byte read and the
    // tail — the shape that made the regression visible.
    let windows = [
        (0u64, 64u64),
        (64, 1024),
        (2000, 1500),
        (4999, 1),
        (4000, 1000),
        (0, 5000),
    ];
    for (offset, len) in windows {
        let got = stream
            .read_at(offset, len)
            .await
            .unwrap_or_else(|e| panic!("read {len} bytes at {offset} from the stream: {e}"));
        assert_eq!(
            got,
            &full[offset as usize..(offset + len) as usize],
            "the stream serves the plaintext slice at {offset}..{}",
            offset + len,
        );
    }

    assert_eq!(
        (home.exact_stream_read_count(), home.exact_full_read_count(),),
        whole_reads_before,
        "{} ranges off one opened stream fetched no whole object",
        windows.len(),
    );
    assert!(
        !home.exact_range_reads().is_empty(),
        "the ranges were served by ranged requests, not from somewhere else",
    );
}

/// The receipt for the browsable carve-out. A home that stores plaintext in the
/// clear has no per-chunk tags, so a range read there could not tell a tampered
/// object from a real one. Ranged reading is refused for those blobs and the
/// stream materializes the whole blob instead — which is where the row's content
/// hash still applies, and this proves it does rather than asserting it in prose.
///
/// Same-length tampering, so only a content check can catch it: a length check
/// would pass.
#[tokio::test]
async fn a_stream_over_a_tampered_browsable_blob_is_refused() {
    const CLOUD_PATH: &str = "Artist/Album/track.flac";
    // A browsable blob is keyed at a readable cloud path, so its declaration
    // names the column holding one. `kind` carries it for this table.
    let db = open_test_db_with_blob(
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy)
            .with_cloud_path_column("kind"),
    );
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = TestStoreFixture::create_browsable(
        &db,
        "browsable-store",
        coven_keys::keys::UserKeypair::generate(),
        home.clone(),
    )
    .await
    .expect("create a browsable test Store");
    let (_tmp, ld) = temp_store_dir();

    let full = ramp(5000);
    db.database
        .insert_browsable_blob_row_for_test("browsable-track", CLOUD_PATH, &full)
        .await
        .expect("plant the browsable blob row");
    let source_db = open_test_db();
    let changeset = source_db
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
          VALUES ('browsable-owner', 'owner', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let device = storage
        .founder_device()
        .await
        .expect("retain browsable test producer");
    let sequence = device
        .latest_local_store_position()
        .await
        .expect("load the browsable test producer position")
        .map_or(1, |reference| reference.coord.sequence() + 1);
    let owner = storage
        .publish_changeset("founder", sequence, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish the browsable blob owner commit");
    let stored = storage
        .create_exact_browsable_blob("audio", "browsable-track", CLOUD_PATH, &full)
        .await;
    db.database
        .bind_stored_blob_to_row_for_test(&stored, "note_photos", "browsable-track", owner)
        .await
        .expect("install exact browsable blob binding");
    let reference = db
        .database
        .row_blob_ref("note_photos", "browsable-track")
        .await
        .expect("load the browsable row blob reference");

    // Untampered, the stream serves ranges — through the whole-object path, so
    // the cache is populated (unlike a sealed blob's ranged read).
    let stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .open_blob_stream(Some(cloud_storage.clone()), &reference)
    .await
    .expect("open a stream over a browsable blob");
    assert_eq!(
        stream.read_at(100, 50).await.expect("serve a range"),
        &full[100..150]
    );
    assert!(
        cache_path(&ld, &reference).exists(),
        "a browsable blob takes the materializing path, which populates the cache",
    );

    // Now tamper the stored object with same-length bytes and read it fresh.
    StoreBlobCache::new(StoreDatabase::new(&db.database), ld.clone())
        .clear_for_test()
        .await
        .expect("drop the populated cache");
    home.replace_exact_object(stored.object().slot(), vec![b'!'; full.len()]);
    let refused = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .open_blob_stream(Some(cloud_storage.clone()), &reference)
    .await
    .err()
    .expect("a tampered browsable object must not open a stream");
    assert!(
        matches!(
            refused,
            BlobCacheError::Storage(_) | BlobCacheError::LocalIntegrity { .. }
        ),
        "the whole-object path refuses the tampered bytes: {refused:?}",
    );
    assert!(
        !cache_path(&ld, &reference).exists(),
        "refused bytes are never published into the cache",
    );
}

/// A stream over a Remote cache miss does **not** populate the cache: populating
/// means downloading the whole object, which is the cost the ranged read exists
/// to remove — a stream that asks for a kilobyte must not pay for the blob. A
/// whole [`read_blob`] still populates, because it reads every byte anyway.
#[tokio::test]
async fn opening_a_stream_over_a_remote_miss_leaves_the_cache_alone() {
    let db = read_test_db("audio");
    let home = crate::sync::test_helpers::test_cloud_home();
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, home.clone()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-bbbb", "audio", CacheFill::CacheLazy);
    let full = ramp(5000);
    db.database
        .plant_blob_row_for_test(&blob.id, true, &full)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &full)
        .await;

    assert!(!pinned_path(&ld, &reference).exists());
    assert!(!cache_path(&ld, &reference).exists());

    let (offset, len) = (2000u64, 1500u64);
    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
    .await
    .expect("open a stream over a cache miss and read a range");
    assert_eq!(
        got,
        &full[offset as usize..(offset + len) as usize],
        "the fetched range matches the plaintext slice",
    );

    assert!(
        !pinned_path(&ld, &reference).exists() && !cache_path(&ld, &reference).exists(),
        "a ranged read populates neither cache folder — it never held the whole blob",
    );

    // The whole read is the operation that populates, and after it a stream over
    // the same blob is served from the cache file with no cloud read at all.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("a whole read of the same blob");
    assert!(cache_path(&ld, &reference).exists());
    cloud_storage
        .clone()
        .delete_blob_object(reference.stored().expect("remote blob has exact storage"))
        .await
        .expect("delete exact remote blob");
    let reads_before = home.exact_range_reads().len();
    let second = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
    .await
    .expect("a stream over the populated cache");
    assert_eq!(second, &full[offset as usize..(offset + len) as usize]);
    assert_eq!(
        home.exact_range_reads().len(),
        reads_before,
        "a stream opened over a cache hit reads nothing from the cloud",
    );
}

/// A full `read_blob` populates the evictable cache: after one whole-file read, the exact cache path
/// exists and a second read is served from it even with the cloud copy gone.
#[tokio::test]
async fn full_read_blob_still_populates_the_cache() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("blob-cccc", "audio", CacheFill::CacheLazy);
    let bytes = b"WHOLE-FILE-PAYLOAD".to_vec();
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;

    let first = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("first whole-file read fetches from the cloud");
    assert_eq!(first, bytes);
    assert!(
        cache_path(&ld, &reference).exists(),
        "a whole-file read populates the exact locator cache path",
    );

    // Cloud copy gone → the second whole-file read must be a local hit.
    cloud_storage
        .clone()
        .delete_blob_object(reference.stored().expect("remote blob has exact storage"))
        .await
        .expect("delete exact remote blob");
    let second = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("second whole-file read is served from the cache");
    assert_eq!(second, bytes);
}

/// The ranged contract is pinned and identical however the stream was opened: an
/// `offset + len` past the blob's plaintext size is an error (never a short read),
/// and a zero-length read is an empty result (never an error) — checked for a blob
/// whose stream opened over a cache hit and one whose stream opened over a cloud
/// miss.
#[tokio::test]
async fn ranged_read_out_of_range_errors_and_zero_len_is_empty_on_both_paths() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let full = ramp(1000);

    // Non-cached blob: the stream opens over a cloud miss, then bounds the range
    // against the plaintext size it proved.
    let remote = blob_ref("blob-dddd", "audio", CacheFill::CacheLazy);
    db.database
        .plant_blob_row_for_test(&remote.id, true, &full)
        .await;
    let remote_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&remote.id, &remote.namespace, &full)
        .await;
    assert!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob_range(Some(cloud_storage.clone()), &remote_reference, 900, 200)
        .await
        .is_err(),
        "a range past the blob size must error on the cloud path",
    );
    assert!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob_range(Some(cloud_storage.clone()), &remote_reference, 500, 0)
        .await
        .expect("zero-length read is not an error")
        .is_empty(),
        "a zero-length read is an empty result on the cloud path",
    );

    // Cached blob: same contract on the local-file path. Populate the cache, drop
    // the cloud copy so only the local path can serve.
    let cached = blob_ref("blob-eeee", "audio", CacheFill::CacheLazy);
    db.database
        .plant_blob_row_for_test(&cached.id, true, &full)
        .await;
    let cached_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&cached.id, &cached.namespace, &full)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &cached_reference)
    .await
    .expect("populate the cache");
    cloud_storage
        .clone()
        .delete_blob_object(
            cached_reference
                .stored()
                .expect("remote blob has exact storage"),
        )
        .await
        .expect("delete exact remote blob");
    assert!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob_range(Some(cloud_storage.clone()), &cached_reference, 900, 200)
        .await
        .is_err(),
        "a range past the blob size must error on the local-file path too",
    );
    assert!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob_range(Some(cloud_storage.clone()), &cached_reference, 500, 0)
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
    let TestStoreFixture {
        store: _,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (tmp, ld) = temp_store_dir();

    let blob = blob_ref("extr-aaaa", "audio", CacheFill::CacheLazy);
    // Local + user-provided: the gate dispatches to the external file.
    let full = ramp(5000);
    db.database
        .plant_blob_row_for_test(&blob.id, false, &full)
        .await;
    let path = write_external_file(tmp.path(), "song.flac", &full);

    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &blob.id, &path)
        .await;
    let reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load Local row blob reference");

    let whole = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("read serves the external file (no cloud copy exists)");
    assert_eq!(
        whole, full,
        "the whole read returns the external file's bytes"
    );

    let (offset, len) = (1234u64, 1000u64);
    let mid = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
    .await
    .expect("ranged read off the external file");
    assert_eq!(
        mid,
        &full[offset as usize..(offset + len) as usize],
        "the ranged read returns the correct slice of the external file",
    );

    assert!(
        !ld.pinned_blob_path(
            &blob.namespace,
            crate::sync::test_helpers::test_cache_locator_hash(&blob.id)
        )
        .unwrap()
        .exists()
            && !ld
                .cache_blob_path(
                    &blob.namespace,
                    crate::sync::test_helpers::test_cache_locator_hash(&blob.id)
                )
                .unwrap()
                .exists(),
        "an external read populates neither cache folder",
    );
}

/// The exactness contract of the stream: it holds the open file it proved, so the
/// bytes it serves cannot be swapped out from under it. Whatever happens to the
/// *name* after the stream opens — unlinked, or atomically replaced with
/// same-length different bytes — every range still serves the plaintext the row
/// named when the stream opened.
///
/// This is also the receipt that identity is proved exactly **once** per stream.
/// A per-range re-open by path cannot survive its path being removed, so ranges
/// that keep working after the file is gone are ranges that never re-opened it.
#[tokio::test]
async fn a_stream_serves_proven_bytes_after_its_file_is_unlinked_or_replaced() {
    let (tmp, ld) = temp_store_dir();
    let full = ramp(5000);
    let (offset, len) = (1234u64, 1000u64);
    let expected = &full[offset as usize..(offset + len) as usize];
    let decoy = vec![b'!'; full.len()];

    // Local + user-provided: the user's own external file, unlinked after open.
    let external_db = read_test_db("audio");
    external_db
        .database
        .plant_blob_row_for_test("strm-ext1", false, &full)
        .await;
    let external_path = write_external_file(tmp.path(), "strm-ext1.flac", &full);
    coven_database::StoreDatabase::new(&external_db.database)
        .register_external_blob_for_test("note_photos", "strm-ext1", &external_path)
        .await;
    let external = external_db
        .database
        .row_blob_ref("note_photos", "strm-ext1")
        .await
        .expect("load external row reference");
    let external_stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&external_db.database),
        ld.clone(),
    )
    .open_blob_stream(None, &external)
    .await
    .expect("open a stream over the external file");

    std::fs::remove_file(&external_path).expect("unlink the external file after open");
    assert_eq!(
        external_stream
            .read_at(offset, len)
            .await
            .expect("the stream reads on after its path is gone"),
        expected,
        "an unlinked path cannot take the bytes the stream already proved",
    );

    // Local + host-provided: the local store, atomically replaced after open with
    // same-length different bytes — the temp-then-rename shape every coven publish
    // and every download uses, which a per-range re-open by name would follow.
    let host_db = open_test_db_with_blob(photo_decl());
    host_db
        .database
        .plant_blob_row_for_test("strm-hst1", false, &full)
        .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "strm-hst1", &full)
        .await
        .expect("store the host-provided local source");
    let host_path = ld
        .local_blob_path("photos", "strm-hst1")
        .expect("host-local path");
    let host = host_db
        .database
        .row_blob_ref("note_photos", "strm-hst1")
        .await
        .expect("load host-local row reference");
    let host_stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&host_db.database),
        ld.clone(),
    )
    .open_blob_stream(None, &host)
    .await
    .expect("open a stream over the local store");

    coven_foundation::local_file::AtomicStagedFile::write_for_test(&host_path, &decoy)
        .await
        .expect("atomically replace the local-store file after open");
    assert_eq!(
        std::fs::read(&host_path).expect("read the replaced path"),
        decoy,
        "the name now resolves to the decoy",
    );
    assert_eq!(
        host_stream
            .read_at(offset, len)
            .await
            .expect("the stream reads on after its path is replaced"),
        expected,
        "a replaced path cannot redirect a stream to bytes it never proved",
    );

    // Remote: the cache copy, evicted after open. Eviction moves and deletes cache
    // *names*; a stream holds the file, so a sweep cannot break a live read.
    let remote_db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&remote_db, crate::sync::test_helpers::test_cloud_home()).await;
    remote_db
        .database
        .plant_blob_row_for_test("strm-rem1", true, &full)
        .await;
    let remote = ExactRemoteBlobFixture::new(&remote_db, &storage)
        .install("strm-rem1", "audio", &full)
        .await;
    let remote_stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&remote_db.database),
        ld.clone(),
    )
    .open_blob_stream(Some(cloud_storage.clone()), &remote)
    .await
    .expect("open a stream over a Remote blob");

    StoreBlobCache::new(StoreDatabase::new(&remote_db.database), ld.clone())
        .clear_for_test()
        .await
        .expect("evict the whole cache");
    assert!(
        !cache_path(&ld, &remote).exists(),
        "the cache file the stream opened is gone from disk",
    );
    assert_eq!(
        remote_stream
            .read_at(offset, len)
            .await
            .expect("the stream reads on after its cache file is evicted"),
        expected,
        "eviction cannot take the bytes a live stream is serving",
    );
}

/// A local source answers a read with its current bytes. The stream holds the
/// file it opened, so replacing the *path* cannot redirect a range, but a
/// rewrite of that same file is what the file now says and is what the stream
/// serves. There is no per-range check to catch it and none is wanted: a blob's
/// bytes are checked against the hash its row declares at publication, where
/// they become canonical synced content, and a read is a read.
///
/// A whole `read_blob` still re-checks, because reading the whole blob is what
/// makes the check free — it reads every byte either way.
#[tokio::test]
async fn a_local_stream_serves_the_file_s_current_bytes() {
    let (tmp, ld) = temp_store_dir();
    let full = ramp(5000);
    let (offset, len) = (1234u64, 1000u64);

    let db = read_test_db("audio");
    db.database
        .plant_blob_row_for_test("strm-ext2", false, &full)
        .await;
    let path = write_external_file(tmp.path(), "strm-ext2.flac", &full);
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", "strm-ext2", &path)
        .await;
    let reference = db
        .database
        .row_blob_ref("note_photos", "strm-ext2")
        .await
        .expect("load external row reference");

    let stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .open_blob_stream(None, &reference)
    .await
    .expect("open a stream over the external file");
    assert_eq!(
        stream
            .read_at(offset, len)
            .await
            .expect("the first range is served from the opened file"),
        &full[offset as usize..(offset + len) as usize],
    );

    // The user rewrites their own file in place: same inode, same length.
    let rewritten = vec![b'!'; full.len()];
    std::fs::write(&path, &rewritten).expect("rewrite the external file in place");

    assert_eq!(
        stream
            .read_at(offset, len)
            .await
            .expect("a range after the rewrite reads the held file"),
        &rewritten[offset as usize..(offset + len) as usize],
    );

    // The whole read reads every byte anyway, so it still checks them.
    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob(None, &reference)
        .await,
        Err(BlobCacheError::LocalIntegrity { .. })
    ));
}

/// The receipt that a local stream costs its ranges and nothing else: a stream
/// opens over a local file whose bytes no longer match the row's declared hash,
/// and serves ranges of it. Under a design that proved the whole file at open,
/// this open failed — proving means reading every byte, which is exactly the
/// scan the stream exists to avoid. Integrity has one home, and it is
/// publication, not the read path.
///
/// The length is still checked for an external file: it is the registered fact
/// the ref carries, and a range is bounded by it.
#[tokio::test]
async fn a_local_stream_opens_over_a_file_that_no_longer_matches_its_row() {
    let (tmp, ld) = temp_store_dir();
    let full = ramp(5000);
    let corrupt = vec![b'!'; full.len()];

    let external_db = read_test_db("audio");
    external_db
        .database
        .plant_blob_row_for_test("strm-ext3", false, &full)
        .await;
    let external_path = write_external_file(tmp.path(), "strm-ext3.flac", &full);
    coven_database::StoreDatabase::new(&external_db.database)
        .register_external_blob_for_test("note_photos", "strm-ext3", &external_path)
        .await;
    std::fs::write(&external_path, &corrupt).expect("write same-length external corruption");
    let external = external_db
        .database
        .row_blob_ref("note_photos", "strm-ext3")
        .await
        .expect("load external row reference");
    let stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&external_db.database),
        ld.clone(),
    )
    .open_blob_stream(None, &external)
    .await
    .expect("a local stream opens without reading the file's content");
    assert_eq!(
        stream.read_at(100, 50).await.expect("serve a range"),
        &corrupt[100..150],
    );
    // The whole read still refuses: it reads every byte, so it checks them.
    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&external_db.database),
            ld.clone()
        )
        .read_blob(None, &external)
        .await,
        Err(BlobCacheError::LocalIntegrity { .. })
    ));

    let host_db = open_test_db_with_blob(photo_decl());
    host_db
        .database
        .plant_blob_row_for_test("strm-hst3", false, &full)
        .await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&ld, "photos", "strm-hst3", &full)
        .await
        .expect("store the host-provided local source");
    let host_path = ld
        .local_blob_path("photos", "strm-hst3")
        .expect("host-local path");
    std::fs::write(&host_path, vec![b'?'; full.len()])
        .expect("write same-length host-local corruption");
    let host = host_db
        .database
        .row_blob_ref("note_photos", "strm-hst3")
        .await
        .expect("load host-local row reference");
    let stream = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&host_db.database),
        ld.clone(),
    )
    .open_blob_stream(None, &host)
    .await
    .expect("a local-store stream opens the same way");
    assert_eq!(stream.read_at(0, 4).await.expect("serve a range"), b"????");
    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&host_db.database),
            ld.clone()
        )
        .read_blob(None, &host)
        .await,
        Err(BlobCacheError::LocalIntegrity { .. })
    ));
}

/// An external file whose length no longer matches its registered size is
/// refused at open: the length is the ref's own recorded fact and every range is
/// bounded by it.
#[tokio::test]
async fn a_relengthened_external_file_is_refused_at_open() {
    let (tmp, ld) = temp_store_dir();
    let full = ramp(5000);

    let db = read_test_db("audio");
    db.database
        .plant_blob_row_for_test("strm-ext4", false, &full)
        .await;
    let path = write_external_file(tmp.path(), "strm-ext4.flac", &full);
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", "strm-ext4", &path)
        .await;
    std::fs::write(&path, &full[..full.len() - 1]).expect("truncate the external file");
    let reference = db
        .database
        .row_blob_ref("note_photos", "strm-ext4")
        .await
        .expect("load external row reference");

    assert!(matches!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .open_blob_stream(None, &reference)
        .await,
        Err(BlobCacheError::ExternalSizeMismatch { .. })
    ));
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (tmp, ld) = temp_store_dir();

    let cloud_bytes = b"CLOUD-FALLBACK-BYTES".to_vec();

    // Missing file: a ref pointing at a path that does not exist.
    let missing = blob_ref("extm-aaaa", "audio", CacheFill::CacheLazy);
    let missing_hash = coven_protocol::blob::content_hash(&vec![0; 1234]);
    db.database
        .plant_blob_row_with_facts_for_test(&missing.id, false, 1234, Some(&missing_hash))
        .await;
    storage
        .create_exact_opaque_blob(&missing.namespace, &missing.id, &cloud_bytes)
        .await;
    let missing_path = tmp.path().join("external").join("gone.flac");
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &missing.id, &missing_path)
        .await;
    let missing_reference = db
        .database
        .row_blob_ref("note_photos", &missing.id)
        .await
        .expect("load missing Local row blob reference");
    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &missing_reference)
    .await
    .expect_err("a missing external file is terminal, never a cloud fetch");
    assert!(
        matches!(err, BlobCacheError::ExternalMissing { .. }),
        "a missing external file maps to ExternalMissing: {err:?}",
    );

    // Present file, wrong length: register a size one byte off the real file.
    let mism = blob_ref("exts-aaaa", "audio", CacheFill::CacheLazy);
    storage
        .create_exact_opaque_blob(&mism.namespace, &mism.id, &cloud_bytes)
        .await;
    let actual = ramp(2000);
    let mism_hash = coven_protocol::blob::content_hash(&actual);
    db.database
        .plant_blob_row_with_facts_for_test(
            &mism.id,
            false,
            actual.len() as u64 + 1,
            Some(&mism_hash),
        )
        .await;
    let mism_path = write_external_file(tmp.path(), "wrong-size.flac", &actual);
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &mism.id, &mism_path)
        .await;
    let mism_reference = db
        .database
        .row_blob_ref("note_photos", &mism.id)
        .await
        .expect("load size-mismatched Local row blob reference");
    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &mism_reference)
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (tmp, ld) = temp_store_dir();

    let blob = blob_ref("extc-aaaa", "audio", CacheFill::CacheLazy);
    // Local + user-provided: serves the user's external file.
    let ext_bytes = ramp(1500);
    db.database
        .plant_blob_row_for_test(&blob.id, false, &ext_bytes)
        .await;
    let path = write_external_file(tmp.path(), "owned.flac", &ext_bytes);
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &blob.id, &path)
        .await;
    let local_reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load Local row blob reference");

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &local_reference)
    .await
    .expect("Local + user-provided read serves the external file");
    assert_eq!(
        got, ext_bytes,
        "the registered ref serves the external file"
    );

    // The cloud object's length must match the row's declared size (1500) — the
    // whole-blob read verifies it before serving.
    let cloud_bytes = ramp(1500);
    ExactRemoteBlobFixture::new(&db, &storage)
        .bind_for_row("note_photos", &blob.id, &blob.namespace, &cloud_bytes)
        .await;
    // Install the exact destination before the host write, then atomically clear
    // the Local source and flip the gate. No observer can see a Remote row without
    // its exact locator.
    let note_id = format!("note-{}", blob.id);
    StoreDatabase::new(&db.database)
        .complete_note_blob_transition_to_remote_for_test(local_reference.clone(), note_id)
        .await
        .expect("commit exact Local-to-Remote transition");
    let remote_reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load exact Remote row blob reference");
    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &remote_reference)
    .await
    .expect("after going Remote, the read fetches from the cloud");
    assert_eq!(
        got, cloud_bytes,
        "once Remote the blob resolves through cache/cloud",
    );
    assert!(
        cache_path(&ld, &remote_reference).exists(),
        "the cloud fetch populated the evictable cache",
    );
}

/// A **Local + user-provided** blob reads its external file — and only that. Gate-first
/// dispatch picks the external source from the blob's provenance, so a same-id cache
/// file and a stray same-id local-store file are both ignored (whole and ranged).
#[tokio::test]
async fn local_user_provided_blob_reads_its_external_file_ignoring_decoys() {
    let db = read_test_db("audio");
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: _,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (tmp, ld) = temp_store_dir();

    // Local + user-provided.
    let blob = blob_ref("extp-aaaa", "audio", CacheFill::CacheLazy);
    let ext_bytes = ramp(2048);
    db.database
        .plant_blob_row_for_test(&blob.id, false, &ext_bytes)
        .await;

    // Decoys at the other on-device stores — both must be ignored.
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .populate_bytes_for_test(
            &blob.namespace,
            crate::sync::test_helpers::test_cache_locator_hash(&blob.id),
            b"OWNED-CACHE-BYTES",
        )
        .await
        .expect("write a same-id cache decoy");
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        &blob.namespace,
        &blob.id,
        b"STALE-LOCAL-STORE",
    )
    .await
    .expect("write a same-id local-store decoy");

    // The real bytes: the user's external file.
    let path = write_external_file(tmp.path(), "precedence.flac", &ext_bytes);
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &blob.id, &path)
        .await;
    let reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load Local row blob reference");

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("Local + user-provided read");
    assert_eq!(
        got, ext_bytes,
        "the read serves the external file, not the cache or local-store decoys",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
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
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (tmp, ld) = temp_store_dir();

    // Local + host-provided: its bytes live in the local store, never the cloud.
    let blob = host_blob_ref("res0aaaa", "photos", CacheFill::CacheEager);
    let store_bytes = ramp(2048);
    db.database
        .plant_blob_row_for_test(&blob.id, false, &store_bytes)
        .await;

    // Decoys at every other store — all must be ignored.
    storage
        .create_exact_opaque_blob(&blob.namespace, &blob.id, b"FROM-CLOUD")
        .await;
    StoreBlobCache::new(store_database.clone(), ld.clone())
        .populate_bytes_for_test(
            &blob.namespace,
            crate::sync::test_helpers::test_cache_locator_hash(&blob.id),
            b"FROM-CACHE",
        )
        .await
        .expect("write a same-id cache decoy");
    let ext_bytes = b"FROM-EXTERNAL".to_vec();
    let ext_path = write_external_file(tmp.path(), "res.bin", &ext_bytes);
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &blob.id, &ext_path)
        .await;

    // The real bytes: the host-provided local store.
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        &blob.namespace,
        &blob.id,
        &store_bytes,
    )
    .await
    .expect("store the host-provided local copy");
    let reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load Local row blob reference");

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("Local + host-provided read");
    assert_eq!(
        got, store_bytes,
        "the read serves the local store, not the external/cache/cloud decoys",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("rem0cccc", "audio", CacheFill::CacheLazy);
    let cloud_bytes = ramp(2048);
    db.database
        .plant_blob_row_for_test(&blob.id, true, &cloud_bytes)
        .await;

    // A stale local-store file (a Remote + user-provided read must NOT serve it) and
    // the real cloud copy with distinct bytes.
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        &blob.namespace,
        &blob.id,
        b"STALE-LOCAL-STORE",
    )
    .await
    .expect("write a stale local-store file");
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &cloud_bytes)
        .await;

    let whole = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("Remote read fetches from the cloud");
    assert_eq!(
        whole, cloud_bytes,
        "a Remote + user-provided read serves the cloud copy, not the stale local-store file",
    );
    assert!(
        cache_path(&ld, &reference).exists(),
        "the Remote read populated the evictable cache",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    let blob = blob_ref("rem0dddd", "audio", CacheFill::CacheLazy);
    let remote_bytes = b"REMOTE-CLOUD-BYTES";

    db.database
        .plant_blob_row_for_test(&blob.id, true, remote_bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, remote_bytes)
        .await;
    cloud_storage
        .clone()
        .delete_blob_object(reference.stored().expect("remote blob has exact storage"))
        .await
        .expect("delete exact remote blob");
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        &blob.namespace,
        &blob.id,
        b"STALE-LOCAL-STORE",
    )
    .await
    .expect("write a stale local-store file");

    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(None, &reference)
    .await
    .expect_err("Remote + user-provided read needs cache or cloud");
    assert!(
        matches!(err, BlobCacheError::NoCloudHome),
        "a stale local-store file does not satisfy a Remote + user-provided read: {err:?}",
    );

    let range_err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(None, &reference, 0, 5)
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    let blob = host_blob_ref("rrrr0001", "photos", CacheFill::CacheEager);

    let cloud_bytes = b"REMOTE-ROOT-CLOUD";
    db.database
        .plant_blob_row_for_test(&blob.id, false, cloud_bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, cloud_bytes)
        .await;

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("remote-root blob reads through cache/cloud");
    assert_eq!(
        got, b"REMOTE-ROOT-CLOUD",
        "a remote-root blob is Remote even when the table has no gate column"
    );
}

#[tokio::test]
async fn remote_root_cache_lazy_host_blob_pulls_row_then_reads_on_demand() {
    let declaration = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy);
    let db1 = remote_root_db(declaration.clone());
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db1, crate::sync::test_helpers::test_cloud_home()).await;
    storage
        .open_into(&db1)
        .await
        .expect("open source into exact test Store");
    let (_source_tmp, source_store_dir) = temp_store_dir();
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &source_store_dir,
        "photos",
        "lazy0001",
        b"LAZY-REMOTE-ROOT",
    )
    .await
    .expect("store source blob");
    db1.database
        .execute_test_host_write(&format!(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'RemoteRoot', NULL, '0000000001000-0000-dev1', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('lazy0001', 'n1', 'cover', 16, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            coven_protocol::blob::content_hash(b"LAZY-REMOTE-ROOT"),
        ))
        .await;
    assert!(
        storage
            .publish_pending(&db1, &source_store_dir)
            .await
            .expect("publish exact source write"),
        "source write publishes a Store commit",
    );

    let db2 = remote_root_db(declaration);
    let (_tmp, ld) = temp_store_dir();
    let (_updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    let reference = db2
        .database
        .row_blob_ref("note_photos", "lazy0001")
        .await
        .expect("load pulled exact row blob reference");
    assert!(
        !cache_path(&ld, &reference).exists()
            && !pinned_path(&ld, &reference).exists()
            && !ld.local_blob_path("photos", "lazy0001").unwrap().exists(),
        "CacheLazy host-provided blobs under a remote root are not downloaded on pull"
    );

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db2.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("CacheLazy remote-root blob reads on demand");
    assert_eq!(got, b"LAZY-REMOTE-ROOT");
    assert!(
        cache_path(&ld, &reference).exists(),
        "the on-demand read populates the evictable cache"
    );
}

#[tokio::test]
async fn plain_blob_table_uses_the_store_audience() {
    let db = plain_blob_db(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, store_dir) = temp_store_dir();
    let blob = host_blob_ref("plain001", "photos", CacheFill::CacheEager);

    let cloud_bytes = b"PLAIN-CLOUD";
    db.database
        .plant_blob_row_for_test(&blob.id, true, cloud_bytes)
        .await;

    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, cloud_bytes)
        .await;
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            store_dir.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &reference)
        .await
        .expect("plain blob table reads its exact Store object"),
        cloud_bytes,
    );
}

/// A blob resolved **Local + user-provided** whose external ref is missing is fail-loud
/// [`BlobCacheError::NoExternalRef`], not a fall-through. The user's file is the only
/// copy and the ref is how coven finds it; its absence is corruption, surfaced loud.
#[tokio::test]
async fn local_user_provided_blob_without_an_external_ref_errors() {
    let db = read_test_db("audio");
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = blob_ref("extn-aaaa", "audio", CacheFill::CacheLazy);
    // Gate Local + the BlobRef's user-provided provenance ⇒ the external arm, but no
    // ref is registered. A cloud copy exists as a decoy a store-probing read would serve.
    let declared_hash = coven_protocol::blob::content_hash(b"CLOUD-DECOY");
    db.database
        .plant_blob_row_with_facts_for_test(&blob.id, false, 11, Some(&declared_hash))
        .await;
    storage
        .create_exact_opaque_blob(&blob.namespace, &blob.id, b"CLOUD-DECOY")
        .await;
    let reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load Local row blob reference");

    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("a Local user-provided blob with no external ref is terminal");
    assert!(
        matches!(err, BlobCacheError::NoExternalRef { .. }),
        "a missing external ref maps to NoExternalRef: {err:?}",
    );
    let range_err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, 0, 5)
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
    let db = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    let blob = host_blob_ref("res1bbbb", "photos", CacheFill::CacheEager);
    let declared_hash = coven_protocol::blob::content_hash(b"CLOUD-DECOY");
    db.database
        .plant_blob_row_with_facts_for_test(&blob.id, false, 11, Some(&declared_hash))
        .await;

    // A cloud copy under the same id: a read that probed every store would serve these
    // bytes. No external ref, no local-store file, no cache file.
    storage
        .create_exact_opaque_blob(&blob.namespace, &blob.id, b"CLOUD-DECOY")
        .await;
    let reference = db
        .database
        .row_blob_ref("note_photos", &blob.id)
        .await
        .expect("load Local row blob reference");

    let err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect_err("a Local blob missing from the local store is terminal, never a cloud fetch");
    assert!(
        matches!(err, BlobCacheError::NoLocalCopy { .. }),
        "a missing Local copy maps to NoLocalCopy: {err:?}",
    );

    let range_err = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, 0, 5)
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (tmp, ld) = temp_store_dir();

    // Remote + user-provided, with a stale external ref still registered.
    let blob = blob_ref("drft0aaa", "audio", CacheFill::CacheLazy);
    let stale_ext = ramp(1500);
    let path = write_external_file(tmp.path(), "stale.flac", &stale_ext);
    let cloud_bytes = ramp(2048);
    db.database
        .plant_blob_row_for_test(&blob.id, false, &cloud_bytes)
        .await;
    coven_database::StoreDatabase::new(&db.database)
        .register_external_blob_for_test("note_photos", &blob.id, &path)
        .await;
    db.database.set_blob_remote_for_test(&blob.id, true).await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &cloud_bytes)
        .await;

    let whole = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("a Remote blob reads the cloud");
    assert_eq!(
        whole, cloud_bytes,
        "a Remote blob serves the cloud, ignoring the stale external ref",
    );

    let (offset, len) = (100u64, 500u64);
    let mid = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob_range(Some(cloud_storage.clone()), &reference, offset, len)
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
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    // One id carried by a row in each table, under different gates: note_photos
    // (ns_local) below a Local note, note_covers (ns_remote) below a Remote note.
    let id = "dup0aaaa";
    let local_hash = coven_protocol::blob::content_hash(b"LOCAL-STORE-BYTES");
    let remote_hash = coven_protocol::blob::content_hash(b"REMOTE-CLOUD-BYTES");
    StoreDatabase::new(&db.database)
        .plant_blob_namespace_collision_for_test(id, &local_hash, &remote_hash)
        .await
        .expect("plant the colliding-id rows");

    // Distinct bytes per source so the result reveals which gate was read.
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld,
        "ns_local",
        id,
        b"LOCAL-STORE-BYTES",
    )
    .await
    .expect("store the Local blob's local copy");
    let remote_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install_for_row("note_covers", id, "ns_remote", b"REMOTE-CLOUD-BYTES")
        .await;
    let local_reference = db
        .database
        .row_blob_ref("note_photos", id)
        .await
        .expect("load colliding Local row blob reference");

    // The ns_remote blob resolves note_covers' gate (Remote) → its cloud copy.
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &remote_reference)
        .await
        .unwrap(),
        b"REMOTE-CLOUD-BYTES",
        "the ns_remote blob resolves its own table's Remote gate, not the colliding Local row",
    );

    // The ns_local blob resolves note_photos' gate (Local) → its local store.
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &local_reference)
        .await
        .unwrap(),
        b"LOCAL-STORE-BYTES",
        "the ns_local blob resolves its own table's Local gate",
    );
}

// ---- Eviction (per-namespace budgets, folder-model) ----

fn set_path_mtime(path: &std::path::Path, secs: u64) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open cache file to set mtime");
    file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
        .expect("set cache file mtime");
}

/// Over budget, eviction deletes the OLDEST files (by mtime) in that namespace first
/// and stops once the total is back under the namespace's budget: the oldest go, the
/// newest stay, and the summed `cache/<namespace>/` size ends `<=` the budget. Driven
/// by staging files with distinct mtimes (no budget), then setting the budget and
/// running a sweep.
#[tokio::test]
async fn eviction_drops_oldest_cache_files_until_under_budget() {
    let db = open_test_db();
    let store_database = StoreDatabase::new(&db.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    // Four 100-byte files, oldest → newest by mtime. No budget yet, so staging does
    // not evict; the cache holds all 400 bytes.
    cache
        .populate_bytes_with_mtime_for_test("release_files", "old1aaaa", &[1u8; 100], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "old2bbbb", &[2u8; 100], 2000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "new3cccc", &[3u8; 100], 3000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "new4dddd", &[4u8; 100], 4000)
        .await
        .expect("stage blob into cache");
    assert_eq!(
        ld.cache_total_bytes("release_files").await,
        400,
        "all four files are cached"
    );

    // Budget of 250 bytes: the two oldest (200 bytes) must go to bring the total
    // (then 200) under budget; the two newest stay. A bare sweep (`None`) — no file
    // is being protected as just-written here.
    store_database
        .set_cache_budget("release_files", 250)
        .await
        .expect("set budget");
    cache
        .enforce_budget("release_files", None)
        .await
        .expect("evict to budget");

    assert!(
        !ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("old1aaaa")
        )
        .unwrap()
        .exists(),
        "the oldest file is evicted first",
    );
    assert!(
        !ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("old2bbbb")
        )
        .unwrap()
        .exists(),
        "the second-oldest file is evicted next",
    );
    assert!(
        ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("new3cccc")
        )
        .unwrap()
        .exists(),
        "a newer file survives once the total is back under budget",
    );
    assert!(
        ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("new4dddd")
        )
        .unwrap()
        .exists(),
        "the newest file survives",
    );
    assert!(
        ld.cache_total_bytes("release_files").await <= 250,
        "the cache is back within budget after eviction",
    );
}

/// Per-namespace isolation: filling `release_files` past its budget evicts that
/// namespace's oldest audio files but leaves a `covers` blob in another namespace
/// untouched — the sweep walks only `cache/release_files/`, never `cache/covers/`.
#[tokio::test]
async fn release_files_eviction_leaves_covers_intact() {
    let db = open_test_db();
    let store_database = StoreDatabase::new(&db.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    // A cover in the small `covers` namespace, plus four audio files in the big
    // `release_files` namespace. No budgets yet.
    cache
        .populate_bytes_with_mtime_for_test("covers", "cov00aaa", &[9u8; 500], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "aud01aaa", &[1u8; 100], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "aud02bbb", &[2u8; 100], 2000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "aud03ccc", &[3u8; 100], 3000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "aud04ddd", &[4u8; 100], 4000)
        .await
        .expect("stage blob into cache");

    // Only `release_files` is budgeted and swept. `covers` is never touched.
    store_database
        .set_cache_budget("release_files", 250)
        .await
        .expect("set release_files budget");
    cache
        .enforce_budget("release_files", None)
        .await
        .expect("evict release_files to budget");

    assert!(
        !ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("aud01aaa")
        )
        .unwrap()
        .exists(),
        "the oldest audio file is evicted under release_files pressure",
    );
    assert!(
        ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("aud04ddd")
        )
        .unwrap()
        .exists(),
        "the newest audio file survives within release_files' budget",
    );
    assert!(
        ld.cache_total_bytes("release_files").await <= 250,
        "release_files is back within its budget",
    );
    assert!(
        ld.cache_blob_path(
            "covers",
            crate::sync::test_helpers::test_cache_locator_hash("cov00aaa")
        )
        .unwrap()
        .exists(),
        "the cover in another namespace is untouched by release_files eviction",
    );
    assert_eq!(
        ld.cache_total_bytes("covers").await,
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
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    // Two 200-byte covers in the cloud, both `CacheEager`. The `covers` budget holds
    // one at a time (250 bytes).
    let cov1 = blob_ref("cov01aaa", "covers", CacheFill::CacheEager);
    let cov2 = blob_ref("cov02bbb", "covers", CacheFill::CacheEager);
    let cov1_bytes = [1u8; 200];
    let cov2_bytes = [2u8; 200];
    db.database
        .plant_blob_row_for_test(&cov1.id, true, &cov1_bytes)
        .await;
    db.database
        .plant_blob_row_for_test(&cov2.id, true, &cov2_bytes)
        .await;
    let cov1_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&cov1.id, &cov1.namespace, &cov1_bytes)
        .await;
    let cov2_reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&cov2.id, &cov2.namespace, &cov2_bytes)
        .await;
    store_database
        .set_cache_budget("covers", 250)
        .await
        .expect("set covers budget");

    // Read the first cover into the cache, then age it so it is the oldest.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &cov1_reference)
    .await
    .expect("first cover read populates the cache");
    set_path_mtime(&cache_path(&ld, &cov1_reference), 1000);

    // Read the second cover: populating it pushes `covers` to 400 (> 250), so the
    // read's own sweep evicts the oldest (the first cover), leaving only the second.
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &cov2_reference)
    .await
    .expect("second cover read populates and evicts to budget");
    assert!(
        !cache_path(&ld, &cov1_reference).exists(),
        "the oldest cover is evicted when covers goes over its small budget",
    );
    assert!(
        cache_path(&ld, &cov2_reference).exists(),
        "the just-read cover stays",
    );

    // A read of the evicted cover re-fetches it from the cloud back into the cache.
    let refetched = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &cov1_reference)
    .await
    .expect("a read of the evicted cover re-fetches it");
    assert_eq!(refetched, vec![1u8; 200], "the re-fetch returns the bytes");
    assert!(
        cache_path(&ld, &cov1_reference).exists(),
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
    // dev1 records a note + a (master-scoped) photo row; pull on dev2 lands the
    // CacheEager blob in the evictable cache, then a pin promotes it to `pinned/`.
    let lazy_decl = BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy);
    let db1 = open_test_db_with_user_and_host_blobs(photo_decl(), lazy_decl.clone());
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db1, crate::sync::test_helpers::test_cloud_home()).await;
    let source_device = storage
        .founder_device()
        .await
        .expect("retain source Store device");
    storage
        .open_into(&db1)
        .await
        .expect("open source into exact test Store");
    let (_source_tmp, source_store_dir) = temp_store_dir();
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &source_store_dir,
        "photos",
        "mir0aaaa",
        &[9u8; 500],
    )
    .await
    .expect("store source blob");
    db1.database.execute_test_host_write(&format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01'); \
             INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('mir0aaaa', 'n1', 'attach', 500, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            coven_protocol::blob::content_hash(&[9u8; 500]),
        ),
    )
    .await;
    assert!(
        storage
            .publish_pending(&db1, &source_store_dir)
            .await
            .expect("publish exact source write"),
        "source write publishes a Store commit",
    );
    let blob_owner = source_device
        .latest_local_store_position()
        .await
        .expect("load source Store position")
        .expect("published source has a Store position");

    let db2 = open_test_db_with_user_and_host_blobs(photo_decl(), lazy_decl);
    let store_database2 = StoreDatabase::new(&db2.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database2.clone(), ld.clone());
    let (_updated, result) = storage.pull_into(&db2, &ld).await;
    assert_eq!(result.changesets_applied, 1);
    let eager = db2
        .database
        .row_blob_ref("note_photos", "mir0aaaa")
        .await
        .expect("load pulled exact row blob reference");
    assert!(
        cache_path(&ld, &eager).exists(),
        "the CacheEager blob lands in the evictable cache on pull",
    );
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db2.database),
        ld.clone(),
    )
    .pin_blobs(Some(cloud_storage.clone()), std::slice::from_ref(&eager))
    .await
    .expect("pin the eager blob into pinned/");
    assert!(pinned_path(&ld, &eager).exists());

    // Also user-pin a CacheLazy blob (a different namespace) into pinned/.
    let lazy = blob_ref("usr0bbbb", "audio", CacheFill::CacheLazy);
    let lazy_bytes = [7u8; 500];
    let lazy_hash = coven_protocol::blob::content_hash(&lazy_bytes);
    StoreDatabase::new(&db2.database)
        .plant_note_cover_blob_row_for_test("usr0bbbb", "n1", 500, &lazy_hash)
        .await
        .expect("plant lazy blob row");
    ExactRemoteBlobFixture::new(&db2, &storage)
        .bind_for_row_with_owner(
            "note_covers",
            &lazy.id,
            &lazy.namespace,
            &lazy_bytes,
            blob_owner,
        )
        .await;
    let lazy_reference = db2
        .database
        .row_blob_ref("note_covers", &lazy.id)
        .await
        .expect("load exact lazy row blob reference");
    cache
        .populate_bytes_for_test(&lazy.namespace, locator_hash(&lazy_reference), &[7u8; 500])
        .await
        .expect("write the lazy blob into the cache");
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db2.database),
        ld.clone(),
    )
    .pin_blobs(
        Some(cloud_storage.clone()),
        std::slice::from_ref(&lazy_reference),
    )
    .await
    .expect("user-pin the lazy blob");
    assert!(pinned_path(&ld, &lazy_reference).exists());

    // Flood BOTH namespaces' evictable caches, set a tiny budget on EACH, and sweep
    // each. The pinned files live in pinned/ — the per-namespace sweep never touches
    // them, whichever namespace's budget runs.
    cache
        .populate_bytes_with_mtime_for_test("photos", "junkp1cc", &[1u8; 1000], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("audio", "junka1cc", &[2u8; 1000], 2000)
        .await
        .expect("stage blob into cache");
    store_database2
        .set_cache_budget("photos", 10)
        .await
        .expect("set tiny photos budget");
    store_database2
        .set_cache_budget("audio", 10)
        .await
        .expect("set tiny audio budget");
    cache
        .enforce_budget("photos", None)
        .await
        .expect("evict photos to budget");
    cache
        .enforce_budget("audio", None)
        .await
        .expect("evict audio to budget");

    assert!(
        pinned_path(&ld, &eager).exists(),
        "a pinned CacheEager blob survives its namespace's eviction (it is in pinned/)",
    );
    assert!(
        pinned_path(&ld, &lazy_reference).exists(),
        "a user-pinned CacheLazy blob survives its namespace's eviction (it is in pinned/)",
    );
    cloud_storage
        .clone()
        .delete_blob_object(eager.stored().expect("eager blob has exact storage"))
        .await
        .expect("delete eager cloud object");
    cloud_storage
        .clone()
        .delete_blob_object(
            lazy_reference
                .stored()
                .expect("lazy blob has exact storage"),
        )
        .await
        .expect("delete lazy cloud object");
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db2.database),
            ld.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &eager)
        .await
        .expect("read retained eager pin"),
        vec![9u8; 500],
    );
    assert_eq!(
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db2.database),
            ld.clone()
        )
        .read_blob(Some(cloud_storage.clone()), &lazy_reference)
        .await
        .expect("read retained lazy pin"),
        vec![7u8; 500],
    );
    assert!(
        ld.cache_total_bytes("photos").await <= 10,
        "the photos evictable cache is trimmed to budget, ignoring pinned/",
    );
    assert!(
        ld.cache_total_bytes("audio").await <= 10,
        "the audio evictable cache is trimmed to budget, ignoring pinned/",
    );
}

/// With no budget set for a namespace its cache is unlimited: even a large cache and
/// an explicit eviction sweep leave every file in place. The host opts a namespace
/// into a budget; until then nothing in it is evicted.
#[tokio::test]
async fn unset_namespace_budget_never_evicts() {
    let db = open_test_db();
    let store_database = StoreDatabase::new(&db.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    cache
        .populate_bytes_with_mtime_for_test("release_files", "keep1aaa", &[1u8; 5000], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "keep2bbb", &[2u8; 5000], 2000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "keep3ccc", &[3u8; 5000], 3000)
        .await
        .expect("stage blob into cache");
    assert_eq!(ld.cache_total_bytes("release_files").await, 15000);

    // No budget set for this namespace — an explicit sweep is a no-op.
    cache
        .enforce_budget("release_files", None)
        .await
        .expect("evict is a no-op with no budget");
    assert_eq!(
        ld.cache_total_bytes("release_files").await,
        15000,
        "a big cache stays whole when no budget is set",
    );
    for id in ["keep1aaa", "keep2bbb", "keep3ccc"] {
        assert!(
            ld.cache_blob_path(
                "release_files",
                crate::sync::test_helpers::test_cache_locator_hash(id)
            )
            .unwrap()
            .exists(),
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
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    // An older cached file (100 bytes, old mtime), no budget yet.
    cache
        .populate_bytes_with_mtime_for_test("release_files", "older1aa", &[1u8; 100], 1000)
        .await
        .expect("stage blob into cache");

    // Budget of 150 bytes: holds either file alone, not both. Now a read fetches a
    // second 100-byte blob; populating it pushes the total to 200 (> 150), so the
    // read's own eviction must drop the older file (the newest — the one just
    // read — survives).
    store_database
        .set_cache_budget("release_files", 150)
        .await
        .expect("set budget");
    let blob = blob_ref("newer2bb", "release_files", CacheFill::CacheLazy);
    let bytes = vec![2u8; 100];
    db.database
        .plant_blob_row_for_test(&blob.id, true, &bytes)
        .await;
    let reference = ExactRemoteBlobFixture::new(&db, &storage)
        .install(&blob.id, &blob.namespace, &bytes)
        .await;

    let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
        StoreDatabase::new(&db.database),
        ld.clone(),
    )
    .read_blob(Some(cloud_storage.clone()), &reference)
    .await
    .expect("read fetches and populates, then evicts to budget");
    assert_eq!(got, bytes, "the triggering read still returns its bytes");
    assert!(
        cache_path(&ld, &reference).exists(),
        "the just-populated (newest) blob survives its own over-budget eviction",
    );
    assert!(
        !ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("older1aa")
        )
        .unwrap()
        .exists(),
        "the older blob is the one evicted",
    );
    assert!(
        ld.cache_total_bytes("release_files").await <= 150,
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
    let store_database = StoreDatabase::new(&db.database);
    let TestStoreFixture {
        store: storage,
        storage: cloud_storage,
    } = create_store(&db, crate::sync::test_helpers::test_cloud_home()).await;
    let (_tmp, ld) = temp_store_dir();

    // Budget of 250 bytes; each blob is 100 bytes, so at most two fit.
    store_database
        .set_cache_budget("release_files", 250)
        .await
        .expect("set budget");

    let mut first_path = None;
    let mut last_path = None;
    for i in 0..6u8 {
        let id = format!("seqr{i:04}"); // ≥4 chars, distinct per i, for the shard
        let bytes = vec![i; 100];
        db.database.plant_blob_row_for_test(&id, true, &bytes).await;
        let reference = ExactRemoteBlobFixture::new(&db, &storage)
            .install(&id, "release_files", &bytes)
            .await;
        let exact_cache_path = cache_path(&ld, &reference);
        if i == 0 {
            first_path = Some(exact_cache_path.clone());
        }
        if i == 5 {
            last_path = Some(exact_cache_path);
        }

        let got = crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&db.database),
            ld.clone(),
        )
        .read_blob(Some(cloud_storage.clone()), &reference)
        .await
        .expect("each read populates then evicts to budget");
        assert_eq!(got, bytes, "each read returns its freshly-fetched bytes");

        // The cache is within budget after every populate, never drifting over as
        // new blobs arrive.
        assert!(
            ld.cache_total_bytes("release_files").await <= 250,
            "after populate {i} the cache is within the 250-byte budget",
        );
    }

    // Eviction ran rather than the budget never being reached: with a 250-byte
    // budget and 100-byte blobs, at most two fit, so after six reads the earliest
    // blobs must have been evicted and the just-read last blob must still be present.
    assert!(
        ld.cache_total_bytes("release_files").await <= 200,
        "at most two 100-byte blobs remain under the 250-byte budget",
    );
    assert!(
        !first_path.expect("first exact cache path").exists(),
        "the first blob read was evicted by later populates",
    );
    assert!(
        last_path.expect("last exact cache path").exists(),
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
    let store_database = StoreDatabase::new(&db.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    // Two 100-byte files. The protected one is OLDER (mtime 1000); the other is
    // NEWER (mtime 2000). A naive oldest-first sweep would evict the protected one.
    cache
        .populate_bytes_with_mtime_for_test("release_files", "prot0aaa", &[1u8; 100], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "othr0bbb", &[2u8; 100], 2000)
        .await
        .expect("stage blob into cache");

    // Budget of 100 bytes: exactly one file fits, so one must be evicted.
    store_database
        .set_cache_budget("release_files", 100)
        .await
        .expect("set budget");
    let protected = ld
        .cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("prot0aaa"),
        )
        .unwrap();
    cache
        .enforce_budget("release_files", Some(&protected))
        .await
        .expect("evict to budget, protecting the older file");

    assert!(
        ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("prot0aaa")
        )
        .unwrap()
        .exists(),
        "the protected file survives even though it is the older by mtime",
    );
    assert!(
        !ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("othr0bbb")
        )
        .unwrap()
        .exists(),
        "the newer, unprotected file is the one evicted instead",
    );
    assert!(
        ld.cache_total_bytes("release_files").await <= 100,
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
    let store_database = StoreDatabase::new(&db.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    // A 100-byte protected file plus a 100-byte evictable one. The protected file
    // alone (100 bytes) is larger than the 50-byte budget.
    cache
        .populate_bytes_with_mtime_for_test("release_files", "biginuse", &[1u8; 100], 1000)
        .await
        .expect("stage blob into cache");
    cache
        .populate_bytes_with_mtime_for_test("release_files", "othr0bbb", &[2u8; 100], 2000)
        .await
        .expect("stage blob into cache");

    store_database
        .set_cache_budget("release_files", 50)
        .await
        .expect("set budget");
    let protected = ld
        .cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("biginuse"),
        )
        .unwrap();
    cache
        .enforce_budget("release_files", Some(&protected))
        .await
        .expect("eviction returns Ok even when the in-use file alone exceeds budget");

    assert!(
        ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("biginuse")
        )
        .unwrap()
        .exists(),
        "the protected in-use file is kept even though it alone exceeds the budget",
    );
    assert!(
        !ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("othr0bbb")
        )
        .unwrap()
        .exists(),
        "every other candidate is still evicted",
    );
    assert_eq!(
        ld.cache_total_bytes("release_files").await,
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
    let store_database = StoreDatabase::new(&db.database);
    let (_tmp, ld) = temp_store_dir();
    let cache = StoreBlobCache::new(store_database.clone(), ld.clone());

    // A committed 100-byte cache file (newer by mtime), already at the budget.
    cache
        .populate_bytes_with_mtime_for_test("release_files", "keep0aaa", &[1u8; 100], 2000)
        .await
        .expect("stage blob into cache");

    // A concurrent populate's in-flight temp: a `.tmp.<uuid>` sibling in the shard dir
    // where its committed blob will land, aged OLDER than the committed file so a sweep
    // that treated it as a candidate would evict it first.
    let dest = ld
        .cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("new0bbbb"),
        )
        .unwrap();
    let mut stage = ld
        .stage_atomic_file(&dest)
        .await
        .expect("create concurrent populate stage");
    stage
        .write_bytes(&[9u8; 100])
        .await
        .expect("write concurrent populate temp");
    let temp_path = stage.leave_unpublished_for_test();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&temp_path)
        .expect("open temp to age it")
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000))
        .expect("age the temp so a candidate sweep would evict it first");

    // Budget of 100 bytes: exactly the committed file fits. A sweep that counted the
    // temp would see 200 bytes and evict the oldest (the temp) first.
    store_database
        .set_cache_budget("release_files", 100)
        .await
        .expect("set budget");
    cache
        .enforce_budget("release_files", None)
        .await
        .expect("evict to budget");

    assert!(
        temp_path.exists(),
        "eviction leaves a concurrent populate's temp in place",
    );
    assert!(
        ld.cache_blob_path(
            "release_files",
            crate::sync::test_helpers::test_cache_locator_hash("keep0aaa")
        )
        .unwrap()
        .exists(),
        "the committed cache file survives — the temp's bytes never counted toward the budget",
    );

    // The populate's finishing rename still finds its temp and commits the blob intact.
    tokio::fs::rename(&temp_path, &dest)
        .await
        .expect("the populate's rename succeeds after the sweep");
    assert_eq!(
        std::fs::read(&dest).expect("committed blob readable"),
        [9u8; 100],
        "the renamed temp became the committed blob intact",
    );
}
