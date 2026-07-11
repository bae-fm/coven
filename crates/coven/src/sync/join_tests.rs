//! "Join then sync" end-to-end: a device that bootstraps from a snapshot and
//! then runs the real [`run_single_sync_cycle`] must not republish — and thereby
//! clobber — the shared snapshot.
//!
//! `multi_device_managed_edit_reaches_restore` (in `snapshot.rs`) covers
//! bootstrap plus a *hand-rolled* pull and snapshot push; it never drives the
//! real cycle, so it can't see that the cycle, run on a just-joined device whose
//! `sync_state` the join path left empty, trips `is_initial_sync` and overwrites
//! the owner's snapshot with its own.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::blob::{CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::config::Config;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::id_provider::SequentialIdProvider;
use crate::join_code::{encode, InviteCode, JoinCodeError};
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::join::{join_from_invite_code, open_db_and_pull, BootstrapError};
use crate::sync::session::BlobDecl;
use crate::sync::snapshot::{
    bootstrap_from_snapshot, create_snapshot, push_snapshot, SnapshotBlobPreflight,
};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

/// An invite code carrying an attacker-chosen `store_id` is the path-traversal
/// vector: the code is unsigned, and the id becomes a directory the joiner creates
/// and — on a bootstrap failure — recursively deletes. The id is refused the moment
/// the code is decoded, so a crafted code never reaches the directory step: decode
/// fails, nothing is created outside the stores root, and no network or
/// filesystem work runs.
fn invite_code_with_store_id(store_id: &str) -> InviteCode {
    InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: "Evil".to_string(),
        join_info: CloudHomeJoinInfo::S3 {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            // Port 1 / loopback: nothing listens, so a connect fails at once. A
            // normal id reaches the network unwrap and fails here fast, instead of
            // resolving a real AWS endpoint.
            endpoint: Some("http://127.0.0.1:1".to_string()),
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
            key_prefix: None,
        },
        owner_pubkey: hex::encode([0xAB_u8; 32]),
    }
}

/// Drive the full join path for an invite code string and return its result. Uses
/// an app dir under `tmp` and no-op blob source; a malicious id fails at decode
/// before any cloud home is built, so the cloud details never matter.
async fn join_result_for(
    code_str: &str,
    app_dir: &std::path::Path,
) -> Result<Config, BootstrapError> {
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    join_from_invite_code(
        code_str,
        &crate::store_dir::StoreLayout::new(app_dir),
        &test_synced_tables(),
        &test_migrations(),
        None,
        None,
        Arc::new(SystemClock),
        ids,
        |_| {},
    )
    .await
}

/// Every traversal-shaped `store_id` is refused at the decode boundary: `decode`
/// returns `JoinCodeError::InvalidStoreId`, so a decoded `InviteCode` never
/// carries a traversal id. Driven end to end, the decode error propagates as
/// `BootstrapError::InvalidCode` and the join creates nothing outside the stores root.
///
/// The cases share one mechanism and differ only in the malicious id and the
/// directory it would escape to, so they run as a table:
/// - `../escape`: `app_dir/stores/../escape` resolves to `app_dir/escape`.
/// - an absolute path: `stores`.join("/abs") == "/abs" replaces the base.
/// - `.`: a trailing `.` normalizes away, so `stores/.` lands on the data dir.
#[tokio::test]
async fn join_rejects_traversal_store_id_at_decode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // The absolute case escapes to a path *inside* the temp dir, so even a
    // regressed guard never writes to a real shared location.
    let parent_escape = app_dir.join("escape");
    let abs_escape = app_dir.join("abs_escape");
    let abs_id = abs_escape.to_str().expect("utf8 path").to_string();

    let cases: [(&str, Option<&std::path::Path>); 3] = [
        ("../escape", Some(parent_escape.as_path())),
        (&abs_id, Some(abs_escape.as_path())),
        (".", None),
    ];

    for (store_id, escape_target) in cases {
        let encoded = encode(&invite_code_with_store_id(store_id));
        assert!(
            matches!(
                crate::join_code::decode(&encoded),
                Err(JoinCodeError::InvalidStoreId(_))
            ),
            "decode must refuse `{store_id}` with InvalidStoreId",
        );

        let result = join_result_for(&encoded, app_dir).await;
        assert!(
            matches!(result, Err(BootstrapError::InvalidCode(_))),
            "`{store_id}` must fail the join with the propagated decode error, got {result:?}",
        );
        if let Some(target) = escape_target {
            assert!(
                !target.exists(),
                "join must not create an escape directory at {}",
                target.display(),
            );
        }
    }
}

/// A normal `store_id` decodes and the join reaches the cloud step, where it
/// fails on the unreachable endpoint (`BootstrapError::Invite`, from the wrapped-key
/// download) rather than on the id — proving the decoder rejects only unsafe ids
/// and the directory the join would create sits under `stores/`.
///
/// Join reads the device's user keypair from the keyring before the cloud download,
/// so a keypair must exist for execution to reach the cloud. The user keypair is one
/// process-wide keyring account, so this seeds it under `SIGNING_KEY_GUARD` to keep
/// a parallel test from deleting it mid-join; the guard is held across the join await
/// (sound here: a `#[tokio::test]` is a single-task current-thread runtime, so the
/// blocking `std` lock never deadlocks against another task on this runtime).
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn join_accepts_a_normal_store_id_past_decode() {
    let encoded = encode(&invite_code_with_store_id("abc-123"));
    let decoded = crate::join_code::decode(&encoded).expect("a normal id decodes");
    assert_eq!(decoded.store_id, "abc-123");

    crate::keys::test_keyring::install();
    let _guard = crate::keys::test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
    crate::keys::DeviceKeys::get_or_create_user_keypair().expect("seed the device user keypair");

    // End to end the join still fails — the S3 endpoint above is bogus — but it
    // fails at the cloud read past the decode boundary, not at the id.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Invite(_))),
        "the bogus cloud endpoint must fail the join at the cloud read, got {result:?}",
    );
}

/// A store already present locally is the data — re-joining it adds nothing, and
/// the old code would delete its database and blobs during the failure-cleanup once
/// bootstrap failed. The join now refuses up front with a typed error naming the
/// store and leaves the existing files untouched. The keypair is seeded (and the
/// endpoint is unreachable) so that, absent the guard, execution would reach the
/// cloud read and the destructive cleanup — the guard is what stops it first.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn join_refuses_when_store_exists_and_leaves_it_untouched() {
    let encoded = encode(&invite_code_with_store_id("abc-123"));

    crate::keys::test_keyring::install();
    let _guard = crate::keys::test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
    crate::keys::DeviceKeys::get_or_create_user_keypair().expect("seed the device user keypair");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A store with this id is already present locally, holding a database file
    // and a blob the join must not touch.
    let store_dir = app_dir.join("stores").join("abc-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create existing store dir");
    let db_path = store_dir.join("store.db");
    let blob_path = store_dir.join("storage").join("cover.blob");
    std::fs::write(&db_path, b"existing-db-bytes").expect("seed existing db");
    std::fs::write(&blob_path, b"existing-blob-bytes").expect("seed existing blob");

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "abc-123"),
        "join must refuse a store already present locally, got {result:?}",
    );
    assert_eq!(
        std::fs::read(&db_path).expect("existing db still present"),
        b"existing-db-bytes",
        "the existing database must be left untouched",
    );
    assert_eq!(
        std::fs::read(&blob_path).expect("existing blob still present"),
        b"existing-blob-bytes",
        "the existing blob must be left untouched",
    );
}

/// A join for a store not present locally that fails at the cloud read still
/// removes the directory it created — the failure-cleanup keeps working for the
/// directory this invocation owns.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn fresh_join_failure_cleans_up_its_own_directory() {
    let encoded = encode(&invite_code_with_store_id("fresh-123"));

    crate::keys::test_keyring::install();
    let _guard = crate::keys::test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
    crate::keys::DeviceKeys::get_or_create_user_keypair().expect("seed the device user keypair");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let store_dir = app_dir.join("stores").join("fresh-123");

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Invite(_))),
        "the bogus cloud endpoint must fail the join at the cloud read, got {result:?}",
    );
    assert!(
        !store_dir.exists(),
        "a fresh join that fails must remove the directory it created at {}",
        store_dir.display(),
    );
}

/// `join_store` is the lower-level, reusable function `join_from_invite_code`
/// delegates to after its own exists-check — any other direct caller reaches
/// `join_store` without that check. Its failure-cleanup removes the store
/// directory unconditionally, so `join_store` must refuse a store already
/// present locally itself, independent of any check a wrapper does or doesn't
/// do around it. The keypair is seeded (and the endpoint is unreachable) so
/// that, absent its own guard, `join_store` would reach the cloud read and the
/// destructive cleanup — the guard is what stops it first.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn join_store_refuses_when_store_exists_and_leaves_it_untouched() {
    let code = invite_code_with_store_id("abc-123");

    crate::keys::test_keyring::install();
    let _guard = crate::keys::test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
    crate::keys::DeviceKeys::get_or_create_user_keypair().expect("seed the device user keypair");

    let tmp = tempfile::tempdir().expect("temp dir");
    let data_dir = tmp.path();

    // A store with this id is already present locally, holding a database file
    // and a blob join_store must not touch.
    let store_dir = data_dir.join("stores").join("abc-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create existing store dir");
    let db_path = store_dir.join("store.db");
    let blob_path = store_dir.join("storage").join("cover.blob");
    std::fs::write(&db_path, b"existing-db-bytes").expect("seed existing db");
    std::fs::write(&blob_path, b"existing-blob-bytes").expect("seed existing blob");

    // The S3 client builds without touching the network; its endpoint is never
    // reached when the exists-check refuses the join first.
    let cloud_home: Box<dyn crate::storage::cloud::CloudHome> = Box::new(
        crate::storage::cloud::s3::S3CloudHome::new(
            "b".to_string(),
            "us-east-1".to_string(),
            Some("http://127.0.0.1:1".to_string()),
            "ak".to_string(),
            "sk".to_string(),
            None,
        )
        .await
        .expect("construct S3 cloud home"),
    );
    let ids = crate::id_provider::SequentialIdProvider::new("dev");

    let result = crate::sync::join::join_store(
        &crate::store_dir::StoreLayout::new(data_dir),
        code,
        &test_synced_tables(),
        &test_migrations(),
        cloud_home,
        &ids,
        |_| {},
    )
    .await;

    assert!(
        matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "abc-123"),
        "join_store must refuse a store already present locally, got {result:?}",
    );
    assert_eq!(
        std::fs::read(&db_path).expect("existing db still present"),
        b"existing-db-bytes",
        "the existing database must be left untouched",
    );
    assert_eq!(
        std::fs::read(&blob_path).expect("existing blob still present"),
        b"existing-blob-bytes",
        "the existing blob must be left untouched",
    );
}

#[tokio::test]
async fn joined_device_first_cycle_does_not_clobber_the_shared_snapshot() {
    let enc = CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

    // Owner A leaves the cloud in the shape "empty store, then import" produces:
    // an empty snapshot at seq 0 (the initial-sync snapshot of the empty store),
    // and the imported row as changeset A/1. `should_create_snapshot` does not
    // refresh the snapshot for a single sub-threshold changeset, so the catalog
    // lives in the changeset — not the snapshot.
    let db_a = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let empty_snap = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner empty snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        empty_snap,
        "A",
        HashMap::new(),
        0,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push empty snapshot");

    let cs1 = capture_bytes(
        &db_a,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Album Title', NULL, '0000000001000-0000-A', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("A", 1, &cs1, SCHEMA_VERSION);

    // The shared snapshot pointer before B joins. B must not republish (which
    // would flip the pointer to a new generation).
    let snapshot_before = storage
        .get_snapshot_pointer()
        .await
        .expect("snapshot pointer present");

    // Device B joins through the real path: bootstrap from the snapshot, then
    // pull the changesets published after it.
    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
    )
    .await
    .expect("B open_db_and_pull");

    let (db_b, _stamper) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        "B".to_string(),
        &test_migrations(),
    )
    .expect("open B db");
    assert_eq!(
        query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Album Title",
        "B must receive the imported row through bootstrap + pull",
    );

    // B's first real sync cycle, with no local changes of its own.
    let enc_lock = RwLock::new(enc.clone());
    let keypair = UserKeypair::generate();
    let b_hlc = Hlc::new("B".to_string());
    run_single_sync_cycle(
        &storage,
        "test-lib",
        "B",
        &b_hlc,
        &SystemClock,
        &db_b,
        &enc_lock,
        &keypair,
        None,
        &lib_b,
        None,
        None,
    )
    .await
    .expect("B sync cycle");

    // A just-joined device with no local changes must leave the shared snapshot
    // untouched: the pointer still names the owner's generation, not a republished
    // one of B's own.
    let snapshot_after = storage
        .get_snapshot_pointer()
        .await
        .expect("snapshot pointer present");
    assert_eq!(
        snapshot_after, snapshot_before,
        "a just-joined device's first cycle must not republish/clobber the shared snapshot",
    );
}

/// A device that bootstraps from a snapshot must receive not just the catalog
/// rows but the blob *files* those rows reference. The snapshot is a whole-DB
/// image carrying a `note_photos` row, but no per-row blob file; the pull that
/// follows starts past the snapshot's cursors, so the INSERT changeset that first
/// carried the photo (seq <= cursor) is never re-walked and the per-changeset
/// blob download never fires for it. Without the bootstrap backfill, the
/// bootstrapped device has the photo row but no blob file in its cache — a synced
/// album renders a placeholder cover. Asserts the file lands.
#[tokio::test]
async fn bootstrap_backfills_blob_files_for_snapshot_rows() {
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('photo1', 'n1', 'cover', 11, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "A",
        HashMap::new(),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push snapshot");

    // The cover blob exists in the cloud (uploaded when A first imported the
    // album), keyed `photos/photo1` master-scoped as the declaration maps a cover
    // row.
    storage
        .put_blob(
            "photos",
            "photo1",
            crate::blob::BlobScope::Master,
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    // Device B bootstraps from the snapshot, then runs the real bootstrap path,
    // which derives `note_photos`'s blob from the declaration. A `CacheEager` blob
    // lands in B's evictable cache folder (`storage/cache/<id>`) on reconciliation,
    // which coven builds from the validated id.
    let (_tmp_b, lib_b) = temp_store_dir();
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
    )
    .await
    .expect("B open_db_and_pull");

    assert!(
        expected_blob.exists(),
        "the cover blob file must be backfilled to {} after bootstrap",
        expected_blob.display(),
    );
    assert_eq!(
        std::fs::read(&expected_blob).expect("read backfilled blob"),
        b"cover-bytes",
        "the backfilled file must hold the blob's plaintext bytes",
    );
}

/// A blob whose download fails at bootstrap (its object isn't in the cloud yet)
/// refuses the bootstrap before the store is saved.
#[tokio::test]
async fn snapshot_blob_backfill_failure_aborts_bootstrap_pull() {
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));

    // Owner A: a shared note with a cover photo, both captured into the snapshot.
    let db_a = open_test_db();
    exec(
        &db_a,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;
    exec(
        &db_a,
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('photo1', 'n1', 'cover', 11, '0000000001000-0000-A', '2026-01-01')",
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    push_snapshot(
        &storage,
        "test-lib",
        snapshot,
        "A",
        HashMap::new(),
        1,
        db_a.schema_version(),
        &UserKeypair::generate(),
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db_a,
            blobs: &[],
        },
    )
    .await
    .expect("push snapshot");

    // Unlike the happy-path test above, the cover blob is NOT in the cloud yet at
    // bootstrap time (e.g. A's upload of it hadn't landed). So the bootstrap's
    // download attempt fails.
    let (_tmp_b, lib_b) = temp_store_dir();
    // A reconciled `CacheEager` blob lands in B's evictable cache folder.
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", None, 1, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
    )
    .await
    .expect_err("B open_db_and_pull must fail when snapshot eager blob is missing");

    assert!(
        !expected_blob.exists(),
        "the missing cover blob must not be materialized at {}",
        expected_blob.display(),
    );
}
