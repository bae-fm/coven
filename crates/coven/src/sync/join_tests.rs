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

use rusqlite::OptionalExtension;

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
use crate::sync::join::{join_from_invite_code, open_db_and_pull, JoinError};
use crate::sync::session::BlobDecl;
use crate::sync::snapshot::{
    bootstrap_from_snapshot, create_snapshot, push_snapshot, SnapshotBlobPreflight,
    SNAPSHOT_BLOB_BACKFILL_PENDING,
};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// The synthetic test db opens with a single migration, so its
/// [`Database::schema_version`] is 1. Changesets are stored at that version.
const SCHEMA_VERSION: u32 = 1;

/// An invite code carrying an attacker-chosen `library_id` is the path-traversal
/// vector: the code is unsigned, and the id becomes a directory the joiner creates
/// and — on a bootstrap failure — recursively deletes. The id is refused the moment
/// the code is decoded, so a crafted code never reaches the directory step: decode
/// fails, nothing is created outside the libraries root, and no network or
/// filesystem work runs.
fn invite_code_with_library_id(library_id: &str) -> InviteCode {
    InviteCode {
        library_id: library_id.to_string(),
        library_name: "Evil".to_string(),
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
        owner_pubkey: "deadbeef".to_string(),
    }
}

/// Drive the full join path for an invite code string and return its result. Uses
/// an app dir under `tmp` and no-op blob source; a malicious id fails at decode
/// before any cloud home is built, so the cloud details never matter.
async fn join_result_for(code_str: &str, app_dir: &std::path::Path) -> Result<Config, JoinError> {
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    join_from_invite_code(
        code_str,
        app_dir,
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

/// Every traversal-shaped `library_id` is refused at the decode boundary: `decode`
/// returns `JoinCodeError::InvalidLibraryId`, so a decoded `InviteCode` never
/// carries a traversal id. Driven end to end, the decode error propagates as
/// `JoinError::InvalidCode` and the join creates nothing outside the libraries root.
///
/// The cases share one mechanism and differ only in the malicious id and the
/// directory it would escape to, so they run as a table:
/// - `../escape`: `app_dir/libraries/../escape` resolves to `app_dir/escape`.
/// - an absolute path: `libraries`.join("/abs") == "/abs" replaces the base.
/// - `.`: a trailing `.` normalizes away, so `libraries/.` lands on the data dir.
#[tokio::test]
async fn join_rejects_traversal_library_id_at_decode() {
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

    for (library_id, escape_target) in cases {
        let encoded = encode(&invite_code_with_library_id(library_id));
        assert!(
            matches!(
                crate::join_code::decode(&encoded),
                Err(JoinCodeError::InvalidLibraryId(_))
            ),
            "decode must refuse `{library_id}` with InvalidLibraryId",
        );

        let result = join_result_for(&encoded, app_dir).await;
        assert!(
            matches!(result, Err(JoinError::InvalidCode(_))),
            "`{library_id}` must fail the join with the propagated decode error, got {result:?}",
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

/// A normal `library_id` decodes and the join reaches the cloud step, where it
/// fails on the unreachable endpoint (`JoinError::Invite`, from the wrapped-key
/// download) rather than on the id — proving the decoder rejects only unsafe ids
/// and the directory the join would create sits under `libraries/`.
///
/// Join reads the device's user keypair from the keyring before the cloud download,
/// so a keypair must exist for execution to reach the cloud. The user keypair is one
/// process-wide keyring account, so this seeds it under `SIGNING_KEY_GUARD` to keep
/// a parallel test from deleting it mid-join; the guard is held across the join await
/// (sound here: a `#[tokio::test]` is a single-task current-thread runtime, so the
/// blocking `std` lock never deadlocks against another task on this runtime).
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn join_accepts_a_normal_library_id_past_decode() {
    let encoded = encode(&invite_code_with_library_id("abc-123"));
    let decoded = crate::join_code::decode(&encoded).expect("a normal id decodes");
    assert_eq!(decoded.library_id, "abc-123");

    crate::keys::test_keyring::install();
    let _guard = crate::keys::test_keyring::SIGNING_KEY_GUARD.lock().unwrap();
    crate::keys::KeyService::new("join-test".to_string())
        .get_or_create_user_keypair()
        .expect("seed the device user keypair");

    // End to end the join still fails — the S3 endpoint above is bogus — but it
    // fails at the cloud read past the decode boundary, not at the id.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(JoinError::Invite(_))),
        "the bogus cloud endpoint must fail the join at the cloud read, got {result:?}",
    );
}

#[tokio::test]
async fn joined_device_first_cycle_does_not_clobber_the_shared_snapshot() {
    let enc = CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

    // Owner A leaves the cloud in the shape "empty library, then import" produces:
    // an empty snapshot at seq 0 (the initial-sync snapshot of the empty library),
    // and the imported row as changeset A/1. `should_create_snapshot` does not
    // refresh the snapshot for a single sub-threshold changeset, so the catalog
    // lives in the changeset — not the snapshot.
    let db_a = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let (tables_c, enc_c) = (tables.clone(), enc.clone());
    let empty_snap = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c, &enc_c).map_err(|e| DbError(e.to_string()))
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
    let (_tmp_b, lib_b) = temp_library_dir();
    let boot = bootstrap_from_snapshot(&storage, "test-lib", &enc, None, 1, &lib_b.db_path())
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
    let enc = CloudCipher::Encrypted(EncryptionService::from_key([9u8; 32]));
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
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('photo1', 'n1', 'cover', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let (tables_c, enc_c) = (tables.clone(), enc.clone());
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c, &enc_c).map_err(|e| DbError(e.to_string()))
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
            crate::blob::ResolvedScope::Master,
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    // Device B bootstraps from the snapshot, then runs the real bootstrap path,
    // which derives `note_photos`'s blob from the declaration. A `CacheEager` blob
    // lands in B's evictable cache folder (`storage/cache/<id>`) on reconciliation,
    // which coven builds from the validated id.
    let (_tmp_b, lib_b) = temp_library_dir();
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", &enc, None, 1, &lib_b.db_path())
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

/// Read the snapshot-blob-backfill pending flag (true while a bootstrap could not
/// land every referenced blob) from a library's `sync_state`.
async fn backfill_pending(db: &Database) -> bool {
    let key = SNAPSHOT_BLOB_BACKFILL_PENDING.to_string();
    db.call(move |conn| {
        conn.query_row("SELECT value FROM sync_state WHERE key = ?1", [key], |r| {
            r.get::<_, String>(0)
        })
        .optional()
        .map(|value| value.is_some_and(|v| !v.is_empty()))
        .map_err(DbError::from)
    })
    .await
    .expect("read backfill flag")
}

/// A blob whose download fails at bootstrap (its object isn't in the cloud yet)
/// must not be lost: the bootstrap records that the reconciliation is incomplete,
/// and a later sync cycle re-runs it once the object is available. This is the
/// retry the bootstrap's swallow-and-continue relies on — before it existed, a
/// transient failure stranded the file permanently, because the pull only
/// downloads blobs for changesets past the per-device cursor and bootstrap
/// advanced that cursor past the INSERT that carried this one.
#[tokio::test]
async fn snapshot_blob_backfill_retries_on_a_later_cycle() {
    let enc = CloudCipher::Encrypted(EncryptionService::from_key([11u8; 32]));
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
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('photo1', 'n1', 'cover', '0000000001000-0000-A', '2026-01-01')",
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let (tables_c, enc_c) = (tables.clone(), enc.clone());
    let snapshot = db_a
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c, &enc_c).map_err(|e| DbError(e.to_string()))
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
    let (_tmp_b, lib_b) = temp_library_dir();
    // A reconciled `CacheEager` blob lands in B's evictable cache folder.
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let boot = bootstrap_from_snapshot(&storage, "test-lib", &enc, None, 1, &lib_b.db_path())
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

    // After bootstrap the file is absent and the pending flag is set: the catalog
    // landed, the blob did not, and the cycle must reconcile it later.
    assert!(
        !expected_blob.exists(),
        "the cover blob must be absent after a bootstrap whose download failed",
    );
    let (db_b, _stamper) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        "B".to_string(),
        &test_migrations(),
    )
    .expect("open B db");
    assert!(
        backfill_pending(&db_b).await,
        "bootstrap must record the backfill as pending when a blob did not land",
    );

    // The object becomes available in the cloud (A's upload landed).
    storage
        .put_blob(
            "photos",
            "photo1",
            crate::blob::ResolvedScope::Derived("n1".to_string()),
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    // A normal sync cycle now reconciles the missing blob and clears the flag.
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

    assert!(
        expected_blob.exists(),
        "a later sync cycle must land the previously-missing blob at {}",
        expected_blob.display(),
    );
    assert_eq!(
        std::fs::read(&expected_blob).expect("read reconciled blob"),
        b"cover-bytes",
        "the reconciled file must hold the blob's plaintext bytes",
    );
    assert!(
        !backfill_pending(&db_b).await,
        "the cycle that lands every blob must clear the pending flag",
    );
}
