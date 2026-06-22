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
use crate::sync::push::SCHEMA_VERSION;
use crate::sync::snapshot::{
    bootstrap_from_snapshot, create_snapshot, push_snapshot, SNAPSHOT_BLOB_BACKFILL_PENDING,
};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

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
        None,
        None,
        Arc::new(SystemClock),
        ids,
        |_| Box::new(NoopBlobSource),
        |_| {},
    )
    .await
}

/// A `library_id` containing `..` is refused at the decode boundary: `decode`
/// returns the invalid-library-id error, so a decoded `InviteCode` never carries a
/// traversal id. Driven end to end, the join fails and creates nothing outside the
/// libraries root.
#[tokio::test]
async fn join_rejects_parent_dir_library_id_at_decode() {
    let encoded = encode(&invite_code_with_library_id("../escape"));
    assert!(
        matches!(
            crate::join_code::decode(&encoded),
            Err(JoinCodeError::InvalidLibraryId(_))
        ),
        "decode must refuse a `..` library_id with the invalid-library-id error",
    );

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // `app_dir/libraries/../escape` resolves to `app_dir/escape` — outside the
    // libraries root. The decode rejects the id, so it is never created.
    let escape_target = app_dir.join("escape");

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "a traversal library_id must fail the join, got {result:?}",
    );
    assert!(
        !escape_target.exists(),
        "join must not create a directory outside the libraries root at {}",
        escape_target.display(),
    );
}

/// An absolute `library_id` escapes by replacing the base (`libraries`.join("/abs")
/// == "/abs"). It is refused at the decode boundary, and the join creates nothing
/// at the absolute target.
#[tokio::test]
async fn join_rejects_absolute_library_id_at_decode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let abs_escape = app_dir.join("abs_escape");
    let abs_id = abs_escape.to_str().expect("utf8 path").to_string();

    let encoded = encode(&invite_code_with_library_id(&abs_id));
    assert!(
        matches!(
            crate::join_code::decode(&encoded),
            Err(JoinCodeError::InvalidLibraryId(_))
        ),
        "decode must refuse an absolute library_id with the invalid-library-id error",
    );

    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "an absolute library_id must fail the join, got {result:?}",
    );
    assert!(
        !abs_escape.exists(),
        "join must not create a directory at an absolute library_id path {}",
        abs_escape.display(),
    );
}

/// A lone `.` library_id is refused at decode too: a trailing `.` component
/// normalizes away, so `libraries/.` would resolve to the data dir rather than a
/// child of `libraries/`. The decoder rejects it before it can name a directory.
#[tokio::test]
async fn join_rejects_current_dir_library_id_at_decode() {
    let encoded = encode(&invite_code_with_library_id("."));
    assert!(
        matches!(
            crate::join_code::decode(&encoded),
            Err(JoinCodeError::InvalidLibraryId(_))
        ),
        "decode must refuse a `.` library_id with the invalid-library-id error",
    );

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "a `.` library_id must fail the join, got {result:?}",
    );
}

/// A normal `library_id` decodes cleanly and the join proceeds past the boundary
/// (it later fails on the unreachable cloud, not on the id), proving the decoder
/// rejects only unsafe ids and the directory the join would create sits under
/// `libraries/`.
#[tokio::test]
async fn join_accepts_a_normal_library_id_past_decode() {
    let encoded = encode(&invite_code_with_library_id("abc-123"));
    let decoded = crate::join_code::decode(&encoded).expect("a normal id decodes");
    assert_eq!(decoded.library_id, "abc-123");

    // End to end the join still fails — the S3 endpoint above is bogus — but it
    // fails past the decode boundary, not at it: the id is accepted.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = join_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "the bogus cloud endpoint must fail the join after the id is accepted",
    );
}

#[tokio::test]
async fn joined_device_first_cycle_does_not_clobber_the_shared_snapshot() {
    let enc = CloudCipher::Encrypted(EncryptionService::new_with_key(&[7u8; 32]));
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
    push_snapshot(&storage, empty_snap, "A", HashMap::new(), 0, &SystemClock)
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

    // The shared snapshot before B joins. B must not replace it.
    let snapshot_before = storage.get_snapshot().await.expect("snapshot present");

    // Device B joins through the real path: bootstrap from the snapshot, then
    // pull the changesets published after it.
    let (_tmp_b, lib_b) = temp_library_dir();
    let boot = bootstrap_from_snapshot(&storage, &enc, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
        &NoopBlobSource,
    )
    .await
    .expect("B open_db_and_pull");

    let (db_b, _stamper) =
        Database::open(&lib_b.db_path(), tables.clone(), "B".to_string(), |_c| {
            Ok(())
        })
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
        "B",
        &b_hlc,
        &SystemClock,
        &db_b,
        &enc_lock,
        &keypair,
        &lib_b,
        None,
        &NoopBlobSource,
        None,
    )
    .await
    .expect("B sync cycle");

    // A just-joined device with no local changes must leave the shared snapshot
    // untouched. Today the join path persists no `snapshot_seq`, so the cycle
    // trips `is_initial_sync` and overwrites the owner's snapshot with B's own.
    let snapshot_after = storage.get_snapshot().await.expect("snapshot present");
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
/// bootstrapped device has the photo row but the file at its `local_path` is
/// absent — a synced album renders a placeholder cover. Asserts the file lands.
#[tokio::test]
async fn bootstrap_backfills_blob_files_for_snapshot_rows() {
    let enc = CloudCipher::Encrypted(EncryptionService::new_with_key(&[9u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

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
    push_snapshot(&storage, snapshot, "A", HashMap::new(), 1, &SystemClock)
        .await
        .expect("push snapshot");

    // The cover blob exists in the cloud (uploaded when A first imported the
    // album), keyed `photos/photo1` as `PhotoBlobSource` maps a cover row. The
    // mock ignores the scope, so any resolved scope seeds it.
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

    // Device B bootstraps from the snapshot, then runs the real bootstrap path
    // with a plan that maps `note_photos` rows to blobs under B's library dir.
    let (_tmp_b, lib_b) = temp_library_dir();
    let blob_dir = lib_b.join("photos");
    let plan = PhotoBlobSource {
        dir: blob_dir.clone(),
    };
    let expected_blob = blob_dir.join("photo1");

    let boot = bootstrap_from_snapshot(&storage, &enc, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
        &plan,
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
    db.get_sync_state(SNAPSHOT_BLOB_BACKFILL_PENDING)
        .await
        .expect("read backfill flag")
        .is_some_and(|v| !v.is_empty())
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
    let enc = CloudCipher::Encrypted(EncryptionService::new_with_key(&[11u8; 32]));
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();

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
    push_snapshot(&storage, snapshot, "A", HashMap::new(), 1, &SystemClock)
        .await
        .expect("push snapshot");

    // Unlike the happy-path test above, the cover blob is NOT in the cloud yet at
    // bootstrap time (e.g. A's upload of it hadn't landed). So the bootstrap's
    // download attempt fails.
    let (_tmp_b, lib_b) = temp_library_dir();
    let blob_dir = lib_b.join("photos");
    let plan = PhotoBlobSource {
        dir: blob_dir.clone(),
    };
    let expected_blob = blob_dir.join("photo1");

    let boot = bootstrap_from_snapshot(&storage, &enc, &lib_b.db_path())
        .await
        .expect("B bootstrap");
    open_db_and_pull(
        &lib_b.db_path(),
        &tables,
        "B",
        None,
        &storage,
        &boot.cursors,
        &lib_b,
        &plan,
    )
    .await
    .expect("B open_db_and_pull");

    // After bootstrap the file is absent and the pending flag is set: the catalog
    // landed, the blob did not, and the cycle must reconcile it later.
    assert!(
        !expected_blob.exists(),
        "the cover blob must be absent after a bootstrap whose download failed",
    );
    let (db_b, _stamper) =
        Database::open(&lib_b.db_path(), tables.clone(), "B".to_string(), |_c| {
            Ok(())
        })
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
        "B",
        &b_hlc,
        &SystemClock,
        &db_b,
        &enc_lock,
        &keypair,
        &lib_b,
        None,
        &plan,
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
