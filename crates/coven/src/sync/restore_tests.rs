//! A restore code's `sid` (store id) names a directory the restorer creates.
//!
//! A restore code is unsigned, so its `sid` is attacker-controlled.
//! `restore_from_cloud` turns it into a directory under `app_dir/stores/` and,
//! on a bootstrap failure, recursively deletes that directory. An `sid` like
//! `../escape` or an absolute path would put that create/delete outside the
//! stores root — arbitrary directory creation and recursive deletion. These
//! tests pin that the id is refused the moment the code is decoded, so it never
//! reaches the directory step: a decoded `RestoreCode` always carries a safe id.

use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::SystemClock;
use crate::config::HomeStorage;
use crate::database::DbError;
use crate::id_provider::SequentialIdProvider;
use crate::keys::{StoreKeys, UserKeypair};
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::store_dir::StoreLayout;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::join::{
    bootstrap_and_save_store, cleanup_after_bootstrap_failure, BootstrapContext, BootstrapError,
};
use crate::sync::restore::restore_from_code;
use crate::sync::restore_code::{
    decode_restore_code, encode_restore_code, RestoreCode, RestoreCodeError,
};
use crate::sync::snapshot::{create_snapshot, push_snapshot, SnapshotBlobPreflight};
use crate::sync::test_helpers::{open_test_db, test_migrations, test_synced_tables};

/// A restore code carrying the given `sid`. The provider points at a loopback
/// endpoint nothing listens on, so if execution ever reached the network it would
/// fail at once — but a malicious `sid` is refused at decode, well before that.
fn restore_code_with_sid(sid: &str) -> String {
    let code = RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: sid.to_string(),
        ek: Some("aa".repeat(32)),
        name: "Evil".to_string(),
        provider: CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            // Port 1 / loopback: nothing listens, so a connect fails at once.
            endpoint: Some("http://127.0.0.1:1".to_string()),
            key_prefix: None,
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        },
        // A real Ed25519 keypair's 64 bytes: a malicious `sid` is rejected at decode
        // before the key is touched, and a valid `sid` rebuilds this keypair and
        // proceeds to the cloud step (where the loopback endpoint fails it).
        sk: hex::encode(crate::keys::UserKeypair::generate().to_keypair_bytes()),
    };
    encode_restore_code(&code)
}

/// Drive the full restore path for a code string and return its result. A
/// malicious `sid` fails at decode before any cloud home is built, so the cloud
/// details never matter.
async fn restore_result_for(
    code_str: &str,
    app_dir: &std::path::Path,
) -> Result<crate::config::Config, BootstrapError> {
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    restore_from_code(
        code_str,
        &test_synced_tables(),
        &test_migrations(),
        None,
        None,
        &crate::store_dir::StoreLayout::new(app_dir),
        Arc::new(SystemClock),
        ids,
        |_| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await
}

/// Every traversal-shaped `sid` is refused at the decode boundary:
/// `decode_restore_code` returns `RestoreCodeError::InvalidStoreId`, so a decoded
/// `RestoreCode` never carries a traversal id. Driven end to end, the decode error
/// propagates as `BootstrapError::InvalidCode` and the restore creates nothing outside
/// the stores root.
///
/// The cases share one mechanism and differ only in the malicious id and the
/// directory it would escape to, so they run as a table:
/// - `../escape`: `app_dir/stores/../escape` resolves to `app_dir/escape`.
/// - an absolute path: `stores`.join("/abs") == "/abs" replaces the base.
/// - `.`: a trailing `.` normalizes away, so `stores/.` lands on the data dir.
#[tokio::test]
async fn restore_rejects_traversal_lid_at_decode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // The absolute case escapes to a path *inside* the temp dir, so even a
    // regressed guard never writes to a real shared location.
    let parent_escape = app_dir.join("escape");
    let abs_escape = app_dir.join("abs_escape");
    let abs_lid = abs_escape.to_str().expect("utf8 path").to_string();

    let cases: [(&str, Option<&std::path::Path>); 3] = [
        ("../escape", Some(parent_escape.as_path())),
        (&abs_lid, Some(abs_escape.as_path())),
        (".", None),
    ];

    for (sid, escape_target) in cases {
        let encoded = restore_code_with_sid(sid);
        assert!(
            matches!(
                decode_restore_code(&encoded),
                Err(RestoreCodeError::InvalidStoreId(_))
            ),
            "decode must refuse `{sid}` with InvalidStoreId",
        );

        let result = restore_result_for(&encoded, app_dir).await;
        assert!(
            matches!(result, Err(BootstrapError::InvalidCode(_))),
            "`{sid}` must fail the restore with the propagated decode error, got {result:?}",
        );
        if let Some(target) = escape_target {
            assert!(
                !target.exists(),
                "restore must not create an escape directory at {}",
                target.display(),
            );
        }
    }
}

/// A store already present locally is the data — re-running a restore for it adds
/// nothing, and the old code would delete its database and blobs during the
/// failure-cleanup once the snapshot download failed. The restore now refuses up
/// front with a typed error naming the store and leaves the existing files
/// untouched. The endpoint is unreachable so that, absent the guard, execution would
/// reach the snapshot download and the destructive cleanup — the guard stops it first.
#[tokio::test]
async fn restore_refuses_when_store_exists_and_leaves_it_untouched() {
    let encoded = restore_code_with_sid("abc-123");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A store with this id is already present locally, holding a database file
    // and a blob the restore must not touch.
    let store_dir = app_dir.join("stores").join("abc-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create existing store dir");
    let db_path = store_dir.join("store.db");
    let blob_path = store_dir.join("storage").join("cover.blob");
    std::fs::write(&db_path, b"existing-db-bytes").expect("seed existing db");
    std::fs::write(&blob_path, b"existing-blob-bytes").expect("seed existing blob");

    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "abc-123"),
        "restore must refuse a store already present locally, got {result:?}",
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

/// A normal `sid` decodes and the restore reaches the cloud step, where it fails on
/// the unreachable endpoint (`BootstrapError::Snapshot`) rather than on the id —
/// proving the decoder rejects only unsafe ids and the directory the restore would
/// create sits under `stores/`.
#[tokio::test]
async fn restore_accepts_a_normal_lid_past_decode() {
    let encoded = restore_code_with_sid("abc-123");
    let decoded = decode_restore_code(&encoded).expect("a normal sid decodes");
    assert_eq!(decoded.sid, "abc-123");

    // End to end the restore still fails — the S3 endpoint above is unreachable —
    // but it fails at the snapshot download past the decode boundary, not at the id.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Snapshot(_))),
        "the unreachable cloud endpoint must fail the restore at the snapshot download, got {result:?}",
    );
}

/// Drive `restore_from_code` with a caller-supplied cancel signal and status
/// callback. Returns the restore result; the caller inspects the store directory
/// and keyring afterward.
async fn restore_with_cancel(
    code: &str,
    app_dir: &std::path::Path,
    cancel: &tokio::sync::watch::Receiver<bool>,
    on_status: impl Fn(&str),
) -> Result<crate::config::Config, BootstrapError> {
    restore_from_code(
        code,
        &test_synced_tables(),
        &test_migrations(),
        None,
        None,
        &crate::store_dir::StoreLayout::new(app_dir),
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("dev")),
        on_status,
        cancel,
    )
    .await
}

/// A restore whose cancel signal is already set never reaches the network: the
/// first phase-boundary check returns `Cancelled`, the shared failure-cleanup
/// removes the store directory it created, and the keyring is never written — a
/// cancelled restore leaves no residue, the same guarantee a failed one gives.
#[tokio::test]
async fn restore_cancelled_before_snapshot_leaves_no_residue() {
    crate::keys::test_keyring::install();
    let encoded = restore_code_with_sid("cancel-preset");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    let (tx, cancel) = tokio::sync::watch::channel(false);
    tx.send(true).expect("prime cancel before restore starts");

    let result = restore_with_cancel(&encoded, app_dir, &cancel, |_| {}).await;

    assert!(
        matches!(result, Err(BootstrapError::Cancelled)),
        "a pre-set cancel must stop the restore with Cancelled, got {result:?}",
    );
    let store_dir = app_dir.join("stores").join("cancel-preset");
    assert!(
        !store_dir.exists(),
        "a cancelled restore must leave no store directory at {}",
        store_dir.display(),
    );
    let store_keys = crate::keys::StoreKeys::new("cancel-preset".to_string());
    assert_eq!(
        store_keys.get_encryption_key().expect("read keyring"),
        None,
        "a cancelled restore must not write the store's encryption key",
    );
    assert!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_none(),
        "a cancelled restore must not leave cloud home credentials in the keyring",
    );
}

/// A cancel delivered through the status callback while the restore runs — not
/// pre-set — is honored at the next phase boundary, with the same no-residue
/// guarantee: the store directory is created and then removed by the
/// failure-cleanup.
#[tokio::test]
async fn restore_cancelled_via_status_callback_leaves_no_residue() {
    crate::keys::test_keyring::install();
    let encoded = restore_code_with_sid("cancel-status");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    let (tx, cancel) = tokio::sync::watch::channel(false);
    // The cancel fires from inside the status callback the moment the restore
    // reports its first progress: a cancel arriving mid-flow via the host's
    // reporting channel, the shape a real UI cancel takes.
    let on_status = move |status: &str| {
        if status.contains("Preparing restore") {
            tx.send(true).expect("deliver cancel via status callback");
        }
    };

    let result = restore_with_cancel(&encoded, app_dir, &cancel, on_status).await;

    assert!(
        matches!(result, Err(BootstrapError::Cancelled)),
        "a cancel delivered via the status callback must stop the restore with Cancelled, got {result:?}",
    );
    let store_dir = app_dir.join("stores").join("cancel-status");
    assert!(
        !store_dir.exists(),
        "a cancelled restore must leave no store directory at {}",
        store_dir.display(),
    );
}

/// A failed restore must not leave a store directory behind that blocks a
/// retry: a second `restore_from_code` attempt for the same `sid` must reach
/// the same failure again, never `StoreExists`. `restore_from_code`'s signing-
/// key import runs only after `restore_from_cloud` returns `Ok`, so an import
/// failure needs this exact postcondition too — nothing durable survives a
/// failed attempt, so the next attempt starts clean.
#[tokio::test]
async fn failed_restore_does_not_block_a_retry_with_store_exists() {
    let encoded = restore_code_with_sid("retry-postcondition-test");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    let first = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(first, Err(BootstrapError::Snapshot(_))),
        "the unreachable cloud endpoint must fail the first attempt at the snapshot download, got {first:?}",
    );

    let second = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(second, Err(BootstrapError::Snapshot(_))),
        "a retry after a failed attempt must reach the same failure again, not StoreExists, got {second:?}",
    );
}

/// A failure at the very last step of bootstrap — saving `config.yaml`, after
/// the encryption key and the cloud-home credentials are already written to the
/// keyring (steps 7 and 8) — must roll back both keyring accounts, not just
/// whichever the OLD code happened to reach. The public entry points
/// (`join_store`, `restore_from_cloud`) refuse to run at all when the store
/// directory already exists, so there is no way to pre-seed a conflicting path
/// inside it before calling them; this drives `bootstrap_and_save_store`
/// directly — the same function those entry points call — and blocks its
/// final write by seeding a directory at the exact path `config.yaml` needs.
///
/// The store is chain-less (no membership entries published), so `open_db_and_pull`
/// takes the no-owner-to-pin path and this test needs no invite/membership
/// setup — only a snapshot the owner published, mirroring the bootstrap tests
/// in `join_tests.rs`.
#[tokio::test]
async fn late_step_failure_after_both_keyring_writes_rolls_back_both() {
    crate::keys::test_keyring::install();

    let store_id = "late-step-rollback-test";
    let tmp = tempfile::tempdir().expect("temp dir");
    let layout = StoreLayout::new(tmp.path());
    let store_dir = layout.store_dir(store_id);
    // No exists-guard runs here, so the store dir and its blocking content can
    // be seeded directly, unlike through the public entry points.
    std::fs::create_dir_all(&*store_dir).expect("create store dir directly");
    std::fs::create_dir_all(store_dir.config_path())
        .expect("seed a directory at the config path to block the final write");

    let cloud = crate::InMemoryCloudHome::new();
    let owner_keypair = UserKeypair::generate();
    let cipher = CloudCipher::Plaintext;
    let blob_paths = BlobPathScheme::for_storage(HomeStorage::Browsable);
    let owner_storage = CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        owner_keypair.clone(),
    );

    let tables = test_synced_tables();
    let db = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("create owner snapshot");
    push_snapshot(
        &owner_storage,
        store_id,
        snapshot,
        "owner-device",
        HashMap::new(),
        0,
        db.schema_version(),
        &owner_keypair,
        &SystemClock,
        SnapshotBlobPreflight {
            db: &db,
            blobs: &[],
        },
    )
    .await
    .expect("push owner snapshot");

    let joiner_storage = CloudSyncStorage::new(
        Arc::new(cloud) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        UserKeypair::generate(),
    );
    let store_keys = StoreKeys::new(store_id.to_string());
    let join_info = CloudHomeJoinInfo::S3 {
        bucket: "b".to_string(),
        region: "us-east-1".to_string(),
        endpoint: None,
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
        key_prefix: None,
    };

    let result = bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        Some(&"bb".repeat(32)),
        &store_dir,
        store_id,
        "device-late",
        BootstrapContext::Restore,
        &tables,
        &test_migrations(),
        &join_info,
        "Late Step Test",
        &store_keys,
        &|_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await;

    let err = result.expect_err("the blocked config.yaml write must fail bootstrap");

    // The meaningful precondition: steps 7 and 8 ran and wrote both keyring
    // accounts before the failure at step 9.
    assert!(
        store_keys
            .get_encryption_key()
            .expect("read keyring")
            .is_some(),
        "the encryption key must have been written before the late failure",
    );
    assert!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_some(),
        "the cloud home credentials must have been written before the late failure",
    );

    let wrapped = cleanup_after_bootstrap_failure(&store_dir, &store_keys, err);
    assert!(
        !matches!(wrapped, BootstrapError::Cleanup { .. }),
        "cleanup of a directory blocked only by its own contents must fully succeed, got {wrapped:?}",
    );
    assert!(
        !store_dir.exists(),
        "the store dir, including the blocking config.yaml directory, must be fully removed",
    );
    assert!(
        store_keys
            .get_encryption_key()
            .expect("read keyring")
            .is_none(),
        "the encryption key must be rolled back",
    );
    assert!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_none(),
        "the cloud home credentials must be rolled back",
    );
}
