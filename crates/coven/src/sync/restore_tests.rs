//! A restore code's `lid` (library id) names a directory the restorer creates.
//!
//! A restore code is unsigned, so its `lid` is attacker-controlled.
//! `restore_from_cloud` turns it into a directory under `app_dir/libraries/` and,
//! on a bootstrap failure, recursively deletes that directory. An `lid` like
//! `../escape` or an absolute path would put that create/delete outside the
//! libraries root — arbitrary directory creation and recursive deletion. These
//! tests pin that the id is refused the moment the code is decoded, so it never
//! reaches the directory step: a decoded `RestoreCode` always carries a safe id.

use std::sync::Arc;

use crate::clock::SystemClock;
use crate::id_provider::SequentialIdProvider;
use crate::sync::restore::{restore_from_code, RestoreError};
use crate::sync::restore_code::{
    decode_restore_code, encode_restore_code, RestoreCode, RestoreCodeError, RestoreProvider,
};
use crate::sync::test_helpers::{test_migrations, test_synced_tables};

/// A restore code carrying the given `lid`. The provider points at a loopback
/// endpoint nothing listens on, so if execution ever reached the network it would
/// fail at once — but a malicious `lid` is refused at decode, well before that.
fn restore_code_with_lid(lid: &str) -> String {
    let code = RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        lid: lid.to_string(),
        ek: Some("aa".repeat(32)),
        name: "Evil".to_string(),
        provider: RestoreProvider::S3 {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            // Port 1 / loopback: nothing listens, so a connect fails at once.
            endpoint: Some("http://127.0.0.1:1".to_string()),
            key_prefix: None,
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        },
        // A real Ed25519 keypair's 64 bytes: a malicious `lid` is rejected at decode
        // before the key is touched, and a valid `lid` rebuilds this keypair and
        // proceeds to the cloud step (where the loopback endpoint fails it).
        sk: hex::encode(crate::keys::UserKeypair::generate().to_keypair_bytes()),
    };
    encode_restore_code(&code)
}

/// Drive the full restore path for a code string and return its result. A
/// malicious `lid` fails at decode before any cloud home is built, so the cloud
/// details never matter.
async fn restore_result_for(
    code_str: &str,
    app_dir: &std::path::Path,
) -> Result<crate::config::Config, RestoreError> {
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    restore_from_code(
        code_str,
        &test_synced_tables(),
        &test_migrations(),
        None,
        None,
        app_dir,
        Arc::new(SystemClock),
        ids,
        |_| {},
    )
    .await
}

/// Every traversal-shaped `lid` is refused at the decode boundary:
/// `decode_restore_code` returns `RestoreCodeError::InvalidLibraryId`, so a decoded
/// `RestoreCode` never carries a traversal id. Driven end to end, the decode error
/// propagates as `RestoreError::InvalidCode` and the restore creates nothing outside
/// the libraries root.
///
/// The cases share one mechanism and differ only in the malicious id and the
/// directory it would escape to, so they run as a table:
/// - `../escape`: `app_dir/libraries/../escape` resolves to `app_dir/escape`.
/// - an absolute path: `libraries`.join("/abs") == "/abs" replaces the base.
/// - `.`: a trailing `.` normalizes away, so `libraries/.` lands on the data dir.
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

    for (lid, escape_target) in cases {
        let encoded = restore_code_with_lid(lid);
        assert!(
            matches!(
                decode_restore_code(&encoded),
                Err(RestoreCodeError::InvalidLibraryId(_))
            ),
            "decode must refuse `{lid}` with InvalidLibraryId",
        );

        let result = restore_result_for(&encoded, app_dir).await;
        assert!(
            matches!(result, Err(RestoreError::InvalidCode(_))),
            "`{lid}` must fail the restore with the propagated decode error, got {result:?}",
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

/// A library already present locally is the data — re-running a restore for it adds
/// nothing, and the old code would delete its database and blobs during the
/// failure-cleanup once the snapshot download failed. The restore now refuses up
/// front with a typed error naming the library and leaves the existing files
/// untouched. The endpoint is unreachable so that, absent the guard, execution would
/// reach the snapshot download and the destructive cleanup — the guard stops it first.
#[tokio::test]
async fn restore_refuses_when_library_exists_and_leaves_it_untouched() {
    let encoded = restore_code_with_lid("abc-123");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A library with this id is already present locally, holding a database file
    // and a blob the restore must not touch.
    let library_dir = app_dir.join("libraries").join("abc-123");
    std::fs::create_dir_all(library_dir.join("storage")).expect("create existing library dir");
    let db_path = library_dir.join("library.db");
    let blob_path = library_dir.join("storage").join("cover.blob");
    std::fs::write(&db_path, b"existing-db-bytes").expect("seed existing db");
    std::fs::write(&blob_path, b"existing-blob-bytes").expect("seed existing blob");

    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(RestoreError::LibraryExists(ref id)) if id == "abc-123"),
        "restore must refuse a library already present locally, got {result:?}",
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

/// A normal `lid` decodes and the restore reaches the cloud step, where it fails on
/// the unreachable endpoint (`RestoreError::Snapshot`) rather than on the id —
/// proving the decoder rejects only unsafe ids and the directory the restore would
/// create sits under `libraries/`.
#[tokio::test]
async fn restore_accepts_a_normal_lid_past_decode() {
    let encoded = restore_code_with_lid("abc-123");
    let decoded = decode_restore_code(&encoded).expect("a normal lid decodes");
    assert_eq!(decoded.lid, "abc-123");

    // End to end the restore still fails — the S3 endpoint above is unreachable —
    // but it fails at the snapshot download past the decode boundary, not at the id.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(RestoreError::Snapshot(_))),
        "the unreachable cloud endpoint must fail the restore at the snapshot download, got {result:?}",
    );
}
