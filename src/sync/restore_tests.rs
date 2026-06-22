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

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::clock::SystemClock;
use crate::id_provider::SequentialIdProvider;
use crate::sync::restore::{restore_from_code, RestoreError};
use crate::sync::restore_code::{
    decode_restore_code, encode_restore_code, RestoreCode, RestoreCodeError, RestoreProvider,
};
use crate::sync::test_helpers::{test_synced_tables, NoopBlobSource};

/// A restore code carrying the given `lid`. The provider points at a loopback
/// endpoint nothing listens on, so if execution ever reached the network it would
/// fail at once — but a malicious `lid` is refused at decode, well before that.
fn restore_code_with_lid(lid: &str) -> String {
    let code = RestoreCode {
        v: 1,
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
        // A valid 64-byte signing key so decode reaches (and rejects on) the id,
        // not the key.
        sk: URL_SAFE_NO_PAD.encode([0xAB_u8; 64]),
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
        None,
        None,
        app_dir,
        Arc::new(SystemClock),
        ids,
        |_| Box::new(NoopBlobSource),
        |_| {},
    )
    .await
}

/// An `lid` containing `..` is refused at the decode boundary: `decode_restore_code`
/// returns the invalid-library-id error, so a decoded `RestoreCode` never carries a
/// traversal id. Driven end to end, the restore fails and creates nothing outside
/// the libraries root.
#[tokio::test]
async fn restore_rejects_parent_dir_lid_at_decode() {
    let encoded = restore_code_with_lid("../escape");
    assert!(
        matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidLibraryId(_))
        ),
        "decode must refuse a `..` lid with the invalid-library-id error",
    );

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // `app_dir/libraries/../escape` resolves to `app_dir/escape` — outside the
    // libraries root. The decode rejects the id, so it is never created.
    let escape_target = app_dir.join("escape");

    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "a traversal lid must fail the restore, got {result:?}",
    );
    assert!(
        !escape_target.exists(),
        "restore must not create a directory outside the libraries root at {}",
        escape_target.display(),
    );
}

/// An absolute `lid` escapes by replacing the base (`libraries`.join("/abs") ==
/// "/abs"). It is refused at the decode boundary, and the restore creates nothing
/// at the absolute target.
#[tokio::test]
async fn restore_rejects_absolute_lid_at_decode() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // An absolute path *inside* the temp dir so the test never writes to a real
    // shared location even if the guard regresses.
    let abs_escape = app_dir.join("abs_escape");
    let abs_lid = abs_escape.to_str().expect("utf8 path").to_string();

    let encoded = restore_code_with_lid(&abs_lid);
    assert!(
        matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidLibraryId(_))
        ),
        "decode must refuse an absolute lid with the invalid-library-id error",
    );

    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "an absolute lid must fail the restore, got {result:?}",
    );
    assert!(
        !abs_escape.exists(),
        "restore must not create a directory at an absolute lid path {}",
        abs_escape.display(),
    );
}

/// A lone `.` lid is refused at decode too: a trailing `.` component normalizes
/// away, so `libraries/.` would resolve to the data dir rather than a child of
/// `libraries/`. The decoder rejects it before it can name a directory.
#[tokio::test]
async fn restore_rejects_current_dir_lid_at_decode() {
    let encoded = restore_code_with_lid(".");
    assert!(
        matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidLibraryId(_))
        ),
        "decode must refuse a `.` lid with the invalid-library-id error",
    );

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "a `.` lid must fail the restore, got {result:?}",
    );
}

/// A normal `lid` decodes cleanly and the restore proceeds past the boundary (it
/// later fails on the unreachable cloud, not on the id), proving the decoder
/// rejects only unsafe ids and the directory the restore would create sits under
/// `libraries/`.
#[tokio::test]
async fn restore_accepts_a_normal_lid_past_decode() {
    let encoded = restore_code_with_lid("abc-123");
    let decoded = decode_restore_code(&encoded).expect("a normal lid decodes");
    assert_eq!(decoded.lid, "abc-123");

    // End to end the restore still fails — the S3 endpoint above is unreachable —
    // but it fails past the decode boundary, not at it: the id is accepted.
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        result.is_err(),
        "the unreachable cloud endpoint must fail the restore after the id is accepted",
    );
}
