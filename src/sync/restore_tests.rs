//! Restore rejects a path-traversal `lid` before touching the filesystem.
//!
//! A restore code is unsigned, so its `lid` (library id) is attacker-controlled.
//! `restore_from_cloud` turns it into a directory under `app_dir/libraries/` and,
//! on a bootstrap failure, recursively deletes that directory. An `lid` like
//! `../escape` or an absolute path would put that create/delete outside the
//! libraries root — arbitrary directory creation and recursive deletion. These
//! tests pin that the id is refused at the boundary, before any filesystem op.

use std::sync::Arc;

use crate::clock::SystemClock;
use crate::id_provider::SequentialIdProvider;
use crate::sync::restore::{restore_from_cloud, RestoreError, RestoreSource};
use crate::sync::test_helpers::{test_synced_tables, NoopBlobSource};

/// An S3 restore source whose endpoint refuses connections immediately, so any
/// network read fails fast. The directory creation a malicious `lid` triggers
/// runs *before* the first bucket read, so the traversal is observable without
/// the read mattering — but a fast-failing endpoint keeps the test deterministic
/// if execution ever reaches the network.
fn unreachable_s3_source() -> RestoreSource {
    RestoreSource::S3 {
        bucket: "bucket".to_string(),
        region: "us-east-1".to_string(),
        // Port 0 / loopback: nothing listens, so a connect fails at once.
        endpoint: Some("http://127.0.0.1:1".to_string()),
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
    }
}

/// A `lid` containing `..` must be refused before any filesystem op: nothing is
/// created outside the libraries root, and the call fails loudly with the typed
/// "invalid library id" error rather than proceeding to create_dir_all.
#[tokio::test]
async fn restore_rejects_parent_dir_lid_before_touching_disk() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // `app_dir/libraries/../escape` resolves to `app_dir/escape` — outside the
    // libraries root. Pre-fix, create_dir_all materializes it; post-fix the id is
    // rejected first and it never appears.
    let escape_target = app_dir.join("escape");

    let keypair = crate::keys::UserKeypair::generate();
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    let result = restore_from_cloud(
        "../escape",
        Some(&"aa".repeat(32)),
        "Evil",
        &test_synced_tables(),
        unreachable_s3_source(),
        &keypair,
        app_dir,
        Arc::new(SystemClock),
        ids,
        |_| Box::new(NoopBlobSource),
        |_| {},
    )
    .await;

    assert!(
        matches!(result, Err(RestoreError::InvalidLibraryId(_))),
        "a traversal lid must be refused with the invalid-library-id error, got {result:?}",
    );
    assert!(
        !escape_target.exists(),
        "restore must not create a directory outside the libraries root at {}",
        escape_target.display(),
    );
}

/// An absolute `lid` escapes by replacing the base (`libraries`.join("/abs") ==
/// "/abs"). It must be refused before any filesystem op, leaving the absolute
/// target absent.
#[tokio::test]
async fn restore_rejects_absolute_lid_before_touching_disk() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();
    // An absolute path *inside* the temp dir so the test never writes to a real
    // shared location even if the guard regresses.
    let abs_escape = app_dir.join("abs_escape");
    let abs_lid = abs_escape.to_str().expect("utf8 path").to_string();

    let keypair = crate::keys::UserKeypair::generate();
    let ids: crate::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    let result = restore_from_cloud(
        &abs_lid,
        Some(&"aa".repeat(32)),
        "Evil",
        &test_synced_tables(),
        unreachable_s3_source(),
        &keypair,
        app_dir,
        Arc::new(SystemClock),
        ids,
        |_| Box::new(NoopBlobSource),
        |_| {},
    )
    .await;

    assert!(
        matches!(result, Err(RestoreError::InvalidLibraryId(_))),
        "an absolute lid must be refused with the invalid-library-id error, got {result:?}",
    );
    assert!(
        !abs_escape.exists(),
        "restore must not create a directory at an absolute lid path {}",
        abs_escape.display(),
    );
}
