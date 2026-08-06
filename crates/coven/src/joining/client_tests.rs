use super::*;

/// A store whose `config.yaml` marker is present is a completed store: the
/// guard refuses it with `StoreExists` and touches nothing — neither the
/// directory nor the keyring entries a live store depends on.
#[test]
fn guard_refuses_a_completed_store_and_leaves_it_untouched() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let store_dir = StoreDir::new(tmp.path().join("completed"));
    std::fs::create_dir_all(&*store_dir).expect("create store dir");
    std::fs::write(store_dir.config_path(), b"store_id: completed\n")
        .expect("seed completion marker");
    let store_keys = StoreKeys::bind("guard-completed-test".to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    custody
        .persist(&MasterKeyring::generate())
        .expect("seed the master key");
    let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);

    let cleanup = BootstrapCleanup::new(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    let result = cleanup.refuse_completed_or_clear("guard-completed-test");

    assert!(
        matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "guard-completed-test"),
        "a completed store must be refused with StoreExists, got {result:?}",
    );
    assert!(store_dir.config_path().exists(), "the marker is untouched");
    assert!(
        custody.unlock().expect("read master key").is_some(),
        "a refused completed store keeps its keyring entries",
    );
}

/// A store directory with no `config.yaml` marker is a torn bootstrap a
/// crash interrupted before completion: the guard clears it — the directory
/// and the store-scoped keyring entries — and returns `Ok` so the caller
/// retries from a clean slate.
#[test]
fn guard_clears_a_torn_bootstrap_and_lets_the_retry_proceed() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let store_dir = StoreDir::new(tmp.path().join("torn"));
    std::fs::create_dir_all(&*store_dir).expect("create store dir");
    // Partial bootstrap residue: a torn database image, no config marker.
    std::fs::write(store_dir.db_path(), b"half-written-db").expect("seed torn db");
    let store_keys = StoreKeys::bind("guard-torn-test".to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    custody
        .persist(&MasterKeyring::generate())
        .expect("seed the master key");
    let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
    identity_custody
        .persist(&UserKeypair::generate())
        .expect("seed the identity");
    store_keys
        .set_cloud_home_credentials(&CloudHomeCredentials::S3 {
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        })
        .expect("seed cloud home credentials");

    let cleanup = BootstrapCleanup::new(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    let result = cleanup.refuse_completed_or_clear("guard-torn-test");

    assert!(
        result.is_ok(),
        "a torn bootstrap clears and proceeds, got {result:?}"
    );
    assert!(!store_dir.exists(), "the torn directory was removed");
    assert!(
        custody.unlock().expect("read master key").is_none(),
        "the torn store's master key was cleared",
    );
    assert!(
        identity_custody.unlock().expect("read identity").is_none(),
        "the torn store's identity was cleared",
    );
    assert!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_none(),
        "the torn store's cloud home credentials were cleared",
    );
}

/// When the post-failure directory cleanup itself fails, both failures are
/// carried: `cleanup` records why the removal failed and `cause` preserves
/// the ORIGINAL bootstrap error as a value — not flattened into a string.
/// Join and restore both route their failure path through this one helper,
/// so exercising it covers both flows' cleanup behavior. The dir removal is
/// the failure here: a *file* sits where the store dir should be, so
/// `remove_dir_all` fails with something other than not-found (which is
/// tolerated, not a failure — see the dedicated test below).
#[test]
fn cleanup_failure_carries_the_original_bootstrap_cause() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let blocked = StoreDir::new(tmp.path().join("blocked-by-a-file"));
    std::fs::write(&*blocked, b"not a directory").expect("seed a file at the store dir path");
    let store_keys = StoreKeys::bind("cleanup-failure-cause-test".to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &blocked);
    let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &blocked);

    let cleanup = BootstrapCleanup::new(
        &blocked,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    let wrapped = cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

    match wrapped {
        BootstrapError::Cleanup { cleanup, cause } => {
            assert!(!cleanup.is_empty(), "the removal failure is recorded");
            assert!(
                matches!(*cause, BootstrapError::Database(ref m) if m == "bootstrap boom"),
                "the original bootstrap cause is preserved as a value, got {cause:?}",
            );
        }
        other => panic!("a failed cleanup must yield Cleanup, got {other:?}"),
    }
}

/// When the cleanup succeeds, the original bootstrap error propagates
/// unchanged — no `Cleanup` wrapper — and the partial store dir is gone.
#[test]
fn successful_cleanup_returns_the_cause_unchanged() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let store_dir = StoreDir::new(tmp.path().join("to-remove"));
    std::fs::create_dir_all(&*store_dir).expect("create store dir");
    let store_keys = StoreKeys::bind("successful-cleanup-test".to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);

    let cleanup = BootstrapCleanup::new(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    let returned = cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

    assert!(
        matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
        "a clean removal returns the cause unchanged, got {returned:?}",
    );
    assert!(!store_dir.exists(), "the partial store dir was removed");
}

/// A bootstrap failure before `create_dir_all` ever ran (e.g. the OAuth
/// persist or cloud-home construction failed first) leaves no store dir to
/// remove. `remove_dir_all` on a path that never existed returns
/// `NotFound`, and that must be tolerated — not folded into `Cleanup` — so a
/// pre-directory failure still reports as the plain original cause.
#[test]
fn cleanup_tolerates_a_store_dir_that_was_never_created() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let never_created = StoreDir::new(tmp.path().join("never-created"));
    let store_keys = StoreKeys::bind("never-created-dir-test".to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &never_created);
    let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &never_created);

    let cleanup = BootstrapCleanup::new(
        &never_created,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    let returned = cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

    assert!(
        matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
        "a missing store dir must not itself count as a cleanup failure, got {returned:?}",
    );
}

/// The extended rollback also removes the store-scoped keyring accounts —
/// the encryption master key, this store's identity, and the cloud-home
/// credentials (which is also where an OAuth token lands, via
/// `set_cloud_home_oauth_tokens`) — not just the directory. Seed all three
/// the way a partial bootstrap would have written them, then assert
/// cleanup leaves none behind.
#[test]
fn cleanup_also_removes_both_keyring_accounts() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("temp dir");
    let store_dir = StoreDir::new(tmp.path().join("keyring-cleanup-test"));
    std::fs::create_dir_all(&*store_dir).expect("create store dir");
    let store_keys = StoreKeys::bind("keyring-cleanup-test".to_string());
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    custody
        .persist(&MasterKeyring::generate())
        .expect("seed the master key via custody");
    let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
    identity_custody
        .persist(&coven_keys::keys::UserKeypair::generate())
        .expect("seed this store's identity via custody");
    store_keys
        .set_cloud_home_credentials(&CloudHomeCredentials::S3 {
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        })
        .expect("seed cloud home credentials");

    let cleanup = BootstrapCleanup::new(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
    );
    let returned = cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

    assert!(
        matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
        "a clean removal returns the cause unchanged, got {returned:?}",
    );
    assert!(!store_dir.exists(), "the partial store dir was removed");
    assert_eq!(
        store_keys.get_encryption_key().expect("read keyring"),
        None,
        "the encryption key must be removed from the keyring",
    );
    assert!(
        identity_custody
            .unlock()
            .expect("read identity custody")
            .is_none(),
        "this store's identity must be removed from custody",
    );
    assert!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_none(),
        "the cloud home credentials must be removed from the keyring",
    );
}

/// Only S3 maps to a stored value; every other provider returns `None` so
/// the join never overwrites an already-saved OAuth token (or a CloudKit
/// container) with credentials.
#[test]
fn derive_credentials_only_stores_for_s3() {
    let s3 = CloudHomeJoinInfo::S3 {
        bucket: "b".to_string(),
        region: "r".to_string(),
        endpoint: None,
        key_prefix: None,
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
    };
    match derive_credentials(&s3) {
        Some(CloudHomeCredentials::S3 {
            access_key,
            secret_key,
        }) => {
            assert_eq!(access_key, "ak");
            assert_eq!(secret_key, "sk");
        }
        other => panic!("expected Some(S3), got {other:?}"),
    }

    for oauth in [
        CloudHomeJoinInfo::GoogleDrive {
            folder_id: "f".to_string(),
        },
        CloudHomeJoinInfo::Dropbox {
            folder_path: "f".to_string(),
        },
        CloudHomeJoinInfo::OneDrive {
            drive_id: "d".to_string(),
            folder_id: "f".to_string(),
        },
        CloudHomeJoinInfo::CloudKit,
    ] {
        assert!(
            derive_credentials(&oauth).is_none(),
            "non-S3 provider must not map to stored credentials"
        );
    }
}
