//! A restore code's `sid` (store id) names a directory the restorer creates.
//!
//! A restore code is unsigned, so its `sid` is attacker-controlled.
//! `restore_from_cloud` turns it into a directory under `app_dir/stores/` and,
//! on a bootstrap failure, recursively deletes that directory. An `sid` like
//! `../escape` or an absolute path would put that create/delete outside the
//! stores root — arbitrary directory creation and recursive deletion. These
//! tests pin that the id is refused the moment the code is decoded, so it never
//! reaches the directory step: a decoded `RestoreCode` always carries a safe id.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use crate::blob::{CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::config::HomeStorage;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::id_provider::SequentialIdProvider;
use crate::join_code::MembershipFloor;
use crate::keys::{StoreKeys, UserKeypair};
use crate::storage::cloud::cloudkit::{CloudKitOps, CloudKitScope, CloudKitShare};
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::storage::cloud::{
    CloudHeadCreateError, CloudHeadReplaceError, CloudHeadVersion, CloudHomeError,
    CloudVersionedHead,
};
use crate::store_dir::StoreLayout;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::hlc::Hlc;
use crate::sync::join::{
    bootstrap_and_save_store, cleanup_after_bootstrap_failure, open_db_and_pull, BootstrapContext,
    BootstrapError,
};
use crate::sync::membership::MembershipChain;
use crate::sync::restore::restore_from_code;
use crate::sync::restore_code::{
    decode_restore_code, encode_restore_code, RestoreCode, RestoreCodeError,
};
use crate::sync::session::BlobDecl;
use crate::sync::snapshot::{bootstrap_from_snapshot, create_snapshot};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::run_test_cycle;
use crate::sync::test_helpers::{
    append_membership_entry, exec, open_test_db, open_test_db_with_blob, pubkey_hex,
    publish_membership_chain_head, temp_store_dir, test_migrations, test_synced_tables,
    test_synced_tables_with_blob, MockSyncStorage,
};

struct RestoreCloudKitOps {
    records: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
    versions: Mutex<HashMap<(CloudKitScope, String), u64>>,
}

impl RestoreCloudKitOps {
    fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            versions: Mutex::new(HashMap::new()),
        }
    }
}

impl CloudKitOps for RestoreCloudKitOps {
    fn write_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), CloudHomeError> {
        self.records
            .lock()
            .unwrap()
            .insert((scope.clone(), key.to_string()), data);
        Ok(())
    }

    fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.records
            .lock()
            .unwrap()
            .get(&(scope.clone(), key.to_string()))
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
    }

    fn list_records(
        &self,
        scope: &CloudKitScope,
        prefix: &str,
    ) -> Result<Vec<String>, CloudHomeError> {
        let mut keys: Vec<_> = self
            .records
            .lock()
            .unwrap()
            .keys()
            .filter(|(record_scope, key)| record_scope == scope && key.starts_with(prefix))
            .map(|(_, key)| key.clone())
            .collect();
        keys.sort();
        Ok(keys)
    }

    fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError> {
        self.records
            .lock()
            .unwrap()
            .remove(&(scope.clone(), key.to_string()));
        self.versions
            .lock()
            .unwrap()
            .remove(&(scope.clone(), key.to_string()));
        Ok(())
    }

    fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .contains_key(&(scope.clone(), key.to_string())))
    }

    fn read_versioned_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
    ) -> Result<CloudVersionedHead, CloudHomeError> {
        let record = (scope.clone(), key.to_string());
        let bytes = self
            .records
            .lock()
            .unwrap()
            .get(&record)
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        let version = self
            .versions
            .lock()
            .unwrap()
            .get(&record)
            .copied()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        Ok(CloudVersionedHead {
            bytes,
            version: CloudHeadVersion::from_provider(version.to_string())?,
        })
    }

    fn create_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        data: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
        let record = (scope.clone(), key.to_string());
        let mut records = self.records.lock().unwrap();
        if records.contains_key(&record) {
            return Err(CloudHeadCreateError::AlreadyExists);
        }
        records.insert(record.clone(), data.clone());
        self.versions.lock().unwrap().insert(record, 1);
        Ok(CloudVersionedHead {
            bytes: data,
            version: CloudHeadVersion::from_provider("1".to_string())?,
        })
    }

    fn replace_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        expected: &CloudHeadVersion,
        data: Vec<u8>,
    ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
        let record = (scope.clone(), key.to_string());
        let mut versions = self.versions.lock().unwrap();
        let current = versions
            .get(&record)
            .copied()
            .ok_or(CloudHeadReplaceError::VersionMismatch)?;
        if expected.as_provider() != current.to_string() {
            return Err(CloudHeadReplaceError::VersionMismatch);
        }
        let next = current + 1;
        self.records
            .lock()
            .unwrap()
            .insert(record.clone(), data.clone());
        versions.insert(record, next);
        Ok(CloudVersionedHead {
            bytes: data,
            version: CloudHeadVersion::from_provider(next.to_string())?,
        })
    }

    fn share_for_member(
        &self,
        _member_pubkey: &str,
    ) -> Result<Option<CloudKitShare>, CloudHomeError> {
        Ok(None)
    }

    fn grant_share(&self, _member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
        Err(CloudHomeError::Configuration(
            "sharing is not used by restore tests".to_string(),
        ))
    }

    fn revoke_share(&self, _member_pubkey: &str) -> Result<(), CloudHomeError> {
        Ok(())
    }

    fn accept_share(&self, _share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
        Err(CloudHomeError::Configuration(
            "shared zones are not used by restore tests".to_string(),
        ))
    }
}

fn membership_floor(author_pubkey: String) -> MembershipFloor {
    MembershipFloor::MergeConcurrent(vec![crate::sync::membership::MembershipCoord {
        author_pubkey,
        author_owner_grant: crate::sync::membership::OwnerGrantId(
            crate::sync::store_commit::ObjectHash::digest(b"restore test owner grant"),
        ),
        seq: 1,
        entry_hash: crate::sync::store_commit::ObjectHash::digest(b"restore test founder entry"),
    }])
}

fn serialized_keyring(byte: u8) -> String {
    crate::encryption::MasterKeyring::from(crate::encryption::EncryptionService::from_key(
        [byte; 32],
    ))
    .to_serialized()
}

/// A restore code carrying the given `sid`. The provider points at a loopback
/// endpoint nothing listens on, so if execution ever reached the network it would
/// fail at once — but a malicious `sid` is refused at decode, well before that.
fn restore_code_with_sid(sid: &str) -> String {
    let code = RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: sid.to_string(),
        ek: Some(serialized_keyring(0xaa)),
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
        store_root_hash: crate::sync::store_commit::ObjectHash::digest(
            b"restore test store protocol root",
        ),
        founder_pubkey: hex::encode([0xAB_u8; 32]),
        membership_floor: membership_floor(hex::encode([0xAB_u8; 32])),
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
        crate::WritePolicy::MergeConcurrent,
        None,
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
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

#[tokio::test]
async fn restore_refuses_the_code_policy_before_any_provider_or_local_write() {
    let code = restore_code_with_sid("restore-policy-mismatch");
    let app = tempfile::tempdir().unwrap();
    let result = restore_from_code(
        &code,
        &test_synced_tables(),
        &test_migrations(),
        crate::WritePolicy::Serial,
        None,
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
        None,
        None,
        &crate::store_dir::StoreLayout::new(app.path()),
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device")),
        |_| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await;
    assert!(matches!(
        result,
        Err(BootstrapError::WritePolicyMismatch {
            expected: crate::WritePolicy::Serial,
            actual: crate::WritePolicy::MergeConcurrent,
        })
    ));
    assert!(!app.path().join("stores/restore-policy-mismatch").exists());
}

#[tokio::test]
async fn restore_refuses_a_known_unsupported_serial_provider_before_any_provider_or_local_write() {
    let code = encode_restore_code(&RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: "restore-serial-google-drive".to_string(),
        ek: Some(serialized_keyring(0xaa)),
        name: "Serial Store".to_string(),
        provider: CloudHomeJoinInfo::GoogleDrive {
            folder_id: "never-read".to_string(),
        },
        sk: hex::encode(crate::keys::UserKeypair::generate().to_keypair_bytes()),
        store_root_hash: crate::sync::store_commit::ObjectHash::digest(b"Serial restore root"),
        founder_pubkey: hex::encode([0xAB_u8; 32]),
        membership_floor: MembershipFloor::Serial(Some(
            crate::sync::store_commit::CommitPosition {
                seq: 1,
                commit_hash: crate::sync::store_commit::ObjectHash::digest(b"Serial restore floor"),
            },
        )),
    });
    let app = tempfile::tempdir().unwrap();
    let result = restore_from_code(
        &code,
        &test_synced_tables(),
        &test_migrations(),
        crate::WritePolicy::Serial,
        None,
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
        None,
        None,
        &crate::store_dir::StoreLayout::new(app.path()),
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device")),
        |_| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await;
    assert!(matches!(
        result,
        Err(BootstrapError::SerialCoordinationUnavailable {
            provider: crate::CloudProvider::GoogleDrive,
        })
    ));
    assert!(!app
        .path()
        .join("stores/restore-serial-google-drive")
        .exists());
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

/// A completed store already present locally is the data — re-running a restore
/// for it adds nothing, and the old code would delete its database and blobs
/// during the failure-cleanup once the snapshot download failed. The restore
/// refuses up front with a typed error naming the store and leaves the existing
/// files untouched. "Completed" is what the saved `config.yaml` marks: the guard
/// dispatches on that marker, so the store carries one here. The endpoint is
/// unreachable so that, absent the guard, execution would reach the snapshot
/// download and the destructive cleanup — the guard stops it first.
#[tokio::test]
async fn restore_refuses_when_completed_store_exists_and_leaves_it_untouched() {
    let encoded = restore_code_with_sid("abc-123");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A completed store with this id is already present locally, holding a saved
    // config (the completion marker), a database file, and a blob the restore
    // must not touch.
    let store_dir = app_dir.join("stores").join("abc-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create existing store dir");
    std::fs::write(store_dir.join("config.yaml"), b"store_id: abc-123\n")
        .expect("seed completion marker");
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

/// A store directory with no saved config is a torn bootstrap a crash left
/// behind, not a completed store: the restore must NOT refuse it with
/// `StoreExists`. It clears the torn residue and proceeds — here reaching the
/// (unreachable) cloud endpoint and failing at the snapshot download — and the
/// torn directory is gone, so a real retry could complete. Without the fix the
/// config-less directory would block every retry forever.
#[tokio::test]
async fn restore_clears_a_torn_bootstrap_and_proceeds() {
    crate::keys::test_keyring::install();
    let encoded = restore_code_with_sid("torn-restore-123");

    let tmp = tempfile::tempdir().expect("temp dir");
    let app_dir = tmp.path();

    // A torn bootstrap: a store directory with a half-written database and a
    // blob, but no `config.yaml` marker — what a crash mid-restore leaves.
    let store_dir = app_dir.join("stores").join("torn-restore-123");
    std::fs::create_dir_all(store_dir.join("storage")).expect("create torn store dir");
    std::fs::write(store_dir.join("store.db"), b"half-written-db").expect("seed torn db");
    std::fs::write(
        store_dir.join("storage").join("cover.blob"),
        b"partial-blob",
    )
    .expect("seed torn blob");

    let result = restore_result_for(&encoded, app_dir).await;
    assert!(
        matches!(result, Err(BootstrapError::Snapshot(_))),
        "a torn bootstrap must not be refused — the restore clears it and reaches the snapshot download, got {result:?}",
    );
    assert!(
        !store_dir.exists(),
        "the torn directory must be gone at {}, not left to block retries",
        store_dir.display(),
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
        crate::WritePolicy::MergeConcurrent,
        None,
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
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
/// the same failure again, never `StoreExists`. The failure here is before the
/// config marker is ever written, so the directory the first attempt created is
/// removed by its own cleanup and — even had a crash skipped that cleanup — the
/// config-less directory the second attempt finds is a torn bootstrap it clears
/// rather than a completed store it refuses.
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
/// keyring (steps 7 and 8) and restore's identity is imported (the step right
/// before the save) — must roll back all of them, not just whichever the OLD
/// code happened to reach. The public entry points (`join_store`,
/// `restore_from_cloud`) refuse a store whose config marker already exists, and
/// otherwise clear the directory, so there is no way to pre-seed a conflicting
/// path inside a completed store before calling them; this drives
/// `bootstrap_and_save_store` directly — the same function those entry points
/// call — and blocks its final write by seeding a directory at the exact path
/// `config.yaml` needs.
///
/// The bootstrap uses the founder membership root and a snapshot the owner
/// published, mirroring the bootstrap tests in `join_tests.rs`.
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
    )
    .with_copy_ids(Arc::new(
        crate::storage::cloud::SequentialCopyIdGenerator::new("late-rollback-owner"),
    ));

    let tables = test_synced_tables();
    let db = open_test_db();
    let store_root_hash = crate::sync::test_helpers::publish_test_store_protocol_root(
        &db,
        &owner_storage,
        store_id,
        "owner-device",
        &owner_keypair,
    )
    .await;
    let membership = crate::sync::test_helpers::publish_test_founder_membership(
        &owner_storage,
        store_id,
        &owner_keypair,
    )
    .await;
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("create owner snapshot");
    crate::sync::test_helpers::push_test_store_snapshot(
        &owner_storage,
        store_root_hash,
        snapshot,
        BTreeMap::new(),
        db.schema_version(),
        &owner_keypair,
        &membership,
        &db,
    )
    .await;

    let joiner_keypair = owner_keypair.clone();
    let joiner_storage = CloudSyncStorage::new(
        Arc::new(cloud) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        joiner_keypair.clone(),
    )
    .with_copy_ids(Arc::new(
        crate::storage::cloud::SequentialCopyIdGenerator::new("late-rollback-reader"),
    ));
    let store_keys = StoreKeys::new(store_id.to_string());
    let custody = crate::custody::KeyCustody::Keyring.resolve(store_id, &store_dir);
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(store_id, &store_dir);
    let join_info = CloudHomeJoinInfo::S3 {
        bucket: "b".to_string(),
        region: "us-east-1".to_string(),
        endpoint: None,
        access_key: "ak".to_string(),
        secret_key: "sk".to_string(),
        key_prefix: None,
    };

    let master_key = crate::encryption::MasterKeyring::from(
        crate::encryption::EncryptionService::from_key([0xbb; 32]),
    );
    let result = bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        Some(&master_key),
        &store_dir,
        store_id,
        "device-late",
        store_root_hash,
        BootstrapContext::Restore {
            founder_pubkey: &pubkey_hex(&owner_keypair),
            keypair: &joiner_keypair,
        },
        &MembershipFloor::MergeConcurrent(membership.author_heads()),
        &tables,
        &test_migrations(),
        &join_info,
        "Late Step Test",
        None,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &SystemClock,
        &|_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await;

    let err = result.expect_err("the blocked config.yaml write must fail bootstrap");

    // The meaningful precondition: steps 7 and 8 ran and wrote both keyring
    // accounts, and restore imported its identity, all before the failure at
    // the config save.
    assert!(
        store_keys
            .get_encryption_key()
            .expect("read keyring")
            .is_some(),
        "the encryption key must have been written before the late failure",
    );
    assert_eq!(
        identity_custody
            .unlock()
            .expect("read identity custody")
            .map(|kp| kp.public_key()),
        Some(joiner_keypair.public_key()),
        "restore's identity import runs before the config save, so it is present when the save fails",
    );
    assert!(
        store_keys
            .get_cloud_home_credentials()
            .expect("read keyring")
            .is_some(),
        "the cloud home credentials must have been written before the late failure",
    );

    let wrapped = cleanup_after_bootstrap_failure(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        err,
    );
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
    assert!(
        identity_custody
            .unlock()
            .expect("read identity custody")
            .is_none(),
        "the imported identity must be rolled back",
    );
}

/// `bootstrap_and_save_store` is the shared helper both `join_store` and
/// `restore_from_cloud` call — join_tests.rs's
/// `joined_device_first_cycle_does_not_clobber_the_shared_snapshot` already pins
/// its lower-level `open_db_and_pull` (with `owner_pubkey: None`, restore's own
/// no-owner shape) against a synthetic `MockSyncStorage`. This test drives one
/// level up, through `bootstrap_and_save_store` itself with
/// `BootstrapContext::Restore` over a real [`CloudSyncStorage`] /
/// `InMemoryCloudHome`, so the restore entry's bookkeeping (steps 7-9: keyring,
/// config, and — inside `open_db_and_pull` — the `protocol_state` that keeps a first
/// real cycle from thinking it's a brand-new store) is proven wired correctly,
/// not just the shared helper's own inner logic.
///
/// Driving through the actual public entry points (`restore_from_cloud` /
/// `restore_from_code`) isn't reachable here: their `build_cloud_home` only
/// dispatches to real S3/CloudKit/OAuth homes, with no test-only hook to inject
/// an in-memory one (unlike join, which has `join_store` as a lower-level entry
/// that accepts a pre-built `CloudHome` directly). `bootstrap_and_save_store` is
/// the exact shared helper the gap this test fixes names, so it's the reachable
/// unit that best pins the restore entry's wiring.
#[tokio::test]
async fn restore_first_cycle_does_not_clobber_the_shared_snapshot() {
    crate::keys::test_keyring::install();

    let store_id = "restore-anti-clobber-test";
    let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
    let cloud =
        crate::storage::cloud::cloudkit::CloudKitCloudHome::new_private(cloudkit_ops.clone());
    let cipher = CloudCipher::Plaintext;
    let blob_paths = BlobPathScheme::for_storage(HomeStorage::Browsable);
    let tables = test_synced_tables();
    let owner_keypair = UserKeypair::generate();

    let owner_storage = CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        owner_keypair.clone(),
    )
    .with_copy_ids(Arc::new(
        crate::storage::cloud::SequentialCopyIdGenerator::new("restore-cycle-owner"),
    ));

    // Owner: a store with one shared note, captured straight into the published
    // snapshot — the shape a device sees the first time it opens a shared store.
    let db_owner = open_test_db();
    let store_root_hash = crate::sync::test_helpers::publish_test_store_protocol_root(
        &db_owner,
        &owner_storage,
        store_id,
        "owner-device",
        &owner_keypair,
    )
    .await;
    let membership = crate::sync::test_helpers::publish_test_founder_membership(
        &owner_storage,
        store_id,
        &owner_keypair,
    )
    .await;
    exec(
        &db_owner,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', 1, '0000000001000-0000-owner', '2026-01-01')",
    )
    .await;
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    crate::sync::test_helpers::push_test_store_snapshot(
        &owner_storage,
        store_root_hash,
        snapshot,
        BTreeMap::new(),
        db_owner.schema_version(),
        &owner_keypair,
        &membership,
        &db_owner,
    )
    .await;

    let snapshot_before: Vec<_> =
        crate::sync::store_objects::list_snapshot_metas(&owner_storage, store_root_hash)
            .await
            .expect("list Store snapshots")
            .metas
            .into_iter()
            .map(|meta| meta.semantic_hash)
            .collect();

    // Device B restores through the public restore-code service over its own
    // CloudKit home onto the same records.
    let app = tempfile::tempdir().expect("restore app dir");
    let layout = StoreLayout::new(app.path());
    let joiner_keypair = owner_keypair.clone();
    let restore_code = encode_restore_code(&RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: store_id.to_string(),
        ek: None,
        name: "Restored Store".to_string(),
        provider: CloudHomeJoinInfo::CloudKit,
        sk: hex::encode(joiner_keypair.to_keypair_bytes()),
        store_root_hash,
        founder_pubkey: pubkey_hex(&owner_keypair),
        membership_floor: MembershipFloor::MergeConcurrent(membership.author_heads()),
    });
    let config = restore_from_code(
        &restore_code,
        &tables,
        &test_migrations(),
        crate::WritePolicy::MergeConcurrent,
        None,
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
        None,
        Some(cloudkit_ops.clone()),
        &layout,
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device-b")),
        |_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await
    .expect("restore through code service");
    let lib_b = config.store_dir.clone();
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(store_id, &lib_b);

    // The config is saved, and a saved config implies the identity was imported
    // before it — the restored device's signing identity resolves in custody.
    assert_eq!(
        identity_custody
            .unlock()
            .expect("read restored identity")
            .map(|kp| kp.public_key()),
        Some(joiner_keypair.public_key()),
        "a completed restore has its signing identity in custody",
    );
    let other_identity_custody = crate::identity_custody::IdentityCustody::Keyring.resolve(
        "restore-anti-clobber-other-store",
        &layout.store_dir("restore-anti-clobber-other-store"),
    );
    assert!(
        other_identity_custody
            .unlock()
            .expect("read unrelated store identity")
            .is_none(),
        "restoring one store establishes no identity for another store",
    );

    let (db_b, _stamper) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        coven_core::WritePolicy::MergeConcurrent,
        config.device_id.clone(),
        &test_migrations(),
    )
    .expect("open B db");

    // B's first real sync cycle, with no local changes of its own.
    let enc_lock = RwLock::new(cipher.clone());
    let joiner_storage = CloudSyncStorage::new(
        Arc::new(crate::storage::cloud::cloudkit::CloudKitCloudHome::new_private(cloudkit_ops)),
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        joiner_keypair.clone(),
    )
    .with_copy_ids(Arc::new(
        crate::storage::cloud::SequentialCopyIdGenerator::new("restore-cycle-reader"),
    ));
    let b_hlc = Hlc::new(config.device_id.clone());
    run_test_cycle(
        &joiner_storage,
        store_id,
        &config.device_id,
        &b_hlc,
        &SystemClock,
        &db_b,
        &enc_lock,
        &PendingRotation::none(),
        &joiner_keypair,
        None,
        &lib_b,
        None,
        None,
    )
    .await
    .expect("B sync cycle");

    let snapshot_after: Vec<_> =
        crate::sync::store_objects::list_snapshot_metas(&joiner_storage, store_root_hash)
            .await
            .expect("list Store snapshots")
            .metas
            .into_iter()
            .map(|meta| meta.semantic_hash)
            .collect();
    assert_eq!(
        snapshot_after, snapshot_before,
        "a restored device's first cycle must not republish/clobber the shared snapshot",
    );
}

/// The restore-only branch in `open_db_and_pull` (join.rs, around the
/// `owner_pubkey.is_none()` block): restore carries no owner from an invite, so
/// it adopts the chain founder as the pinned owner itself, after the pull, from
/// membership entries loaded straight from the bootstrapped storage. No existing
/// test publishes a founder chain and checks this — join_tests.rs's
/// `open_db_and_pull` calls also pass `owner_pubkey: None`, but never publish any
/// membership entries, so that branch's `if !entries.is_empty()` body never runs
/// there. This seeds a real founder entry (and its head) before bootstrapping,
/// then asserts the joiner's `protocol_state` pins that founder's pubkey.
#[tokio::test]
async fn restore_pins_the_chain_founder_as_owner() {
    let tables = test_synced_tables();

    let owner_keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_store_and_keypair("test-lib", owner_keypair.clone());
    let owner_pk = pubkey_hex(&owner_keypair);
    let founder = storage.store_protocol_root().founder;
    let chain = MembershipChain::from_entries(vec![founder.clone()]).unwrap();
    crate::sync::store_objects::append_membership_entry_object(
        &storage,
        &founder.coord(),
        &founder,
    )
    .await
    .unwrap();
    publish_membership_chain_head(&storage, &chain, &owner_keypair).await;

    // The owner also publishes an empty snapshot, the shape
    // `bootstrap_from_snapshot` needs to succeed.
    let db_owner = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    crate::sync::test_helpers::push_test_store_snapshot(
        &storage,
        storage.store_root_hash(),
        snapshot,
        BTreeMap::new(),
        db_owner.schema_version(),
        &owner_keypair,
        &chain,
        &db_owner,
    )
    .await;

    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(
        &storage,
        "test-lib",
        storage.store_root_hash(),
        &owner_pk,
        &MembershipFloor::MergeConcurrent(chain.author_heads()),
        1,
        &lib_b.db_path(),
    )
    .await
    .expect("B bootstrap");
    open_db_and_pull(
        "test-lib",
        &lib_b.db_path(),
        &tables,
        &test_migrations(),
        "B",
        &owner_pk,
        None,
        &MembershipFloor::MergeConcurrent(chain.author_heads()),
        &storage,
        None,
        None,
        boot,
        &lib_b,
        &tokio::sync::watch::channel(false).1,
    )
    .await
    .expect("B open_db_and_pull");

    let (db_b, _stamper) = Database::open(
        &lib_b.db_path(),
        tables.clone(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        coven_core::WritePolicy::MergeConcurrent,
        "B".to_string(),
        &test_migrations(),
    )
    .expect("open B db");

    let pinned_owner_sql = format!(
        "SELECT value FROM protocol_state WHERE key = '{}'",
        crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY
    );
    assert_eq!(
        crate::sync::test_helpers::query_text(&db_b, &pinned_owner_sql).await,
        owner_pk,
        "restore must pin the chain founder's pubkey as the store owner",
    );
}

/// Mirrors join_tests.rs's `a_fresh_joiner_refuses_a_rolled_back_membership_head`:
/// a restore code seeds the same per-author watermark from its own floor. Owner
/// pinning follows this call in the restore flow, but the accepted floor is
/// already authoritative: the bootstrap pull must reject a lower signed head
/// instead of treating a failed unpinned chain load as pre-initialization.
#[tokio::test]
async fn a_fresh_restorer_refuses_a_rolled_back_membership_head_during_bootstrap() {
    use crate::sync::membership::{entry_hash, AuthorHead, MemberRole};

    let tables = test_synced_tables();

    let owner = UserKeypair::generate();
    let storage = MockSyncStorage::with_store_and_keypair("test-lib", owner.clone());
    let member = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);

    // Build the chain: found, add a member, then remove them — head at seq 3.
    let founder = storage.store_protocol_root().founder;
    let mut chain = MembershipChain::from_entries(vec![founder.clone()]).unwrap();
    crate::sync::store_objects::append_membership_entry_object(
        &storage,
        &founder.coord(),
        &founder,
    )
    .await
    .unwrap();
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "0000000002000-0000-owner".to_string(),
        )
        .unwrap();
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member.clone()).await;
    let remove_member = chain
        .signed_remove_member(
            &owner,
            pubkey_hex(&member),
            "0000000003000-0000-owner".to_string(),
        )
        .unwrap();
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // The restore code is minted right after the removal: its floor is the
    // current (post-removal) chain state.
    let membership_floor = MembershipFloor::MergeConcurrent(chain.author_heads());
    assert_eq!(
        membership_floor,
        MembershipFloor::MergeConcurrent(vec![chain
            .entries()
            .last()
            .expect("removal entry")
            .coord(),]),
    );

    let db_owner = open_test_db();
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    crate::sync::test_helpers::push_test_store_snapshot(
        &storage,
        storage.store_root_hash(),
        snapshot,
        BTreeMap::new(),
        db_owner.schema_version(),
        &owner,
        &chain,
        &db_owner,
    )
    .await;

    // The provider now serves the PRE-removal head — seq 2 — after the floor
    // was minted from the post-removal state.
    let stale_head = AuthorHead::signed(
        "test-lib".to_string(),
        add_member.author_owner_grant.clone(),
        2,
        entry_hash(&add_member),
        &owner,
    );
    storage.remove_membership_head(&owner_pk);
    storage
        .append_membership_head_bytes(&owner_pk, serde_json::to_vec(&stale_head).unwrap())
        .await
        .expect("provider serves the rolled-back head");

    let (_tmp_b, lib_b) = temp_store_dir();
    let error = bootstrap_from_snapshot(
        &storage,
        "test-lib",
        storage.store_root_hash(),
        &owner_pk,
        &membership_floor,
        1,
        &lib_b.db_path(),
    )
    .await
    .expect_err("the restore must enforce its floor before accepting a snapshot");

    let message = match &error {
        crate::sync::snapshot::SnapshotError::UnauthorizedAuthor(message) => message,
        other => panic!("expected membership rejection before snapshot adoption, got {other:?}"),
    };
    assert!(message.contains(&owner_pk), "{message}");
    assert!(
        message.contains("regressed to seq 2 below the accepted 3"),
        "{message}"
    );
}

/// Mirrors `bootstrap_backfills_blob_files_for_snapshot_rows` (join_tests.rs),
/// which already pins the blob backfill at the `open_db_and_pull` level with
/// `owner_pubkey: None` (restore's own shape). This drives one level up, through
/// `bootstrap_and_save_store` with `BootstrapContext::Restore` over a real
/// `CloudSyncStorage` / `InMemoryCloudHome`, to pin that a restored store's blob
/// backfill survives the full shared-helper entry, not just the inner pull. See
/// the anti-clobber test above for why `restore_from_cloud` itself isn't
/// reachable here.
#[tokio::test]
async fn restore_bootstrap_backfills_blob_files_for_snapshot_rows() {
    crate::keys::test_keyring::install();

    let store_id = "restore-blob-backfill-test";
    let cloud = crate::InMemoryCloudHome::new();
    let cipher = CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32]));
    let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let owner_keypair = UserKeypair::generate();

    let owner_storage = CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        owner_keypair.clone(),
    )
    .with_copy_ids(Arc::new(
        crate::storage::cloud::SequentialCopyIdGenerator::new("restore-blob-owner"),
    ));

    // Owner: a shared note with a cover photo, both captured into the snapshot.
    let db_owner = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let store_root_hash = crate::sync::test_helpers::publish_test_store_protocol_root(
        &db_owner,
        &owner_storage,
        store_id,
        "owner-device",
        &owner_keypair,
    )
    .await;
    let membership = crate::sync::test_helpers::publish_test_founder_membership(
        &owner_storage,
        store_id,
        &owner_keypair,
    )
    .await;
    exec(
        &db_owner,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
    )
    .await;
    exec(
        &db_owner,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-owner', '2026-01-01')",
            crate::blob::content_hash(b"cover-bytes"),
        ),
    )
    .await;
    // The owner recorded who uploaded the cover when it imported the album; the
    // snapshot carries this uploader index forward so the restoring device resolves
    // the blob's prefix from it (there is no listing scan). The cover is keyed under
    // the owner's own uploader segment on the hashed home.
    crate::sync::test_helpers::record_blob_uploader(
        &db_owner,
        "photos",
        "photo1",
        &owner_storage.own_uploader().expect("owner uploader"),
    )
    .await;

    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    crate::sync::test_helpers::push_test_store_snapshot(
        &owner_storage,
        store_root_hash,
        snapshot,
        BTreeMap::new(),
        db_owner.schema_version(),
        &owner_keypair,
        &membership,
        &db_owner,
    )
    .await;

    // The cover blob exists in the cloud (uploaded when the owner first imported
    // the album).
    owner_storage
        .put_blob(
            "photos",
            "photo1",
            crate::blob::BlobScope::Master,
            None,
            b"cover-bytes".to_vec(),
        )
        .await
        .expect("seed cover blob");

    let (_tmp_b, lib_b) = temp_store_dir();
    let expected_blob = lib_b
        .cache_blob_path("photos", "photo1")
        .expect("cache blob path");

    let joiner_keypair = owner_keypair.clone();
    let joiner_storage = CloudSyncStorage::new(
        Arc::new(cloud) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        joiner_keypair.clone(),
    )
    .with_copy_ids(Arc::new(
        crate::storage::cloud::SequentialCopyIdGenerator::new("restore-blob-reader"),
    ));
    let store_keys = StoreKeys::new(store_id.to_string());
    let custody = crate::custody::KeyCustody::Keyring.resolve(store_id, &lib_b);
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(store_id, &lib_b);
    let join_info = CloudHomeJoinInfo::CloudKit;

    bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        None,
        &lib_b,
        store_id,
        "device-b",
        store_root_hash,
        BootstrapContext::Restore {
            founder_pubkey: &pubkey_hex(&owner_keypair),
            keypair: &joiner_keypair,
        },
        &MembershipFloor::MergeConcurrent(membership.author_heads()),
        &tables,
        &test_migrations(),
        &join_info,
        "Restored Store",
        None,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &SystemClock,
        &|_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await
    .expect("restore bootstrap backfills the blob");

    assert!(
        expected_blob.exists(),
        "the cover blob file must be backfilled to {} after restore",
        expected_blob.display(),
    );
    assert_eq!(
        std::fs::read(&expected_blob).expect("read backfilled blob"),
        b"cover-bytes",
        "the backfilled file must hold the blob's plaintext bytes",
    );
}
