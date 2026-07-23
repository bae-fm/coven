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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::blob::{CacheFill, Provenance};
use crate::clock::SystemClock;
use crate::config::HomeStorage;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::id_provider::SequentialIdProvider;
use crate::join_code::MembershipFloor;
use crate::keys::{StoreKeys, UserKeypair};
use crate::storage::cloud::cloudkit::{
    CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare,
};
use crate::storage::cloud::{CloudHome, CloudHomeJoinInfo};
use crate::storage::cloud::{CloudHomeError, CloudObjectVersion, CloudVersionedObject};
use crate::store_dir::StoreLayout;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::hlc::Hlc;
use crate::sync::join::{
    bootstrap_and_save_store, cleanup_after_bootstrap_failure, open_db_and_pull, BootstrapError,
    RestoreBootstrapContext,
};
use crate::sync::restore::restore_from_code;
use crate::sync::restore_code::{
    decode_restore_code, encode_restore_code, OwnerRecoveryAuthority, RestoreAuthority,
    RestoreCode, RestoreCodeError,
};
use crate::sync::session::BlobDecl;
use crate::sync::storage::SyncStorage;
use crate::sync::store::snapshot::{bootstrap_from_snapshot, create_snapshot};
use crate::sync::test_helpers::{
    host_exec, open_test_db, open_test_db_with_blob, pubkey_hex, publish_store_ack_fixture,
    temp_store_dir, test_migrations, test_synced_tables, test_synced_tables_with_blob,
};

struct RestoreCloudKitOps {
    records: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
    versions: Mutex<HashMap<(CloudKitScope, String), u64>>,
    batches: Mutex<HashMap<String, Vec<CloudKitRecordCreate>>>,
    next_batch: AtomicUsize,
}

impl RestoreCloudKitOps {
    fn new() -> Self {
        Self {
            records: Mutex::new(HashMap::new()),
            versions: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            next_batch: AtomicUsize::new(0),
        }
    }
}

impl CloudKitOps for RestoreCloudKitOps {
    fn provider_identity(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
        let (owner_name, zone_name) = match scope {
            CloudKitScope::Private => ("restore-owner", "restore-zone"),
            CloudKitScope::Shared {
                owner_name,
                zone_name,
            } => (owner_name.as_str(), zone_name.as_str()),
        };
        Ok(CloudKitProviderIdentity {
            container_id: "iCloud.restore.coven".to_string(),
            environment: crate::CloudKitEnvironment::Development,
            owner_name: owner_name.to_string(),
            zone_name: zone_name.to_string(),
            current_user_record_name: "restore-user".to_string(),
        })
    }

    fn accepted_read_write_share(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError> {
        Err(CloudHomeError::NotFound(
            "accepted CloudKit share".to_string(),
        ))
    }

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
    ) -> Result<CloudVersionedObject, CloudHomeError> {
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
        Ok(CloudVersionedObject {
            bytes,
            version: CloudObjectVersion::from_provider(version.to_string())?,
        })
    }

    fn begin_atomic_create(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
        let batch = CloudKitAtomicCreateBatch::from_provider(format!(
            "restore-batch-{}",
            self.next_batch.fetch_add(1, Ordering::SeqCst)
        ))?;
        self.batches
            .lock()
            .unwrap()
            .insert(batch.as_provider().to_string(), Vec::new());
        Ok(batch)
    }

    fn stage_atomic_create_record(
        &self,
        _scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
        create: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError> {
        self.batches
            .lock()
            .unwrap()
            .get_mut(batch.as_provider())
            .ok_or_else(|| CloudHomeError::NotFound(batch.as_provider().to_string()))?
            .push(create);
        Ok(())
    }

    fn commit_atomic_create(
        &self,
        scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        let mut batches = self.batches.lock().unwrap();
        let creates = batches
            .get(batch.as_provider())
            .ok_or_else(|| CloudHomeError::NotFound(batch.as_provider().to_string()))?;
        let mut records = self.records.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();
        for create in creates {
            if records.contains_key(&(scope.clone(), create.key.clone())) {
                return Err(CloudHomeError::AlreadyExists(create.key.clone()));
            }
        }
        let creates = batches
            .remove(batch.as_provider())
            .expect("validated restore CloudKit batch disappeared");
        let mut created = Vec::with_capacity(creates.len());
        for create in creates {
            let coordinate = (scope.clone(), create.key.clone());
            records.insert(coordinate.clone(), create.data);
            versions.insert(coordinate, 1);
            created.push(CloudKitRecordVersion {
                key: create.key,
                version: CloudObjectVersion::from_provider("1".to_string())?,
            });
        }
        Ok(created)
    }

    fn discard_atomic_create(
        &self,
        _scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
    ) -> Result<(), CloudHomeError> {
        self.batches.lock().unwrap().remove(batch.as_provider());
        Ok(())
    }

    fn delete_record_versions(
        &self,
        scope: &CloudKitScope,
        exact_records: &[CloudKitRecordVersion],
    ) -> Result<(), CloudHomeError> {
        let mut records = self.records.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();
        for record in exact_records {
            let coordinate = (scope.clone(), record.key.clone());
            let version = versions
                .get(&coordinate)
                .ok_or_else(|| CloudHomeError::NotFound(record.key.clone()))?;
            if version.to_string() != record.version.as_provider() {
                return Err(CloudHomeError::Transport(format!(
                    "restore CloudKit record {:?} changed before exact deletion",
                    record.key
                )));
            }
            if !records.contains_key(&coordinate) {
                return Err(CloudHomeError::NotFound(record.key.clone()));
            }
        }
        for record in exact_records {
            let coordinate = (scope.clone(), record.key.clone());
            records.remove(&coordinate);
            versions.remove(&coordinate);
        }
        Ok(())
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
    let coord = crate::sync::membership::MembershipCoord {
        author_pubkey,
        author_owner_grant: crate::sync::membership::MembershipGrantId(
            crate::sync::store_commit::ObjectHash::digest(b"restore test owner grant"),
        ),
        stream_id: format!("{:064x}", 1)
            .parse()
            .expect("canonical test author stream id"),
        seq: 1,
        entry_hash: crate::sync::store_commit::ObjectHash::digest(b"restore test founder entry"),
    };
    let stored = b"restore test membership head";
    MembershipFloor(vec![crate::sync::membership::MembershipHeadRef {
        coord,
        head_hash: crate::sync::store_commit::ObjectHash::digest(
            b"restore test membership head semantic bytes",
        ),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(
                "store-v1/membership/heads/restore-test/1.json".to_string(),
            )
            .expect("valid test membership-head slot"),
            stored.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(stored),
        ),
    }])
}

fn store_root_ref(label: &str) -> crate::sync::store_commit::StoreRootRef {
    let stored = format!("{label} stored root");
    crate::sync::store_commit::StoreRootRef {
        store_root_id: crate::sync::store_commit::ObjectHash::digest(
            format!("{label} identity").as_bytes(),
        ),
        store_root_hash: crate::sync::store_commit::ObjectHash::digest(label.as_bytes()),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!(
                "store-v1/protocol/root/{}.json",
                crate::sync::store_commit::ObjectHash::digest(label.as_bytes())
            ))
            .expect("valid test Store-root slot"),
            stored.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(stored.as_bytes()),
        ),
    }
}

fn serialized_keyring(byte: u8) -> String {
    crate::encryption::MasterKeyring::from(crate::encryption::EncryptionService::from_key(
        [byte; 32],
    ))
    .to_serialized()
}

fn owner_recovery_authority(
    root: &crate::sync::store_commit::StoreRootRef,
    owner: &UserKeypair,
) -> RestoreAuthority {
    let owner_grant = crate::sync::membership::MembershipGrantId(
        crate::sync::store_commit::ObjectHash::digest(b"restore test owner grant"),
    );
    let anchor = crate::sync::store_commit::GrantStreamAnchor::OwnerRecovery {
        first_slot: crate::storage::cloud::ObjectSlot::logical(
            "store-v1/recovery/restore-tests/first.json".to_string(),
        )
        .expect("valid recovery slot"),
    };
    let activation = crate::sync::store_commit::OwnerRecoveryActivationId::derive(
        root,
        &pubkey_hex(owner),
        &owner_grant,
        &anchor,
    )
    .expect("valid recovery activation");
    RestoreAuthority::OwnerRecovery(OwnerRecoveryAuthority {
        owner_identity_secret: hex::encode(owner.to_keypair_bytes()),
        owner_grant: owner_grant.clone(),
        recovery: crate::sync::store_commit::OwnerRecoveryCursor {
            owner_grant,
            position: crate::sync::store_commit::OwnerRecoveryPosition::BeforeFirst { activation },
        },
        published_at: "2026-07-17T00:00:00Z".to_string(),
    })
}

async fn published_owner_recovery_authority(
    storage: &dyn crate::sync::storage::SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    owner: &UserKeypair,
) -> RestoreAuthority {
    let protocol = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .expect("load published Store root")
        .value;
    let owner_grant = protocol.descriptor.founder_grant.clone();
    let activation = crate::sync::store_commit::OwnerRecoveryActivationId::derive(
        root,
        &pubkey_hex(owner),
        &owner_grant,
        &protocol.descriptor.founder_recovery,
    )
    .expect("derive published recovery activation");
    RestoreAuthority::OwnerRecovery(OwnerRecoveryAuthority {
        owner_identity_secret: hex::encode(owner.to_keypair_bytes()),
        owner_grant: owner_grant.clone(),
        recovery: crate::sync::store_commit::OwnerRecoveryCursor {
            owner_grant,
            position: crate::sync::store_commit::OwnerRecoveryPosition::BeforeFirst { activation },
        },
        published_at: "2026-07-17T00:00:00Z".to_string(),
    })
}

/// A restore code carrying the given `sid`. The provider points at a loopback
/// endpoint nothing listens on, so if execution ever reached the network it would
/// fail at once — but a malicious `sid` is refused at decode, well before that.
fn restore_code_with_sid(sid: &str) -> String {
    let root = store_root_ref("restore test store protocol root");
    let owner = crate::keys::UserKeypair::generate();
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
        store_root: root.clone(),
        founder_pubkey: hex::encode([0xAB_u8; 32]),
        membership_floor: membership_floor(hex::encode([0xAB_u8; 32])),
        authority: owner_recovery_authority(&root, &owner),
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
        Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
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
async fn restore_accepts_blob_schema_for_google_drive_and_reaches_provider_setup() {
    crate::keys::test_keyring::install();
    let store_id = "restore-immutable-google-drive";
    let root = store_root_ref("restore root");
    let owner = crate::keys::UserKeypair::generate();
    let code = encode_restore_code(&RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: store_id.to_string(),
        ek: Some(serialized_keyring(0xaa)),
        name: "Blob Store".to_string(),
        provider: CloudHomeJoinInfo::GoogleDrive {
            folder_id: "never-read".to_string(),
        },
        store_root: root.clone(),
        founder_pubkey: hex::encode([0xAB_u8; 32]),
        membership_floor: membership_floor(hex::encode([0xAB_u8; 32])),
        authority: owner_recovery_authority(&root, &owner),
    });
    let tables = test_synced_tables_with_blob(crate::BlobDecl::new(
        "photos",
        crate::Provenance::HostProvided,
        crate::CacheFill::CacheLazy,
    ));
    let app = tempfile::tempdir().expect("app directory");

    let result = restore_from_code(
        &code,
        &tables,
        &test_migrations(),
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

    assert!(
        matches!(result, Err(BootstrapError::Provider(_))),
        "unexpected restore result: {result:?}"
    );
    assert!(!app.path().join("stores").join(store_id).exists());
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
    crate::keys::test_keyring::install();
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
        Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
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
    crate::keys::test_keyring::install();
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

/// A restore failure while saving `config.yaml`, after the key, credentials,
/// and identity are durable, rolls all of them back. The test calls the restore
/// bootstrap helper directly so it can block that exact final write.
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
    .expect("build owner cloud storage");

    let tables = test_synced_tables();
    let db = open_test_db();
    let (store_root, membership) = crate::sync::test_helpers::initialize_store_fixture(
        &db,
        &owner_storage,
        store_id,
        &owner_keypair,
    )
    .await
    .expect("initialize owner Store");
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError::Message(e.to_string()))
        })
        .await
        .expect("create owner snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
    crate::sync::test_helpers::publish_snapshot_fixture(
        &owner_storage,
        &store_root,
        snapshot,
        snapshot_coverage.clone(),
        &owner_keypair,
        &membership,
        &db,
    )
    .await
    .expect("publish owner snapshot");
    publish_store_ack_fixture(&db, &owner_storage, snapshot_coverage, &owner_keypair)
        .await
        .expect("publish owner snapshot acknowledgement");

    let joiner_keypair = owner_keypair.clone();
    let joiner_storage = CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        joiner_keypair.clone(),
    )
    .expect("build joiner cloud storage");
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
    let authority =
        published_owner_recovery_authority(&owner_storage, &store_root, &owner_keypair).await;
    let migrations = test_migrations();
    let result = bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        Some(&master_key),
        &store_dir,
        store_id,
        "device-late",
        store_root.clone(),
        RestoreBootstrapContext {
            founder_pubkey: &pubkey_hex(&owner_keypair),
            keypair: &joiner_keypair,
            authority: &authority,
            continuation: None,
        },
        &MembershipFloor(membership.head_refs().to_vec()),
        &tables,
        &migrations,
        &join_info,
        "Late Step Test",
        None,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &|_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await;

    let err = result.expect_err("the blocked config.yaml write must fail bootstrap");
    assert!(
        matches!(&err, BootstrapError::Config(_)),
        "bootstrap must reach the blocked config save, got {err:?}"
    );

    let failed_registration = {
        let connection =
            rusqlite::Connection::open(store_dir.db_path()).expect("open failed restore database");
        connection
            .query_row(
                "SELECT r.device_id, r.registration_bytes, a.activation_authority \
                 FROM local_store_device_registration r \
                 JOIN store_device_registration_activations a ON a.device_id = r.device_id \
                 WHERE r.singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .expect("load activated recovery registration after config failure")
    };
    let candidate_prefix = "store-v1/candidates/";
    let candidates_after_failure = cloud
        .list(candidate_prefix)
        .await
        .expect("list candidate objects after config failure");

    // The keyring accounts and restore identity were durable before the config
    // save failed.
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

    std::fs::create_dir_all(&*store_dir).expect("recreate store directory for retry");
    let retry = Box::pin(bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        Some(&master_key),
        &store_dir,
        store_id,
        "device-late",
        store_root,
        RestoreBootstrapContext {
            founder_pubkey: &pubkey_hex(&owner_keypair),
            keypair: &joiner_keypair,
            authority: &authority,
            continuation: None,
        },
        &MembershipFloor(membership.head_refs().to_vec()),
        &tables,
        &migrations,
        &join_info,
        "Late Step Test",
        None,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &|_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    ))
    .await
    .expect("retry reuses the activated recovery registration");
    assert_eq!(retry.device_id, "device-late");

    let retried_registration = {
        let connection =
            rusqlite::Connection::open(store_dir.db_path()).expect("open retried restore database");
        connection
            .query_row(
                "SELECT r.device_id, r.registration_bytes, a.activation_authority \
                 FROM local_store_device_registration r \
                 JOIN store_device_registration_activations a ON a.device_id = r.device_id \
                 WHERE r.singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .expect("load retried recovery registration")
    };
    assert_eq!(retried_registration, failed_registration);
    assert_eq!(
        cloud
            .list(candidate_prefix)
            .await
            .expect("list candidate objects after retry"),
        candidates_after_failure,
        "retry must reuse the recovery commit",
    );
}

#[tokio::test]
async fn merge_owner_recovery_restore_code_creates_an_activated_replacement_device() {
    let fixture = Box::pin(prepare_owner_recovery_restore()).await;
    Box::pin(assert_owner_recovery_restore(fixture)).await;
}

struct OwnerRecoveryRestoreFixture {
    code: String,
    tables: Vec<crate::sync::session::SyncedTable>,
    migrations: Vec<crate::migration::Migration>,
    cloudkit_ops: Arc<RestoreCloudKitOps>,
    app: tempfile::TempDir,
}

async fn prepare_owner_recovery_restore() -> OwnerRecoveryRestoreFixture {
    crate::keys::test_keyring::install();
    let store_id = "owner-recovery-restore";
    let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
    let cloud = Arc::new(
        crate::storage::cloud::cloudkit::CloudKitCloudHome::new_private(cloudkit_ops.clone()),
    );
    let owner = UserKeypair::generate();
    let owner_storage = CloudSyncStorage::new(
        cloud.clone(),
        CloudCipher::Plaintext,
        BlobPathScheme::for_storage(HomeStorage::Browsable),
        store_id.to_string(),
        owner.clone(),
    )
    .expect("build Owner recovery storage");
    let owner_db = open_test_db();
    let (root, membership) = crate::sync::test_helpers::initialize_store_fixture(
        &owner_db,
        &owner_storage,
        store_id,
        &owner,
    )
    .await
    .expect("initialize recovery Store");
    let floor = MembershipFloor(membership.head_refs().to_vec());
    let tables = test_synced_tables();
    let snapshot_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snapshot_dir = snapshot_tmp.path().to_path_buf();
    let snapshot_tables = tables.clone();
    let snapshot = owner_db
        .call(move |connection| {
            create_snapshot(connection, &snapshot_dir, &snapshot_tables)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await
        .expect("create recovery snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
    crate::sync::test_helpers::publish_snapshot_fixture(
        &owner_storage,
        &root,
        snapshot,
        snapshot_coverage.clone(),
        &owner,
        &membership,
        &owner_db,
    )
    .await
    .expect("publish recovery snapshot");
    publish_store_ack_fixture(&owner_db, &owner_storage, snapshot_coverage, &owner)
        .await
        .expect("publish recovery snapshot acknowledgement");
    let authority = published_owner_recovery_authority(&owner_storage, &root, &owner).await;
    let code = encode_restore_code(&RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: store_id.to_string(),
        ek: None,
        name: "Recovered Store".to_string(),
        provider: CloudHomeJoinInfo::CloudKit,
        store_root: root,
        founder_pubkey: pubkey_hex(&owner),
        membership_floor: floor,
        authority,
    });
    let app = tempfile::tempdir().expect("restore app dir");
    OwnerRecoveryRestoreFixture {
        code,
        tables,
        migrations: test_migrations(),
        cloudkit_ops,
        app,
    }
}

async fn assert_owner_recovery_restore(fixture: OwnerRecoveryRestoreFixture) {
    let OwnerRecoveryRestoreFixture {
        code,
        tables,
        migrations,
        cloudkit_ops,
        app,
    } = fixture;
    let layout = StoreLayout::new(app.path());
    let config = Box::pin(restore_from_code(
        &code,
        &tables,
        &migrations,
        None,
        crate::custody::KeyCustody::Keyring,
        crate::identity_custody::IdentityCustody::Keyring,
        None,
        Some(cloudkit_ops),
        &layout,
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("unused-recovery-device")),
        |_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    ))
    .await
    .expect("restore through OwnerRecovery code");
    let (restored, _stamper) = Database::open(
        &config.store_dir.db_path(),
        tables,
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        config.device_id.clone(),
        &migrations,
    )
    .expect("open recovered database");
    let store_device_id = restored
        .get_protocol_state(coven_core::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load recovered Store device identity")
        .expect("recovered Store device identity exists");
    let activation: crate::sync::store_commit::StoreDeviceRegistrationActivation = restored
        .call(move |connection| {
            let authority: String = connection
                .query_row(
                    "SELECT activation_authority FROM store_device_registration_activations \
                     WHERE device_id = ?1",
                    [store_device_id],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            serde_json::from_str(&authority).map_err(|error| DbError::Message(error.to_string()))
        })
        .await
        .expect("load config device activation");
    assert!(matches!(
        activation,
        crate::sync::store_commit::StoreDeviceRegistrationActivation::Recovery { .. }
    ));
}

/// Restore bootstrap records the snapshot position before its first real sync
/// cycle, so that cycle does not replace the shared snapshot as an initial sync.
#[tokio::test]
async fn restore_first_cycle_does_not_clobber_the_shared_snapshot() {
    Box::pin(run_restore_first_cycle_does_not_clobber_snapshot()).await;
}

async fn run_restore_first_cycle_does_not_clobber_snapshot() {
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
    .expect("build owner cloud storage");

    // Owner: a store with one shared note, captured straight into the published
    // snapshot — the shape a device sees the first time it opens a shared store.
    let db_owner = open_test_db();
    let (store_root, membership) = crate::sync::test_helpers::initialize_store_fixture(
        &db_owner,
        &owner_storage,
        store_id,
        &owner_keypair,
    )
    .await
    .expect("initialize owner Store");
    host_exec(
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
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError::Message(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
    crate::sync::test_helpers::publish_snapshot_fixture(
        &owner_storage,
        &store_root,
        snapshot,
        snapshot_coverage.clone(),
        &owner_keypair,
        &membership,
        &db_owner,
    )
    .await
    .expect("publish owner snapshot");
    publish_store_ack_fixture(&db_owner, &owner_storage, snapshot_coverage, &owner_keypair)
        .await
        .expect("publish owner snapshot acknowledgement");

    let snapshot_before = owner_storage
        .cloud_home()
        .list("store-v1/snapshots/")
        .await
        .expect("list Store snapshot objects");

    // Device B restores through the public restore-code service over its own
    // CloudKit home onto the same records.
    let app = Arc::new(tempfile::tempdir().expect("restore app dir"));
    let joiner_keypair = owner_keypair.clone();
    let continuation = crate::sync::store::StoreDatabase::from_database(db_owner.clone())
        .export_activated_device_continuation(&joiner_keypair)
        .await
        .expect("export exact activated continuation");
    let expected_latest_snapshot = continuation.latest_snapshot.clone();
    let restore_code = encode_restore_code(&RestoreCode {
        v: crate::sync::restore_code::RESTORE_CODE_VERSION,
        sid: store_id.to_string(),
        ek: None,
        name: "Restored Store".to_string(),
        provider: CloudHomeJoinInfo::CloudKit,
        store_root: store_root.clone(),
        founder_pubkey: pubkey_hex(&owner_keypair),
        membership_floor: MembershipFloor(membership.head_refs().to_vec()),
        authority: RestoreAuthority::ActivatedContinuation(continuation),
    });
    let decoded = decode_restore_code(&restore_code).expect("decode continuation restore code");
    let RestoreAuthority::ActivatedContinuation(decoded_continuation) = decoded.authority else {
        panic!("decoded restore authority changed variants");
    };
    assert_eq!(
        decoded_continuation.latest_snapshot,
        expected_latest_snapshot
    );
    let restore_app = app.clone();
    let restore_tables = tables.clone();
    let restore_cloudkit = cloudkit_ops.clone();
    let config = tokio::spawn(async move {
        let layout = StoreLayout::new(restore_app.path());
        let cancel = tokio::sync::watch::channel(false).1;
        restore_from_code(
            &restore_code,
            &restore_tables,
            &test_migrations(),
            None,
            crate::custody::KeyCustody::Keyring,
            crate::identity_custody::IdentityCustody::Keyring,
            None,
            Some(restore_cloudkit),
            &layout,
            Arc::new(SystemClock),
            Arc::new(SequentialIdProvider::new("device-b")),
            |_status: &str| {},
            &cancel,
        )
        .await
    })
    .await
    .expect("restore task completes")
    .expect("restore through code service");
    let layout = StoreLayout::new(app.path());
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
        crate::blob::TransferLimits::one_at_a_time(),
        config.device_id.clone(),
        &test_migrations(),
    )
    .expect("open B db");

    // B's first real sync cycle, with no local changes of its own.
    let joiner_storage = CloudSyncStorage::new(
        Arc::new(crate::storage::cloud::cloudkit::CloudKitCloudHome::new_private(cloudkit_ops)),
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        joiner_keypair.clone(),
    )
    .expect("build joiner cloud storage");
    let components = crate::sync::test_helpers::run_cycle_fixture(&db_b, joiner_storage, &lib_b)
        .await
        .expect("B sync cycle");
    let joiner_storage = components.storage();

    let snapshot_after = joiner_storage
        .cloud_home()
        .list("store-v1/snapshots/")
        .await
        .expect("list Store snapshot objects");
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
    let db_owner = open_test_db();
    let storage =
        crate::sync::test_helpers::TestStore::create(&db_owner, "test-lib", owner_keypair.clone())
            .await
            .expect("create exact owner Store");
    let owner_pk = pubkey_hex(&owner_keypair);
    let chain = storage
        .open_into(&db_owner)
        .await
        .expect("load exact founder membership");

    // The owner also publishes an empty snapshot, the shape
    // `bootstrap_from_snapshot` needs to succeed.
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError::Message(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier(BTreeMap::new());
    crate::sync::test_helpers::publish_snapshot_fixture(
        &storage.storage,
        &storage.root,
        snapshot,
        snapshot_coverage.clone(),
        &owner_keypair,
        &chain,
        &db_owner,
    )
    .await
    .expect("publish owner snapshot");
    publish_store_ack_fixture(
        &db_owner,
        &storage.storage,
        snapshot_coverage,
        &owner_keypair,
    )
    .await
    .expect("publish owner snapshot acknowledgement");

    let (_tmp_b, lib_b) = temp_store_dir();
    let boot = bootstrap_from_snapshot(
        &storage.storage,
        "test-lib",
        storage.root.clone(),
        &MembershipFloor(chain.head_refs().to_vec()),
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
        storage.root.clone(),
        None,
        None,
        &MembershipFloor(chain.head_refs().to_vec()),
        &storage.storage,
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
        crate::blob::TransferLimits::one_at_a_time(),
        "B".to_string(),
        &test_migrations(),
    )
    .expect("open B db");

    let pinned_owner_sql = format!(
        "SELECT value FROM protocol_state WHERE key = '{}'",
        crate::sync::store::membership::OWNER_PUBKEY_STATE_KEY
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
    let tables = test_synced_tables();
    let owner = UserKeypair::generate();
    let db_owner = open_test_db();
    let storage =
        crate::sync::test_helpers::TestStore::create(&db_owner, "test-lib", owner.clone())
            .await
            .expect("create exact owner Store");
    let member = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::store::membership::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        None,
        crate::sync::membership::MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &crate::sync::store::StoreDatabase::from_database(db_owner.clone()),
    )
    .await
    .expect("add member");
    let pre_removal_chain = crate::sync::store::pull::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::from_database(db_owner.clone()),
    )
    .await
    .expect("load pre-removal membership")
    .chain
    .expect("pre-removal membership chain");
    let pre_removal_heads = pre_removal_chain.head_refs().to_vec();
    let custody = crate::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    let live_cipher = RwLock::new(CloudCipher::Encrypted(encryption.clone()));
    crate::sync::store::membership::remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &Hlc::new("owner".to_string()),
        &pubkey_hex(&member),
        &encryption,
        &custody,
        &live_cipher,
        &PendingRotation::none(),
        &crate::sync::store::StoreDatabase::from_database(db_owner.clone()),
    )
    .await
    .expect("remove member");
    let chain = crate::sync::store::pull::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::from_database(db_owner.clone()),
    )
    .await
    .expect("load post-removal membership")
    .chain
    .expect("post-removal membership chain");

    // The restore code is minted right after the removal: its floor is the
    // current (post-removal) chain state.
    let membership_floor = MembershipFloor(chain.head_refs().to_vec());
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let tables_c = tables.clone();
    let snapshot = db_owner
        .call(move |conn| {
            create_snapshot(conn, &snap_dir, &tables_c).map_err(|e| DbError::Message(e.to_string()))
        })
        .await
        .expect("owner snapshot");
    crate::sync::test_helpers::publish_snapshot_fixture(
        &storage.storage,
        &storage.root,
        snapshot,
        crate::sync::store_commit::CommitFrontier(BTreeMap::new()),
        &owner,
        &chain,
        &db_owner,
    )
    .await
    .expect("publish post-removal snapshot");

    for head in chain.head_refs() {
        if !pre_removal_heads.contains(head) {
            storage
                .delete_protocol_object(&head.object)
                .await
                .expect("remove post-removal membership head");
        }
    }

    let (_tmp_b, lib_b) = temp_store_dir();
    let error = bootstrap_from_snapshot(
        &storage.storage,
        "test-lib",
        storage.root.clone(),
        &membership_floor,
        1,
        &lib_b.db_path(),
    )
    .await
    .expect_err("the restore must enforce its floor before accepting a snapshot");

    let message = error.to_string();
    assert!(message.contains(&owner_pk), "{message}");
    assert!(message.contains("object not found"), "{message}");
}

/// Restore bootstrap downloads every eager blob referenced by snapshot rows.
#[tokio::test]
async fn restore_bootstrap_backfills_blob_files_for_snapshot_rows() {
    Box::pin(run_restore_bootstrap_backfills_blob_files_for_snapshot_rows()).await;
}

async fn run_restore_bootstrap_backfills_blob_files_for_snapshot_rows() {
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
    .expect("build owner cloud storage");

    // Owner: a shared note with a cover photo, both captured into the snapshot.
    let db_owner = open_test_db_with_blob(BlobDecl::new(
        "photos",
        Provenance::HostProvided,
        CacheFill::CacheEager,
    ));
    let (store_root, _initial_membership) =
        Box::pin(crate::sync::test_helpers::initialize_store_fixture(
            &db_owner,
            &owner_storage,
            store_id,
            &owner_keypair,
        ))
        .await
        .expect("initialize owner Store");
    host_exec(
        &db_owner,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
    )
    .await;
    host_exec(
        &db_owner,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-owner', '2026-01-01')",
            crate::blob::content_hash(b"cover-bytes"),
        ),
    )
    .await;
    let (owner_tmp, owner_dir) = temp_store_dir();
    crate::blob::local_files::store(&owner_dir, "photos", "photo1", b"cover-bytes")
        .await
        .expect("stage owner blob");
    let components = Box::pin(crate::sync::test_helpers::run_cycle_fixture(
        &db_owner,
        owner_storage,
        &owner_dir,
    ))
    .await
    .expect("publish owner row and blob");
    let membership = Box::pin(crate::sync::store::pull::load_cycle_membership(
        components.storage().as_ref(),
        &crate::sync::store::StoreDatabase::from_database(db_owner.clone()),
    ))
    .await
    .expect("load owner membership")
    .chain
    .expect("owner membership chain");
    let (_tmp_b, lib_b) = temp_store_dir();
    let owner_blob = db_owner
        .row_blob_ref("note_photos", "photo1")
        .await
        .expect("capture exact snapshot blob");
    let expected_blob = lib_b
        .cache_blob_path(
            "photos",
            owner_blob
                .stored()
                .expect("published snapshot blob has exact storage")
                .locator()
                .locator_hash(),
        )
        .expect("cache blob path");

    let joiner_keypair = owner_keypair.clone();
    let joiner_storage = CloudSyncStorage::new(
        Arc::new(cloud.clone()) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        joiner_keypair.clone(),
    )
    .expect("build joiner cloud storage");
    let store_keys = StoreKeys::new(store_id.to_string());
    let custody = crate::custody::KeyCustody::Keyring.resolve(store_id, &lib_b);
    let identity_custody =
        crate::identity_custody::IdentityCustody::Keyring.resolve(store_id, &lib_b);
    let join_info = CloudHomeJoinInfo::CloudKit;
    let continuation = crate::sync::store::StoreDatabase::from_database(db_owner.clone())
        .export_activated_device_continuation(&joiner_keypair)
        .await
        .expect("export exact activated continuation");
    let materialized_commits_without_device_state = db_owner
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM materialized_commits AS commits \
                 LEFT JOIN store_device_state_snapshots AS states \
                   ON states.commit_ref = commits.commit_ref \
                 WHERE states.commit_ref IS NULL",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("verify source device-state snapshots");
    assert_eq!(materialized_commits_without_device_state, 0);
    let published_snapshot_bytes = db_owner
        .call(|conn| {
            conn.query_row(
                "SELECT meta_bytes FROM published_store_snapshot \
                 ORDER BY generation DESC LIMIT 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("read published snapshot metadata");
    let published_snapshot: crate::sync::store_commit::SnapshotMeta =
        serde_json::from_slice(&published_snapshot_bytes)
            .expect("parse published snapshot metadata");
    let snapshot_coverage = published_snapshot.coverage.into_refs();
    let latest_position = continuation
        .latest_position
        .as_ref()
        .expect("continuation has a latest Store position");
    let source_registration = crate::sync::store_commit::StoreDeviceRegistration::parse_at(
        &continuation.registration_bytes,
        &store_root,
        continuation.registration.device_id,
    )
    .expect("parse continuation Store registration");
    let mut expected_device_snapshots = snapshot_coverage
        .values()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut cursor = latest_position.clone();
    loop {
        if snapshot_coverage.values().any(|covered| covered == &cursor) {
            break;
        }
        expected_device_snapshots.insert(cursor.clone());
        let commit = crate::sync::store_objects::load_commit_ref(
            components.storage().as_ref(),
            store_root.store_root_hash,
            &cursor,
            &source_registration,
        )
        .await
        .expect("load continuation ancestry")
        .value;
        cursor = commit
            .order
            .predecessor()
            .cloned()
            .expect("continuation descends from the snapshot coverage");
    }
    let device_signing_key: [u8; crate::keys::SIGN_SECRETKEYBYTES] =
        hex::decode(&continuation.device_signing_secret)
            .expect("decode continuation device signing key")
            .try_into()
            .expect("continuation device signing key length");
    let device_signer = UserKeypair::from_signing_key_bytes(&device_signing_key)
        .expect("restore continuation device signer");
    let restore_device_id = continuation.registration.device_id.to_string();
    let authority = RestoreAuthority::ActivatedContinuation(continuation.clone());

    let config = Box::pin(bootstrap_and_save_store(
        &joiner_storage,
        &cipher,
        None,
        &lib_b,
        store_id,
        &restore_device_id,
        store_root,
        RestoreBootstrapContext {
            founder_pubkey: &pubkey_hex(&owner_keypair),
            keypair: &joiner_keypair,
            authority: &authority,
            continuation: Some((&continuation, &device_signer)),
        },
        &MembershipFloor(membership.head_refs().to_vec()),
        &tables,
        &test_migrations(),
        &join_info,
        "Restored Store",
        None,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &|_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    ))
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

    drop(owner_tmp);
    let (restored, _stamper) = Database::open(
        &lib_b.db_path(),
        tables,
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        config.device_id,
        &test_migrations(),
    )
    .expect("open restored database");
    let (restored_notes, restored_photos, restored_parent_links, foreign_key_violations) = restored
        .call(|conn| {
            Ok((
                conn.query_row("SELECT COUNT(*) FROM notes WHERE id = 'n1'", [], |row| {
                    row.get::<_, i64>(0)
                })?,
                conn.query_row(
                    "SELECT COUNT(*) FROM note_photos WHERE id = 'photo1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?,
                conn.query_row(
                    "SELECT COUNT(*) FROM note_photos AS photo
                     JOIN notes AS note ON note.id = photo.note_id
                     WHERE photo.id = 'photo1' AND note.id = 'n1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?,
                conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                    row.get::<_, i64>(0)
                })?,
            ))
        })
        .await
        .expect("inspect restored snapshot rows");
    assert_eq!(
        (
            restored_notes,
            restored_photos,
            restored_parent_links,
            foreign_key_violations,
        ),
        (1, 1, 1, 0)
    );
    let restored_device_snapshots = restored
        .call(|conn| {
            let mut statement = conn
                .prepare("SELECT commit_ref FROM store_device_state_snapshots")
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(DbError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(DbError::from)?;
            Ok(rows)
        })
        .await
        .expect("load restored device-state snapshots")
        .into_iter()
        .map(|encoded| {
            serde_json::from_str::<crate::sync::store_commit::StoreBatchCommitRef>(&encoded)
                .expect("parse restored device-state snapshot reference")
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(restored_device_snapshots, expected_device_snapshots);
    host_exec(
        &restored,
        "UPDATE note_photos
         SET _updated_at = '0000000002000-0000-restored'
         WHERE id = 'photo1'",
    )
    .await;
    let restored_storage = CloudSyncStorage::new(
        Arc::new(cloud) as Arc<dyn crate::storage::cloud::CloudHome>,
        cipher,
        blob_paths,
        store_id.to_string(),
        joiner_keypair,
    )
    .expect("build restored cloud storage");
    Box::pin(crate::sync::test_helpers::run_cycle_fixture(
        &restored,
        restored_storage,
        &lib_b,
    ))
    .await
    .expect("publish restored row by reusing its exact remote blob");
}
