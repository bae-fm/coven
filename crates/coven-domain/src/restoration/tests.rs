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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::joining::BootstrapError;
use crate::restoration::restore_from_code;
use crate::restoration::{
    decode_restore_code, encode_restore_code, RestoreCode, RESTORE_CODE_VERSION,
};
use crate::restoration::{OwnerRecoveryAuthority, RestoreAuthority, RestoreCodeError};
use coven_database::Database;
use coven_foundation::clock::SystemClock;
use coven_foundation::config::HomeStorage;
use coven_foundation::id_provider::SequentialIdProvider;
use coven_foundation::store_dir::StoreLayout;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{StoreKeys, UserKeypair};
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::membership::MembershipFloor;
use coven_protocol::synced_schema::BlobDecl;
use coven_replication::sync::test_helpers::{
    open_test_db, open_test_db_with_blob, pubkey_hex, temp_store_dir, test_migrations,
    test_synced_tables, test_synced_tables_with_blob, TestDevice, TestStore,
};
use coven_storage::cloud::cloudkit::{
    CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare,
};
use coven_storage::cloud::{CloudHome, CloudHomeJoinInfo};
use coven_storage::cloud::{CloudHomeError, CloudObjectVersion, CloudVersionedObject};
use coven_storage::CloudSyncObjectStorage;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

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
            environment: coven_protocol::CloudKitEnvironment::Development,
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
    let coord = coven_protocol::membership::MembershipCoord {
        author_pubkey,
        author_owner_grant: coven_protocol::membership::MembershipGrantId(
            coven_protocol::store_commit::ObjectHash::digest(b"restore test owner grant"),
        ),
        stream_id: format!("{:064x}", 1)
            .parse()
            .expect("canonical test author stream id"),
        seq: 1,
        entry_hash: coven_protocol::store_commit::ObjectHash::digest(b"restore test founder entry"),
    };
    let stored = b"restore test membership head";
    MembershipFloor(vec![coven_protocol::membership::MembershipHeadRef {
        coord,
        head_hash: coven_protocol::store_commit::ObjectHash::digest(
            b"restore test membership head semantic bytes",
        ),
        object: coven_protocol::objects::ExactObjectRef::new(
            coven_protocol::objects::ObjectSlot::logical(
                "store-v1/membership/heads/restore-test/1.json".to_string(),
            )
            .expect("valid test membership-head slot"),
            stored.len() as u64,
            coven_protocol::store_commit::ObjectHash::digest(stored),
        ),
    }])
}

fn store_root_ref(label: &str) -> coven_protocol::store_commit::StoreRootRef {
    let stored = format!("{label} stored root");
    coven_protocol::store_commit::StoreRootRef {
        store_root_id: coven_protocol::store_commit::ObjectHash::digest(
            format!("{label} identity").as_bytes(),
        ),
        store_root_hash: coven_protocol::store_commit::ObjectHash::digest(label.as_bytes()),
        object: coven_protocol::objects::ExactObjectRef::new(
            coven_protocol::objects::ObjectSlot::logical(format!(
                "store-v1/protocol/root/{}.json",
                coven_protocol::store_commit::ObjectHash::digest(label.as_bytes())
            ))
            .expect("valid test Store-root slot"),
            stored.len() as u64,
            coven_protocol::store_commit::ObjectHash::digest(stored.as_bytes()),
        ),
    }
}

fn serialized_keyring(byte: u8) -> String {
    coven_keys::encryption::MasterKeyring::from(
        coven_keys::encryption::EncryptionService::from_key([byte; 32]),
    )
    .to_serialized()
}

fn owner_recovery_authority(
    root: &coven_protocol::store_commit::StoreRootRef,
    owner: &UserKeypair,
) -> RestoreAuthority {
    let owner_grant = coven_protocol::membership::MembershipGrantId(
        coven_protocol::store_commit::ObjectHash::digest(b"restore test owner grant"),
    );
    let anchor = coven_protocol::store_commit::GrantStreamAnchor::OwnerRecovery {
        first_slot: coven_protocol::objects::ObjectSlot::logical(
            "store-v1/recovery/restore-tests/first.json".to_string(),
        )
        .expect("valid recovery slot"),
    };
    let activation = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
        root,
        &pubkey_hex(owner),
        &owner_grant,
        &anchor,
    )
    .expect("valid recovery activation");
    RestoreAuthority::OwnerRecovery(OwnerRecoveryAuthority {
        owner_identity_secret: hex::encode(owner.to_keypair_bytes()),
        owner_grant: owner_grant.clone(),
        recovery: coven_protocol::store_commit::OwnerRecoveryCursor {
            owner_grant,
            position: coven_protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                activation,
            },
        },
        published_at: "2026-07-17T00:00:00Z".to_string(),
    })
}

trait OwnerRecoveryTestDevice {
    fn published_owner_recovery_authority(&self, owner: &UserKeypair) -> RestoreAuthority;
}

impl OwnerRecoveryTestDevice for TestDevice {
    fn published_owner_recovery_authority(&self, owner: &UserKeypair) -> RestoreAuthority {
        let protocol = self.protocol_root();
        let root = self.store_root();
        let owner_grant = protocol.descriptor.founder_grant.clone();
        let activation = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
            root,
            &pubkey_hex(owner),
            &owner_grant,
            &protocol.descriptor.founder_recovery,
        )
        .expect("derive published recovery activation");
        RestoreAuthority::OwnerRecovery(OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(owner.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: coven_protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: coven_protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                    activation,
                },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        })
    }
}

/// A restore code carrying the given `sid`. The provider points at a loopback
/// endpoint nothing listens on, so if execution ever reached the network it would
/// fail at once — but a malicious `sid` is refused at decode, well before that.
fn restore_code_with_sid(sid: &str) -> String {
    let root = store_root_ref("restore test store protocol root");
    let owner = coven_keys::keys::UserKeypair::generate();
    let code = RestoreCode {
        v: RESTORE_CODE_VERSION,
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
) -> Result<coven_foundation::config::Config, BootstrapError> {
    let ids: coven_foundation::id_provider::IdRef = Arc::new(SequentialIdProvider::new("dev"));
    restore_from_code(
        code_str,
        &test_synced_tables(),
        &test_migrations(),
        coven_foundation::config::ExactUploadVerification::MetadataHash,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        coven_storage::oauth::OAuthClients::empty(),
        None,
        None,
        &coven_foundation::store_dir::StoreLayout::new(app_dir),
        Arc::new(SystemClock),
        ids,
        |_| {},
        &tokio::sync::watch::channel(false).1,
    )
    .await
}

#[tokio::test]
async fn restore_accepts_blob_schema_for_google_drive_and_reaches_provider_setup() {
    coven_keys::keys::test_keyring::install();
    let store_id = "restore-immutable-google-drive";
    let root = store_root_ref("restore root");
    let owner = coven_keys::keys::UserKeypair::generate();
    let code = encode_restore_code(&RestoreCode {
        v: RESTORE_CODE_VERSION,
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
    let tables = test_synced_tables_with_blob(coven_protocol::synced_schema::BlobDecl::new(
        "photos",
        coven_protocol::blob::Provenance::HostProvided,
        coven_protocol::blob::CacheFill::CacheLazy,
    ));
    let app = tempfile::tempdir().expect("app directory");

    let result = restore_from_code(
        &code,
        &tables,
        &test_migrations(),
        coven_foundation::config::ExactUploadVerification::MetadataHash,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        coven_storage::oauth::OAuthClients::empty(),
        None,
        None,
        &coven_foundation::store_dir::StoreLayout::new(app.path()),
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
/// propagates as `BootstrapError::RestoreCode` and the restore creates nothing outside
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
            matches!(result, Err(BootstrapError::RestoreCode(_))),
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

/// A completed store already present locally is the data, so restore refuses up
/// front with a typed error naming the store and leaves the existing files
/// untouched. "Completed" is what the saved `config.yaml` marks: the guard
/// dispatches on that marker, so the store carries one here. The endpoint is
/// unreachable to prove the guard stops before snapshot download or cleanup.
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
    coven_keys::keys::test_keyring::install();
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
    coven_keys::keys::test_keyring::install();
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
) -> Result<coven_foundation::config::Config, BootstrapError> {
    restore_from_code(
        code,
        &test_synced_tables(),
        &test_migrations(),
        coven_foundation::config::ExactUploadVerification::MetadataHash,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        coven_storage::oauth::OAuthClients::empty(),
        None,
        None,
        &coven_foundation::store_dir::StoreLayout::new(app_dir),
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
    coven_keys::keys::test_keyring::install();
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
    let store_keys = coven_keys::keys::StoreKeys::bind("cancel-preset".to_string());
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
    coven_keys::keys::test_keyring::install();
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
    coven_keys::keys::test_keyring::install();
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

/// A restore failure while saving `config.yaml`, after the key and identity are
/// durable, rolls both back. Retrying the complete restore reuses the remote
/// recovery publication left by the failed local completion.
#[tokio::test]
async fn late_config_failure_rolls_back_custody_and_retries_recovery() {
    coven_keys::keys::test_keyring::install();

    let store_id = "late-step-rollback-test";
    let tmp = tempfile::tempdir().expect("temp dir");
    let layout = StoreLayout::new(tmp.path());
    let store_dir = layout.store_dir(store_id);
    let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
    let cloud = Arc::new(
        coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        ),
    );
    let owner_keypair = UserKeypair::generate();
    let master_key = coven_keys::encryption::MasterKeyring::from(
        coven_keys::encryption::EncryptionService::from_key([0xbb; 32]),
    );
    let serialized_keyring = master_key.to_serialized();
    let cipher = CloudCipher::Encrypted(EncryptionService::from(master_key.clone()));
    let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
    let owner_storage = Arc::new(CloudSyncConnection::new(
        cloud.clone(),
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        owner_keypair.clone(),
    ));

    let tables = test_synced_tables();
    let db_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let db = open_test_db(db_store_dir.clone());
    let owner_device = TestDevice::create(
        &db,
        db_store_dir.clone(),
        owner_storage.clone(),
        store_id,
        owner_keypair.clone(),
    )
    .await
    .expect("initialize owner Store");
    let store_root = owner_device.store_root().clone();
    let membership = owner_device
        .membership()
        .await
        .expect("load owner membership");
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let store_database = coven_database::StoreDatabase::new(&db);
    crate::test_snapshots::publish_owner_snapshot(
        &owner_device,
        &store_database,
        store_root.clone(),
        snap_tmp.path(),
    )
    .await;

    let joiner_keypair = owner_keypair.clone();
    let store_keys = StoreKeys::bind(store_id.to_string());
    let identity_custody =
        coven_keys::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
    let authority = owner_device.published_owner_recovery_authority(&owner_keypair);
    let migrations = test_migrations();
    let reached_config_save = std::cell::Cell::new(false);
    let result = crate::restoration::restore_from_cloud(
        store_id,
        store_root.clone(),
        Some(&serialized_keyring),
        "Late Step Test",
        &tables,
        &migrations,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        crate::restoration::RestoreSource::new(
            CloudHomeJoinInfo::CloudKit,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            Some(cloudkit_ops.clone()),
        ),
        &MembershipFloor(membership.head_refs().to_vec()),
        &joiner_keypair,
        &authority,
        None,
        &layout,
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device-late")),
        |status| {
            if status == "Saving configuration..." {
                reached_config_save.set(true);
                assert!(
                    store_keys
                        .get_encryption_key()
                        .expect("read keyring before config save")
                        .is_some(),
                    "the encryption key is durable before the config marker",
                );
                assert_eq!(
                    identity_custody
                        .unlock()
                        .expect("read identity before config save")
                        .map(|keypair| keypair.public_key()),
                    Some(joiner_keypair.public_key()),
                    "the signing identity is durable before the config marker",
                );
                std::fs::create_dir_all(store_dir.config_path())
                    .expect("block the config file with a directory");
            }
        },
        &tokio::sync::watch::channel(false).1,
    )
    .await;

    let err = result.expect_err("the blocked config.yaml write must fail restore");
    assert!(
        matches!(&err, BootstrapError::Config(_)),
        "restore must reach the blocked config save, got {err:?}"
    );
    assert!(
        reached_config_save.get(),
        "the restore reached the final completion marker"
    );
    let candidate_prefix = "store-v1/candidates/";
    let candidates_after_failure = cloud
        .list(candidate_prefix)
        .await
        .expect("list candidate objects after config failure");

    assert!(
        !store_dir.exists(),
        "the failed restore removes its store directory",
    );
    assert!(
        store_keys
            .get_encryption_key()
            .expect("read keyring")
            .is_none(),
        "the encryption key must be rolled back",
    );
    assert!(
        identity_custody
            .unlock()
            .expect("read identity custody")
            .is_none(),
        "the imported identity must be rolled back",
    );

    let retry = Box::pin(crate::restoration::restore_from_cloud(
        store_id,
        store_root,
        Some(&serialized_keyring),
        "Late Step Test",
        &tables,
        &migrations,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        crate::restoration::RestoreSource::new(
            CloudHomeJoinInfo::CloudKit,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_storage::oauth::OAuthClients::empty(),
            None,
            Some(cloudkit_ops),
        ),
        &MembershipFloor(membership.head_refs().to_vec()),
        &joiner_keypair,
        &authority,
        None,
        &layout,
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("device-late")),
        |_status| {},
        &tokio::sync::watch::channel(false).1,
    ))
    .await
    .expect("retry reuses the activated recovery registration");
    assert_eq!(retry.device_id, "device-late-0");
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
    Box::pin(fixture.assert_restored()).await;
}

struct OwnerRecoveryRestoreFixture {
    code: String,
    owner_pubkey: String,
    tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<coven_database::Migration>,
    cloudkit_ops: Arc<RestoreCloudKitOps>,
    app: tempfile::TempDir,
}

impl OwnerRecoveryRestoreFixture {
    async fn assert_restored(self) {
        let Self {
            code,
            owner_pubkey,
            tables,
            migrations,
            cloudkit_ops,
            app,
        } = self;
        let layout = StoreLayout::new(app.path());
        let config = Box::pin(restore_from_code(
            &code,
            &tables,
            &migrations,
            coven_foundation::config::ExactUploadVerification::MetadataHash,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            coven_storage::oauth::OAuthClients::empty(),
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
        let store_dir = layout.store_dir(&config.store_id);
        let restored = Database::open(
            &store_dir.db_path(),
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id.clone(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &migrations,
        )
        .expect("open recovered database");
        let store_device_id = restored
            .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load recovered Store device identity")
            .expect("recovered Store device identity exists");
        assert_eq!(
            restored
                .get_protocol_state(coven_protocol::membership::OWNER_PUBKEY_STATE_KEY)
                .await
                .expect("load recovered Store owner"),
            Some(owner_pubkey),
            "restore pins the verified chain founder as the Store owner",
        );
        let activation: coven_protocol::store_commit::StoreDeviceRegistrationActivation = restored
            .store_device_registration_activation_for_test(&store_device_id)
            .await
            .expect("load config device activation");
        assert!(matches!(
            activation,
            coven_protocol::store_commit::StoreDeviceRegistrationActivation::Recovery { .. }
        ));
    }
}

async fn prepare_owner_recovery_restore() -> OwnerRecoveryRestoreFixture {
    coven_keys::keys::test_keyring::install();
    let store_id = "owner-recovery-restore";
    let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
    let cloud = Arc::new(
        coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        ),
    );
    let owner = UserKeypair::generate();
    let owner_storage = Arc::new(CloudSyncConnection::new(
        cloud.clone(),
        CloudCipher::Plaintext,
        BlobPathScheme::for_storage(HomeStorage::Browsable),
        store_id.to_string(),
        owner.clone(),
    ));
    let owner_db_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let owner_db = open_test_db(owner_db_store_dir.clone());
    let owner_device = TestDevice::create(
        &owner_db,
        owner_db_store_dir.clone(),
        owner_storage.clone(),
        store_id,
        owner.clone(),
    )
    .await
    .expect("initialize recovery Store");
    let root = owner_device.store_root().clone();
    let membership = owner_device
        .membership()
        .await
        .expect("load recovery membership");
    let floor = MembershipFloor(membership.head_refs().to_vec());
    let tables = test_synced_tables();
    let snapshot_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let store_database = coven_database::StoreDatabase::new(&owner_db);
    crate::test_snapshots::publish_owner_snapshot(
        &owner_device,
        &store_database,
        root.clone(),
        snapshot_tmp.path(),
    )
    .await;
    let authority = owner_device.published_owner_recovery_authority(&owner);
    let code = encode_restore_code(&RestoreCode {
        v: RESTORE_CODE_VERSION,
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
        owner_pubkey: pubkey_hex(&owner),
        tables,
        migrations: test_migrations(),
        cloudkit_ops,
        app,
    }
}

/// A restored continuation preserves its imported immutable snapshot generation.
/// When cadence is already due, the first cycle appends an exact successor
/// without replacing the imported objects.
#[tokio::test]
async fn restore_first_cycle_extends_the_imported_snapshot_stream() {
    Box::pin(async {
        coven_keys::keys::test_keyring::install();

        let store_id = "restore-anti-clobber-test";
        let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
        let cloud = coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
            cloudkit_ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
        );
        let cipher = CloudCipher::Plaintext;
        let blob_paths = BlobPathScheme::for_storage(HomeStorage::Browsable);
        let tables = test_synced_tables();
        let owner_keypair = UserKeypair::generate();

        let owner_storage = Arc::new(CloudSyncConnection::new(
            Arc::new(cloud.clone()) as Arc<dyn coven_storage::cloud::ExactCloudHome>,
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        ));

        // Owner: a store with one shared note, captured straight into the published
        // snapshot — the shape a device sees the first time it opens a shared store.
        let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
        let db_owner = open_test_db(db_owner_store_dir.clone());
        let owner_device = TestDevice::create(
            &db_owner,
            db_owner_store_dir.clone(),
            owner_storage.clone(),
            store_id,
            owner_keypair.clone(),
        )
        .await
        .expect("initialize owner Store");
        let store_root = owner_device.store_root().clone();
        let membership = owner_device
            .membership()
            .await
            .expect("load owner membership");
        db_owner
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album Title', 1, '0000000001000-0000-owner', '2026-01-01')",
            )
            .await;
        let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
        let store_database = coven_database::StoreDatabase::new(&db_owner);
        crate::test_snapshots::publish_owner_snapshot(
            &owner_device,
            &store_database,
            store_root.clone(),
            snap_tmp.path(),
        )
        .await;

        let snapshot_before = owner_storage
            .list_provider_keys_for_test("store-v1/snapshots/")
            .await
            .expect("list Store snapshot objects");

        // Device B restores through the public restore-code service over its own
        // CloudKit home onto the same records.
        let app = Arc::new(tempfile::tempdir().expect("restore app dir"));
        let joiner_keypair = owner_keypair.clone();
        let continuation = owner_device
            .export_activated_device_continuation()
            .await
            .expect("export exact activated continuation");
        let expected_latest_snapshot = continuation.latest_snapshot.clone();
        let restore_code = encode_restore_code(&RestoreCode {
            v: RESTORE_CODE_VERSION,
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
        let RestoreAuthority::ActivatedContinuation(decoded_continuation) = decoded.authority
        else {
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
                coven_foundation::config::ExactUploadVerification::MetadataHash,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                coven_keys::custody::KeyCustody::Keyring,
                coven_keys::identity_custody::IdentityCustody::Keyring,
                coven_storage::oauth::OAuthClients::empty(),
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
        let lib_b = layout.store_dir(&config.store_id);
        let store_keys = StoreKeys::bind(store_id.to_string());
        let identity_custody =
            coven_keys::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &lib_b);

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
        let other_store_keys = StoreKeys::bind("restore-anti-clobber-other-store".to_string());
        let other_identity_custody = coven_keys::identity_custody::IdentityCustody::Keyring
            .resolve(
                &other_store_keys,
                &layout.store_dir("restore-anti-clobber-other-store"),
            );
        assert!(
            other_identity_custody
                .unlock()
                .expect("read unrelated store identity")
                .is_none(),
            "restoring one store establishes no identity for another store",
        );

        let db_b = Database::open(
            &lib_b.db_path(),
            tables.clone(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id.clone(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open B db");

        // B's first real sync cycle, with no local changes of its own.
        let joiner_storage = Arc::new(CloudSyncConnection::new(
            Arc::new(
                coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                    cloudkit_ops,
                    coven_foundation::config::ExactUploadVerification::MetadataHash,
                ),
            ),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            joiner_keypair.clone(),
        ));
        coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
            coven_database::StoreDatabase::new(&db_b),
            lib_b.clone(),
        )
        .run_sync_cycle(joiner_storage.clone(), joiner_keypair)
        .await
        .expect("run B sync cycle");
        let snapshot_after = joiner_storage
            .list_provider_keys_for_test("store-v1/snapshots/")
            .await
            .expect("list Store snapshot objects");
        assert!(
            snapshot_before
                .iter()
                .all(|object| snapshot_after.contains(object)),
            "the restored cycle preserves every imported snapshot object",
        );
        let imported_snapshot = expected_latest_snapshot.expect("continued snapshot reference");
        let (successor_generation, successor_bytes) = db_b
            .latest_published_store_snapshot_for_test()
            .await
            .expect("read continued snapshot stream");
        let successor: coven_protocol::store_commit::SnapshotMeta =
            serde_json::from_slice(&successor_bytes).expect("parse continued snapshot metadata");
        assert_eq!(
            u64::try_from(successor_generation).expect("non-negative snapshot generation"),
            imported_snapshot
                .generation
                .checked_add(1)
                .expect("snapshot generation successor"),
            "the due snapshot advances exactly one immutable generation",
        );
        assert_eq!(
            successor.predecessor,
            Some(imported_snapshot),
            "the due snapshot extends the imported exact generation",
        );
    })
    .await;
}

/// Mirrors join_tests.rs's `a_fresh_joiner_refuses_a_rolled_back_membership_head`:
/// a restore code seeds the same per-author watermark from its own floor. Owner
/// pinning follows this call in the restore flow, but the accepted floor is
/// already authoritative: the bootstrap pull must reject a lower signed head
/// instead of treating a failed unpinned chain load as pre-initialization.
#[tokio::test]
async fn a_fresh_restorer_refuses_a_rolled_back_membership_head_during_bootstrap() {
    let owner = UserKeypair::generate();
    let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let db_owner = open_test_db(db_owner_store_dir.clone());
    let fixture = TestStore::create_with_connection(
        &db_owner,
        db_owner_store_dir.clone(),
        "test-lib",
        owner.clone(),
        coven_replication::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact owner Store");
    let (storage, cloud_storage) = fixture;
    let member = UserKeypair::generate();
    let owner_pk = pubkey_hex(&owner);
    let encryption = EncryptionService::from_key([42; 32]);
    storage
        .admit_member(
            &db_owner,
            db_owner_store_dir.clone(),
            &owner,
            &pubkey_hex(&member),
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("add member");
    let owner_device = storage
        .bind_device(&db_owner, db_owner_store_dir.clone(), &owner)
        .await
        .expect("bind owner Store");
    let pre_removal_chain = owner_device
        .membership()
        .await
        .expect("load pre-removal membership");
    let pre_removal_heads = pre_removal_chain.head_refs().to_vec();
    let custody = coven_replication::sync::test_helpers::TestCustody::default();
    custody.set_initial_key([42; 32]);
    storage
        .remove_member(
            &db_owner,
            db_owner_store_dir.clone(),
            &owner,
            &pubkey_hex(&member),
            &encryption,
            &custody,
        )
        .await
        .expect("remove member");
    let chain = owner_device
        .membership()
        .await
        .expect("load post-removal membership");

    // The restore code is minted right after the removal: its floor is the
    // current (post-removal) chain state.
    let membership_floor = MembershipFloor(chain.head_refs().to_vec());
    let snap_tmp = tempfile::tempdir().expect("snapshot temp dir");
    let snap_dir = snap_tmp.path().to_path_buf();
    let store_database = coven_database::StoreDatabase::new(&db_owner);
    let snapshot = store_database
        .capture_snapshot_image_for_test(storage.root(), snap_dir, None)
        .await
        .expect("owner snapshot");
    // Deliberately unacknowledged: this case is about the membership floor, not
    // about stability. The coverage is still the frontier the image holds.
    owner_device
        .publish_snapshot(
            snapshot,
            crate::test_snapshots::captured_coverage(&store_database).await,
        )
        .await
        .expect("publish post-removal snapshot");

    for head in chain.head_refs() {
        if !pre_removal_heads.contains(head) {
            cloud_storage
                .delete_protocol_object(&head.object)
                .await
                .expect("remove post-removal membership head");
        }
    }

    let (_tmp_b, lib_b) = temp_store_dir();
    let error = storage
        .prepare_snapshot_bootstrap(
            &membership_floor,
            1,
            &lib_b.db_path(),
            &UserKeypair::generate(),
        )
        .await
        .expect_err("the restore must enforce its floor before accepting a snapshot");

    let message = error.to_string();
    assert!(message.contains(&owner_pk), "{message}");
    assert!(message.contains("object not found"), "{message}");
}

/// Restore bootstrap installs the complete row graph without downloading eager
/// blobs. CacheEager materialization belongs to the connected post-open worker,
/// so restore completion is never coupled to artwork availability.
#[tokio::test]
async fn restore_bootstrap_defers_eager_blob_files_until_open() {
    Box::pin(async {
        coven_keys::keys::test_keyring::install();

        let store_id = "restore-blob-backfill-test";
        let cloudkit_ops = Arc::new(RestoreCloudKitOps::new());
        let cloud = Arc::new(
            coven_storage::cloud::cloudkit::CloudKitCloudHome::new_private(
                cloudkit_ops.clone(),
                coven_foundation::config::ExactUploadVerification::MetadataHash,
            ),
        );
        let master_key =
            coven_keys::encryption::MasterKeyring::from(EncryptionService::from_key([7u8; 32]));
        let serialized_keyring = master_key.to_serialized();
        let cipher = CloudCipher::Encrypted(EncryptionService::from(master_key));
        let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
        let tables = test_synced_tables_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        ));
        let owner_keypair = UserKeypair::generate();

        let owner_storage = Arc::new(CloudSyncConnection::new(
            cloud.clone(),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        ));

        // Owner: a shared note with a cover photo, both captured into the snapshot.
        let db_owner_store_dir = coven_replication::sync::test_helpers::test_store_dir();
        let db_owner = open_test_db_with_blob(
            db_owner_store_dir.clone(),
            BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager),
        );
        let owner_device = Box::pin(TestDevice::create(
            &db_owner,
            db_owner_store_dir.clone(),
            owner_storage,
            store_id,
            owner_keypair.clone(),
        ))
        .await
        .expect("initialize owner Store");
        let store_root = owner_device.store_root().clone();
        db_owner
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
            )
            .await;
        db_owner
            .execute_test_host_write(&format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('photo1', 'n1', 'cover', 11, '{}', '0000000001000-0000-owner', '2026-01-01')",
                coven_protocol::blob::content_hash(b"cover-bytes"),
            ))
            .await;
        coven_foundation::store_dir::StoreDir::store_local_blob(
            &db_owner_store_dir,
            "photos",
            "photo1",
            b"cover-bytes",
        )
        .await
        .expect("stage owner blob");
        let cycle_storage = CloudSyncConnection::new(
            cloud.clone(),
            cipher.clone(),
            blob_paths,
            store_id.to_string(),
            owner_keypair.clone(),
        );
        Box::pin(
            coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
                coven_database::StoreDatabase::new(&db_owner),
                db_owner_store_dir.clone(),
            )
            .run_sync_cycle(cycle_storage, owner_keypair.clone()),
        )
        .await
        .expect("publish owner row and blob");
        let membership = owner_device
            .membership()
            .await
            .expect("load owner membership");
        let restore_app = tempfile::tempdir().expect("restore app dir");
        let layout = StoreLayout::new(restore_app.path());
        let lib_b = layout.store_dir(store_id);
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
        let continuation = owner_device
            .export_activated_device_continuation()
            .await
            .expect("export exact activated continuation");
        let materialized_commits_without_device_state = db_owner
            .materialized_commits_without_device_state_count_for_test()
            .await
            .expect("verify source device-state snapshots");
        assert_eq!(materialized_commits_without_device_state, 0);
        let published_snapshot_bytes = db_owner
            .latest_published_store_snapshot_bytes_for_test()
            .await
            .expect("read published snapshot metadata");
        let published_snapshot: coven_protocol::store_commit::SnapshotMeta =
            serde_json::from_slice(&published_snapshot_bytes)
                .expect("parse published snapshot metadata");
        let snapshot_coverage = published_snapshot.coverage.clone().into_refs();
        let snapshot_frontier =
            coven_protocol::store_commit::CommitFrontier::from_refs(snapshot_coverage.clone())
                .expect("snapshot coverage has valid stream ids");
        let latest_position = continuation
            .latest_position
            .as_ref()
            .expect("continuation has a latest Store position");
        let source_registration = coven_protocol::store_commit::StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &store_root,
            continuation.registration.device_id,
        )
        .expect("parse continuation Store registration");
        let mut expected_device_snapshots = snapshot_coverage
            .values()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let ancestry = owner_device
            .load_commit_ancestry_until(latest_position.clone(), &snapshot_frontier)
            .await
            .expect("load continuation ancestry");
        for (reference, commit) in ancestry {
            expected_device_snapshots.insert(reference);
            assert_eq!(commit.author(), &source_registration);
        }
        let device_signing_key: [u8; coven_keys::keys::SIGN_SECRETKEYBYTES] =
            hex::decode(&continuation.device_signing_secret)
                .expect("decode continuation device signing key")
                .try_into()
                .expect("continuation device signing key length");
        let device_signer = UserKeypair::from_signing_key_bytes(&device_signing_key)
            .expect("restore continuation device signer");
        let authority = RestoreAuthority::ActivatedContinuation(continuation.clone());

        let config = Box::pin(crate::restoration::restore_from_cloud(
            store_id,
            store_root,
            Some(&serialized_keyring),
            "Restored Store",
            &tables,
            &test_migrations(),
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            coven_keys::custody::KeyCustody::Keyring,
            coven_keys::identity_custody::IdentityCustody::Keyring,
            crate::restoration::RestoreSource::new(
                CloudHomeJoinInfo::CloudKit,
                coven_foundation::config::ExactUploadVerification::MetadataHash,
                coven_storage::oauth::OAuthClients::empty(),
                None,
                Some(cloudkit_ops),
            ),
            &MembershipFloor(membership.head_refs().to_vec()),
            &joiner_keypair,
            &authority,
            Some(&device_signer),
            &layout,
            Arc::new(SystemClock),
            Arc::new(SequentialIdProvider::new("unused-continuation-device")),
            |_status| {},
            &tokio::sync::watch::channel(false).1,
        ))
        .await
        .expect("restore bootstrap installs the snapshot rows");

        assert!(
            !expected_blob.exists(),
            "the cover blob file must remain remote after restore at {}",
            expected_blob.display(),
        );

        let restored = Database::open(
            &lib_b.db_path(),
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            config.device_id,
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
        .expect("open restored database");
        let (restored_notes, restored_photos, restored_parent_links, foreign_key_violations) =
            restored
                .restored_row_graph_counts_for_test()
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
            .store_device_state_snapshot_refs_for_test()
            .await
            .expect("load restored device-state snapshots")
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(restored_device_snapshots, expected_device_snapshots);
        restored
            .execute_test_host_write(
                "UPDATE note_photos
         SET _updated_at = '0000000002000-0000-restored'
         WHERE id = 'photo1'",
            )
            .await;
        let restored_storage = CloudSyncConnection::new(
            cloud,
            cipher,
            blob_paths,
            store_id.to_string(),
            joiner_keypair.clone(),
        );
        Box::pin(
            coven_replication::sync::test_owner_graph::TestOwnerGraph::new(
                coven_database::StoreDatabase::new(&restored),
                lib_b.clone(),
            )
            .run_sync_cycle(restored_storage, joiner_keypair),
        )
        .await
        .expect("publish restored row by reusing its exact remote blob");
    })
    .await;
}
