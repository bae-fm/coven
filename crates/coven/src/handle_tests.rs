use super::*;

use crate::store_sync::{ConfigProvider, SyncError};
use coven_foundation::clock::SystemClock;
use coven_foundation::config::{CloudProvider, Config, HomeStorage};
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_keys::keys::{test_keyring, StoreKeys};
use coven_protocol::blob::{CacheFill, Provenance};
use coven_replication::sync::test_helpers::{
    read_test_db, temp_store_dir, test_migrations, test_synced_tables_with_blob, TestStore,
};
use coven_storage::cloud::cloudkit::{
    CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare,
};
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::cloud::CloudHomeError;
use coven_storage::{BlobPathScheme, CloudCipher, ExactSlotStorage};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

type TestCloudKitCoordinate = (CloudKitScope, String);
type TestCloudKitObject = (Vec<u8>, u64);

struct TestCloudKitOps {
    store: Mutex<HashMap<TestCloudKitCoordinate, TestCloudKitObject>>,
    shares: Mutex<HashMap<String, CloudKitShare>>,
    batches: Mutex<HashMap<String, Vec<CloudKitRecordCreate>>>,
    next_batch: AtomicUsize,
}

/// A ready-to-use custody for tests that build a [`CovenHandle`] directly
/// (bypassing the builder) and never exercise master-key lifecycle
/// methods — the blob/storage/status tests in this module. Seeded
/// in-memory so it needs no keyring registration.
fn test_store_keys(store_id: &str) -> StoreKeys {
    coven_keys::keys::test_keyring::install();
    StoreKeys::bind(store_id.to_string())
}

fn test_key_custody() -> Arc<dyn coven_keys::keys::MasterKeyCustody> {
    let store_keys = test_store_keys("unused-store-id");
    coven_keys::custody::KeyCustody::InMemory(coven_keys::encryption::MasterKeyring::generate())
        .resolve(
            &store_keys,
            &coven_foundation::store_dir::StoreDir::new_ephemeral("unused-store-dir"),
        )
}

/// A ready-to-use identity custody for the same tests, seeded in-memory
/// so it needs no keyring registration — the identity sibling of
/// [`test_key_custody`].
fn test_identity_custody() -> Arc<dyn DeviceIdentityCustody> {
    let store_keys = test_store_keys("unused-store-id");
    coven_keys::identity_custody::IdentityCustody::InMemory(
        coven_keys::keys::UserKeypair::generate(),
    )
    .resolve(
        &store_keys,
        &coven_foundation::store_dir::StoreDir::new_ephemeral("unused-store-dir"),
    )
}

fn host_blob_test_db(namespace: &str, store_dir: &StoreDir) -> coven_database::Database {
    coven_database::Database::open_synthetic_for_test(
        &store_dir.db_path(),
        store_dir.clone(),
        test_synced_tables_with_blob(
            coven_protocol::synced_schema::BlobDecl::new(
                namespace,
                Provenance::HostProvided,
                CacheFill::CacheLazy,
            )
            .with_cloud_path_column("cloud_path"),
        ),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        Arc::new(SystemClock),
        &test_migrations(),
    )
    .expect("open host blob test database")
}

trait HostBlobTestOps {
    async fn queue_host_blob(
        &self,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
        remote: bool,
    ) -> crate::WriteId;

    async fn wait_for_host_blob_publication(
        &self,
        id: &str,
        write_id: &crate::WriteId,
    ) -> RowBlobRef;

    async fn publish_host_blob(&self, id: &str, cloud_path: &str, bytes: &[u8]) -> RowBlobRef;
}

impl HostBlobTestOps for CovenHandle {
    async fn queue_host_blob(
        &self,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
        remote: bool,
    ) -> crate::WriteId {
        let note_id = format!("note-{id}");
        let id = id.to_string();
        let cloud_path = cloud_path.to_string();
        let bytes = bytes.to_vec();
        let size = bytes.len() as i64;
        let hash = coven_protocol::blob::content_hash(&bytes);
        let write = self
                .write_with_blobs(
                    {
                        let id = id.clone();
                        let bytes = bytes.clone();
                        move |batch| {
                            batch.put_blob("images", id, bytes);
                            Ok(())
                        }
                    },
                    {
                        let id = id.clone();
                        move |sql| {
                            let stamp = sql.stamp();
                            sql.execute(
                                "INSERT INTO notes \
                                 (id, title, shared, _updated_at, created_at) \
                                 VALUES (?1, 'blob owner', ?2, ?3, '2026-01-01')",
                                rusqlite::params![note_id, remote as i64, stamp],
                            )?;
                            sql.execute(
                                "INSERT INTO note_photos \
                                 (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
                                 VALUES (?1, ?2, 'cover', ?3, ?4, ?5, '2026-01-01', ?6)",
                                rusqlite::params![id, note_id, size, hash, stamp, cloud_path],
                            )?;
                            Ok(())
                        }
                    },
                )
                .await
                .expect("queue host blob write");
        write.write_id
    }

    async fn wait_for_host_blob_publication(
        &self,
        id: &str,
        write_id: &crate::WriteId,
    ) -> RowBlobRef {
        let mut status = self
            .subscribe_write_status(write_id)
            .await
            .expect("subscribe to host blob publication");
        self.sync_now();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let current = status.borrow().clone();
                match current {
                    crate::WriteStatus::Published(_) => break,
                    crate::WriteStatus::Pending | crate::WriteStatus::Publishing => status
                        .changed()
                        .await
                        .expect("write status channel remains open"),
                    other => panic!("host blob write did not publish: {other:?}"),
                }
            }
        })
        .await
        .expect("host blob publishes");
        self.row_blob_ref("note_photos", id)
            .await
            .expect("capture published host blob row")
    }

    async fn publish_host_blob(&self, id: &str, cloud_path: &str, bytes: &[u8]) -> RowBlobRef {
        let write_id = self.queue_host_blob(id, cloud_path, bytes, true).await;
        self.wait_for_host_blob_publication(id, &write_id).await
    }
}

#[tokio::test]
async fn read_blob_with_unbuildable_storage_is_a_typed_setup_error_not_io() {
    let (_tmp, store_dir) = temp_store_dir();
    let db = host_blob_test_db("images", &store_dir);
    let mut config = Config::with_defaults(
        "lib-setup-error".to_string(),
        "device".to_string(),
        "Test".to_string(),
    );
    // A provider is selected but its bucket is unset, so the read path cannot
    // build sync storage. That is a configuration fault the user must fix — it
    // must reach the caller as StorageSetup, not be mislabeled as disk I/O.
    config.cloud_home.provider = Some(CloudProvider::S3);
    let config_provider: ConfigProvider = Arc::new(move || config.clone());
    let handle = CovenHandle::new(
        db.clone(),
        // `read_db`: this test never calls `read`, so the writer clone stands in.
        db.clone(),
        store_dir.clone(),
        config_provider,
        StoreKeys::bind("lib-setup-error".to_string()),
        test_key_custody(),
        test_identity_custody(),
        coven_storage::oauth::OAuthClients::empty(),
        Arc::new(SystemClock),
        None,
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        coven_storage::BlobChunking::DEFAULT,
    );

    db.plant_blob_row_for_test("anyblob0", false, b"typed setup error")
        .await;
    let blob = db
        .row_blob_ref("note_photos", "anyblob0")
        .await
        .expect("capture local blob row");
    let err = handle
        .read_blob(&blob)
        .await
        .expect_err("no sync storage can be built from the broken config");
    assert!(
        matches!(err, BlobCacheError::StorageSetup(_)),
        "got {err:?}"
    );
}

fn test_handle(store_id: &str, store_dir: StoreDir, db: coven_database::Database) -> CovenHandle {
    test_handle_with_custody_and_storage(
        store_id,
        store_dir,
        db,
        test_key_custody(),
        HomeStorage::Browsable,
    )
}

fn test_handle_with_custody(
    store_id: &str,
    store_dir: StoreDir,
    db: coven_database::Database,
    key_custody: coven_keys::custody::KeyCustody,
) -> CovenHandle {
    let store_keys = test_store_keys(store_id);
    let key_custody = key_custody.resolve(&store_keys, &store_dir);
    test_handle_with_custody_and_storage(store_id, store_dir, db, key_custody, HomeStorage::Opaque)
}

fn test_handle_with_custody_and_storage(
    store_id: &str,
    store_dir: StoreDir,
    db: coven_database::Database,
    key_custody: Arc<dyn coven_keys::keys::MasterKeyCustody>,
    storage: HomeStorage,
) -> CovenHandle {
    let db = db;
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = storage;
    let config_provider: ConfigProvider = Arc::new(move || config.clone());
    CovenHandle::new(
        db.clone(),
        // `read_db`: these tests never call `read`, and the test db is
        // `:memory:` (unique per connection, no shareable read-only companion),
        // so the writer clone stands in.
        db.clone(),
        store_dir.clone(),
        config_provider,
        StoreKeys::bind(store_id.to_string()),
        key_custody,
        test_identity_custody(),
        coven_storage::oauth::OAuthClients::empty(),
        Arc::new(SystemClock),
        None,
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        coven_storage::BlobChunking::DEFAULT,
    )
}

impl TestCloudKitOps {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            shares: Mutex::new(HashMap::new()),
            batches: Mutex::new(HashMap::new()),
            next_batch: AtomicUsize::new(0),
        }
    }
}

impl CloudKitOps for TestCloudKitOps {
    fn provider_identity(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
        let (owner_name, zone_name) = match scope {
            CloudKitScope::Private => ("test-owner", "test-zone"),
            CloudKitScope::Shared {
                owner_name,
                zone_name,
            } => (owner_name.as_str(), zone_name.as_str()),
        };
        Ok(CloudKitProviderIdentity {
            container_id: "iCloud.test.coven".to_string(),
            environment: crate::CloudKitEnvironment::Development,
            owner_name: owner_name.to_string(),
            zone_name: zone_name.to_string(),
            current_user_record_name: "test-user".to_string(),
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
        let mut store = self.store.lock().unwrap();
        let coordinate = (scope.clone(), key.to_string());
        let version = store.get(&coordinate).map_or(1, |(_, version)| version + 1);
        store.insert(coordinate, (data, version));
        Ok(())
    }

    fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.store
            .lock()
            .unwrap()
            .get(&(scope.clone(), key.to_string()))
            .map(|(bytes, _)| bytes.clone())
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
    }

    fn list_records(
        &self,
        scope: &CloudKitScope,
        prefix: &str,
    ) -> Result<Vec<String>, CloudHomeError> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .keys()
            .filter(|(stored_scope, key)| stored_scope == scope && key.starts_with(prefix))
            .map(|(_, key)| key.clone())
            .collect())
    }

    fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError> {
        self.store
            .lock()
            .unwrap()
            .remove(&(scope.clone(), key.to_string()));
        Ok(())
    }

    fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .contains_key(&(scope.clone(), key.to_string())))
    }

    fn read_versioned_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
    ) -> Result<coven_storage::cloud::CloudVersionedObject, CloudHomeError> {
        let store = self.store.lock().unwrap();
        let (bytes, version) = store
            .get(&(scope.clone(), key.to_string()))
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        Ok(coven_storage::cloud::CloudVersionedObject {
            bytes: bytes.clone(),
            version: coven_storage::cloud::CloudObjectVersion::from_provider(version.to_string())?,
        })
    }

    fn replace_record_if_version(
        &self,
        scope: &CloudKitScope,
        key: &str,
        expected: &coven_storage::cloud::CloudObjectVersion,
        data: Vec<u8>,
    ) -> Result<coven_storage::cloud::ConditionalWriteOutcome, CloudHomeError> {
        let mut store = self.store.lock().unwrap();
        let coordinate = (scope.clone(), key.to_string());
        let (_, current) = store
            .get(&coordinate)
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        if current.to_string() != expected.as_provider() {
            return Ok(coven_storage::cloud::ConditionalWriteOutcome::VersionChanged);
        }
        let next = current
            .checked_add(1)
            .expect("handle record version overflow");
        store.insert(coordinate, (data, next));
        Ok(coven_storage::cloud::ConditionalWriteOutcome::Replaced(
            coven_storage::cloud::CloudObjectVersion::from_provider(next.to_string())?,
        ))
    }

    fn begin_atomic_create(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
        let batch = CloudKitAtomicCreateBatch::from_provider(format!(
            "handle-batch-{}",
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
        let mut store = self.store.lock().unwrap();
        for create in creates {
            if store.contains_key(&(scope.clone(), create.key.clone())) {
                return Err(CloudHomeError::AlreadyExists(create.key.clone()));
            }
        }
        let creates = batches
            .remove(batch.as_provider())
            .expect("validated handle CloudKit batch disappeared");
        let mut created = Vec::with_capacity(creates.len());
        for create in creates {
            store.insert((scope.clone(), create.key.clone()), (create.data, 1));
            created.push(CloudKitRecordVersion {
                key: create.key,
                version: coven_storage::cloud::CloudObjectVersion::from_provider("1".to_string())?,
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
        let mut store = self.store.lock().unwrap();
        for record in exact_records {
            let coordinate = (scope.clone(), record.key.clone());
            let (_, version) = store
                .get(&coordinate)
                .ok_or_else(|| CloudHomeError::NotFound(record.key.clone()))?;
            if version.to_string() != record.version.as_provider() {
                return Err(CloudHomeError::Transport(format!(
                    "handle CloudKit record {:?} changed before exact deletion",
                    record.key
                )));
            }
        }
        for record in exact_records {
            store.remove(&(scope.clone(), record.key.clone()));
        }
        Ok(())
    }

    fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
        let share = CloudKitShare {
            share_url: format!("coven-test-share-{member_pubkey}"),
            owner_name: "owner".to_string(),
            zone_name: "zone".to_string(),
        };
        self.shares
            .lock()
            .unwrap()
            .insert(member_pubkey.to_string(), share.clone());
        Ok(share)
    }

    fn share_for_member(
        &self,
        member_pubkey: &str,
    ) -> Result<Option<CloudKitShare>, CloudHomeError> {
        Ok(self.shares.lock().unwrap().get(member_pubkey).cloned())
    }

    fn revoke_share(&self, member_pubkey: &str) -> Result<(), CloudHomeError> {
        self.shares.lock().unwrap().remove(member_pubkey);
        Ok(())
    }

    fn accept_share(&self, _share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
        Ok(CloudKitShare {
            share_url: "coven-test-share".to_string(),
            owner_name: "owner".to_string(),
            zone_name: "zone".to_string(),
        })
    }
}

/// `connect_sync_with_test_home` starts the production sync loop over an injected
/// `InMemoryCloudHome`. A host write creates a pending exact Store row/blob; the
/// loop uploads and publishes it, and `read_blob` uses the activated row-bound
/// locator to read the same object through the handle.
#[tokio::test]
async fn test_home_drives_drain_and_read_through_the_handle() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            // `note_photos` carries a blob in the `images` namespace so the read path can
            // resolve a planted row up to its gated `notes` root (the gate that decides
            // Local vs Remote).
            let db = host_blob_test_db("images", &store_dir);

            // Pre-create the exact Store in the same home the handle will connect to,
            // with the same signing identity and cipher.
            let mut config = Config::with_defaults(
                "lib-test".to_string(),
                "test-device".to_string(),
                "Test Store".to_string(),
            );
            config.cloud_home.storage = HomeStorage::Opaque;
            let config_provider: ConfigProvider = {
                let config = config.clone();
                Arc::new(move || config.clone())
            };
            let signer = coven_keys::keys::UserKeypair::generate();
            let home = coven_replication::sync::test_helpers::test_cloud_home();
            TestStore::create(
                &db,
                store_dir.clone(),
                "lib-test",
                signer.clone(),
                home.clone(),
            )
            .await
            .expect("create exact test Store");
            let store_keys = test_store_keys("lib-test");
            let identity_custody = coven_keys::identity_custody::IdentityCustody::InMemory(signer)
                .resolve(&store_keys, &store_dir);

            let handle = CovenHandle::new(
                db.clone(),
                // `read_db`: this test never calls `read`, so the writer clone stands in.
                db.clone(),
                store_dir.clone(),
                config_provider,
                store_keys,
                test_key_custody(),
                identity_custody,
                coven_storage::oauth::OAuthClients::empty(),
                Arc::new(SystemClock),
                None,
                None,
                StoreOpenGuard::acquire_for_test(&store_dir),
                coven_storage::BlobChunking::DEFAULT,
            );

            // Inject the mock home; the host hands over only the home + cipher.
            handle
                .connect_sync_with_test_home(
                    home.clone(),
                    CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
                )
                .await
                .expect("connect over the injected test home");
            let mut outbox = handle.subscribe_cloud_outbox();
            assert!(outbox
                .next()
                .await
                .expect("read initial cloud outbox")
                .uploads
                .is_empty());

            let plaintext = b"cover-art-bytes-for-the-test-home".to_vec();
            handle
                .queue_host_blob("cover-1", "cover-cover-1.jpg", &plaintext, false)
                .await;
            handle
                .make_remote_with_discovered_order_for_test(
                    "notes",
                    "note-cover-1",
                    "Notes Root",
                    false,
                )
                .await
                .expect("queue the exact row/blob transition");
            handle.sync_now();
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let snapshot = outbox.next().await.expect("read cloud outbox change");
                    if snapshot.uploads.is_empty() && snapshot.make_remotes.is_empty() {
                        break;
                    }
                }
            })
            .await
            .expect("the production loop publishes the make-remote transition");
            let blob = handle
                .row_blob_ref("note_photos", "cover-1")
                .await
                .expect("capture Remote row after loop publication");
            let object = blob
                .stored()
                .expect("published blob has exact storage")
                .object();
            let at_rest = home
                .read_at(object.slot())
                .await
                .expect("the exact blob object exists");
            assert!(
                !at_rest.is_empty(),
                "the exact blob object contains its sealed payload",
            );

            // The published `RowBlobRef` carries the exact remote object and authority;
            // the read resolves it through the same connected home.
            let read = handle
                .read_blob(&blob)
                .await
                .expect("read through the handle");
            assert_eq!(
                read, plaintext,
                "read_blob fetched the blob's plaintext from the injected test home",
            );
        })
        .await;
}

/// `connect_sync_with_test_home_caller_driven` installs the same connection
/// its loop-starting sibling does, minus the loop thread — so the host's own
/// `drain_uploads` is the only drain of the queue, and its count is the whole
/// truth about what went to the cloud.
///
/// The loop-started connect cannot promise that. Its cycle drains the same
/// queue the host does; both succeed, and whichever runs second finds the
/// rows gone and reports an empty queue. A host asserting "my drain uploaded
/// one blob" therefore fails intermittently, which is what sent this test
/// here. Nothing below waits for a window to elapse: with no thread there is
/// no second drainer to wait for, and every assertion holds by construction.
#[tokio::test]
async fn caller_driven_connect_leaves_the_only_drain_to_the_caller() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            let db = host_blob_test_db("images", &store_dir);

            let mut config = Config::with_defaults(
                "lib-caller-driven".to_string(),
                "test-device".to_string(),
                "Test Store".to_string(),
            );
            config.cloud_home.storage = HomeStorage::Opaque;
            let config_provider: ConfigProvider = {
                let config = config.clone();
                Arc::new(move || config.clone())
            };
            let signer = coven_keys::keys::UserKeypair::generate();
            let home = coven_replication::sync::test_helpers::test_cloud_home();
            TestStore::create(
                &db,
                store_dir.clone(),
                "lib-caller-driven",
                signer.clone(),
                home.clone(),
            )
            .await
            .expect("create exact test Store");
            let store_keys = test_store_keys("lib-caller-driven");
            let identity_custody = coven_keys::identity_custody::IdentityCustody::InMemory(signer)
                .resolve(&store_keys, &store_dir);

            let handle = CovenHandle::new(
                db.clone(),
                // `read_db`: this test never calls `read`, so the writer clone stands in.
                db.clone(),
                store_dir.clone(),
                config_provider,
                store_keys,
                test_key_custody(),
                identity_custody,
                coven_storage::oauth::OAuthClients::empty(),
                Arc::new(SystemClock),
                None,
                // No paused-drain observer: holding a running loop off the queue is
                // the dance this connect exists to remove.
                None,
                StoreOpenGuard::acquire_for_test(&store_dir),
                coven_storage::BlobChunking::DEFAULT,
            );

            handle
                .connect_sync_with_test_home_caller_driven(
                    home.clone(),
                    CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
                )
                .await
                .expect("connect over the injected test home without a loop");
            assert!(!handle.is_syncing());
            assert!(matches!(
                handle.circles().create("caller-driven circle").await,
                Err(crate::CircleError::LoopNotRunning)
            ));

            let plaintext = b"caller-driven-cover-art".to_vec();
            handle
                .queue_host_blob("cover-1", "cover-cover-1.jpg", &plaintext, false)
                .await;
            handle
                .make_remote_with_discovered_order_for_test(
                    "notes",
                    "note-cover-1",
                    "Notes Root",
                    false,
                )
                .await
                .expect("queue the exact row/blob transition");

            // The queue still holds the upload. No cycle exists to have taken it, so
            // this is a fact about the connection rather than a race this test won.
            assert_eq!(
                handle
                    .queued_uploads()
                    .await
                    .expect("read the durable upload queue")
                    .len(),
                1,
            );
            let local = handle
                .row_blob_ref("note_photos", "cover-1")
                .await
                .expect("capture Local row before the drain");
            assert!(matches!(
                local.authority(),
                coven_protocol::blob::RowBlobAuthority::Local
            ));

            let outcome = handle
                .drain_uploads()
                .await
                .expect("drain the queue through the public handle");
            assert!(
                matches!(
                    outcome,
                    DrainOutcome::Drained {
                        uploaded: 1,
                        yielded_for_publish: true,
                        ..
                    }
                ),
                "the caller's drain uploaded the blob and reported it: {outcome:?}",
            );

            // The blob's bytes are in the injected home, read back through the row's
            // own published locator.
            let blob = handle
                .row_blob_ref("note_photos", "cover-1")
                .await
                .expect("capture Remote row after the drain");
            assert_eq!(
                handle
                    .read_blob(&blob)
                    .await
                    .expect("read through the handle"),
                plaintext,
            );

            assert!(
                !handle.is_syncing(),
                "no loop thread appeared over the connection's life",
            );
        })
        .await;
}

/// The chunk size a handle is built with is what the connected sync storage
/// seals under. The receipt is the stored object's own header: it names the
/// configured size, so the setting decides how little a later ranged read can
/// fetch. A connect path that builds its connection or its storage on
/// `BlobChunking::DEFAULT` instead seals at 64 KiB and this fails.
#[tokio::test]
async fn connected_seal_honors_the_handles_configured_blob_chunking() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            test_keyring::install();

            // Distinctive on both axes: neither number is `BlobChunking::DEFAULT`'s
            // (64 KiB chunk, 1 MiB window), so a dropped configuration is visible
            // rather than coinciding with the default.
            const CHUNK: u32 = 4096;
            let chunking = coven_storage::BlobChunking::new(
                std::num::NonZeroU32::new(CHUNK).expect("nonzero chunk"),
                std::num::NonZeroU64::new(1 << 16).expect("nonzero window"),
            );

            let (_tmp, store_dir) = temp_store_dir();
            let db = host_blob_test_db("images", &store_dir);

            let mut config = Config::with_defaults(
                "lib-chunking".to_string(),
                "test-device".to_string(),
                "Test Store".to_string(),
            );
            config.cloud_home.storage = HomeStorage::Opaque;
            let config_provider: ConfigProvider = {
                let config = config.clone();
                Arc::new(move || config.clone())
            };
            let signer = coven_keys::keys::UserKeypair::generate();
            let home = coven_replication::sync::test_helpers::test_cloud_home();
            TestStore::create(
                &db,
                store_dir.clone(),
                "lib-chunking",
                signer.clone(),
                home.clone(),
            )
            .await
            .expect("create exact test Store");
            let store_keys = test_store_keys("lib-chunking");
            let identity_custody = coven_keys::identity_custody::IdentityCustody::InMemory(signer)
                .resolve(&store_keys, &store_dir);
            let handle = CovenHandle::new(
                db.clone(),
                // `read_db`: this test never calls `read`, so the writer clone stands in.
                db.clone(),
                store_dir.clone(),
                config_provider,
                store_keys,
                test_key_custody(),
                identity_custody,
                coven_storage::oauth::OAuthClients::empty(),
                Arc::new(SystemClock),
                None,
                None,
                StoreOpenGuard::acquire_for_test(&store_dir),
                chunking,
            );

            handle
                .connect_sync_with_test_home_caller_driven(
                    home.clone(),
                    CloudCipher::Encrypted(EncryptionService::from_key([42; 32])),
                )
                .await
                .expect("connect over the injected test home");

            // Several chunks' worth of plaintext, so the configured size frames the
            // object rather than fitting inside one chunk either way.
            let plaintext: Vec<u8> = (0..3 * CHUNK as usize + 17)
                .map(|value| (value % 251) as u8)
                .collect();
            handle
                .queue_host_blob("cover-1", "cover-cover-1.jpg", &plaintext, false)
                .await;
            handle
                .make_remote_with_discovered_order_for_test(
                    "notes",
                    "note-cover-1",
                    "Notes Root",
                    false,
                )
                .await
                .expect("queue the exact row/blob transition");
            let outcome = handle
                .drain_uploads()
                .await
                .expect("drain the prepared exact blob through the public handle");
            assert_eq!(outcome.uploaded(), 1);
            assert!(outcome.failures().failures().is_empty());

            let blob = handle
                .row_blob_ref("note_photos", "cover-1")
                .await
                .expect("capture Remote row after exact upload");
            let object = blob
                .stored()
                .expect("published blob has exact storage")
                .object();
            let at_rest = home
                .read_at(object.slot())
                .await
                .expect("the exact blob object exists");

            // `[key tag][header][chunks]` — the header the sealer wrote is what every
            // later reader frames the object by.
            let header = coven_keys::encryption::SealedBlobHeader::parse(
                &at_rest[coven_keys::encryption::KeyTag::LEN..],
            )
            .expect("stored blob carries a sealed header");
            assert_eq!(
                header.chunk_size().get(),
                CHUNK,
                "the sealed blob is framed at the chunking the handle was built with",
            );
            assert_eq!(header.plaintext_len(), plaintext.len() as u64);

            let read = handle
                .read_blob(&blob)
                .await
                .expect("read through the handle");
            assert_eq!(
                read, plaintext,
                "the blob sealed at the configured chunk size reads back whole",
            );
        })
        .await;
}

#[tokio::test]
async fn connected_sync_reuses_connection_storage_for_loop() {
    test_keyring::install();

    let (_tmp, store_dir) = temp_store_dir();
    let db = host_blob_test_db("images", &store_dir);

    let mut config = Config::with_defaults(
        "lib-cloudkit-home-reuse".to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.provider = Some(CloudProvider::CloudKit);
    config.cloud_home.storage = HomeStorage::Browsable;
    let config_provider: ConfigProvider = {
        let config = config.clone();
        Arc::new(move || config.clone())
    };

    let handle = CovenHandle::new(
        db.clone(),
        // `read_db`: this test never calls `read`, so the writer clone stands in.
        db.clone(),
        store_dir.clone(),
        config_provider,
        StoreKeys::bind("lib-cloudkit-home-reuse".to_string()),
        test_key_custody(),
        test_identity_custody(),
        coven_storage::oauth::OAuthClients::empty(),
        Arc::new(SystemClock),
        Some(Arc::new(TestCloudKitOps::new())),
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        coven_storage::BlobChunking::DEFAULT,
    );

    handle
        .connect_sync()
        .await
        .expect("connect sync over the test CloudKit driver");

    assert!(
        handle.sync.loop_uses_connected_storage_for_test(),
        "StoreSync and its sync loop must retain the same storage instance",
    );
}

#[tokio::test]
async fn cloudkit_setup_commits_the_generated_key_with_the_connection() {
    test_keyring::install();

    let store_id = "lib-atomic-cloudkit-setup";
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let store_keys = test_store_keys(store_id);
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let handle = test_handle_with_custody_and_storage(
        store_id,
        store_dir,
        db,
        custody.clone(),
        HomeStorage::Browsable,
    );
    let cloud_home = coven_foundation::config::CloudHomeConfig {
        storage: HomeStorage::Opaque,
        ..Default::default()
    };
    let mut expected_cloud_home = cloud_home.clone();
    expected_cloud_home.provider = Some(CloudProvider::CloudKit);

    let connected = handle
        .setup_cloudkit_cloud_home(cloud_home.clone(), Arc::new(TestCloudKitOps::new()))
        .await
        .expect("atomic CloudKit setup succeeds");

    assert_eq!(connected.cloud_home, expected_cloud_home);
    assert_eq!(connected.key_state, crate::CloudHomeKeyState::Available);
    assert!(custody.unlock().expect("unlock committed key").is_some());
    assert!(handle.sync.is_connected());
    assert!(handle.sync.is_syncing());
}

#[tokio::test]
async fn importing_a_master_key_during_cloud_setup_cannot_report_a_lost_write() {
    test_keyring::install();

    let store_id = "lib-import-during-cloud-setup";
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let store_keys = test_store_keys(store_id);
    let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let handle = test_handle_with_custody_and_storage(
        store_id,
        store_dir,
        db,
        custody,
        HomeStorage::Browsable,
    );
    let home = Arc::new(InMemoryCloudHome::new());
    let (setup_reached_provider, release_setup) = home.pause_after_exact_create_call(1);
    let cloud_home = coven_foundation::config::CloudHomeConfig {
        provider: Some(CloudProvider::S3),
        storage: HomeStorage::Opaque,
        ..Default::default()
    };
    let setup = tokio::spawn({
        let handle = handle.clone();
        let home = home.clone();
        async move {
            handle
                .setup_cloud_home_with_test_home(
                    cloud_home,
                    home,
                    Some(coven_keys::keys::CloudHomeCredentials::S3 {
                        access_key: "access".to_string(),
                        secret_key: "secret".to_string(),
                    }),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), setup_reached_provider.notified())
        .await
        .expect("cloud setup reaches provider initialization");

    let imported = MasterKeyring::generate();
    let import = tokio::spawn({
        let handle = handle.clone();
        async move { handle.import_master_key(&imported.to_serialized()).await }
    });
    release_setup.notify_one();
    setup
        .await
        .expect("join cloud setup task")
        .expect("cloud setup succeeds");

    let import = import.await.expect("join master-key import task");
    assert!(matches!(import, Err(MasterKeyError::CloudHomeConnected)));
}

/// A read-only handle holds no sync loop, so every cloud-miss read builds
/// storage fresh from config via the `cipher: None` path. The writer publishes
/// a host-provided row and exact encrypted blob through the normal Store path;
/// publication releases its local staging bytes, forcing the reader to use the
/// row's exact cloud locator and resolve the same cipher through custody.
#[tokio::test]
async fn read_only_handle_resolves_an_encrypted_cipher_through_custody() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            test_keyring::install();

            let store_id = "ro-encrypted-custody-test";
            let (_tmp, store_dir) = temp_store_dir();
            let db = host_blob_test_db("images", &store_dir);

            let mut config = Config::with_defaults(
                store_id.to_string(),
                "test-device".to_string(),
                "Test Store".to_string(),
            );
            config.cloud_home.provider = Some(CloudProvider::CloudKit);
            config.cloud_home.storage = HomeStorage::Opaque;

            let key_service = test_store_keys(store_id);
            let custody =
                coven_keys::custody::KeyCustody::Keyring.resolve(&key_service, &store_dir);
            custody
                .persist(&coven_keys::encryption::MasterKeyring::generate())
                .expect("establish a master key");

            // Exact opaque blob locators bind their uploader registration, so establish
            // the writer's signing identity before connecting storage.
            let identity_custody = coven_keys::identity_custody::IdentityCustody::Keyring
                .resolve(&key_service, &store_dir);
            identity_custody
                .persist(&coven_keys::keys::UserKeypair::generate())
                .expect("establish this store's signing identity");

            let ops = Arc::new(TestCloudKitOps::new());
            let config_provider: ConfigProvider = {
                let config = config.clone();
                Arc::new(move || config.clone())
            };
            let writer = CovenHandle::new(
                db.clone(),
                db.clone(),
                store_dir.clone(),
                config_provider,
                key_service.clone(),
                custody.clone(),
                identity_custody.clone(),
                coven_storage::oauth::OAuthClients::empty(),
                Arc::new(SystemClock),
                Some(ops.clone()),
                None,
                StoreOpenGuard::acquire_for_test(&store_dir),
                coven_storage::BlobChunking::DEFAULT,
            );
            writer
                .connect_sync_with_cloudkit(ops.clone())
                .await
                .expect("connect encrypted CloudKit writer");
            let plaintext = b"encrypted-cloud-blob-for-the-read-only-handle".to_vec();
            let blob = writer
                .publish_host_blob("cover-1", "cover-cover-1.jpg", &plaintext)
                .await;

            let config_provider: ConfigProvider = {
                let config = config.clone();
                Arc::new(move || config.clone())
            };
            let reader = crate::read_handle::CovenReadHandle::new(
                db.clone(),
                store_dir,
                config_provider,
                key_service,
                custody,
                identity_custody,
                coven_storage::oauth::OAuthClients::empty(),
                Arc::new(SystemClock),
                Some(ops),
                coven_storage::BlobChunking::DEFAULT,
            );

            let read = reader
                .read_blob(&blob)
                .await
                .expect("the read-only handle resolves the same cipher through custody");
            assert_eq!(
                read, plaintext,
                "the blob decrypts back to its original plaintext",
            );
        })
        .await;
}

#[tokio::test]
async fn sync_not_configured_is_typed() {
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let handle = test_handle("lib-no-sync", store_dir, db);

    let result = handle.get_members().await;

    assert!(matches!(result, Err(SyncError::NotConfigured)));
}

/// Atomic cloud-home setup generates and commits the key that actually seals
/// cloud traffic. No cipher is injected: setup prepares the opaque connection,
/// commits custody only once that connection is ready, and starts its loop.
#[tokio::test]
async fn cloud_home_setup_seals_cloud_traffic_with_its_committed_key() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            test_keyring::install();

            let (_tmp, store_dir) = temp_store_dir();
            let db = host_blob_test_db("images", &store_dir);
            let store_id = "lib-init-master-key-seals-traffic";

            // Opaque storage: the master key established below seals every object at
            // rest. A configured provider is unnecessary — the injected test home is
            // the enablement.
            let mut config = Config::with_defaults(
                store_id.to_string(),
                "test-device".to_string(),
                "Test Store".to_string(),
            );
            config.cloud_home.storage = HomeStorage::Opaque;
            let config_provider: ConfigProvider = {
                let config = config.clone();
                Arc::new(move || config.clone())
            };

            let store_keys = test_store_keys(store_id);
            let custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
            let identity_custody = coven_keys::identity_custody::IdentityCustody::Keyring
                .resolve(&store_keys, &store_dir);
            let handle = CovenHandle::new(
                db.clone(),
                db.clone(),
                store_dir.clone(),
                config_provider,
                store_keys,
                custody,
                identity_custody,
                coven_storage::oauth::OAuthClients::empty(),
                Arc::new(SystemClock),
                None,
                None,
                StoreOpenGuard::acquire_for_test(&store_dir),
                coven_storage::BlobChunking::DEFAULT,
            );

            handle
                .initialize_identity()
                .expect("establish this store's identity before connecting");

            let home = Arc::new(InMemoryCloudHome::new());
            let cloud_home = coven_foundation::config::CloudHomeConfig {
                provider: Some(CloudProvider::S3),
                storage: HomeStorage::Opaque,
                ..Default::default()
            };
            handle
                .setup_cloud_home_with_test_home(
                    cloud_home,
                    home.clone(),
                    Some(coven_keys::keys::CloudHomeCredentials::S3 {
                        access_key: "access".to_string(),
                        secret_key: "secret".to_string(),
                    }),
                )
                .await
                .expect("prepare, commit, and connect the opaque cloud home");

            // Publish a host-provided row and exact blob under the opaque home. The
            // resulting row reference carries its uploader authority and stored slot.
            let plaintext = b"cover-art-sealed-under-the-established-master-key".to_vec();
            let blob = handle
                .publish_host_blob("cover-1", "cover-cover-1.jpg", &plaintext)
                .await;
            let cloud_key = blob
                .stored()
                .expect("published blob has exact storage")
                .object()
                .slot()
                .logical_key();

            // At rest the object is ciphertext: the stored bytes are not the
            // plaintext, and no object in the home holds the plaintext verbatim.
            let at_rest = home.get(cloud_key).expect("the blob landed in the home");
            assert_ne!(
                at_rest, plaintext,
                "the master key sealed the upload — the bytes at rest are not the plaintext",
            );
            assert!(
                home.keys()
                    .iter()
                    .all(|k| home.get(k).as_deref() != Some(plaintext.as_slice())),
                "no object in the home holds the plaintext",
            );

            // Read back through the row's activated exact locator and the same
            // custody-resolved cipher.
            let read = handle
                .read_blob(&blob)
                .await
                .expect("read through the handle");
            assert_eq!(
                read, plaintext,
                "read_blob decrypts the sealed blob back to its original plaintext",
            );
        })
        .await;
}

#[tokio::test]
async fn import_master_key_rejects_raw_hex() {
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let handle = test_handle("lib-import-master-key", store_dir, db);

    let raw_hex = hex::encode([0x22u8; 32]);
    assert!(handle.import_master_key(&raw_hex).await.is_err());
}

#[tokio::test]
async fn import_master_key_accepts_the_current_serialized_keyring() {
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let handle = test_handle("lib-import-master-key", store_dir, db);

    let keyring = coven_keys::encryption::MasterKeyring::generate();
    handle
        .import_master_key(&keyring.to_serialized())
        .await
        .expect("import the serialized keyring");
    assert_eq!(
        handle
            .cloud_home_key_state(HomeStorage::Opaque)
            .expect("read key availability"),
        crate::CloudHomeKeyState::Available,
    );
}

// =========================================================================
// Identity lifecycle
// =========================================================================

/// A handle over a real (keyring-backed) identity custody, for tests that
/// need to prove something about a store's *own* keyring account rather
/// than the shared in-memory `test_identity_custody`.
fn test_handle_with_real_identity(
    store_id: &str,
    store_dir: StoreDir,
    db: coven_database::Database,
) -> CovenHandle {
    let db = db;
    let config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    let config_provider: ConfigProvider = Arc::new(move || config.clone());
    let store_keys = test_store_keys(store_id);
    let identity_custody =
        coven_keys::identity_custody::IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
    CovenHandle::new(
        db.clone(),
        db.clone(),
        store_dir.clone(),
        config_provider,
        store_keys,
        test_key_custody(),
        identity_custody,
        coven_storage::oauth::OAuthClients::empty(),
        Arc::new(SystemClock),
        None,
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        coven_storage::BlobChunking::DEFAULT,
    )
}

/// `initialize_identity` is the only place coven ever generates a
/// store's signing identity, and it refuses to run again once one is
/// established — coven never generates over an existing identity. The
/// identity lifecycle refusal through the public handle.
#[tokio::test]
async fn initialize_identity_refuses_a_second_call() {
    test_keyring::install();
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let handle = test_handle_with_real_identity("lib-init-identity-twice", store_dir, db);

    let pubkey = handle
        .initialize_identity()
        .expect("the first call establishes an identity");
    assert!(!pubkey.is_empty());

    let error = handle
        .initialize_identity()
        .expect_err("a second call must refuse rather than generate over an existing identity");
    assert!(matches!(
        error,
        coven_keys::keys::IdentityError::AlreadyEstablished
    ));
}

/// Creating two stores on one device establishes two different
/// identities — each store's `initialize_identity` generates its own
/// keypair, under its own keyring account, independent of the other.
#[tokio::test]
async fn creating_two_stores_yields_two_different_identities() {
    test_keyring::install();
    let (_tmp_a, store_dir_a) = temp_store_dir();
    let (_tmp_b, store_dir_b) = temp_store_dir();
    let db_a_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let db_a = read_test_db(db_a_store_dir.clone(), "images");
    let handle_a = test_handle_with_real_identity("lib-two-stores-identity-a", store_dir_a, db_a);
    let db_b_store_dir = coven_replication::sync::test_helpers::test_store_dir();
    let db_b = read_test_db(db_b_store_dir.clone(), "images");
    let handle_b = test_handle_with_real_identity("lib-two-stores-identity-b", store_dir_b, db_b);

    let pubkey_a = handle_a
        .initialize_identity()
        .expect("establish store a's identity");
    let pubkey_b = handle_b
        .initialize_identity()
        .expect("establish store b's identity");

    assert_ne!(
        pubkey_a, pubkey_b,
        "two stores on one device must not share an identity",
    );
}

// =========================================================================
// Host secrets
// =========================================================================

/// The host-facing round trip: `set_host_secret` / `host_secret` /
/// `delete_host_secret` through the handle, with an absent secret
/// reading `None` both before it's ever set and after it's deleted.
#[tokio::test]
async fn host_secret_round_trips_through_the_handle() {
    test_keyring::install();
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let handle = test_handle("lib-host-secret-round-trip", store_dir, db);

    assert_eq!(
        handle.host_secret("discogs_api_key").expect("get"),
        None,
        "an unset host secret reads as absent",
    );

    handle
        .set_host_secret("discogs_api_key", "the-discogs-key")
        .expect("set");
    assert_eq!(
        handle.host_secret("discogs_api_key").expect("get"),
        Some("the-discogs-key".to_string()),
    );

    handle
        .delete_host_secret("discogs_api_key")
        .expect("delete");
    assert_eq!(
        handle
            .host_secret("discogs_api_key")
            .expect("get after delete"),
        None,
    );
}

// =========================================================================
// App-data sealing
// =========================================================================

/// The host-facing round trip over a keyring-custody store: what the handle
/// seals under the store's established master key, the same handle opens —
/// and a payload presented with a different `aad` than it was bound to does
/// not open, so a value lifted into another row stays shut.
#[tokio::test]
async fn seal_and_open_app_data_round_trip_through_the_handle() {
    test_keyring::install();
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let store_id = "lib-app-data-round-trip";
    let handle = test_handle_with_custody(
        store_id,
        store_dir.clone(),
        db,
        coven_keys::custody::KeyCustody::Keyring,
    );
    let keyring = coven_keys::encryption::MasterKeyring::generate();
    handle
        .import_master_key(&keyring.to_serialized())
        .await
        .expect("establish the store's master key");

    let sealed = handle
        .seal_app_data(b"entry-secret", b"row-42")
        .expect("seal under the established key");
    assert_ne!(
        sealed, b"entry-secret",
        "the sealed payload is not the plaintext",
    );

    assert_eq!(
        handle.open_app_data(&sealed, b"row-42").unwrap(),
        b"entry-secret",
        "the handle opens what it sealed",
    );

    let error = handle
        .open_app_data(&sealed, b"row-99")
        .expect_err("a different aad must not open the payload");
    assert!(matches!(error, SealError::Crypto(_)), "{error:?}");
}

/// A read-only handle over the same store opens what the writer sealed: it
/// resolves the same master keyring through its own custody (the same
/// `store_id` keyring account), so a secondary reader — a File Provider
/// extension, a second process — reads the host's sealed rows.
#[tokio::test]
async fn open_app_data_round_trips_through_the_read_handle() {
    test_keyring::install();
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let store_id = "lib-app-data-read-handle";

    let writer = test_handle_with_custody(
        store_id,
        store_dir.clone(),
        db.clone(),
        coven_keys::custody::KeyCustody::Keyring,
    );
    let keyring = coven_keys::encryption::MasterKeyring::generate();
    writer
        .import_master_key(&keyring.to_serialized())
        .await
        .expect("establish the store's master key");
    let sealed = writer
        .seal_app_data(b"read-me-back", b"ctx")
        .expect("seal through the write handle");

    let config_provider: ConfigProvider = {
        let config = Config::with_defaults(
            store_id.to_string(),
            "test-device".to_string(),
            "Test Store".to_string(),
        );
        Arc::new(move || config.clone())
    };
    let store_keys = test_store_keys(store_id);
    let key_custody = coven_keys::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
    let reader = crate::read_handle::CovenReadHandle::new(
        db.clone(),
        store_dir.clone(),
        config_provider,
        store_keys,
        key_custody,
        test_identity_custody(),
        coven_storage::oauth::OAuthClients::empty(),
        Arc::new(SystemClock),
        None,
        coven_storage::BlobChunking::DEFAULT,
    );

    assert_eq!(
        reader.open_app_data(&sealed, b"ctx").unwrap(),
        b"read-me-back",
        "the read handle opens what the write handle sealed",
    );
}

/// A store whose custody holds no master key has nothing to seal under and
/// nothing to open with. Both directions refuse with `Locked` rather than
/// inventing a key — the app-data counterpart of the sync engine's
/// `MasterKeyNotEstablished` gate. Here the store is genuinely never
/// initialized: a real keyring custody whose account holds no key.
#[tokio::test]
async fn app_data_is_locked_when_no_master_key_is_established() {
    test_keyring::install();
    let (_tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let store_id = "lib-app-data-locked";
    let handle = test_handle_with_custody(
        store_id,
        store_dir.clone(),
        db,
        coven_keys::custody::KeyCustody::Keyring,
    );
    assert_eq!(
        handle
            .cloud_home_key_state(HomeStorage::Opaque)
            .expect("read key availability"),
        crate::CloudHomeKeyState::Locked,
        "the store starts with locked master-key custody",
    );

    let seal_error = handle
        .seal_app_data(b"nothing to seal under", b"ctx")
        .expect_err("sealing a locked store must refuse");
    assert!(matches!(seal_error, SealError::Locked), "{seal_error:?}");

    let open_error = handle
        .open_app_data(b"nothing to open with", b"ctx")
        .expect_err("opening on a locked store must refuse");
    assert!(matches!(open_error, SealError::Locked), "{open_error:?}");
}

#[tokio::test]
async fn plaintext_membership_operations_are_typed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                await_test_orchestration(tokio::spawn(async {
                    test_keyring::install();

                    let (_tmp, store_dir) = temp_store_dir();
                    let db = read_test_db(store_dir.clone(), "images");
                    let handle = test_handle("lib-plaintext-membership", store_dir, db);
                    handle
                        .connect_sync_with_test_home(
                            Arc::new(InMemoryCloudHome::new()),
                            CloudCipher::Plaintext,
                        )
                        .await
                        .expect("connect plaintext home");

                    let joining_identity = coven_keys::keys::UserKeypair::generate();
                    let public_key_hex = hex::encode(joining_identity.public_key());
                    let admission = handle
                        .admit_member_for_test(&public_key_hex, MemberRole::Member)
                        .await;
                    let remove = handle.remove_member(&public_key_hex).await;
                    let circle = handle.circles().create("Household").await;

                    assert!(matches!(admission, Err(SyncError::NotEncryptedHome)));
                    assert!(matches!(remove, Err(SyncError::NotEncryptedHome)));
                    assert!(
                        matches!(&circle, Err(crate::CircleError::BrowsableStorage)),
                        "{circle:?}"
                    );
                }))
                .await;
            })
            .await
            .expect("plaintext membership test task");
        })
        .await;
}

async fn await_test_orchestration(task: tokio::task::JoinHandle<()>) {
    task.await.expect("test orchestration task completes");
}

#[tokio::test]
async fn create_circle_returns_after_merge_activation_is_materialized() {
    await_test_orchestration(tokio::spawn(async {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db(store_dir.clone(), "images");
        let keyring = coven_keys::encryption::MasterKeyring::generate();
        let handle = test_handle_with_custody(
            "lib-create-circle-merge",
            store_dir,
            db.clone(),
            coven_keys::custody::KeyCustody::InMemory(keyring.clone()),
        );
        handle
            .connect_sync_with_test_home(
                Arc::new(InMemoryCloudHome::new()),
                CloudCipher::Encrypted(EncryptionService::from(keyring)),
            )
            .await
            .expect("connect encrypted Merge home");

        let circle_id = handle
            .circles()
            .create("Household")
            .await
            .expect("create and activate circle");

        handle
            .circles()
            .rename(circle_id, "Household money")
            .await
            .expect("rename and activate circle");

        assert_eq!(
            handle.circles().list().await.expect("read active circles"),
            vec![crate::Circle {
                id: circle_id,
                name: Some("Household money".to_string()),
                role: Some(crate::CircleRole::Owner),
                state: crate::CircleState::Active,
            }]
        );
        assert_eq!(
            handle
                .circles()
                .members(circle_id)
                .await
                .expect("read active Circle members"),
            vec![crate::CircleMemberInfo {
                pubkey: handle
                    .security
                    .required_identity_public_key_hex()
                    .expect("read test identity"),
                role: crate::CircleRole::Owner,
                is_self: true,
            }]
        );
        let identity = handle
            .security
            .required_identity_public_key_hex()
            .expect("read test identity");
        assert!(StoreDatabase::from_database(db.clone())
            .get_circle_members(circle_id, &identity, std::collections::BTreeSet::new(),)
            .await
            .expect("intersect Circle roster with an empty Store membership")
            .is_empty());
        assert!(handle
            .circles()
            .operations()
            .await
            .expect("read completed circle operations")
            .is_empty());

        let counts = db
            .circle_state_counts_for_test(circle_id)
            .await
            .expect("read activated circle state");
        assert_eq!(counts, (2, 2, 0));
    }))
    .await;
}

/// The `circles()` namespace round-trips through the running loop across
/// derived states: create and rename land `Active`, read back through `list`;
/// deletion lands `Deleted`. Each write dispatches through the loop-thread
/// command channel and each state is read back through the public list surface.
#[tokio::test]
async fn circles_namespace_round_trips_across_states() {
    await_test_orchestration(tokio::spawn(async {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db(store_dir.clone(), "images");
        let keyring = coven_keys::encryption::MasterKeyring::generate();
        let handle = test_handle_with_custody(
            "lib-circles-namespace",
            store_dir,
            db.clone(),
            coven_keys::custody::KeyCustody::InMemory(keyring.clone()),
        );
        handle
            .connect_sync_with_test_home(
                Arc::new(InMemoryCloudHome::new()),
                CloudCipher::Encrypted(EncryptionService::from(keyring)),
            )
            .await
            .expect("connect encrypted home");

        let circles = handle.circles();
        let circle_id = circles.create("Family").await.expect("create the Circle");
        circles
            .rename(circle_id, "Household")
            .await
            .expect("rename the Circle");

        let state_of = |list: Vec<crate::Circle>| {
            list.into_iter()
                .find(|circle| circle.id == circle_id)
                .expect("the Circle is listed")
                .state
        };
        assert_eq!(
            state_of(circles.list().await.expect("list after rename")),
            crate::CircleState::Active,
        );

        circles.delete(circle_id).await.expect("delete the Circle");
        assert_eq!(
            state_of(circles.list().await.expect("list after delete")),
            crate::CircleState::Deleted,
        );
    }))
    .await;
}

/// Every Circle write command reaches the loop thread through its own
/// `SyncCommand` dispatch arm and returns a reply. Each is fired at a state
/// that refuses it: the three close/resolve commands come back with distinct
/// typed errors naming the forwarded circle id (which also proves each arm
/// forwards to the *right* components method — a swapped arm would return a
/// different typed error); retry, remove, and add come back carrying the
/// forwarded operation or circle id in their message. A wrong-field or
/// wrong-method bug in any arm would surface here.
#[tokio::test]
async fn circle_write_commands_dispatch_through_their_command_arms() {
    await_test_orchestration(tokio::spawn(async {
        test_keyring::install();

        let (_tmp, store_dir) = temp_store_dir();
        let db = read_test_db(store_dir.clone(), "images");
        let keyring = coven_keys::encryption::MasterKeyring::generate();
        let handle = test_handle_with_custody(
            "lib-circles-dispatch",
            store_dir,
            db,
            coven_keys::custody::KeyCustody::InMemory(keyring.clone()),
        );
        handle
            .connect_sync_with_test_home(
                Arc::new(InMemoryCloudHome::new()),
                CloudCipher::Encrypted(EncryptionService::from(keyring)),
            )
            .await
            .expect("connect encrypted home");

        let circles = handle.circles();
        let circle_id = circles.create("Family").await.expect("create the Circle");
        let member = hex::encode(coven_keys::keys::UserKeypair::generate().public_key());

        // Distinct typed refusals: a swapped arm would return a different one.
        assert!(
            matches!(
                circles.cancel_close(circle_id).await,
                Err(crate::CircleError::NoCloseToCancel { circle_id: refused })
                    if refused == circle_id
            ),
            "cancel_close dispatches to its arm and returns NoCloseToCancel"
        );
        let device = "aa"
            .repeat(32)
            .parse::<crate::StoreDeviceId>()
            .expect("device id");
        assert!(
            matches!(
                circles.exclude_close_device(circle_id, device).await,
                Err(crate::CircleError::NoCloseToExclude { circle_id: refused })
                    if refused == circle_id
            ),
            "exclude_close_device dispatches to its arm and returns NoCloseToExclude"
        );
        assert!(
            matches!(
                circles
                    .resolve(circle_id, crate::CircleControlCoord::placeholder(1))
                    .await,
                Err(crate::CircleError::NotConflicted { circle_id: refused })
                    if refused == circle_id
            ),
            "resolve dispatches to its arm and returns NotConflicted"
        );

        // The remaining three retain errors whose display carries the forwarded id.
        let retry = circles
            .retry_operation(crate::CircleOperationId::placeholder("dispatch-op-seed"))
            .await;
        assert!(
            retry
                .as_ref()
                .is_err_and(|error| error.to_string().contains("dispatch-op-seed")),
            "retry_operation forwards the operation id: {retry:?}"
        );

        let discard = circles
            .discard_operation(crate::CircleOperationId::placeholder("dispatch-op-seed"))
            .await;
        assert!(
            discard
                .as_ref()
                .is_err_and(|error| error.to_string().contains("dispatch-op-seed")),
            "discard_operation forwards the operation id: {discard:?}"
        );

        let absent_circle = crate::CircleId::from_bytes([9u8; 16]);
        let remove = circles.remove_member(absent_circle, &member).await;
        assert!(
            remove
                .as_ref()
                .is_err_and(|error| error.to_string().contains(&absent_circle.to_string())),
            "remove_member forwards the circle id: {remove:?}"
        );

        let add = circles.add_member(circle_id, &member).await;
        assert!(
            add.is_err(),
            "add_member dispatches to its arm and returns a reply: {add:?}"
        );
    }))
    .await;
}

#[tokio::test]
async fn reconnect_sync_stops_the_previous_loop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, store_dir) = temp_store_dir();
                let db = read_test_db(store_dir.clone(), "images");
                let mut config = Config::with_defaults(
                    "lib-reconnect-loop".to_string(),
                    "test-device".to_string(),
                    "Test Store".to_string(),
                );
                config.cloud_home.storage = HomeStorage::Browsable;
                let config_provider: ConfigProvider = {
                    let config = config.clone();
                    Arc::new(move || config.clone())
                };
                // These tests never call `read`, and the in-memory test database has
                // no shareable read-only companion, so the writer clone stands in.
                let handle = CovenHandle::new(
                    db.clone(),
                    db.clone(),
                    store_dir.clone(),
                    config_provider,
                    StoreKeys::bind("lib-reconnect-loop".to_string()),
                    test_key_custody(),
                    test_identity_custody(),
                    coven_storage::oauth::OAuthClients::empty(),
                    Arc::new(SystemClock),
                    None,
                    None,
                    StoreOpenGuard::acquire_for_test(&store_dir),
                    coven_storage::BlobChunking::DEFAULT,
                );

                let home = Arc::new(InMemoryCloudHome::new());
                handle
                    .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
                    .await
                    .expect("first connect over injected home");
                let stopped_before = handle.sync.stopped_loop_count_for_test();
                assert!(handle.is_syncing());

                handle
                    .connect_sync_with_test_home(home, CloudCipher::Plaintext)
                    .await
                    .expect("second connect over injected home");
                assert_eq!(
                    handle.sync.stopped_loop_count_for_test(),
                    stopped_before + 1,
                );
                assert!(handle.is_syncing());
            })
            .await
            .expect("sync reconnect test task");
        })
        .await;
}

#[tokio::test]
async fn stopped_installed_loop_blocks_blob_transitions() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, store_dir) = temp_store_dir();
                let db = read_test_db(store_dir.clone(), "images");
                let mut config = Config::with_defaults(
                    "lib-stopped-loop-readiness".to_string(),
                    "test-device".to_string(),
                    "Test Store".to_string(),
                );
                config.cloud_home.storage = HomeStorage::Browsable;
                let config_provider: ConfigProvider = {
                    let config = config.clone();
                    Arc::new(move || config.clone())
                };
                // These tests never call `read`, and the in-memory test database has
                // no shareable read-only companion, so the writer clone stands in.
                let handle = CovenHandle::new(
                    db.clone(),
                    db.clone(),
                    store_dir.clone(),
                    config_provider,
                    StoreKeys::bind("lib-stopped-loop-readiness".to_string()),
                    test_key_custody(),
                    test_identity_custody(),
                    coven_storage::oauth::OAuthClients::empty(),
                    Arc::new(SystemClock),
                    None,
                    None,
                    StoreOpenGuard::acquire_for_test(&store_dir),
                    coven_storage::BlobChunking::DEFAULT,
                );

                handle
                    .connect_sync_with_test_home(
                        Arc::new(InMemoryCloudHome::new()),
                        CloudCipher::Plaintext,
                    )
                    .await
                    .expect("connect over injected home");
                handle
                    .sync
                    .stop_loop_for_test()
                    .expect("stop installed loop");

                let make_remote = handle
                    .make_remote_with_discovered_order_for_test(
                        "notes",
                        "note-1",
                        "Notes Root",
                        false,
                    )
                    .await;
                assert!(matches!(make_remote, Err(MakeRemoteError::SyncNotReady)));

                let (_cancel_tx, cancel_rx) = watch::channel(false);
                let make_local = handle
                    .make_local("notes", "note-1", &HashMap::new(), &cancel_rx)
                    .await;
                assert!(matches!(make_local, Err(MakeLocalError::SyncNotReady)));
            })
            .await
            .expect("stopped-loop readiness test task");
        })
        .await;
}

#[tokio::test]
async fn encrypted_session_keeps_its_binding_after_config_changes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, store_dir) = temp_store_dir();
                let db = host_blob_test_db("images", &store_dir);

                let config = Config::with_defaults(
                    "lib-test".to_string(),
                    "test-device".to_string(),
                    "Test Store".to_string(),
                );
                let live_config = Arc::new(RwLock::new(config));
                let config_provider: ConfigProvider = {
                    let live_config = live_config.clone();
                    Arc::new(move || {
                        live_config
                            .read()
                            .expect("test config lock is not poisoned")
                            .clone()
                    })
                };

                let handle = CovenHandle::new(
                    db.clone(),
                    // `read_db`: this test never calls `read`, so the writer clone stands in.
                    db.clone(),
                    store_dir.clone(),
                    config_provider,
                    StoreKeys::bind("lib-test".to_string()),
                    test_key_custody(),
                    test_identity_custody(),
                    coven_storage::oauth::OAuthClients::empty(),
                    Arc::new(SystemClock),
                    None,
                    None,
                    StoreOpenGuard::acquire_for_test(&store_dir),
                    coven_storage::BlobChunking::DEFAULT,
                );

                let home = Arc::new(InMemoryCloudHome::new());
                handle
                    .connect_sync_with_test_home(
                        home.clone(),
                        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
                    )
                    .await
                    .expect("connect encrypted injected home");
                {
                    let mut next_config = live_config
                        .write()
                        .expect("test config lock is not poisoned");
                    next_config.store_id = "next-lib".to_string();
                    next_config.cloud_home.storage = HomeStorage::Browsable;
                }

                assert_eq!(
                    handle.sync.connected_store_id_for_test().as_deref(),
                    Some("lib-test")
                );
                assert!(handle.sync.connected_uses_store_dir_for_test(&store_dir));
                assert!(matches!(
                    handle.sync.connected_blob_path_scheme_for_test(),
                    Some(BlobPathScheme::Hashed)
                ));

                let rotated = EncryptionService::from_key([7u8; 32])
                    .with_appended_generation(2, [8u8; 32])
                    .expect("append generation");
                handle
                    .sync
                    .adopt_key_rotation_for_test(rotated)
                    .expect("adopt encrypted generation");
                assert_eq!(handle.sync.encryption_generation_for_test(), Some(2));

                let plaintext = b"encrypted-drain-bytes-after-key-rotation".to_vec();
                let blob = handle
                    .publish_host_blob("plain-cover", "plain-cover", &plaintext)
                    .await;
                let cloud_key = blob
                    .stored()
                    .expect("published blob has exact storage")
                    .object()
                    .slot()
                    .logical_key();
                let stored = home.get(cloud_key).expect("uploaded cloud object");
                assert_ne!(
                    stored.as_slice(),
                    plaintext.as_slice(),
                    "an encrypted session must never upload plaintext cloud bytes",
                );

                let aad_context = |store_id: &str| {
                    let mut context = Vec::new();
                    context.extend_from_slice(&(store_id.len() as u64).to_le_bytes());
                    context.extend_from_slice(store_id.as_bytes());
                    context.extend_from_slice(&(cloud_key.len() as u64).to_le_bytes());
                    context.extend_from_slice(cloud_key.as_bytes());
                    context
                };
                let expected_fingerprint = EncryptionService::from_key([7u8; 32])
                    .with_appended_generation(2, [8u8; 32])
                    .expect("append expected generation")
                    .seal_key_fingerprint();
                let (fingerprint, opened) = handle
                    .sync
                    .open_sealed_blob_for_test(&stored, &aad_context("lib-test"))
                    .expect("open with the installed session binding");
                assert_eq!(fingerprint, expected_fingerprint);
                assert_eq!(opened, plaintext);
                assert!(
                    handle
                        .sync
                        .open_sealed_blob_for_test(&stored, &aad_context("next-lib"))
                        .is_err(),
                    "a later config must not change the installed session's store binding",
                );
            })
            .await
            .expect("encrypted-session binding test task");
        })
        .await;
}

fn status_test_handle(store_id: &str) -> (tempfile::TempDir, CovenHandle) {
    let (tmp, store_dir) = temp_store_dir();
    let db = read_test_db(store_dir.clone(), "images");
    let mut config = Config::with_defaults(
        store_id.to_string(),
        "test-device".to_string(),
        "Test Store".to_string(),
    );
    config.cloud_home.storage = HomeStorage::Browsable;
    let config_provider: ConfigProvider = {
        let config = config.clone();
        Arc::new(move || config.clone())
    };
    let handle = CovenHandle::new(
        db.clone(),
        // `read_db`: these tests never call `read`, and the test db is
        // `:memory:` (unique per connection, no shareable read-only companion),
        // so the writer clone stands in.
        db.clone(),
        store_dir.clone(),
        config_provider,
        StoreKeys::bind(store_id.to_string()),
        test_key_custody(),
        test_identity_custody(),
        coven_storage::oauth::OAuthClients::empty(),
        Arc::new(SystemClock),
        None,
        None,
        StoreOpenGuard::acquire_for_test(&store_dir),
        coven_storage::BlobChunking::DEFAULT,
    );
    (tmp, handle)
}

#[tokio::test]
async fn sync_now_interrupts_the_startup_delay() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, handle) = status_test_handle("lib-sync-now-startup");
                let home = Arc::new(InMemoryCloudHome::new());
                handle
                    .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
                    .await
                    .expect("connect over injected home");

                let (probe_reached, release_probe) = home.pause_next_probe();
                handle.sync_now();
                let reached =
                    tokio::time::timeout(Duration::from_secs(1), probe_reached.notified()).await;
                release_probe.notify_one();
                assert!(
                    reached.is_ok(),
                    "an explicit sync request must start a cycle without waiting for startup"
                );
            })
            .await
            .expect("startup trigger test task");
        })
        .await;
}

/// The current state starts offline, moves through storage checking and
/// publication, then reports synchronization.
#[tokio::test]
async fn subscribed_host_sees_offline_checking_publishing_then_synchronized() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, handle) = status_test_handle("lib-status-syncing");
                let mut rx = handle.subscribe_sync_status();
                assert_eq!(format!("{:?}", *rx.borrow()), "Offline");

                let home = InMemoryCloudHome::new();
                handle
                    .connect_sync_with_test_home(Arc::new(home.clone()), CloudCipher::Plaintext)
                    .await
                    .expect("connect over injected home");
                let (probe_reached, release_probe) = home.pause_next_probe();

                tokio::time::timeout(Duration::from_secs(20), probe_reached.notified())
                    .await
                    .expect("the reachability probe reaches its test pause");
                assert_eq!(format!("{:?}", *rx.borrow()), "CheckingStorage");

                let (publication_reached, release_publication) =
                    home.pause_after_exact_create_call(1);
                release_probe.notify_one();
                tokio::time::timeout(Duration::from_secs(20), publication_reached.notified())
                    .await
                    .expect("publication reaches its test pause");
                let publishing = rx.borrow().clone();
                assert_eq!(format!("{publishing:?}"), "Publishing");

                release_publication.notify_one();
                tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        if matches!(&*rx.borrow(), SyncLoopStatus::Synchronized(_)) {
                            break;
                        }
                        rx.changed().await.expect("the status channel remains open");
                    }
                })
                .await
                .expect("a synchronized status arrives within the timeout");
                let done = rx.borrow().clone();
                assert!(
                    format!("{done:?}").starts_with("Synchronized("),
                    "a successful cycle ends synchronized, got {done:?}",
                );
            })
            .await
            .expect("sync status sequence test task");
        })
        .await;
}

#[tokio::test]
async fn transport_failure_after_reachability_probe_returns_to_offline() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, handle) = status_test_handle("lib-status-cycle-transport");
                let mut rx = handle.subscribe_sync_status();
                let home = InMemoryCloudHome::new();
                handle
                    .connect_sync_with_test_home(Arc::new(home.clone()), CloudCipher::Plaintext)
                    .await
                    .expect("connect over injected home");
                let (probe_reached, release_probe) = home.pause_next_probe();

                tokio::time::timeout(Duration::from_secs(20), probe_reached.notified())
                    .await
                    .expect("the reachability probe reaches the provider");
                assert_eq!(format!("{:?}", *rx.borrow()), "CheckingStorage");
                home.arm_write_failures();
                release_probe.notify_one();

                tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        rx.changed().await.expect("the status channel remains open");
                        match rx.borrow().clone() {
                            SyncLoopStatus::CheckingStorage | SyncLoopStatus::Publishing => {}
                            SyncLoopStatus::Offline => break,
                            status => {
                                panic!(
                                    "a provider transport failure must end offline, got {status:?}"
                                )
                            }
                        }
                    }
                })
                .await
                .expect("the failed cycle publishes a terminal status");
            })
            .await
            .expect("transport failure status test task");
        })
        .await;
}

/// A subscription created before any provider is connected keeps receiving
/// across a reconnect — the channel is owned by the handle, not the loop that
/// a reconnect replaces. Under a per-loop channel the receiver would observe
/// `Closed` after the reconnect dropped the first loop's sender.
#[tokio::test]
async fn subscription_survives_a_reconnect() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, handle) = status_test_handle("lib-status-reconnect");

                // Subscribe before any provider is connected — valid because the channel
                // is handle-owned.
                let mut rx = handle.subscribe_sync_status();
                let home = Arc::new(InMemoryCloudHome::new());

                handle
                    .connect_sync_with_test_home(home.clone(), CloudCipher::Plaintext)
                    .await
                    .expect("first connect");
                // Reconnect immediately: this drops the first loop and starts a second one
                // over the same store home before the first loop's startup delay elapses.
                handle
                    .connect_sync_with_test_home(home, CloudCipher::Plaintext)
                    .await
                    .expect("reconnect");

                tokio::time::timeout(Duration::from_secs(20), rx.changed())
                    .await
                    .expect("a status arrives from the post-reconnect loop")
                    .expect("a reconnect does not close the handle-owned status channel");
                let status = rx.borrow().clone();
                assert!(
                    matches!(
                        status,
                        SyncLoopStatus::CheckingStorage | SyncLoopStatus::Publishing
                    ),
                    "the received status is a cycle start marker, got {status:?}",
                );
            })
            .await
            .expect("status subscription reconnect test task");
        })
        .await;
}

/// `stop_sync` keeps the provider connection so `start_sync` can resume it;
/// `disconnect_sync` drops it outright. The resolved cipher and device
/// keypair live only inside the active loop, so nothing resolved by the
/// connection survives disconnection. A later `connect_sync` resolves fresh
/// material from custody.
#[tokio::test]
async fn disconnect_sync_drops_the_connection_not_just_the_loop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                test_keyring::install();

                let (_tmp, store_dir) = temp_store_dir();
                let db = read_test_db(store_dir.clone(), "images");
                let handle = test_handle("lib-disconnect-drops-connection", store_dir, db);

                handle
                    .connect_sync_with_test_home(
                        Arc::new(InMemoryCloudHome::new()),
                        CloudCipher::Plaintext,
                    )
                    .await
                    .expect("connect over injected home");
                assert!(handle.sync.is_connected(), "connect installs a connection");

                handle.stop_sync();
                assert!(
                    handle.sync.is_connected(),
                    "stop_sync keeps the connection installed so start_sync can resume it",
                );

                handle.disconnect_sync();
                assert!(
                    !handle.sync.is_connected(),
                    "disconnect_sync drops the connection entirely — nothing it \
             cached survives past this call",
                );
            })
            .await
            .expect("disconnect-connection test task");
        })
        .await;
}

#[path = "handle_tests/join_through_the_facade.rs"]
mod join_through_the_facade;
