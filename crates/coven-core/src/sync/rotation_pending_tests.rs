//! A device that has not adopted a store-key rotation the cloud has already
//! committed must seal nothing new for the cloud — not a changeset, not a blob,
//! not a tombstone, not a snapshot — until it adopts. Confidentiality after a
//! member removal rests entirely on the rotation: the removed member keeps its
//! S3 credential and residual bucket read, so anything this device seals under
//! the superseded generation in the meantime is readable to them.
//!
//! These drive the real [`CloudSyncStorage`] over an [`InMemoryCloudHome`] (not
//! the plaintext-shaped `TestStore` other sync tests use), because the
//! point here is to observe actual sealed bytes at rest: whether an object
//! reaches the cloud at all, and whether the removed member's superseded key
//! can open it.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use crate::clock::SystemClock;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::{
    BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError, CloudHomeJoinInfo,
};
use crate::sync::cloud_storage::{
    BlobPathScheme, CloudCipher, CloudCipherAccess, CloudSyncStorage,
};
use crate::sync::cycle::run_single_sync_cycle;
use crate::sync::hlc::Hlc;
use crate::sync::invite::unwrap_store_keyring_for_refs;
use crate::sync::membership::MemberRole;
use crate::sync::membership_ops::{
    invite_member, invite_serial_member, remove_member, remove_serial_member_and_adopt,
    MembershipOpsError, OWNER_PUBKEY_STATE_KEY,
};
use crate::sync::storage::{
    CoordinationError, CoordinationStorage, CreateHeadError, ReplaceHeadError, StorageError,
    SyncStorage, VersionToken, VersionedObject,
};
use crate::sync::store_commit::{StoreDeviceRegistration, StoreRootRef, StoreSerialHeadState};
use crate::sync::test_helpers::{
    host_exec, open_serial_test_db, open_test_db, pubkey_hex, temp_store_dir, test_migrations,
    test_synced_tables, TestCustody,
};

const LIB_ID: &str = "rotation-pending-test";
const DEVICE_ID: &str = "owner-device";

fn storage_for(home: &InMemoryCloudHome, key: [u8; 32], keypair: &UserKeypair) -> CloudSyncStorage {
    CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Encrypted(EncryptionService::from_key(key)),
        BlobPathScheme::Hashed,
        LIB_ID,
        keypair.clone(),
    )
    .expect("build exact test cloud storage")
}

async fn create_test_store(
    db: &crate::database::Database,
    storage: &CloudSyncStorage,
    owner: &UserKeypair,
) -> StoreRootRef {
    crate::sync::store_protocol_root::create_store(db, storage, LIB_ID, owner)
        .await
        .expect("create exact test Store");
    db.local_store_root_ref()
        .await
        .expect("read exact test Store root")
        .expect("created test Store root is present")
}

async fn local_store_device_id(db: &crate::database::Database) -> String {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device id")
        .expect("local Store device registration is active")
}

async fn founder_registration(
    storage: &CloudSyncStorage,
    root: &StoreRootRef,
) -> StoreDeviceRegistration {
    crate::sync::store_objects::load_founder_registration(storage, root)
        .await
        .expect("load exact founder Store device registration")
        .value
}

/// `InMemoryCloudHome` refuses `grant_access` — it models a backend with no
/// concept of a per-member cloud account, which is not what these tests are
/// about. This forwards every other call straight through to the same backing
/// store and returns a dummy S3 grant, exactly so `invite_member`'s access-grant
/// step (irrelevant here — these tests are about the store-key rotation, not
/// provider access control) does not stand in the way of building the chain.
#[derive(Clone)]
struct GrantingCloudHome(InMemoryCloudHome);

#[async_trait]
impl CloudHome for GrantingCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.0.put_object(key, data).await
    }
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        self.0.open_multipart(key, total_len).await
    }
    fn multipart_threshold(&self) -> u64 {
        self.0.multipart_threshold()
    }
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.0.read(key).await
    }
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.0.read_range(key, start, end).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.0.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.0.delete(key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.0.exists(key).await
    }
    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        match desired {
            CloudAccessState::Present { .. } => {
                Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                    bucket: "test-bucket".to_string(),
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    access_key: "test-access-key".to_string(),
                    secret_key: "test-secret-key".to_string(),
                    key_prefix: None,
                }))
            }
            absent => self.0.set_access(absent).await,
        }
    }
}

#[derive(Clone)]
struct SerialMembershipFixture {
    storage: Arc<CloudSyncStorage>,
    home: GrantingCloudHome,
    db: crate::database::Database,
    _directory: Arc<tempfile::TempDir>,
    db_path: std::path::PathBuf,
    owner: UserKeypair,
    member: UserKeypair,
    device_id: String,
    key: [u8; 32],
}

impl SerialMembershipFixture {
    async fn create(key: [u8; 32]) -> Self {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let directory = Arc::new(tempfile::tempdir().expect("create Serial membership database"));
        let db_path = directory.path().join("store.sqlite3");
        let storage = Arc::new(
            storage_for(&home, key, &owner).with_test_serial_coordination(Arc::new(home.clone())),
        );
        let (db, _stamper) = crate::database::Database::open(
            &db_path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "test-device".to_string(),
            &test_migrations(),
        )
        .expect("open file-backed Serial membership database");
        create_test_store(&db, storage.as_ref(), &owner).await;
        let device_id = local_store_device_id(&db).await;
        Self {
            storage,
            home: GrantingCloudHome(home),
            db,
            _directory: directory,
            db_path,
            owner,
            member,
            device_id,
            key,
        }
    }

    async fn restart(self, encryption: EncryptionService) -> Self {
        let Self {
            storage,
            home,
            db,
            _directory,
            db_path,
            owner,
            member,
            device_id,
            key,
        } = self;
        drop(db);
        drop(storage);

        let (db, _stamper) = crate::database::Database::open(
            &db_path,
            test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "test-device".to_string(),
            &test_migrations(),
        )
        .expect("reopen file-backed Serial membership database");
        let storage = Arc::new(
            CloudSyncStorage::new(
                Arc::new(home.0.clone()),
                CloudCipher::Encrypted(encryption),
                BlobPathScheme::Hashed,
                LIB_ID,
                owner.clone(),
            )
            .expect("rebuild exact test cloud storage")
            .with_test_serial_coordination(Arc::new(home.0.clone())),
        );
        crate::sync::cloud_storage::restore_pending_rotation(
            &db,
            storage.shared_pending_rotation().as_ref(),
        )
        .await
        .expect("restore durable rotation gate");

        Self {
            storage,
            home,
            db,
            _directory,
            db_path,
            owner,
            member,
            device_id,
            key,
        }
    }

    async fn invite(&self) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
        invite_serial_member(
            self.storage.as_ref(),
            &self.home,
            self.storage.serial_coordination().unwrap(),
            &self.device_id,
            &self.owner,
            &Hlc::new(DEVICE_ID.to_string()),
            &pubkey_hex(&self.member),
            None,
            MemberRole::Member,
            &EncryptionService::from_key(self.key),
            LIB_ID,
            "Serial Store",
            &self.db,
        )
        .await
    }

    async fn remove(
        &self,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &dyn CloudCipherAccess,
    ) -> Result<String, MembershipOpsError> {
        self.remove_with_coordination(custody, cipher, self.storage.serial_coordination().unwrap())
            .await
    }

    async fn remove_with_coordination(
        &self,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &dyn CloudCipherAccess,
        coordination: &dyn CoordinationStorage,
    ) -> Result<String, MembershipOpsError> {
        remove_serial_member_and_adopt(
            self.storage.as_ref(),
            &self.home,
            coordination,
            &self.device_id,
            &self.owner,
            &Hlc::new(DEVICE_ID.to_string()),
            &pubkey_hex(&self.member),
            &EncryptionService::from_key(self.key),
            custody,
            cipher,
            self.storage.shared_pending_rotation().as_ref(),
            &self.db,
        )
        .await
    }
}

struct RefreshHeadVersionBeforeFirstReplace {
    inner: Arc<CloudSyncStorage>,
    refreshed: AtomicBool,
    replacements: AtomicUsize,
}

#[async_trait]
impl CoordinationStorage for RefreshHeadVersionBeforeFirstReplace {
    async fn provider_binding(
        &self,
    ) -> Result<crate::sync::storage::ResolvedProviderBinding, CoordinationError> {
        CoordinationStorage::provider_binding(self.inner.as_ref()).await
    }

    async fn read_head(&self, key: &str) -> Result<VersionedObject, CoordinationError> {
        self.inner.read_head(key).await
    }

    async fn create_head(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<VersionedObject, CreateHeadError> {
        self.inner.create_head(key, bytes).await
    }

    async fn replace_head(
        &self,
        key: &str,
        expected: &VersionToken,
        bytes: &[u8],
    ) -> Result<VersionedObject, ReplaceHeadError> {
        self.replacements.fetch_add(1, Ordering::SeqCst);
        if !self.refreshed.swap(true, Ordering::SeqCst) {
            let current = self.inner.read_head(key).await?;
            self.inner
                .replace_head(key, expected, &current.bytes)
                .await?;
            return Err(ReplaceHeadError::VersionMismatch);
        }
        self.inner.replace_head(key, expected, bytes).await
    }

    async fn delete_head(&self, key: &str) -> Result<(), CoordinationError> {
        self.inner.delete_head(key).await
    }
}

async fn activate_external_serial_member_role(
    fixture: &SerialMembershipFixture,
    role: MemberRole,
    encryption: &EncryptionService,
    created_at: &str,
) -> crate::sync::store_outbound::PreparedStoreOperationCommit {
    let root = fixture
        .db
        .local_store_root_ref()
        .await
        .expect("read Serial Store root")
        .expect("Serial Store root exists");
    let authorization =
        crate::sync::store_engine::serial::publication::current_serial_authorization(
            &fixture.db,
            fixture.storage.as_ref(),
            fixture.storage.serial_coordination().unwrap(),
        )
        .await
        .expect("load current Serial authorization");
    assert_eq!(
        authorization.key_generation,
        encryption.current_generation()
    );
    let member_pubkey = pubkey_hex(&fixture.member);
    let wrapped_key = crate::sync::wrapped_store_key::prepare_wrapped_store_key(
        fixture.storage.as_ref(),
        root.store_root_hash,
        &member_pubkey,
        crate::sync::invite::signed_serial_wrapped_key(
            &root.store_root_id.to_string(),
            &member_pubkey,
            encryption,
            &fixture.owner,
        )
        .expect("sign external Serial wrapped key"),
    )
    .await
    .expect("prepare external Serial wrapped key");
    let entry = authorization
        .membership
        .signed_set_member_with_wrapped_key(
            &fixture.owner,
            member_pubkey,
            None,
            role,
            wrapped_key.reference.clone(),
            created_at.to_string(),
        )
        .expect("prepare external Serial membership change");
    crate::sync::store_outbound::activate_test_serial_control_candidate(
        &fixture.db,
        fixture.storage.as_ref(),
        fixture.storage.serial_coordination().unwrap(),
        &fixture.device_id,
        &fixture.owner,
        crate::sync::store_commit::StoreControl::SerialMembership { entry },
        vec![wrapped_key],
    )
    .await
    .expect("activate external Serial membership change")
}

#[tokio::test]
async fn serial_invite_retries_after_head_cas_before_sqlite_materialization() {
    let fixture = SerialMembershipFixture::create([0x30; 32]).await;
    let (head_activated, _resume) = fixture
        .db
        .arm_test_pause(crate::database::DatabaseTestPoint::SerialStoreHeadActivated);
    let mut task = {
        let fixture = fixture.clone();
        tokio::spawn(async move { fixture.invite().await })
    };
    tokio::select! {
        () = head_activated.notified() => {}
        result = &mut task => panic!("Serial invitation finished before its head pause: {result:?}"),
    }
    assert!(!fixture
        .db
        .serial_membership_state()
        .await
        .expect("read pre-materialization Serial membership")
        .expect("Serial membership is initialized")
        .can_write(&pubkey_hex(&fixture.member)));
    task.abort();
    task.await
        .expect_err("simulate process loss after head CAS");
    let key = fixture.key;
    let fixture = fixture.restart(EncryptionService::from_key(key)).await;

    let code = fixture
        .invite()
        .await
        .expect("retry materializes the already-activated invitation");
    assert!(matches!(
        code.membership_floor,
        crate::join_code::MembershipFloor::Serial(Some(_))
    ));
    assert!(fixture
        .db
        .serial_membership_state()
        .await
        .expect("read retried Serial membership")
        .expect("Serial membership is initialized")
        .can_write(&pubkey_hex(&fixture.member)));
}

#[tokio::test]
async fn serial_materialization_records_activated_invite_progress_atomically() {
    let fixture = SerialMembershipFixture::create([0x31; 32]).await;
    let (materialized, _resume) = fixture
        .db
        .arm_test_pause(crate::database::DatabaseTestPoint::SerialStoreMaterialized);
    let mut task = {
        let fixture = fixture.clone();
        tokio::spawn(async move { fixture.invite().await })
    };
    tokio::select! {
        () = materialized.notified() => {}
        result = &mut task => panic!("Serial invitation finished before materialization pause: {result:?}"),
    }
    let mutation = fixture
        .db
        .outbound_membership_mutation()
        .await
        .expect("read materialized Serial invitation")
        .expect("materialized Serial invitation remains resumable");
    let progress: serde_json::Value = serde_json::from_slice(&mutation.progress_bytes)
        .expect("parse materialized Serial invitation progress");
    assert_eq!(progress["state"], "activated");
    task.abort();
    task.await
        .expect_err("simulate process loss after materialization");

    let code = fixture
        .invite()
        .await
        .expect("retry terminalizes the materialized invitation");
    let receipt = fixture
        .db
        .terminal_serial_invite_mutation()
        .await
        .expect("read terminal Serial invitation")
        .expect("terminal Serial invitation receipt exists");
    let receipt_code: crate::join_code::InviteCode = serde_json::from_slice(&receipt.result_bytes)
        .expect("parse terminal Serial invitation result");
    assert_eq!(
        crate::join_code::encode(&code),
        crate::join_code::encode(&receipt_code)
    );
}

#[tokio::test]
async fn serial_removal_activation_retains_payload_and_committed_gate_until_adoption() {
    let fixture = SerialMembershipFixture::create([0x32; 32]).await;
    fixture.invite().await.expect("invite Serial member");
    let custody = Arc::new(TestCustody::default());
    custody.set_initial_key(fixture.key);
    let cipher = fixture.storage.cipher_state().clone();
    let (before_adoption, _resume) = fixture
        .db
        .arm_test_pause(crate::database::DatabaseTestPoint::SerialRemovalBeforeAdoption);
    let mut task = {
        let fixture = fixture.clone();
        let custody = Arc::clone(&custody);
        let cipher = cipher.clone();
        tokio::spawn(async move { fixture.remove(custody.as_ref(), cipher.as_ref()).await })
    };
    tokio::select! {
        () = before_adoption.notified() => {}
        result = &mut task => panic!("Serial removal finished before adoption pause: {result:?}"),
    }
    let mutation = fixture
        .db
        .outbound_membership_mutation()
        .await
        .expect("read activated Serial removal")
        .expect("activated Serial removal retains its journal");
    let plan: serde_json::Value =
        serde_json::from_slice(&mutation.plan_bytes).expect("parse activated Serial removal plan");
    assert!(plan["keyring_payload"]
        .as_array()
        .is_some_and(|payload| !payload.is_empty()));
    let progress: serde_json::Value = serde_json::from_slice(&mutation.progress_bytes)
        .expect("parse activated Serial removal progress");
    assert_eq!(progress["state"], "activated");
    assert_eq!(
        fixture
            .storage
            .shared_pending_rotation()
            .pending_generation(),
        Some(2)
    );
    task.abort();
    task.await
        .expect_err("simulate process loss before key adoption");
    let key = fixture.key;
    let fixture = fixture.restart(EncryptionService::from_key(key)).await;
    assert!(matches!(
        fixture.storage.store_blob_protection(),
        Err(StorageError::RotationPending(_))
    ));
    let restarted_cipher = fixture.storage.cipher_state().clone();

    fixture
        .remove(custody.as_ref(), restarted_cipher.as_ref())
        .await
        .expect("retry adopts the activated Serial removal");
    fixture
        .storage
        .store_blob_protection()
        .expect("Store-encrypted sealing resumes after adoption");
}

#[tokio::test]
async fn serial_removal_retry_returns_the_terminal_result_after_lost_response() {
    let fixture = SerialMembershipFixture::create([0x33; 32]).await;
    fixture.invite().await.expect("invite Serial member");
    let custody = Arc::new(TestCustody::default());
    custody.set_initial_key(fixture.key);
    let cipher = fixture.storage.cipher_state().clone();
    let (terminalized, _resume) = fixture
        .db
        .arm_test_pause(crate::database::DatabaseTestPoint::SerialMembershipTerminalized);
    let mut task = {
        let fixture = fixture.clone();
        let custody = Arc::clone(&custody);
        let cipher = cipher.clone();
        tokio::spawn(async move { fixture.remove(custody.as_ref(), cipher.as_ref()).await })
    };
    tokio::select! {
        () = terminalized.notified() => {}
        result = &mut task => panic!("Serial removal finished before terminal pause: {result:?}"),
    }
    let expected = match cipher.snapshot() {
        CloudCipher::Encrypted(encryption) => encryption.fingerprint(),
        CloudCipher::Plaintext => panic!("Serial removal changed to plaintext"),
    };
    task.abort();
    task.await
        .expect_err("simulate a lost terminal removal response");
    let persisted = crate::keys::MasterKeyCustody::unlock(custody.as_ref())
        .expect("unlock persisted rotated keyring")
        .expect("rotated keyring is persisted");
    let fixture = fixture.restart(EncryptionService::from(persisted)).await;
    let restarted_cipher = fixture.storage.cipher_state().clone();

    let retried = fixture
        .remove(custody.as_ref(), restarted_cipher.as_ref())
        .await
        .expect("retry returns the durable terminal removal result");
    assert_eq!(retried, expected);
    assert!(fixture
        .db
        .outbound_membership_mutation()
        .await
        .expect("read active membership journal")
        .is_none());
    assert_eq!(
        fixture
            .storage
            .shared_pending_rotation()
            .pending_generation(),
        None
    );
}

#[tokio::test]
async fn serial_removal_reprepare_replaces_the_exact_rotation_owner() {
    let fixture = SerialMembershipFixture::create([0x36; 32]).await;
    fixture.invite().await.expect("invite Serial member");
    let custody = Arc::new(TestCustody::default());
    custody.set_initial_key(fixture.key);
    let cipher = fixture.storage.cipher_state().clone();
    let coordination = RefreshHeadVersionBeforeFirstReplace {
        inner: Arc::clone(&fixture.storage),
        refreshed: AtomicBool::new(false),
        replacements: AtomicUsize::new(0),
    };
    let (before_adoption, resume) = fixture
        .db
        .arm_test_pause(crate::database::DatabaseTestPoint::SerialRemovalBeforeAdoption);
    let mut task = {
        let fixture = fixture.clone();
        let custody = Arc::clone(&custody);
        let cipher = cipher.clone();
        tokio::spawn(async move {
            fixture
                .remove_with_coordination(custody.as_ref(), cipher.as_ref(), &coordination)
                .await
                .map(|result| (result, coordination.replacements.load(Ordering::SeqCst)))
        })
    };
    tokio::select! {
        () = before_adoption.notified() => {}
        result = &mut task => panic!("reprepared Serial removal finished before adoption pause: {result:?}"),
    }
    let mutation = fixture
        .db
        .outbound_membership_mutation()
        .await
        .expect("read activated reprepared removal")
        .expect("reprepared removal remains resumable before adoption");
    let gate: serde_json::Value = serde_json::from_str(
        &fixture
            .db
            .get_protocol_state(crate::sync::cloud_storage::ROTATION_GATE_STATE_KEY)
            .await
            .expect("read durable rotation gate")
            .expect("committed rotation gate remains durable"),
    )
    .expect("parse durable rotation gate");
    assert_eq!(
        gate["local_committed"]["mutation"].as_str(),
        Some(mutation.intent_hash.to_string().as_str())
    );
    assert_eq!(
        fixture
            .storage
            .shared_pending_rotation()
            .pending_generation(),
        Some(2)
    );
    resume.notify_one();
    let (result, replacements) = task
        .await
        .expect("join reprepared Serial removal")
        .expect("reprepared Serial removal activates and adopts");
    assert_eq!(replacements, 2);
    let receipt = fixture
        .db
        .terminal_serial_removal_mutation()
        .await
        .expect("read terminal reprepared removal")
        .expect("reprepared removal terminal receipt exists");
    let receipt_result: String =
        serde_json::from_slice(&receipt.result_bytes).expect("parse terminal removal result");
    assert_eq!(result, receipt_result);
    assert!(fixture
        .db
        .outbound_membership_mutation()
        .await
        .expect("read completed membership journal")
        .is_none());
    assert_eq!(
        fixture
            .storage
            .shared_pending_rotation()
            .pending_generation(),
        None
    );
}

#[tokio::test]
async fn serial_invite_does_not_reuse_receipt_after_remote_role_change() {
    let fixture = SerialMembershipFixture::create([0x34; 32]).await;
    let first = fixture.invite().await.expect("invite Serial member");
    activate_external_serial_member_role(
        &fixture,
        MemberRole::Follower,
        &EncryptionService::from_key(fixture.key),
        "0000000000002-0000-external",
    )
    .await;

    let second = fixture
        .invite()
        .await
        .expect("re-invite after external role change");

    assert_ne!(
        crate::join_code::encode(&first),
        crate::join_code::encode(&second)
    );
    let authorization =
        crate::sync::store_engine::serial::publication::current_serial_authorization(
            &fixture.db,
            fixture.storage.as_ref(),
            fixture.storage.serial_coordination().unwrap(),
        )
        .await
        .expect("load re-invited Serial authorization");
    assert_eq!(
        authorization
            .membership
            .current_members()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>(),
        std::collections::BTreeMap::from([
            (pubkey_hex(&fixture.owner), MemberRole::Owner),
            (pubkey_hex(&fixture.member), MemberRole::Member),
        ])
    );
}

#[tokio::test]
async fn serial_removal_does_not_reuse_receipt_after_remote_reinvite() {
    let fixture = SerialMembershipFixture::create([0x35; 32]).await;
    fixture.invite().await.expect("invite Serial member");
    let custody = Arc::new(TestCustody::default());
    custody.set_initial_key(fixture.key);
    let cipher = fixture.storage.cipher_state().clone();
    let first = fixture
        .remove(custody.as_ref(), cipher.as_ref())
        .await
        .expect("remove Serial member");
    let current_encryption = match cipher.snapshot() {
        CloudCipher::Encrypted(encryption) => encryption,
        CloudCipher::Plaintext => panic!("Serial removal changed to plaintext"),
    };
    activate_external_serial_member_role(
        &fixture,
        MemberRole::Member,
        &current_encryption,
        "0000000000003-0000-external",
    )
    .await;

    let second = fixture
        .remove(custody.as_ref(), cipher.as_ref())
        .await
        .expect("remove externally re-invited Serial member");

    assert_ne!(first, second);
    let authorization =
        crate::sync::store_engine::serial::publication::current_serial_authorization(
            &fixture.db,
            fixture.storage.as_ref(),
            fixture.storage.serial_coordination().unwrap(),
        )
        .await
        .expect("load second Serial removal authorization");
    assert_eq!(authorization.key_generation, 3);
    assert!(!authorization
        .membership
        .current_members()
        .iter()
        .any(|(pubkey, _)| pubkey == &pubkey_hex(&fixture.member)));
}

#[tokio::test]
async fn public_serial_invite_activates_one_control_only_commit() {
    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let key = [0x41; 32];
    let storage =
        storage_for(&home, key, &owner).with_test_serial_coordination(Arc::new(home.clone()));
    let db = open_serial_test_db();
    let root = create_test_store(&db, &storage, &owner).await;
    let device_id = local_store_device_id(&db).await;
    let registration = founder_registration(&storage, &root).await;
    let granting_home = GrantingCloudHome(home.clone());
    let code = invite_serial_member(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &Hlc::new(DEVICE_ID.to_string()),
        &pubkey_hex(&member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key(key),
        LIB_ID,
        "Serial Store",
        &db,
    )
    .await
    .expect("public Serial invitation");
    let invite_wrapped_key = code.wrapped_key.clone();
    let commit_ref = match code.membership_floor {
        crate::join_code::MembershipFloor::Serial(Some(reference)) => reference,
        crate::join_code::MembershipFloor::Serial(None) => {
            panic!("Serial invitation returned the root floor")
        }
        crate::join_code::MembershipFloor::MergeConcurrent(_) => {
            panic!("Serial invitation returned a causal membership floor")
        }
    };
    let head = storage
        .serial_coordination()
        .unwrap()
        .read_head(crate::sync::store_commit::serial_head_key())
        .await
        .unwrap();
    let head = crate::sync::store_commit::StoreSerialHead::parse(
        &head.bytes,
        root.store_root_hash,
        &registration,
    )
    .unwrap();
    assert!(matches!(
        &head.state,
        StoreSerialHeadState::Commit { commit, .. } if commit == &commit_ref
    ));
    let commit = crate::sync::store_objects::load_commit_ref(
        &storage,
        root.store_root_hash,
        &commit_ref,
        &registration,
    )
    .await
    .unwrap()
    .value;
    commit_ref
        .verify_commit(&commit)
        .expect("prepared exact commit ref verifies its signed commit");
    assert!(matches!(
        commit.control(),
        Some(crate::sync::store_commit::StoreControl::SerialMembership { .. })
    ));
    assert!(
        crate::sync::store_objects::load_store_package(&storage, &commit_ref, &commit)
            .await
            .unwrap()
            .is_none()
    );
    assert!(commit.store_package().is_none());
    assert!(db
        .serial_membership_state()
        .await
        .unwrap()
        .unwrap()
        .can_write(&pubkey_hex(&member)));
    assert_eq!(
        db.serial_authorization_state()
            .await
            .unwrap()
            .unwrap()
            .active_wrapped_keys_for(&pubkey_hex(&member)),
        vec![invite_wrapped_key],
        "the exact invitation wrap is durable with Serial authorization",
    );

    let custody = TestCustody::default();
    custody.set_initial_key(key);
    let cipher = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    remove_serial_member_and_adopt(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &Hlc::new(DEVICE_ID.to_string()),
        &pubkey_hex(&member),
        &EncryptionService::from_key(key),
        &custody,
        &cipher,
        &pending_rotation,
        &db,
    )
    .await
    .expect("public Serial removal and rotation");
    assert!(!db
        .serial_membership_state()
        .await
        .unwrap()
        .unwrap()
        .can_write(&pubkey_hex(&member)));
    assert_eq!(db.serial_key_generation().await.unwrap(), Some(2));
    let authorization = db.serial_authorization_state().await.unwrap().unwrap();
    assert!(authorization
        .active_wrapped_keys_for(&pubkey_hex(&member))
        .is_empty());
    let owner_wraps = authorization.active_wrapped_keys_for(&pubkey_hex(&owner));
    assert_eq!(owner_wraps.len(), 1);
    assert_eq!(owner_wraps[0].generation, 2);
    let head = storage
        .serial_coordination()
        .unwrap()
        .read_head(crate::sync::store_commit::serial_head_key())
        .await
        .unwrap();
    let head = crate::sync::store_commit::StoreSerialHead::parse(
        &head.bytes,
        root.store_root_hash,
        &registration,
    )
    .unwrap();
    let StoreSerialHeadState::Commit {
        commit: removal_ref,
        ..
    } = head.state
    else {
        panic!("Serial removal did not publish a commit head")
    };
    assert_eq!(removal_ref.coord.sequence(), 2);
    let removal = crate::sync::store_objects::load_commit_ref(
        &storage,
        root.store_root_hash,
        &removal_ref,
        &registration,
    )
    .await
    .unwrap()
    .value;
    assert!(matches!(
        removal.control(),
        Some(
            crate::sync::store_commit::StoreControl::SerialMembershipAndKeyRotation {
                generation: 2,
                ..
            }
        )
    ));
    assert!(
        crate::sync::store_objects::load_store_package(&storage, &removal_ref, &removal)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn serial_rotation_extends_the_committed_wrapped_keyring() {
    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let first_member = UserKeypair::generate();
    let second_member = UserKeypair::generate();
    let initial_key = [0x51; 32];
    let initial = EncryptionService::from_key(initial_key);
    let storage = storage_for(&home, initial_key, &owner)
        .with_test_serial_coordination(Arc::new(home.clone()));
    let db = open_serial_test_db();
    let root = create_test_store(&db, &storage, &owner).await;
    let device_id = local_store_device_id(&db).await;
    let granting_home = GrantingCloudHome(home);
    let hlc = Hlc::new(DEVICE_ID.to_string());

    for member in [&first_member, &second_member] {
        invite_serial_member(
            &storage,
            &granting_home,
            storage.serial_coordination().unwrap(),
            &device_id,
            &owner,
            &hlc,
            &pubkey_hex(member),
            None,
            MemberRole::Member,
            &initial,
            LIB_ID,
            "Serial Store",
            &db,
        )
        .await
        .expect("publish Serial invitation");
    }

    let custody = TestCustody::default();
    custody.set_initial_key(initial_key);
    let cipher = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    remove_serial_member_and_adopt(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &hlc,
        &pubkey_hex(&first_member),
        &initial,
        &custody,
        &cipher,
        &pending_rotation,
        &db,
    )
    .await
    .expect("publish first Serial removal");
    let first_rotation = match cipher.snapshot() {
        CloudCipher::Encrypted(encryption) => encryption,
        CloudCipher::Plaintext => panic!("Serial Store lost its encrypted cipher"),
    };
    let sealed_before_second_rotation =
        first_rotation.seal_app_data(b"committed generation two", b"serial-rotation");

    let unrelated_generation_two = EncryptionService::from_key([0x61; 32])
        .with_appended_generation(2, [0x62; 32])
        .expect("build unrelated generation-two keyring");
    remove_serial_member_and_adopt(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &hlc,
        &pubkey_hex(&second_member),
        &unrelated_generation_two,
        &custody,
        &cipher,
        &pending_rotation,
        &db,
    )
    .await
    .expect("publish second Serial removal");

    let latest_refs = db
        .serial_authorization_state()
        .await
        .unwrap()
        .unwrap()
        .active_wrapped_keys_for(&pubkey_hex(&owner))
        .into_iter()
        .filter(|reference| reference.generation == 3)
        .collect::<Vec<_>>();
    let latest_keyring = unwrap_store_keyring_for_refs(
        &storage,
        root.store_root_hash,
        &owner,
        &root.store_root_id.to_string(),
        &latest_refs,
    )
    .await
    .expect("unwrap the second Serial rotation");
    assert_eq!(
        latest_keyring
            .open_app_data(&sealed_before_second_rotation, b"serial-rotation")
            .expect("latest Serial wrap retains the prior committed key"),
        b"committed generation two",
    );
}

#[tokio::test]
async fn serial_invitation_after_rotation_uses_committed_key_authority() {
    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let invited_member = UserKeypair::generate();
    let initial_key = [0x71; 32];
    let initial = EncryptionService::from_key(initial_key);
    let storage = storage_for(&home, initial_key, &owner)
        .with_test_serial_coordination(Arc::new(home.clone()));
    let db = open_serial_test_db();
    let root = create_test_store(&db, &storage, &owner).await;
    let device_id = local_store_device_id(&db).await;
    let granting_home = GrantingCloudHome(home);
    let hlc = Hlc::new(DEVICE_ID.to_string());

    invite_serial_member(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &hlc,
        &pubkey_hex(&removed_member),
        None,
        MemberRole::Member,
        &initial,
        LIB_ID,
        "Serial Store",
        &db,
    )
    .await
    .expect("publish initial Serial invitation");
    let custody = TestCustody::default();
    custody.set_initial_key(initial_key);
    let cipher = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    remove_serial_member_and_adopt(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &hlc,
        &pubkey_hex(&removed_member),
        &initial,
        &custody,
        &cipher,
        &pending_rotation,
        &db,
    )
    .await
    .expect("publish Serial removal and rotation");
    let rotated = match cipher.snapshot() {
        CloudCipher::Encrypted(encryption) => encryption,
        CloudCipher::Plaintext => panic!("Serial Store lost its encrypted cipher"),
    };
    let sealed = rotated.seal_app_data(b"current Serial data", b"serial invitation");

    let invite = invite_serial_member(
        &storage,
        &granting_home,
        storage.serial_coordination().unwrap(),
        &device_id,
        &owner,
        &hlc,
        &pubkey_hex(&invited_member),
        None,
        MemberRole::Member,
        &initial,
        LIB_ID,
        "Serial Store",
        &db,
    )
    .await
    .expect("publish post-rotation Serial invitation");
    let invited_keyring = unwrap_store_keyring_for_refs(
        &storage,
        root.store_root_hash,
        &invited_member,
        &root.store_root_id.to_string(),
        &[invite.wrapped_key],
    )
    .await
    .expect("invited member opens the activated Serial wrap");
    assert_eq!(
        invited_keyring
            .open_app_data(&sealed, b"serial invitation")
            .expect("Serial invitation retains the committed current key"),
        b"current Serial data",
    );
}

async fn insert_shareable_row(db: &crate::database::Database, id: &str, stamp: &str) {
    host_exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{id}', 'title', NULL, 1, '{stamp}', '2026-01-01')"
        ),
    )
    .await;
}

async fn mark_snapshot_floor(db: &crate::database::Database) {
    db.set_protocol_state("snapshot_seq", "0")
        .await
        .expect("persist snapshot floor");
}

/// Found a store with `owner` as its sole owner, add `member`, then remove
/// `member` while `custody` is failing — so the cloud rotation commits (to
/// generation 2) but this device's local adoption fails. Returns the storage
/// whose cipher and pending-rotation marker a later cycle or a retried removal
/// reads, and the `Hlc` used throughout.
async fn found_add_and_fail_to_adopt_a_removal(
    db: &crate::database::Database,
    home: &InMemoryCloudHome,
    owner: &UserKeypair,
    member: &UserKeypair,
    custody: &TestCustody,
    old_key: [u8; 32],
) -> (CloudSyncStorage, Hlc, StoreRootRef) {
    let storage = storage_for(home, old_key, owner);
    let store_root = create_test_store(db, &storage, owner).await;
    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(owner))
        .await
        .expect("pin test Store founder");
    let hlc = Hlc::new(DEVICE_ID.to_string());
    let granting_home = GrantingCloudHome(home.clone());
    invite_member(
        &storage,
        &granting_home,
        owner,
        &hlc,
        &pubkey_hex(member),
        None,
        MemberRole::Member,
        &EncryptionService::from_key(old_key),
        LIB_ID,
        "Test Store",
        db,
    )
    .await
    .expect("invite member");

    custody.set_initial_key(old_key);
    custody.fail_writes();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    let err = remove_member(
        &storage,
        storage.cloud_home(),
        owner,
        &hlc,
        &pubkey_hex(member),
        &EncryptionService::from_key(old_key),
        custody,
        &cipher_lock,
        &pending_rotation,
        db,
    )
    .await
    .expect_err("adoption fails while custody is unwritable");
    assert!(
        matches!(
            err,
            MembershipOpsError::RotationCommittedAdoptionFailed { .. }
        ),
        "the failure is the rotation-committed/adoption-failed variant, got {err:?}",
    );

    (storage, hlc, store_root)
}

/// A failed adoption remains a loud cycle failure and the durable rotation gate
/// prevents the queued write from being sealed under the removed member's key.
#[tokio::test]
async fn failed_rotation_adoption_fails_the_cycle_before_sealing() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let old_key: [u8; 32] = [40u8; 32];
    let custody = TestCustody::default();
    let home = InMemoryCloudHome::new();
    let db = open_test_db();

    let (storage, hlc, _store_root_hash) =
        found_add_and_fail_to_adopt_a_removal(&db, &home, &owner, &member, &custody, old_key).await;
    let device_id = local_store_device_id(&db).await;

    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    mark_snapshot_floor(&db).await;
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();
    run_single_sync_cycle(
        &storage,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect_err("the cycle reports the failed key adoption");

    let pending = pending_rotation
        .check(&cipher_lock.snapshot())
        .expect_err("the rotation gate remains armed");
    assert_eq!(
        pending.state,
        super::cloud_storage::RotationPendingState::LocalCommittedAndPeer {
            local_generation: 2,
            peer_generation: 2,
        }
    );
    assert_eq!(pending.live_generation, 1);

    assert_eq!(
        db.get_protocol_state("local_seq").await.unwrap(),
        None,
        "the pending Store write stays queued while key adoption is incomplete",
    );
}

/// Retrying the removal (idempotent: the member is already gone, so it re-derives
/// and re-adopts the same generation) clears the gate, and the changeset that
/// stayed queued through the stuck cycle now drains — sealed under the rotated
/// generation, not the one the removed member holds.
#[tokio::test]
async fn retrying_the_removal_adopts_the_rotation_and_drains_the_pending_changeset() {
    tokio::spawn(run_retrying_the_removal_adopts_the_rotation_and_drains_the_pending_changeset())
        .await
        .expect("removal retry and pending-write orchestration task");
}

async fn run_retrying_the_removal_adopts_the_rotation_and_drains_the_pending_changeset() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let old_key: [u8; 32] = [41u8; 32];
    let custody = TestCustody::default();
    let home = InMemoryCloudHome::new();
    let db = open_test_db();

    let (storage, hlc, _store_root) =
        found_add_and_fail_to_adopt_a_removal(&db, &home, &owner, &member, &custody, old_key).await;
    let device_id = local_store_device_id(&db).await;

    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    mark_snapshot_floor(&db).await;
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();

    // The failed refresh reports its error and seals nothing.
    run_single_sync_cycle(
        &storage,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect_err("key persistence failure aborts the cycle");
    assert_eq!(
        db.latest_local_store_position().await.unwrap(),
        None,
        "the queued Store write has no published commit while rotation is pending",
    );

    // Retry the removal now that custody is writable again.
    custody.allow_writes();
    remove_member(
        &storage,
        storage.cloud_home(),
        &owner,
        &hlc,
        &pubkey_hex(&member),
        &EncryptionService::from_key(old_key),
        &custody,
        &cipher_lock,
        &pending_rotation,
        &db,
    )
    .await
    .expect("retrying the removal converges");
    assert_eq!(
        pending_rotation.pending_generation(),
        None,
        "adoption clears the gate",
    );

    // The queued changeset now drains, sealed under the rotated generation.
    let result = run_single_sync_cycle(
        &storage,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("cycle after adoption");
    assert!(result.rotation_pending.is_none());

    db.latest_local_store_position()
        .await
        .expect("read published Store position")
        .expect("published Store write has an exact commit reference");
    assert!(matches!(
        cipher_lock.snapshot(),
        CloudCipher::Encrypted(encryption) if encryption.current_generation() == 2
    ));
}

/// Authorization refresh may adopt the key bytes, but it cannot complete a local
/// removal journal. Only retrying that exact membership operation owns the local
/// completion transition.
#[tokio::test]
async fn authorization_refresh_does_not_complete_a_local_removal() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let old_key: [u8; 32] = [42u8; 32];
    let custody = TestCustody::default();
    let home = InMemoryCloudHome::new();
    let db = open_test_db();

    let (storage, hlc, _store_root) =
        found_add_and_fail_to_adopt_a_removal(&db, &home, &owner, &member, &custody, old_key).await;
    let device_id = local_store_device_id(&db).await;

    db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&owner))
        .await
        .unwrap();
    // Seed the snapshot floor so this device's first cycle takes the ordinary
    // changeset path rather than the initial-sync snapshot coven pushes when a
    // store has pre-existing local data and no snapshot yet — these tests are
    // about the changeset push specifically.
    mark_snapshot_floor(&db).await;
    insert_shareable_row(&db, "n1", "0000000005000-0000-owner-device").await;

    let (_tmp, store_dir) = temp_store_dir();
    let cipher_lock = storage.cipher_state().clone();
    let pending_rotation = storage.shared_pending_rotation();

    run_single_sync_cycle(
        &storage,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect_err("key persistence failure aborts the cycle");
    assert_eq!(
        db.latest_local_store_position().await.unwrap(),
        None,
        "the queued Store write has no published commit while rotation is pending",
    );

    // The refresh can adopt the selected key, but the exact local removal still
    // owns journal completion.
    custody.allow_writes();
    let result = run_single_sync_cycle(
        &storage,
        &device_id,
        &hlc,
        &SystemClock,
        &db,
        &cipher_lock,
        &pending_rotation,
        &owner,
        Some(&custody),
        &store_dir,
        Some(storage.cloud_home()),
        None,
    )
    .await
    .expect("cycle reports the still-open local rotation gate");
    assert!(matches!(
        result.rotation_pending.map(|pending| pending.state),
        Some(super::cloud_storage::RotationPendingState::LocalCommitted { generation: 2 })
    ));
    assert_eq!(
        db.latest_local_store_position().await.unwrap(),
        None,
        "the queued Store write remains unpublished until the removal completes",
    );
    assert!(matches!(
        cipher_lock.snapshot(),
        CloudCipher::Encrypted(encryption) if encryption.current_generation() == 2
    ));
}
