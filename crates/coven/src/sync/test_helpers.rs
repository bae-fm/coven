/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] over an in-memory connection carrying the
/// synthetic test schema, so tests exercise the engine through the same path
/// production does.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::database::{Database, DbError};
use crate::encryption::MasterKeyring;
use crate::keys::{KeyError, MasterKeyCustody, UserKeypair};
use crate::protocol::store_commit::ObjectHash;
use crate::protocol::synced_schema::{BlobDecl, SyncedTable};
use crate::storage::SyncStorage;
use crate::store_dir::StoreDir;
use crate::Migration;

#[cfg(test)]
pub(crate) fn test_cache_locator_hash(label: &str) -> ObjectHash {
    ObjectHash::digest(label.as_bytes())
}

/// In-memory [`MasterKeyCustody`] for tests, with a switch to force `persist`
/// to fail. The switch models a device whose keyring is momentarily
/// unwritable, so a test can drive a key adoption into its failure path and then
/// clear the switch to prove the retry converges. Stores the serialized form
/// (like the real `Keyring` preset), so `stored_key` reflects exactly what a
/// caller wrote.
#[derive(Clone, Default)]
pub(crate) struct TestCustody {
    value: Arc<Mutex<Option<String>>>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl TestCustody {
    pub(crate) fn set_initial_key(&self, key: [u8; 32]) {
        *self.value.lock().unwrap() = Some(
            MasterKeyring::from(crate::encryption::EncryptionService::from_key(key))
                .to_serialized(),
        );
    }

    pub(crate) fn stored_key(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }

    /// Make the next and every subsequent `persist` fail until cleared.
    pub(crate) fn fail_writes(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let `persist` succeed again.
    pub(crate) fn allow_writes(&self) {
        self.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MasterKeyCustody for TestCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.value
            .lock()
            .unwrap()
            .as_deref()
            .map(MasterKeyring::from_serialized)
            .transpose()
            .map_err(|e| KeyError::Crypto(e.to_string()))
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KeyError::Persistence(
                "forced keyring write failure".to_string(),
            ));
        }
        *self.value.lock().unwrap() = Some(keyring.to_serialized());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}

pub(crate) fn test_store_security(
    store_id: &str,
    master_keys: Arc<dyn MasterKeyCustody>,
) -> crate::store_security::StoreSecurity {
    let store_keys = crate::keys::StoreKeys::bind(store_id.to_string());
    let identity = crate::identity_custody::IdentityCustody::InMemory(UserKeypair::generate())
        .resolve(
            &store_keys,
            &StoreDir::new(format!("{store_id}-unused-test-identity-directory")),
        );
    crate::store_security::StoreSecurity::new(store_keys, master_keys, identity)
}

/// The synthetic, domain-free schema the sync tests run against. Three synced
/// tables exercising the engine's generic mechanics: a *gated root* (`notes`,
/// gated by its `shared` boolean), a child with a foreign key (`note_tags`,
/// which inherits the gate and exercises FK-violation retry), and a child that
/// CAN carry a blob (`note_photos`, also FK-to-`notes`, so it inherits the gate).
/// `note_photos` carries no blob here; blob tests declare one with
/// [`test_synced_tables_with_blob`].
pub(crate) fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ]
}

/// [`test_synced_tables`] with `note_photos` declared blob-bearing per `decl`, for
/// tests exercising the blob push/pull/backfill paths. The blob id defaults to the
/// `note_photos` primary key; `note_photos.cloud_path` holds a readable key for
/// plain-scheme tests, and `note_photos.blob_id` is there for a decl that names a
/// blob id apart from the PK — the shape a row repointed at a new blob needs, since
/// the row keeps its primary key.
pub(crate) fn test_synced_tables_with_blob(decl: BlobDecl) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(decl),
    ]
}

/// [`test_synced_tables`] with TWO blob-bearing children of the gated `notes` root:
/// `note_photos` per `photo_decl` (a release file, user-provided) and `note_covers`
/// per `cover_decl` (a host-provided asset). Both inherit the `notes` gate, so a
/// make_remote of a note carries both — the user-provided file through the durable
/// outbox and the host-provided cover through the inline push — exercising the
/// per-provenance split in one subtree.
pub(crate) fn test_synced_tables_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(photo_decl),
        SyncedTable::new(
            "note_covers",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .carries_blob(cover_decl),
    ]
}

/// Open a test [`Database`] over the synthetic schema with `note_photos` declared
/// blob-bearing per `decl`.
pub(crate) fn open_test_db_with_blob(decl: BlobDecl) -> Database {
    open_test_db_schema(test_synced_tables_with_blob(decl), test_migrations())
}

/// Open a read-test [`Database`] whose `note_photos` child carries a blob in
/// `namespace`, so `read_blob`'s locality dispatch can resolve a
/// blob in that namespace up to its gated `notes` root. The decl's namespace MUST
/// match the blobs the test reads (the read path resolves the carrying table from the
/// blob's namespace); its provenance/fill don't matter to that resolution (the read
/// reads the row → root → gate, and takes provenance off the `BlobRef`), so this fixes
/// them. Pair with [`Database::plant_blob_row_for_test`].
pub(crate) fn read_test_db(namespace: &str) -> Database {
    open_test_db_with_blob(BlobDecl::new(
        namespace,
        crate::protocol::blob::Provenance::UserProvided,
        crate::protocol::blob::CacheFill::CacheLazy,
    ))
}

/// Like [`read_test_db`] but with a chosen `max_concurrent_downloads`, so a pin test
/// can drive the download loop concurrently. Uploads run one at a time (not exercised here).
pub(crate) fn read_test_db_with_download_limit(namespace: &str, downloads: usize) -> Database {
    let tables = test_synced_tables_with_blob(BlobDecl::new(
        namespace,
        crate::protocol::blob::Provenance::UserProvided,
        crate::protocol::blob::CacheFill::CacheLazy,
    ));
    let limits = crate::protocol::blob::TransferLimits {
        uploads: std::num::NonZeroUsize::MIN,
        downloads: std::num::NonZeroUsize::new(downloads).expect("downloads limit is nonzero"),
    };
    Database::open(
        std::path::Path::new(":memory:"),
        tables,
        crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
        limits,
        "test-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &test_migrations(),
    )
    .expect("open test database")
}

/// Open a test [`Database`] with both `note_photos` (per `photo_decl`) and
/// `note_covers` (per `cover_decl`) declared blob-bearing — the schema for the
/// per-provenance transition tests.
pub(crate) fn open_test_db_with_user_and_host_blobs(
    photo_decl: BlobDecl,
    cover_decl: BlobDecl,
) -> Database {
    open_test_db_schema(
        test_synced_tables_with_user_and_host_blobs(photo_decl, cover_decl),
        test_migrations(),
    )
}

/// The synthetic test schema as a single-migration ladder, so a test db opens at
/// `schema_version() == 1`. The host-schema ladder for every `open_test_db*`
/// helper.
pub(crate) fn test_migrations() -> Vec<Migration> {
    vec![Migration::run(1, "test-schema", create_synced_schema)]
}

/// Create the synthetic test schema on a connection. Run as the host migration
/// step for [`open_test_db`] (see [`test_migrations`]).
pub(crate) fn create_synced_schema(conn: &crate::MigrationContext<'_>) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            shared INTEGER NOT NULL DEFAULT 0,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        ) STRICT;
        CREATE TABLE note_tags (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_photos (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            blob_id TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;
        CREATE TABLE note_covers (
            id TEXT PRIMARY KEY,
            note_id TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            hash TEXT,
            _updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            cloud_path TEXT,
            FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
        ) STRICT;",
    )
    .map_err(DbError::from)
}

/// Open a [`Database`] over a fresh in-memory connection with the synthetic test
/// schema and the [`test_synced_tables`] synced set.
pub(crate) fn open_test_db() -> Database {
    open_test_db_schema(test_synced_tables(), test_migrations())
}

pub(crate) fn open_test_db_with_tombstone_grace(grace: chrono::Duration) -> Database {
    open_test_db_schema_with_tombstone_grace(test_synced_tables(), test_migrations(), grace)
}

/// Like [`open_test_db`] but with an explicit synced set and migration ladder, for
/// tests that exercise a different schema (gate tests).
pub(crate) fn open_test_db_schema(
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
) -> Database {
    open_test_db_schema_with_tombstone_grace(
        tables,
        migrations,
        crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
    )
}

fn open_test_db_schema_with_tombstone_grace(
    tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
    grace: chrono::Duration,
) -> Database {
    // `:memory:` is unique per connection; the Database owns exactly one.
    Database::open(
        std::path::Path::new(":memory:"),
        tables,
        grace,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &migrations,
    )
    .expect("open test database")
}

/// Open a test [`Database`] over the synthetic schema with a caller-supplied
/// register clock (so a test can control the wall clock), plus an extra `seed`
/// step run after the host schema is created to plant host rows before
/// `Database::open` reads its floor.
///
/// Used only by the register-clock tests (`hlc_register_tests`).
pub(crate) fn open_test_db_with_hlc(
    hlc: std::sync::Arc<crate::protocol::hlc::Hlc>,
    seed: impl for<'connection> Fn(&crate::MigrationContext<'connection>) -> Result<(), DbError>
        + Send
        + Sync
        + 'static,
) -> Database {
    let migrations = vec![Migration::run(1, "test-schema", move |conn| {
        create_synced_schema(conn)?;
        seed(conn)
    })];
    Database::open_with_hlc(
        std::path::Path::new(":memory:"),
        test_synced_tables(),
        crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        hlc,
        &migrations,
    )
    .expect("open test database with hlc")
}

/// A temp dir plus a [`StoreDir`] rooted at it. The returned `TempDir` must be
/// held for the directory to outlive the test.
pub(crate) fn temp_store_dir() -> (tempfile::TempDir, StoreDir) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new(tmp.path());
    (tmp, dir)
}

/// Hex-encoded ed25519 public key, as membership entries and the wrapped-key
/// store identify a member.
pub(crate) fn pubkey_hex(kp: &UserKeypair) -> String {
    crate::keys::public_key_hex(kp)
}

/// Ed25519 identity derived from exact test-owned seed bytes.
pub(crate) fn user_keypair_from_seed(seed: [u8; 32]) -> UserKeypair {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
    UserKeypair::from_signing_key_bytes(&signing_key.to_keypair_bytes())
        .expect("seed-derived signing key is valid")
}

pub(crate) fn test_cloud_home() -> Arc<crate::storage::cloud::test_utils::InMemoryCloudHome> {
    test_cloud_home_with_binding(crate::protocol::objects::ResolvedProviderBinding {
        store: crate::protocol::objects::StoreProviderBinding::GoogleDrive {
            corpus: crate::protocol::objects::GoogleDriveCorpus::SharedDrive {
                drive_id: "test-drive".to_string(),
                folder_id: "test-folder".to_string(),
            },
        },
        device: crate::protocol::objects::ProviderDeviceBinding {
            principal: crate::protocol::objects::ProviderPrincipalId::GoogleDrive {
                permission_id: "test-permission".to_string(),
            },
        },
    })
}

pub(crate) fn test_cloud_home_with_binding(
    binding: crate::protocol::objects::ResolvedProviderBinding,
) -> Arc<crate::storage::cloud::test_utils::InMemoryCloudHome> {
    Arc::new(
        crate::storage::cloud::test_utils::InMemoryCloudHome::new().with_provider_binding(binding),
    )
}

/// Grants a Dropbox shared-folder membership to whichever peer account asks —
/// the provider-side step a cross-principal admission needs before the joining
/// device can write to the store's namespace.
pub(crate) struct TestDropboxAccessAdministrator {
    pub namespace_id: String,
}

#[async_trait::async_trait]
impl crate::sync::store::DeviceProviderAccessAdministrator for TestDropboxAccessAdministrator {
    async fn grant_member_access(
        &self,
        _member_pubkey: &str,
        _provider_account_email: Option<&str>,
        peer: &crate::protocol::objects::ProviderDeviceBinding,
    ) -> Result<crate::protocol::provider::ProviderAccessLocator, crate::sync::store::DeviceJoinError>
    {
        let crate::protocol::objects::ProviderPrincipalId::Dropbox { account_id } = &peer.principal
        else {
            return Err(crate::sync::store::DeviceJoinError::Provider(
                "test Dropbox access administrator received a non-Dropbox peer".to_string(),
            ));
        };
        Ok(
            crate::protocol::provider::ProviderAccessLocator::DropboxSharedFolderMember {
                namespace_id: self.namespace_id.clone(),
                account_id: account_id.clone(),
            },
        )
    }
}

pub(crate) struct TestStore {
    home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
    pub root: crate::protocol::store_commit::StoreRootRef,
    signer: UserKeypair,
    founder: TestDevice,
    producers: Arc<tokio::sync::Mutex<TestStoreProducers>>,
}

#[async_trait::async_trait]
impl crate::storage::SyncStorage for TestStore {
    fn blob_path_scheme(&self) -> crate::storage::BlobPathScheme {
        self.storage.blob_path_scheme()
    }

    fn self_uploader(&self) -> String {
        self.storage.self_uploader()
    }

    async fn probe_provider(&self) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.probe_provider().await
    }

    async fn set_member_access(
        &self,
        state: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, crate::protocol::objects::StorageError>
    {
        self.storage.set_member_access(state).await
    }

    async fn read_blob_tombstone(
        &self,
        key: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.storage.read_blob_tombstone(key).await
    }

    async fn write_blob_tombstone(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.write_blob_tombstone(key, stored_bytes).await
    }

    async fn list_blob_tombstones(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::protocol::objects::StorageError> {
        self.storage.list_blob_tombstones(prefix).await
    }

    async fn blob_tombstone_exists(
        &self,
        key: &str,
    ) -> Result<bool, crate::protocol::objects::StorageError> {
        self.storage.blob_tombstone_exists(key).await
    }

    async fn delete_blob_tombstone(
        &self,
        key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.delete_blob_tombstone(key).await
    }

    async fn list_provider_objects_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::protocol::objects::StorageError> {
        self.storage.list_provider_objects_for_test(prefix).await
    }

    async fn read_provider_object_for_test(
        &self,
        key: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.storage.read_provider_object_for_test(key).await
    }

    async fn provider_object_exists_for_test(
        &self,
        key: &str,
    ) -> Result<bool, crate::protocol::objects::StorageError> {
        self.storage.provider_object_exists_for_test(key).await
    }

    async fn probe_exact_slots(
        &self,
        journal: &dyn crate::protocol::provider::ProviderProbeJournal,
        probe_id: crate::protocol::provider::ProviderProbeId,
        binding: &crate::protocol::objects::ResolvedProviderBinding,
    ) -> Result<
        crate::protocol::provider::ExactSlotProbeReceipt,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.storage
            .probe_exact_slots(journal, probe_id, binding)
            .await
    }

    async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: crate::protocol::provider::ProviderProbeId,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::provider::ProviderProbeError>
    {
        self.storage
            .reserve_cross_principal_response_slot(probe_id)
            .await
    }

    async fn observe_exact_slot(
        &self,
        slot: &crate::protocol::objects::ObjectSlot,
    ) -> Result<
        Option<crate::protocol::objects::ExactObjectRef>,
        crate::protocol::objects::StorageError,
    > {
        self.storage.observe_exact_slot(slot).await
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &crate::protocol::objects::ObjectSlot,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.delete_exact_slot_and_verify_absent(slot).await
    }

    async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn crate::protocol::provider::DeviceJoinChallengePublicationJournal,
        probe_id: crate::protocol::provider::ProviderProbeId,
        store: &crate::protocol::objects::StoreProviderBinding,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeChallenge,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.storage
            .prepare_cross_principal_challenge(
                publication_journal,
                probe_id,
                store,
                context,
                administrator_signer,
            )
            .await
    }

    async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn crate::protocol::provider::DeviceJoinChallengePublicationJournal,
        authorization: &crate::protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        store: &crate::protocol::objects::StoreProviderBinding,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeChallenge,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.storage
            .settle_cross_principal_challenge(
                publication_journal,
                authorization,
                challenge,
                context,
                store,
            )
            .await
    }

    async fn create_cross_principal_response(
        &self,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalResponseContext,
        store: &crate::protocol::objects::StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &crate::keys::UserKeypair,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeResponse,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.storage
            .create_cross_principal_response(
                challenge,
                context,
                store,
                administrator_signing_pubkey,
                peer_signer,
            )
            .await
    }

    async fn complete_cross_principal_probe(
        &self,
        journal: &dyn crate::protocol::provider::ProviderProbeJournal,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        response: &crate::protocol::provider::CrossPrincipalProbeResponse,
        context: &crate::protocol::provider::CrossPrincipalResponseContext,
        store: &crate::protocol::objects::StoreProviderBinding,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
        peer_signing_pubkey: &str,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeReceipt,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.storage
            .complete_cross_principal_probe(
                journal,
                challenge,
                response,
                context,
                store,
                administrator_signer,
                peer_signing_pubkey,
            )
            .await
    }

    fn store_blob_protection(
        &self,
    ) -> Result<crate::protocol::objects::BlobSpoolProtection, crate::protocol::objects::StorageError>
    {
        self.storage.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<
        crate::protocol::objects::ResolvedProviderBinding,
        crate::protocol::objects::StorageError,
    > {
        self.storage.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::objects::StorageError> {
        self.storage
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<crate::protocol::objects::PreparedExactObject, crate::protocol::objects::StorageError>
    {
        self.storage
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &crate::protocol::objects::PreparedExactObject,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        object: &crate::protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: &crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, crate::protocol::objects::ExactObjectRef),
        crate::protocol::objects::StorageError,
    > {
        self.storage
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: &crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, crate::protocol::objects::PreparedExactObject),
        crate::protocol::objects::StorageError,
    > {
        self.storage
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::protocol::objects::ExactObjectRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::objects::StorageError> {
        self.storage.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        protection: crate::protocol::objects::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool_file: &std::path::Path,
    ) -> Result<crate::protocol::objects::BlobSpoolWrite, crate::protocol::objects::StorageError>
    {
        self.storage
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        slot: crate::protocol::objects::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<crate::protocol::blob::locator::StoredBlobRef, crate::protocol::objects::StorageError>
    {
        self.storage
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.verify_blob_object(blob).await
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        dest: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, crate::protocol::objects::StorageError> {
        self.storage.stage_exact_blob_download(blob, dest).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
        dest: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, crate::protocol::objects::StorageError> {
        self.storage
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn open_blob_range_reader(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
    ) -> Result<crate::storage::BlobRangeReader, crate::protocol::objects::StorageError> {
        self.storage.open_blob_range_reader(blob, protection).await
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.delete_blob_object(blob).await
    }
}

mod test_device {
    use super::*;

    pub(crate) struct TestDeviceSigningAuthority {
        registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    }

    impl TestDeviceSigningAuthority {
        pub(crate) fn registration_ref(
            &self,
        ) -> &crate::protocol::store_commit::StoreDeviceRegistrationRef {
            self.registration.reference()
        }

        pub(crate) fn registration(
            &self,
        ) -> &crate::protocol::store_commit::StoreDeviceRegistration {
            self.registration.value()
        }

        pub(crate) fn referenced_registration(
            &self,
        ) -> &crate::protocol::store_commit::ReferencedStoreDeviceRegistration {
            &self.registration
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) fn sign_device_join_attempt_for_test(
            &self,
            store_root: crate::protocol::store_commit::StoreRootRef,
            attempt_id: crate::protocol::store_commit::DeviceJoinAttemptId,
            attempt_slot: crate::protocol::objects::ObjectSlot,
            expected_registration: crate::protocol::store_commit::StoreDeviceRegistration,
            registration_slot: crate::protocol::objects::ObjectSlot,
            outcome_slot: crate::protocol::objects::ObjectSlot,
            bootstrap_cut: crate::protocol::store_commit::StoreHistoryCut,
            membership: crate::protocol::circle_control::StoreMembershipStateRef,
            provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
            provider_approval: crate::sync::store::DeviceProviderAdmissionApproval,
            provider_response: crate::sync::store::DeviceProviderResponseReservation,
            owner_grant: crate::protocol::membership::MembershipGrantId,
        ) -> Result<
            crate::protocol::store_commit::DeviceJoinAttempt,
            crate::protocol::store_commit::StoreProtocolError,
        > {
            crate::protocol::store_commit::DeviceJoinAttempt::signed(
                store_root,
                attempt_id,
                attempt_slot,
                expected_registration,
                registration_slot,
                outcome_slot,
                bootstrap_cut,
                membership,
                provider_admin_grant,
                provider_approval,
                provider_response,
                self.registration.reference().clone(),
                owner_grant,
                self.registration.value(),
                &self.device_signer,
            )
        }

        pub(crate) fn sign_provider_admission_approval_without_shape_validation_for_test(
            &self,
            request: crate::sync::store::DeviceProviderAccessRequest,
            access_grant: crate::protocol::provider::ActivatedStoreMemberProviderAccessGrant,
            admission: crate::sync::store::DeviceProviderAdmissionChallenge,
        ) -> crate::sync::store::DeviceProviderAdmissionApproval {
            crate::sync::store::DeviceProviderAdmissionApproval::signed_without_shape_validation_for_test(
                request,
                access_grant,
                admission,
                &self.device_signer,
            )
        }

        pub(crate) fn sign_device_head_for_test(
            &self,
            store_root_hash: crate::protocol::store_commit::ObjectHash,
            commit: crate::protocol::store_commit::StoreBatchCommitRef,
            history_summary: crate::protocol::store_commit::ObjectHash,
            successor: crate::protocol::store_commit::SuccessorLink,
        ) -> Result<
            crate::protocol::store_commit::StoreDeviceHead,
            crate::protocol::store_commit::StoreProtocolError,
        > {
            crate::protocol::store_commit::StoreDeviceHead::signed(
                store_root_hash,
                self.registration.reference().clone(),
                commit,
                history_summary,
                successor,
                &self.device_signer,
            )
        }

        pub(crate) fn sign_reclaim_receipt_for_test(
            &self,
            store_root_hash: crate::protocol::store_commit::ObjectHash,
            authorization: crate::protocol::reclaim::ReclaimAuthorizationRef,
            provider_admin_state: crate::protocol::circle_control::StoreMembershipStateRef,
            provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
        ) -> Result<
            crate::protocol::reclaim::ReclaimReceipt,
            crate::protocol::store_commit::StoreProtocolError,
        > {
            crate::protocol::reclaim::ReclaimReceipt::signed(
                store_root_hash,
                authorization,
                provider_admin_state,
                provider_admin_grant,
                self.registration.reference().clone(),
                self.registration.value(),
                &self.device_signer,
            )
        }
    }

    #[derive(Clone)]
    pub(crate) struct TestDevice {
        db: crate::database::StoreDatabase,
        store: std::sync::Arc<crate::sync::store::Store>,
        _store_dir_temp: std::sync::Arc<tempfile::TempDir>,
        pub device_id: String,
        storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
        identity: UserKeypair,
    }

    impl TestDevice {
        pub(crate) async fn create(
            db: &Database,
            storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
            founder_timestamp: &str,
            identity: UserKeypair,
        ) -> Result<Self, String> {
            Self::create_with_database(
                crate::database::StoreDatabase::new(db),
                storage,
                founder_timestamp,
                identity,
            )
            .await
        }

        pub(crate) async fn create_with_database(
            database: crate::database::StoreDatabase,
            storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
            founder_timestamp: &str,
            identity: UserKeypair,
        ) -> Result<Self, String> {
            let (store_dir_temp, store_dir) = temp_store_dir();
            let initialized = crate::sync::store::Store::create(
                database.clone(),
                storage.clone(),
                store_dir,
                founder_timestamp,
                &identity,
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(initialized.store),
                _store_dir_temp: std::sync::Arc::new(store_dir_temp),
                device_id: initialized.device_id,
                storage,
                identity,
            })
        }

        pub(crate) async fn open_with_database(
            database: crate::database::StoreDatabase,
            storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
            root: &crate::protocol::store_commit::StoreRootRef,
            identity: &UserKeypair,
        ) -> Result<Self, String> {
            let (store_dir_temp, store_dir) = temp_store_dir();
            let initialized = crate::sync::store::Store::open(
                database.clone(),
                storage.clone(),
                store_dir,
                root,
                identity,
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(initialized.store),
                _store_dir_temp: std::sync::Arc::new(store_dir_temp),
                device_id: initialized.device_id,
                storage,
                identity: identity.clone(),
            })
        }

        pub(crate) async fn load(
            db: &Database,
            storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
            identity: UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreError> {
            Self::load_with_database(crate::database::StoreDatabase::new(db), storage, identity)
                .await
        }

        pub(crate) async fn load_with_database(
            database: crate::database::StoreDatabase,
            storage: std::sync::Arc<crate::storage::CloudSyncStorage>,
            identity: UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreError> {
            let (store_dir_temp, store_dir) = temp_store_dir();
            let store = crate::sync::store::Store::load(
                database.clone(),
                storage.clone(),
                store_dir,
                identity.clone(),
            )
            .await?;
            let device_id = database
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?
                .ok_or(crate::sync::store::StoreError::MissingState {
                    key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
                })?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(store),
                _store_dir_temp: std::sync::Arc::new(store_dir_temp),
                device_id,
                storage,
                identity,
            })
        }

        pub(crate) fn adopt_key_rotation(
            &self,
            encryption: &crate::encryption::EncryptionService,
            custody: &dyn crate::keys::MasterKeyCustody,
        ) -> Result<String, crate::keys::KeyError> {
            self.storage
                .adopt_key_rotation_for_test(encryption, custody)
        }

        pub(crate) fn store_root(&self) -> &crate::protocol::store_commit::StoreRootRef {
            self.store.store_root()
        }

        pub(crate) async fn authorize_writer(
            &self,
        ) -> Result<crate::sync::store::AuthorizedWriterOperation<'_>, crate::sync::store::StoreError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
        }

        pub(crate) async fn membership_for_test(
            &self,
        ) -> Result<crate::protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store.membership_for_test().await
        }

        pub(crate) async fn latest_local_store_position(
            &self,
        ) -> Result<
            Option<crate::protocol::store_commit::StoreBatchCommitRef>,
            crate::sync::store::StoreError,
        > {
            self.store.latest_local_store_position().await
        }

        pub(crate) async fn load_commit_for_test(
            &self,
            reference: &crate::protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
            crate::sync::store::StoreError,
        > {
            self.store.load_commit_for_test(reference).await
        }

        pub(crate) async fn load_membership_head_for_test(
            &self,
            reference: &crate::protocol::membership::MembershipHeadRef,
        ) -> Result<crate::protocol::membership::AuthorHead, crate::sync::store::StoreError>
        {
            self.store.load_membership_head_for_test(reference).await
        }

        pub(crate) async fn load_exact_materialized_commit(
            &self,
            stream_id: &str,
            sequence: u64,
        ) -> Result<
            Option<(
                crate::protocol::store_commit::StoreBatchCommitRef,
                crate::protocol::store_commit::VerifiedStoreBatchCommit,
            )>,
            String,
        > {
            self.store
                .load_exact_materialized_commit(stream_id, sequence)
                .await
        }

        pub(crate) fn device_join_transport(
            &self,
        ) -> crate::sync::store::owner::device_join_transport::StoreDeviceJoinTransport<'_>
        {
            self.store.device_join_transport()
        }

        pub(crate) fn circles(&self) -> crate::sync::store::owner::StoreCircleCommands<'_> {
            self.store.circles()
        }

        pub(crate) async fn circle_epoch_access(
            &self,
            circle_id: crate::protocol::circle::CircleId,
            expected_control: crate::protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<crate::protocol::circle_activation::CircleEpochAccess>,
            crate::database::DbError,
        > {
            self.store
                .circle_epoch_access(circle_id, expected_control)
                .await
        }

        pub(crate) async fn discard_blocked_write(
            &self,
            write_id: crate::WriteId,
        ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
            self.store.discard_blocked_write(write_id).await
        }

        pub(crate) async fn restore_membership(
            &self,
        ) -> Result<
            crate::sync::store::StoreRestoreMembership,
            crate::sync::store::MembershipOpsError,
        > {
            self.store.restore_membership().await
        }

        pub(crate) async fn owner_recovery_for_test(
            &self,
        ) -> Result<crate::sync::store::RestoringStore<'_>, String> {
            self.store.owner_recovery_for_test().await
        }

        pub(crate) async fn begin_device_join(
            &self,
            member_pubkey: &str,
        ) -> Result<crate::DeviceJoinOffer, crate::DeviceJoinError> {
            self.store.begin_device_join(member_pubkey).await
        }

        pub(crate) async fn begin_owner_promotion_for_device(
            &self,
            device_id: crate::StoreDeviceId,
        ) -> Result<
            crate::protocol::store_commit::OwnerPromotionRequest,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.begin_owner_promotion_for_device(device_id).await
        }

        pub(crate) async fn begin_owner_promotion(
            &self,
            member_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            crate::protocol::store_commit::OwnerPromotionRequest,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.begin_owner_promotion(member_registration).await
        }

        pub(crate) async fn accept_owner_promotion(
            &self,
            request: crate::protocol::store_commit::OwnerPromotionRequest,
        ) -> Result<
            crate::protocol::store_commit::OwnerPromotionAcceptance,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.accept_owner_promotion(request).await
        }

        pub(crate) async fn finalize_owner_promotion(
            &self,
            encryption: &crate::encryption::EncryptionService,
            acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
        ) -> Result<
            crate::protocol::circle_control::StoreMembershipStateRef,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store
                .finalize_owner_promotion(encryption, acceptance)
                .await
        }

        pub(crate) async fn blob_protection_for_test(
            &self,
            authority: &crate::protocol::blob::RowBlobAuthority,
            stored: &crate::protocol::blob::locator::StoredBlobRef,
        ) -> Result<crate::protocol::objects::BlobSpoolProtection, String> {
            self.store.blob_protection_for_test(authority, stored).await
        }

        pub(crate) async fn announcement_stream_id_for_test(
            &self,
        ) -> Result<crate::protocol::membership::AuthorStreamId, crate::sync::store::StoreError>
        {
            self.store.announcement_stream_id_for_test().await
        }

        pub(crate) async fn sign_device_head_for_test(
            &self,
            commit: crate::protocol::store_commit::StoreBatchCommitRef,
            history_summary: crate::protocol::store_commit::ObjectHash,
            successor: crate::protocol::store_commit::SuccessorLink,
        ) -> Result<crate::protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
        {
            self.store
                .sign_device_head_for_test(commit, history_summary, successor)
                .await
        }

        pub(crate) async fn owner_promotion_target_for_test(
            &self,
        ) -> Result<
            crate::protocol::store_commit::StoreDeviceRegistrationRef,
            crate::sync::store::StoreError,
        > {
            self.store.owner_promotion_target_for_test().await
        }

        pub(crate) async fn observe_excluded_candidate_head_for_test(
            &self,
            candidate: &crate::protocol::store_commit::StoreDeviceHead,
            candidate_commit: &crate::protocol::store_commit::StoreBatchCommit,
            candidate_object: &crate::protocol::objects::ExactObjectRef,
        ) -> Result<
            crate::sync::store::ExcludedCandidateHeadObservation,
            crate::sync::store::StoreError,
        > {
            self.store
                .observe_excluded_candidate_head_for_test(
                    candidate,
                    candidate_commit,
                    candidate_object,
                )
                .await
        }

        pub(crate) async fn cleanup_merge_candidate_for_test(
            &self,
            write_id: crate::WriteId,
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store.cleanup_merge_candidate_for_test(write_id).await
        }

        pub(crate) async fn resign_snapshot_meta_for_test(
            &self,
            meta: crate::protocol::store_commit::SnapshotMeta,
        ) -> Result<crate::protocol::store_commit::SnapshotMeta, crate::sync::store::StoreError>
        {
            self.store.resign_snapshot_meta_for_test(meta).await
        }

        pub(crate) async fn parse_local_snapshot_meta_for_test(
            &self,
            bytes: &[u8],
            reference: &crate::protocol::store_commit::StoreSnapshotRef,
        ) -> Result<crate::protocol::store_commit::SnapshotMeta, crate::sync::store::StoreError>
        {
            self.store
                .parse_local_snapshot_meta_for_test(bytes, reference)
                .await
        }

        pub(crate) async fn prepare_operation_plan_for_test(
            &self,
        ) -> Result<crate::sync::store::StoreOperationCommitPlan, crate::sync::store::StoreError>
        {
            self.store.prepare_operation_plan_for_test().await
        }

        pub(crate) async fn authorize_retained_outbound_for_test(
            &self,
            order: &crate::protocol::store_commit::StoreCommitOrder,
            candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
        ) -> Result<crate::sync::store::MergeOutboundAuthorization, crate::sync::store::StoreError>
        {
            self.store
                .authorize_retained_outbound_for_test(order, candidate_membership_heads)
                .await
        }

        pub(crate) async fn complete_revoke_rotation_adoption_for_test(
            &self,
            pending_rotation: &dyn crate::storage::CloudRotationAccess,
            adopted_generation: u64,
        ) -> Result<(), crate::sync::store::InviteError> {
            self.store
                .complete_revoke_rotation_adoption_for_test(pending_rotation, adopted_generation)
                .await
        }

        pub(crate) async fn retained_merge_replay_inputs_for_test(
            &self,
        ) -> Result<Vec<crate::database::OwnedVerifiedMergeMaterialization>, crate::database::DbError>
        {
            self.store.retained_merge_replay_inputs_for_test().await
        }

        pub(crate) async fn resolved_store_device_state_for_test(
            &self,
            reference: &crate::protocol::store_commit::StoreDeviceStateRef,
        ) -> Result<crate::protocol::store_commit::ResolvedStoreDeviceState, crate::database::DbError>
        {
            self.store
                .resolved_store_device_state_for_test(reference)
                .await
        }

        pub(crate) async fn retained_merge_materialization_for_test(
            &self,
            reference: crate::protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<crate::database::OwnedVerifiedMergeMaterialization, crate::database::DbError>
        {
            self.store
                .retained_merge_materialization_for_test(reference)
                .await
        }

        pub(crate) async fn prepare_conflict_resolution_plan_for_test(
            &self,
            candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .prepare_conflict_resolution_plan_for_test(candidate_membership_heads)
                .await
        }

        pub(crate) async fn load_membership_at_exact_heads_for_test(
            &self,
            heads: &[crate::protocol::membership::MembershipHeadRef],
            resolutions: &[crate::protocol::membership::StoreMembershipConflictResolutionRef],
        ) -> Result<crate::protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store
                .load_membership_at_exact_heads_for_test(heads, resolutions)
                .await
        }

        pub(crate) async fn project_membership_for_test(
            &self,
            candidate_heads: &[crate::protocol::membership::MembershipHeadRef],
        ) -> Result<crate::protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store
                .project_membership_for_test(candidate_heads)
                .await
        }

        pub(crate) async fn assert_deep_membership_projection_for_test(
            &self,
            heads: &[crate::protocol::membership::MembershipHeadRef],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .assert_deep_membership_projection_for_test(heads)
                .await
        }

        pub(crate) async fn verify_device_join_attempt_for_test(
            &self,
            reference: &crate::protocol::store_commit::DeviceJoinAttemptRef,
            owner: &crate::protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .verify_device_join_attempt_for_test(reference, owner)
                .await
        }

        pub(crate) async fn exact_next_announcement_slot_for_test(
            &self,
            registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
            registration: &crate::protocol::store_commit::StoreDeviceRegistration,
            previous: Option<&crate::protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            (
                crate::protocol::objects::ObjectSlot,
                Option<crate::protocol::store_commit::StoreDeviceHeadRef>,
            ),
            crate::sync::store::StoreError,
        > {
            self.store
                .exact_next_announcement_slot_for_test(registration_ref, registration, previous)
                .await
        }

        pub(crate) async fn load_registration_for_test(
            &self,
            reference: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            crate::protocol::store_commit::StoreDeviceRegistration,
            crate::sync::store::StoreError,
        > {
            self.store.load_registration_for_test(reference).await
        }

        pub(crate) async fn verify_snapshots_for_acknowledgement_for_test(
            &self,
            snapshots: &[crate::database::PublishedStoreSnapshot],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .verify_snapshots_for_acknowledgement_for_test(snapshots)
                .await
        }

        pub(crate) async fn open_circle_package_for_test(
            &self,
            access: &crate::protocol::circle_activation::CircleEpochAccess,
            commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
            reference: &crate::protocol::store_commit::CirclePackageRef,
        ) -> Result<Vec<u8>, crate::sync::store::StoreError> {
            self.store
                .open_circle_package_for_test(access, commit, reference)
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) async fn pull_readiness_for_test(
            &self,
            coverage: &crate::protocol::store_commit::CommitFrontier,
            frontier: &std::collections::BTreeMap<
                String,
                crate::protocol::store_commit::StoreBatchCommitRef,
            >,
            device_state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
            exclusion_freezes: &[crate::protocol::store_commit::StoreDeviceProposalAck],
            commit_ref: &crate::protocol::store_commit::StoreBatchCommitRef,
            commit: &crate::protocol::store_commit::StoreBatchCommit,
        ) -> Result<crate::sync::store::Readiness, crate::sync::store::StorePullError> {
            self.store
                .pull_readiness_for_test(
                    coverage,
                    frontier,
                    device_state,
                    exclusion_freezes,
                    commit_ref,
                    commit,
                )
                .await
        }

        pub(crate) async fn verified_merge_membership_prefix_for_test(
            &self,
            references: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
            predecessors: impl IntoIterator<Item = crate::protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            crate::sync::store::VerifiedMergeMembershipPrefix,
            crate::sync::store::StorePullError,
        > {
            self.store
                .verified_merge_membership_prefix_for_test(references, predecessors)
                .await
        }

        pub(crate) async fn retained_merge_history_frontier_for_test(
            &self,
            references: Vec<crate::protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            Vec<crate::protocol::store_commit::OpenedRetainedMergeHistorySummary>,
            crate::database::DbError,
        > {
            self.store
                .retained_merge_history_frontier_for_test(references)
                .await
        }

        pub(crate) async fn verified_circle_activation_for_test(
            &self,
            circle_id: crate::protocol::circle::CircleId,
            control: crate::protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<crate::protocol::circle_activation::VerifiedCircleReference>,
            crate::database::DbError,
        > {
            self.store
                .verified_circle_activation_for_test(circle_id, control)
                .await
        }

        pub(crate) async fn finalized_circle_close_outcome_for_test(
            &self,
            circle_id: crate::protocol::circle::CircleId,
        ) -> Result<
            crate::protocol::circle::CircleEpochCloseOutcome,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .finalized_circle_close_outcome_for_test(circle_id)
                .await
        }

        pub(crate) async fn circle_package_is_retained_for_replay_for_test(
            &self,
            target: crate::protocol::store_commit::CirclePackageRef,
            activation: crate::protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<bool, crate::database::DbError> {
            self.store
                .circle_package_is_retained_for_replay_for_test(target, activation)
                .await
        }

        pub(crate) async fn load_circle_acknowledgement_for_test(
            &self,
            reference: &crate::protocol::store_commit::CircleAckRef,
        ) -> Result<crate::protocol::store_commit::CircleAck, crate::sync::store::StoreAckError>
        {
            self.store
                .load_circle_acknowledgement_for_test(reference)
                .await
        }

        pub(crate) async fn load_applicable_circle_packages_for_test(
            &self,
            verified: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
            activations: &[crate::protocol::circle_activation::VerifiedCircleReference],
            author: &crate::protocol::store_commit::StoreDeviceRegistration,
            local_store_membership: crate::protocol::membership::LocalStoreMembership,
        ) -> Result<
            Vec<crate::sync::store::LoadedCirclePackage>,
            crate::sync::store::CirclePackageReadError,
        > {
            self.store
                .load_applicable_circle_packages_for_test(
                    verified,
                    activations,
                    author,
                    local_store_membership,
                )
                .await
        }

        pub(crate) fn protocol_root_for_test(
            &self,
        ) -> &crate::protocol::store_commit::StoreProtocolRoot {
            self.store.protocol_root_for_test()
        }

        pub(crate) async fn prepare_acknowledgement_activation_for_test(
            &self,
            acknowledgement: crate::protocol::store_commit::StoreAckRef,
            candidate: crate::protocol::prepared_commit::PreparedStoreOperationCommit,
        ) -> Result<(), crate::database::DbError> {
            self.store
                .prepare_acknowledgement_activation_for_test(acknowledgement, candidate)
                .await
        }

        pub(crate) async fn prepare_merge_history_successor_for_test(
            &self,
            verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
            recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
            evidence: crate::sync::store::MergeHistorySuccessorEvidence,
        ) -> Result<crate::sync::store::PreparedMergeHistorySuccessor, crate::sync::store::StoreError>
        {
            self.store
                .prepare_merge_history_successor_for_test(
                    verified_commit,
                    recovery_author,
                    evidence,
                )
                .await
        }

        pub(crate) async fn prepare_device_join_bootstrap_for_test(
            &self,
            coverage: &crate::protocol::store_commit::StoreHistoryCut,
            attempt_activation: &crate::protocol::store_commit::StoreBatchCommitRef,
            membership_state: &crate::protocol::circle_control::StoreMembershipStateRef,
        ) -> Result<crate::database::DeviceJoinBootstrapPlan, crate::sync::store::StoreError>
        {
            self.store
                .prepare_device_join_bootstrap_for_test(
                    coverage,
                    attempt_activation,
                    membership_state,
                )
                .await
        }

        pub(crate) async fn load_store_package_for_test(
            &self,
            reference: &crate::protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<
            Option<crate::protocol::objects::VerifiedObject<Vec<u8>>>,
            crate::sync::store::StoreError,
        > {
            self.store.load_store_package_for_test(reference).await
        }

        pub(crate) async fn load_store_ack_for_test(
            &self,
            reference: &crate::protocol::store_commit::StoreAckRef,
            registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<crate::protocol::store_commit::StoreAck, crate::sync::store::StoreError>
        {
            self.store
                .load_store_ack_for_test(reference, registration)
                .await
        }

        pub(crate) async fn load_head_for_test(
            &self,
            reference: &crate::protocol::store_commit::StoreDeviceHeadRef,
            registration: &crate::protocol::store_commit::StoreDeviceRegistration,
            commit: &crate::protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<crate::protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
        {
            self.store
                .load_head_for_test(reference, registration, commit)
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) async fn remove_member(
            &self,
            public_key_hex: &str,
            encryption: &crate::encryption::EncryptionService,
            security: &crate::store_security::StoreSecurity,
            cipher: &dyn crate::storage::CloudCipherAccess,
            pending_rotation: &dyn crate::storage::CloudRotationAccess,
        ) -> Result<String, crate::sync::store::MembershipOpsError> {
            self.store
                .remove_member(
                    public_key_hex,
                    encryption,
                    security,
                    cipher,
                    pending_rotation,
                )
                .await
        }

        pub(crate) async fn authorize_device_provider_access(
            &self,
            request: crate::sync::store::DeviceProviderAccessRequest,
            access_administrator: Option<
                &dyn crate::sync::store::DeviceProviderAccessAdministrator,
            >,
        ) -> Result<crate::sync::store::DeviceProviderAdmissionApproval, crate::DeviceJoinError>
        {
            self.store
                .authorize_device_provider_access(request, access_administrator)
                .await
        }

        pub(crate) async fn publish_device_provider_challenge(
            &self,
            bootstrap: crate::sync::store::ProvisionalDeviceBootstrap,
        ) -> Result<crate::sync::store::ProviderReadyDeviceBootstrap, crate::DeviceJoinError>
        {
            self.store
                .publish_device_provider_challenge(bootstrap)
                .await
        }

        pub(crate) async fn complete_device_provider_admission(
            &self,
            readiness: crate::sync::store::DeviceJoinReadiness,
        ) -> Result<crate::sync::store::DeviceProviderAdmissionCompletion, crate::DeviceJoinError>
        {
            self.store
                .complete_device_provider_admission(readiness)
                .await
        }

        pub(crate) async fn close_device_provider_admission(
            &self,
            cancellation: crate::sync::store::DeviceJoinCancellation,
        ) -> Result<crate::sync::store::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
            self.store
                .close_device_provider_admission(cancellation)
                .await
        }

        pub(crate) async fn revoke_device_provider_admission_writes(
            &self,
            cancellation: crate::sync::store::DeviceJoinCancellation,
            revocation_executor: &dyn crate::sync::store::DeviceJoinWriteRevocationExecutor,
        ) -> Result<crate::sync::store::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
            self.store
                .revoke_device_provider_admission_writes(cancellation, revocation_executor)
                .await
        }

        pub(crate) async fn abandon_device_join(
            &self,
            offer: crate::sync::store::DeviceJoinOffer,
        ) -> Result<crate::sync::store::DeviceJoinAbandonment, crate::DeviceJoinError> {
            self.store.abandon_device_join(offer).await
        }

        pub(crate) async fn accept_device_registration_request(
            &self,
            request: crate::sync::store::DeviceRegistrationRequest,
        ) -> Result<crate::sync::store::ProvisionalDeviceBootstrap, crate::DeviceJoinError>
        {
            self.store.accept_device_registration_request(request).await
        }

        pub(crate) async fn cancel_device_join(
            &self,
            attempt: crate::protocol::store_commit::DeviceJoinAttemptRef,
        ) -> Result<crate::sync::store::DeviceJoinCancellation, crate::DeviceJoinError> {
            self.store.cancel_device_join(attempt).await
        }

        pub(crate) async fn finalize_device_join(
            &self,
            completion: crate::sync::store::DeviceProviderAdmissionCompletion,
        ) -> Result<crate::sync::store::DeviceJoinActivation, crate::DeviceJoinError> {
            self.store.finalize_device_join(completion).await
        }

        pub(crate) async fn complete_owner_device_join_cleanup(
            &self,
            activation: crate::sync::store::DeviceJoinCleanupActivation,
        ) -> Result<crate::sync::store::DeviceJoinCleanupActivation, crate::DeviceJoinError>
        {
            self.store
                .complete_owner_device_join_cleanup(activation)
                .await
        }

        pub(crate) async fn revoke_joining_device_writes(
            &self,
            cancellation: crate::sync::store::DeviceJoinCancellation,
            revocation_executor: &dyn crate::sync::store::DeviceJoinWriteRevocationExecutor,
        ) -> Result<crate::sync::store::JoinerJoinTerminal, crate::DeviceJoinError> {
            self.store
                .revoke_joining_device_writes(cancellation, revocation_executor)
                .await
        }

        pub(crate) async fn prepare_device_join_cleanup(
            &self,
            cancellation: crate::sync::store::DeviceJoinCancellation,
            administrator_terminal: crate::sync::store::ProviderAdminJoinTerminal,
            joiner_terminal: crate::sync::store::JoinerJoinTerminal,
        ) -> Result<crate::sync::store::DeviceJoinCleanupReceipt, crate::DeviceJoinError> {
            self.store
                .prepare_device_join_cleanup(cancellation, administrator_terminal, joiner_terminal)
                .await
        }

        pub(crate) async fn activate_device_join_cleanup(
            &self,
            receipt: crate::sync::store::DeviceJoinCleanupReceipt,
        ) -> Result<crate::sync::store::DeviceJoinCleanupActivation, crate::DeviceJoinError>
        {
            self.store.activate_device_join_cleanup(receipt).await
        }

        pub(crate) async fn device_exclusion_operations_for_test(
            &self,
        ) -> Result<
            Vec<crate::sync::store::StoreDeviceExclusionOperationInfo>,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.device_exclusion_operations_for_test().await
        }

        pub(crate) async fn stage_uploaded_device_exclusion_proposal_for_test(
            &self,
        ) -> Result<
            crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store
                .stage_uploaded_device_exclusion_proposal_for_test()
                .await
        }

        pub(crate) async fn propose_device_exclusion(
            &self,
            target: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.propose_device_exclusion(target).await
        }

        pub(crate) async fn cancel_device_exclusion(
            &self,
            proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.cancel_device_exclusion(proposal).await
        }

        pub(crate) async fn finalize_device_exclusion(
            &self,
            proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.finalize_device_exclusion(proposal).await
        }

        pub(crate) async fn pending_device_join_observation_for_test(
            &self,
            pending: &crate::sync::store::DeviceJoinJournalDatabase,
            offer: &crate::sync::store::DeviceJoinOffer,
        ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, String> {
            self.store
                .pending_device_join_observation_for_test(pending, offer)
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn open_pending_device_join_for_test(
            &self,
            pending: &crate::sync::store::DeviceJoinJournalDatabase,
            identity: &UserKeypair,
            offer: crate::sync::store::DeviceJoinOffer,
        ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, String> {
            self.store
                .open_pending_device_join_for_test(pending, identity, offer)
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn prepare_snapshot_bootstrap_for_test(
            &self,
            membership_floor: &crate::joining::MembershipFloor,
            binary_schema_version: u32,
            target_path: &std::path::Path,
            restorer_identity: &UserKeypair,
        ) -> Result<
            crate::sync::store::PreparedSnapshotBootstrap<'_>,
            crate::sync::store::SnapshotError,
        > {
            self.store
                .prepare_snapshot_bootstrap_for_test(
                    membership_floor,
                    binary_schema_version,
                    target_path,
                    restorer_identity,
                )
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub(crate) async fn invite_member(
            &self,
            member_pubkey: &str,
            invitee_email: Option<&str>,
            role: crate::protocol::membership::MemberRole,
            encryption: &crate::encryption::EncryptionService,
            store_id: &str,
            store_name: &str,
        ) -> Result<crate::joining::InviteCode, crate::sync::store::MembershipOpsError> {
            self.store
                .invite_member(
                    member_pubkey,
                    invitee_email,
                    role,
                    encryption,
                    store_id,
                    store_name,
                )
                .await
        }

        pub(crate) async fn drain_uploads(
            &self,
            store_dir: &StoreDir,
            clock: &dyn crate::clock::Clock,
            routing_encryption: Option<&crate::encryption::EncryptionService>,
            observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::protocol::blob::DrainOutcome, crate::database::DbError> {
            self.store
                .with_test_store_dir(store_dir.clone())
                .authorize_writer()
                .await
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?
                .drain_uploads(clock, routing_encryption, observer)
                .await
        }

        pub(crate) async fn publish_pending_store_database(
            &self,
            store_dir: &StoreDir,
        ) -> Result<bool, String> {
            let store = self.store.with_test_store_dir(store_dir.clone());
            let mut writer = store
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?;
            let prepared = writer
                .prepare_pending_store_write()
                .await
                .map_err(|error| error.to_string())?;
            let published = writer
                .drain_store_writes()
                .await
                .map_err(|error| error.to_string())?;
            if published > 0 {
                crate::sync::test_owner_graph::local_blob_access(
                    self.db.clone(),
                    store_dir.clone(),
                )
                .drain_published_blob_drop_intents(u64::MAX)
                .await?;
                crate::database::LocalBlobCleanup::new(&self.db, store_dir)
                    .drain()
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(prepared || published > 0)
        }

        pub(crate) async fn publish_fixture_position(
            &self,
            store_dir: &StoreDir,
            note_id: &str,
        ) -> u64 {
            self.db
                .insert_fixture_position_for_test(note_id)
                .await
                .expect("insert fixture Store position");
            assert!(self
                .publish_pending_store_database(store_dir)
                .await
                .expect("publish fixture Store position"));
            self.latest_local_store_position()
                .await
                .expect("read fixture Store position")
                .expect("fixture Store write has an exact position")
                .coord
                .sequence()
        }

        pub(crate) async fn publish_exact_remote_blob_binding(
            &self,
            store_dir: &StoreDir,
            root_id: &str,
            row_id: &str,
            bytes: &[u8],
        ) -> crate::protocol::blob::locator::StoredBlobRef {
            let local = self
                .db
                .row_blob_ref("note_photos", row_id)
                .await
                .expect("load exact Local row blob reference");
            let source = store_dir
                .local_blob_path(&local.blob().namespace, &local.blob().id)
                .expect("resolve host blob source");
            crate::local_file::AtomicStagedFile::write_for_test(&source, bytes)
                .await
                .expect("write host blob source");
            crate::sync::test_owner_graph::TestOwnerGraph::new(self.db.clone(), store_dir.clone())
                .make_remote("notes", root_id, false)
                .await
                .expect("start exact make_remote");
            let clock = crate::clock::FixedClock(
                chrono::DateTime::parse_from_rfc3339("2024-06-01T01:00:00Z")
                    .expect("valid exact blob publication time")
                    .with_timezone(&chrono::Utc),
            );
            let outcome = self
                .drain_uploads(store_dir, &clock, None, None)
                .await
                .expect("drain exact blob upload");
            assert_eq!(outcome.uploaded(), 1);
            assert!(self
                .publish_pending_store_database(store_dir)
                .await
                .expect("publish exact remote blob binding"));
            self.db
                .row_blob_ref("note_photos", row_id)
                .await
                .expect("load exact Remote row blob reference")
                .stored()
                .cloned()
                .expect("Remote row owns an exact stored blob reference")
        }

        pub(crate) async fn activated_store_device_registration_for_test(
            &self,
            reference: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
            crate::database::DbError,
        > {
            self.db.activated_store_device_registration(reference).await
        }

        pub(crate) fn schema_version(&self) -> u32 {
            self.db.schema_version()
        }

        pub(crate) async fn device_authority_for_test(
            &self,
        ) -> Result<TestDeviceSigningAuthority, String> {
            let registration = self
                .db
                .activated_store_device_registration_records()
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|registration| registration.value().device_id.to_string() == self.device_id)
                .ok_or_else(|| "test device registration is not active".to_string())?;
            let device_signer = registration
                .value()
                .device_signer(&self.identity)
                .map_err(|error| error.to_string())?;
            Ok(TestDeviceSigningAuthority {
                registration,
                device_signer,
            })
        }

        pub(crate) async fn retained_merge_history_summary_for_test(
            &self,
            reference: crate::protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary, String>
        {
            Ok(self
                .db
                .retained_merge_materialization(self.store_root().clone(), reference)
                .await
                .map_err(|error| error.to_string())?
                .history_summary()
                .clone())
        }

        pub(crate) async fn publish_changeset_for_test(
            &self,
            sequence: u64,
            changeset: Vec<u8>,
            schema_version: u32,
        ) -> Result<crate::protocol::store_commit::StoreBatchCommitRef, String> {
            if schema_version != self.db.schema_version() {
                return Err(format!(
                    "test changeset schema version {schema_version} differs from producer schema {}",
                    self.db.schema_version()
                ));
            }
            let before = self
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?;
            let expected = before
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            if sequence != expected {
                return Err(format!(
                    "test producer expected sequence {expected}, got {sequence}"
                ));
            }
            self.db
                .enqueue_store_changeset_for_test(changeset)
                .await
                .map_err(|error| error.to_string())?;
            let mut writer = self
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?;
            let published = writer
                .publish_pending_store_writes()
                .await
                .map_err(|error| error.to_string())?;
            if published == 0 {
                return Err("test changeset did not prepare a Store commit".to_string());
            }
            writer
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "published test changeset has no Store position".to_string())
        }

        pub(crate) async fn publish_changeset_after_for_test(
            &self,
            store_dir: &StoreDir,
            changeset: Vec<u8>,
            previous_sequence: u64,
        ) -> Result<crate::protocol::store_commit::StoreBatchCommitRef, String> {
            let before = self
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?;
            let actual_previous_sequence = before
                .as_ref()
                .map_or(0, |position| position.coord.sequence());
            if actual_previous_sequence != previous_sequence {
                return Err(format!(
                    "Store position is {actual_previous_sequence}, expected {previous_sequence}"
                ));
            }
            self.db
                .enqueue_store_changeset_for_test(changeset)
                .await
                .map_err(|error| error.to_string())?;
            let store = self.store.with_test_store_dir(store_dir.clone());
            let mut writer = store
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?;
            if !writer
                .prepare_pending_store_write()
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("test changeset did not prepare a Store commit".to_string());
            }
            writer
                .drain_store_writes()
                .await
                .map_err(|error| error.to_string())?;
            writer
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "published test changeset has no Store position".to_string())
        }

        pub(crate) async fn create_exact_opaque_blob(
            &self,
            namespace: &str,
            id: &str,
            bytes: &[u8],
        ) -> crate::protocol::blob::locator::StoredBlobRef {
            let registration = self
                .db
                .local_blob_write_authority()
                .await
                .expect("load exact blob write authority");
            let authority = crate::protocol::objects::BlobWriteAuthority::new(&registration);
            let protection = crate::encryption::EncryptionService::from_key([42; 32]);
            let locator = crate::protocol::blob::locator::BlobLocator::opaque(
                namespace,
                id,
                authority.reference.clone(),
                crate::protocol::blob::locator::RemoteAudience::Store,
                crate::protocol::blob::BlobScope::Master,
                protection.seal_key_fingerprint(),
                bytes.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(bytes),
            )
            .expect("build exact blob locator");
            let temp = tempfile::tempdir().expect("create exact blob spool directory");
            let plaintext = temp.path().join("plaintext");
            let spool = temp.path().join("stored");
            crate::local_file::AtomicStagedFile::write_for_test(&plaintext, bytes)
                .await
                .expect("write exact blob plaintext");
            let slot = self
                .storage
                .allocate_blob_slot(&locator, &authority)
                .await
                .expect("allocate exact blob slot");
            self.storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    crate::protocol::objects::BlobSpoolProtection::Opaque(protection),
                    &plaintext,
                    &spool,
                )
                .await
                .expect("seal exact blob");
            let stored = self
                .storage
                .prepare_blob_object(&locator, &authority, slot, &spool)
                .await
                .expect("prepare exact blob object");
            self.storage
                .create_blob_object_from_file(
                    &stored,
                    &authority,
                    &spool,
                    &crate::storage::cloud::no_progress(),
                )
                .await
                .expect("create exact blob object");
            stored
        }

        pub(crate) async fn create_exact_browsable_blob(
            &self,
            namespace: &str,
            id: &str,
            cloud_path: &str,
            bytes: &[u8],
        ) -> crate::protocol::blob::locator::StoredBlobRef {
            let registration = self
                .db
                .local_blob_write_authority()
                .await
                .expect("load browsable blob write authority");
            let authority = crate::protocol::objects::BlobWriteAuthority::new(&registration);
            let locator = crate::protocol::blob::locator::BlobLocator::browsable(
                namespace,
                id,
                authority.reference.clone(),
                cloud_path,
                bytes.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(bytes),
            )
            .expect("build browsable blob locator");
            let temp = tempfile::tempdir().expect("create browsable blob spool directory");
            let plaintext = temp.path().join("plaintext");
            let spool = temp.path().join("stored");
            crate::local_file::AtomicStagedFile::write_for_test(&plaintext, bytes)
                .await
                .expect("write browsable blob plaintext");
            let slot = self
                .storage
                .allocate_blob_slot(&locator, &authority)
                .await
                .expect("allocate browsable blob slot");
            self.storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    crate::protocol::objects::BlobSpoolProtection::Browsable,
                    &plaintext,
                    &spool,
                )
                .await
                .expect("stage browsable blob");
            let stored = self
                .storage
                .prepare_blob_object(&locator, &authority, slot, &spool)
                .await
                .expect("prepare browsable blob object");
            self.storage
                .create_blob_object_from_file(
                    &stored,
                    &authority,
                    &spool,
                    &crate::storage::cloud::no_progress(),
                )
                .await
                .expect("create browsable blob object");
            stored
        }

        pub(crate) async fn run_cycle(
            &self,
            store_dir: &StoreDir,
            observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        {
            self.run_cycle_with(&crate::clock::SystemClock, None, store_dir, observer)
                .await
        }

        pub(crate) async fn run_cycle_with(
            &self,
            clock: &dyn crate::clock::Clock,
            security: Option<&dyn crate::sync::RotationKeyAdoption>,
            store_dir: &StoreDir,
            observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        {
            self.run_cycle_with_storage(
                self.store.clone(),
                self.storage.clone(),
                clock,
                security,
                store_dir,
                observer,
            )
            .await
        }

        pub(crate) async fn run_cycle_with_interceptor<I>(
            &self,
            clock: &dyn crate::clock::Clock,
            security: Option<&dyn crate::sync::RotationKeyAdoption>,
            store_dir: &StoreDir,
            observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
            interceptor: I,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        where
            I: super::StorageInterceptor + 'static,
        {
            let storage = std::sync::Arc::new(super::InterceptedStorage::new(
                self.storage.clone(),
                interceptor,
            ));
            let store_storage: std::sync::Arc<dyn crate::storage::SyncStorage> = storage.clone();
            let store = std::sync::Arc::new(self.store.with_test_storage(store_storage));
            self.run_cycle_with_storage(store, storage, clock, security, store_dir, observer)
                .await
        }

        async fn run_cycle_with_storage<S>(
            &self,
            store: std::sync::Arc<crate::sync::store::Store>,
            storage: std::sync::Arc<S>,
            clock: &dyn crate::clock::Clock,
            security: Option<&dyn crate::sync::RotationKeyAdoption>,
            store_dir: &StoreDir,
            observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        where
            S: crate::sync::cycle::SyncCycleStorage + 'static,
        {
            let local_blob_access = crate::sync::test_owner_graph::local_blob_access(
                self.db.clone(),
                store_dir.clone(),
            );
            let store = std::sync::Arc::new(store.with_test_store_dir(store_dir.clone()));
            let components = crate::sync::cycle::SyncComponents::from_retained_test_device(
                store,
                self.db.clone(),
                local_blob_access,
                storage,
                self.storage.store_id().to_string(),
                self.device_id.clone(),
            );
            components.run_cycle(clock, security, observer).await
        }

        pub(crate) fn current_encryption_for_test(
            &self,
        ) -> Option<crate::encryption::EncryptionService> {
            self.storage.current_encryption()
        }

        pub(crate) fn mark_rotation_committed_for_test(
            &self,
            generation: u64,
        ) -> Result<(), String> {
            self.storage.mark_rotation_committed_for_test(generation)
        }

        pub(crate) fn pending_rotation_generation_for_test(&self) -> Option<u64> {
            self.storage.pending_rotation_generation_for_test()
        }

        pub(crate) fn clear_rotation_gate_for_test(&self) {
            self.storage.clear_rotation_gate_for_test();
        }

        pub(crate) async fn create_circle(
            &self,
            metadata_stamp: &str,
            name: &str,
        ) -> Result<crate::CircleId, crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .create_circle(metadata_stamp, name)
                .await
        }

        pub(crate) async fn prepare_circle_operation(
            &self,
            metadata_stamp: &str,
            name: &str,
        ) -> Result<
            crate::protocol::circle_journal::CircleOperationJournal,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .prepare_create_for_test(metadata_stamp, name)
                .await
        }

        pub(crate) async fn publish_circle_epoch_close_response(
            &self,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .publish_circle_epoch_close_responses()
                .await
        }

        pub(crate) async fn publish_circle_operation(
            &self,
            operation_id: &crate::protocol::circle::CircleOperationId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            let routing_key = crate::protocol::circle::derive_row_routing_key(
                &crate::encryption::EncryptionService::from_key([42; 32]),
                self.store.store_root().store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .publish_prepared_operation_for_test(operation_id, Some(&routing_key))
                .await
        }

        pub(crate) async fn resume_circle_operations(
            &self,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            let routing_key = crate::protocol::circle::derive_row_routing_key(
                &crate::encryption::EncryptionService::from_key([42; 32]),
                self.store.store_root().store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .resume_circle_operations(Some(&routing_key))
                .await
        }

        pub(crate) async fn retry_circle_operation(
            &self,
            operation_id: &crate::protocol::circle::CircleOperationId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .retry_circle_operation(
                    operation_id,
                    Some(&crate::encryption::EncryptionService::from_key([42; 32])),
                )
                .await
        }

        pub(crate) async fn prepare_pending_store_write(
            &self,
            store_dir: &StoreDir,
        ) -> Result<bool, crate::sync::store::StoreError> {
            self.store
                .with_test_store_dir(store_dir.clone())
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .prepare_pending_store_write()
                .await
        }

        #[cfg(test)]
        pub(crate) async fn prepare_blocked_transfer_candidate(
            &self,
            label: &str,
        ) -> (tempfile::TempDir, StoreDir, crate::WriteId) {
            let statement = format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{label}', 'pending', NULL, 1, \
                     '0000000002000-0000-{label}', '2026-07-18')"
            );
            self.db
                .run_host_store_write_for_test(None, None, move |transaction| {
                    transaction
                        .execute_batch(&statement)
                        .map_err(crate::database::DbError::from)
                })
                .await
                .expect("capture transfer candidate host write");
            let (temporary, store_dir) = temp_store_dir();
            assert!(self
                .prepare_pending_store_write(&store_dir)
                .await
                .expect("prepare transfer candidate"));
            let candidate = self
                .db
                .oldest_prepared_store_write()
                .await
                .expect("load transfer candidate")
                .expect("transfer candidate exists");
            let write_id = candidate.commit.value.write_id.clone();
            self.db
                .set_write_status(
                    &write_id,
                    crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                        reason: "exercise restored author-exclusion evidence".to_string(),
                    }),
                )
                .await
                .expect("block transfer candidate");
            (temporary, store_dir, write_id)
        }

        #[cfg(test)]
        pub(crate) async fn prepare_store_operation_plan_for_test(
            &self,
        ) -> Result<crate::sync::store::StoreOperationCommitPlan, crate::sync::store::StoreError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .prepare_plan()
                .await
        }

        pub(crate) async fn drain_store_writes(
            &self,
        ) -> Result<u64, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .drain_store_writes()
                .await
        }

        pub(crate) async fn reclaim_packages(
            &self,
        ) -> Result<crate::sync::store::StoreReclaimResult, crate::sync::store::StoreReclaimError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreReclaimError::Authorization(error.to_string())
                })?
                .reclaim_packages()
                .await
        }

        pub(crate) async fn abandon_merge_candidate(
            &self,
            write_id: crate::WriteId,
        ) -> Result<crate::sync::store::MergeCandidateAbandonment, crate::sync::store::StoreError>
        {
            self.store.abandon_merge_candidate(write_id).await
        }

        pub(crate) async fn prepare_merge_candidate_abandonment(
            &self,
            write_id: crate::WriteId,
        ) -> Result<bool, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                })?
                .prepare_merge_candidate_abandonment(write_id)
                .await
        }

        pub(crate) async fn prepare_peer_exclusion(
            &self,
            target: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> crate::protocol::store_commit::StoreDeviceExclusionProposalRef {
            let proposal = match self
                .propose_device_exclusion(target)
                .await
                .expect("propose peer exclusion")
            {
                crate::sync::store::StoreDeviceExclusionResult::ProposalActivated {
                    proposal,
                    ..
                } => proposal,
                result => panic!("unexpected exclusion proposal result: {result:?}"),
            };
            let freezes = self
                .db
                .store_device_exclusion_freezes()
                .await
                .expect("read owner exclusion freeze");
            assert_eq!(freezes.len(), 1);
            assert_eq!(freezes[0].proposal, proposal);
            assert_eq!(&freezes[0].proposal.target, target);
            let frontier = crate::protocol::store_commit::CommitFrontier::from_refs(
                self.db
                    .materialized_frontier()
                    .await
                    .expect("read owner exclusion frontier"),
            )
            .expect("shape owner exclusion frontier");
            let acknowledgement = self
                .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
                .await
                .expect("stage owner exclusion acknowledgement");
            let crate::protocol::store_commit::StoreAckExclusionState { proposal_freezes } =
                acknowledgement.exclusions;
            assert_eq!(proposal_freezes, freezes);
            assert_eq!(
                self.drain_acknowledgements()
                    .await
                    .expect("publish owner exclusion acknowledgement"),
                1
            );
            proposal
        }

        pub(crate) async fn activate_peer_exclusion(
            &self,
            proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> crate::protocol::store_commit::StoreDeviceExclusionRef {
            let result = self
                .finalize_device_exclusion(proposal)
                .await
                .expect("finalize peer exclusion");
            let crate::sync::store::StoreDeviceExclusionResult::OutcomeActivated {
                outcome:
                    crate::protocol::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(exclusion),
                ..
            } = result
            else {
                panic!("unexpected exclusion result: {result:?}")
            };
            assert!(self
                .db
                .store_device_exclusion_freezes()
                .await
                .expect("read released owner exclusion freeze")
                .is_empty());
            exclusion
        }

        pub(crate) async fn finalize_peer_exclusion(
            &self,
            target: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> crate::protocol::store_commit::StoreDeviceExclusionRef {
            let proposal = self.prepare_peer_exclusion(target).await;
            self.activate_peer_exclusion(&proposal).await
        }

        pub(crate) async fn prepare_circle_object(
            &self,
            context: &crate::protocol::objects::ProtocolObjectContext,
            semantic_prefix: &str,
            extension: &str,
            bytes: Vec<u8>,
        ) -> Result<
            crate::protocol::objects::PreparedExactObject,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .prepare_circle_object_for_test(context, semantic_prefix, extension, bytes)
                .await
        }

        pub(crate) async fn prepare_circle_object_at(
            &self,
            context: &crate::protocol::objects::ProtocolObjectContext,
            slot: crate::protocol::objects::ObjectSlot,
            semantic_prefix: &str,
            bytes: Vec<u8>,
        ) -> Result<
            crate::protocol::objects::PreparedExactObject,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .prepare_circle_object_at_for_test(context, slot, semantic_prefix, bytes)
        }

        pub(crate) async fn prepare_circle_activation_objects(
            &self,
            draft: crate::protocol::circle::CircleTransitionDraft,
            history: &crate::sync::store::CircleTransitionHistory,
            candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        ) -> Result<
            (
                crate::protocol::circle::PreparedCircleTransition,
                crate::protocol::store_commit::CircleActivationObjects,
                std::collections::BTreeMap<String, crate::protocol::objects::PreparedExactObject>,
                Option<crate::protocol::objects::ExactObjectRef>,
                Vec<crate::protocol::store_commit::StreamActivation>,
            ),
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .prepare_circle_activation_objects_for_test(draft, history, candidate_family)
                .await
        }

        pub(crate) async fn sign_circle_commit(
            &self,
            old_commit: &crate::protocol::store_commit::StoreBatchCommit,
            coord: crate::protocol::store_commit::StoreCommitCoord,
            reference: crate::protocol::store_commit::CircleControlRef,
            stream_activations: Vec<crate::protocol::store_commit::StreamActivation>,
        ) -> Result<
            crate::protocol::store_commit::StoreBatchCommit,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::CircleOperationError::InvalidState(error.to_string())
                })?
                .circles()
                .sign_circle_commit_for_test(old_commit, coord, reference, stream_activations)
        }

        pub(crate) async fn rename_circle(
            &self,
            metadata_stamp: &str,
            circle_id: crate::CircleId,
            name: &str,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .rename_circle(metadata_stamp, circle_id, name)
                .await
        }

        pub(crate) async fn delete_circle(
            &self,
            circle_id: crate::CircleId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store.circles().delete_circle(circle_id).await
        }

        pub(crate) async fn load_circle_activations(
            &self,
            commit_ref: &crate::protocol::store_commit::StoreBatchCommitRef,
            commit: &crate::protocol::store_commit::StoreBatchCommit,
            author: &crate::protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<
            crate::protocol::circle_activation::VerifiedCircleActivations,
            crate::sync::store::CircleOperationError,
        > {
            let routing_key = crate::protocol::circle::derive_row_routing_key(
                &crate::encryption::EncryptionService::from_key([42; 32]),
                commit.store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.store
                .load_circle_activations_for_test(commit_ref, commit, author, Some(&routing_key))
                .await
        }

        pub(crate) async fn circle_blob_opening_error(
            &self,
            authority: &crate::protocol::blob::RowBlobAuthority,
            stored: &crate::protocol::blob::locator::StoredBlobRef,
        ) -> String {
            match self.store.blob_protection_for_test(authority, stored).await {
                Ok(_) => panic!("invalid Circle blob authority must fail"),
                Err(error) => error,
            }
        }

        pub(crate) async fn load_circle_snapshot_refs(
            &self,
            circle_id: crate::CircleId,
            access: &crate::protocol::circle_activation::CircleEpochAccess,
        ) -> Result<
            Vec<(
                crate::protocol::store_commit::CircleSnapshotRef,
                crate::protocol::store_commit::CircleSnapshotMeta,
            )>,
            String,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| error.to_string())?
                .circles()
                .snapshots()
                .load_circle_snapshot_refs_for_test(circle_id, access)
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn membership(
            &self,
        ) -> Result<crate::protocol::membership::MembershipChain, String> {
            self.store
                .membership_for_test()
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) fn protocol_root(&self) -> &crate::protocol::store_commit::StoreProtocolRoot {
            self.store.protocol_root_for_test()
        }

        #[cfg(test)]
        pub(crate) async fn prepare_wrapped_key(
            &self,
            recipient: &str,
            value: crate::protocol::wrapped_store_key::WrappedStoreKey,
        ) -> Result<crate::protocol::wrapped_store_key::PreparedWrappedStoreKey, String> {
            self.store
                .prepare_wrapped_key_for_test(recipient, value)
                .await
        }

        #[cfg(test)]
        pub(crate) async fn open_membership_keyring(
            &self,
        ) -> Result<crate::encryption::EncryptionService, String> {
            self.store.open_membership_keyring_for_test().await
        }

        pub(crate) async fn publish_snapshot(
            &self,
            db_image: Vec<u8>,
            coverage: crate::protocol::store_commit::CommitFrontier,
        ) -> Result<crate::protocol::store_commit::SnapshotMeta, String> {
            self.publish_snapshot_at(db_image, coverage, "2026-07-16T00:00:00Z")
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn publish_snapshot_at(
            &self,
            db_image: Vec<u8>,
            coverage: crate::protocol::store_commit::CommitFrontier,
            created_at: &str,
        ) -> Result<crate::protocol::store_commit::SnapshotMeta, crate::sync::store::SnapshotError>
        {
            self.store
                .publish_snapshot_for_test(
                    crate::database::CreatedSnapshot {
                        db_image,
                        blobs: Vec::new(),
                    },
                    coverage,
                    created_at.to_string(),
                )
                .await
        }

        pub(crate) async fn resume_snapshot_publication(
            &self,
        ) -> Result<
            Option<crate::protocol::store_commit::SnapshotMeta>,
            crate::sync::store::SnapshotError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(|error| {
                    crate::sync::store::SnapshotError::PublicationState(error.to_string())
                })?
                .resume_snapshot_publication()
                .await
        }

        pub(crate) async fn publish_acknowledgement(
            &self,
            frontier: crate::protocol::store_commit::CommitFrontier,
        ) -> Result<(), String> {
            self.store
                .stage_acknowledgement_for_test(frontier, "2026-07-16T00:00:01Z".to_string())
                .await
                .map_err(|error| error.to_string())?;
            let published = self
                .store
                .drain_acknowledgements_for_test()
                .await
                .map_err(|error| error.to_string())?;
            if published != 1 {
                return Err(format!(
                "snapshot acknowledgement fixture published {published} acknowledgements instead of one"
            ));
            }
            Ok(())
        }

        pub(crate) async fn stage_acknowledgement(
            &self,
            frontier: crate::protocol::store_commit::CommitFrontier,
            sync_time: String,
        ) -> Result<crate::protocol::store_commit::StoreAck, String> {
            self.store
                .stage_acknowledgement_for_test(frontier, sync_time)
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn materialized_frontier(
            &self,
        ) -> Result<
            std::collections::BTreeMap<String, crate::protocol::store_commit::StoreBatchCommitRef>,
            String,
        > {
            self.db
                .materialized_frontier()
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn drain_acknowledgements(&self) -> Result<u64, String> {
            self.store
                .drain_acknowledgements_for_test()
                .await
                .map_err(|error| error.to_string())
        }

        #[cfg(test)]
        pub(crate) async fn stage_acknowledgement_exact(
            &self,
            frontier: crate::protocol::store_commit::CommitFrontier,
            sync_time: String,
        ) -> Result<crate::protocol::store_commit::StoreAck, crate::sync::store::StoreAckError>
        {
            self.store
                .stage_acknowledgement_for_test(frontier, sync_time)
                .await
        }

        #[cfg(test)]
        pub(crate) async fn acknowledgement_frontier(
            &self,
        ) -> Result<crate::protocol::store_commit::CommitFrontier, crate::sync::store::StoreAckError>
        {
            crate::protocol::store_commit::CommitFrontier::from_refs(
                self.db.materialized_frontier().await?,
            )
            .map_err(|error| crate::sync::store::StoreAckError::Database(error.to_string()))
        }

        #[cfg(test)]
        pub(crate) async fn stage_current_acknowledgement(
            &self,
            sync_time: &str,
        ) -> Result<crate::protocol::store_commit::StoreAck, crate::sync::store::StoreAckError>
        {
            let frontier = self.acknowledgement_frontier().await?;
            self.stage_acknowledgement_exact(frontier, sync_time.to_string())
                .await
        }

        #[cfg(test)]
        pub(crate) fn typed_device_id(&self) -> crate::protocol::store_commit::StoreDeviceId {
            self.device_id
                .parse()
                .expect("TestDevice retains a valid Store device id")
        }

        #[cfg(test)]
        pub(crate) async fn prepare_acknowledgement_candidate_for_test(
            &self,
            outbound: &crate::database::OutboundStoreAck,
        ) -> crate::protocol::prepared_commit::PreparedStoreOperationCommit {
            let mut writer = self
                .authorize_writer()
                .await
                .expect("authorize acknowledgement writer");
            let plan = writer
                .prepare_plan()
                .await
                .expect("prepare acknowledgement activation");
            plan.common()
                .validate_acknowledgement(&outbound.ack.value)
                .expect("acknowledgement matches activation predecessor");
            let candidate = writer
                .prepare_candidate(
                    plan,
                    crate::sync::store::StoreOperationBatch::Acknowledgement {
                        reference: outbound.reference.clone(),
                        value: outbound.ack.value.clone(),
                        circle_acknowledgements: Vec::new(),
                    },
                )
                .await
                .expect("prepare acknowledgement candidate");
            self.prepare_acknowledgement_activation_for_test(
                outbound.reference.clone(),
                candidate.clone(),
            )
            .await
            .expect("persist acknowledgement candidate");
            candidate
        }

        #[cfg(test)]
        pub(crate) async fn drain_acknowledgements_exact(
            &self,
        ) -> Result<u64, crate::sync::store::StoreAckError> {
            self.store.drain_acknowledgements_for_test().await
        }

        #[cfg(test)]
        pub(crate) async fn stage_circle_acknowledgements(
            &self,
            frontier: &crate::protocol::store_commit::CommitFrontier,
            sync_time: &str,
        ) -> Result<(), crate::sync::store::StoreAckError> {
            self.store
                .stage_circle_acknowledgements_for_test(frontier, sync_time)
                .await
        }

        pub(crate) async fn load_commit_ancestry_until(
            &self,
            start: crate::protocol::store_commit::StoreBatchCommitRef,
            coverage: &crate::protocol::store_commit::CommitFrontier,
        ) -> Result<
            Vec<(
                crate::protocol::store_commit::StoreBatchCommitRef,
                crate::protocol::store_commit::VerifiedStoreBatchCommit,
            )>,
            String,
        > {
            self.store
                .load_commit_ancestry_until_for_test(start, coverage)
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn export_activated_device_continuation(
            &self,
        ) -> Result<crate::restoration::ActivatedContinuation, String> {
            self.store
                .export_activated_device_continuation_for_test()
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn latest_store_position(
            &self,
        ) -> Result<Option<crate::protocol::store_commit::StoreBatchCommitRef>, String> {
            self.store
                .latest_local_store_position()
                .await
                .map_err(|error| error.to_string())
        }

        pub(crate) async fn pull_store(
            &self,
            store_dir: &StoreDir,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            crate::sync::store::StorePullError,
        > {
            let routing_encryption = crate::encryption::EncryptionService::from_key([42; 32]);
            self.pull_store_with_encryption(store_dir, &routing_encryption)
                .await
        }

        pub(crate) async fn pull_store_with_encryption(
            &self,
            store_dir: &StoreDir,
            routing_encryption: &crate::encryption::EncryptionService,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            crate::sync::store::StorePullError,
        > {
            let store = self.store.with_test_store_dir(store_dir.clone());
            let mut authorization = store
                .authorize_writer()
                .await
                .map_err(|error| crate::sync::store::StorePullError::Database(error.to_string()))?;
            let result = authorization
                .pull(Some(routing_encryption))
                .await
                .map_err(|error| crate::sync::store::StorePullError::Database(error.to_string()))?;
            let sequences = result
                .frontier
                .iter()
                .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
                .collect();
            Ok((sequences, result))
        }
    }
}

pub(crate) use test_device::{TestDevice, TestDeviceSigningAuthority};

struct TestStoreProducers {
    unassigned: Option<TestDevice>,
    by_name: HashMap<String, TestDevice>,
}

impl TestStore {
    pub(crate) async fn bind_founder_device(
        &self,
        database: &Database,
    ) -> Result<TestDevice, String> {
        self.bind_device(database, &self.signer).await
    }

    pub(crate) async fn open_store_with_identity(
        &self,
        database: &Database,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store::Store, String> {
        self.open_store_with_storage(
            crate::database::StoreDatabase::new(database),
            self.storage.clone(),
            store_dir,
            identity,
        )
        .await
    }

    pub(crate) async fn open_store_with_storage(
        &self,
        database: crate::database::StoreDatabase,
        storage: Arc<dyn crate::storage::SyncStorage>,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store::Store, String> {
        crate::sync::store::Store::open(database, storage, store_dir, &self.root, identity)
            .await
            .map(|initialized| initialized.store)
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn open_founder_store_with_storage(
        &self,
        database: crate::database::StoreDatabase,
        storage: Arc<dyn crate::storage::SyncStorage>,
        store_dir: StoreDir,
    ) -> Result<crate::sync::store::Store, String> {
        self.open_store_with_storage(database, storage, store_dir, &self.signer)
            .await
    }

    pub(crate) fn tombstone_deletions(&self) -> Vec<String> {
        self.home.deletes_seen()
    }

    pub(crate) fn stored_tombstone_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.home.get(key)
    }

    pub(crate) async fn plant_tombstone_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.write_blob_tombstone(key, bytes).await
    }

    /// Plants a typed tombstone through the exact plaintext Store layout while
    /// bypassing the signing drain, so deletion tests can exercise rejected
    /// signatures and Store identities.
    pub(crate) async fn plant_tombstone(&self, tombstone: &crate::blob::delete::BlobTombstoneJson) {
        let key = exact_tombstone_key(&tombstone.stored);
        let bytes = serde_json::to_vec(tombstone).expect("serialize tombstone");
        self.plant_tombstone_bytes(&key, bytes)
            .await
            .expect("plant tombstone");
    }

    pub(crate) fn fail_exact_delete_on_call(&self, call: usize) {
        self.home.fail_exact_delete_on_call(call);
    }

    pub(crate) fn fail_nth_exact_delete_of(
        &self,
        slots: &[&crate::protocol::objects::ObjectSlot],
        call: usize,
    ) {
        self.home.fail_nth_exact_delete_of(slots, call);
    }

    pub(crate) fn sort_provider_listings(&self) {
        self.home.sort_listings();
    }

    pub(crate) fn provider_object_is_absent(&self, logical_key: &str) -> bool {
        self.home.get(logical_key).is_none()
    }

    pub(crate) fn arm_provider_write_failures(&self) {
        self.home.arm_write_failures();
    }

    pub(crate) fn fail_exact_create_before_call(&self, call: usize) {
        self.home.fail_exact_create_before_call(call);
    }

    pub(crate) fn fail_exact_create_after_call(&self, call: usize) {
        self.home.fail_exact_create_after_call(call);
    }

    pub(crate) fn pause_after_exact_create_call(
        &self,
        call: usize,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.home.pause_after_exact_create_call(call)
    }

    pub(crate) async fn pull_with_storage_for_test(
        &self,
        database: &Database,
        storage: Arc<dyn crate::storage::SyncStorage>,
        store_dir: &StoreDir,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, crate::sync::cycle::SyncCycleFailure> {
        let store = crate::sync::store::Store::load(
            crate::database::StoreDatabase::new(database),
            storage,
            store_dir.clone(),
            self.signer.clone(),
        )
        .await
        .map_err(|error| crate::sync::cycle::SyncCycleFailure::from(error.to_string()))?;
        store
            .authorize_writer()
            .await
            .map_err(|error| crate::sync::cycle::SyncCycleFailure::from(error.to_string()))?
            .pull(routing_encryption)
            .await
    }

    pub(crate) async fn founder_recovery_authority(
        &self,
    ) -> crate::restoration::OwnerRecoveryAuthority {
        let device = self.founder_device().await.expect("load founder Store");
        let protocol_root = device.protocol_root_for_test();
        let owner_grant = protocol_root.descriptor.founder_grant.clone();
        let activation = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
            &self.root,
            &crate::keys::public_key_hex(&self.signer),
            &owner_grant,
            &protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        crate::restoration::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(self.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: crate::protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: crate::protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                    activation,
                },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        }
    }

    #[cfg(test)]
    pub(crate) fn install_cross_principal_device<'a>(
        &'a self,
        local_database: crate::database::StoreDatabase,
        identity: &'a UserKeypair,
        peer_account_id: &'a str,
        published_at: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
        Box::pin(async move {
            let observer = self.founder.clone();
            let provider_binding = crate::storage::SyncStorage::provider_binding(&*self.storage)
                .await
                .map_err(|error| error.to_string())?;
            let crate::protocol::objects::StoreProviderBinding::Dropbox { namespace_id } =
                &provider_binding.store
            else {
                return Err("cross-principal test Store is not Dropbox".to_string());
            };
            let namespace_id = namespace_id.clone();
            let peer_binding = crate::protocol::objects::ResolvedProviderBinding {
                store: provider_binding.store.clone(),
                device: crate::protocol::objects::ProviderDeviceBinding {
                    principal: crate::protocol::objects::ProviderPrincipalId::Dropbox {
                        account_id: peer_account_id.to_string(),
                    },
                },
            };
            let peer_home = std::sync::Arc::new(
                self.home
                    .as_ref()
                    .clone()
                    .with_provider_binding(peer_binding),
            );
            let peer_storage: std::sync::Arc<dyn crate::storage::SyncStorage> = std::sync::Arc::new(
                crate::storage::CloudSyncStorage::new(
                    peer_home.clone(),
                    crate::storage::CloudCipher::Encrypted(
                        crate::encryption::EncryptionService::from_key([42; 32]),
                    ),
                    crate::storage::BlobPathScheme::Hashed,
                    "cross-principal-test-store",
                    identity.clone(),
                )
                .map_err(|error| error.to_string())?,
            );
            let pending_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
            let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
                pending_dir.path().join("pending-device-join.sqlite"),
            )
            .map_err(|error| error.to_string())?;
            let offer = observer
                .begin_device_join(&pubkey_hex(identity))
                .await
                .map_err(|error| error.to_string())?;
            let join_history =
                crate::sync::store::HistoryConstructionAuthority::for_pending_device_join()
                    .open_pinned(peer_storage.as_ref(), &offer.store_root)
                    .await
                    .map_err(|error| error.to_string())?;
            let observation = crate::sync::store::PendingDeviceJoinObservation::new(
                &pending,
                &peer_storage,
                join_history,
                offer.attempt_id,
            );
            let mut pending_join =
                crate::sync::store::PendingDeviceJoinAuthority::open(observation, identity, offer)
                    .await
                    .map_err(|error| error.to_string())?;
            let access_request = pending_join
                .prepare_provider_access_request()
                .await
                .map_err(|error| error.to_string())?;
            let access_administrator = TestDropboxAccessAdministrator { namespace_id };
            let approval = observer
                .authorize_device_provider_access(access_request, Some(&access_administrator))
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                approval.admission,
                crate::sync::store::DeviceProviderAdmissionChallenge::CrossPrincipal(_)
            ) {
                return Err(
                    "distinct provider principals produced same-principal admission".into(),
                );
            }
            let registration_request = pending_join
                .prepare_registration_request(approval)
                .await
                .map_err(|error| error.to_string())?;
            let provisional = observer
                .accept_device_registration_request(registration_request)
                .await
                .map_err(|error| error.to_string())?;
            let provider_ready = observer
                .publish_device_provider_challenge(provisional)
                .await
                .map_err(|error| error.to_string())?;
            let (_store_dir_temp, store_dir) = temp_store_dir();
            let mut joining = pending_join
                .begin_joining_store(local_database, &store_dir)
                .await
                .map_err(|error| error.to_string())?;
            let readiness = joining
                .bootstrap(provider_ready, published_at)
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                readiness.provider,
                crate::sync::store::DeviceProviderReadiness::CrossPrincipal(_)
            ) {
                return Err(
                    "distinct provider principals produced same-principal readiness".into(),
                );
            }
            let completion = observer
                .complete_device_provider_admission(readiness)
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(
                completion.admission,
                crate::sync::store::DeviceProviderAdmission::CrossPrincipal(_)
            ) {
                return Err(
                    "distinct provider principals produced same-principal completion".into(),
                );
            }
            let activation = observer
                .finalize_device_join(completion)
                .await
                .map_err(|error| error.to_string())?;
            joining
                .complete(activation)
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        })
    }

    pub(crate) async fn run_founder_cycle(
        &self,
        store_dir: &StoreDir,
        observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
    ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure> {
        self.founder.run_cycle(store_dir, observer).await
    }

    pub(crate) async fn publish_fixture_position(
        &self,
        store_dir: &StoreDir,
        note_id: &str,
    ) -> u64 {
        self.founder
            .publish_fixture_position(store_dir, note_id)
            .await
    }

    pub(crate) async fn create_exact_opaque_blob(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
    ) -> crate::protocol::blob::locator::StoredBlobRef {
        self.founder
            .create_exact_opaque_blob(namespace, id, bytes)
            .await
    }

    pub(crate) async fn create_exact_browsable_blob(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> crate::protocol::blob::locator::StoredBlobRef {
        self.founder
            .create_exact_browsable_blob(namespace, id, cloud_path, bytes)
            .await
    }

    pub(crate) async fn publish_exact_remote_blob_binding(
        &self,
        store_dir: &StoreDir,
        root_id: &str,
        row_id: &str,
        bytes: &[u8],
    ) -> crate::protocol::blob::locator::StoredBlobRef {
        self.founder
            .publish_exact_remote_blob_binding(store_dir, root_id, row_id, bytes)
            .await
    }

    pub(crate) async fn pull_into_result(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> Result<
        (
            std::collections::BTreeMap<String, u64>,
            crate::sync::store::StorePullResult,
        ),
        crate::sync::store::StorePullError,
    > {
        let device = Box::pin(self.open_into(db)).await.map_err(|error| {
            crate::sync::store::StorePullError::Membership(
                crate::sync::store::StorePullMembershipError::Message(error),
            )
        })?;
        device.pull_store(store_dir).await
    }

    pub(crate) async fn pull_into(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ) {
        self.pull_into_result(db, store_dir)
            .await
            .expect("pull exact test Store")
    }

    pub(crate) async fn promote_active_member_fixture(
        &self,
        owner_db: &Database,
        member_db: &Database,
        owner: &UserKeypair,
        member: &UserKeypair,
        encryption: &crate::encryption::EncryptionService,
    ) -> Result<crate::protocol::circle_control::StoreMembershipStateRef, String> {
        let owner_device = self.bind_device(owner_db, owner).await?;
        let member_device = self.bind_device(member_db, member).await?;
        let request = owner_device
            .begin_owner_promotion_for_device(member_device.typed_device_id())
            .await
            .map_err(|error| format!("begin Owner promotion: {error}"))?;
        let acceptance = member_device
            .accept_owner_promotion(request)
            .await
            .map_err(|error| format!("accept Owner promotion: {error}"))?;
        let finalized = owner_device
            .finalize_owner_promotion(encryption, acceptance)
            .await
            .map_err(|error| format!("finalize Owner promotion: {error}"))?;
        let (_temp, store_dir) = temp_store_dir();
        let (_, pull) = member_device
            .pull_store_with_encryption(&store_dir, encryption)
            .await
            .map_err(|error| error.to_string())?;
        if !pull.held_positions.is_empty() {
            return Err(format!(
                "Owner promotion pull held signed positions: {:?}",
                pull.held_positions
            ));
        }
        Ok(finalized)
    }

    fn storage_for_device(
        &self,
        identity: UserKeypair,
    ) -> Result<std::sync::Arc<crate::storage::CloudSyncStorage>, String> {
        if identity.public_key() == self.signer.public_key() {
            return Ok(self.storage.clone());
        }
        crate::storage::CloudSyncStorage::new(
            self.home.clone(),
            self.storage.cipher_snapshot(),
            self.storage.blob_path_scheme(),
            self.storage.store_id(),
            identity,
        )
        .map(std::sync::Arc::new)
        .map_err(|error| error.to_string())
    }

    pub(crate) async fn create(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, String> {
        Box::pin(Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            crate::storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    pub(crate) async fn create_encrypted(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
        encryption: crate::encryption::EncryptionService,
    ) -> Result<Arc<Self>, String> {
        Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Encrypted(encryption),
            crate::storage::BlobPathScheme::Hashed,
        )
        .await
    }

    pub(crate) async fn create_with_database(
        database: crate::database::StoreDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, String> {
        Box::pin(Self::create_with_protection_database(
            database,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Encrypted(crate::encryption::EncryptionService::from_key(
                [42; 32],
            )),
            crate::storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    /// A store whose home keeps blobs **browsable**: stored in the clear under
    /// readable paths. The counterpart of [`Self::create`], whose home is opaque
    /// (sealed under the store key, hashed paths). The pair is fixed per home,
    /// so a test that needs the browsable verification story needs this store.
    pub(crate) async fn create_browsable(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, String> {
        Box::pin(Self::create_with_protection(
            db,
            store_id,
            signer,
            home,
            crate::storage::CloudCipher::Plaintext,
            crate::storage::BlobPathScheme::Plain,
        ))
        .await
    }

    async fn create_with_protection(
        db: &Database,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: crate::storage::CloudCipher,
        blob_paths: crate::storage::BlobPathScheme,
    ) -> Result<Arc<Self>, String> {
        Self::create_with_protection_database(
            crate::database::StoreDatabase::new(db),
            store_id,
            signer,
            home,
            cipher,
            blob_paths,
        )
        .await
    }

    async fn create_with_protection_database(
        database: crate::database::StoreDatabase,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<crate::storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: crate::storage::CloudCipher,
        blob_paths: crate::storage::BlobPathScheme,
    ) -> Result<Arc<Self>, String> {
        let storage = std::sync::Arc::new(
            crate::storage::CloudSyncStorage::new(
                home.clone(),
                cipher,
                blob_paths,
                store_id,
                signer.clone(),
            )
            .map_err(|error| error.to_string())?,
        );
        let founder =
            TestDevice::create_with_database(database, storage.clone(), store_id, signer.clone())
                .await?;
        let root = founder.store_root().clone();
        Ok(Arc::new(Self {
            home,
            storage,
            root,
            signer,
            founder: founder.clone(),
            producers: Arc::new(tokio::sync::Mutex::new(TestStoreProducers {
                unassigned: Some(founder),
                by_name: HashMap::new(),
            })),
        }))
    }

    pub(crate) fn protocol_founder_pubkey(&self) -> String {
        crate::keys::public_key_hex(&self.signer)
    }

    pub(crate) async fn create_exact_protocol_object(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: &[u8],
    ) -> Result<crate::protocol::objects::ExactObjectRef, String> {
        let slot = self
            .storage
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
            .map_err(|error| error.to_string())?;
        let prepared = self
            .storage
            .prepare_protocol_object(context, slot, semantic_prefix, bytes.to_vec())
            .map_err(|error| error.to_string())?;
        self.storage
            .create_protocol_object(&prepared)
            .await
            .map_err(|error| error.to_string())?;
        Ok(prepared.reference().clone())
    }

    pub(crate) async fn publish_prepared_protocol_object(
        &self,
        prepared: &crate::protocol::objects::PreparedExactObject,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.create_protocol_object(prepared).await
    }

    pub(crate) async fn read_exact_protocol_object(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        object: &crate::protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    pub(crate) async fn contains_blob_object(
        &self,
        reference: &crate::protocol::blob::RowBlobRef,
    ) -> bool {
        match reference.stored() {
            Some(stored) => self
                .contains_stored_blob_object(stored)
                .await
                .unwrap_or_else(|error| panic!("verify exact blob object: {error}")),
            None => false,
        }
    }

    pub(crate) async fn contains_stored_blob_object(
        &self,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, crate::protocol::objects::StorageError> {
        match self.storage.verify_blob_object(stored).await {
            Ok(()) => Ok(true),
            Err(crate::protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn contains_blob_tombstone(
        &self,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, crate::storage::cloud::CloudHomeError> {
        let key =
            crate::blob::delete::tombstone_key_for_test(stored, &self.storage.cipher_snapshot());
        crate::storage::cloud::CloudHome::exists(self.home.as_ref(), &key).await
    }

    pub(crate) async fn contains_circle_snapshot_image(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        meta: &crate::protocol::store_commit::CircleSnapshotMeta,
    ) -> Result<bool, String> {
        let access = self
            .founder
            .circle_epoch_access(circle_id, meta.control.clone())
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "the Circle snapshot control has no retained access".to_string())?;
        let context = access.protocol_context(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::CircleSnapshotImage,
        );
        let prefix = crate::protocol::store_commit::semantic_prefix_from_exact_object(
            &meta.bootstrap.image.object,
            crate::protocol::objects::ProtectedObjectDomain::CircleSnapshotImage.extension(),
        )
        .map_err(|error| error.to_string())?;
        match self
            .storage
            .read_protocol_object(&context, &meta.bootstrap.image.object, &prefix)
            .await
        {
            Ok(_) => Ok(true),
            Err(crate::protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    pub(crate) async fn circle_package_in(
        &self,
        commit_ref: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> crate::protocol::store_commit::CirclePackageRef {
        let commit = self
            .founder
            .load_commit_for_test(commit_ref)
            .await
            .expect("load the exact Circle package commit");
        let [package] = commit.value().circle_packages() else {
            panic!("the commit must carry exactly one Circle package");
        };
        package.clone()
    }

    pub(crate) async fn circle_package_object_present(
        &self,
        package: &crate::protocol::store_commit::CirclePackageRef,
        activation: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> bool {
        let access = self
            .founder
            .circle_epoch_access(package.circle_id, package.control.clone())
            .await
            .expect("resolve Circle package access")
            .expect("the package's control stays retained after its epoch closed");
        let context = access.protocol_context(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::CirclePackage,
        );
        let prefix = crate::protocol::store_commit::circle_package_semantic_prefix(
            package.circle_id,
            package.package.candidate_family,
            &activation.coord.stream_id.to_string(),
            activation.coord.sequence(),
            package.package.content_hash,
        );
        match self
            .storage
            .read_protocol_object(&context, &package.package.object, &prefix)
            .await
        {
            Ok(_) => true,
            Err(crate::protocol::objects::StorageError::NotFound(_)) => false,
            Err(error) => panic!("read the exact Circle package object: {error}"),
        }
    }

    pub(crate) async fn publish_competing_store_head(
        &self,
        journal: &crate::protocol::circle_journal::CircleOperationJournal,
    ) -> (
        crate::protocol::objects::ExactObjectRef,
        crate::protocol::objects::ExactObjectRef,
    ) {
        let candidate = journal.commit().expect("parse candidate Store commit");
        let coord = journal.operation().commit_ref.coord.clone();
        let head = &journal.operation().policy.head;
        let registration = self
            .founder
            .activated_store_device_registration_for_test(candidate.author_registration.clone())
            .await
            .expect("load candidate author registration");
        let device_signer = registration
            .value()
            .device_signer(&self.signer)
            .expect("derive candidate device signer");
        let schema_version = self.founder.schema_version();
        let package = crate::protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate.candidate_family(),
            candidate.write_id.clone(),
            coord.clone(),
            schema_version,
            b"competing valid package".to_vec(),
            Vec::new(),
        )
        .expect("construct competing package");
        let package_bytes = package.to_bytes();
        let package_prefix = crate::protocol::store_commit::package_semantic_prefix(
            candidate.candidate_family(),
            &coord.stream_id.to_string(),
            candidate.seq(),
            crate::protocol::store_commit::ObjectHash::digest(&package_bytes),
        );
        let package_context = crate::protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("reserve competing package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare competing package");
        self.storage
            .create_protocol_object(&package_prepared)
            .await
            .expect("publish competing package");
        let membership = self
            .founder
            .membership()
            .await
            .expect("load competing commit membership");
        let predecessor = membership
            .write_grant_authority(&registration.value().author_pubkey)
            .expect("competing author has an active write grant");
        let winner = crate::protocol::store_commit::StoreBatchCommit::signed(
            self.root.store_root_hash,
            candidate.write_id.clone(),
            coord.clone(),
            candidate.author_registration.clone(),
            registration.value(),
            candidate.order.clone(),
            candidate.membership_state.clone(),
            candidate.device_state.clone(),
            crate::protocol::store_commit::StoreOperationMembershipAuthority { predecessor },
            crate::protocol::store_commit::StorePackageInput {
                candidate_family: candidate.candidate_family(),
                schema_version,
                bytes: &package_bytes,
                object: package_prepared.reference().clone(),
            },
            &device_signer,
        )
        .expect("sign competing commit");
        let commit_prefix = crate::protocol::store_commit::commit_semantic_prefix(
            winner.candidate_family(),
            &coord.stream_id.to_string(),
            winner.seq(),
            winner.commit_hash(),
        );
        let commit_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("reserve competing commit slot");
        let commit_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                winner.to_bytes(),
            )
            .expect("prepare competing commit");
        self.storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish competing commit");
        let winner_ref = crate::protocol::store_commit::StoreBatchCommitRef::from_commit(
            &winner,
            coord,
            commit_prepared.reference().clone(),
        )
        .expect("reference competing commit");
        assert_ne!(winner_ref, journal.operation().commit_ref);
        let winner_head = crate::protocol::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.author_registration.clone(),
            winner_ref,
            head.history_summary,
            head.successor.clone(),
            &device_signer,
        )
        .expect("sign competing head");
        let head_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_slot = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("candidate carries a prepared Store head")
            .reference()
            .slot()
            .clone();
        let head_prefix = crate::protocol::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.seq(),
        );
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                head_slot,
                &head_prefix,
                winner_head.to_bytes(),
            )
            .expect("prepare competing head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish competing head");
        (
            commit_prepared.reference().clone(),
            head_prepared.reference().clone(),
        )
    }

    pub(crate) async fn publish_third_candidate_winner(
        &self,
        peer_db: &Database,
        candidate: &crate::database::BlockedMergeCandidate,
    ) {
        let registration = crate::database::StoreDatabase::new(peer_db)
            .activated_store_device_registration(candidate.commit.value.author_registration.clone())
            .await
            .expect("load third-winner device registration");
        let device_signer = registration
            .value()
            .device_signer(&self.signer)
            .expect("derive third-winner device signer");
        let coord = candidate.head.value.commit.coord.clone();
        let candidate_family = candidate.commit.value.candidate_family();
        let package = crate::protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate_family,
            candidate.commit.value.write_id.clone(),
            coord.clone(),
            peer_db.schema_version(),
            b"third winner package".to_vec(),
            Vec::new(),
        )
        .expect("construct third winner package");
        let crate::protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence,
        } = coord.clone();
        let package_bytes = package.to_bytes();
        let package_context = crate::protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        let package_prefix = crate::protocol::store_commit::package_semantic_prefix(
            candidate_family,
            &stream_id.to_string(),
            sequence,
            crate::protocol::store_commit::ObjectHash::digest(&package_bytes),
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("allocate third winner package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare third winner package");
        let third = crate::protocol::store_commit::StoreBatchCommit::signed(
            self.root.store_root_hash,
            candidate.commit.value.write_id.clone(),
            coord.clone(),
            candidate.commit.value.author_registration.clone(),
            registration.value(),
            candidate.commit.value.order.clone(),
            candidate.commit.value.membership_state.clone(),
            candidate.commit.value.device_state.clone(),
            candidate
                .commit
                .value
                .operations_membership_authority()
                .expect("load third winner membership authority"),
            crate::protocol::store_commit::StorePackageInput {
                candidate_family,
                schema_version: peer_db.schema_version(),
                bytes: &package_bytes,
                object: package_prepared.reference().clone(),
            },
            &device_signer,
        )
        .expect("sign third ordinary winner");
        let commit_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let commit_prefix = crate::protocol::store_commit::commit_semantic_prefix(
            third.candidate_family(),
            &stream_id.to_string(),
            sequence,
            third.commit_hash(),
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("allocate third winner commit slot");
        let third_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                third.to_bytes(),
            )
            .expect("prepare third winner commit");
        self.storage
            .create_protocol_object(&third_prepared)
            .await
            .expect("publish third winner commit");
        let third_ref = crate::protocol::store_commit::StoreBatchCommitRef::from_commit(
            &third,
            coord,
            third_prepared.reference().clone(),
        )
        .expect("reference third winner commit");
        let third_head = crate::protocol::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.commit.value.author_registration.clone(),
            third_ref,
            candidate.head.value.history_summary,
            candidate.head.value.successor.clone(),
            &device_signer,
        )
        .expect("sign third winner head");
        let head_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = crate::protocol::store_commit::head_slot_prefix(
            &candidate
                .commit
                .value
                .author_registration
                .device_id
                .to_string(),
            sequence,
        );
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                candidate.head.object.slot().clone(),
                &head_prefix,
                third_head.to_bytes(),
            )
            .expect("prepare third winner head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish third winner head");
    }

    pub(crate) async fn overwrite_membership_head(
        &self,
        reference: &crate::protocol::membership::MembershipHeadRef,
        head: &crate::protocol::membership::AuthorHead,
    ) {
        self.storage
            .delete_protocol_object(&reference.object)
            .await
            .expect("delete exact head before replacement");
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix = crate::protocol::store_commit::membership_head_slot_prefix(
            &reference.coord.author_pubkey,
            &reference.coord.author_owner_grant,
            reference.coord.stream_id,
            reference.coord.seq,
        );
        let prepared = self
            .storage
            .prepare_protocol_object(
                &context,
                reference.object.slot().clone(),
                &prefix,
                serde_json::to_vec(head).expect("serialize replacement head"),
            )
            .expect("prepare replacement head");
        self.storage
            .create_protocol_object(&prepared)
            .await
            .expect("write replacement head");
    }

    pub(crate) async fn delete_membership_head_for_test(
        &self,
        reference: &crate::protocol::membership::MembershipHeadRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.delete_protocol_object(&reference.object).await
    }

    pub(crate) async fn pending_device_join_observation(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: &crate::sync::store::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, String> {
        self.founder
            .pending_device_join_observation_for_test(pending, offer)
            .await
    }

    pub(crate) async fn open_pending_device_join(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        identity: &UserKeypair,
        offer: crate::sync::store::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, String> {
        self.founder
            .open_pending_device_join_for_test(pending, identity, offer)
            .await
    }

    pub(crate) async fn prepare_snapshot_bootstrap<'a>(
        &'a self,
        membership_floor: &crate::joining::MembershipFloor,
        binary_schema_version: u32,
        target_path: &std::path::Path,
        restorer_identity: &UserKeypair,
    ) -> Result<crate::sync::store::PreparedSnapshotBootstrap<'a>, crate::sync::store::SnapshotError>
    {
        self.founder
            .prepare_snapshot_bootstrap_for_test(
                membership_floor,
                binary_schema_version,
                target_path,
                restorer_identity,
            )
            .await
    }

    pub(crate) async fn bind_device(
        &self,
        db: &Database,
        identity: &UserKeypair,
    ) -> Result<TestDevice, String> {
        self.bind_store_device(&crate::database::StoreDatabase::new(db), identity)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn push_circle_snapshots(
        &self,
        db: &Database,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: &crate::encryption::EncryptionService,
    ) -> Result<crate::protocol::store_commit::CircleSnapshotMeta, crate::sync::store::SnapshotError>
    {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .author_one_circle_snapshot_for_test(
                temp_dir,
                schema_version,
                created_at,
                store_routing,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_circle_snapshot_metas(
        &self,
        db: &Database,
        circle_id: crate::protocol::circle::CircleId,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<crate::protocol::store_commit::CircleSnapshotMeta>,
        crate::sync::store::SnapshotError,
    > {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .load_circle_snapshot_metas_for_test(circle_id, access)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn verify_standalone_circle_snapshot_image(
        &self,
        db: &Database,
        circle_id: crate::protocol::circle::CircleId,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
        store_routing: &crate::encryption::EncryptionService,
    ) -> Result<(), crate::sync::store::SnapshotError> {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .verify_standalone_circle_snapshot_image_for_test(circle_id, access, store_routing)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_snapshot_is_stable(
        &self,
        db: &Database,
        circle_id: crate::protocol::circle::CircleId,
        snapshot_cut: &crate::protocol::store_commit::CommitFrontier,
    ) -> Result<bool, crate::sync::store::SnapshotError> {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::SnapshotError::PublicationState)?
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::store::SnapshotError::PublicationState(error.to_string())
            })?
            .circles()
            .snapshots()
            .circle_snapshot_is_stable(circle_id, snapshot_cut)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_circle_acknowledgement(
        &self,
        db: &Database,
        reference: &crate::protocol::store_commit::CircleAckRef,
    ) -> Result<crate::protocol::store_commit::CircleAck, crate::sync::store::StoreAckError> {
        self.bind_device(db, &self.signer)
            .await
            .map_err(crate::sync::store::StoreAckError::InvalidOutbound)?
            .load_circle_acknowledgement_for_test(reference)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn read_circle_snapshot_image(
        &self,
        selected: &crate::protocol::store_commit::CircleSnapshotMeta,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        let context = access.protocol_context(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::CircleSnapshotImage,
        );
        self.storage
            .read_protocol_object(
                &context,
                &selected.bootstrap.image.object,
                &crate::protocol::store_commit::circle_snapshot_image_semantic_prefix(
                    selected.circle_id,
                    &selected.author_registration.device_id.to_string(),
                    selected.bootstrap.image.image_hash,
                ),
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_snapshot_meta_is_unreadable(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        encryption: crate::encryption::EncryptionService,
    ) -> bool {
        let context = crate::protocol::objects::ProtocolObjectContext::circle(
            self.root.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::CircleSnapshotMeta,
            encryption,
        );
        let prefix = crate::protocol::store_commit::circle_snapshot_slot_prefix(
            circle_id,
            &self.founder.device_id,
            0,
        );
        let slot = crate::protocol::objects::ObjectSlot::logical(format!("{prefix}.json"))
            .expect("valid generation-zero Circle snapshot slot");
        self.storage
            .read_protocol_slot(&context, &slot, &prefix)
            .await
            .is_err()
    }

    #[cfg(test)]
    pub(crate) fn store_root_hash(&self) -> crate::protocol::store_commit::ObjectHash {
        self.root.store_root_hash
    }

    pub(crate) async fn drain_uploads(
        &self,
        database: &crate::database::StoreDatabase,
        store_dir: &crate::store_dir::StoreDir,
        clock: &dyn crate::clock::Clock,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::protocol::blob::BlobTransitionObserver>,
    ) -> Result<crate::protocol::blob::DrainOutcome, crate::database::DbError> {
        let store = self
            .bind_store_device(database, &self.signer)
            .await
            .map_err(crate::database::DbError::Message)?;
        store
            .drain_uploads(store_dir, clock, routing_encryption, observer)
            .await
    }

    pub(crate) async fn activate_joined_device(
        &self,
        observer_db: &Database,
        joining_db: &Database,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, String> {
        let observer = self.bind_device(observer_db, &self.signer).await?;
        self.activate_joined_device_with_observer(
            observer,
            joining_db,
            joining_identity,
            published_at,
        )
        .await
    }

    async fn activate_joined_device_with_observer(
        &self,
        observer: TestDevice,
        joining_db: &Database,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, String> {
        let joining_database = crate::database::StoreDatabase::new(joining_db);
        let activated_database = joining_database.clone();
        let pending_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
        let pending = crate::sync::store::DeviceJoinJournalDatabase::open(
            pending_dir.path().join("pending-device-join.sqlite"),
        )
        .map_err(|error| error.to_string())?;
        let offer = observer
            .begin_device_join(&pubkey_hex(joining_identity))
            .await
            .map_err(|error| format!("begin device join: {error}"))?;
        let mut pending_join = observer
            .open_pending_device_join_for_test(&pending, joining_identity, offer)
            .await
            .map_err(|error| format!("open pending device join: {error}"))?;
        let access_request = pending_join
            .prepare_provider_access_request()
            .await
            .map_err(|error| format!("prepare provider access request: {error}"))?;
        let approval = observer
            .authorize_device_provider_access(access_request, None)
            .await
            .map_err(|error| format!("authorize device provider access: {error}"))?;
        let registration_request = pending_join
            .prepare_registration_request(approval)
            .await
            .map_err(|error| format!("prepare device registration request: {error}"))?;
        let provisional = observer
            .accept_device_registration_request(registration_request)
            .await
            .map_err(|error| format!("accept device registration request: {error}"))?;
        let provider_ready = observer
            .publish_device_provider_challenge(provisional)
            .await
            .map_err(|error| format!("publish device provider challenge: {error}"))?;
        let (_bootstrap_temp, bootstrap_store_dir) = temp_store_dir();
        let mut joining = pending_join
            .begin_joining_store(joining_database, &bootstrap_store_dir)
            .await
            .map_err(|error| format!("begin joining Store: {error}"))?;
        let routing_encryption = crate::encryption::EncryptionService::from_key([42; 32]);
        let bootstrap_pull = joining
            .pull_store_history(Some(&routing_encryption))
            .await
            .map_err(|error| format!("pull joining Store history: {error}"))?;
        if !bootstrap_pull.held_positions.is_empty() {
            return Err(format!(
                "device join bootstrap pull held signed positions: {:?}",
                bootstrap_pull.held_positions
            ));
        }
        let readiness = joining
            .bootstrap(provider_ready, published_at)
            .await
            .map_err(|error| format!("bootstrap joining Store: {error}"))?;
        let completion = observer
            .complete_device_provider_admission(readiness)
            .await
            .map_err(|error| format!("complete device provider admission: {error}"))?;
        let activation = observer
            .finalize_device_join(completion)
            .await
            .map_err(|error| format!("finalize device join: {error}"))?;
        joining
            .complete(activation)
            .await
            .map_err(|error| format!("complete joining Store: {error}"))?;
        TestDevice::load_with_database(
            activated_database,
            self.storage_for_device(joining_identity.clone())?,
            joining_identity.clone(),
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub(crate) async fn bind_store_device(
        &self,
        database: &crate::database::StoreDatabase,
        identity: &UserKeypair,
    ) -> Result<TestDevice, String> {
        TestDevice::load_with_database(
            database.clone(),
            self.storage_for_device(identity.clone())?,
            identity.clone(),
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub(crate) async fn invite_member(
        &self,
        db: &Database,
        identity: &UserKeypair,
        member_pubkey: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_name: &str,
    ) -> Result<crate::joining::InviteCode, crate::sync::store::MembershipOpsError> {
        let device = self.bind_device(db, identity).await.map_err(|error| {
            crate::sync::store::MembershipOpsError::Chain(
                crate::sync::store::AnchoredChainError::LoadFailed(error),
            )
        })?;
        device
            .invite_member(
                member_pubkey,
                invitee_email,
                role,
                encryption,
                self.storage.store_id(),
                store_name,
            )
            .await
    }

    pub(crate) async fn invite_and_activate_peer(
        &self,
        observer_db: &Database,
        peer_db: &Database,
        peer: &UserKeypair,
    ) -> Result<TestDevice, String> {
        self.invite_member(
            observer_db,
            &self.signer,
            &pubkey_hex(peer),
            None,
            crate::protocol::membership::MemberRole::Member,
            &crate::encryption::EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .map_err(|error| format!("invite peer identity: {error}"))?;
        self.activate_joined_device(observer_db, peer_db, peer, "2026-07-16T00:00:00Z")
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        db: &Database,
        identity: &UserKeypair,
        member_pubkey: &str,
        encryption: &crate::encryption::EncryptionService,
        security: &crate::store_security::StoreSecurity,
    ) -> Result<String, crate::sync::store::MembershipOpsError> {
        let device = self.bind_device(db, identity).await.map_err(|error| {
            crate::sync::store::MembershipOpsError::Chain(
                crate::sync::store::AnchoredChainError::LoadFailed(error),
            )
        })?;
        device
            .remove_member(
                member_pubkey,
                encryption,
                security,
                self.storage.as_ref(),
                self.storage.as_ref(),
            )
            .await
    }

    pub(crate) async fn device_id(&self, name: &str) -> Result<String, String> {
        Ok(self.ensure_producer(name).await?.device_id)
    }

    pub(crate) async fn founder_device(&self) -> Result<TestDevice, String> {
        Ok(self.founder.clone())
    }

    pub(crate) async fn next_commit_sequence(&self, name: &str) -> Result<u64, String> {
        self.ensure_producer(name)
            .await?
            .latest_local_store_position()
            .await
            .map_err(|error| error.to_string())?
            .map_or(Ok(1), |reference| {
                reference
                    .coord
                    .sequence()
                    .checked_add(1)
                    .ok_or_else(|| "test producer sequence exhausted u64".to_string())
            })
    }

    pub(crate) async fn founder_device_authority(
        &self,
    ) -> Result<TestDeviceSigningAuthority, String> {
        let device = self.ensure_producer("founder").await?;
        device.device_authority_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn retained_merge_history_summary(
        &self,
        device_id: &crate::protocol::store_commit::StoreDeviceId,
        reference: crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary, String> {
        let device = {
            let producers = self.producers.lock().await;
            producers
                .by_name
                .values()
                .chain(producers.unassigned.iter())
                .find(|producer| producer.device_id == device_id.to_string())
                .cloned()
                .ok_or_else(|| format!("test Store has no producer for device {device_id}"))?
        };
        device
            .retained_merge_history_summary_for_test(reference)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn publish_changeset(
        &self,
        name: &str,
        sequence: u64,
        changeset: &[u8],
        schema_version: u32,
    ) -> Result<crate::protocol::store_commit::StoreBatchCommitRef, String> {
        let device = self.ensure_producer(name).await?;
        device
            .publish_changeset_for_test(sequence, changeset.to_vec(), schema_version)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn publish_founder_changeset(
        &self,
        store_dir: &StoreDir,
        changeset: Vec<u8>,
        previous_sequence: u64,
    ) -> Result<crate::protocol::store_commit::StoreBatchCommitRef, String> {
        self.founder
            .publish_changeset_after_for_test(store_dir, changeset, previous_sequence)
            .await
    }

    async fn ensure_producer(&self, name: &str) -> Result<TestDevice, String> {
        {
            let producers = self.producers.lock().await;
            if let Some(producer) = producers.by_name.get(name) {
                return Ok(producer.clone());
            }
        }

        let unassigned = {
            let mut producers = self.producers.lock().await;
            producers.unassigned.take()
        };
        let producer = match unassigned {
            Some(producer) => producer,
            None => {
                let db = open_test_db();
                let observer = {
                    let producers = self.producers.lock().await;
                    producers
                        .by_name
                        .values()
                        .next()
                        .ok_or_else(|| "test Store has no active device observer".to_string())?
                        .clone()
                };
                self.activate_joined_device_with_observer(
                    observer,
                    &db,
                    &self.signer,
                    "2026-07-16T00:00:00Z",
                )
                .await?
            }
        };
        let mut producers = self.producers.lock().await;
        if producers
            .by_name
            .insert(name.to_string(), producer)
            .is_some()
        {
            return Err(format!("test producer {name:?} was registered twice"));
        }
        Ok(producers
            .by_name
            .get(name)
            .expect("inserted test producer exists")
            .clone())
    }

    pub(crate) async fn open_into(&self, db: &Database) -> Result<TestDevice, String> {
        self.open_into_store_database(&crate::database::StoreDatabase::new(db))
            .await
    }

    pub(crate) async fn open_into_store_database(
        &self,
        database: &crate::database::StoreDatabase,
    ) -> Result<TestDevice, String> {
        TestDevice::open_with_database(
            database.clone(),
            self.storage_for_device(self.signer.clone())?,
            &self.root,
            &self.signer,
        )
        .await
    }

    pub(crate) async fn publish_pending(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> Result<bool, String> {
        self.publish_pending_store_database(&crate::database::StoreDatabase::new(db), store_dir)
            .await
    }

    pub(crate) async fn publish_pending_store_database(
        &self,
        database: &crate::database::StoreDatabase,
        store_dir: &StoreDir,
    ) -> Result<bool, String> {
        let device = self.bind_store_device(database, &self.signer).await?;
        device.publish_pending_store_database(store_dir).await
    }
}

/// The Store view of a test database. Every sync test builds one; naming it here
/// keeps the three test modules that used to declare it from drifting apart.
#[cfg(test)]
pub(crate) fn store_database(db: &Database) -> crate::database::StoreDatabase {
    crate::database::StoreDatabase::new(db)
}

/// A plaintext cloud cipher — the default for tests that are not exercising
/// sealing.
#[cfg(test)]
pub(crate) fn plaintext_cipher() -> std::sync::RwLock<crate::storage::CloudCipher> {
    std::sync::RwLock::new(crate::storage::CloudCipher::Plaintext)
}

/// The host-provided, eagerly-cached photo blob declaration most blob tests use.
#[cfg(test)]
pub(crate) fn photo_decl() -> BlobDecl {
    BlobDecl::new(
        "photos",
        crate::protocol::blob::Provenance::HostProvided,
        crate::protocol::blob::CacheFill::CacheEager,
    )
}

/// The notes schema with a remote-root parent, carrying `decl` on `note_photos`.
#[cfg(test)]
pub(crate) fn remote_root_db(decl: BlobDecl) -> Database {
    open_test_db_schema(
        vec![
            SyncedTable::new(
                "notes",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .remote_root(),
            SyncedTable::new(
                "note_tags",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "note_photos",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .carries_blob(decl),
        ],
        test_migrations(),
    )
}

/// The cloud key a tombstone for `stored` is written under.
#[cfg(test)]
pub(crate) fn exact_tombstone_key(
    stored: &crate::protocol::blob::locator::StoredBlobRef,
) -> String {
    crate::blob::delete::tombstone_key_for_test(stored, &crate::storage::CloudCipher::Plaintext)
}

/// Which protocol read an interceptor hook is running ahead of.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtocolRead {
    Object,
    Slot,
    PreparedSlot,
}

#[cfg(test)]
pub(crate) enum TombstoneExistsInterception {
    Proceed,
    DeleteAndReportAbsent,
}

/// Test-side observation of a [`SyncStorage`] call.
///
/// Every hook runs before the wrapped storage does the work, and returning `Err`
/// fails the call without reaching it. All hooks default to doing nothing, so an
/// interceptor states only the operations its test is about — which is the point:
/// a test that intercepts two reads should not also have to restate the sixteen
/// operations it does not care about.
#[cfg(test)]
#[async_trait::async_trait]
pub(crate) trait StorageInterceptor: Send + Sync {
    async fn before_protocol_create(
        &self,
        _prepared: &crate::protocol::objects::PreparedExactObject,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_protocol_read(
        &self,
        _read: ProtocolRead,
        _semantic_prefix: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_allocate(&self) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_prepare(&self) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_create(
        &self,
        _blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_stage(&self) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_tombstone_read(
        &self,
        _key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_tombstone_write(
        &self,
        _key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_tombstone_exists(
        &self,
        _key: &str,
    ) -> Result<TombstoneExistsInterception, crate::protocol::objects::StorageError> {
        Ok(TombstoneExistsInterception::Proceed)
    }

    async fn before_blob_tombstone_delete(
        &self,
        _key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        Ok(())
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl<T> StorageInterceptor for std::sync::Arc<T>
where
    T: StorageInterceptor + ?Sized,
{
    async fn before_protocol_create(
        &self,
        prepared: &crate::protocol::objects::PreparedExactObject,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_protocol_create(prepared).await
    }

    async fn before_protocol_read(
        &self,
        read: ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_protocol_read(read, semantic_prefix).await
    }

    async fn before_blob_allocate(&self) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_allocate().await
    }

    async fn before_blob_prepare(&self) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_prepare().await
    }

    async fn before_blob_create(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_create(blob).await
    }

    async fn before_blob_stage(&self) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_stage().await
    }

    async fn before_blob_tombstone_read(
        &self,
        key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_tombstone_read(key).await
    }

    async fn before_blob_tombstone_write(
        &self,
        key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_tombstone_write(key).await
    }

    async fn before_blob_tombstone_exists(
        &self,
        key: &str,
    ) -> Result<TombstoneExistsInterception, crate::protocol::objects::StorageError> {
        (**self).before_blob_tombstone_exists(key).await
    }

    async fn before_blob_tombstone_delete(
        &self,
        key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        (**self).before_blob_tombstone_delete(key).await
    }
}

/// A [`SyncStorage`] that forwards every call to `inner`, giving `interceptor`
/// its chance first.
#[cfg(test)]
pub(crate) struct InterceptedStorage<S, I: StorageInterceptor>
where
    S: std::ops::Deref,
{
    inner: S,
    interceptor: I,
}

#[cfg(test)]
impl<S, I> crate::storage::CloudCipherAccess for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::storage::CloudCipherAccess,
    I: StorageInterceptor,
{
    fn snapshot(&self) -> crate::storage::CloudCipher {
        self.inner.snapshot()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        self.inner.merge_key_rotation(new_encryption, custody)
    }
}

#[cfg(test)]
impl<S, I> crate::storage::CloudRotationAccess for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::storage::CloudRotationAccess,
    I: StorageInterceptor,
{
    fn mark_candidate(
        &self,
        generation: u64,
        mutation: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner.mark_candidate(generation, mutation)
    }

    fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner.mark_committed_mutation(generation, mutation)
    }

    fn remove_candidate(
        &self,
        generation: u64,
        mutation: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner.remove_candidate(generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: crate::protocol::store_commit::ObjectHash,
        replacement: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        self.inner
            .replace_candidate_mutation(generation, previous, replacement)
    }

    fn gate(&self) -> Option<crate::protocol::objects::RotationGate> {
        self.inner.gate()
    }

    fn install_durable_gate(&self, gate: Option<crate::protocol::objects::RotationGate>) {
        self.inner.install_durable_gate(gate);
    }

    fn check(
        &self,
        cipher: &crate::storage::CloudCipher,
    ) -> Result<(), crate::protocol::objects::RotationPending> {
        self.inner.check(cipher)
    }
}

#[cfg(test)]
impl<S, I> crate::sync::cycle::SyncCycleStorage for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::sync::cycle::SyncCycleStorage,
    I: StorageInterceptor,
{
}

#[cfg(test)]
impl<S, I: StorageInterceptor> InterceptedStorage<S, I>
where
    S: std::ops::Deref,
{
    pub(crate) fn new(inner: S, interceptor: I) -> Self {
        Self { inner, interceptor }
    }

    pub(crate) fn interceptor(&self) -> &I {
        &self.interceptor
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl<S, I> crate::storage::SyncStorage for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::storage::SyncStorage,
    I: StorageInterceptor,
{
    fn blob_path_scheme(&self) -> crate::storage::BlobPathScheme {
        self.inner.blob_path_scheme()
    }

    fn self_uploader(&self) -> String {
        self.inner.self_uploader()
    }

    async fn probe_provider(&self) -> Result<(), crate::protocol::objects::StorageError> {
        self.inner.probe_provider().await
    }

    async fn set_member_access(
        &self,
        state: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, crate::protocol::objects::StorageError>
    {
        self.inner.set_member_access(state).await
    }

    async fn read_blob_tombstone(
        &self,
        key: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_tombstone_read(key).await?;
        self.inner.read_blob_tombstone(key).await
    }

    async fn write_blob_tombstone(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_tombstone_write(key).await?;
        self.inner.write_blob_tombstone(key, stored_bytes).await
    }

    async fn list_blob_tombstones(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::protocol::objects::StorageError> {
        self.inner.list_blob_tombstones(prefix).await
    }

    async fn blob_tombstone_exists(
        &self,
        key: &str,
    ) -> Result<bool, crate::protocol::objects::StorageError> {
        match self.interceptor.before_blob_tombstone_exists(key).await? {
            TombstoneExistsInterception::Proceed => self.inner.blob_tombstone_exists(key).await,
            TombstoneExistsInterception::DeleteAndReportAbsent => {
                self.inner.delete_blob_tombstone(key).await?;
                Ok(false)
            }
        }
    }

    async fn delete_blob_tombstone(
        &self,
        key: &str,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_tombstone_delete(key).await?;
        self.inner.delete_blob_tombstone(key).await
    }

    async fn list_provider_objects_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::protocol::objects::StorageError> {
        self.inner.list_provider_objects_for_test(prefix).await
    }

    async fn read_provider_object_for_test(
        &self,
        key: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.inner.read_provider_object_for_test(key).await
    }

    async fn provider_object_exists_for_test(
        &self,
        key: &str,
    ) -> Result<bool, crate::protocol::objects::StorageError> {
        self.inner.provider_object_exists_for_test(key).await
    }

    async fn probe_exact_slots(
        &self,
        journal: &dyn crate::protocol::provider::ProviderProbeJournal,
        probe_id: crate::protocol::provider::ProviderProbeId,
        binding: &crate::protocol::objects::ResolvedProviderBinding,
    ) -> Result<
        crate::protocol::provider::ExactSlotProbeReceipt,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.inner
            .probe_exact_slots(journal, probe_id, binding)
            .await
    }

    async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: crate::protocol::provider::ProviderProbeId,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::provider::ProviderProbeError>
    {
        self.inner
            .reserve_cross_principal_response_slot(probe_id)
            .await
    }

    async fn observe_exact_slot(
        &self,
        slot: &crate::protocol::objects::ObjectSlot,
    ) -> Result<
        Option<crate::protocol::objects::ExactObjectRef>,
        crate::protocol::objects::StorageError,
    > {
        self.inner.observe_exact_slot(slot).await
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &crate::protocol::objects::ObjectSlot,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.inner.delete_exact_slot_and_verify_absent(slot).await
    }

    async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn crate::protocol::provider::DeviceJoinChallengePublicationJournal,
        probe_id: crate::protocol::provider::ProviderProbeId,
        store: &crate::protocol::objects::StoreProviderBinding,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeChallenge,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.inner
            .prepare_cross_principal_challenge(
                publication_journal,
                probe_id,
                store,
                context,
                administrator_signer,
            )
            .await
    }

    async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn crate::protocol::provider::DeviceJoinChallengePublicationJournal,
        authorization: &crate::protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        store: &crate::protocol::objects::StoreProviderBinding,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeChallenge,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.inner
            .settle_cross_principal_challenge(
                publication_journal,
                authorization,
                challenge,
                context,
                store,
            )
            .await
    }

    async fn create_cross_principal_response(
        &self,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalResponseContext,
        store: &crate::protocol::objects::StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &crate::keys::UserKeypair,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeResponse,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.inner
            .create_cross_principal_response(
                challenge,
                context,
                store,
                administrator_signing_pubkey,
                peer_signer,
            )
            .await
    }

    async fn complete_cross_principal_probe(
        &self,
        journal: &dyn crate::protocol::provider::ProviderProbeJournal,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        response: &crate::protocol::provider::CrossPrincipalProbeResponse,
        context: &crate::protocol::provider::CrossPrincipalResponseContext,
        store: &crate::protocol::objects::StoreProviderBinding,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
        peer_signing_pubkey: &str,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeReceipt,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.inner
            .complete_cross_principal_probe(
                journal,
                challenge,
                response,
                context,
                store,
                administrator_signer,
                peer_signing_pubkey,
            )
            .await
    }

    fn store_blob_protection(
        &self,
    ) -> Result<crate::protocol::objects::BlobSpoolProtection, crate::protocol::objects::StorageError>
    {
        self.inner.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<
        crate::protocol::objects::ResolvedProviderBinding,
        crate::protocol::objects::StorageError,
    > {
        self.inner.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::objects::StorageError> {
        self.inner
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<crate::protocol::objects::PreparedExactObject, crate::protocol::objects::StorageError>
    {
        self.inner
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &crate::protocol::objects::PreparedExactObject,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.interceptor.before_protocol_create(prepared).await?;
        self.inner.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        object: &crate::protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, crate::protocol::objects::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::Object, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: &crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, crate::protocol::objects::ExactObjectRef),
        crate::protocol::objects::StorageError,
    > {
        self.interceptor
            .before_protocol_read(ProtocolRead::Slot, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: &crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, crate::protocol::objects::PreparedExactObject),
        crate::protocol::objects::StorageError,
    > {
        self.interceptor
            .before_protocol_read(ProtocolRead::PreparedSlot, semantic_prefix)
            .await?;
        self.inner
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::protocol::objects::ExactObjectRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_allocate().await?;
        self.inner.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        protection: crate::protocol::objects::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool_file: &std::path::Path,
    ) -> Result<crate::protocol::objects::BlobSpoolWrite, crate::protocol::objects::StorageError>
    {
        self.inner
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        slot: crate::protocol::objects::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<crate::protocol::blob::locator::StoredBlobRef, crate::protocol::objects::StorageError>
    {
        self.interceptor.before_blob_prepare().await?;
        self.inner
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_create(blob).await?;
        self.inner
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.inner.verify_blob_object(blob).await
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        dest: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_stage().await?;
        self.inner.stage_exact_blob_download(blob, dest).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
        dest: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, crate::protocol::objects::StorageError> {
        self.interceptor.before_blob_stage().await?;
        self.inner
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn open_blob_range_reader(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
    ) -> Result<crate::storage::BlobRangeReader, crate::protocol::objects::StorageError> {
        self.inner.open_blob_range_reader(blob, protection).await
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::protocol::objects::StorageError> {
        self.inner.delete_blob_object(blob).await
    }
}
