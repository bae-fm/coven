//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `TestStore`; a second device
//! pulls and applies them through a real [`coven_database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use crate::sync::store::pull::HeldStorePositionReason;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::sync::store::{HeldStoreCoordinate, HeldStorePosition};
use coven_database::Database;
use coven_database::Migration;
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::membership::OWNER_PUBKEY_STATE_KEY;
use coven_protocol::membership::{MemberRole, MembershipChain, MembershipCoord};
use coven_protocol::store_commit::StoreDeviceHead;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};
/// The synthetic test db opens with a single migration, so its
/// [`coven_database::Database::schema_version`] is 1. Changesets are stored at
/// that version; a newer peer's changeset or floor uses `SCHEMA_VERSION + 1`.
const SCHEMA_VERSION: u32 = 1;
use crate::sync::test_helpers::*;
use coven_protocol::objects::ProtocolObjectDomain;
use coven_protocol::synced_schema::{BlobDecl, SyncedTable};
use coven_storage::CloudSyncObjectStorage;

fn exact_cache_path(
    store_dir: &coven_foundation::store_dir::StoreDir,
    reference: &coven_protocol::blob::RowBlobRef,
) -> std::path::PathBuf {
    let stored = reference.stored().expect("Remote row has exact storage");
    store_dir
        .cache_blob_path(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )
        .expect("build exact locator cache path")
}

fn exact_pinned_path(
    store_dir: &coven_foundation::store_dir::StoreDir,
    reference: &coven_protocol::blob::RowBlobRef,
) -> std::path::PathBuf {
    let stored = reference.stored().expect("Remote row has exact storage");
    store_dir
        .pinned_blob_path(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )
        .expect("build exact locator pinned path")
}

trait PullTestDatabaseOps {
    async fn exact_row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> coven_protocol::blob::RowBlobRef;
    async fn stored_remote_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> coven_protocol::remote_object::RemoteObjectRecord;
    async fn stored_remote_objects(&self)
        -> Vec<coven_protocol::remote_object::RemoteObjectRecord>;
    async fn replace_retained_merge_input(&self, stream_id: String, canonical_input: Vec<u8>);
    async fn replace_stored_remote_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
        remote: &coven_protocol::remote_object::RemoteObjectRecord,
    );
    async fn local_announcement_stream(&self) -> coven_protocol::membership::AuthorStreamId;
    async fn pull_exact_store_into(
        &self,
        source: &coven_database::Database,
        storage: &Arc<CloudSyncConnection>,
        identity: &UserKeypair,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    );
    async fn materialized_sequences(&self) -> HashMap<String, u64>;
    async fn row_blob_object_key(&self, table: &str, row_id: &str) -> String;
}

impl PullTestDatabaseOps for coven_database::Database {
    async fn exact_row_blob_ref(
        &self,
        table: &str,
        row_id: &str,
    ) -> coven_protocol::blob::RowBlobRef {
        self.row_blob_ref(table, row_id)
            .await
            .expect("load exact row blob reference")
    }

    async fn stored_remote_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> coven_protocol::remote_object::RemoteObjectRecord {
        self.remote_object_for_test(object.clone())
            .await
            .expect("load exact remote object")
    }

    async fn stored_remote_objects(
        &self,
    ) -> Vec<coven_protocol::remote_object::RemoteObjectRecord> {
        self.remote_objects_for_test()
            .await
            .expect("load remote objects")
    }

    async fn replace_retained_merge_input(&self, stream_id: String, canonical_input: Vec<u8>) {
        self.replace_retained_merge_input_for_test(stream_id, canonical_input)
            .await
            .expect("replace retained Merge input and its exact ownership closure");
    }

    async fn replace_stored_remote_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
        remote: &coven_protocol::remote_object::RemoteObjectRecord,
    ) {
        self.replace_remote_object_for_test(object.clone(), remote.clone())
            .await
            .expect("replace test remote object");
    }

    async fn local_announcement_stream(&self) -> coven_protocol::membership::AuthorStreamId {
        let registration = store_database(self)
            .local_blob_write_authority()
            .await
            .expect("read active local Store registration");
        registration
            .value()
            .store_announcement_activation(registration.reference())
            .expect("derive local Store announcement activation")
            .author_stream_id()
    }

    async fn pull_exact_store_into(
        &self,
        source: &coven_database::Database,
        storage: &Arc<CloudSyncConnection>,
        identity: &UserKeypair,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ) {
        let root = coven_database::StoreDatabase::new(source)
            .local_store_root_ref()
            .await
            .expect("read source Store root")
            .expect("source Store has exact root authority");
        let initialized = crate::sync::store::Store::open(
            coven_database::StoreDatabase::new(self),
            storage.clone(),
            store_dir.clone(),
            &root,
            identity,
        )
        .await
        .expect("open exact Store on destination");
        let (store, _device_id) = initialized.into_parts();
        let routing_encryption = EncryptionService::from_key([42; 32]);
        let result = store
            .authorize_writer()
            .await
            .expect("authorize exact destination Store")
            .pull(Some(&routing_encryption))
            .await
            .expect("pull exact Store commits");
        let positions = result
            .frontier
            .iter()
            .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
            .collect();
        (positions, result)
    }

    async fn materialized_sequences(&self) -> HashMap<String, u64> {
        store_database(self)
            .materialized_frontier()
            .await
            .expect("read materialized Store frontier")
            .into_iter()
            .map(|(device_id, position)| (device_id, position.coord.sequence()))
            .collect()
    }

    async fn row_blob_object_key(&self, table: &str, row_id: &str) -> String {
        self.exact_row_blob_ref(table, row_id)
            .await
            .stored()
            .expect("Remote row has exact blob object authority")
            .object()
            .slot()
            .logical_key()
            .to_string()
    }
}

trait PullTestStoreDirOps {
    async fn store_local(&self, id: &str, bytes: &[u8]);
}

impl PullTestStoreDirOps for coven_foundation::store_dir::StoreDir {
    async fn store_local(&self, id: &str, bytes: &[u8]) {
        self.store_local_blob("photos", id, bytes)
            .await
            .expect("store host-provided blob in the local store");
    }
}

fn is_external_circle_package(
    remote: &coven_protocol::remote_object::RemoteObjectRecord,
    retained_for_replay: bool,
) -> bool {
    let coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) = remote else {
        return false;
    };
    if !matches!(
        record.identity.domain,
        coven_protocol::remote_object::SharedLiveSetObjectDomain::CirclePackage { .. }
    ) || record.payloads != coven_protocol::remote_object::RemoteObjectPayloads::SpooledExternal
    {
        return false;
    }
    let coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &record.state
    else {
        return false;
    };
    let has_commit = ownership.activated.iter().any(|owner| {
        matches!(
            owner,
            coven_protocol::remote_object::SharedObjectOwner::StoreCommit(_)
        )
    });
    let has_replay = ownership.activated.iter().any(|owner| {
        matches!(
            owner,
            coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(_)
        )
    });
    has_commit && has_replay == retained_for_replay
}

fn commit_stream_id(reference: &coven_protocol::store_commit::StoreBatchCommitRef) -> String {
    reference.coord.stream_id.to_string()
}

#[async_trait]
trait TestStoreStorage: Sync {
    async fn store_for_test_publish(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        keypair: &UserKeypair,
    ) -> Result<crate::sync::store::Store, crate::sync::test_helpers::TestError>;

    async fn sync_for_test(
        &self,
        db: &coven_database::Database,
        outgoing: Vec<u8>,
        local_seq: u64,
        message: &str,
        keypair: &UserKeypair,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) -> Result<
        Option<coven_protocol::store_commit::StoreBatchCommitRef>,
        crate::sync::test_helpers::TestError,
    > {
        assert!(
            message.is_empty(),
            "Store commits carry no arbitrary message"
        );
        let store = self.store_for_test_publish(db, store_dir, keypair).await?;
        let before = store.latest_local_store_position().await?;
        assert_eq!(
            before
                .as_ref()
                .map_or(0, |position| position.coord.sequence()),
            local_seq
        );
        coven_database::StoreDatabase::new(db)
            .enqueue_store_changeset_for_test(outgoing)
            .await?;
        let mut writer = store.authorize_writer().await?;
        let prepared = writer.prepare_pending_store_write().await?;
        if !prepared {
            return Ok(None);
        }
        writer.drain_store_writes().await?;
        writer
            .latest_local_store_position()
            .await
            .map_err(Into::into)
    }

    /// Push one captured changeset through Store write preparation and publish
    /// the resulting immutable Store objects as `keypair`.
    async fn publish_test_cycle(
        &self,
        db: &coven_database::Database,
        outgoing: Vec<u8>,
        local_seq: u64,
        keypair: &UserKeypair,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) {
        let result = self
            .sync_for_test(db, outgoing, local_seq, "", keypair, store_dir)
            .await
            .expect("sync");
        assert!(result.is_some(), "the captured rows publish a Store commit");
    }
}

#[async_trait]
impl TestStoreStorage for TestStore {
    async fn store_for_test_publish(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        keypair: &UserKeypair,
    ) -> Result<crate::sync::store::Store, crate::sync::test_helpers::TestError> {
        self.bind_device_in(db, store_dir.clone(), keypair).await?;
        self.open_store_with_identity(db, store_dir.clone(), keypair)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl TestStoreStorage for std::sync::Arc<CloudSyncConnection> {
    async fn store_for_test_publish(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        keypair: &UserKeypair,
    ) -> Result<crate::sync::store::Store, crate::sync::test_helpers::TestError> {
        if coven_database::StoreDatabase::new(db)
            .local_store_root_ref()
            .await?
            .is_none()
        {
            crate::sync::store::Store::create(
                coven_database::StoreDatabase::new(db),
                self.clone(),
                store_dir.clone(),
                self.store_id(),
                keypair,
            )
            .await?;
        }
        crate::sync::store::Store::load(
            coven_database::StoreDatabase::new(db),
            self.clone(),
            store_dir.clone(),
            keypair.clone(),
        )
        .await
        .map_err(Into::into)
    }
}

trait PullTestStoreOps {
    async fn make_root_remote(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        root_id: &str,
    );

    async fn read_exact_blob(
        &self,
        cloud_storage: &CloudSyncConnection,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Vec<u8>;

    async fn author_scoped_write(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        sql: String,
    );

    async fn pull_scoped(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) -> Result<crate::sync::store::StorePullResult, crate::sync::cycle::SyncCycleFailure>;

    async fn publish_exact_changeset_with_authority(
        &self,
        cloud_storage: &CloudSyncConnection,
        signer: &UserKeypair,
        name: &str,
        sequence: u64,
        changeset: &[u8],
        authority: Option<coven_protocol::membership::MembershipCoord>,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef;
}

impl PullTestStoreOps for TestStore {
    async fn make_root_remote(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        root_id: &str,
    ) {
        self.open_into(db, store_dir.clone())
            .await
            .expect("open exact test Store");
        let device = self
            .bind_founder_device(db, store_dir.clone())
            .await
            .expect("load exact fixture Store");
        crate::sync::test_owner_graph::TestOwnerGraph::new(store_database(db), store_dir.clone())
            .make_remote("notes", root_id, false)
            .await
            .expect("queue exact blob fixture upload");
        let outcome = device
            .drain_uploads(&coven_foundation::clock::SystemClock, None, None)
            .await
            .expect("upload exact blob fixture");
        assert!(outcome.uploaded() > 0);
        assert!(outcome.yielded_for_publish());
        assert!(self
            .publish_pending(db, store_dir)
            .await
            .expect("publish exact blob fixture"));
    }

    async fn read_exact_blob(
        &self,
        cloud_storage: &CloudSyncConnection,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Vec<u8> {
        let temp = tempfile::tempdir().expect("create exact blob read directory");
        let destination = temp.path().join("plaintext");
        let stage = coven_foundation::store_dir::StoreDir::new_ephemeral(temp.path())
            .stage_atomic_file(&destination)
            .await
            .expect("create exact blob stage");
        let staged = cloud_storage
            .stage_verified_blob_plaintext(
                blob,
                match blob.locator() {
                    coven_protocol::blob::locator::BlobLocator::Opaque { .. } => {
                        coven_protocol::objects::BlobSpoolProtection::Opaque(
                            EncryptionService::from_key([42; 32]),
                        )
                    }
                    coven_protocol::blob::locator::BlobLocator::Browsable { .. } => {
                        coven_protocol::objects::BlobSpoolProtection::Browsable
                    }
                },
                stage,
            )
            .await
            .expect("read exact blob object");
        tokio::fs::read(staged.path())
            .await
            .expect("read staged exact blob plaintext")
    }

    async fn author_scoped_write(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
        sql: String,
    ) {
        coven_database::StoreDatabase::new(db)
            .run_host_store_write_for_test(
                Some(EncryptionService::from_key([42; 32])),
                None,
                move |tx| {
                    tx.execute_batch(&sql)
                        .map_err(coven_database::DbError::from)
                },
            )
            .await
            .expect("commit scoped host transaction");
        self.publish_pending(db, store_dir)
            .await
            .expect("publish scoped write");
    }

    async fn pull_scoped(
        &self,
        db: &coven_database::Database,
        store_dir: &coven_foundation::store_dir::StoreDir,
    ) -> Result<crate::sync::store::StorePullResult, crate::sync::cycle::SyncCycleFailure> {
        let device = self
            .open_into(db, store_dir.clone())
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation("open Store", error)
            })?;
        let routing_encryption = EncryptionService::from_key([42; 32]);
        device
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation("authorize Store writer", error)
            })?
            .pull(Some(&routing_encryption))
            .await
    }

    async fn publish_exact_changeset_with_authority(
        &self,
        cloud_storage: &CloudSyncConnection,
        signer: &UserKeypair,
        name: &str,
        sequence: u64,
        changeset: &[u8],
        authority: Option<coven_protocol::membership::MembershipCoord>,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef {
        let reference = self
            .publish_changeset(name, sequence, changeset, SCHEMA_VERSION)
            .await
            .expect("publish exact Store changeset");
        let graph = ExactPublishedCommit::load(self, cloud_storage, reference, signer).await;
        let commit = graph
            .resign_commit(
                SCHEMA_VERSION,
                authority.map(coven_protocol::membership::MembershipGrantCreationAuthority::Entry),
            )
            .await;
        graph
            .replace_commit_bytes(
                commit.to_bytes(),
                commit.commit_hash(),
                graph.head.author_registration.clone(),
                &graph.device_signer,
            )
            .await
    }
}

fn cloud_test_storage(
    home: std::sync::Arc<dyn coven_storage::ExactCloudHome>,
    cipher: CloudCipher,
    blob_paths: BlobPathScheme,
    store_id: &str,
    keypair: UserKeypair,
) -> std::sync::Arc<CloudSyncConnection> {
    std::sync::Arc::new(CloudSyncConnection::new(
        home, cipher, blob_paths, store_id, keypair,
    ))
}

/// The common `note_photos` blob declaration: namespace `"photos"`, master scope,
/// host-provided · `CacheEager` (a cover — fetched into the cache on pull), hashed
/// scheme.
fn photo_decl_with_blob_id_column() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("cloud_path")
}

fn unique_note_db(store_dir: coven_foundation::store_dir::StoreDir) -> coven_database::Database {
    open_test_db_schema(
        store_dir,
        vec![SyncedTable::new(
            "unique_notes",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )],
        vec![Migration::run(1, "unique-note-schema", |conn| {
            conn.execute_batch(
                "CREATE TABLE unique_notes (
                    id TEXT PRIMARY KEY,
                    slug TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    _updated_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                ) STRICT;",
            )
            .map_err(coven_database::DbError::from)
        })],
    )
}

fn uuid_note_db(store_dir: coven_foundation::store_dir::StoreDir) -> coven_database::Database {
    open_test_db_schema(
        store_dir,
        vec![SyncedTable::new(
            "uuid_notes",
            coven_protocol::synced_schema::RowIdentity::IndependentUuid,
        )],
        vec![Migration::run(1, "uuid-note-schema", |conn| {
            conn.execute_batch(
                "CREATE TABLE uuid_notes (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    _updated_at TEXT NOT NULL
                ) STRICT;",
            )
            .map_err(coven_database::DbError::from)
        })],
    )
}

fn mixed_constraint_db(
    store_dir: coven_foundation::store_dir::StoreDir,
) -> coven_database::Database {
    open_test_db_schema(
        store_dir,
        vec![
            SyncedTable::new(
                "constraint_parents",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "constraint_items",
                coven_protocol::synced_schema::RowIdentity::SharedKey,
            ),
        ],
        vec![Migration::run(1, "mixed-constraint-schema", |conn| {
            conn.execute_batch(
                "CREATE TABLE constraint_parents (
                    id TEXT PRIMARY KEY,
                    _updated_at TEXT NOT NULL
                ) STRICT;
                CREATE TABLE constraint_items (
                    id TEXT PRIMARY KEY,
                    parent_id TEXT NOT NULL,
                    slug TEXT NOT NULL UNIQUE,
                    _updated_at TEXT NOT NULL,
                    FOREIGN KEY (parent_id) REFERENCES constraint_parents (id)
                ) STRICT;",
            )
            .map_err(coven_database::DbError::from)
        })],
    )
}

fn open_blob_test_db_at(
    path: &std::path::Path,
    store_dir: coven_foundation::store_dir::StoreDir,
    decl: BlobDecl,
) -> coven_database::Database {
    coven_database::Database::open_synthetic_for_test(
        path,
        store_dir,
        test_synced_tables_with_blob(decl),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "restart-test-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &test_migrations(),
    )
    .expect("open file-backed blob test database")
}

async fn create_store(
    db: &coven_database::Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    signer: UserKeypair,
) -> std::sync::Arc<TestStore> {
    TestStore::create(
        db,
        db_store_dir.clone(),
        "test-store",
        signer,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact test Store for the test database")
}

async fn create_store_fixture(
    db: &coven_database::Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    signer: UserKeypair,
) -> TestStoreParts {
    TestStore::create_with_connection(
        db,
        db_store_dir.clone(),
        "test-store",
        signer,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact test Store for the test database")
}

fn constraint_conflicts(result: &crate::sync::store::StorePullResult) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| matches!(held.reason, HeldStorePositionReason::ConstraintConflict(_)))
        .collect()
}

fn newer_schema_positions(result: &crate::sync::store::StorePullResult) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| matches!(held.reason, HeldStorePositionReason::NewerSchema { .. }))
        .collect()
}

fn unauthorized_positions(result: &crate::sync::store::StorePullResult) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| held.reason == HeldStorePositionReason::Unauthorized)
        .collect()
}

fn invalid_changeset_positions(
    result: &crate::sync::store::StorePullResult,
) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| matches!(held.reason, HeldStorePositionReason::InvalidChangeset(_)))
        .collect()
}

fn missing_exact_membership_authority_positions(
    result: &crate::sync::store::StorePullResult,
) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| {
            matches!(
                &held.reason,
                HeldStorePositionReason::InvalidObjectPull(error)
                    if matches!(
                        error.as_ref(),
                        crate::sync::store::StorePullError::InvalidState(detail)
                            if detail == "Merge history commit lacks exact membership authority"
                    )
            )
        })
        .collect()
}

/// A plaintext, plain-path test Store on a fresh in-memory home, with the
/// identity that signs for it.
fn plain_cloud_test_store() -> (
    InMemoryCloudHome,
    UserKeypair,
    std::sync::Arc<CloudSyncConnection>,
) {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    (home, keypair, storage)
}

fn membership_author_stream(
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> coven_protocol::membership::AuthorStreamId {
    let author = pubkey_hex(signer);
    let owner_grant = chain
        .active_owner_grant(&author)
        .expect("membership author has an active Owner grant");
    chain
        .entries()
        .iter()
        .rev()
        .find(|entry| entry.author_pubkey == author && entry.author_owner_grant == owner_grant)
        .map(|entry| entry.stream_id)
        .or_else(|| chain.membership_stream_id(&owner_grant))
        .expect("membership author has an anchored Store-membership stream")
}

/// The active Owner's signed grant of ordinary membership to `member`, authored
/// on the Owner's own membership stream.
fn signed_member_grant(
    chain: &MembershipChain,
    owner: &UserKeypair,
    member: &UserKeypair,
) -> coven_protocol::membership::MembershipEntry {
    chain
        .signed_set_member_in_stream(
            owner,
            membership_author_stream(chain, owner),
            pubkey_hex(member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant")
}

struct ExactMembershipChain<'storage> {
    store: &'storage TestStore,
    cloud_storage: &'storage CloudSyncConnection,
    chain: MembershipChain,
}

impl std::ops::Deref for ExactMembershipChain<'_> {
    type Target = MembershipChain;

    fn deref(&self) -> &Self::Target {
        &self.chain
    }
}

impl std::ops::DerefMut for ExactMembershipChain<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.chain
    }
}

impl<'storage> ExactMembershipChain<'storage> {
    async fn load(
        store: &'storage TestStore,
        cloud_storage: &'storage CloudSyncConnection,
    ) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let device = store
            .open_into(&db, db_store_dir.clone())
            .await
            .expect("load exact test Store membership");
        Self::load_from_device(store, cloud_storage, &device).await
    }

    async fn load_from_device(
        store: &'storage TestStore,
        cloud_storage: &'storage CloudSyncConnection,
        device: &crate::sync::test_helpers::TestDevice,
    ) -> Self {
        let chain = device
            .membership_for_test()
            .await
            .expect("authorize exact test Store membership");
        Self {
            store,
            cloud_storage,
            chain,
        }
    }

    async fn publish_entry(
        &mut self,
        entry: coven_protocol::membership::MembershipEntry,
        signer: &UserKeypair,
    ) {
        use coven_protocol::membership::{AuthorHead, MembershipHeadRef};
        use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
        use coven_protocol::store_commit::{
            membership_head_slot_prefix, StreamActivation, SuccessorLink,
        };

        let store = self.store;
        let cloud_storage = self.cloud_storage;
        let chain = &mut self.chain;
        let coord = entry.coord();
        let (registration_ref, registration, device_signer) = if let Some(predecessor) = chain
            .head_ref_for_stream(
                &entry.author_pubkey,
                &entry.author_owner_grant,
                entry.stream_id,
            ) {
            let head = store
                .load_membership_head_for_test(predecessor)
                .await
                .expect("load exact predecessor membership head");
            let registration = store
                .load_registration_for_test(&head.body.author_registration)
                .await
                .expect("load exact membership author registration");
            let device_signer = registration
                .device_signer(signer)
                .expect("membership signer owns exact device registration");
            (
                head.body.author_registration.clone(),
                registration,
                device_signer,
            )
        } else {
            use coven_protocol::store_commit::{
                DeviceRecoveryId, DeviceStreamAnchor, StoreDeviceRegistration,
                StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
            };

            let recovery_id = DeviceRecoveryId::from_hash(
                coven_protocol::store_commit::ObjectHash::digest(entry.author_pubkey.as_bytes()),
            );
            let recovery_prefix = coven_protocol::store_commit::owner_recovery_semantic_prefix(
                &entry.author_pubkey,
                entry.author_owner_grant.clone(),
                1,
            );
            let recovery_slot = cloud_storage
                .allocate_protocol_slot(
                    &ProtocolObjectContext::signed_plaintext(
                        store.root().store_root_hash,
                        ProtocolObjectDomain::OwnerRecoveryNode,
                    ),
                    &recovery_prefix,
                    ".json",
                )
                .await
                .expect("allocate exact recovery registration slot");
            let origin = StoreDeviceRegistrationOrigin::Recovery {
                recovery_id,
                recovery_slot,
                owner_grant: entry.author_owner_grant.clone(),
            };
            let device_id =
                coven_protocol::store_commit::StoreDeviceId::derive(&store.root(), &origin);
            let announcement_slot = cloud_storage
                .allocate_protocol_slot(
                    &ProtocolObjectContext::signed_plaintext(
                        store.root().store_root_hash,
                        ProtocolObjectDomain::StoreHead,
                    ),
                    &coven_protocol::store_commit::head_slot_prefix(&device_id.to_string(), 1),
                    ".json",
                )
                .await
                .expect("allocate exact announcement slot");
            let acknowledgement_slot = cloud_storage
                .allocate_protocol_slot(
                    &ProtocolObjectContext::signed_plaintext(
                        store.root().store_root_hash,
                        ProtocolObjectDomain::StoreAck,
                    ),
                    &coven_protocol::store_commit::ack_slot_prefix(&device_id.to_string(), 1),
                    ".json",
                )
                .await
                .expect("allocate exact acknowledgement slot");
            let snapshot_slot = cloud_storage
                .allocate_protocol_slot(
                    &ProtocolObjectContext::signed_plaintext(
                        store.root().store_root_hash,
                        ProtocolObjectDomain::StoreSnapshotMeta,
                    ),
                    &coven_protocol::store_commit::snapshot_slot_prefix(&device_id.to_string(), 0),
                    ".json",
                )
                .await
                .expect("allocate exact snapshot slot");
            let founder_authority = store
                .founder_device_authority()
                .await
                .expect("load exact founder device registration");
            let registration = StoreDeviceRegistration::signed(
                store.root().clone(),
                origin,
                founder_authority.registration().provider.clone(),
                DeviceStreamAnchor::StoreAnnouncements {
                    first_slot: announcement_slot,
                },
                DeviceStreamAnchor::StoreAcknowledgements {
                    first_slot: acknowledgement_slot,
                },
                DeviceStreamAnchor::StoreSnapshots {
                    first_slot: snapshot_slot,
                },
                signer,
            )
            .expect("sign exact membership author registration");
            let semantic_prefix = coven_protocol::store_commit::registration_semantic_prefix(
                &registration.device_id.to_string(),
            );
            let context = ProtocolObjectContext::signed_plaintext(
                store.root().store_root_hash,
                ProtocolObjectDomain::StoreDeviceRegistration,
            );
            let slot = cloud_storage
                .allocate_protocol_slot(&context, &semantic_prefix, ".json")
                .await
                .expect("allocate exact membership registration object");
            let prepared = cloud_storage
                .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
                .expect("prepare exact membership registration object");
            cloud_storage
                .create_protocol_object(&prepared)
                .await
                .expect("publish exact membership registration object");
            let reference = StoreDeviceRegistrationRef::from_registration(
                &registration,
                prepared.reference().clone(),
            );
            let device_signer = registration
                .device_signer(signer)
                .expect("derive exact membership device signer");
            (reference, registration, device_signer)
        };
        let predecessor = chain
            .head_ref_for_stream(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
            )
            .cloned();
        let anchor = chain
            .membership_anchor(&coord.author_owner_grant)
            .expect("membership author has an exact stream anchor")
            .clone();
        let current_slot = match predecessor.as_ref() {
            Some(reference) => store
                .load_membership_head_for_test(reference)
                .await
                .expect("load exact membership predecessor")
                .body
                .successor
                .next_slot
                .clone(),
            None => match &anchor {
                coven_protocol::store_commit::GrantStreamAnchor::StoreMembership { first_slot } => {
                    first_slot.clone()
                }
                coven_protocol::store_commit::GrantStreamAnchor::OwnerRecovery { .. } => {
                    panic!("test membership author has a recovery stream anchor")
                }
                coven_protocol::store_commit::GrantStreamAnchor::CircleControl { .. }
                | coven_protocol::store_commit::GrantStreamAnchor::CircleRoster { .. }
                | coven_protocol::store_commit::GrantStreamAnchor::CircleMetadata { .. } => {
                    panic!("test membership author has a Circle stream anchor")
                }
            },
        };
        let (entry_object, entry_ref) = coven_storage::prepare_membership_entry(
            cloud_storage,
            store.root().store_root_hash,
            &entry,
        )
        .await
        .expect("prepare exact membership entry");
        cloud_storage
            .create_protocol_object(&entry_object)
            .await
            .expect("publish exact membership entry");

        let context = ProtocolObjectContext::signed_plaintext(
            store.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let next_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord
                .seq
                .checked_add(1)
                .expect("membership sequence overflow"),
        );
        let next_slot = cloud_storage
            .allocate_protocol_slot(&context, &next_prefix, ".json")
            .await
            .expect("allocate exact membership successor slot");
        let head = AuthorHead::signed(
            entry.store_id.clone(),
            coven_protocol::membership::MembershipHeadBody {
                author_registration: registration_ref.clone(),
                entry: entry_ref,
                predecessor: predecessor.clone(),
                resolutions: entry.resolution_dependencies.clone(),
                successor: SuccessorLink {
                    activation: StreamActivation::grant_authorized(
                        store.root().store_root_hash,
                        registration_ref.clone(),
                        coord.author_owner_grant.clone(),
                        anchor.clone(),
                    )
                    .activation_id(),
                    predecessor: predecessor
                        .as_ref()
                        .map(|reference| reference.object.clone()),
                    next_slot,
                },
            },
            coven_protocol::membership::MembershipHeadActivation::Direct,
            &device_signer,
        );
        assert!(head.verify(&registration));
        let prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        );
        let prepared = cloud_storage
            .prepare_protocol_object(
                &context,
                current_slot,
                &prefix,
                serde_json::to_vec(&head).expect("serialize exact membership head"),
            )
            .expect("prepare exact membership head");
        cloud_storage
            .create_protocol_object(&prepared)
            .await
            .expect("publish exact membership head");
        chain
            .add_entry(entry)
            .expect("extend exact membership test chain");
        chain
            .activate_head_ref(MembershipHeadRef {
                coord,
                head_hash: head.head_hash(),
                object: prepared.reference().clone(),
            })
            .expect("activate exact membership test head");
    }
}

struct ExactPublishedCommit<'storage> {
    store: &'storage TestStore,
    cloud_storage: &'storage CloudSyncConnection,
    reference: coven_protocol::store_commit::StoreBatchCommitRef,
    commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    registration: coven_protocol::store_commit::StoreDeviceRegistration,
    device_signer: UserKeypair,
    head: StoreDeviceHead,
    head_object: coven_protocol::objects::ExactObjectRef,
}

impl<'storage> ExactPublishedCommit<'storage> {
    fn sign_commit_with_package(
        &self,
        schema_version: u32,
        membership_authority: coven_protocol::store_commit::StoreOperationMembershipAuthority,
        package_bytes: &[u8],
        package_object: coven_protocol::objects::ExactObjectRef,
    ) -> coven_protocol::store_commit::StoreBatchCommit {
        coven_protocol::store_commit::StoreBatchCommit::signed_operations(
            self.commit.store_root_hash,
            self.commit.write_id.clone(),
            self.reference.coord.clone(),
            self.commit.author_registration.clone(),
            &self.registration,
            self.commit.order.clone(),
            self.commit.membership_state.clone(),
            self.commit.device_state.clone(),
            membership_authority,
            coven_protocol::store_commit::StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: self.commit.control().cloned(),
                device_join_attempt_decisions: self.commit.device_join_attempt_decisions().to_vec(),
                device_join_outcomes: self.commit.device_join_outcomes().to_vec(),
                device_join_cleanup_receipts: self.commit.device_join_cleanup_receipts().to_vec(),
                provider_access_grants: self.commit.provider_access_grants().to_vec(),
                device_registrations: self.commit.device_registrations().to_vec(),
                device_exclusion_proposals: self.commit.device_exclusion_proposals().to_vec(),
                device_exclusion_outcomes: self.commit.device_exclusion_outcomes().to_vec(),
                stream_activations: self.commit.stream_activations().to_vec(),
                circle_controls: self.commit.circle_controls().to_vec(),
                store_package: Some(coven_protocol::store_commit::StorePackageInput {
                    candidate_family: self.commit.candidate_family(),
                    schema_version,
                    bytes: package_bytes,
                    object: package_object,
                }),
                circle_packages: &[],
            },
            &self.device_signer,
        )
        .expect("re-sign exact Store commit")
    }

    async fn load(
        store: &'storage TestStore,
        cloud_storage: &'storage CloudSyncConnection,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
        identity: &UserKeypair,
    ) -> Self {
        Self::load_as(store, cloud_storage, reference, identity).await
    }

    async fn load_as(
        store: &'storage TestStore,
        cloud_storage: &'storage CloudSyncConnection,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
        identity: &UserKeypair,
    ) -> Self {
        use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
        use coven_protocol::store_commit::{
            head_slot_prefix, StoreDeviceHead, StoreDeviceRegistration,
        };

        let commit = store
            .load_commit_for_test(&reference)
            .await
            .expect("verify exact published Store commit");
        let registration = commit.author().clone();
        let device_signer = registration
            .device_signer(identity)
            .expect("derive exact published Store device signer");
        let coven_protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot } =
            &registration.store_commits
        else {
            panic!("pull test registration has a Store announcement anchor")
        };
        let head_context = ProtocolObjectContext::signed_plaintext(
            store.root().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let mut slot = first_slot.clone();
        let mut sequence = 1_u64;
        let (head, head_object) = loop {
            let prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
            let (bytes, object) = cloud_storage
                .read_protocol_slot(&head_context, &slot, &prefix)
                .await
                .expect("read exact published Store head");
            let head: StoreDeviceHead =
                serde_json::from_slice(&bytes).expect("parse exact published Store head");
            if sequence == reference.coord.sequence() {
                assert_eq!(head.commit, reference);
                break (head, object);
            }
            slot = head.successor.next_slot.clone();
            sequence = sequence
                .checked_add(1)
                .expect("Store head sequence overflow");
        };
        assert!(head
            .author_registration
            .verify_registration(&registration)
            .is_ok());
        let _: StoreDeviceRegistration = registration.clone();
        Self {
            store,
            cloud_storage,
            reference,
            commit,
            registration,
            device_signer,
            head,
            head_object,
        }
    }
    async fn replace_commit_bytes(
        &self,
        commit_bytes: Vec<u8>,
        commit_hash: coven_protocol::store_commit::ObjectHash,
        head_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        head_signer: &UserKeypair,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef {
        let reference = self
            .publish_replacement_commit(commit_bytes, commit_hash)
            .await;
        self.replace_commit_head(reference.clone(), head_registration, head_signer)
            .await;
        reference
    }

    async fn publish_replacement_commit(
        &self,
        commit_bytes: Vec<u8>,
        commit_hash: coven_protocol::store_commit::ObjectHash,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef {
        use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};

        let stream_id = commit_stream_id(&self.reference);
        let commit_context = ProtocolObjectContext::signed_plaintext(
            self.store.root().store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let semantic_prefix = coven_protocol::store_commit::commit_semantic_prefix(
            self.commit.candidate_family(),
            &stream_id,
            self.reference.coord.sequence(),
            commit_hash,
        );
        let slot = self
            .cloud_storage
            .allocate_protocol_slot(&commit_context, &semantic_prefix, ".json")
            .await
            .expect("allocate replacement exact Store commit slot");
        let commit_prepared = self
            .cloud_storage
            .prepare_protocol_object(&commit_context, slot, &semantic_prefix, commit_bytes)
            .expect("prepare replacement exact Store commit");
        self.cloud_storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish replacement exact Store commit");
        let commit_object = commit_prepared.reference().clone();
        coven_protocol::store_commit::StoreBatchCommitRef {
            coord: self.reference.coord.clone(),
            commit_hash,
            object: commit_object,
        }
    }

    async fn replace_commit_head(
        &self,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
        head_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        head_signer: &UserKeypair,
    ) {
        use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};

        let head_context = ProtocolObjectContext::signed_plaintext(
            self.store.root().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        self.cloud_storage
            .delete_protocol_object(&self.head_object)
            .await
            .expect("delete replaced exact Store head");
        let head = StoreDeviceHead::signed(
            self.store.root().store_root_hash,
            head_registration,
            reference.clone(),
            self.head.successor.clone(),
            head_signer,
        )
        .expect("sign replacement exact Store head");
        let prefix = coven_protocol::store_commit::head_slot_prefix(
            &self.registration.device_id.to_string(),
            reference.coord.sequence(),
        );
        let head_prepared = self
            .cloud_storage
            .prepare_protocol_object(
                &head_context,
                self.head_object.slot().clone(),
                &prefix,
                head.to_bytes(),
            )
            .expect("prepare replacement exact Store head");
        self.cloud_storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish replacement exact Store head");
    }

    async fn replace_commit_bytes_before_validation(
        &self,
        commit_bytes: Vec<u8>,
        commit_hash: coven_protocol::store_commit::ObjectHash,
        head_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        head_signer: &UserKeypair,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef {
        let reference = self
            .publish_replacement_commit(commit_bytes, commit_hash)
            .await;
        self.replace_commit_head(reference.clone(), head_registration, head_signer)
            .await;
        reference
    }

    async fn replace_head(
        &self,
        commit: coven_protocol::store_commit::StoreBatchCommitRef,
        author_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        signer: &UserKeypair,
    ) {
        use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};

        let context = ProtocolObjectContext::signed_plaintext(
            self.store.root().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        self.cloud_storage
            .delete_protocol_object(&self.head_object)
            .await
            .expect("delete replaced exact Store head");
        let head = StoreDeviceHead::signed(
            self.store.root().store_root_hash,
            author_registration,
            commit,
            self.head.successor.clone(),
            signer,
        )
        .expect("sign replacement exact Store head");
        let prefix = coven_protocol::store_commit::head_slot_prefix(
            &self.registration.device_id.to_string(),
            self.reference.coord.sequence(),
        );
        let prepared = self
            .cloud_storage
            .prepare_protocol_object(
                &context,
                self.head_object.slot().clone(),
                &prefix,
                head.to_bytes(),
            )
            .expect("prepare replacement exact Store head");
        self.cloud_storage
            .create_protocol_object(&prepared)
            .await
            .expect("publish replacement exact Store head");
    }

    async fn resign_commit(
        &self,
        schema_version: u32,
        membership_authority: Option<coven_protocol::membership::MembershipGrantCreationAuthority>,
    ) -> coven_protocol::store_commit::StoreBatchCommit {
        let package = self
            .commit
            .store_package()
            .expect("test Store commit carries a Store package");
        let package_bytes = self
            .store
            .load_store_package_for_test(&self.reference)
            .await
            .expect("load exact Store package")
            .expect("exact Store package exists")
            .value;
        let predecessor = match &membership_authority {
            Some(authority) => authority.clone(),
            None => self
                .commit
                .membership_authority
                .clone()
                .expect("published Merge operations commit carries membership authority"),
        };
        let mut commit = self.sign_commit_with_package(
            schema_version,
            coven_protocol::store_commit::StoreOperationMembershipAuthority { predecessor },
            &package_bytes,
            package.object.clone(),
        );
        if membership_authority.is_none() {
            commit.body_mut().membership_authority = None;
            commit.resign(&self.device_signer);
        }
        commit
    }

    async fn replace_package_bytes(&self, bytes: Vec<u8>) {
        use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};

        let package = self
            .commit
            .store_package()
            .expect("test Store commit carries a Store package");
        let stream_id = commit_stream_id(&self.reference);
        let prefix = coven_protocol::store_commit::package_semantic_prefix(
            self.commit.candidate_family(),
            &stream_id,
            self.reference.coord.sequence(),
            package.content_hash,
        );
        let context = ProtocolObjectContext::store_encrypted(
            self.store.root().store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        self.cloud_storage
            .delete_protocol_object(&package.object)
            .await
            .expect("delete replaced exact Store package");
        let prepared = self
            .cloud_storage
            .prepare_protocol_object(&context, package.object.slot().clone(), &prefix, bytes)
            .expect("prepare replacement exact Store package");
        self.cloud_storage
            .create_protocol_object(&prepared)
            .await
            .expect("publish replacement exact Store package");
    }

    async fn replace_package_with_malformed_bytes(
        &self,
    ) -> coven_protocol::store_commit::StoreBatchCommitRef {
        let malformed = b"not a SQLite changeset";
        let stream_id = commit_stream_id(&self.reference);
        let package_object = self
            .store
            .create_exact_protocol_object(
                &coven_protocol::objects::ProtocolObjectContext::store_encrypted(
                    self.store.root().store_root_hash,
                    coven_protocol::objects::ProtocolObjectDomain::StorePackage,
                ),
                &coven_protocol::store_commit::package_semantic_prefix(
                    self.commit.candidate_family(),
                    &stream_id,
                    self.reference.coord.sequence(),
                    coven_protocol::store_commit::ObjectHash::digest(malformed),
                ),
                ".pkg",
                malformed,
            )
            .await
            .expect("publish malformed exact Store package");
        let malformed_commit = self.sign_commit_with_package(
            SCHEMA_VERSION,
            self.commit
                .operations_membership_authority()
                .expect("published test commit carries validated operations"),
            malformed,
            package_object,
        );
        self.replace_commit_bytes(
            malformed_commit.to_bytes(),
            malformed_commit.commit_hash(),
            self.head.author_registration.clone(),
            &self.device_signer,
        )
        .await
    }
}

struct FaultingStorage {
    membership_reads_until_failure: std::sync::atomic::AtomicUsize,
    fail_blob_read: std::sync::atomic::AtomicBool,
}

impl FaultingStorage {
    fn membership(read: usize) -> Self {
        Self {
            membership_reads_until_failure: std::sync::atomic::AtomicUsize::new(read),
            fail_blob_read: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn blob() -> Self {
        Self {
            membership_reads_until_failure: std::sync::atomic::AtomicUsize::new(0),
            fail_blob_read: std::sync::atomic::AtomicBool::new(true),
        }
    }

    fn arm_membership(&self, read: usize) {
        self.membership_reads_until_failure
            .store(read, std::sync::atomic::Ordering::SeqCst);
    }

    fn fail_membership_read(&self, semantic_prefix: &str) -> bool {
        if !semantic_prefix.starts_with("store-v1/membership/entries/")
            && !semantic_prefix.starts_with("store-v1/membership/heads/")
        {
            return false;
        }
        let remaining = self
            .membership_reads_until_failure
            .load(std::sync::atomic::Ordering::SeqCst);
        remaining > 0
            && self
                .membership_reads_until_failure
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                == 1
    }
}

#[async_trait]
impl crate::sync::test_helpers::StorageInterceptor for FaultingStorage {
    async fn before_protocol_read(
        &self,
        _read: crate::sync::test_helpers::ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if self.fail_membership_read(semantic_prefix) {
            return Err(coven_protocol::objects::StorageError::Storage(
                "forced exact membership read failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn before_blob_stage(&self) -> Result<(), coven_protocol::objects::StorageError> {
        if self
            .fail_blob_read
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(coven_protocol::objects::StorageError::Storage(
                "forced exact blob read failure".to_string(),
            ));
        }
        Ok(())
    }
}

struct MissingProtocolSlot {
    semantic_prefix: String,
}

#[async_trait]
impl StorageInterceptor for MissingProtocolSlot {
    async fn before_protocol_read(
        &self,
        read: ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if read == ProtocolRead::Slot && semantic_prefix == self.semantic_prefix {
            return Err(coven_protocol::objects::StorageError::NotFound(format!(
                "lagging provider omits {semantic_prefix}"
            )));
        }
        Ok(())
    }
}

#[tokio::test]
async fn pull_applies_remote_changeset_and_surfaces_row_changes() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

    // Source device records a note as changeset seq 1.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("founder", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish founder changeset");
    let stream_id = commit_stream_id(&commit);

    // Second device pulls.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let ld = db2_store_dir.clone();
    let (updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get(&stream_id), Some(&1));
    assert_eq!(
        db2.materialized_sequences().await.get(&stream_id),
        Some(&1),
        "the row and its durable position commit in the pull that applies it",
    );
    assert_eq!(
        db2.query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "First"
    );
    assert!(result
        .row_changes
        .iter()
        .any(|c| c.table == "notes" && c.pk() == Some("n1")));
}

#[tokio::test]
async fn position_write_failure_rolls_back_the_remote_rows() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Remote', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    storage
        .publish_changeset("founder", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish founder changeset");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    target
        .execute_test_sql(
            "CREATE TEMP TRIGGER reject_materialized_insert BEFORE INSERT ON materialized_commits \
         BEGIN SELECT RAISE(ABORT, 'injected materialized-position write failure'); END;",
        )
        .await;
    let error = storage
        .pull_into_result(&target, &target_store_dir)
        .await
        .expect_err("materialized-position failure aborts the pull");
    assert!(
        matches!(error, TestPullError::Pull(_)),
        "unexpected pull error: {error:?}"
    );
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "the row cannot commit when its position write fails",
    );
    assert!(target.materialized_sequences().await.is_empty());
}

#[tokio::test]
async fn ordinary_pull_starts_from_its_durable_position() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('stale-row', 'Remote', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("founder", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish founder changeset");
    let stream_id = commit_stream_id(&commit);

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let store_dir = target_store_dir.clone();
    let (updated, result) = storage.pull_into(&target, &store_dir).await;

    assert_eq!(updated.get(&stream_id), Some(&1));
    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_positions.is_empty());
    assert_eq!(
        target.materialized_sequences().await.get(&stream_id),
        Some(&1),
    );
    assert!(
        target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'stale-row'")
            .await,
        "ordinary pull derives coverage from durable rows, not caller input",
    );
}

#[tokio::test]
async fn ordinary_pull_uses_its_durable_position_on_every_call() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let first = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('position-row', 'One', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let second = source
        .capture_test_changeset(&["UPDATE notes SET title = 'Two', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'position-row'"])
        .await;
    let first_commit = storage
        .publish_changeset("dev1", 1, &first, SCHEMA_VERSION)
        .await
        .expect("publish first exact Store changeset");
    let stream_id = commit_stream_id(&first_commit);

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let store_dir = target_store_dir.clone();
    storage.pull_into(&target, &store_dir).await;
    storage
        .publish_changeset("dev1", 2, &second, SCHEMA_VERSION)
        .await
        .expect("publish second exact Store changeset");

    let (updated, result) = storage.pull_into(&target, &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_positions.is_empty());
    assert_eq!(updated.get(&stream_id), Some(&2));
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'position-row'")
            .await,
        "Two",
    );
}

#[tokio::test]
async fn ordinary_pull_applies_the_change_immediately_after_its_durable_position() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let first = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('next-row', 'One', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let second = source
        .capture_test_changeset(&["UPDATE notes SET title = 'Two', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'next-row'"])
        .await;
    let first_commit = storage
        .publish_changeset("dev1", 1, &first, SCHEMA_VERSION)
        .await
        .expect("publish first exact Store changeset");
    let stream_id = commit_stream_id(&first_commit);

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let store_dir = target_store_dir.clone();
    storage.pull_into(&target, &store_dir).await;
    storage
        .publish_changeset("dev1", 2, &second, SCHEMA_VERSION)
        .await
        .expect("publish second exact Store changeset");

    let (updated, result) = storage.pull_into(&target, &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get(&stream_id), Some(&2));
    assert_eq!(
        target.materialized_sequences().await.get(&stream_id),
        Some(&2),
    );
}

#[tokio::test]
async fn invalid_materialized_positions_are_rejected_at_the_database_boundary() {
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let invalid_insert = target.insert_invalid_materialized_commit_for_test().await;
    assert!(invalid_insert.is_err());
    assert!(store_database(&target)
        .materialized_frontier()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store_database(&target)
            .snapshot_coverage_frontier()
            .await
            .unwrap(),
        coven_protocol::CommitFrontier(std::collections::BTreeMap::new()),
    );
}

#[tokio::test]
async fn merge_materialization_retains_closed_input_and_rejects_corruption_after_reopen() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('retained-row', 'Retained', NULL, \
                     '0000000001000-0000-retained', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("retained", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish retained-input fixture");
    let stream_id = commit_stream_id(&commit);
    let target_dir = tempfile::tempdir().expect("create retained-input database directory");
    let target_path = target_dir.path().join("target.sqlite");
    let target_store_dir = crate::sync::test_helpers::store_dir_for_test_database(&target_path);
    let open_target = || {
        Database::open_synthetic_for_test(
            &target_path,
            target_store_dir.clone(),
            test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "test-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &test_migrations(),
        )
    };
    let target = open_target().expect("open retained-input target");

    storage.pull_into(&target, &target_store_dir).await;

    let queried_stream = stream_id.clone();
    let (canonical_input, input_hash, retained_ref) = target
        .retained_materialization_input_for_test(queried_stream, 1)
        .await
        .expect("read retained Merge materialization input");
    assert_eq!(
        input_hash,
        coven_protocol::store_commit::ObjectHash::digest(&canonical_input).to_string()
    );
    assert_eq!(
        retained_ref,
        serde_json::to_string(&commit).expect("serialize retained fixture commit ref")
    );
    let retained: serde_json::Value =
        serde_json::from_slice(&canonical_input).expect("parse retained Merge input");
    let retained = retained.as_object().expect("retained input is an object");
    assert_eq!(
        retained
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "activation",
            "activation_head",
            "commit",
            "history_evidence",
            "membership_objects",
            "packages",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );
    let retained_store = retained["packages"]
        .as_array()
        .expect("retained packages are an array")
        .first()
        .and_then(|value| value.get("store"))
        .expect("retained Store package is exact-reference tagged");
    let package_ref: coven_protocol::store_commit::StorePackageRef =
        serde_json::from_value(retained_store["reference"].clone())
            .expect("parse retained Store package ref");
    let package_remote = target.stored_remote_object(&package_ref.object).await;
    let parsed_input_hash = input_hash.parse().expect("parse retained input hash");
    assert!(matches!(
        package_remote,
        coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::SharedLiveSetObjectDomain::StorePackage {
                    reference
                } if reference == &package_ref
            )
                && record.payloads
                    == coven_protocol::remote_object::RemoteObjectPayloads::SpooledExternal
                && record.identity.object == package_ref.object
                && matches!(
                    &record.state,
                    coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.activated.contains(
                        &coven_protocol::remote_object::SharedObjectOwner::StoreCommit(commit.clone())
                    ) && ownership.activated.contains(
                        &coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(
                            coven_protocol::remote_object::RetainedReplayOwner::Commit {
                                commit: commit.clone(),
                                input_hash: parsed_input_hash,
                            }
                        )
                    )
                )
    ));
    let activation = retained["activation"]
        .as_object()
        .expect("retained activation input is an object");
    assert_eq!(
        activation
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "circle_activations",
            "device_operations",
            "package_application",
            "registrations",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );
    let receiver_wall_ms = activation["package_application"]["received"]["receiver_wall_ms"]
        .as_u64()
        .expect("retained package has its receive-time conflict bound");

    let encoded = String::from_utf8(canonical_input.clone()).expect("retained input is UTF-8");
    let package_application = format!(
        ",\"package_application\":{{\"received\":{{\"receiver_wall_ms\":{receiver_wall_ms}}}}}"
    );
    assert!(encoded.contains(&package_application));
    let missing_receiver = encoded.replacen(&package_application, "", 1).into_bytes();
    target
        .replace_retained_merge_input(stream_id.clone(), missing_receiver)
        .await;
    let target_store = storage
        .bind_founder_device(&target, target_store_dir.clone())
        .await
        .expect("load retained materialization Store");
    let error = match target_store
        .retained_merge_materialization_for_test(commit.clone())
        .await
    {
        Ok(_) => panic!("a retained package must carry its receive-time conflict bound"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("package application does not match its applied packages"));

    target
        .replace_retained_merge_input(stream_id.clone(), canonical_input.clone())
        .await;

    let verified_before_corruption = target_store
        .retained_merge_replay_inputs_for_test()
        .await
        .expect("load verified retained Merge history")
        .into_iter()
        .map(|materialization| {
            (
                materialization.commit_ref().clone(),
                materialization.input_hash(),
            )
        })
        .collect::<Vec<_>>();
    let corrupt_stream = stream_id.clone();
    target
        .corrupt_retained_materialization_input_for_test(corrupt_stream, 1)
        .await
        .expect("corrupt retained Merge input");
    let verified_after_corruption = target_store
        .retained_merge_replay_inputs_for_test()
        .await
        .expect("the open connection retains its verified Merge history")
        .into_iter()
        .map(|materialization| {
            (
                materialization.commit_ref().clone(),
                materialization.input_hash(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        verified_after_corruption, verified_before_corruption,
        "raw backing-byte changes cannot replace connection-owned verified history"
    );
    drop(target_store);
    drop(target);
    let reopened = open_target().expect("reopen retained-input target");
    let reopened_store = storage
        .bind_founder_device(&reopened, target_store_dir.clone())
        .await
        .expect("bind reopened retained materialization Store");
    let error = match reopened_store.retained_merge_replay_inputs_for_test().await {
        Ok(_) => panic!("corrupt retained Merge input must fail history verification after reopen"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("input hash differs from its bytes"));
}

#[tokio::test]
async fn merge_materialization_rejects_missing_tampered_and_invented_replay_pins() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let first_changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('pin-first', 'First', NULL, \
                     '0000000001000-0000-pins', '2026-01-01')",
        ])
        .await;
    let first = storage
        .publish_changeset("pins", 1, &first_changeset, SCHEMA_VERSION)
        .await
        .expect("publish first replay-pin fixture");
    let second_changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('pin-second', 'Second', NULL, \
                     '0000000002000-0000-pins', '2026-01-01')",
        ])
        .await;
    let second = storage
        .publish_changeset("pins", 2, &second_changeset, SCHEMA_VERSION)
        .await
        .expect("publish second replay-pin fixture");
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    storage.pull_into(&target, &target_store_dir).await;

    let (_first_owner, first_package, first_remote) = target
        .retained_store_package_pin_for_test(&first)
        .await
        .expect("load first retained Store package pin");
    let (second_owner, second_package, second_remote) = target
        .retained_store_package_pin_for_test(&second)
        .await
        .expect("load second retained Store package pin");

    let mut missing = second_remote.clone();
    let coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut missing
    else {
        unreachable!("retained package is shared")
    };
    let coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &mut record.state
    else {
        unreachable!("retained package is activated")
    };
    assert!(ownership.activated.remove(
        &coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(second_owner.clone())
    ));
    target
        .replace_stored_remote_object(&second_package.object, &missing)
        .await;
    let root = storage.root().clone();
    assert!(target
        .validate_retained_merge_replay_for_test(root)
        .await
        .expect_err("missing replay pin must fail durable retained-history verification")
        .to_string()
        .contains("retained-replay ownership index"));
    target
        .replace_stored_remote_object(&second_package.object, &second_remote)
        .await;

    let mut tampered = missing;
    let coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut tampered
    else {
        unreachable!("retained package is shared")
    };
    let coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &mut record.state
    else {
        unreachable!("retained package is activated")
    };
    let coven_protocol::remote_object::RetainedReplayOwner::Commit { commit, .. } = &second_owner;
    ownership.activated.insert(
        coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(
            coven_protocol::remote_object::RetainedReplayOwner::Commit {
                commit: commit.clone(),
                input_hash: coven_protocol::store_commit::ObjectHash::digest(b"tampered input"),
            },
        ),
    );
    target
        .replace_stored_remote_object(&second_package.object, &tampered)
        .await;
    let root = storage.root().clone();
    assert!(target
        .validate_retained_merge_replay_for_test(root)
        .await
        .expect_err("tampered replay pin must fail durable retained-history verification")
        .to_string()
        .contains("retained-replay ownership index"));
    target
        .replace_stored_remote_object(&second_package.object, &second_remote)
        .await;

    let mut invented = first_remote;
    let coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut invented
    else {
        unreachable!("retained package is shared")
    };
    let coven_protocol::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &mut record.state
    else {
        unreachable!("retained package is activated")
    };
    ownership.activated.insert(
        coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(second_owner.clone()),
    );
    target
        .replace_stored_remote_object(&first_package.object, &invented)
        .await;
    let second_owner_for_insert = second_owner.clone();
    let first_object = first_package.object.clone();
    target
        .insert_retained_replay_object_for_test(second_owner_for_insert, first_object)
        .await
        .expect("invent replay ownership index row");
    assert!(target
        .store_package_is_retained_for_replay_for_test(first_package, first)
        .await
        .expect_err("invented replay pin must block reclamation validation")
        .to_string()
        .contains("ownership differs from its exact object closure"));
    let root = storage.root().clone();
    assert!(target
        .validate_retained_merge_replay_for_test(root)
        .await
        .expect_err("invented replay pin must fail durable retained-history verification")
        .to_string()
        .contains("ownership differs from its exact object closure"));
}

#[tokio::test]
async fn retained_input_collision_rolls_back_remote_rows_and_materialization() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('rollback-row', 'Must roll back', NULL, \
                     '0000000001000-0000-rollback', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("rollback", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish retained-input rollback fixture");
    let stream_id = commit_stream_id(&commit);
    let target_dir = tempfile::tempdir().expect("create retained collision database directory");
    let target_path = target_dir.path().join("store.sqlite");
    let target_store_dir = crate::sync::test_helpers::store_dir_for_test_database(&target_path);
    let copied_path = target_path.clone();
    source
        .vacuum_into_for_test(copied_path.to_string_lossy().into_owned())
        .await
        .expect("copy the locally-authored retained input");
    crate::sync::test_helpers::copy_payload_files(&source_store_dir, &target_store_dir);
    let target = coven_database::Database::open_synthetic_for_test(
        &target_path,
        target_store_dir.clone(),
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &test_migrations(),
    )
    .expect("open copied retained collision database");
    let conflicting_stream = stream_id.clone();
    target
        .remove_materialized_note_for_test(conflicting_stream, 1, "rollback-row".to_string())
        .await
        .expect("remove the locally materialized outcome while retaining its exact input");

    let error = storage
        .pull_into_result(&target, &target_store_dir)
        .await
        .expect_err("retained input collision must fail the pull transaction");
    assert!(
        error
            .to_string()
            .contains("already contains different exact input"),
        "unexpected pull error: {error:?}"
    );
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'rollback-row'")
            .await
    );
    let checked_stream = stream_id.clone();
    let materialized = target
        .materialized_commit_exists_for_test(checked_stream, 1)
        .await
        .expect("read rolled-back materialization state");
    assert!(!materialized);
}

#[tokio::test]
async fn empty_package_materializes_its_exact_commit_position() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let commit = storage
        .publish_changeset("dev1", 1, &[], SCHEMA_VERSION)
        .await
        .expect("publish empty exact Store changeset");
    let stream_id = commit_stream_id(&commit);
    let store_dir = target_store_dir.clone();

    let (updated, result) = storage.pull_into(&target, &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get(&stream_id), Some(&1));
    assert_eq!(
        target.materialized_sequences().await.get(&stream_id),
        Some(&1),
    );
}

#[tokio::test]
async fn host_write_after_remote_apply_observes_the_matching_position() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('remote', 'Remote', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("dev1", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let store_dir = target_store_dir.clone();
    storage.pull_into(&target, &store_dir).await;

    coven_database::StoreDatabase::new(&target)
        .run_host_store_write_for_test(None, None, move |tx| {
            let remote_row: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM notes WHERE id = 'remote')",
                    [],
                    |row| row.get(0),
                )
                .map_err(coven_database::DbError::from)?;
            let materialized = tx.materialized_sequence(&stream_id)?;
            assert!(remote_row, "the host transaction observes the remote row");
            assert_eq!(
                materialized,
                Some(1),
                "the same database cut observes the row's materialized position",
            );
            tx.execute(
                "INSERT INTO notes \
                         (id, title, body, _updated_at, created_at) \
                         VALUES ('local', 'Local', NULL, \
                                 '0000000002000-0000-dev2', '2026-01-01')",
                [],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        })
        .await
        .expect("host write after remote apply");

    assert!(
        target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'local'")
            .await
    );
}

/// A changeset whose object was reclaimed (deleted as superseded) past this
/// device's position surfaces a `MissingChangeset` held reason and holds the
/// position at the gap — the host reports reclaimed history rather than a generic
/// stall, and the device stream never advances over a changeset it did not apply.
#[tokio::test]
async fn pull_holds_and_names_a_reclaimed_changeset_gap() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), founder.clone()).await;

    // The source device's head advertises seq 1, but the changeset object is
    // gone: reclamation deleted it as superseded. `store_changeset` both writes
    // the object and advances the head to seq 1; deleting the object leaves the
    // head pointing past a hole.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, commit, &founder).await;
    let package = graph
        .commit
        .store_package()
        .expect("Store commit carries a Store package");
    cloud_storage
        .delete_protocol_object(&package.object)
        .await
        .expect("delete exact Store package");

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let ld = db2_store_dir.clone();
    let (updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0].coordinate,
        HeldStoreCoordinate::Package {
            device_id,
            seq: 1,
            ..
        } if device_id == &stream_id
    ));
    assert!(matches!(
        &result.held_positions[0].reason,
        HeldStorePositionReason::ObjectUnreadableStorage { key, source }
            if key == "exact Store object" && source.to_string().contains("object not found")
    ));
    // The position holds at the gap: dev1 never advances over the unapplied seq.
    assert_eq!(updated.get(&stream_id).copied().unwrap_or(0), 0);
}

/// A package whose changeset carries a non-canonical primary key for an
/// `IndependentUuid` table (SQLite accepts any TEXT id, and the session capture
/// does not validate identity) is held on the pull path — not applied, not
/// hard-failed — so the stream stalls at the tampered position instead of
/// admitting an id the local contract forbids.
#[tokio::test]
async fn pull_holds_a_non_canonical_uuid_row_identity() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = uuid_note_db(db1_store_dir.clone());
    let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;
    let cs = db1
        .capture_test_changeset(&["INSERT INTO uuid_notes (id, title, _updated_at) \
             VALUES ('NOT-A-CANONICAL-UUID', 'Forged', '0000000001000-0000-dev1')"])
        .await;
    let commit = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = uuid_note_db(db2_store_dir.clone());
    let ld = db2_store_dir.clone();
    let (updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0].reason,
        HeldStorePositionReason::InvalidRowIdentity(error) if error.table() == "uuid_notes"
    ));
    assert!(
        !db2.test_row_exists("SELECT 1 FROM uuid_notes WHERE id = 'NOT-A-CANONICAL-UUID'")
            .await
    );
    assert_eq!(updated.get(&stream_id).copied().unwrap_or(0), 0);
}

/// The converged state of `n1` (`Some(title)` if present, `None` if deleted)
/// after a receiver pulls a concurrent delete and edit of the row in the given
/// arrival order. The delete is authored by the founder (sequence 2, following
/// the shared insert); the edit is authored by a concurrent second device with a
/// strictly later stamp. Only two devices participate so the device-join observer
/// is unambiguously the founder (the provider administrator).
async fn delete_edit_converged_state(delete_first: bool) -> Option<String> {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;

    let insert = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'original', NULL, 1, '0000000001000-0000-founder', '2026-01-01')",
        ])
        .await;
    storage
        .publish_changeset("founder", 1, &insert, SCHEMA_VERSION)
        .await
        .expect("publish shared row");

    // The founder's own delete (captured from a private db so its old image is the
    // shared row) and a concurrent second device's later edit.
    let deleter_store_dir = crate::sync::test_helpers::test_store_dir();
    let deleter = crate::sync::test_helpers::open_test_db(deleter_store_dir.clone());
    deleter
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'original', NULL, 1, '0000000001000-0000-founder', '2026-01-01')",
        )
        .await;
    let delete = deleter
        .capture_test_changeset(&["DELETE FROM notes WHERE id = 'n1'"])
        .await;

    let editor_store_dir = crate::sync::test_helpers::test_store_dir();
    let editor = crate::sync::test_helpers::open_test_db(editor_store_dir.clone());
    editor
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'original', NULL, 1, '0000000001000-0000-founder', '2026-01-01')",
        )
        .await;
    let edit = editor
        .capture_test_changeset(&["UPDATE notes SET title = 'edited', \
           _updated_at = '0000000009000-0000-editor' WHERE id = 'n1'"])
        .await;

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let ld = target_store_dir.clone();
    storage.pull_into(&target, &ld).await;

    // Deliver the two concurrent commits one at a time, in the chosen arrival order.
    // The founder stream advances through Store setup and the editor's device join,
    // so its delete sequence is resolved live rather than hard-coded.
    if delete_first {
        let delete_seq = storage
            .next_commit_sequence("founder")
            .await
            .expect("read founder delete sequence");
        storage
            .publish_changeset("founder", delete_seq, &delete, SCHEMA_VERSION)
            .await
            .expect("publish founder delete");
        storage.pull_into(&target, &ld).await;
        storage
            .publish_changeset("editor", 1, &edit, SCHEMA_VERSION)
            .await
            .expect("publish concurrent editor edit");
    } else {
        storage
            .publish_changeset("editor", 1, &edit, SCHEMA_VERSION)
            .await
            .expect("publish concurrent editor edit");
        storage.pull_into(&target, &ld).await;
        let delete_seq = storage
            .next_commit_sequence("founder")
            .await
            .expect("read founder delete sequence");
        storage
            .publish_changeset("founder", delete_seq, &delete, SCHEMA_VERSION)
            .await
            .expect("publish founder delete");
    }
    let (_updated, result) = storage.pull_into(&target, &ld).await;
    assert!(
        result.held_positions.is_empty(),
        "neither concurrent commit is held: {:?}",
        result.held_positions,
    );

    if target
        .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
        .await
    {
        Some(
            target
                .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
                .await,
        )
    } else {
        None
    }
}

/// A concurrent delete and edit of one row must converge to the same result
/// whichever the receiver pulls first. Store canonical replay is what has to
/// compensate for arrival order; a divergence here would be a real convergence
/// bug, not a test artifact.
#[tokio::test]
async fn delete_and_edit_conflict_converges_across_arrival_orders() {
    let delete_first = delete_edit_converged_state(true).await;
    let edit_first = delete_edit_converged_state(false).await;
    assert_eq!(
        delete_first, edit_first,
        "a delete/edit conflict converges to one state regardless of arrival order",
    );
}

#[tokio::test]
async fn uniqueness_conflict_rolls_back_the_entire_changeset_and_position() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = unique_note_db(db1_store_dir.clone());
    let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('would-partially-land', 'free-slug', 'First row', \
                     '0000000000900-0000-dev1', '2026-01-01')",
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('remote', 'same-slug', 'Remote', '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let commit = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = unique_note_db(db2_store_dir.clone());
    db2.execute_test_sql(
        "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
         VALUES ('local', 'same-slug', 'Local', '0000000002000-0000-dev2', '2026-01-01')",
    )
    .await;
    let ld = db2_store_dir.clone();
    let (updated, result) = storage.pull_into(&db2, &ld).await;

    let conflicts = constraint_conflicts(&result);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        conflicts[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id: stream_id.clone(),
            commit: commit.clone(),
        }
    );
    assert_eq!(
        conflicts[0].reason,
        HeldStorePositionReason::ConstraintConflict(vec!["unique_notes".to_string()])
    );
    assert_eq!(updated.get(&stream_id), None);
    assert_eq!(
        db2.materialized_sequences().await.get(&stream_id),
        None,
        "a rejected changeset has no durable position",
    );
    assert!(
        db2.test_row_exists("SELECT 1 FROM unique_notes WHERE id = 'local'")
            .await
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM unique_notes WHERE id = 'remote'")
            .await
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM unique_notes WHERE id = 'would-partially-land'",)
            .await,
        "rows before the constraint conflict roll back with the rejected changeset",
    );
}

#[tokio::test]
async fn non_retryable_constraint_is_reported_even_when_the_changeset_also_violates_a_foreign_key()
{
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = mixed_constraint_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    source
        .execute_test_sql(
            "INSERT INTO constraint_parents (id, _updated_at) \
         VALUES ('missing-on-target', '0000000001000-0000-dev1'); \
         INSERT INTO constraint_parents (id, _updated_at) \
         VALUES ('present-on-target', '0000000001000-0000-dev1')",
        )
        .await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO constraint_items (id, parent_id, slug, _updated_at) \
             VALUES ('fk-row', 'missing-on-target', 'free-slug', \
                     '0000000002000-0000-dev1')",
            "INSERT INTO constraint_items (id, parent_id, slug, _updated_at) \
             VALUES ('unique-row', 'present-on-target', 'duplicate-slug', \
                     '0000000002001-0000-dev1')",
        ])
        .await;
    storage
        .publish_changeset("dev1", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = mixed_constraint_db(target_store_dir.clone());
    target
        .execute_test_sql(
            "INSERT INTO constraint_parents (id, _updated_at) \
         VALUES ('present-on-target', '0000000001000-0000-dev2'); \
         INSERT INTO constraint_items (id, parent_id, slug, _updated_at) \
         VALUES ('local-row', 'present-on-target', 'duplicate-slug', \
                 '0000000003000-0000-dev2')",
        )
        .await;
    let store_dir = target_store_dir.clone();

    let (updated, result) = storage.pull_into(&target, &store_dir).await;

    assert_eq!(result.changesets_applied, 0);
    let conflicts = constraint_conflicts(&result);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        conflicts[0].reason,
        HeldStorePositionReason::ConstraintConflict(vec!["constraint_items".to_string()])
    );
    assert_eq!(updated.get("dev1"), None);
    assert_eq!(target.materialized_sequences().await.get("dev1"), None);
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM constraint_items WHERE id = 'fk-row'")
            .await
    );
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM constraint_items WHERE id = 'unique-row'")
            .await
    );
    assert!(
        target
            .test_row_exists("SELECT 1 FROM constraint_items WHERE id = 'local-row'")
            .await
    );
}

#[tokio::test]
async fn fk_violation_still_retries_and_resolves() {
    let child_source_store_dir = crate::sync::test_helpers::test_store_dir();
    let child_source = crate::sync::test_helpers::open_test_db(child_source_store_dir.clone());
    let storage = create_store(
        &child_source,
        child_source_store_dir.clone(),
        UserKeypair::generate(),
    )
    .await;
    storage
        .device_id("dev-child")
        .await
        .expect("activate child producer before publishing data");
    storage
        .device_id("dev-parent")
        .await
        .expect("activate parent producer before publishing data");
    let child_sequence = storage
        .next_commit_sequence("dev-child")
        .await
        .expect("load child producer position");
    let parent_sequence = storage
        .next_commit_sequence("dev-parent")
        .await
        .expect("load parent producer position");

    // The parent seed exists in the db so the child's FK is satisfiable, but goes
    // through raw `exec` (not the journal), so it never enters the captured child
    // changeset — the child ships the tag alone, FK-violating until the parent
    // arrives.
    child_source
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Parent', NULL, '0000000001000-0000-parent', '2026-01-01')",
        )
        .await;
    let child_cs = child_source
        .capture_test_changeset(&[
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('t1', 'n1', 'green', '0000000001001-0000-child', '2026-01-01')",
        ])
        .await;
    let child_commit = storage
        .publish_changeset("dev-child", child_sequence, &child_cs, SCHEMA_VERSION)
        .await
        .expect("publish child exact Store changeset");

    let parent_source_store_dir = crate::sync::test_helpers::test_store_dir();
    let parent_source = crate::sync::test_helpers::open_test_db(parent_source_store_dir.clone());
    let parent_cs = parent_source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Parent', NULL, '0000000001000-0000-parent', '2026-01-01')",
        ])
        .await;
    let parent_commit = storage
        .publish_changeset("dev-parent", parent_sequence, &parent_cs, SCHEMA_VERSION)
        .await
        .expect("publish parent exact Store changeset");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let ld = target_store_dir.clone();
    let (_, first) = storage.pull_into(&target, &ld).await;
    assert!(first
        .held_positions
        .iter()
        .any(|held| held.reason == HeldStorePositionReason::ForeignKeyDependency));
    let (updated, result) = storage.pull_into(&target, &ld).await;

    assert_eq!(
        updated.get(&commit_stream_id(&child_commit)),
        Some(&child_commit.coord.sequence()),
    );
    assert_eq!(
        updated.get(&commit_stream_id(&parent_commit)),
        Some(&parent_commit.coord.sequence()),
    );
    assert_eq!(
        result.changesets_applied,
        parent_commit.coord.sequence() + 1,
    );
    assert!(constraint_conflicts(&result).is_empty());
    assert_eq!(
        target
            .query_test_text("SELECT tag FROM note_tags WHERE id = 't1'")
            .await,
        "green"
    );
}

/// The schema-version gate reads `env.schema_version` to classify a held stream
/// as routine version skew (the peer upgraded past us), so it must run only on an
/// authenticated envelope. A forged object carrying a large `schema_version` and
/// an invalid signature must surface as tamper — an invalid signature — not be
/// laundered into the benign `skipped_schema` count, where a host waits for an
/// upgrade that will never resolve it while the real signal is never raised.
#[tokio::test]
async fn a_forged_newer_schema_changeset_reports_tamper_not_a_schema_skip() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), founder.clone()).await;
    // A changeset stamped one schema version above the local db, signed at its own
    // position so the position check passes and the loop reaches the signature and
    // schema checks.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'ForgedFuture', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let commit = graph
        .resign_commit(
            SCHEMA_VERSION + 1,
            graph.commit.membership_authority.clone(),
        )
        .await;
    let mut forged: serde_json::Value = serde_json::from_slice(&commit.to_bytes()).unwrap();
    forged["signature"] = serde_json::Value::String("0".repeat(128));
    let commit_ref = graph
        .replace_commit_bytes_before_validation(
            serde_json::to_vec(&forged).unwrap(),
            commit.commit_hash(),
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;
    let expected_stream_id = commit_stream_id(&graph.reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (_, result) = storage
        .pull_into_result(&db2, &db2_store_dir)
        .await
        .expect("a forged Store commit is held before schema classification");
    assert_eq!(result.held_positions.len(), 1);
    assert_eq!(
        result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit {
                device_id: expected_stream_id.clone(),
                commit: commit_ref.clone(),
            },
            reason: HeldStorePositionReason::InvalidSignature,
        }
    );
    assert!(newer_schema_positions(&result).is_empty());
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(
        db2.materialized_sequences().await.get(&expected_stream_id),
        None,
    );
}

/// A genuine newer-schema changeset is signed, so verifying the signature before
/// the schema gate does not change its handling: it still verifies, still counts
/// as a schema skip, still holds the position, and applies once the local schema
/// catches up. The reorder rejects only forgeries, never an authentic upgrade.
#[tokio::test]
async fn a_signed_newer_schema_changeset_still_counts_as_a_schema_skip() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), founder.clone()).await;
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'SignedFuture', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let commit = graph
        .resign_commit(
            SCHEMA_VERSION + 1,
            graph.commit.membership_authority.clone(),
        )
        .await;
    graph
        .replace_commit_bytes(
            commit.to_bytes(),
            commit.commit_hash(),
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert_eq!(newer_schema_positions(&result).len(), 1);
    assert!(invalid_changeset_positions(&result).is_empty());
    assert_eq!(result.changesets_applied, 0);
    // Held, not advanced: it becomes applicable once this app upgrades.
    assert_eq!(updated.get("dev1"), None);
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
}

/// The pull gate compares an incoming changeset's `schema_version` against the
/// opened db's [`coven_database::Database::schema_version`], not a hand-bumped constant: a peer at
/// version N applies a changeset stamped N and skips one stamped N+1 without
/// advancing its position. The peer's own version is derived from the db, so this
/// fails if the gate stops tracking the schema that actually exists on disk. (The
/// push side — that an *outgoing* changeset is stamped with the db's version — is
/// covered by `push_stamps_the_dbs_schema_version`, which drives the real producer.)
#[tokio::test]
async fn pull_gate_tracks_the_dbs_schema_version() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), founder.clone()).await;

    let n = db1.schema_version();

    // seq 1 stamped at exactly the peer's schema version: applies.
    let cs1 = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'At N', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let first_reference = storage
        .publish_changeset("dev1", 1, &cs1, n)
        .await
        .expect("publish first exact Store changeset");
    let stream_id = commit_stream_id(&first_reference);

    // seq 2 stamped one above the peer's schema version: skipped, position held.
    let cs2 = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n2', 'Above N', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("dev1", 2, &cs2, n)
        .await
        .expect("publish second exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let commit = graph
        .resign_commit(n + 1, graph.commit.membership_authority.clone())
        .await;
    graph
        .replace_commit_bytes(
            commit.to_bytes(),
            commit.commit_hash(),
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    assert_eq!(
        db2.schema_version(),
        n,
        "both peers open the same migration ladder, so they share the wire version"
    );
    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert_eq!(
        result.changesets_applied, 1,
        "the N-stamped changeset applies",
    );
    assert_eq!(
        newer_schema_positions(&result).len(),
        1,
        "the N+1-stamped changeset is skipped"
    );
    assert_eq!(
        updated.get(&stream_id),
        Some(&1),
        "position stops at the applied seq, never past the skipped one"
    );
    assert!(
        db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n2'")
            .await
    );
}

/// The push side stamps an outgoing changeset with the db's
/// [`coven_database::Database::schema_version`], driven through the real producer
/// (Store write preparation) and read back off the produced envelope — so a regression
/// that stamped a constant instead would fail here. Paired with
/// `pull_gate_tracks_the_dbs_schema_version`, which covers the receiver gate.
#[tokio::test]
async fn push_stamps_the_dbs_schema_version() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;
    // `shared = 1` so the gated `notes` root survives the push gate and there is an
    // outgoing changeset to inspect.
    let outgoing = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;

    let position = storage
        .publish_founder_changeset(outgoing, 0)
        .await
        .expect("sync push");

    let stream_id = commit_stream_id(&position);
    let device = storage
        .bind_founder_device(&db1, db1_store_dir.clone())
        .await
        .expect("load exact materialized Store");
    let (_commit_ref, commit) = device
        .load_exact_materialized_commit(&stream_id, position.coord.sequence())
        .await
        .expect("load exact Store commit")
        .expect("Store commit slot");
    assert_eq!(
        commit
            .store_package()
            .expect("outgoing Store commit carries a Store package")
            .schema_version,
        db1.schema_version(),
        "the outgoing Store package is stamped with the database schema version",
    );
}

#[tokio::test]
async fn sync_reuses_opened_schema_models() {
    coven_database::reset_gate_from_tables_call_count();
    coven_database::reset_from_tables_call_count();

    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = create_store(&db, db_store_dir.clone(), UserKeypair::generate()).await;
    assert_eq!(coven_database::gate_from_tables_call_count(), 1);
    assert_eq!(coven_database::gate_from_tables_call_count(), 1);

    let outgoing = db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'One', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;

    storage
        .publish_founder_changeset(outgoing, 0)
        .await
        .expect("sync");

    assert_eq!(coven_database::from_tables_call_count(), 1);
    assert_eq!(coven_database::from_tables_call_count(), 1);
}

#[tokio::test]
async fn pull_does_not_advance_position_past_a_blob_failed_changeset() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 =
        crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;
    // Source dev1: seq 1 references a photo blob; seq 2 is a plain note.
    db1.capture_test_changeset(&[
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'One', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'attach', 11, '{}', '0000000001001-0000-dev1', '2026-01-01')",
            coven_protocol::blob::content_hash(b"PHOTO-BYTES"),
        ),
    ])
    .await;
    let source_store_dir = db1_store_dir.clone();
    source_store_dir.store_local("ph1", b"PHOTO-BYTES").await;
    storage
        .make_root_remote(&db1, &source_store_dir, "n1")
        .await;
    let first_commit = storage
        .latest_store_position()
        .await
        .expect("read first exact Store position")
        .expect("blob publication created a Store commit");
    let stream_id = commit_stream_id(&first_commit);
    let stored = db1
        .row_blob_ref("note_photos", "ph1")
        .await
        .expect("load exact published blob row")
        .stored()
        .cloned()
        .expect("published row carries exact blob authority");
    cloud_storage
        .delete_blob_object(&stored)
        .await
        .expect("remove exact remote blob fixture");
    let cs2 = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n2', 'Two', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ])
        .await;
    storage
        .publish_founder_changeset(cs2, 1)
        .await
        .expect("publish exact blob-bearing Store changeset");

    // The puller declares note_photos blob-bearing, so seq 1's missing blob fails
    // while seq 2 (no blob) would succeed.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 =
        crate::sync::test_helpers::open_test_db_with_blob(db2_store_dir.clone(), photo_decl());
    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert!(
        result.asset_downloads_failed,
        "seq 1's blob download must fail"
    );
    // The position must NOT jump to 2 past the blob-failed seq 1 — otherwise seq 1's
    // blob would never be re-fetched. It stays before seq 1 so the next cycle
    // resumes there.
    assert_ne!(
        updated.get(&stream_id),
        Some(&2),
        "position must not advance past the blob-failed seq",
    );
    assert_eq!(
        updated.get(&stream_id),
        None,
        "position stays before the blob-failed seq 1",
    );
    // The blob-bearing row must NOT have been applied: with download-before-apply
    // (#111), seq 1's failed blob means seq 1 is skipped whole -- "row present,
    // blob missing" never exists. (Before #111 the row was applied and only the
    // position held back, so n1 was visible with no photo file on disk.)
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "seq 1's row must not be applied when its blob download fails",
    );
    // seq 2 is never reached -- the pull stops this device at the failed seq 1.
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n2'")
            .await,
        "seq 2 is not processed past the blob-failed seq 1",
    );
    assert_eq!(result.changesets_applied, 0);
}

/// A changeset whose envelope `changeset_size` disagrees with the actual trailing
/// bytes is corrupt or tampered: it must be rejected, not applied. The size is one
/// The Store package descriptor signs both byte length and content hash. Bytes
/// that do not match that descriptor are an immutable object collision.
#[tokio::test]
async fn pull_rejects_changeset_whose_declared_size_mismatches_actual_bytes() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), founder.clone()).await;

    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Corrupt', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let package = graph
        .commit
        .store_package()
        .expect("Store commit carries a Store package");
    let expected_package_hash = package.content_hash;
    let expected_stream_id = commit_stream_id(&graph.reference);
    graph
        .replace_package_bytes(cs[..cs.len() - 1].to_vec())
        .await;

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (_, result) = storage
        .pull_into_result(&db2, &db2_store_dir)
        .await
        .expect("a Store package that differs from its descriptor is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(
        matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Package {
                    device_id,
                    seq: 1,
                    package_hash,
                },
                reason: HeldStorePositionReason::ObjectUnreadableStorage { key, source },
            } if device_id == &expected_stream_id
                && *package_hash == expected_package_hash
                && key == "exact Store object"
                && source.to_string().contains("does not match stored size/hash")
        ),
        "unexpected held position: {:#?}",
        result.held_positions[0]
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "a size-mismatched changeset must not be applied",
    );
    assert_eq!(
        db2.materialized_sequences().await.get(&expected_stream_id),
        None,
    );
}

/// A Store commit is signed for one exact sequence. Copying its bytes beneath a
/// different immutable slot is an object collision and cannot materialize rows.
#[tokio::test]
async fn a_store_commit_replayed_at_another_sequence_is_rejected() {
    let src_store_dir = crate::sync::test_helpers::test_store_dir();
    let src = crate::sync::test_helpers::open_test_db(src_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&src, src_store_dir.clone(), founder.clone()).await;

    let cs = src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Replayed', NULL, '0000000005000-0000-dev', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("dev", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let coven_protocol::store_commit::StoreCommitCoord { stream_id, .. } = &graph.reference.coord;
    let relocated_coord = coven_protocol::store_commit::StoreCommitCoord {
        stream_id: *stream_id,
        sequence: 2,
    };
    let expected_stream_id = stream_id.to_string();
    let relocated_object = storage
        .create_exact_protocol_object(
            &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                storage.root().store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &coven_protocol::store_commit::commit_semantic_prefix(
                graph.commit.candidate_family(),
                &stream_id.to_string(),
                2,
                graph.commit.commit_hash(),
            ),
            ".json",
            &graph.commit.to_bytes(),
        )
        .await
        .expect("publish relocated exact Store commit");
    let relocated_ref = coven_protocol::store_commit::StoreBatchCommitRef {
        coord: relocated_coord,
        commit_hash: graph.commit.commit_hash(),
        object: relocated_object,
    };
    graph
        .replace_head(
            relocated_ref,
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (_, result) = storage
        .pull_into_result(&db2, &db2_store_dir)
        .await
        .expect("a relocated Store commit is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(
        matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head { device_id, seq: 1, .. },
                reason: HeldStorePositionReason::WrongSlot(_),
            } if device_id == &expected_stream_id
        ),
        "expected stream {expected_stream_id}; unexpected held position: {:#?}",
        result.held_positions[0]
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "a Store commit relocated to another sequence must not be applied",
    );
    assert_eq!(
        db2.materialized_sequences().await.get(&expected_stream_id),
        None
    );
}

/// The signed Store slot includes the device id as well as the sequence.
#[tokio::test]
async fn a_store_commit_relocated_to_another_device_is_rejected() {
    let src_store_dir = crate::sync::test_helpers::test_store_dir();
    let src = crate::sync::test_helpers::open_test_db(src_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&src, src_store_dir.clone(), founder.clone()).await;

    let cs = src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Relocated', NULL, '0000000001000-0000-devVictim', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("devVictim", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let relocated_stream = coven_protocol::membership::AuthorStreamId::from_bytes([99; 32]);
    let relocated_object = storage
        .create_exact_protocol_object(
            &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                storage.root().store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &coven_protocol::store_commit::commit_semantic_prefix(
                graph.commit.candidate_family(),
                &relocated_stream.to_string(),
                1,
                graph.commit.commit_hash(),
            ),
            ".json",
            &graph.commit.to_bytes(),
        )
        .await
        .expect("publish relocated exact Store commit");
    let relocated_ref = coven_protocol::store_commit::StoreBatchCommitRef {
        coord: coven_protocol::store_commit::StoreCommitCoord {
            stream_id: relocated_stream,
            sequence: 1,
        },
        commit_hash: graph.commit.commit_hash(),
        object: relocated_object,
    };
    graph
        .replace_head(
            relocated_ref,
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;
    let expected_stream_id = commit_stream_id(&graph.reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (_, result) = storage
        .pull_into_result(&db2, &db2_store_dir)
        .await
        .expect("a relocated Store commit is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(
        matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head { device_id, seq: 1, .. },
                reason: HeldStorePositionReason::WrongSlot(_),
            } if device_id == &expected_stream_id
        ),
        "expected stream {expected_stream_id}; unexpected held position: {:#?}",
        result.held_positions[0]
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "a Store commit relocated to another device must not be applied",
    );
    assert_eq!(
        db2.materialized_sequences().await.get(&expected_stream_id),
        None
    );
}

/// A signed changeset sitting at the exact position its envelope declares is
/// untouched by the position binding — it applies normally. The check rejects
/// relocation, not authorship.
#[tokio::test]
async fn a_changeset_at_its_own_position_still_applies() {
    let src_store_dir = crate::sync::test_helpers::test_store_dir();
    let src = crate::sync::test_helpers::open_test_db(src_store_dir.clone());
    let storage = create_store(&src, src_store_dir.clone(), UserKeypair::generate()).await;
    let cs = src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'InPlace', NULL, '0000000001000-0000-dev', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("dev", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(
        db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), Some(&1));
    assert!(result.held_positions.is_empty());
}

#[tokio::test]
async fn corrupt_local_register_fails_without_materializing_the_remote_commit() {
    let good_source_store_dir = crate::sync::test_helpers::test_store_dir();
    let good_source = crate::sync::test_helpers::open_test_db(good_source_store_dir.clone());
    let storage = create_store(
        &good_source,
        good_source_store_dir.clone(),
        UserKeypair::generate(),
    )
    .await;
    storage
        .device_id("devA")
        .await
        .expect("activate valid producer before publishing data");
    storage
        .device_id("devB")
        .await
        .expect("activate invalid producer before publishing data");
    let good_sequence = storage
        .next_commit_sequence("devA")
        .await
        .expect("load valid producer position");
    let bad_sequence = storage
        .next_commit_sequence("devB")
        .await
        .expect("load invalid producer position");

    let good_cs = good_source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-good', 'Good', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ])
        .await;
    let good_commit = storage
        .publish_changeset("devA", good_sequence, &good_cs, SCHEMA_VERSION)
        .await
        .expect("publish valid exact Store changeset");

    let bad_source_store_dir = crate::sync::test_helpers::test_store_dir();
    let bad_source = crate::sync::test_helpers::open_test_db(bad_source_store_dir.clone());
    // The base row exists (so the UPDATE below is an UPDATE, not an insert), but
    // through raw `exec`, so only the UPDATE enters the captured changeset.
    bad_source
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-bad', 'Base', NULL, '0000000001000-0000-devB', '2026-01-01')",
        )
        .await;
    let bad_cs = bad_source
        .capture_test_changeset(&[
            "UPDATE notes SET title = 'Bad', _updated_at = '0000000002000-0000-devB' \
             WHERE id = 'n-bad'",
        ])
        .await;
    let bad_commit = storage
        .publish_changeset("devB", bad_sequence, &bad_cs, SCHEMA_VERSION)
        .await
        .expect("publish invalid exact Store changeset bytes");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    target
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-bad', 'Local', NULL, 'not-a-stamp', '2026-01-01')",
        )
        .await;
    let ld = target_store_dir.clone();
    let (_, first) = storage.pull_into(&target, &ld).await;
    assert!(first.held_positions.is_empty());
    let error = storage
        .pull_into_result(&target, &ld)
        .await
        .expect_err("an invalid local register must fail loudly");

    assert!(matches!(error, TestPullError::Pull(_)));
    assert!(
        target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n-good'")
            .await,
        "independent commit did not apply",
    );
    let good_stream_id = commit_stream_id(&good_commit);
    let bad_stream_id = commit_stream_id(&bad_commit);
    assert_eq!(
        target.materialized_sequences().await.get(&good_stream_id),
        Some(&good_commit.coord.sequence()),
        "the independent commit completed before the corrupt local register was read",
    );
    assert_eq!(
        target.materialized_sequences().await.get(&bad_stream_id),
        None,
        "the failing commit never materializes",
    );
    assert_eq!(
        target
            .query_test_text("SELECT title FROM notes WHERE id = 'n-bad'")
            .await,
        "Local",
        "the failing commit rolls back its row mutation",
    );
}

/// A signed Store commit whose package is not a SQLite changeset holds only its
/// own chain. An independent device's valid commit still materializes.
#[tokio::test]
async fn malformed_store_package_isolates_to_one_device() {
    let bad_source_store_dir = crate::sync::test_helpers::test_store_dir();
    let bad_source = crate::sync::test_helpers::open_test_db(bad_source_store_dir.clone());
    let founder = UserKeypair::generate();
    let (storage, cloud_storage) =
        create_store_fixture(&bad_source, bad_source_store_dir.clone(), founder.clone()).await;
    storage
        .device_id("founder")
        .await
        .expect("reserve the founder producer");
    storage
        .device_id("devB")
        .await
        .expect("activate malformed-package producer");
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let ld = target_store_dir.clone();
    let (_, activation_result) = storage
        .pull_into_result(&target, &ld)
        .await
        .expect("materialize device activations before publishing device commits");
    assert!(activation_result.held_positions.is_empty());

    let bad_seed = bad_source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-bad', 'Bad', NULL, '0000000001000-0000-devB', '2026-01-01')",
        ])
        .await;
    let bad_reference = storage
        .publish_changeset("devB", 1, &bad_seed, SCHEMA_VERSION)
        .await
        .expect("publish valid seed Store package");

    let good_source_store_dir = crate::sync::test_helpers::test_store_dir();
    let good_source = crate::sync::test_helpers::open_test_db(good_source_store_dir.clone());
    let good_cs = good_source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-good', 'Good', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ])
        .await;
    let good_sequence = storage
        .next_commit_sequence("founder")
        .await
        .expect("read founder's next Store commit sequence");
    let good_reference = storage
        .publish_changeset("founder", good_sequence, &good_cs, SCHEMA_VERSION)
        .await
        .expect("publish valid exact Store changeset");
    let good_stream_id = commit_stream_id(&good_reference);
    let bad_reference =
        ExactPublishedCommit::load(&storage, &cloud_storage, bad_reference, &founder)
            .await
            .replace_package_with_malformed_bytes()
            .await;
    let bad_stream_id = commit_stream_id(&bad_reference);

    let (updated, result) = storage
        .pull_into_result(&target, &ld)
        .await
        .expect("a malformed Store package must not fail the whole pull");

    assert!(
        target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n-good'")
            .await
    );
    assert_eq!(
        updated.get(&good_stream_id),
        Some(&good_reference.coord.sequence()),
    );
    assert_eq!(
        updated.get(&bad_stream_id),
        None,
        "the malformed device's position is not materialized",
    );
    assert_eq!(result.changesets_applied, 1);
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id,
            commit,
        } if device_id == &bad_stream_id
            && commit == &bad_reference
    ));
    assert!(matches!(
        &result.held_positions[0].reason,
        HeldStorePositionReason::InvalidStorePackage(_)
    ));
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 =
        crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
    let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;
    // Source: a note + a cover photo. The blob id is ≥4 chars so it forms the
    // `{ab}/{cd}` cache shard.
    db1.capture_test_changeset(&[
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('p1ab', 'n1', 'cover', 10, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            coven_protocol::blob::content_hash(b"PHOTOBYTES"),
        ),
    ])
    .await;

    let source_store_dir = db1_store_dir.clone();
    source_store_dir.store_local("p1ab", b"PHOTOBYTES").await;
    storage
        .make_root_remote(&db1, &source_store_dir, "n1")
        .await;
    let commit = storage
        .latest_store_position()
        .await
        .expect("read blob commit position")
        .expect("blob write has a Store commit");

    // Destination pulls. A `CacheEager` photo lands in the store dir's evictable
    // locator-keyed cache on pull.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 =
        crate::sync::test_helpers::open_test_db_with_blob(db2_store_dir.clone(), photo_decl());
    let ld = db2_store_dir.clone();
    let (_updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let blob_row = db2.exact_row_blob_ref("note_photos", "p1ab").await;
    let downloaded = std::fs::read(exact_cache_path(&ld, &blob_row)).expect("downloaded photo");
    assert_eq!(downloaded, b"PHOTOBYTES");
    let stored = blob_row
        .stored()
        .expect("pulled blob row carries exact storage")
        .clone();
    let remote = db2.stored_remote_object(stored.object()).await;
    let stream_id = commit_stream_id(&commit);
    let sequence = commit.coord.sequence();
    let input_hash = db2
        .retained_merge_input_hash_for_test(stream_id, sequence)
        .await
        .expect("load retained blob input hash");
    assert!(matches!(
        remote,
        coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.state,
                coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                    ownership
                } if ownership.activated.contains(
                    &coven_protocol::remote_object::SharedObjectOwner::StoreCommit(commit.clone())
                ) && ownership.activated.contains(
                    &coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(
                        coven_protocol::remote_object::RetainedReplayOwner::Commit {
                            commit: commit.clone(),
                            input_hash,
                        }
                    )
                )
            )
    ));
}

/// A `CacheLazy` blob is authenticated before its row crosses to the puller, but
/// its verified plaintext is discarded instead of being retained in the cache.
#[tokio::test]
async fn user_provided_lazy_blob_is_verified_without_being_retained() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_blob(
        db1_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

    // Source: a shared note + an audio row, declared user-provided · CacheLazy.
    db1.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;
    let source_tmp = tempfile::tempdir().expect("create external source directory");
    let ld1 = db1_store_dir.clone();
    let source = source_tmp.path().join("audio1.flac");
    std::fs::write(&source, b"AUDIO-PAYLOAD").expect("write exact external audio fixture");
    coven_database::StoreDatabase::new(&db1)
        .register_external_blob_for_test("note_photos", "audio1", &source)
        .await;
    storage.make_root_remote(&db1, &ld1, "n1").await;
    let audio_blob = db1
        .row_blob_ref("note_photos", "audio1")
        .await
        .expect("load exact published audio row")
        .stored()
        .cloned()
        .expect("published audio row carries exact blob authority");

    assert_eq!(
        storage.read_exact_blob(&cloud_storage, &audio_blob).await,
        b"AUDIO-PAYLOAD",
        "the transition publishes the exact user-provided bytes",
    );

    // A failed exact read rejects the row and leaves the commit available for retry.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db_with_blob(
        db2_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let ld = db2_store_dir.clone();
    storage
        .open_into(&db2, db2_store_dir.clone())
        .await
        .expect("open exact Store before failed lazy verification");
    let failing: Arc<dyn CloudSyncObjectStorage> =
        Arc::new(crate::sync::test_helpers::InterceptedStorage::new(
            cloud_storage.clone(),
            FaultingStorage::blob(),
        ));
    let error = storage
        .pull_with_storage_for_test(&db2, failing, &ld, None)
        .await
        .expect_err("lazy blob verification failure rejects the Store commit");
    assert!(
        error.contains("blob"),
        "unexpected lazy verification error: {error:?}"
    );
    assert!(error.is_offline());
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );

    // The same commit applies once the exact blob can be opened and verified.
    let (updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.values().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        db2.query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "WithAudio",
        "the row carrying the CacheLazy blob still reaches the peer",
    );
    // Verification used an unpublished temporary file, so the plaintext remains
    // absent from both cache locations until an application read requests it.
    let reference = db2.exact_row_blob_ref("note_photos", "audio1").await;
    assert!(
        !exact_pinned_path(&ld, &reference).exists()
            && !exact_cache_path(&ld, &reference).exists(),
        "a CacheLazy blob must NOT be downloaded on pull — it stays in the cloud for on-demand fetch",
    );
}

fn open_scoped_circle_test_db(
    store_dir: coven_foundation::store_dir::StoreDir,
) -> coven_database::Database {
    open_test_db_schema(
        store_dir,
        vec![
            SyncedTable::new(
                "notes",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
            SyncedTable::new(
                "comments",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .inherits_audience_through("note_id"),
        ],
        vec![coven_database::Migration::sql(
            1,
            "scoped Circle schema",
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 body TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE comments (
                 id TEXT PRIMARY KEY,
                 note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
                 body TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

#[tokio::test]
async fn merge_pull_applies_circle_rows_and_private_routes_atomically() {
    let owner = UserKeypair::generate();
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = open_scoped_circle_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), owner.clone()).await;
    let _source_membership = storage
        .open_into(&source, source_store_dir.clone())
        .await
        .expect("open scoped source Store");
    let circle_id = storage
        .bind_device_in(&source, source_store_dir.clone(), &owner)
        .await
        .expect("load scoped source Store")
        .create_circle("0000000001000-0000-owner", "Readers")
        .await
        .expect("create exact Circle");
    let note_id = "01890a5d-ac96-774b-bcce-b302099c3f74";
    let comment_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    let sql = format!(
        "INSERT INTO notes VALUES ('{note_id}', '{circle_id}', 'private', '0000000002000-0000-owner');
         INSERT INTO comments VALUES ('{comment_id}', '{note_id}', 'child', '0000000002001-0000-owner');"
    );
    coven_database::StoreDatabase::new(&source)
        .run_host_store_write_for_test(
            Some(EncryptionService::from_key([42; 32])),
            None,
            move |tx| {
                tx.execute_batch(&sql)
                    .map_err(coven_database::DbError::from)
            },
        )
        .await
        .expect("commit scoped host transaction");
    let source_dir = source_store_dir.clone();
    storage
        .publish_pending(&source, &source_dir)
        .await
        .expect("publish Circle-scoped rows");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = open_scoped_circle_test_db(target_store_dir.clone());
    let target_device = storage
        .open_into(&target, target_store_dir.clone())
        .await
        .expect("open scoped target Store");
    let routing_encryption = EncryptionService::from_key([42; 32]);
    let result = target_device
        .authorize_writer()
        .await
        .expect("authorize scoped target writer")
        .pull(Some(&routing_encryption))
        .await
        .expect("pull Circle-scoped rows");

    assert!(result.changesets_applied >= 1);
    assert!(
        target
            .test_row_exists(&format!("SELECT 1 FROM notes WHERE id = '{note_id}'"))
            .await,
        "Circle root was not applied: {:?}",
        result.held_positions
    );
    assert!(
        target
            .test_row_exists(&format!("SELECT 1 FROM comments WHERE id = '{comment_id}'"))
            .await
    );
    let (routes, mirrors): (i64, i64) = target
        .scoped_routing_counts_for_test(circle_id)
        .await
        .expect("read pulled routing state");
    assert_eq!((routes, mirrors), (2, 2));
    assert!(
        result
            .row_changes
            .iter()
            .all(|change| !coven_database::is_routing_table(&change.table)),
        "host-visible row changes must not expose Coven routing tables"
    );
    assert!(
        target
            .stored_remote_objects()
            .await
            .iter()
            .any(|remote| is_external_circle_package(remote, true)),
        "pulled Merge Circle package must carry external exact and replay ownership"
    );
}

#[tokio::test]
async fn merge_pull_applies_a_circle_activation_before_its_reversed_order_successor() {
    let owner = UserKeypair::generate();
    let routing_encryption = EncryptionService::from_key([42; 32]);
    let observer_store_dir = crate::sync::test_helpers::test_store_dir();
    let observer = open_scoped_circle_test_db(observer_store_dir.clone());
    let storage = TestStore::create(
        &observer,
        observer_store_dir.clone(),
        "circle-activation-order",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store for Circle activation ordering");
    storage.sort_provider_listings();
    let first_store_dir = crate::sync::test_helpers::test_store_dir();
    let first = open_scoped_circle_test_db(first_store_dir.clone());
    let second_store_dir = crate::sync::test_helpers::test_store_dir();
    let second = open_scoped_circle_test_db(second_store_dir.clone());
    let receiver_store_dir = crate::sync::test_helpers::test_store_dir();
    let receiver = open_scoped_circle_test_db(receiver_store_dir.clone());
    for (participant, participant_store_dir) in [
        (&first, first_store_dir.clone()),
        (&second, second_store_dir.clone()),
        (&receiver, receiver_store_dir.clone()),
    ] {
        storage
            .activate_joined_device(
                &observer,
                observer_store_dir.clone(),
                participant,
                participant_store_dir.clone(),
                &owner,
                "2026-07-19T00:00:00Z",
            )
            .await
            .expect("install active Circle test device");
    }
    for (participant, participant_store_dir) in [
        (&first, &first_store_dir),
        (&second, &second_store_dir),
        (&receiver, &receiver_store_dir),
    ] {
        storage.pull_into(participant, participant_store_dir).await;
    }
    let first_stream = first.local_announcement_stream().await;
    let second_stream = second.local_announcement_stream().await;
    let (activator, activator_store_dir, successor, successor_store_dir) =
        if first_stream > second_stream {
            (&first, &first_store_dir, &second, &second_store_dir)
        } else {
            (&second, &second_store_dir, &first, &first_store_dir)
        };
    assert!(
        successor.local_announcement_stream().await < activator.local_announcement_stream().await
    );
    let circle_id = storage
        .bind_device_in(activator, activator_store_dir.clone(), &owner)
        .await
        .expect("load Circle activator Store")
        .create_circle("0000000001000-0000-owner", "Readers")
        .await
        .expect("create Circle on the later-sorted stream");

    let successor_device = storage
        .bind_device_in(successor, successor_store_dir.clone(), &owner)
        .await
        .expect("open Store before pulling Circle activation");
    successor_device
        .authorize_writer()
        .await
        .expect("authorize Circle successor writer")
        .pull(Some(&routing_encryption))
        .await
        .expect("pull Circle activation before authoring successor");
    storage
        .bind_device_in(successor, successor_store_dir.clone(), &owner)
        .await
        .expect("load Circle successor Store")
        .rename_circle("0000000002000-0000-owner", circle_id, "Renamed readers")
        .await
        .expect("publish Circle successor from the earlier-sorted stream");

    let receiver_device = storage
        .bind_device_in(&receiver, receiver_store_dir.clone(), &owner)
        .await
        .expect("open Store before ordered Circle pull");
    let result = receiver_device
        .authorize_writer()
        .await
        .expect("authorize ordered Circle receiver")
        .pull(Some(&routing_encryption))
        .await
        .expect("pull Circle activation and successor in one pass");

    assert!(result.held_positions.is_empty(), "{result:?}");
    assert_eq!(
        store_database(&receiver)
            .get_circles(
                &coven_keys::keys::public_key_hex(&owner),
                std::collections::BTreeSet::from([coven_keys::keys::public_key_hex(&owner)]),
            )
            .await
            .expect("read ordered Circle result")
            .into_iter()
            .find(|circle| circle.id() == circle_id)
            .expect("Circle exists after ordered pull")
            .name()
            .expect("ordered Circle is active"),
        "Renamed readers"
    );
}

fn scoped_fk_circle_db(
    store_dir: coven_foundation::store_dir::StoreDir,
) -> coven_database::Database {
    open_test_db_schema(
        store_dir,
        vec![
            SyncedTable::new(
                "notes",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
            SyncedTable::new(
                "categories",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
            SyncedTable::new(
                "comments",
                coven_protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .inherits_audience_through("note_id"),
        ],
        vec![coven_database::Migration::sql(
            1,
            "scoped foreign-key schema",
            "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 body TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE categories (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE comments (
                 id TEXT PRIMARY KEY,
                 note_id TEXT NOT NULL REFERENCES notes(id),
                 category_id TEXT NOT NULL REFERENCES categories(id),
                 body TEXT NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
}

/// A receiver holds a category only because a comment it owns references it as an
/// unchanged foreign-key ancestor. A concurrent device — which never saw the
/// comment — moves that category into another Circle. When the receiver applies
/// the move, the comment (still in the first Circle) would point at a category in
/// a second Circle: an FK-invalid connected component the final-component
/// validation must refuse, leaving the receiver's category where it was.
#[tokio::test]
async fn receiver_refuses_a_concurrent_ancestor_move_that_breaks_its_component() {
    let owner = UserKeypair::generate();
    let founder_store_dir = crate::sync::test_helpers::test_store_dir();
    let founder = scoped_fk_circle_db(founder_store_dir.clone());
    let storage = TestStore::create(
        &founder,
        founder_store_dir.clone(),
        "receiver-final-component",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create scoped Store");

    // A second owner device that will hold the comment the mover never sees.
    let receiver_store_dir = crate::sync::test_helpers::test_store_dir();
    let receiver = scoped_fk_circle_db(receiver_store_dir.clone());
    storage
        .activate_joined_device(
            &founder,
            founder_store_dir.clone(),
            &receiver,
            receiver_store_dir.clone(),
            &owner,
            "2026-07-19T00:00:00Z",
        )
        .await
        .expect("install receiver device");

    let loaded = storage
        .bind_device_in(&founder, founder_store_dir.clone(), &owner)
        .await
        .expect("load founder Store");
    let circle_a = loaded
        .create_circle("0000000001000-0000-owner", "First")
        .await
        .expect("create first Circle");
    let circle_b = loaded
        .create_circle("0000000001001-0000-owner", "Second")
        .await
        .expect("create second Circle");

    let note_id = "01890a5d-ac96-774b-bcce-b302099c3f74";
    let category_id = "01890a5d-ac96-774b-bcce-b302099c3f75";
    let comment_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

    // Founder publishes a note and a category, both in the first Circle. It never
    // authors the comment.
    storage.author_scoped_write(
        &founder,
        &founder_store_dir,
        format!(
            "INSERT INTO notes VALUES ('{note_id}', '{circle_a}', 'body', '0000000002000-0000-owner');
             INSERT INTO categories VALUES ('{category_id}', '{circle_a}', '0000000002001-0000-owner');"
        ),
    )
    .await;

    // Receiver pulls the note and category, then authors a comment that references
    // both — the only place the category is held as a foreign-key ancestor.
    storage
        .pull_scoped(&receiver, &receiver_store_dir)
        .await
        .expect("receiver pulls the valid relationship");
    assert!(
        receiver
            .test_row_exists(&format!(
                "SELECT 1 FROM categories WHERE id = '{category_id}'"
            ))
            .await,
        "receiver holds the category before the concurrent move",
    );
    storage
        .author_scoped_write(
            &receiver,
            &receiver_store_dir,
            format!(
                "INSERT INTO comments VALUES \
             ('{comment_id}', '{note_id}', '{category_id}', 'child', '0000000003000-0000-owner');"
            ),
        )
        .await;

    // Founder — which never saw the comment — moves the category into the second
    // Circle. Its own component stays valid, so the move publishes.
    storage
        .author_scoped_write(
            &founder,
            &founder_store_dir,
            format!(
                "UPDATE categories SET audience = '{circle_b}', \
             _updated_at = '0000000004000-0000-owner' WHERE id = '{category_id}';"
            ),
        )
        .await;

    // The receiver applies the move against its comment: the resulting component
    // crosses Circles, so the final-component validation refuses it.
    let outcome = storage.pull_scoped(&receiver, &receiver_store_dir).await;
    let refusal = match &outcome {
        Err(error) => error.to_string(),
        Ok(result) => panic!("the cross-Circle move must be refused, not applied: {result:?}"),
    };
    assert!(
        refusal.contains("relationship through category_id"),
        "the receiver refuses the move by its final-component FK validation: {refusal}",
    );
    let category_audience = receiver
        .query_test_text(&format!(
            "SELECT audience FROM categories WHERE id = '{category_id}'"
        ))
        .await;
    assert_eq!(
        category_audience,
        circle_a.to_string(),
        "the refused move leaves the receiver's category in its original Circle",
    );
}

#[tokio::test]
async fn local_user_provided_blob_does_not_block_changeset_publish() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let storage = create_store(&db, db_store_dir.clone(), UserKeypair::generate()).await;
    let tmp = tempfile::tempdir().expect("create external blob fixture directory");
    let external = tmp.path().join("audio.flac");
    std::fs::write(&external, b"local audio").expect("write external file");
    db.execute_test_sql(&format!(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', 11, '{}', '0000000001000-0000-dev1', '2026-01-01')",
        coven_protocol::blob::content_hash(b"local audio"),
    ))
    .await;
    coven_database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "audio1", &external)
        .await;
    let outgoing = db
        .capture_test_changeset(&["UPDATE notes SET title = 'Changed', \
           _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"])
        .await;

    storage
        .publish_founder_changeset(outgoing, 0)
        .await
        .expect("a Local blob does not require remote object authority");
    assert!(
        storage
            .latest_store_position()
            .await
            .expect("read exact local Store position")
            .is_some(),
        "the publish advances the local Store position",
    );
}

#[tokio::test]
async fn missing_remote_user_provided_blob_aborts_before_changeset_publish() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let storage = create_store(&db, db_store_dir.clone(), UserKeypair::generate()).await;
    let outgoing = db.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;
    let result = storage.publish_founder_changeset(outgoing, 0).await;
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("missing remote user-provided blob must abort publish"),
    };

    assert!(
        err.to_string().contains("audio/audio1") && err.to_string().contains("absent"),
        "the error must name the absent remote blob: {err}",
    );
    assert!(
        storage
            .latest_store_position()
            .await
            .expect("read exact local Store position")
            .is_none(),
        "failed publish created no Store commit",
    );
}

#[tokio::test]
async fn present_remote_user_provided_blob_can_publish_changeset() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let storage = create_store(&db, db_store_dir.clone(), UserKeypair::generate()).await;
    db.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;
    let tmp = tempfile::tempdir().expect("create external source directory");
    let store_dir = db_store_dir.clone();
    let source = tmp.path().join("audio1.flac");
    std::fs::write(&source, b"AUDIO-PAYLOAD").expect("write exact external audio fixture");
    coven_database::StoreDatabase::new(&db)
        .register_external_blob_for_test("note_photos", "audio1", &source)
        .await;
    storage.make_root_remote(&db, &store_dir, "n1").await;
    let result = storage
        .latest_store_position()
        .await
        .expect("read exact local Store position");
    assert_eq!(
        result
            .expect("published exact Store commit")
            .coord
            .sequence(),
        1,
        "publish advances the exact Store position after the remote blob exists",
    );
}

#[tokio::test]
async fn delete_ref_does_not_require_remote_blob_to_publish_changeset() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy),
    );
    let storage = create_store(&db, db_store_dir.clone(), UserKeypair::generate()).await;
    db.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01');
         INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    // The rows above are seed (raw `exec`, unjournaled), so the captured changeset
    // is just the DELETE under test.
    let outgoing = db
        .capture_test_changeset(&["DELETE FROM note_photos WHERE id = 'audio1'"])
        .await;
    let result = storage
        .publish_founder_changeset(outgoing, 0)
        .await
        .expect("delete does not require the removed blob to exist remotely");

    assert_eq!(
        result.coord.sequence(),
        1,
        "delete publishes even when the removed blob is absent remotely",
    );
}

/// A changeset that references absent blob bytes becomes durably blocked instead
/// of publishing a row that every puller would fail to materialize.
#[tokio::test]
async fn sync_aborts_when_a_referenced_blob_file_is_missing() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 =
        crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
    let keypair = UserKeypair::generate();
    let storage = create_store(&db1, db1_store_dir.clone(), keypair.clone()).await;

    // A shared note + a host-provided cover row, but the cover is deliberately never
    // stored in the local store, so the inline push finds nothing in either the local
    // store or the cache.
    let missing_blob = format!(
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('p1ab', 'n1', 'cover', 7, '{}', \
                 '0000000001000-0000-dev1', '2026-01-01')",
        coven_protocol::blob::content_hash(b"missing"),
    );
    let outgoing = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &missing_blob,
        ])
        .await;

    let ld1 = db1_store_dir.clone();
    let result = storage
        .sync_for_test(&db1, outgoing, 0, "", &keypair, &ld1)
        .await;
    let err = result.expect_err("missing blob blocks Store publication");
    assert!(
        err.to_string()
            .contains("outbound blob photos/p1ab is absent from storage"),
        "an absent blob must abort Store publication, got {err:?}",
    );
    let pending = coven_database::StoreDatabase::new(&db1)
        .pending_writes()
        .await
        .expect("read blocked write");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].status,
        coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::MissingBlob {
                namespace: "photos".to_string(),
                id: "p1ab".to_string(),
            }
        ),
    );
}

/// A re-emitted row whose blob this device no longer holds still pushes, because the
/// blob is already in the cloud.
///
/// The two absences are different, and only one of them aborts. `BlobMissing` means *no
/// bytes anywhere* — not in the local store, not in the cache, and not in the cloud. A
/// device that holds no copy but whose blob's object stands at its key has nothing to
/// push: the object at that key is that blob's bytes, because the key names the blob.
///
/// Device A publishes a cover, then loses every local copy of it (the local store's and
/// the cache's), and its row is re-emitted — the shape a `make_remote` gate flip produces
/// when it re-emits a root's whole subtree. The push must skip the upload, not abort, and
/// must leave the cloud object alone.
#[tokio::test]
async fn plain_scheme_a_re_emitted_row_whose_blob_is_only_in_the_cloud_skips_the_upload() {
    let (home, keypair, storage) = plain_cloud_test_store();

    let bytes = b"COVER-BYTES";
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        readable_photo_decl(),
    );
    let ld = db_store_dir.clone();
    ld.store_local("p1cover", bytes).await;
    let rows = [
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        &format!(
            "INSERT INTO note_photos \
             (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
             VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', \
             '0000000001000-0000-dev1', '2026-01-01')",
            bytes.len(),
            coven_protocol::blob::content_hash(bytes),
        ),
    ];
    let outgoing = db.capture_test_changeset(&rows).await;
    storage
        .publish_test_cycle(&db, outgoing.clone(), 0, &keypair, &ld)
        .await;
    let cover_key = db.row_blob_object_key("note_photos", "p1cover").await;
    assert_eq!(
        home.get(&cover_key).as_deref(),
        Some(bytes.as_slice()),
        "the first push uploads the cover",
    );

    // This device now holds no copy of the blob at all: the push moved the local-store
    // copy into the cache, and the cache copy is then evicted.
    ld.remove_local_blob("photos", "p1cover")
        .await
        .expect("drop any local-store copy");
    let cached = exact_cache_path(&ld, &db.exact_row_blob_ref("note_photos", "p1cover").await);
    if cached.exists() {
        std::fs::remove_file(&cached).expect("evict the cached copy");
    }

    // The row is re-emitted. The blob has no local bytes to upload — and needs none.
    let result = storage
        .sync_for_test(&db, outgoing, 1, "", &keypair, &ld)
        .await;
    assert!(
        result.is_ok(),
        "a blob already in the cloud must not abort the push for want of a local copy",
    );
    assert_eq!(
        home.get(&cover_key).as_deref(),
        Some(bytes.as_slice()),
        "the cloud object is left exactly as it stands",
    );
}

/// The `note_photos` declaration for the plain (browsable) scheme: the blob's
/// readable cloud key comes from the row's `cloud_path` column.
fn readable_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_cloud_path_column("cloud_path")
}

/// A plain-scheme home stores a changeset-driven blob at the consumer's readable
/// `cloud_path` (`photos/n1/cover-p1cover.jpg`), not the content-addressed shard, and a
/// second device with the same declaration pulls it from that readable key and
/// recovers the bytes. This is the changeset-push / changeset-pull half of the blob
/// path, end to end over a real `CloudSyncConnection` in `BlobPathScheme::Plain`.
#[tokio::test]
async fn plain_scheme_blob_round_trips_at_the_readable_key() {
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );

    // Device A: a shared note + a cover photo whose file is present locally.
    // Driven through the real Store write preparation and publication path so the
    // production blob-upload path keys the blob from its `cloud_path`.
    let plaintext = b"COVERART";

    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_blob(
        db1_store_dir.clone(),
        readable_photo_decl(),
    );
    let ld1 = db1_store_dir.clone();
    // The cover's readable key lives in the row's `cloud_path` column.
    // The host stages the cover into the cache before the inline push reads it.
    ld1.store_local("p1cover", plaintext).await;
    let outgoing = db1.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', 8, '{}', 'n1/cover-p1cover.jpg', '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(plaintext),
            ),
        ],
    )
    .await;

    let result = storage
        .sync_for_test(&db1, outgoing, 0, "", &keypair, &ld1)
        .await
        .expect("sync");
    assert!(
        result.is_some(),
        "the readable blob row publishes a Store commit"
    );

    // The blob lands at an immutable version below its readable path.
    let blob_key = db1.row_blob_object_key("note_photos", "p1cover").await;
    assert!(
        blob_key.starts_with("photos/readable/n1/cover-p1cover.jpg/.coven-versions/"),
        "the exact object stays grouped below its readable path: {blob_key}",
    );
    assert!(
        storage
            .provider_key_exists_for_test(&blob_key)
            .await
            .expect("exists at exact readable version"),
        "the exact readable blob version exists",
    );
    assert!(
        !storage
            .provider_key_exists_for_test("photos/n1/cover-p1cover.jpg")
            .await
            .expect("check obsolete mutable readable key"),
        "no mutable object occupies the bare readable path",
    );

    // Device B: a fresh DB and its own store dir, same cloud + plain scheme,
    // pulls and downloads the cover from the readable key.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db_with_blob(
        db2_store_dir.clone(),
        readable_photo_decl(),
    );
    let ld = db2_store_dir.clone();
    let (_updated, result) = db2
        .pull_exact_store_into(&db1, &storage, &keypair, &ld)
        .await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(exact_cache_path(
        &ld,
        &db2.exact_row_blob_ref("note_photos", "p1cover").await,
    ))
    .expect("device B downloaded cover");
    assert_eq!(
        downloaded, plaintext,
        "device B recovers the source bytes from the readable plain-scheme key",
    );
}

/// A browsable home's cloud key is `{namespace}/{cloud_path}`, and coven requires a
/// host-provided blob's `cloud_path` to name the blob it holds. A path that does not is
/// refused where coven derives the blob from its row — the push aborts rather than
/// keying a blob at an object another blob could also be keyed at.
#[tokio::test]
async fn plain_scheme_host_blob_whose_cloud_path_does_not_name_it_is_refused() {
    let (home, keypair, storage) = plain_cloud_test_store();

    let bytes = b"COVER-BYTES";
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        readable_photo_decl(),
    );
    let ld = db_store_dir.clone();
    ld.store_local("p1cover", bytes).await;
    // `n1/cover.jpg` names no blob: it would key p1cover today and its replacement
    // tomorrow at one and the same cloud object.
    let outgoing = db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover.jpg', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                bytes.len(),
                coven_protocol::blob::content_hash(bytes),
            ),
        ])
        .await;

    let err = storage
        .sync_for_test(&db, outgoing, 0, "", &keypair, &ld)
        .await
        .expect_err("a cloud path that does not name its blob must fail the cycle");

    let message = err.to_string();
    assert!(
        message.contains("p1cover") && message.contains("n1/cover.jpg"),
        "the error must name the blob and the path it was given, got {message:?}",
    );
    assert!(
        home.get("photos/n1/cover.jpg").is_none(),
        "nothing is uploaded for a blob coven refuses to key",
    );
}

/// Distinct browsable blob rows write distinct immutable cloud objects.
#[tokio::test]
async fn plain_scheme_distinct_blobs_write_objects_at_their_own_keys() {
    tokio::spawn(async {
        // A browsable home: readable keys, objects stored in the clear (the two are one
        // choice), so the test reads the cloud object back as plaintext.
        let (home, keypair, storage) = plain_cloud_test_store();
        let old_bytes = b"OLD-COVER-BYTES";
        let new_bytes = b"NEW-COVER-BYTES";

        let db1_store_dir = crate::sync::test_helpers::test_store_dir();
        let db1 = crate::sync::test_helpers::open_test_db_with_blob(
            db1_store_dir.clone(),
            readable_photo_decl(),
        );
        let ld1 = db1_store_dir.clone();
        ld1.store_local("p1cover", old_bytes).await;
        let outgoing = db1
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                    old_bytes.len(),
                    coven_protocol::blob::content_hash(old_bytes),
                ),
            ])
            .await;
        storage
            .publish_test_cycle(&db1, outgoing, 0, &keypair, &ld1)
            .await;
        let old_key = db1.row_blob_object_key("note_photos", "p1cover").await;
        assert_eq!(
            home.get(&old_key).as_deref(),
            Some(old_bytes.as_slice()),
            "the first push puts the cover at the key its path names",
        );

        // Device B takes the cover before the replacement, so it is a peer holding the
        // replaced blob when the new one arrives.
        let db2_store_dir = crate::sync::test_helpers::test_store_dir();
        let db2 = crate::sync::test_helpers::open_test_db_with_blob(
            db2_store_dir.clone(),
            readable_photo_decl(),
        );
        let ld2 = db2_store_dir.clone();
        db2.pull_exact_store_into(&db1, &storage, &keypair, &ld2)
            .await;

        // Add another blob whose readable path names it.
        ld1.store_local("p2cover", new_bytes).await;
        let outgoing = db1
            .capture_test_changeset(&[&format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p2cover', 'n1', 'cover', {}, '{}', 'n1/cover-p2cover.jpg', \
                 '0000000002000-0000-dev1', '2026-01-01')",
                new_bytes.len(),
                coven_protocol::blob::content_hash(new_bytes),
            )])
            .await;
        storage
            .publish_test_cycle(&db1, outgoing, 1, &keypair, &ld1)
            .await;
        let new_key = db1.row_blob_object_key("note_photos", "p2cover").await;

        assert_eq!(
            home.get(&new_key).as_deref(),
            Some(new_bytes.as_slice()),
            "the second blob writes its own cloud object",
        );
        assert_eq!(
            home.get(&old_key).as_deref(),
            Some(old_bytes.as_slice()),
            "the first blob's object is untouched",
        );

        // Device B pulls the replacement. Its download verifies the object against the new
        // row's content hash, so an object holding the replaced bytes would fail the pull.
        let (_updated, result) = db2
            .pull_exact_store_into(&db1, &storage, &keypair, &ld2)
            .await;

        assert!(
            !result.asset_downloads_failed,
            "device B downloads blobs matching their row hashes",
        );
        assert_eq!(result.changesets_applied, 1);
        let cached = std::fs::read(exact_cache_path(
            &ld2,
            &db2.exact_row_blob_ref("note_photos", "p2cover").await,
        ))
        .expect("device B cached the replacement cover");
        assert_eq!(
            cached,
            new_bytes.as_slice(),
            "device B serves the second blob's bytes",
        );
    })
    .await
    .expect("distinct browsable blob orchestration task");
}

/// Sequential replacements write separate immutable objects.
#[tokio::test]
async fn plain_scheme_two_replacements_write_two_objects() {
    tokio::spawn(async {
        let (home, keypair, storage) = plain_cloud_test_store();

        let original = b"ORIGINAL-COVER";
        let from_a = b"COVER-FROM-A";
        let from_b = b"COVER-FROM-B-BYTES";

        // The source publishes the original cover.
        let db_a_store_dir = crate::sync::test_helpers::test_store_dir();
        let db_a = crate::sync::test_helpers::open_test_db_with_blob(
            db_a_store_dir.clone(),
            replaceable_photo_decl(),
        );
        let ld_a = db_a_store_dir.clone();
        ld_a.store_local("p0cover", original).await;
        let outgoing = db_a
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'cover', {}, '{}', 'n1/cover-p0cover.jpg', 'p0cover', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                    original.len(),
                    coven_protocol::blob::content_hash(original),
                ),
            ])
            .await;
        storage
            .publish_test_cycle(&db_a, outgoing, 0, &keypair, &ld_a)
            .await;

        // Each replacement uses a fresh blob id and readable path.
        ld_a.store_local("pAcover", from_a).await;
        let outgoing_a = db_a
            .capture_test_changeset(&[&format!(
                "UPDATE note_photos SET blob_id = 'pAcover', cloud_path = 'n1/cover-pAcover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
                from_a.len(),
                coven_protocol::blob::content_hash(from_a),
            )])
            .await;
        storage
            .publish_test_cycle(&db_a, outgoing_a, 1, &keypair, &ld_a)
            .await;
        let from_a_key = db_a.row_blob_object_key("note_photos", "ph1").await;

        ld_a.store_local("pBcover", from_b).await;
        let outgoing_b = db_a
            .capture_test_changeset(&[&format!(
                "UPDATE note_photos SET blob_id = 'pBcover', cloud_path = 'n1/cover-pBcover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000003000-0000-dev2' WHERE id = 'ph1'",
                from_b.len(),
                coven_protocol::blob::content_hash(from_b),
            )])
            .await;
        storage
            .publish_test_cycle(&db_a, outgoing_b, 2, &keypair, &ld_a)
            .await;
        let from_b_key = db_a.row_blob_object_key("note_photos", "ph1").await;

        // Neither replacement overwrote the other: both objects stand, each holding the bytes
        // of the blob its key names. Under a key that did not name its blob, these two writes
        // would have been one object, and its bytes would be whichever device the bucket saw
        // last — not necessarily the device the row's conflict resolved to.
        assert_eq!(
            home.get(&from_a_key).as_deref(),
            Some(from_a.as_slice()),
            "the first replacement is at its own key",
        );
        assert_eq!(
            home.get(&from_b_key).as_deref(),
            Some(from_b.as_slice()),
            "the second replacement is at its own key",
        );

        // A peer pulls every changeset and serves the latest object.
        let db_c_store_dir = crate::sync::test_helpers::test_store_dir();
        let db_c = crate::sync::test_helpers::open_test_db_with_blob(
            db_c_store_dir.clone(),
            replaceable_photo_decl(),
        );
        let ld_c = db_c_store_dir.clone();
        let (_updated, result) = db_c
            .pull_exact_store_into(&db_a, &storage, &keypair, &ld_c)
            .await;
        assert!(
            !result.asset_downloads_failed,
            "every row the third device applies names an object that holds its bytes",
        );
        assert_eq!(
            result.changesets_applied, 3,
            "the original and both replacements all apply",
        );

        let winner: String = coven_database::StoreDatabase::new(&db_c)
            .read(|sql| {
                sql.query_row(
                    "SELECT blob_id FROM note_photos WHERE id = 'ph1'",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .map_err(coven_database::DbError::from)
            })
            .await
            .expect("access the cover row")
            .expect("the cover row");
        assert_eq!(winner, "pBcover");
        let expected = from_b.as_slice();
        let cached = std::fs::read(exact_cache_path(
            &ld_c,
            &db_c.exact_row_blob_ref("note_photos", "ph1").await,
        ))
        .expect("the third device cached the cover its row names");
        assert_eq!(
            cached, expected,
            "the latest row names the second replacement's bytes",
        );
    })
    .await
    .expect("sequential browsable blob replacement orchestration task");
}

/// A device replaying two blob-bearing changesets can fetch each immutable object.
#[tokio::test]
async fn plain_scheme_a_laggard_finds_blobs_from_each_changeset() {
    let (_home, keypair, storage) = plain_cloud_test_store();

    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_blob(
        db1_store_dir.clone(),
        readable_photo_decl(),
    );
    let ld1 = db1_store_dir.clone();
    ld1.store_local("p1cover", old_bytes).await;
    let outgoing = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                old_bytes.len(),
                coven_protocol::blob::content_hash(old_bytes),
            ),
        ])
        .await;
    storage
        .publish_test_cycle(&db1, outgoing, 0, &keypair, &ld1)
        .await;

    // Another blob is published while the laggard is away.
    ld1.store_local("p2cover", new_bytes).await;
    let outgoing = db1
        .capture_test_changeset(&[&format!(
            "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p2cover', 'n1', 'cover', {}, '{}', 'n1/cover-p2cover.jpg', \
                 '0000000002000-0000-dev1', '2026-01-01')",
            new_bytes.len(),
            coven_protocol::blob::content_hash(new_bytes),
        )])
        .await;
    storage
        .publish_test_cycle(&db1, outgoing, 1, &keypair, &ld1)
        .await;

    // The laggard pulls from zero: it applies the pre-replacement changeset first, whose
    // row names the replaced blob. Its bytes are still at their own key.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db_with_blob(
        db2_store_dir.clone(),
        readable_photo_decl(),
    );
    let ld2 = db2_store_dir.clone();
    let (_positions, result) = db2
        .pull_exact_store_into(&db1, &storage, &keypair, &ld2)
        .await;

    assert!(
        !result.asset_downloads_failed,
        "each changeset finds the exact blob object it names",
    );
    assert_eq!(result.changesets_applied, 2, "both changesets apply",);
    let cached = std::fs::read(exact_cache_path(
        &ld2,
        &db2.exact_row_blob_ref("note_photos", "p2cover").await,
    ))
    .expect("the laggard cached the current cover");
    assert_eq!(
        cached,
        new_bytes.as_slice(),
        "having caught up, the laggard serves the second blob",
    );
}

/// A **write-once** browsable declaration: the row is never repointed at a different
/// blob, so its readable cloud path is free to be a stable, fully human-readable name —
/// no blob id in it. The blob id is its own column, so the shape of an (illegal)
/// repointing is expressible and can be tested.
fn write_once_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("blob_id")
        .with_cloud_path_column("cloud_path")
        .write_once()
}

/// A write-once blob keeps a stable, fully readable cloud path — no blob id in the name —
/// and round-trips through the cloud on it.
///
/// This is the shape a browsable home exists for: the bucket mirrors the consumer's own
/// names (`n1/Sonata No. 3.flac`), and a reader who is not coven can find and play the
/// file. It is safe precisely because the row is never repointed, so nothing ever rewrites
/// the object standing at that key — which is what [`BlobDecl::write_once`] declares and
/// what the test below enforces.
#[tokio::test]
async fn plain_scheme_a_write_once_blob_keeps_a_stable_readable_path() {
    let (home, keypair, storage) = plain_cloud_test_store();
    // A readable name with no blob id anywhere in it.
    let bytes = b"AUDIO-BYTES";
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_blob(
        db1_store_dir.clone(),
        write_once_photo_decl(),
    );
    let ld1 = db1_store_dir.clone();
    ld1.store_local("f1audio", bytes).await;
    let outgoing = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'audio', {}, '{}', 'n1/Sonata No. 3.flac', 'f1audio', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                bytes.len(),
                coven_protocol::blob::content_hash(bytes),
            ),
        ])
        .await;
    storage
        .publish_test_cycle(&db1, outgoing, 0, &keypair, &ld1)
        .await;
    let audio_key = db1.row_blob_object_key("note_photos", "ph1").await;

    assert_eq!(
        home.get(&audio_key).as_deref(),
        Some(bytes.as_slice()),
        "the blob lands at the consumer's own readable name, with no blob id in it",
    );

    // A peer pulls it off that readable key and verifies it against the row's hash.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db_with_blob(
        db2_store_dir.clone(),
        write_once_photo_decl(),
    );
    let ld2 = db2_store_dir.clone();
    let (_positions, result) = db2
        .pull_exact_store_into(&db1, &storage, &keypair, &ld2)
        .await;
    assert!(!result.asset_downloads_failed);
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(exact_cache_path(
        &ld2,
        &db2.exact_row_blob_ref("note_photos", "ph1").await,
    ))
    .expect("device B cached the audio");
    assert_eq!(cached, bytes.as_slice());
}

/// Repointing a write-once row is refused.
///
/// A write-once row's cloud path is a stable readable name that does NOT carry its blob
/// id, so a second blob under that row would be keyed at the first blob's cloud object and
/// overwrite it — the corruption the whole model exists to prevent. Write-once is the
/// declaration that this never happens, and coven holds the consumer to it: the repointing
/// is a loud error, not a silently rewritten object.
///
/// A changeset UPDATE reports only the columns whose values changed, so the blob-id column
/// appearing in one *is* the repointing.
#[tokio::test]
async fn plain_scheme_repointing_a_write_once_row_is_refused() {
    let (home, keypair, storage) = plain_cloud_test_store();
    let first = b"FIRST-AUDIO";
    let second = b"SECOND-AUDIO-BYTES";

    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db_with_blob(
        db_store_dir.clone(),
        write_once_photo_decl(),
    );
    let ld = db_store_dir.clone();
    ld.store_local("f1audio", first).await;
    let outgoing = db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'audio', {}, '{}', 'n1/Sonata No. 3.flac', 'f1audio', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                first.len(),
                coven_protocol::blob::content_hash(first),
            ),
        ])
        .await;
    storage
        .publish_test_cycle(&db, outgoing, 0, &keypair, &ld)
        .await;
    let audio_key = db.row_blob_object_key("note_photos", "ph1").await;

    // Repoint the write-once row at a second blob — the move that would rewrite the object
    // the first blob occupies.
    ld.store_local("f2audio", second).await;
    let outgoing = db
        .capture_test_changeset(&[&format!(
            "UPDATE note_photos SET blob_id = 'f2audio', size = {}, hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
            second.len(),
            coven_protocol::blob::content_hash(second),
        )])
        .await;
    let err = storage
        .sync_for_test(&db, outgoing, 1, "", &keypair, &ld)
        .await
        .expect_err("repointing a write-once row must fail the cycle");

    let message = err.to_string();
    assert!(
        message.contains("f2audio") && message.contains("write-once"),
        "the error must name the blob the row was repointed at, got {message:?}",
    );
    assert_eq!(
        home.get(&audio_key).as_deref(),
        Some(first.as_slice()),
        "the first blob's cloud object is untouched — the cycle aborted before any upload",
    );
}

/// A browsable home's `note_photos` declaration: the blob id is its own column, apart
/// from the primary key, so a row can be repointed at a new blob while keeping its
/// identity; the readable cloud key comes from `cloud_path`, which moves with the blob.
fn replaceable_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("blob_id")
        .with_cloud_path_column("cloud_path")
}

/// Repointing a row at a new blob moves its cloud key, and the new bytes land there.
///
/// The row keeps its primary key and gets a new blob — which means a new blob id, and
/// therefore a new `cloud_path`, because a host-provided path must name its blob. So the
/// repointing writes a new cloud object and leaves the one it replaced standing at its own
/// key.
///
/// Device A publishes a cover and device B pulls it; A then repoints the row at a fresh
/// blob and pushes; B pulls again and must serve the new bytes and drop the old.
#[tokio::test]
async fn plain_scheme_repointing_a_row_moves_its_blob_to_a_new_key() {
    tokio::spawn(async {
        // A browsable home: readable keys, objects stored in the clear (the two are one
        // choice), so the test reads the cloud object back as plaintext.
        let (home, keypair, storage) = plain_cloud_test_store();
        let old_bytes = b"OLD-COVER-BYTES";
        let new_bytes = b"NEW-COVER-BYTES";

        let db1_store_dir = crate::sync::test_helpers::test_store_dir();
        let db1 = crate::sync::test_helpers::open_test_db_with_blob(
            db1_store_dir.clone(),
            replaceable_photo_decl(),
        );
        let ld1 = db1_store_dir.clone();
        ld1.store_local("p1cover", old_bytes).await;
        let outgoing = db1
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', 'p1cover', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                    old_bytes.len(),
                    coven_protocol::blob::content_hash(old_bytes),
                ),
            ])
            .await;
        storage
            .publish_test_cycle(&db1, outgoing, 0, &keypair, &ld1)
            .await;
        let old_key = db1.row_blob_object_key("note_photos", "ph1").await;
        assert_eq!(
            home.get(&old_key).as_deref(),
            Some(old_bytes.as_slice()),
            "the first push puts the cover at the key its path names",
        );

        // Device B takes the cover before the replacement, so it is a peer holding the
        // replaced blob when the new one arrives.
        let db2_store_dir = crate::sync::test_helpers::test_store_dir();
        let db2 = crate::sync::test_helpers::open_test_db_with_blob(
            db2_store_dir.clone(),
            replaceable_photo_decl(),
        );
        let ld2 = db2_store_dir.clone();
        db2.pull_exact_store_into(&db1, &storage, &keypair, &ld2)
            .await;
        let old_cache_path =
            exact_cache_path(&ld2, &db2.exact_row_blob_ref("note_photos", "ph1").await);

        // Repoint the row at a new blob: same primary key, new blob id, and the cloud path
        // moves with it because it names the blob. The replaced blob's local copy goes away.
        ld1.store_local("p2cover", new_bytes).await;
        ld1.remove_local_blob("photos", "p1cover")
            .await
            .expect("drop the replaced blob's local copy");
        let outgoing = db1
            .capture_test_changeset(&[&format!(
                "UPDATE note_photos SET blob_id = 'p2cover', cloud_path = 'n1/cover-p2cover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
                new_bytes.len(),
                coven_protocol::blob::content_hash(new_bytes),
            )])
            .await;
        storage
            .publish_test_cycle(&db1, outgoing, 1, &keypair, &ld1)
            .await;
        let new_key = db1.row_blob_object_key("note_photos", "ph1").await;

        assert_eq!(
            home.get(&new_key).as_deref(),
            Some(new_bytes.as_slice()),
            "the repointed row's blob writes its own cloud object",
        );
        assert_eq!(
            home.get(&old_key).as_deref(),
            Some(old_bytes.as_slice()),
            "the replaced blob's object is not overwritten — it is tombstoned and stands until \
         the GC collects it",
        );

        // Device B pulls the repointing. Its download verifies the object against the new
        // row's content hash, so serving it the replaced bytes would fail the pull outright.
        let (_updated, result) = db2
            .pull_exact_store_into(&db1, &storage, &keypair, &ld2)
            .await;

        assert!(
            !result.asset_downloads_failed,
            "device B must download a cover matching the row's hash",
        );
        assert_eq!(result.changesets_applied, 1);
        let cached = std::fs::read(exact_cache_path(
            &ld2,
            &db2.exact_row_blob_ref("note_photos", "ph1").await,
        ))
        .expect("device B cached the replacement cover");
        assert_eq!(
            cached,
            new_bytes.as_slice(),
            "device B serves the replacement bytes, not the cover it replaced",
        );
        assert!(
            !old_cache_path.exists(),
            "device B drops its cached copy of the blob the row no longer points at",
        );
    })
    .await
    .expect("browsable blob repointing orchestration task");
}

/// Repointing a row at a new blob while HOLDING its cloud path is the shape the rule
/// exists to refuse, and it is the one a changeset cannot show on its own: an UPDATE
/// reports only the columns whose values changed, so it carries the new blob id and not
/// the (unchanged) path. coven reads the path from the row that owns the blob — which is
/// where it catches that the path names the blob the row no longer points at.
#[tokio::test]
async fn plain_scheme_repointing_a_row_without_moving_its_cloud_path_is_refused() {
    let (home, keypair, storage) = plain_cloud_test_store();
    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_blob(
        db1_store_dir.clone(),
        replaceable_photo_decl(),
    );
    let ld1 = db1_store_dir.clone();
    ld1.store_local("p1cover", old_bytes).await;
    let outgoing = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', 'p1cover', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                old_bytes.len(),
                coven_protocol::blob::content_hash(old_bytes),
            ),
        ])
        .await;
    storage
        .publish_test_cycle(&db1, outgoing, 0, &keypair, &ld1)
        .await;
    let old_key = db1.row_blob_object_key("note_photos", "ph1").await;

    // The repointing leaves `cloud_path` naming the blob it replaced, so the new blob
    // would be keyed at the old blob's object.
    ld1.store_local("p2cover", new_bytes).await;
    let outgoing = db1
        .capture_test_changeset(&[&format!(
            "UPDATE note_photos SET blob_id = 'p2cover', size = {}, hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
            new_bytes.len(),
            coven_protocol::blob::content_hash(new_bytes),
        )])
        .await;
    let err = storage
        .sync_for_test(&db1, outgoing, 1, "", &keypair, &ld1)
        .await
        .expect_err("a repointing that holds its cloud path must fail the cycle");

    let message = err.to_string();
    assert!(
        message.contains("p2cover") && message.contains("n1/cover-p1cover.jpg"),
        "the error must name the new blob and the path it kept, got {message:?}",
    );
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the replaced blob's object is untouched — the cycle aborted before any upload",
    );
}

/// Full encrypted blob round-trip through `CloudSyncConnection` (encrypted) over a
/// shared `CloudHome`. Device A publishes a note plus its cover photo via the real
/// Store write preparation; the blob lands ciphertext at rest. Device B — a fresh DB
/// with its own asset directory but the same store key — pulls, downloads the
/// blob, decrypts it, and recovers the original bytes byte-for-byte.
#[tokio::test]
async fn encrypted_blob_round_trips_and_second_device_decrypts() {
    // One cloud and one store key, shared by both devices. The device that
    // authors the changeset is the one that uploads its blobs, so storage carries
    // the same keypair the push signs with — its public key is the `{uploader}`
    // prefix the blob lands under and the author peers resolve it by.
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(InMemoryCloudHome::new()),
        CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
        BlobPathScheme::Hashed,
        "test-lib",
        keypair.clone(),
    );

    // Device A: a note and its cover photo, scoped to a per-store derived key.
    let plaintext = b"COVER-ART-BYTES";
    let decl = || {
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager).with_scope(
            coven_protocol::blob::BlobScope::Derived("covers".to_string()),
        )
    };

    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), decl());
    let ld1 = db1_store_dir.clone();
    // The host stages the cover into the cache before the inline push reads it.
    ld1.store_local("p1cover", plaintext).await;
    let outgoing = db1.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', 15, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(plaintext),
            ),
        ],
    )
    .await;

    let result = storage
        .sync_for_test(&db1, outgoing, 0, "", &keypair, &ld1)
        .await
        .expect("sync");
    assert!(
        result.is_some(),
        "the encrypted blob row publishes a Store commit"
    );

    // At rest the cover photo is ciphertext, not the source bytes.
    let source_blob = db1
        .row_blob_ref("note_photos", "p1cover")
        .await
        .expect("load exact published blob row");
    let blob_key = source_blob
        .stored()
        .expect("published blob row has exact object authority")
        .object()
        .slot()
        .logical_key();
    let at_rest = storage
        .read_provider_bytes_for_test(blob_key)
        .await
        .expect("blob present in cloud");
    assert_ne!(
        at_rest, plaintext,
        "blob must be encrypted at rest in the cloud"
    );

    // Device B: a fresh DB and its own store dir, same cloud + key + declaration.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db_with_blob(db2_store_dir.clone(), decl());
    let ld = db2_store_dir.clone();
    let (updated, result) = db2
        .pull_exact_store_into(&db1, &storage, &keypair, &ld)
        .await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.values().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        db2.query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "WithPhoto"
    );
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(exact_cache_path(
        &ld,
        &db2.exact_row_blob_ref("note_photos", "p1cover").await,
    ))
    .expect("device B downloaded photo");
    assert_eq!(
        downloaded, plaintext,
        "device B must recover the source bytes after decrypting with the shared key"
    );

    // The pull recorded, atomically with applying the row, that A uploaded this
    // blob — so a later read (after a cache eviction) keys it under A's prefix
    // without a listing scan.
    let row_blob = db2
        .row_blob_ref("note_photos", "p1cover")
        .await
        .expect("read exact pulled row blob reference");
    let uploader = store_database(&db2)
        .activated_store_device_registration(
            row_blob
                .stored()
                .expect("pulled row carries an exact blob reference")
                .locator()
                .uploader()
                .clone(),
        )
        .await
        .expect("load exact blob uploader registration");
    assert_eq!(
        uploader.value().author_pubkey,
        hex::encode(keypair.public_key()),
        "device B's exact blob reference names A's registration",
    );
}

/// One Local-to-Remote transition publishes every host-provided blob below the
/// root, independent of whether peers fill that namespace eagerly or lazily.
#[tokio::test]
async fn make_remote_publishes_host_blobs_with_different_cache_fill() {
    let eager_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
    let lazy_decl = || BlobDecl::new("covers", Provenance::HostProvided, CacheFill::CacheLazy);
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db_with_user_and_host_blobs(
        db1_store_dir.clone(),
        eager_decl(),
        lazy_decl(),
    );
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

    // Both children host-provided, differing only in fill: the photo is CacheEager,
    // the cover CacheLazy. Both inherit the `notes` gate, so a shared note carries
    // both through the inline push in one cycle.
    let ld1 = db1_store_dir.clone();
    db1.execute_test_sql(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithBlobs', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    db1.execute_test_sql(&format!(
        "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('peager01', 'n1', 'cover', 11, '{}', '0000000001000-0000-dev1', '2026-01-01')",
        coven_protocol::blob::content_hash(b"EAGER-BYTES"),
    ))
    .await;
    db1.execute_test_sql(&format!(
        "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at) \
             VALUES ('clazy001', 'n1', 10, '{}', '0000000001001-0000-dev1', '2026-01-01')",
        coven_protocol::blob::content_hash(b"LAZY-BYTES"),
    ))
    .await;
    // The host stores both blobs in the local store (their Local home) before the
    // inline push reads them to upload.
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld1,
        "photos",
        "peager01",
        b"EAGER-BYTES",
    )
    .await
    .expect("store eager blob in local store");
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &ld1,
        "covers",
        "clazy001",
        b"LAZY-BYTES",
    )
    .await
    .expect("store lazy blob in local store");
    storage.make_root_remote(&db1, &ld1, "n1").await;

    // Both blobs reached the cloud — the inline push uploads regardless of fill.
    let eager = db1
        .row_blob_ref("note_photos", "peager01")
        .await
        .expect("load exact eager row blob reference");
    cloud_storage
        .verify_blob_object(eager.stored().expect("eager blob was published"))
        .await
        .expect("verify exact eager blob object");
    let lazy = db1
        .row_blob_ref("note_covers", "clazy001")
        .await
        .expect("load exact lazy row blob reference");
    cloud_storage
        .verify_blob_object(lazy.stored().expect("lazy blob was published"))
        .await
        .expect("verify exact lazy blob object");
}

/// When a peer applies a changeset that DELETEs a blob-bearing row (a gate retract
/// or a genuine delete), it drops that blob's local copy — both cache folders and the
/// local store — or it would leak forever once the row is gone. The peer drops only
/// its own local copy; it never writes a cloud tombstone.
#[tokio::test]
async fn applying_a_blob_bearing_delete_drops_the_local_copy() {
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 =
        crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

    // Source dev1: a note + a CacheEager cover row, the cover present in the cloud.
    db1.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('pdel1234', 'n1', 'cover', {}, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                b"COVERBYTES".len(),
                coven_protocol::blob::content_hash(b"COVERBYTES"),
            ),
        ],
    )
    .await;
    let source_store_dir = db1_store_dir.clone();
    source_store_dir
        .store_local("pdel1234", b"COVERBYTES")
        .await;
    storage
        .make_root_remote(&db1, &source_store_dir, "n1")
        .await;

    // dev2 pulls → the CacheEager cover lands in the evictable cache.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 =
        crate::sync::test_helpers::open_test_db_with_blob(db2_store_dir.clone(), photo_decl());
    let ld = db2_store_dir.clone();
    storage.pull_into(&db2, &ld).await;
    let deleted_reference = db2.exact_row_blob_ref("note_photos", "pdel1234").await;
    let deleted_cache_path = exact_cache_path(&ld, &deleted_reference);
    let deleted_pinned_path = exact_pinned_path(&ld, &deleted_reference);
    assert!(
        deleted_cache_path.exists(),
        "the cover lands in the evictable cache after the first pull",
    );

    // The source makes the root Local again. Its gate retraction carries the child
    // DELETE through the real transition publication path.
    let (_cancel_tx, cancel) = tokio::sync::watch::channel(false);
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        coven_database::StoreDatabase::new(&db1),
        source_store_dir.clone(),
    )
    .make_local(
        cloud_storage,
        None,
        None,
        "notes",
        "n1",
        &HashMap::new(),
        &cancel,
    )
    .await
    .expect("make exact blob root Local");
    assert!(storage
        .publish_pending(&db1, &source_store_dir)
        .await
        .expect("publish exact gate retraction"));
    let (_positions, result) = storage.pull_into(&db2, &ld).await;
    assert_eq!(result.changesets_applied, 1, "the DELETE changeset applied");
    assert!(
        !deleted_pinned_path.exists() && !deleted_cache_path.exists(),
        "applying the blob-bearing DELETE drops the cache copies",
    );
}

#[tokio::test]
async fn local_blob_cleanup_intent_survives_restart_after_position_commit() {
    let cleanup_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy);
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source =
        crate::sync::test_helpers::open_test_db_with_blob(source_store_dir.clone(), cleanup_decl());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('cleanup01', 'n1', 'cover', 7, '{}', \
                         '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(b"cleanup"),
            ),
        ])
        .await;
    source_store_dir.store_local("cleanup01", b"cleanup").await;
    storage
        .make_root_remote(&source, &source_store_dir, "n1")
        .await;

    let database_dir = tempfile::tempdir().expect("database temp dir");
    let database_path = database_dir.path().join("store.db");
    let store_dir = crate::sync::test_helpers::store_dir_for_test_database(&database_path);
    let target = open_blob_test_db_at(&database_path, store_dir.clone(), cleanup_decl());
    storage.pull_into(&target, &store_dir).await;
    let deleted_locator_hash = target
        .row_blob_ref("note_photos", "cleanup01")
        .await
        .expect("load exact blob before deletion")
        .stored()
        .expect("pulled blob has exact storage")
        .locator()
        .locator_hash()
        .to_string();
    let deletion = source
        .capture_test_changeset(&["DELETE FROM note_photos WHERE id = 'cleanup01'"])
        .await;
    storage
        .publish_founder_changeset(deletion, 1)
        .await
        .expect("publish exact blob-bearing Store changeset");
    if store_dir.storage_dir().exists() {
        std::fs::remove_dir_all(store_dir.storage_dir()).expect("remove storage directory");
    }
    let obstructing_file = store_dir.as_ref().join("storage");
    std::fs::write(&obstructing_file, b"not a directory").expect("obstruct cleanup paths");

    let error = storage
        .pull_into_result(&target, &store_dir)
        .await
        .expect_err("post-commit filesystem cleanup failure fails the pull");
    assert!(error.to_string().contains("local blob cleanup"), "{error}");
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM note_photos WHERE id = 'cleanup01'")
            .await
    );
    let pending_before_restart = target
        .cleanup_intent_copy_identities_for_test()
        .await
        .unwrap();
    assert_eq!(
        pending_before_restart,
        [deleted_locator_hash, "local".to_string()]
    );

    tokio::task::spawn_blocking(move || drop(target))
        .await
        .expect("close database before restart");
    std::fs::remove_file(&obstructing_file).expect("restore cleanup paths");

    let restarted = open_blob_test_db_at(&database_path, store_dir.clone(), cleanup_decl());
    let (_updated, second) = storage.pull_into(&restarted, &store_dir).await;
    assert_eq!(second.changesets_applied, 0);
    assert!(!second.asset_downloads_failed);
    assert!(!second.local_blob_cleanup_pending);
    let pending_after_restart = coven_database::StoreDatabase::new(&restarted)
        .cleanup_intent_count_for_test("photos", "cleanup01")
        .await
        .unwrap();
    assert_eq!(pending_after_restart, 0);
}

#[tokio::test]
async fn host_write_cannot_make_a_blob_live_during_its_filesystem_cleanup() {
    let decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db_with_blob(target_store_dir.clone(), decl);
    let storage = std::sync::Arc::new(
        create_store(&target, target_store_dir.clone(), UserKeypair::generate()).await,
    );
    target
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Host write parent', NULL, \
                 '0000000001000-0000-dev2', '2026-01-01'); \
         INSERT INTO note_photos \
         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
         VALUES ('existing-row', 'n1', 'cover', 9, NULL, 'other-blob', \
                 '0000000001000-0000-dev2', '2026-01-01')",
        )
        .await;
    target
        .insert_cleanup_intent_for_test(
            "photos".to_string(),
            "cleanup-race".to_string(),
            "local".to_string(),
        )
        .await
        .unwrap();

    let store_dir = target_store_dir;
    store_dir.store_local("cleanup-race", b"old bytes").await;
    let (reached_filesystem, resume_cleanup) = target.arm_test_pause(
        coven_database::DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
            namespace: "photos".to_string(),
            blob_id: "cleanup-race".to_string(),
        },
    );
    let pull_db = target.clone();
    let pull_storage = storage.clone();
    let pull_store_dir = store_dir.clone();
    let cleanup =
        tokio::spawn(async move { pull_storage.pull_into(&pull_db, &pull_store_dir).await });

    reached_filesystem.notified().await;
    let store_database = coven_database::StoreDatabase::new(&target);
    let host_write = store_database
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "INSERT INTO note_photos \
                         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                         VALUES ('new-row', 'n1', 'cover', 9, NULL, 'cleanup-race', \
                                 '0000000002000-0000-dev2', '2026-01-01')",
                [],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        })
        .await;
    let host_update = store_database
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "UPDATE note_photos SET blob_id = 'cleanup-race', \
                         _updated_at = '0000000002001-0000-dev2' \
                         WHERE id = 'existing-row'",
                [],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        })
        .await;
    resume_cleanup.notify_one();
    cleanup.await.expect("cleanup pull task");

    assert!(
        host_write.is_err(),
        "the host insert must abort while the cleanup intent owns the blob",
    );
    assert!(
        host_update.is_err(),
        "the host update must abort while the cleanup intent owns the blob",
    );
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM note_photos WHERE id = 'new-row'")
            .await
    );
    assert!(
        target
            .test_row_exists(
                "SELECT 1 FROM note_photos \
         WHERE id = 'existing-row' AND blob_id = 'other-blob'",
            )
            .await
    );
    assert!(
        !store_dir
            .local_blob_path("photos", "cleanup-race")
            .unwrap()
            .exists(),
        "cleanup removes the unreferenced old bytes",
    );
}

#[tokio::test]
async fn concurrent_local_cleanup_drains_share_one_intent_owner() {
    use coven_database::DatabaseTestPoint;

    let decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db_with_blob(target_store_dir.clone(), decl);
    target
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Cleanup parent', NULL, \
                 '0000000001000-0000-dev2', '2026-01-01');",
        )
        .await;
    target
        .insert_cleanup_intent_for_test(
            "photos".to_string(),
            "shared-intent".to_string(),
            "local".to_string(),
        )
        .await
        .unwrap();

    let store_dir = target_store_dir.clone();
    store_dir.store_local("shared-intent", b"old bytes").await;
    let mut points = target.observe_test_points();
    let before_filesystem = DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
        namespace: "photos".to_string(),
        blob_id: "shared-intent".to_string(),
    };
    let (first_reached_filesystem, resume_first) = target.arm_test_pause(before_filesystem.clone());

    let first_db = coven_database::StoreDatabase::new(&target);
    let first = tokio::spawn(async move {
        coven_database::LocalBlobCleanup::new(&first_db)
            .drain()
            .await
    });
    first_reached_filesystem.notified().await;
    assert_eq!(
        points.recv().await,
        Some(DatabaseTestPoint::LocalBlobCleanupRequested)
    );
    assert_eq!(
        points.recv().await,
        Some(DatabaseTestPoint::LocalBlobCleanupAcquired)
    );
    assert_eq!(points.recv().await, Some(before_filesystem));

    let host_re_reference = coven_database::StoreDatabase::new(&target)
        .run_host_store_write_for_test(None, None, move |tx| {
            tx.execute(
                "INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                     VALUES ('blocked-row', 'n1', 'cover', 9, NULL, 'shared-intent', \
                             '0000000002000-0000-dev2', '2026-01-01')",
                [],
            )
            .map(|_| ())
            .map_err(coven_database::DbError::from)
        })
        .await;
    assert!(
        host_re_reference.is_err(),
        "the cleanup intent rejects a host row re-reference"
    );

    let second_db = coven_database::StoreDatabase::new(&target);
    let second = tokio::spawn(async move {
        coven_database::LocalBlobCleanup::new(&second_db)
            .drain()
            .await
    });
    assert_eq!(
        points.recv().await,
        Some(DatabaseTestPoint::LocalBlobCleanupRequested)
    );

    resume_first.notify_one();
    assert_eq!(
        points.recv().await,
        Some(DatabaseTestPoint::LocalBlobCleanupFinished),
        "the first drain must finish before the second drain acquires cleanup ownership"
    );
    assert_eq!(
        points.recv().await,
        Some(DatabaseTestPoint::LocalBlobCleanupAcquired)
    );
    assert_eq!(
        points.recv().await,
        Some(DatabaseTestPoint::LocalBlobCleanupFinished)
    );
    assert!(!first.await.expect("first drain task").unwrap());
    assert!(!second.await.expect("second drain task").unwrap());

    target
        .execute_test_sql(
            "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
         VALUES ('live-row', 'n1', 'cover', 9, NULL, 'shared-intent', \
                 '0000000003000-0000-dev2', '2026-01-01')",
        )
        .await;
    store_dir
        .store_local("shared-intent", b"recreated bytes")
        .await;
    assert!(
        !coven_database::LocalBlobCleanup::new(&coven_database::StoreDatabase::new(&target))
            .drain()
            .await
            .unwrap()
    );
    assert!(
        store_dir
            .local_blob_path("photos", "shared-intent")
            .unwrap()
            .exists(),
        "a live blob recreated after cleanup is not owned by an old drain"
    );
}

#[tokio::test]
async fn blob_changing_update_keeps_old_blob_copy_while_another_row_references_it() {
    let decl = photo_decl_with_blob_id_column();
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 =
        crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), decl.clone());
    let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

    db1.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'SharedBlob', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('photo-a', 'n1', 'cover', 12, '{h}', 'sharedblob', '0000000001000-0000-dev1', '2026-01-01')",
                h = coven_protocol::blob::content_hash(b"SHARED-BYTES"),
            ),
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('photo-b', 'n1', 'cover', 12, '{h}', 'sharedblob', '0000000001000-0000-dev1', '2026-01-01')",
                h = coven_protocol::blob::content_hash(b"SHARED-BYTES"),
            ),
        ],
    )
    .await;
    let source_store_dir = db1_store_dir.clone();
    source_store_dir
        .store_local("sharedblob", b"SHARED-BYTES")
        .await;
    storage
        .make_root_remote(&db1, &source_store_dir, "n1")
        .await;

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db_with_blob(db2_store_dir.clone(), decl);
    let ld = db2_store_dir.clone();
    let (_positions, result) = storage.pull_into(&db2, &ld).await;
    assert_eq!(result.changesets_applied, 1);
    let shared_reference = db2.exact_row_blob_ref("note_photos", "photo-b").await;
    let shared_cache_path = exact_cache_path(&ld, &shared_reference);
    assert!(
        shared_cache_path.exists(),
        "the shared CacheEager blob lands in the cache",
    );

    source_store_dir.store_local("newblob", b"NEW-BYTES").await;
    let cs2 = db1
        .capture_test_changeset(&[&format!(
            "UPDATE note_photos \
             SET cloud_path = 'newblob', size = 9, hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'photo-a'",
            coven_protocol::blob::content_hash(b"NEW-BYTES"),
        )])
        .await;
    storage
        .publish_founder_changeset(cs2, 1)
        .await
        .expect("publish exact blob-bearing Store changeset");

    let (_updated, result) = storage.pull_into(&db2, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(
        db2.test_row_exists(
            "SELECT 1 FROM note_photos WHERE id = 'photo-b' AND cloud_path = 'sharedblob'",
        )
        .await,
        "another row still references the old blob",
    );
    assert!(
        shared_cache_path.exists(),
        "a blob-changing update must not drop a copy another live row still references",
    );
    assert!(
        exact_cache_path(&ld, &db2.exact_row_blob_ref("note_photos", "photo-a").await).exists(),
        "the replacement blob lands in the cache",
    );
}

#[tokio::test]
async fn pull_rejects_store_commit_missing_its_signature_when_chain_exists() {
    let founder = UserKeypair::generate();
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), founder.clone()).await;

    let chain = ExactMembershipChain::load(&storage, &cloud_storage).await;

    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &founder,
            "dev1",
            1,
            &cs,
            Some(
                chain
                    .founder_coord()
                    .cloned()
                    .expect("exact membership has a founder coordinate"),
            ),
        )
        .await;
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &founder).await;
    let mut unsigned: serde_json::Value = serde_json::from_slice(&graph.commit.to_bytes()).unwrap();
    unsigned
        .as_object_mut()
        .expect("Store commit is a JSON object")
        .remove("signature");
    let commit_ref = graph
        .replace_commit_bytes_before_validation(
            serde_json::to_vec(&unsigned).unwrap(),
            graph.commit.commit_hash(),
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;
    let expected_stream_id = commit_stream_id(&commit_ref);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (_, result) = storage
        .pull_into_result(&db2, &db2_store_dir)
        .await
        .expect("a Store commit without its required signature is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(
        matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Commit { device_id, commit },
                reason: HeldStorePositionReason::ObjectUnreadableProtocol { key, source },
            } if device_id == &expected_stream_id
                && commit == &commit_ref
                && key == commit_ref.object.slot().logical_key()
                && source.to_string().contains("missing field `signature`")
        ),
        "unexpected held position: {:#?}",
        result.held_positions[0]
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(
        db2.materialized_sequences().await.get(&expected_stream_id),
        None,
    );
}

/// Owner anchoring (issue #95/#102): a puller with a pinned owner refuses a chain
/// whose founder is a different key — the wipe-and-refound takeover — rather than
/// adopting it and authorizing the attacker.
#[tokio::test]
async fn pull_refuses_a_chain_not_anchored_to_the_pinned_owner() {
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let storage = create_store(&source, source_store_dir.clone(), UserKeypair::generate()).await;
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let loaded = storage
        .open_into(&db2, db2_store_dir.clone())
        .await
        .expect("open exact Store before replacing the owner pin");

    // The puller has a different owner pinned from the exact root authority.
    let owner = UserKeypair::generate();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = loaded.membership_for_test().await;
    assert!(
        result.as_ref().is_err_and(|error| error
            .to_string()
            .contains("Store owner anchor differs from its signed root")),
        "a chain founded by a non-owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// Owner anchoring (issue #104/#102): a puller with a pinned owner refuses an
/// empty membership listing — the chain was wiped — rather than falling open to
/// "no chain, accept everything."
#[tokio::test]
async fn pull_refuses_wiped_membership_when_owner_pinned() {
    let owner = UserKeypair::generate();
    let owner_pubkey = hex::encode(owner.public_key());
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&source, source_store_dir.clone(), owner).await;
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let loaded = storage
        .open_into(&db2, db2_store_dir.clone())
        .await
        .expect("open exact Store before removing its membership head");
    let chain = loaded
        .membership_for_test()
        .await
        .expect("load exact membership");
    let founder_head = chain
        .head_refs()
        .iter()
        .find(|head| head.coord.author_pubkey == owner_pubkey)
        .expect("founder has an exact membership head")
        .clone();

    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();
    cloud_storage
        .delete_protocol_object(&founder_head.object)
        .await
        .expect("remove exact founder membership head");

    let result = loaded.membership_for_test().await;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.to_string().contains("membership")),
        "an empty chain with a pinned owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

struct PersistedCycleRemoval {
    storage: std::sync::Arc<TestStore>,
    cloud_storage: std::sync::Arc<CloudSyncConnection>,
    db: coven_database::Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    founder_pubkey: String,
    second_owner_head: coven_protocol::membership::MembershipHeadRef,
    removed_member_pubkey: String,
}

impl PersistedCycleRemoval {
    async fn build() -> Self {
        let founder = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let removed_member = UserKeypair::generate();
        let founder_pubkey = hex::encode(founder.public_key());
        let second_owner_pubkey = hex::encode(second_owner.public_key());
        let removed_member_pubkey = hex::encode(removed_member.public_key());
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let (storage, cloud_storage) =
            create_store_fixture(&db, db_store_dir.clone(), founder.clone()).await;
        let encryption = EncryptionService::from_key([42; 32]);
        storage
            .admit_member(
                &db,
                db_store_dir.clone(),
                &founder,
                &second_owner_pubkey,
                None,
                MemberRole::Member,
                &encryption,
                "Test Store",
            )
            .await
            .expect("admit second Owner as a Member");
        let second_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let second_owner_db =
            crate::sync::test_helpers::open_test_db(second_owner_db_store_dir.clone());
        storage
            .activate_joined_device(
                &db,
                db_store_dir.clone(),
                &second_owner_db,
                second_owner_db_store_dir.clone(),
                &second_owner,
                "2026-03-01T00:00:45Z",
            )
            .await
            .expect("activate second Owner device");
        storage
            .promote_active_member_fixture(
                &db,
                db_store_dir.clone(),
                &second_owner_db,
                second_owner_db_store_dir.clone(),
                &founder,
                &second_owner,
                &encryption,
            )
            .await
            .expect("promote active second Owner");
        let loaded = storage
            .open_into(&db, db_store_dir.clone())
            .await
            .expect("load membership after second Owner promotion");
        let mut chain =
            ExactMembershipChain::load_from_device(&storage, &cloud_storage, &loaded).await;
        let second_owner_stream = membership_author_stream(&chain, &second_owner);
        let add_member = chain
            .signed_set_member_in_stream(
                &founder,
                membership_author_stream(&chain, &founder),
                pubkey_hex(&removed_member),
                None,
                MemberRole::Member,
                "2026-03-01T00:02:00Z".to_string(),
            )
            .expect("active Owner signs membership grant");
        chain.publish_entry(add_member, &founder).await;
        let remove_member = chain
            .signed_remove_member_in_stream(
                &second_owner,
                second_owner_stream,
                pubkey_hex(&removed_member),
                "2026-03-01T00:03:00Z".to_string(),
            )
            .expect("active Owner removes membership grant");
        chain.publish_entry(remove_member, &second_owner).await;
        let second_owner_head = chain
            .head_refs()
            .iter()
            .find(|head| head.coord.author_pubkey == second_owner_pubkey)
            .expect("second Owner has an exact membership head")
            .clone();

        let initial = loaded
            .membership_for_test()
            .await
            .expect("accept and persist the complete multi-author chain");
        assert!(!initial.can_write_now(&removed_member_pubkey));

        Self {
            storage,
            cloud_storage,
            db,
            db_store_dir,
            founder_pubkey,
            second_owner_head,
            removed_member_pubkey,
        }
    }
}

#[tokio::test]
async fn pinned_cycle_recovers_persisted_authors_when_membership_listing_is_empty() {
    let fixture = PersistedCycleRemoval::build().await;

    let recovered = fixture
        .storage
        .bind_founder_device(&fixture.db, fixture.db_store_dir.clone())
        .await
        .expect("load persisted-author Store")
        .membership_for_test()
        .await
        .expect("empty LIST must use the persisted author floors");

    assert_eq!(
        fixture
            .db
            .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
            .await
            .expect("read persisted owner pin")
            .as_deref(),
        Some(fixture.founder_pubkey.as_str())
    );
    assert!(!recovered.can_write_now(&fixture.removed_member_pubkey));
}

#[tokio::test]
async fn cycle_rejects_missing_state_required_by_a_persisted_floor() {
    let fixture = PersistedCycleRemoval::build().await;
    fixture
        .cloud_storage
        .delete_protocol_object(&fixture.second_owner_head.object)
        .await
        .expect("delete exact persisted membership head");

    let error = match fixture
        .storage
        .bind_founder_device(&fixture.db, fixture.db_store_dir.clone())
        .await
        .expect("load persisted-author Store")
        .membership_for_test()
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a persisted author floor requires its signed head"),
    };

    assert!(
        error.to_string().contains("durable cursor"),
        "missing persisted-author state must be membership tamper: {error}"
    );
}

#[tokio::test]
async fn mid_cycle_empty_membership_listing_loads_an_advanced_head_from_the_floor() {
    let owner = UserKeypair::generate();
    let owner_pubkey = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&source, source_store_dir.clone(), owner.clone()).await;
    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    let loaded = storage
        .open_into(&target, target_store_dir.clone())
        .await
        .expect("bind mid-cycle fixture to its exact Store root");
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();

    let mut chain = ExactMembershipChain::load_from_device(&storage, &cloud_storage, &loaded).await;
    let mut writer = loaded
        .authorize_writer()
        .await
        .expect("authorize the cycle before membership advances");

    let add_member = signed_member_grant(&chain, &owner, &member);
    chain.publish_entry(add_member, &owner).await;
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'AdvancedHead', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "devM",
            1,
            &changeset,
            Some(
                chain
                    .founder_coord()
                    .cloned()
                    .expect("exact membership has a founder coordinate"),
            ),
        )
        .await;
    let stream_id = commit_stream_id(&reference);

    let result = writer
        .pull(None)
        .await
        .expect("pull with an empty mid-cycle membership LIST");
    let updated: HashMap<_, _> = result
        .frontier
        .iter()
        .map(|(device_id, position)| (device_id.clone(), position.coord.sequence()))
        .collect();

    assert_eq!(result.changesets_applied, 1);
    assert!(unauthorized_positions(&result).is_empty());
    assert!(
        target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), Some(&1));
}

/// `list_membership_entries` itself failing (a flaky LIST, not bad chain data) on
/// an owner-pinned store must abort the cycle, not fall open to "no chain,
/// accept everything" — the first failure mode #88 names. A real chain and a
/// changeset are staged so the old fall-open behavior would load no chain and
/// apply the changeset unvalidated; the fail-closed path must instead surface the
/// error and apply nothing.
#[tokio::test]
async fn pull_aborts_when_membership_listing_fails_on_owner_pinned_store() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    // A founder entry + a changeset the owner authored: without the fail-closed
    // guard the cycle would (fail to list, drop to chain=None, then) apply this.
    let _chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'X', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ])
        .await;
    storage
        .publish_changeset(&owner_pk, 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish owner exact Store changeset");

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    storage
        .open_into(&db2, db2_store_dir.clone())
        .await
        .expect("open exact Store before fault injection");
    let failing = std::sync::Arc::new(crate::sync::test_helpers::InterceptedStorage::new(
        cloud_storage.clone(),
        FaultingStorage::membership(1),
    ));
    let store_dir = db2_store_dir.clone();
    let result = crate::sync::store::Store::load(
        coven_database::StoreDatabase::new(&db2),
        failing,
        store_dir,
        owner,
    )
    .await
    .expect("load fault-injected Store")
    .membership_for_test()
    .await;
    assert!(
        result.is_err(),
        "an exact membership read failure on an owner-pinned store must abort the cycle",
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await,
        "nothing is applied when membership cannot be verified",
    );
}

/// The positive case: a chain founded by the pinned owner is accepted, and a
/// changeset signed by that owner applies.
#[tokio::test]
async fn pull_accepts_a_chain_anchored_to_the_pinned_owner() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    // The owner's device is the mock: it signs the head it publishes for
    // `devOwner` with the owner keypair, so the head's author is a current member
    // and passes the head-authorization check.
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    let chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    // The owner authors a signed changeset.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromOwner', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "devOwner",
            1,
            &cs,
            Some(
                chain
                    .founder_coord()
                    .cloned()
                    .expect("exact membership has a founder coordinate"),
            ),
        )
        .await;
    let stream_id = commit_stream_id(&reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(
        db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), Some(&1));
}

#[tokio::test]
async fn pull_authorizes_merge_operations_at_their_exact_predecessor_membership() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let second_owner = UserKeypair::generate();
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&source, source_store_dir.clone(), owner.clone()).await;
    storage
        .device_id("founder")
        .await
        .expect("reserve founder Store producer");
    storage
        .device_id("devOwner")
        .await
        .expect("activate a separate founder-identity Store producer");
    let encryption = EncryptionService::from_key([42; 32]);
    storage
        .admit_member(
            &source,
            source_store_dir.clone(),
            &owner,
            &pubkey_hex(&second_owner),
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("admit second Owner as a Member");
    let second_owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_owner_db =
        crate::sync::test_helpers::open_test_db(second_owner_db_store_dir.clone());
    storage
        .activate_joined_device(
            &source,
            source_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            "2026-03-01T00:00:45Z",
        )
        .await
        .expect("activate second Owner device");
    storage
        .promote_active_member_fixture(
            &source,
            source_store_dir.clone(),
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &owner,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote active second Owner");
    let chain = storage
        .open_into(&source, source_store_dir.clone())
        .await
        .expect("load Store after second Owner promotion")
        .membership_for_test()
        .await
        .expect("load membership after second Owner promotion");
    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'BeforeDemotion', NULL, '0000000002000-0000-owner', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "devOwner",
            1,
            &changeset,
            Some(
                chain
                    .founder_coord()
                    .cloned()
                    .expect("exact membership has a founder coordinate"),
            ),
        )
        .await;

    let second_owner_custody = TestCustody::default();
    second_owner_custody.set_initial_key(encryption.key_bytes());
    storage
        .remove_member(
            &second_owner_db,
            second_owner_db_store_dir.clone(),
            &second_owner,
            &owner_pk,
            &encryption,
            &second_owner_custody,
        )
        .await
        .expect("successor Owner removes founder with exact recovery state");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    storage.pull_into(&target, &target_store_dir).await;
    let (_, result) = storage.pull_into(&target, &target_store_dir).await;

    assert!(result.changesets_applied > 0);
    assert!(unauthorized_positions(&result).is_empty());
    let stream_id = commit_stream_id(&reference);
    assert_eq!(
        store_database(&target)
            .exact_materialized_ref(&stream_id, reference.coord.sequence())
            .await
            .expect("load exact materialized predecessor-authorized commit"),
        Some(reference),
        "{result:#?}"
    );
}

/// An operations commit cannot substitute current membership
/// for its exact predecessor grant authority.
#[tokio::test]
async fn pull_rejects_a_current_owner_changeset_without_a_membership_grant() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&source, source_store_dir.clone(), owner.clone()).await;

    let _chain = ExactMembershipChain::load(&storage, &cloud_storage).await;

    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'MissingGrant', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_changeset("devOwner", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish valid Store changeset before removing its authority");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &owner).await;
    let commit = graph.resign_commit(SCHEMA_VERSION, None).await;
    let reference = graph
        .replace_commit_bytes_before_validation(
            commit.to_bytes(),
            commit.commit_hash(),
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;
    let stream_id = commit_stream_id(&reference);

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (updated, result) = storage.pull_into(&target, &target_store_dir).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), None);
}

/// A signed device head commits to its registration's exact Store stream. A
/// commit from another stream cannot be replayed through that head.
#[tokio::test]
async fn pull_rejects_a_head_that_names_another_device_stream() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&source, source_store_dir.clone(), owner.clone()).await;
    storage
        .device_id("devOwner")
        .await
        .expect("reserve founder producer");
    storage
        .device_id("other-device")
        .await
        .expect("activate second device");

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let pull_store_dir = target_store_dir.clone();
    let (_, activation_result) = storage
        .pull_into_result(&target, &pull_store_dir)
        .await
        .expect("materialize device activation before replacing heads");
    assert!(activation_result.held_positions.is_empty());

    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WrongStreamSigner', NULL, '0000000002000-0000-devOwner', '2026-01-01')",
        ])
        .await;
    let owner_sequence = storage
        .next_commit_sequence("devOwner")
        .await
        .expect("read founder producer sequence");
    let reference = storage
        .publish_changeset("devOwner", owner_sequence, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &owner).await;
    let other_sequence = storage
        .next_commit_sequence("other-device")
        .await
        .expect("read second device sequence");
    let other_reference = storage
        .publish_changeset("other-device", other_sequence, &[], SCHEMA_VERSION)
        .await
        .expect("publish second exact device graph");
    let other = ExactPublishedCommit::load(&storage, &cloud_storage, other_reference, &owner).await;
    cloud_storage
        .delete_protocol_object(&graph.head_object)
        .await
        .expect("remove original stream head");
    other
        .replace_head(
            graph.reference.clone(),
            other.head.author_registration.clone(),
            &other.device_signer,
        )
        .await;
    let expected_stream_id = commit_stream_id(&other.reference);

    let (_, result) = storage
        .pull_into_result(&target, &pull_store_dir)
        .await
        .expect("a head signer mismatch holds only that device");

    assert!(
        result.held_positions.iter().any(|held| matches!(
            (&held.coordinate, &held.reason),
            (
                HeldStoreCoordinate::Head { device_id, .. },
                HeldStorePositionReason::WrongSlot(detail)
            ) if device_id == &expected_stream_id
                && detail.contains("activated successor chain")
        )),
        "unexpected held positions: {:#?}",
        result.held_positions
    );
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(
        target
            .materialized_sequences()
            .await
            .get(&expected_stream_id),
        None
    );
}

/// A member's signed changeset may be pulled before the listing that rebuilds
/// the chain shows the entry that authorizes it: membership entries and
/// changesets are separate, unordered object streams. The changeset carries
/// the coordinate of its authorizing entry, so an exact read resolves that
/// entry even while the listing lags. The changeset remains pending until it
/// can apply; its stream position never advances over missing authority.
#[tokio::test]
async fn pull_resolves_a_changeset_whose_authorizing_entry_lags_the_listing() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&owner_db, owner_db_store_dir.clone(), owner.clone()).await;

    // Founder at (owner, 1); the owner adds the member as a Member at (owner, 2).
    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    let lagging_authority = add_member.coord();
    chain.publish_entry(add_member, &owner).await;
    let hidden_head = chain
        .head_ref_for_stream(
            &lagging_authority.author_pubkey,
            &lagging_authority.author_owner_grant,
            lagging_authority.stream_id,
        )
        .expect("published member Add head")
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .expect("membership head slot key has a .json suffix")
        .to_string();

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-03-01T00:02:00Z",
        )
        .await
        .expect("activate member device");
    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add that is lagging the LIST.
    let cs = member_db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ])
        .await;
    let member_store_dir = member_db_store_dir.clone();
    let reference = storage
        .sync_for_test(&member_db, cs, 0, "", &member, &member_store_dir)
        .await
        .expect("publish member Store changeset")
        .expect("member Store changeset produces a commit");
    let stream_id = commit_stream_id(&reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let lagging = Arc::new(InterceptedStorage::new(
        cloud_storage.clone(),
        MissingProtocolSlot {
            semantic_prefix: hidden_head,
        },
    ));
    let pull_store_dir = db2_store_dir.clone();
    let (store, _device_id) = crate::sync::store::Store::open(
        coven_database::StoreDatabase::new(&db2),
        lagging,
        pull_store_dir,
        &storage.root(),
        &owner,
    )
    .await
    .expect("open Store through lagging membership listing")
    .into_parts();
    let mut writer = store
        .authorize_writer()
        .await
        .expect("authorize pull through lagging membership listing");
    let activation = writer
        .pull(None)
        .await
        .expect("materialize member device activation through lagging membership listing");
    assert!(
        activation.held_positions.is_empty(),
        "device activation must materialize before its stream is discovered: {activation:#?}"
    );
    let result = writer
        .pull(None)
        .await
        .expect("pull through lagging membership listing");
    let updated = db2.materialized_sequences().await;

    // The lagging entry was fetched by coordinate and the changeset applied — not
    // dropped as non-member, and not surfaced as a rejection.
    assert_eq!(result.changesets_applied, 1);
    assert!(unauthorized_positions(&result).is_empty());
    assert!(
        db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), Some(&1));
}

/// An operations commit cannot substitute a different membership grant for the
/// authority contained in its exact predecessor membership.
#[tokio::test]
async fn pull_skips_and_surfaces_a_forged_changeset_whose_grant_does_not_authorize_it() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let outsider = UserKeypair::generate();
    // Head signed by the owner (a current member) so the head passes its check and
    // pull reaches the changeset-level judgment.
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_outsider = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&outsider),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs outsider grant");
    let unrelated_authority = add_outsider.coord();
    chain.publish_entry(add_outsider, &owner).await;

    // Replace the valid commit's authority with another membership coordinate.
    // The commit remains signed, but its proof no longer matches its predecessor.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-devX', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "devX",
            1,
            &cs,
            Some(unrelated_authority),
        )
        .await;
    let stream_id = commit_stream_id(&reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    // Nothing applies and the durable frontier remains before the forged commit.
    assert_eq!(result.changesets_applied, 0);
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    let invalid_authority = missing_exact_membership_authority_positions(&result);
    assert_eq!(
        invalid_authority.len(),
        1,
        "unexpected forged-authorization result: {result:#?}"
    );
    assert_eq!(
        invalid_authority[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id: stream_id.clone(),
            commit: reference,
        }
    );
    assert_eq!(updated.get(&stream_id), None);
    assert_eq!(db2.materialized_sequences().await.get(&stream_id), None,);
}

/// Issue #86 — a changeset whose signature does not verify (forged or corrupt in
/// transit) is rejected, logged at error, and surfaced as `invalid_signatures` so
/// the host can warn; the position holds at it. The signature check runs before the
/// authorization judgment, so a corrupt signature is reported as an invalid
/// signature, not as unauthorized.
#[tokio::test]
async fn pull_holds_and_surfaces_a_changeset_with_an_invalid_signature() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    let chain = ExactMembershipChain::load(&storage, &cloud_storage).await;

    // The owner (a current member) authors a changeset that WOULD be authorized,
    // then its signature is corrupted. The signature check must reject it before
    // authorization is even considered.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Tampered', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "dev1",
            1,
            &cs,
            Some(
                chain
                    .founder_coord()
                    .cloned()
                    .expect("exact membership has a founder coordinate"),
            ),
        )
        .await;
    let graph = ExactPublishedCommit::load(&storage, &cloud_storage, reference, &owner).await;
    let mut forged: serde_json::Value = serde_json::from_slice(&graph.commit.to_bytes()).unwrap();
    forged["signature"] = serde_json::Value::String("0".repeat(128));
    let commit_ref = graph
        .replace_commit_bytes_before_validation(
            serde_json::to_vec(&forged).unwrap(),
            graph.commit.commit_hash(),
            graph.head.author_registration.clone(),
            &graph.device_signer,
        )
        .await;
    let expected_stream_id = commit_stream_id(&graph.reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (_, result) = storage
        .pull_into_result(&db2, &db2_store_dir)
        .await
        .expect("a Store commit with an invalid signature is held");

    // Nothing applied; surfaced as an invalid signature (NOT unauthorized) and the
    // position holds at the bad object.
    assert_eq!(result.held_positions.len(), 1);
    assert_eq!(
        result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit {
                device_id: expected_stream_id.clone(),
                commit: commit_ref.clone(),
            },
            reason: HeldStorePositionReason::InvalidSignature,
        }
    );
    assert!(unauthorized_positions(&result).is_empty());
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(
        db2.materialized_sequences().await.get(&expected_stream_id),
        None
    );
}

/// A member publishes a changeset from its activated device, then the Owner
/// removes that member before another device pulls it. The removal is not
/// retroactive: the commit's exact predecessor membership proves that its
/// author was allowed to write when the commit was created.
#[tokio::test]
async fn pull_accepts_a_member_write_authorized_before_removal() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&owner_db, owner_db_store_dir.clone(), owner.clone()).await;

    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    chain.publish_entry(add_member, &owner).await;
    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-03-01T00:02:00Z",
        )
        .await
        .expect("activate member device");

    let cs = member_db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromRemoved', NULL, '0000000003000-0000-devM', '2026-01-01')",
        ])
        .await;
    let member_store_dir = member_db_store_dir.clone();
    let reference = storage
        .sync_for_test(&member_db, cs, 0, "", &member, &member_store_dir)
        .await
        .expect("publish member Store changeset")
        .expect("member Store changeset produces a commit");
    let stream_id = commit_stream_id(&reference);

    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    chain.publish_entry(remove_member, &owner).await;

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (_, activation_result) = storage.pull_into(&db2, &db2_store_dir).await;
    assert!(
        activation_result.held_positions.is_empty(),
        "device activation must materialize before its stream is discovered: {activation_result:#?}"
    );
    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert!(
        db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert!(
        unauthorized_positions(&result).is_empty(),
        "causally authorized member write was held: {result:#?}"
    );
    assert_eq!(
        updated.get(&stream_id).copied(),
        Some(reference.coord.sequence())
    );
    assert_eq!(
        store_database(&db2)
            .exact_materialized_ref(&stream_id, reference.coord.sequence())
            .await
            .expect("load causally authorized member Store position"),
        Some(reference),
    );
}

#[tokio::test]
async fn removed_member_candidate_cleanup_verifies_the_exact_revocation_witness() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&owner_db, owner_db_store_dir.clone(), owner.clone()).await;
    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    chain.publish_entry(add_member, &owner).await;

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-03-01T00:02:00Z",
        )
        .await
        .expect("activate member device");
    let member_changeset = member_db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('member-candidate', 'Member candidate', NULL, \
                   '0000000003000-0000-member', '2026-01-01')",
        ])
        .await;
    let member_store_dir = member_db_store_dir.clone();
    let candidate = storage
        .sync_for_test(
            &member_db,
            member_changeset,
            0,
            "",
            &member,
            &member_store_dir,
        )
        .await
        .expect("publish member candidate")
        .expect("member candidate produces a Store commit");
    let candidate_graph =
        ExactPublishedCommit::load_as(&storage, &cloud_storage, candidate, &member).await;
    let write_id = candidate_graph.commit.write_id.clone();

    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:04:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    chain.publish_entry(remove_member, &owner).await;
    let owner_changeset = owner_db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('revocation-witness', 'Revocation witness', NULL, \
                   '0000000005000-0000-owner', '2026-01-01')",
        ])
        .await;
    let owner_store_dir = owner_db_store_dir.clone();
    let owner_sequence = storage
        .latest_store_position()
        .await
        .expect("read Owner witness predecessor")
        .map_or(0, |reference| reference.coord.sequence());
    storage
        .sync_for_test(
            &owner_db,
            owner_changeset,
            owner_sequence,
            "",
            &owner,
            &owner_store_dir,
        )
        .await
        .expect("publish accepted revocation witness")
        .expect("revocation witness produces a Store commit");

    storage.fail_nth_exact_delete_of(
        &[
            candidate_graph.reference.object.slot(),
            candidate_graph.head_object.slot(),
        ],
        1,
    );
    let member_device = storage
        .bind_device_in(&member_db, member_db_store_dir.clone(), &member)
        .await
        .expect("bind removed member for cleanup pull");
    member_device
        .pull_store()
        .await
        .expect_err("interrupted cleanup retains the verified retraction journal");
    storage
        .bind_device_in(&member_db, member_db_store_dir.clone(), &member)
        .await
        .expect("load removed-member Store")
        .cleanup_merge_candidate_for_test(write_id.clone())
        .await
        .expect("verify and resume removed-member candidate cleanup");
    coven_database::StoreDatabase::new(&member_db)
        .finish_retracted_merge_candidate_cleanup(write_id.clone())
        .await
        .expect("finalize removed-member candidate cleanup");
    assert!(
        storage.provider_object_is_absent(candidate_graph.reference.object.slot().logical_key())
    );
    assert!(storage.provider_object_is_absent(candidate_graph.head_object.slot().logical_key()));
    assert!(matches!(
        coven_database::StoreDatabase::new(&member_db)
            .write_status(&write_id)
            .await
            .expect("read retracted member write"),
        coven_protocol::write::WriteStatus::Resolved(coven_protocol::write::WriteResolution::Retracted { witness })
            if witness.original_position().commit() == &candidate_graph.reference
    ));
    assert!(!coven_database::StoreDatabase::new(&member_db)
        .merge_candidate_cleanup_pending(&write_id)
        .await
        .expect("read completed member cleanup"));
    assert!(coven_database::StoreDatabase::new(&member_db)
        .protocol_inert_object(candidate_graph.head_object)
        .await
        .expect("read terminal member head")
        .is_some());
}

/// A hash-linked membership chain detects a missing MIDDLE entry via `previous_hash`,
/// but nothing points forward to a missing TAIL entry, so a chain reload whose
/// successor-slot walk stops at an absent slot still hash-links cleanly and reads
/// the removed member as current. The owner removes the member at (owner, 3) and
/// publishes a head covering it; a puller learns the full chain while the provider
/// serves everything. Then the provider lags: the slot holding the Remove's head
/// reads as absent — indistinguishable from "never published" — while every keyed
/// exact read still serves. The chain load must fail when its walk regresses
/// below the durable cursor the earlier pull committed: a shorter chain is
/// indistinguishable from tampering.
#[tokio::test]
async fn removed_member_is_not_re_admitted_by_a_lagging_listing() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    chain.publish_entry(add_member, &owner).await;
    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    let remove_coord = remove_member.coord();
    chain.publish_entry(remove_member, &owner).await;

    // The puller learns the full chain, Remove included, while the provider
    // still serves everything.
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let store_dir = db2_store_dir.clone();
    let (_, first) = storage.pull_into(&db2, &store_dir).await;
    assert_eq!(first.changesets_applied, 0);

    // The provider now lags: the slot holding the Remove's head reads as absent,
    // while keyed exact reads still serve every object.
    let hidden = chain
        .head_ref_for_stream(
            &remove_coord.author_pubkey,
            &remove_coord.author_owner_grant,
            remove_coord.stream_id,
        )
        .expect("published Remove head")
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .expect("membership head slot key has a .json suffix")
        .to_string();
    let lagging = std::sync::Arc::new(InterceptedStorage::new(
        cloud_storage.clone(),
        MissingProtocolSlot {
            semantic_prefix: hidden,
        },
    ));

    // The lagging view is refused outright: the walk comes up shorter than the
    // durable cursor the first pull committed, and a shorter chain is
    // indistinguishable from tampering, so the load fails loud instead of
    // re-admitting whatever the truncated walk hash-links into.
    let error = crate::sync::store::Store::load(
        coven_database::StoreDatabase::new(&db2),
        lagging,
        store_dir,
        owner,
    )
    .await
    .expect("load lagging Store")
    .membership_for_test()
    .await
    .expect_err("a chain regressing below the durable cursor is refused");
    assert!(
        format!("{error:?}").contains("regressed below its durable cursor"),
        "unexpected refusal: {error:?}"
    );
}

/// A membership entry is not authoritative until its author publishes a signed
/// head covering it. A changeset cannot turn a stored-but-uncommitted Add into an
/// authorization grant merely by naming that entry's coordinate.
#[tokio::test]
async fn pull_rejects_a_changeset_naming_a_grant_no_head_covers() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    // The owner publishes a head covering only the founder entry (seq 1) before
    // adding the member, so the Add at seq 2 is uploaded but no head certifies it
    // yet — genuinely uncommitted, not just list-lagging.
    let chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    let grant = add_member.coord();
    let (prepared, _) = coven_storage::prepare_membership_entry(
        &*cloud_storage,
        storage.root().store_root_hash,
        &add_member,
    )
    .await
    .expect("prepare uncommitted exact membership entry");
    cloud_storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish uncommitted exact membership entry");

    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add no head covers yet.
    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromUncommittedMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(&cloud_storage, &owner, "devM", 1, &cs, Some(grant))
        .await;
    let stream_id = commit_stream_id(&reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(
        missing_exact_membership_authority_positions(&result).len(),
        1,
        "unexpected uncovered-grant result: {result:#?}"
    );
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), None);
}

#[tokio::test]
async fn relocated_membership_grant_cannot_authorize_a_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let relocated_author = hex::encode(UserKeypair::generate().public_key());
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&source, source_store_dir.clone(), owner.clone()).await;

    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    let owner_grant = add_member.coord();
    let grant_bytes = serde_json::to_vec(&add_member).expect("serialize exact membership grant");
    chain.publish_entry(add_member, &owner).await;
    let relocated_prefix = coven_protocol::store_commit::membership_entry_semantic_prefix(
        &relocated_author,
        &owner_grant.author_owner_grant,
        owner_grant.stream_id,
        2,
        owner_grant.entry_hash,
    );
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        storage.root().store_root_hash,
        coven_protocol::objects::ProtocolObjectDomain::StoreMembershipEntry,
    );
    let slot = cloud_storage
        .allocate_protocol_slot(&context, &relocated_prefix, ".json")
        .await
        .expect("allocate relocated exact membership grant slot");
    let prepared = cloud_storage
        .prepare_protocol_object(&context, slot, &relocated_prefix, grant_bytes)
        .expect("prepare relocated exact membership grant");
    cloud_storage
        .create_protocol_object(&prepared)
        .await
        .expect("relocate the grant to another author's coordinate");

    let changeset = source
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'RelocatedGrant', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "devM",
            1,
            &changeset,
            Some(MembershipCoord {
                author_pubkey: relocated_author,
                author_owner_grant: owner_grant.author_owner_grant,
                stream_id: owner_grant.stream_id,
                seq: 2,
                entry_hash: owner_grant.entry_hash,
            }),
        )
        .await;
    let stream_id = commit_stream_id(&reference);

    let target_store_dir = crate::sync::test_helpers::test_store_dir();
    let target = crate::sync::test_helpers::open_test_db(target_store_dir.clone());
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (_, result) = storage
        .pull_into_result(&target, &target_store_dir)
        .await
        .expect("a relocated membership grant holds its Store stream");

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(
        missing_exact_membership_authority_positions(&result).len(),
        1,
        "unexpected relocated-grant result: {result:#?}"
    );
    assert!(
        !target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(target.materialized_sequences().await.get(&stream_id), None);
}

/// A storage read failure while resolving a grant holds the affected stream at
/// the undecided commit. The pull must not replace an unavailable committed-chain
/// read with a bare keyed entry or abort independent streams.
#[tokio::test]
async fn pull_holds_the_position_when_the_mid_cycle_membership_list_fails() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    // Capture the cycle's founder-only membership view before committing the
    // member Add and activating that member's device.
    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    storage
        .open_into(&db2, db2_store_dir.clone())
        .await
        .expect("load exact committed membership prefix");
    let failing = Arc::new(crate::sync::test_helpers::InterceptedStorage::new(
        cloud_storage.clone(),
        FaultingStorage::membership(0),
    ));
    let store_dir = db2_store_dir.clone();
    let retained_store = crate::sync::store::Store::load(
        store_database(&db2),
        failing.clone(),
        store_dir,
        owner.clone(),
    )
    .await
    .expect("bind fault-injected Store");
    let mut retained_writer = retained_store
        .authorize_writer()
        .await
        .expect("authorize retained founder-only membership");

    let add_member = signed_member_grant(&chain, &owner, &member);
    chain.publish_entry(add_member, &owner).await;
    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    storage
        .activate_joined_device(
            &db1,
            db1_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            "2026-03-01T00:02:00Z",
        )
        .await
        .expect("activate member device");

    let cs = member_db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ])
        .await;
    let publish_store_dir = member_db_store_dir.clone();
    let reference = storage
        .sync_for_test(&member_db, cs, 0, "", &member, &publish_store_dir)
        .await
        .expect("publish member Store changeset")
        .expect("member Store changeset produces a commit");
    let stream_id = commit_stream_id(&reference);

    failing.interceptor().arm_membership(1);
    let result = retained_writer
        .pull(None)
        .await
        .expect("a failed membership reload holds only the affected stream");

    // The failed read leaves authorization undecided and the position unchanged.
    assert!(result.held_positions.iter().any(|held| matches!(
        &held.reason,
        HeldStorePositionReason::InvalidObjectPull(error)
            if error.to_string().contains("forced exact membership read failure")
    )));
    assert!(
        !db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(db2.materialized_sequences().await.get(&stream_id), None);
}

/// Cycle start and the mid-cycle reload now share the same head-committed,
/// watermarked loader, so a membership head that regresses below a reader's
/// accepted watermark is refused wherever it is read — there is no longer a
/// second, unwatermarked path a stale or rewound head could slip through. Driven
/// across two cycles on the same reader database: the first accepts the head at
/// the Remove's seq (persisting the watermark), the second serves a head from
/// before the Remove and must be refused rather than adopted.
#[tokio::test]
async fn pull_refuses_a_membership_head_that_regresses_the_watermark_across_cycles() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db2, db2_store_dir.clone(), owner.clone()).await;

    let mut chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let add_member = signed_member_grant(&chain, &owner, &member);
    chain.publish_entry(add_member.clone(), &owner).await;
    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    let remove_coord = remove_member.coord();
    chain.publish_entry(remove_member, &owner).await;
    let remove_head = chain
        .head_ref_for_stream(
            &remove_coord.author_pubkey,
            &remove_coord.author_owner_grant,
            remove_coord.stream_id,
        )
        .expect("load exact remove membership head reference")
        .clone();

    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // First cycle: accepts the head at seq 3 (member removed), persisting the
    // reader's watermark at 3.
    storage.pull_into(&db2, &db2_store_dir).await;

    cloud_storage
        .delete_protocol_object(&remove_head.object)
        .await
        .expect("hide exact remove head to serve the predecessor as terminal");

    let result = storage.pull_into_result(&db2, &db2_store_dir).await;
    assert!(
        matches!(result, Err(TestPullError::Open(_))),
        "a head regressing below the accepted watermark must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// The fail-open the auditor reproduced: a non-empty but MALFORMED chain (here a
/// founder with a corrupt signature, so `download_chain` errors) on a pinned-owner
/// store must be refused — not treated as "no chain, accept everything", which
/// would let an attacker who wipes+refounds with junk get their changesets applied.
#[tokio::test]
async fn pull_refuses_a_malformed_chain_when_owner_pinned() {
    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db2, db2_store_dir.clone(), UserKeypair::generate()).await;
    let chain = ExactMembershipChain::load(&storage, &cloud_storage).await;
    let founder = chain.entries().first().expect("exact founder entry");
    let coord = founder.coord();
    let head_ref = chain
        .head_ref_for_stream(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
        )
        .expect("exact founder head reference");
    let head = storage
        .load_membership_head_for_test(head_ref)
        .await
        .expect("load exact founder head");
    let mut bad = founder.clone();
    bad.corrupt_signature_for_test();
    let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
        storage.root().store_root_hash,
        coven_protocol::objects::ProtocolObjectDomain::StoreMembershipEntry,
    );
    let prefix = coven_protocol::store_commit::membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
        coord.entry_hash,
    );
    cloud_storage
        .delete_protocol_object(&head.body.entry.object)
        .await
        .expect("delete exact founder entry before corruption");
    let prepared = cloud_storage
        .prepare_protocol_object(
            &context,
            head.body.entry.object.slot().clone(),
            &prefix,
            serde_json::to_vec(&bad).expect("serialize corrupt founder"),
        )
        .expect("prepare corrupt exact founder entry");
    cloud_storage
        .create_protocol_object(&prepared)
        .await
        .expect("publish corrupt exact founder entry");

    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &storage.protocol_founder_pubkey())
        .await
        .unwrap();

    let result = storage.pull_into_result(&db2, &db2_store_dir).await;
    assert!(
        matches!(result, Err(TestPullError::Open(_))),
        "a malformed chain on a pinned-owner store must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// The honored case: a head authored by a current member (here a second device
/// whose head and changeset the owner signs) is kept, and its changeset applies.
#[tokio::test]
async fn pull_honors_a_head_authored_by_a_current_member() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    // The mock is the owner's device, so the head it publishes for `devA` is
    // owner-signed — a current member.
    let db1_store_dir = crate::sync::test_helpers::test_store_dir();
    let db1 = crate::sync::test_helpers::open_test_db(db1_store_dir.clone());
    let (storage, cloud_storage) =
        create_store_fixture(&db1, db1_store_dir.clone(), owner.clone()).await;

    let chain = ExactMembershipChain::load(&storage, &cloud_storage).await;

    let cs = db1
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromMember', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ])
        .await;
    let reference = storage
        .publish_exact_changeset_with_authority(
            &cloud_storage,
            &owner,
            "devA",
            1,
            &cs,
            Some(
                chain
                    .founder_coord()
                    .cloned()
                    .expect("exact membership has a founder coordinate"),
            ),
        )
        .await;
    let stream_id = commit_stream_id(&reference);

    let db2_store_dir = crate::sync::test_helpers::test_store_dir();
    let db2 = crate::sync::test_helpers::open_test_db(db2_store_dir.clone());
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = storage.pull_into(&db2, &db2_store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(
        db2.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
    assert_eq!(updated.get(&stream_id), Some(&1));
}

/// A pulled blob's `id` is the primary key of a row authored by any write-capable
/// member (or anyone with the bucket credential). It is interpolated into the
/// blob's local file path, so an unconstrained `id` lets a member's row direct a
/// blob write to an attacker-chosen file outside the store directory — an
/// arbitrary file write that clobbers config/rc/binaries on every pulling device.
/// The Local-to-Remote transition must reject an `id` that could escape the
/// store directory, or that cannot form a partition prefix, before publication.
mod blob_path_traversal {
    use super::*;

    /// A blob whose `id` climbs out of the cache directory with `..` must NOT have
    /// its bytes written outside it. coven builds the destination from the id under
    /// its store cache; without the boundary check the id would resolve to a path
    /// above the cache and the downloaded bytes land there (an arbitrary-file-write
    /// RCE); the check refuses such a row as bad data, so nothing is written outside
    /// the cache and the apply is held.
    #[tokio::test]
    async fn traversal_id_does_not_write_outside_the_blob_dir() {
        let db1_store_dir = crate::sync::test_helpers::test_store_dir();
        let db1 =
            crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
        create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

        // The attacker's blob bytes, planted in the cloud under the malicious id's
        // flat mock key (the same key the puller's `get_blob` computes for it). No
        // local file is written on the source side, so nothing escapes here.
        // The source's changeset adds a note + a photo row whose id is the
        // traversal string. (The mock stored the blob above; this is the row that
        // references it.)
        db1.capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES ('x/../../../PWNED', 'n1', 'cover', 5, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                    coven_protocol::blob::content_hash(b"PWNED"),
                ),
            ],
        )
        .await;
        let store_dir = db1_store_dir.clone();
        let error =
            crate::sync::test_owner_graph::TestOwnerGraph::new(store_database(&db1), store_dir)
                .make_remote("notes", "n1", false)
                .await
                .expect_err("a traversal blob id cannot enter the upload journal");
        assert!(matches!(
            error,
            crate::blob::transition::MakeRemoteError::SourcePath { ref blob_id, .. }
                if blob_id == "x/../../../PWNED"
        ));
    }

    /// A short blob id does not panic while make_remote verifies its local source.
    /// Partitioned remote-path rejection belongs to the connected storage scheme;
    /// this local transition reports the absent source as the typed file failure.
    #[tokio::test]
    async fn short_id_missing_source_is_typed_not_panicked() {
        let db1_store_dir = crate::sync::test_helpers::test_store_dir();
        let db1 =
            crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
        create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

        db1.capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES ('a', 'n1', 'cover', 1, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                coven_protocol::blob::content_hash(b"A"),
            ),
        ])
        .await;
        let store_dir = db1_store_dir.clone();
        let error =
            crate::sync::test_owner_graph::TestOwnerGraph::new(store_database(&db1), store_dir)
                .make_remote("notes", "n1", false)
                .await
                .expect_err("a missing short-id source cannot enter the upload journal");
        assert!(matches!(
            error,
            crate::blob::transition::MakeRemoteError::SourceFile { ref blob_id, .. }
                if blob_id == "a"
        ));
    }

    /// A normal blob id still round-trips: the boundary check rejects only ids that
    /// could escape the cache or can't be partitioned, and a well-formed id writes
    /// its blob into the pinned cache at its partitioned `{ab}/{cd}/<id>` path.
    #[tokio::test]
    async fn normal_id_still_writes_under_the_blob_dir() {
        let db1_store_dir = crate::sync::test_helpers::test_store_dir();
        let db1 =
            crate::sync::test_helpers::open_test_db_with_blob(db1_store_dir.clone(), photo_decl());
        let storage = create_store(&db1, db1_store_dir.clone(), UserKeypair::generate()).await;

        db1.capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES ('p1ab', 'n1', 'attach', 10, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                    coven_protocol::blob::content_hash(b"PHOTOBYTES"),
                ),
            ],
        )
        .await;
        let source_store_dir = db1_store_dir.clone();
        source_store_dir.store_local("p1ab", b"PHOTOBYTES").await;
        storage
            .make_root_remote(&db1, &source_store_dir, "n1")
            .await;

        let db2_store_dir = crate::sync::test_helpers::test_store_dir();
        let db2 =
            crate::sync::test_helpers::open_test_db_with_blob(db2_store_dir.clone(), photo_decl());
        let ld = db2_store_dir;
        let (updated, result) = storage.pull_into(&db2, &ld).await;

        assert_eq!(result.changesets_applied, 1, "a well-formed row applies");
        assert!(!result.asset_downloads_failed);
        assert_eq!(updated.values().copied().collect::<Vec<_>>(), vec![1]);
        let written = std::fs::read(exact_cache_path(
            &ld,
            &db2.exact_row_blob_ref("note_photos", "p1ab").await,
        ))
        .expect("blob written");
        assert_eq!(
            written, b"PHOTOBYTES",
            "the blob lands in the evictable cache"
        );
    }
}

/// Dependency-ready pull applies a causal UPDATE only after its INSERT.
///
/// A device that applies an UPDATE of a row before that row's INSERT (authored by
/// another device) hits `SQLITE_CHANGESET_NOTFOUND`, which the apply reads as "the row
/// was deleted locally, delete wins" and OMITs — dropping the UPDATE and advancing.
/// The updater's commit captures the inserter's exact position. Reversed head discovery
/// therefore holds the UPDATE until the INSERT is durable, independent of listing order.
#[tokio::test]
async fn causal_update_waits_for_its_insert_despite_reversed_discovery() {
    let keypair = UserKeypair::generate();
    let observer_store_dir = crate::sync::test_helpers::test_store_dir();
    let observer = crate::sync::test_helpers::open_test_db(observer_store_dir.clone());
    let storage = TestStore::create(
        &observer,
        observer_store_dir.clone(),
        "test-lib",
        keypair.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Store for dependency-order test");
    storage.sort_provider_listings();

    let first_store_dir = crate::sync::test_helpers::test_store_dir();
    let first = crate::sync::test_helpers::open_test_db(first_store_dir.clone());
    let second_store_dir = crate::sync::test_helpers::test_store_dir();
    let second = crate::sync::test_helpers::open_test_db(second_store_dir.clone());
    let receiver_store_dir = crate::sync::test_helpers::test_store_dir();
    let receiver = crate::sync::test_helpers::open_test_db(receiver_store_dir.clone());
    for (participant, participant_store_dir) in [
        (&first, first_store_dir.clone()),
        (&second, second_store_dir.clone()),
        (&receiver, receiver_store_dir.clone()),
    ] {
        storage
            .activate_joined_device(
                &observer,
                observer_store_dir.clone(),
                participant,
                participant_store_dir.clone(),
                &keypair,
                "2026-01-01T00:00:00Z",
            )
            .await
            .expect("install active test device");
    }
    for (participant, participant_store_dir) in [
        (&first, &first_store_dir),
        (&second, &second_store_dir),
        (&receiver, &receiver_store_dir),
    ] {
        storage.pull_into(participant, participant_store_dir).await;
    }
    let first_stream = first.local_announcement_stream().await;
    let second_stream = second.local_announcement_stream().await;
    let (db_ins, db_ins_store_dir, db_upd, db_upd_store_dir, insert_stream, update_stream) =
        if first_stream > second_stream {
            (
                &first,
                &first_store_dir,
                &second,
                &second_store_dir,
                first_stream,
                second_stream,
            )
        } else {
            (
                &second,
                &second_store_dir,
                &first,
                &first_store_dir,
                second_stream,
                first_stream,
            )
        };
    assert!(update_stream < insert_stream);
    let inserter_device = storage
        .bind_device_in(db_ins, db_ins_store_dir.clone(), &keypair)
        .await
        .expect("retain inserter Store device");

    let insert = db_ins
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('n1', 'orig', NULL, 1, '0000000001000-0000-ins', '2026-01-01')",
        ])
        .await;
    let inserted = storage
        .sync_for_test(db_ins, insert, 0, "", &keypair, db_ins_store_dir)
        .await
        .expect("publish inserter changeset");
    assert!(
        inserted.is_some(),
        "the captured rows publish a Store commit"
    );
    let insert_position = inserter_device
        .latest_local_store_position()
        .await
        .expect("read inserter position")
        .expect("inserter published one Store commit");

    storage.pull_into(db_upd, db_upd_store_dir).await;
    assert_eq!(
        store_database(db_upd)
            .materialized_frontier()
            .await
            .expect("read updater materialized frontier")
            .get(&insert_stream.to_string()),
        Some(&insert_position),
        "the updater durably materializes the exact insert before capturing its update",
    );
    let update = db_upd
        .capture_test_changeset(&[
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-upd' \
           WHERE id = 'n1'",
        ])
        .await;
    let updated = storage
        .sync_for_test(db_upd, update, 0, "", &keypair, db_upd_store_dir)
        .await
        .expect("publish updater changeset");
    assert!(
        updated.is_some(),
        "the captured rows publish a Store commit"
    );
    let updater = storage
        .bind_device_in(db_upd, db_upd_store_dir.clone(), &keypair)
        .await
        .expect("load updater Store");
    let (_, update_commit) = updater
        .load_exact_materialized_commit(&update_stream.to_string(), 1)
        .await
        .expect("load updater Store commit")
        .expect("updater Store commit is materialized");
    assert_eq!(
        update_commit.order.dependencies().get(&insert_stream),
        Some(&insert_position),
        "the update commit captures the exact insert dependency",
    );

    storage.pull_into(&receiver, &receiver_store_dir).await;

    assert_eq!(
        receiver
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "updated",
        "the dependent UPDATE must wait for its exact INSERT dependency",
    );
}
