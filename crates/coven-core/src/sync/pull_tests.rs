//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `TestStore`; a second device
//! pulls and applies them through a real [`crate::database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use async_trait::async_trait;
use rusqlite::OptionalExtension;

use crate::blob::{local_files, CacheFill, Provenance};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::migration::Migration;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::membership::{MemberRole, MembershipChain, MembershipCoord};
use crate::sync::store::{
    HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason, StorePullError,
};
use crate::sync::store::{PullError, OWNER_PUBKEY_STATE_KEY};
use crate::sync::store_commit::StoreDeviceHead;
/// The synthetic test db opens with a single migration, so its
/// [`crate::database::Database::schema_version`] is 1. Changesets are stored at
/// that version; a newer peer's changeset or floor uses `SCHEMA_VERSION + 1`.
const SCHEMA_VERSION: u32 = 1;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::{ProtocolObjectDomain, SyncStorage};
use crate::sync::test_helpers::*;

fn store_database(db: &crate::database::Database) -> crate::sync::store::StoreDatabase {
    crate::sync::store::StoreDatabase::new(db)
}

fn exact_cache_path(
    store_dir: &crate::store_dir::StoreDir,
    reference: &crate::blob::RowBlobRef,
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
    store_dir: &crate::store_dir::StoreDir,
    reference: &crate::blob::RowBlobRef,
) -> std::path::PathBuf {
    let stored = reference.stored().expect("Remote row has exact storage");
    store_dir
        .pinned_blob_path(
            stored.locator().namespace(),
            stored.locator().locator_hash(),
        )
        .expect("build exact locator pinned path")
}

async fn row_blob_ref(
    db: &crate::database::Database,
    table: &str,
    row_id: &str,
) -> crate::blob::RowBlobRef {
    db.row_blob_ref(table, row_id)
        .await
        .expect("load exact row blob reference")
}

async fn stored_remote_object(
    db: &crate::database::Database,
    object: &crate::sync::storage::ExactObjectRef,
) -> crate::sync::remote_object::RemoteObjectRecord {
    let object_id = crate::sync::remote_object::remote_object_id(object);
    db.call(move |conn| {
        let state: String = conn
            .query_row(
                "SELECT state FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(crate::database::DbError::from)?;
        serde_json::from_str(&state)
            .map_err(|error| crate::database::DbError::Message(error.to_string()))
    })
    .await
    .expect("load exact remote object")
}

async fn stored_remote_objects(
    db: &crate::database::Database,
) -> Vec<crate::sync::remote_object::RemoteObjectRecord> {
    db.call(|conn| {
        let mut statement = conn
            .prepare("SELECT state FROM remote_objects ORDER BY object_id")
            .map_err(crate::database::DbError::from)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(crate::database::DbError::from)?;
        let mut objects = Vec::new();
        for row in rows {
            let state = row.map_err(crate::database::DbError::from)?;
            objects.push(serde_json::from_str(&state).map_err(|error| {
                crate::database::DbError::Message(format!("parse remote object: {error}"))
            })?);
        }
        Ok(objects)
    })
    .await
    .expect("load remote objects")
}

async fn replace_retained_merge_input(
    db: &crate::database::Database,
    stream_id: String,
    canonical_input: Vec<u8>,
) {
    db.call(move |conn| {
        let tx = conn
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        tx.pragma_update(None, "defer_foreign_keys", true)
            .map_err(crate::database::DbError::from)?;
        let stored_hash: String = tx
            .query_row(
                "SELECT input_hash FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = 1",
                [&stream_id],
                |row| row.get(0),
            )
            .map_err(crate::database::DbError::from)?;
        let old_hash = stored_hash.parse().map_err(|error| {
            crate::database::DbError::Message(format!("stored retained input hash: {error}"))
        })?;
        let new_hash = crate::sync::store_commit::ObjectHash::digest(&canonical_input);
        let mut statement = tx
            .prepare(
                "SELECT indexed.object_id, remote.state
                 FROM retained_replay_objects AS indexed
                 JOIN remote_objects AS remote ON remote.object_id = indexed.object_id
                 WHERE indexed.device_id = ?1 AND indexed.seq = 1
                 ORDER BY indexed.object_id",
            )
            .map_err(crate::database::DbError::from)?;
        let rows = statement
            .query_map([&stream_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(crate::database::DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(crate::database::DbError::from)?;
        drop(statement);
        if rows.is_empty() {
            return Err(crate::database::DbError::Message(
                "retained Merge input has no indexed replay objects".to_string(),
            ));
        }
        for (object_id, state) in rows {
            let mut remote: crate::sync::remote_object::RemoteObjectRecord =
                serde_json::from_str(&state).map_err(|error| {
                    crate::database::DbError::Message(format!(
                        "parse retained replay object {object_id}: {error}"
                    ))
                })?;
            let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut remote
            else {
                return Err(crate::database::DbError::Message(format!(
                    "retained replay object {object_id} is not shared"
                )));
            };
            let crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership } =
                &mut record.state
            else {
                return Err(crate::database::DbError::Message(format!(
                    "retained replay object {object_id} is not activated"
                )));
            };
            let old_owner = ownership
                .activated
                .iter()
                .find_map(|owner| match owner {
                    crate::sync::remote_object::SharedObjectOwner::RetainedReplay(
                        crate::sync::remote_object::RetainedReplayOwner::Commit {
                            commit,
                            input_hash,
                        },
                    ) if *input_hash == old_hash => Some((owner.clone(), commit.clone())),
                    _ => None,
                })
                .ok_or_else(|| {
                    crate::database::DbError::Message(format!(
                        "retained replay object {object_id} lacks its indexed owner"
                    ))
                })?;
            ownership.activated.remove(&old_owner.0);
            ownership.activated.insert(
                crate::sync::remote_object::SharedObjectOwner::RetainedReplay(
                    crate::sync::remote_object::RetainedReplayOwner::Commit {
                        commit: old_owner.1,
                        input_hash: new_hash,
                    },
                ),
            );
            tx.execute(
                "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                rusqlite::params![
                    object_id,
                    serde_json::to_string(&remote).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "serialize rebound retained replay object: {error}"
                        ))
                    })?
                ],
            )
            .map_err(crate::database::DbError::from)?;
        }
        tx.execute(
            "UPDATE retained_merge_materializations
             SET input_hash = ?2, canonical_input = ?3
             WHERE device_id = ?1 AND seq = 1",
            rusqlite::params![&stream_id, new_hash.to_string(), &canonical_input],
        )
        .map_err(crate::database::DbError::from)?;
        tx.execute(
            "UPDATE materialized_commits SET retained_input_hash = ?2
             WHERE device_id = ?1 AND seq = 1",
            rusqlite::params![&stream_id, new_hash.to_string()],
        )
        .map_err(crate::database::DbError::from)?;
        tx.execute(
            "UPDATE retained_replay_objects SET input_hash = ?2
             WHERE device_id = ?1 AND seq = 1",
            rusqlite::params![&stream_id, new_hash.to_string()],
        )
        .map_err(crate::database::DbError::from)?;
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await
    .expect("replace retained Merge input and its exact ownership closure");
}

fn is_external_circle_package(
    remote: &crate::sync::remote_object::RemoteObjectRecord,
    retained_for_replay: bool,
) -> bool {
    let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record) = remote else {
        return false;
    };
    if !matches!(
        record.identity.domain,
        crate::sync::remote_object::SharedLiveSetObjectDomain::CirclePackage { .. }
    ) || !matches!(
        record.bytes.stored(),
        crate::sync::remote_object::RemoteStoredRepresentation::ExternalExact { .. }
    ) {
        return false;
    }
    let crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &record.state
    else {
        return false;
    };
    let has_commit = ownership.activated.iter().any(|owner| {
        matches!(
            owner,
            crate::sync::remote_object::SharedObjectOwner::StoreCommit(_)
        )
    });
    let has_replay = ownership.activated.iter().any(|owner| {
        matches!(
            owner,
            crate::sync::remote_object::SharedObjectOwner::RetainedReplay(_)
        )
    });
    has_commit && has_replay == retained_for_replay
}

async fn retained_store_package_pin(
    db: &crate::database::Database,
    commit: &crate::sync::store_commit::StoreBatchCommitRef,
) -> (
    crate::sync::remote_object::RetainedReplayOwner,
    crate::sync::store_commit::StorePackageRef,
    crate::sync::remote_object::RemoteObjectRecord,
) {
    let stream_id = commit_stream_id(commit);
    let sequence = commit.coord.sequence() as i64;
    let (input_hash, canonical_input): (String, Vec<u8>) = db
        .call(move |conn| {
            conn.query_row(
                "SELECT input_hash, canonical_input FROM retained_merge_materializations \
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("load retained package input");
    let retained: serde_json::Value =
        serde_json::from_slice(&canonical_input).expect("parse retained package input");
    let reference: crate::sync::store_commit::StorePackageRef =
        serde_json::from_value(retained["packages"][0]["store"]["reference"].clone())
            .expect("parse retained Store package reference");
    let owner = crate::sync::remote_object::RetainedReplayOwner::Commit {
        commit: commit.clone(),
        input_hash: input_hash
            .parse()
            .expect("parse retained package input hash"),
    };
    let remote = stored_remote_object(db, &reference.object).await;
    (owner, reference, remote)
}

async fn replace_stored_remote_object(
    db: &crate::database::Database,
    object: &crate::sync::storage::ExactObjectRef,
    remote: &crate::sync::remote_object::RemoteObjectRecord,
) {
    let object_id = crate::sync::remote_object::remote_object_id(object);
    let state = serde_json::to_string(remote).expect("serialize test remote object");
    db.call(move |conn| {
        let updated = conn
            .execute(
                "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                rusqlite::params![object_id.to_string(), state],
            )
            .map_err(crate::database::DbError::from)?;
        if updated != 1 {
            return Err(crate::database::DbError::Message(
                "test remote object disappeared".to_string(),
            ));
        }
        Ok(())
    })
    .await
    .expect("replace test remote object");
}

fn commit_stream_id(reference: &crate::sync::store_commit::StoreBatchCommitRef) -> String {
    reference.coord.stream_id.to_string()
}

async fn local_announcement_stream(
    db: &crate::database::Database,
) -> crate::sync::membership::AuthorStreamId {
    let (registration_ref, registration) = store_database(db)
        .local_blob_write_authority()
        .await
        .expect("read active local Store registration");
    registration
        .store_announcement_activation(&registration_ref)
        .expect("derive local Store announcement activation")
        .author_stream_id()
}

#[async_trait]
trait TestStoreStorage {
    fn sync_storage(&self) -> &dyn SyncStorage;

    async fn bind_for_test_publish(
        &self,
        db: &crate::database::Database,
        keypair: &UserKeypair,
    ) -> Result<(), String>;
}

#[async_trait]
impl TestStoreStorage for TestStore {
    fn sync_storage(&self) -> &dyn SyncStorage {
        &self.storage
    }

    async fn bind_for_test_publish(
        &self,
        db: &crate::database::Database,
        _keypair: &UserKeypair,
    ) -> Result<(), String> {
        self.open_into(db)
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

#[async_trait]
impl TestStoreStorage for CloudSyncStorage {
    fn sync_storage(&self) -> &dyn SyncStorage {
        self
    }

    async fn bind_for_test_publish(
        &self,
        db: &crate::database::Database,
        keypair: &UserKeypair,
    ) -> Result<(), String> {
        if crate::sync::store::StoreDatabase::new(db)
            .local_store_root_ref()
            .await
            .map_err(|error| error.to_string())?
            .is_none()
        {
            crate::sync::store::protocol_root::create_store(
                &crate::sync::store::StoreDatabase::new(db),
                self,
                self.store_id(),
                keypair,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn cloud_test_storage(
    home: std::sync::Arc<dyn CloudHome>,
    cipher: CloudCipher,
    blob_paths: BlobPathScheme,
    store_id: &str,
    keypair: UserKeypair,
) -> CloudSyncStorage {
    CloudSyncStorage::new(home, cipher, blob_paths, store_id, keypair)
        .expect("test cloud storage supports immutable copies")
}

/// Publish exact package bytes through the durable Store write ledger.
async fn sync_for_test<S: TestStoreStorage>(
    db: &crate::database::Database,
    tables: &[SyncedTable],
    outgoing: Vec<u8>,
    local_seq: u64,
    storage: &S,
    timestamp: &str,
    message: &str,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) -> Result<Option<crate::sync::store_commit::StoreBatchCommitRef>, String> {
    let configured_tables: Vec<_> = db.synced_tables().iter().map(SyncedTable::name).collect();
    let supplied_tables: Vec<_> = tables.iter().map(SyncedTable::name).collect();
    assert_eq!(configured_tables, supplied_tables);
    assert!(
        message.is_empty(),
        "Store commits carry no arbitrary message"
    );
    storage.bind_for_test_publish(db, keypair).await?;
    let before = crate::sync::store::StoreDatabase::new(db)
        .latest_local_store_position()
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(
        before
            .as_ref()
            .map_or(0, |position| position.coord.sequence()),
        local_seq
    );
    crate::sync::store::StoreDatabase::new(db)
        .enqueue_store_changeset_for_test(outgoing)
        .await
        .map_err(|error| error.to_string())?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "exact test Store has no activated local device".to_string())?;
    let authorization = crate::sync::store::Store::authorize_borrowed(storage.sync_storage(), db)
        .await
        .map_err(|error| error.to_string())?;
    let prepared = authorization
        .prepare_pending_store_write(&device_id, timestamp, keypair, store_dir)
        .await
        .map_err(|error| error.to_string())?;
    if !prepared {
        return Ok(None);
    }
    authorization
        .drain_store_writes()
        .await
        .map_err(|error| error.to_string())?;
    crate::sync::store::StoreDatabase::new(db)
        .latest_local_store_position()
        .await
        .map_err(|error| error.to_string())
}

async fn pull_exact_store_into(
    destination: &crate::database::Database,
    source: &crate::database::Database,
    storage: &CloudSyncStorage,
    store_dir: &crate::store_dir::StoreDir,
) -> (
    std::collections::BTreeMap<String, u64>,
    crate::sync::store::StorePullResult,
) {
    let root = crate::sync::store::StoreDatabase::new(source)
        .local_store_root_ref()
        .await
        .expect("read source Store root")
        .expect("source Store has exact root authority");
    let destination_store = crate::sync::store::StoreDatabase::new(destination);
    let protocol_root =
        crate::sync::store::protocol_root::open_store(&destination_store, storage, &root)
            .await
            .expect("open exact Store on destination");
    crate::sync::store::anchor_owner_membership(
        storage,
        &destination_store,
        &root,
        &protocol_root,
        storage.user_keypair(),
    )
    .await
    .expect("anchor exact Store membership");
    let membership = crate::sync::store::load_cycle_membership(storage, &destination_store)
        .await
        .expect("load exact Store membership");
    let result = crate::sync::store::pull_store_commits(
        &store_database(destination),
        destination.synced_tables(),
        storage,
        root.store_root_hash,
        store_dir,
        membership
            .chain
            .as_ref()
            .expect("opened Store has membership"),
        None,
    )
    .await
    .expect("pull exact Store commits");
    let positions = result
        .frontier
        .iter()
        .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
        .collect();
    (positions, result)
}

/// The common `note_photos` blob declaration: namespace `"photos"`, master scope,
/// host-provided · `CacheEager` (a cover — fetched into the cache on pull), hashed
/// scheme.
fn photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
}

fn photo_decl_with_blob_id_column() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("cloud_path")
}

fn unique_note_db() -> crate::database::Database {
    open_test_db_schema(
        vec![SyncedTable::new(
            "unique_notes",
            crate::sync::session::RowIdentity::SharedKey,
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
            .map_err(crate::database::DbError::from)
        })],
    )
}

fn mixed_constraint_db() -> crate::database::Database {
    open_test_db_schema(
        vec![
            SyncedTable::new(
                "constraint_parents",
                crate::sync::session::RowIdentity::SharedKey,
            ),
            SyncedTable::new(
                "constraint_items",
                crate::sync::session::RowIdentity::SharedKey,
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
            .map_err(crate::database::DbError::from)
        })],
    )
}

fn open_blob_test_db_at(path: &std::path::Path, decl: BlobDecl) -> crate::database::Database {
    crate::database::Database::open(
        path,
        test_synced_tables_with_blob(decl),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "restart-test-device".to_string(),
        &test_migrations(),
    )
    .expect("open file-backed blob test database")
    .0
}

async fn create_store(db: &crate::database::Database, signer: UserKeypair) -> TestStore {
    TestStore::create(db, "test-store", signer)
        .await
        .expect("create exact test Store for the test database")
}

/// Store `bytes` into `ld`'s local store under blob id `id`, the way a host stores a
/// host-provided cover (its Local home) before the inline push reads it to upload.
async fn store_local(ld: &crate::store_dir::StoreDir, id: &str, bytes: &[u8]) {
    local_files::store(ld, "photos", id, bytes)
        .await
        .expect("store host-provided blob in the local store");
}

async fn publish_blob_changeset(
    db: &crate::database::Database,
    storage: &TestStore,
    store_dir: &crate::store_dir::StoreDir,
    changeset: Vec<u8>,
    local_sequence: u64,
) -> crate::sync::store_commit::StoreBatchCommitRef {
    sync_for_test(
        db,
        db.synced_tables(),
        changeset,
        local_sequence,
        storage,
        "2026-01-01T00:00:00Z",
        "",
        &storage.protocol_founder_keypair(),
        store_dir,
    )
    .await
    .expect("publish exact blob-bearing Store changeset")
    .expect("blob-bearing changeset produces a Store commit")
}

async fn make_test_root_remote(
    db: &crate::database::Database,
    storage: &TestStore,
    store_dir: &crate::store_dir::StoreDir,
    root_id: &str,
) {
    storage.open_into(db).await.expect("open exact test Store");
    crate::sync::store::ensure_active_registration(&store_database(db), &storage.storage)
        .await
        .expect("activate exact fixture writer");
    let hlc = crate::sync::hlc::Hlc::new("blob-fixture".to_string());
    crate::blob::transition::make_remote(
        &store_database(db),
        store_dir,
        &hlc,
        "notes",
        root_id,
        false,
    )
    .await
    .expect("queue exact blob fixture upload");
    let (registration_ref, registration) = store_database(db)
        .local_blob_write_authority()
        .await
        .expect("load exact blob fixture write authority");
    let authority = crate::sync::storage::BlobWriteAuthority::new(&registration_ref, &registration)
        .expect("validate exact blob fixture write authority");
    let outcome = crate::blob::upload::drain_uploads(
        &store_database(db),
        &storage.storage,
        authority,
        store_dir,
        &crate::clock::SystemClock,
        &hlc,
        None,
        None,
    )
    .await
    .expect("upload exact blob fixture");
    assert!(outcome.uploaded > 0);
    assert!(outcome.yielded_for_publish);
    assert!(storage
        .publish_pending(db, store_dir)
        .await
        .expect("publish exact blob fixture"));
}

async fn materialized_sequences(db: &crate::database::Database) -> HashMap<String, u64> {
    store_database(db)
        .materialized_frontier()
        .await
        .expect("read materialized Store frontier")
        .into_iter()
        .map(|(device_id, position)| (device_id, position.coord.sequence()))
        .collect()
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

fn membership_coord(chain: &MembershipChain, author_pubkey: &str, seq: u64) -> MembershipCoord {
    let entry = chain
        .entries()
        .iter()
        .filter(|entry| entry.author_pubkey == author_pubkey)
        .nth(usize::try_from(seq - 1).expect("membership sequence fits usize"))
        .unwrap_or_else(|| panic!("missing membership coordinate {author_pubkey}/{seq}"));
    entry.coord()
}

fn membership_author_stream(
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> crate::sync::membership::AuthorStreamId {
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

async fn exact_membership_chain(storage: &TestStore) -> MembershipChain {
    let db = open_test_db();
    storage
        .open_into(&db)
        .await
        .expect("load exact test Store membership")
}

async fn exact_membership_registration(
    storage: &TestStore,
    chain: &MembershipChain,
    entry: &crate::sync::membership::MembershipEntry,
    signer: &UserKeypair,
) -> (
    crate::sync::store_commit::StoreDeviceRegistrationRef,
    crate::sync::store_commit::StoreDeviceRegistration,
    UserKeypair,
) {
    if let Some(predecessor) = chain.head_ref_for_stream(
        &entry.author_pubkey,
        &entry.author_owner_grant,
        entry.stream_id,
    ) {
        let head = crate::sync::store::load_exact_membership_head(
            &storage.storage,
            &storage.root,
            predecessor,
        )
        .await
        .expect("load exact predecessor membership head");
        let registration = crate::sync::store_objects::load_registration_ref(
            &storage.storage,
            &storage.root,
            &head.body.author_registration,
        )
        .await
        .expect("load exact membership author registration")
        .value;
        let device_signer = registration
            .device_signer(signer)
            .expect("membership signer owns exact device registration");
        return (head.body.author_registration, registration, device_signer);
    }

    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
    use crate::sync::store_commit::{
        DeviceRecoveryId, DeviceStreamAnchor, StoreDeviceRegistration,
        StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
    };

    let recovery_id = DeviceRecoveryId::from_hash(crate::sync::store_commit::ObjectHash::digest(
        entry.author_pubkey.as_bytes(),
    ));
    let recovery_prefix = crate::sync::store_commit::owner_recovery_semantic_prefix(
        &entry.author_pubkey,
        entry.author_owner_grant.clone(),
        1,
    );
    let recovery_slot = storage
        .allocate_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                storage.root.store_root_hash,
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
    let device_id = crate::sync::store_commit::StoreDeviceId::derive(&storage.root, &origin);
    let announcement_slot = storage
        .allocate_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                storage.root.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            ),
            &crate::sync::store_commit::head_slot_prefix(&device_id.to_string(), 1),
            ".json",
        )
        .await
        .expect("allocate exact announcement slot");
    let acknowledgement_slot = storage
        .allocate_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                storage.root.store_root_hash,
                ProtocolObjectDomain::StoreAck,
            ),
            &crate::sync::store_commit::ack_slot_prefix(&device_id.to_string(), 1),
            ".json",
        )
        .await
        .expect("allocate exact acknowledgement slot");
    let snapshot_slot = storage
        .allocate_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                storage.root.store_root_hash,
                ProtocolObjectDomain::StoreSnapshotMeta,
            ),
            &crate::sync::store_commit::snapshot_slot_prefix(&device_id.to_string(), 0),
            ".json",
        )
        .await
        .expect("allocate exact snapshot slot");
    let (_, founder_registration, _) = storage
        .founder_device_authority()
        .await
        .expect("load exact founder device registration");
    let registration = StoreDeviceRegistration::signed(
        storage.root.clone(),
        origin,
        founder_registration.provider,
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
    let semantic_prefix = crate::sync::store_commit::registration_semantic_prefix(
        &registration.device_id.to_string(),
    );
    let context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &semantic_prefix, ".json")
        .await
        .expect("allocate exact membership registration object");
    let prepared = storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
        .expect("prepare exact membership registration object");
    let object = crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("publish exact membership registration object");
    let reference = StoreDeviceRegistrationRef::from_registration(&registration, object);
    let device_signer = registration
        .device_signer(signer)
        .expect("derive exact membership device signer");
    (reference, registration, device_signer)
}

async fn publish_exact_membership_entry(
    storage: &TestStore,
    chain: &mut MembershipChain,
    entry: crate::sync::membership::MembershipEntry,
    signer: &UserKeypair,
) {
    use crate::sync::membership::{AuthorHead, MembershipHeadRef};
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
    use crate::sync::store_commit::{membership_head_slot_prefix, StreamActivation, SuccessorLink};

    let coord = entry.coord();
    let (registration_ref, registration, device_signer) =
        exact_membership_registration(storage, chain, &entry, signer).await;
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
        Some(reference) => {
            crate::sync::store::load_exact_membership_head(
                &storage.storage,
                &storage.root,
                reference,
            )
            .await
            .expect("load exact membership predecessor")
            .body
            .successor
            .next_slot
        }
        None => match &anchor {
            crate::sync::store_commit::GrantStreamAnchor::StoreMembership { first_slot } => {
                first_slot.clone()
            }
            crate::sync::store_commit::GrantStreamAnchor::OwnerRecovery { .. } => {
                panic!("test membership author has a recovery stream anchor")
            }
            crate::sync::store_commit::GrantStreamAnchor::CircleControl { .. }
            | crate::sync::store_commit::GrantStreamAnchor::CircleRoster { .. }
            | crate::sync::store_commit::GrantStreamAnchor::CircleMetadata { .. } => {
                panic!("test membership author has a Circle stream anchor")
            }
        },
    };
    let (entry_object, entry_ref) = crate::sync::store_objects::prepare_membership_entry(
        &storage.storage,
        storage.root.store_root_hash,
        &entry,
    )
    .await
    .expect("prepare exact membership entry");
    crate::sync::store_objects::create_exact_object(&storage.storage, &entry_object)
        .await
        .expect("publish exact membership entry");

    let context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
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
    let next_slot = storage
        .allocate_protocol_slot(&context, &next_prefix, ".json")
        .await
        .expect("allocate exact membership successor slot");
    let head = AuthorHead::signed(
        entry.store_id.clone(),
        super::membership::MembershipHeadBody {
            author_registration: registration_ref.clone(),
            entry: entry_ref,
            predecessor: predecessor.clone(),
            resolutions: entry.resolution_dependencies.clone(),
            successor: SuccessorLink {
                activation: StreamActivation::grant_authorized(
                    storage.root.store_root_hash,
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
        super::membership::MembershipHeadActivation::Direct,
        &device_signer,
    );
    assert!(head.verify(&registration));
    let prefix = membership_head_slot_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
    );
    let prepared = storage
        .prepare_protocol_object(
            &context,
            current_slot,
            &prefix,
            serde_json::to_vec(&head).expect("serialize exact membership head"),
        )
        .expect("prepare exact membership head");
    let object = crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("publish exact membership head");
    chain
        .add_entry(entry)
        .expect("extend exact membership test chain");
    chain
        .activate_head_ref(MembershipHeadRef {
            coord,
            head_hash: head.head_hash(),
            object,
        })
        .expect("activate exact membership test head");
}

struct ExactPublishedCommit {
    reference: crate::sync::store_commit::StoreBatchCommitRef,
    commit: crate::sync::store_commit::StoreBatchCommit,
    registration: crate::sync::store_commit::StoreDeviceRegistration,
    device_signer: UserKeypair,
    head: StoreDeviceHead,
    head_object: crate::sync::storage::ExactObjectRef,
}

async fn load_exact_published_commit(
    storage: &TestStore,
    reference: crate::sync::store_commit::StoreBatchCommitRef,
) -> ExactPublishedCommit {
    load_exact_published_commit_as(storage, reference, &storage.signer).await
}

async fn load_exact_published_commit_as(
    storage: &TestStore,
    reference: crate::sync::store_commit::StoreBatchCommitRef,
    identity: &UserKeypair,
) -> ExactPublishedCommit {
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
    use crate::sync::store_commit::{head_slot_prefix, StoreDeviceHead, StoreDeviceRegistration};

    let context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let bytes = storage
        .read_protocol_object(
            &context,
            &reference.object,
            &crate::sync::store_commit::semantic_prefix_from_exact_object(
                &reference.object,
                ".json",
            )
            .expect("derive exact Store commit semantic prefix"),
        )
        .await
        .expect("read exact published Store commit");
    let unverified: crate::sync::store_commit::StoreBatchCommit =
        serde_json::from_slice(&bytes).expect("parse exact published Store commit");
    let registration = crate::sync::store_objects::load_registration_ref(
        &storage.storage,
        &storage.root,
        &unverified.author_registration,
    )
    .await
    .expect("load exact published Store registration")
    .value;
    let commit = crate::sync::store_objects::load_commit_ref(
        &storage.storage,
        storage.root.store_root_hash,
        &reference,
        &registration,
    )
    .await
    .expect("verify exact published Store commit")
    .value;
    let device_signer = registration
        .device_signer(identity)
        .expect("derive exact published Store device signer");
    let crate::sync::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot } =
        &registration.store_commits
    else {
        panic!("pull test registration has a Store announcement anchor")
    };
    let head_context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let mut slot = first_slot.clone();
    let mut sequence = 1_u64;
    let (head, head_object) = loop {
        let prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = storage
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
    ExactPublishedCommit {
        reference,
        commit,
        registration,
        device_signer,
        head,
        head_object,
    }
}

async fn replace_exact_commit_bytes(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    commit_bytes: Vec<u8>,
    commit_hash: crate::sync::store_commit::ObjectHash,
    head_registration: crate::sync::store_commit::StoreDeviceRegistrationRef,
    head_signer: &UserKeypair,
) -> crate::sync::store_commit::StoreBatchCommitRef {
    let replacement_commit: crate::sync::store_commit::StoreBatchCommit =
        serde_json::from_slice(&commit_bytes).expect("parse replacement exact Store commit");
    let candidate_summary = storage
        .retained_merge_history_summary(&graph.registration.device_id, graph.reference.clone())
        .await
        .expect("load replacement candidate history summary");
    let reference =
        publish_replacement_exact_commit(storage, graph, commit_bytes, commit_hash).await;
    let history_summary = crate::sync::store::prepare_merge_abandonment_history_summary(
        &candidate_summary,
        &graph.reference,
        &graph.commit,
        &reference,
        &replacement_commit,
    )
    .expect("prepare replacement exact Store history summary");
    replace_exact_commit_head(
        storage,
        graph,
        reference.clone(),
        history_summary.digest(),
        head_registration,
        head_signer,
    )
    .await;
    reference
}

async fn publish_replacement_exact_commit(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    commit_bytes: Vec<u8>,
    commit_hash: crate::sync::store_commit::ObjectHash,
) -> crate::sync::store_commit::StoreBatchCommitRef {
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};

    let stream_id = commit_stream_id(&graph.reference);
    let commit_context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let semantic_prefix = crate::sync::store_commit::commit_semantic_prefix(
        graph.commit.candidate_family(),
        &stream_id,
        graph.reference.coord.sequence(),
        commit_hash,
    );
    let slot = storage
        .allocate_protocol_slot(&commit_context, &semantic_prefix, ".json")
        .await
        .expect("allocate replacement exact Store commit slot");
    let commit_prepared = storage
        .prepare_protocol_object(&commit_context, slot, &semantic_prefix, commit_bytes)
        .expect("prepare replacement exact Store commit");
    let commit_object =
        crate::sync::store_objects::create_exact_object(&storage.storage, &commit_prepared)
            .await
            .expect("publish replacement exact Store commit");
    crate::sync::store_commit::StoreBatchCommitRef {
        coord: graph.reference.coord.clone(),
        commit_hash,
        object: commit_object,
    }
}

async fn replace_exact_commit_head(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    reference: crate::sync::store_commit::StoreBatchCommitRef,
    history_summary: crate::sync::store_commit::ObjectHash,
    head_registration: crate::sync::store_commit::StoreDeviceRegistrationRef,
    head_signer: &UserKeypair,
) {
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};

    let head_context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    storage
        .delete_protocol_object(&graph.head_object)
        .await
        .expect("delete replaced exact Store head");
    let head = StoreDeviceHead::signed(
        storage.root.store_root_hash,
        head_registration,
        reference.clone(),
        history_summary,
        graph.head.successor.clone(),
        head_signer,
    )
    .expect("sign replacement exact Store head");
    let prefix = crate::sync::store_commit::head_slot_prefix(
        &graph.registration.device_id.to_string(),
        reference.coord.sequence(),
    );
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            graph.head_object.slot().clone(),
            &prefix,
            head.to_bytes(),
        )
        .expect("prepare replacement exact Store head");
    crate::sync::store_objects::create_exact_object(&storage.storage, &head_prepared)
        .await
        .expect("publish replacement exact Store head");
}

async fn replace_exact_commit_bytes_before_commit_validation(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    commit_bytes: Vec<u8>,
    commit_hash: crate::sync::store_commit::ObjectHash,
    head_registration: crate::sync::store_commit::StoreDeviceRegistrationRef,
    head_signer: &UserKeypair,
) -> crate::sync::store_commit::StoreBatchCommitRef {
    let reference =
        publish_replacement_exact_commit(storage, graph, commit_bytes, commit_hash).await;
    replace_exact_commit_head(
        storage,
        graph,
        reference.clone(),
        graph.head.history_summary,
        head_registration,
        head_signer,
    )
    .await;
    reference
}

async fn replace_exact_head(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    commit: crate::sync::store_commit::StoreBatchCommitRef,
    author_registration: crate::sync::store_commit::StoreDeviceRegistrationRef,
    signer: &UserKeypair,
) {
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};

    let context = ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    storage
        .delete_protocol_object(&graph.head_object)
        .await
        .expect("delete replaced exact Store head");
    let head = StoreDeviceHead::signed(
        storage.root.store_root_hash,
        author_registration,
        commit,
        graph.head.history_summary,
        graph.head.successor.clone(),
        signer,
    )
    .expect("sign replacement exact Store head");
    let prefix = crate::sync::store_commit::head_slot_prefix(
        &graph.registration.device_id.to_string(),
        graph.reference.coord.sequence(),
    );
    let prepared = storage
        .prepare_protocol_object(
            &context,
            graph.head_object.slot().clone(),
            &prefix,
            head.to_bytes(),
        )
        .expect("prepare replacement exact Store head");
    crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("publish replacement exact Store head");
}

async fn resign_exact_commit(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    schema_version: u32,
    membership_authority: Option<crate::sync::membership::MembershipGrantCreationAuthority>,
) -> crate::sync::store_commit::StoreBatchCommit {
    let package = graph
        .commit
        .store_package()
        .expect("test Store commit carries a Store package");
    let package_bytes = crate::sync::store_objects::load_store_package(
        &storage.storage,
        &graph.reference,
        &graph.commit,
    )
    .await
    .expect("load exact Store package")
    .expect("exact Store package exists")
    .value;
    let predecessor = match &membership_authority {
        Some(authority) => authority.clone(),
        None => graph
            .commit
            .membership_authority
            .clone()
            .expect("published Merge operations commit carries membership authority"),
    };
    let mut commit = sign_exact_commit_with_package(
        graph,
        schema_version,
        crate::sync::store_commit::StoreOperationMembershipAuthority { predecessor },
        &package_bytes,
        package.object.clone(),
    );
    if membership_authority.is_none() {
        commit.membership_authority = None;
        commit.signature =
            crate::keys::sign_hex(&graph.device_signer, &commit.canonical_signed_bytes()).1;
    }
    commit
}

fn sign_exact_commit_with_package(
    graph: &ExactPublishedCommit,
    schema_version: u32,
    membership_authority: crate::sync::store_commit::StoreOperationMembershipAuthority,
    package_bytes: &[u8],
    package_object: crate::sync::storage::ExactObjectRef,
) -> crate::sync::store_commit::StoreBatchCommit {
    crate::sync::store_commit::StoreBatchCommit::signed_operations(
        graph.commit.store_root_hash,
        graph.commit.write_id.clone(),
        graph.reference.coord.clone(),
        graph.commit.author_registration.clone(),
        &graph.registration,
        graph.commit.order.clone(),
        graph.commit.membership_state.clone(),
        graph.commit.device_state.clone(),
        membership_authority,
        crate::sync::store_commit::StoreCommitOperationsInput {
            acknowledgement: None,
            control: graph.commit.control().cloned(),
            device_join_attempt_decisions: graph.commit.device_join_attempt_decisions().to_vec(),
            device_join_outcomes: graph.commit.device_join_outcomes().to_vec(),
            device_join_cleanup_receipts: graph.commit.device_join_cleanup_receipts().to_vec(),
            provider_access_grants: graph.commit.provider_access_grants().to_vec(),
            provider_access_withdrawals: graph.commit.provider_access_withdrawals().to_vec(),
            device_registrations: graph.commit.device_registrations().to_vec(),
            device_exclusion_proposals: graph.commit.device_exclusion_proposals().to_vec(),
            device_exclusion_outcomes: graph.commit.device_exclusion_outcomes().to_vec(),
            stream_activations: graph.commit.stream_activations().to_vec(),
            circle_controls: graph.commit.circle_controls().to_vec(),
            store_package: Some(crate::sync::store_commit::StorePackageInput {
                candidate_family: graph.commit.candidate_family(),
                schema_version,
                bytes: package_bytes,
                object: package_object,
            }),
            circle_packages: &[],
        },
        &graph.device_signer,
    )
    .expect("re-sign exact Store commit")
}

async fn replace_exact_package_bytes(
    storage: &TestStore,
    graph: &ExactPublishedCommit,
    bytes: Vec<u8>,
) {
    use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};

    let package = graph
        .commit
        .store_package()
        .expect("test Store commit carries a Store package");
    let stream_id = commit_stream_id(&graph.reference);
    let prefix = crate::sync::store_commit::package_semantic_prefix(
        graph.commit.candidate_family(),
        &stream_id,
        graph.reference.coord.sequence(),
        package.content_hash,
    );
    let context = ProtocolObjectContext::store_encrypted(
        storage.root.store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    storage
        .delete_protocol_object(&package.object)
        .await
        .expect("delete replaced exact Store package");
    let prepared = storage
        .prepare_protocol_object(&context, package.object.slot().clone(), &prefix, bytes)
        .expect("prepare replacement exact Store package");
    crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("publish replacement exact Store package");
}

async fn replace_store_package_with_malformed_bytes(
    storage: &TestStore,
    reference: crate::sync::store_commit::StoreBatchCommitRef,
) -> crate::sync::store_commit::StoreBatchCommitRef {
    let graph = load_exact_published_commit(storage, reference).await;
    let malformed = b"not a SQLite changeset";
    let stream_id = commit_stream_id(&graph.reference);
    let package_object = create_exact_protocol_object(
        &storage.storage,
        &crate::sync::storage::ProtocolObjectContext::store_encrypted(
            storage.root.store_root_hash,
            crate::sync::storage::ProtocolObjectDomain::StorePackage,
        ),
        &crate::sync::store_commit::package_semantic_prefix(
            graph.commit.candidate_family(),
            &stream_id,
            graph.reference.coord.sequence(),
            crate::sync::store_commit::ObjectHash::digest(malformed),
        ),
        ".pkg",
        malformed,
    )
    .await
    .expect("publish malformed exact Store package");
    let malformed_commit = sign_exact_commit_with_package(
        &graph,
        SCHEMA_VERSION,
        graph
            .commit
            .operations_membership_authority()
            .expect("published test commit carries validated operations"),
        malformed,
        package_object,
    );
    replace_exact_commit_bytes(
        storage,
        &graph,
        malformed_commit.to_bytes(),
        malformed_commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await
}

async fn publish_exact_changeset_with_authority(
    storage: &TestStore,
    name: &str,
    sequence: u64,
    changeset: &[u8],
    authority: Option<crate::sync::membership::MembershipCoord>,
) -> crate::sync::store_commit::StoreBatchCommitRef {
    let reference = storage
        .publish_changeset(name, sequence, changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(storage, reference).await;
    let commit = resign_exact_commit(
        storage,
        &graph,
        SCHEMA_VERSION,
        authority.map(crate::sync::membership::MembershipGrantCreationAuthority::Entry),
    )
    .await;
    replace_exact_commit_bytes(
        storage,
        &graph,
        commit.to_bytes(),
        commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await
}

async fn create_exact_blob(
    db: &crate::database::Database,
    storage: &TestStore,
    namespace: &str,
    id: &str,
    cloud_path: Option<&str>,
    bytes: &[u8],
) -> crate::blob::locator::StoredBlobRef {
    let (uploader, registration) = store_database(db)
        .local_blob_write_authority()
        .await
        .expect("load exact blob write authority");
    let authority = crate::sync::storage::BlobWriteAuthority::new(&uploader, &registration)
        .expect("validate exact blob write authority");
    let protection = EncryptionService::from_key([42; 32]);
    let locator = match cloud_path {
        Some(path) => crate::blob::locator::BlobLocator::browsable(
            namespace,
            id,
            uploader.clone(),
            path,
            bytes.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(bytes),
        ),
        None => crate::blob::locator::BlobLocator::opaque(
            namespace,
            id,
            uploader.clone(),
            crate::blob::locator::RemoteAudience::Store,
            crate::blob::BlobScope::Master,
            protection.seal_key_fingerprint(),
            bytes.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(bytes),
        ),
    }
    .expect("build exact blob locator");
    let temp = tempfile::tempdir().expect("create exact blob spool directory");
    let plaintext = temp.path().join("plaintext");
    let spool = temp.path().join("stored");
    crate::local_blob::write_atomic(&plaintext, bytes)
        .await
        .expect("write exact blob plaintext");
    let slot = storage
        .allocate_blob_slot(&locator, &authority)
        .await
        .expect("allocate exact blob slot");
    let spool_protection = match cloud_path {
        Some(_) => crate::sync::storage::BlobSpoolProtection::Browsable,
        None => crate::sync::storage::BlobSpoolProtection::Opaque(protection),
    };
    storage
        .seal_blob_to_spool(&locator, &authority, spool_protection, &plaintext, &spool)
        .await
        .expect("seal exact blob");
    let stored = storage
        .prepare_blob_object(&locator, &authority, slot, &spool)
        .await
        .expect("prepare exact blob object");
    storage
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

async fn read_exact_blob(
    storage: &TestStore,
    blob: &crate::blob::locator::StoredBlobRef,
) -> Vec<u8> {
    let temp = tempfile::tempdir().expect("create exact blob read directory");
    let staged = storage
        .stage_verified_blob_plaintext(
            blob,
            match blob.locator() {
                crate::blob::locator::BlobLocator::Opaque { .. } => {
                    crate::sync::storage::BlobSpoolProtection::Opaque(EncryptionService::from_key(
                        [42; 32],
                    ))
                }
                crate::blob::locator::BlobLocator::Browsable { .. } => {
                    crate::sync::storage::BlobSpoolProtection::Browsable
                }
            },
            &temp.path().join("plaintext"),
        )
        .await
        .expect("read exact blob object");
    tokio::fs::read(staged.path())
        .await
        .expect("read staged exact blob plaintext")
}

async fn row_blob_object_key(db: &crate::database::Database, table: &str, row_id: &str) -> String {
    db.row_blob_ref(table, row_id)
        .await
        .expect("load exact row blob reference")
        .stored()
        .expect("Remote row has exact blob object authority")
        .object()
        .slot()
        .logical_key()
        .to_string()
}

struct FaultingStorage<'a> {
    inner: &'a CloudSyncStorage,
    membership_reads_until_failure: std::sync::atomic::AtomicUsize,
    fail_blob_read: std::sync::atomic::AtomicBool,
}

impl<'a> FaultingStorage<'a> {
    fn membership(inner: &'a CloudSyncStorage, read: usize) -> Self {
        Self {
            inner,
            membership_reads_until_failure: std::sync::atomic::AtomicUsize::new(read),
            fail_blob_read: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn blob(inner: &'a CloudSyncStorage) -> Self {
        Self {
            inner,
            membership_reads_until_failure: std::sync::atomic::AtomicUsize::new(0),
            fail_blob_read: std::sync::atomic::AtomicBool::new(true),
        }
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
impl SyncStorage for FaultingStorage<'_> {
    fn store_blob_protection(
        &self,
    ) -> Result<crate::sync::storage::BlobSpoolProtection, crate::sync::storage::StorageError> {
        self.inner.store_blob_protection()
    }

    async fn provider_binding(
        &self,
    ) -> Result<crate::sync::storage::ResolvedProviderBinding, crate::sync::storage::StorageError>
    {
        self.inner.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<crate::storage::cloud::ObjectSlot, crate::sync::storage::StorageError> {
        self.inner
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<crate::sync::storage::PreparedExactObject, crate::sync::storage::StorageError> {
        self.inner
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &crate::sync::storage::PreparedExactObject,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        object: &crate::sync::storage::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, crate::sync::storage::StorageError> {
        if self.fail_membership_read(semantic_prefix) {
            return Err(crate::sync::storage::StorageError::Storage(
                "forced exact membership read failure".to_string(),
            ));
        }
        self.inner
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, crate::sync::storage::ExactObjectRef), crate::sync::storage::StorageError>
    {
        if self.fail_membership_read(semantic_prefix) {
            return Err(crate::sync::storage::StorageError::Storage(
                "forced exact membership slot read failure".to_string(),
            ));
        }
        self.inner
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &crate::sync::storage::ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, crate::sync::storage::PreparedExactObject),
        crate::sync::storage::StorageError,
    > {
        if self.fail_membership_read(semantic_prefix) {
            return Err(crate::sync::storage::StorageError::Storage(
                "forced exact membership slot read failure".to_string(),
            ));
        }
        self.inner
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
    ) -> Result<crate::storage::cloud::ObjectSlot, crate::sync::storage::StorageError> {
        self.inner.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        protection: crate::sync::storage::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool_file: &std::path::Path,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        slot: crate::storage::cloud::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<crate::blob::locator::StoredBlobRef, crate::sync::storage::StorageError> {
        self.inner
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.verify_blob_object(blob).await
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        dest: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, crate::sync::storage::StorageError> {
        if self
            .fail_blob_read
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::sync::storage::StorageError::Storage(
                "forced exact blob read failure".to_string(),
            ));
        }
        self.inner.stage_exact_blob_download(blob, dest).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::sync::storage::BlobSpoolProtection,
        dest: &std::path::Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, crate::sync::storage::StorageError> {
        if self
            .fail_blob_read
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::sync::storage::StorageError::Storage(
                "forced exact blob read failure".to_string(),
            ));
        }
        self.inner
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
    ) -> Result<(), crate::sync::storage::StorageError> {
        self.inner.delete_blob_object(blob).await
    }
}

#[tokio::test]
async fn pull_applies_remote_changeset_and_surfaces_row_changes() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // Source device records a note as changeset seq 1.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("founder", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish founder changeset");
    let stream_id = commit_stream_id(&commit);

    // Second device pulls.
    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get(&stream_id), Some(&1));
    assert_eq!(
        materialized_sequences(&db2).await.get(&stream_id),
        Some(&1),
        "the row and its durable position commit in the pull that applies it",
    );
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "First"
    );
    assert!(result
        .row_changes
        .iter()
        .any(|c| c.table == "notes" && c.pk() == Some("n1")));
}

#[tokio::test]
async fn position_write_failure_rolls_back_the_remote_rows() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Remote', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage
        .publish_changeset("founder", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish founder changeset");

    let target = open_test_db();
    exec(
        &target,
        "CREATE TEMP TRIGGER reject_materialized_insert BEFORE INSERT ON materialized_commits \
         BEGIN SELECT RAISE(ABORT, 'injected materialized-position write failure'); END;",
    )
    .await;
    let (_tmp, store_dir) = temp_store_dir();
    let error = pull_into_result(&target, &storage, &store_dir)
        .await
        .expect_err("materialized-position failure aborts the pull");
    assert!(
        matches!(error, StorePullError::Database(_)),
        "unexpected pull error: {error:?}"
    );
    assert!(
        !row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "the row cannot commit when its position write fails",
    );
    assert!(materialized_sequences(&target).await.is_empty());
}

#[tokio::test]
async fn ordinary_pull_starts_from_its_durable_position() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('stale-row', 'Remote', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("founder", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish founder changeset");
    let stream_id = commit_stream_id(&commit);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    let (updated, result) = pull_into(&target, &storage, &store_dir).await;

    assert_eq!(updated.get(&stream_id), Some(&1));
    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_positions.is_empty());
    assert_eq!(
        materialized_sequences(&target).await.get(&stream_id),
        Some(&1),
    );
    assert!(
        row_exists(&target, "SELECT 1 FROM notes WHERE id = 'stale-row'").await,
        "ordinary pull derives coverage from durable rows, not caller input",
    );
}

#[tokio::test]
async fn ordinary_pull_uses_its_durable_position_on_every_call() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let first = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('position-row', 'One', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let second = capture_bytes(
        &source,
        &["UPDATE notes SET title = 'Two', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'position-row'"],
    )
    .await;
    let first_commit = storage
        .publish_changeset("dev1", 1, &first, SCHEMA_VERSION)
        .await
        .expect("publish first exact Store changeset");
    let stream_id = commit_stream_id(&first_commit);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, &store_dir).await;
    storage
        .publish_changeset("dev1", 2, &second, SCHEMA_VERSION)
        .await
        .expect("publish second exact Store changeset");

    let (updated, result) = pull_into(&target, &storage, &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_positions.is_empty());
    assert_eq!(updated.get(&stream_id), Some(&2));
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'position-row'").await,
        "Two",
    );
}

#[tokio::test]
async fn ordinary_pull_applies_the_change_immediately_after_its_durable_position() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let first = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('next-row', 'One', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let second = capture_bytes(
        &source,
        &["UPDATE notes SET title = 'Two', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'next-row'"],
    )
    .await;
    let first_commit = storage
        .publish_changeset("dev1", 1, &first, SCHEMA_VERSION)
        .await
        .expect("publish first exact Store changeset");
    let stream_id = commit_stream_id(&first_commit);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, &store_dir).await;
    storage
        .publish_changeset("dev1", 2, &second, SCHEMA_VERSION)
        .await
        .expect("publish second exact Store changeset");

    let (updated, result) = pull_into(&target, &storage, &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get(&stream_id), Some(&2));
    assert_eq!(
        materialized_sequences(&target).await.get(&stream_id),
        Some(&2),
    );
}

#[tokio::test]
async fn invalid_materialized_positions_are_rejected_at_the_database_boundary() {
    let target = open_test_db();
    let invalid_insert = target
        .call(|conn| {
            conn.execute(
                "INSERT INTO materialized_commits (device_id, seq, commit_ref) \
                 VALUES ('invalid-device', -1, '{}')",
                [],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        })
        .await;
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
        crate::CommitFrontier(std::collections::BTreeMap::new()),
    );
}

#[tokio::test]
async fn merge_materialization_retains_closed_input_and_rejects_corruption() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('retained-row', 'Retained', NULL, \
                     '0000000001000-0000-retained', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("retained", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish retained-input fixture");
    let stream_id = commit_stream_id(&commit);
    let target = open_test_db();

    pull_into(&target, &storage, &temp_store_dir().1).await;

    let queried_stream = stream_id.clone();
    let (canonical_input, input_hash, retained_ref) = target
        .call(move |conn| {
            conn.query_row(
                "SELECT canonical_input, input_hash, commit_ref \
                 FROM retained_merge_materializations \
                 WHERE device_id = ?1 AND seq = 1",
                [queried_stream],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("read retained Merge materialization input");
    assert_eq!(
        input_hash,
        crate::sync::store_commit::ObjectHash::digest(&canonical_input).to_string()
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
            "history_summary",
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
    let package_ref: crate::sync::store_commit::StorePackageRef =
        serde_json::from_value(retained_store["reference"].clone())
            .expect("parse retained Store package ref");
    let package_remote = stored_remote_object(&target, &package_ref.object).await;
    let parsed_input_hash = input_hash.parse().expect("parse retained input hash");
    assert!(matches!(
        package_remote,
        crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.identity.domain,
                crate::sync::remote_object::SharedLiveSetObjectDomain::StorePackage {
                    reference
                } if reference == &package_ref
            )
                && matches!(
                    record.bytes.stored(),
                    crate::sync::remote_object::RemoteStoredRepresentation::ExternalExact {
                        object
                    } if object == &package_ref.object
                )
                && matches!(
                    &record.state,
                    crate::sync::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.activated.contains(
                        &crate::sync::remote_object::SharedObjectOwner::StoreCommit(commit.clone())
                    ) && ownership.activated.contains(
                        &crate::sync::remote_object::SharedObjectOwner::RetainedReplay(
                            crate::sync::remote_object::RetainedReplayOwner::Commit {
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
    replace_retained_merge_input(&target, stream_id.clone(), missing_receiver).await;
    let error = store_database(&target)
        .materialized_frontier()
        .await
        .expect_err("a retained package must carry its receive-time conflict bound");
    assert!(error
        .to_string()
        .contains("package application does not match its applied packages"));

    replace_retained_merge_input(&target, stream_id.clone(), canonical_input.clone()).await;

    let corrupt_stream = stream_id.clone();
    target
        .call(move |conn| {
            conn.execute(
                "UPDATE retained_merge_materializations SET canonical_input = x'7b7d' \
                 WHERE device_id = ?1 AND seq = 1",
                [corrupt_stream],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("corrupt retained Merge input");
    let error = store_database(&target)
        .materialized_frontier()
        .await
        .expect_err("corrupt retained Merge input must invalidate its materialization");
    assert!(error
        .to_string()
        .contains("input hash differs from its bytes"));
}

#[tokio::test]
async fn merge_materialization_rejects_missing_tampered_and_invented_replay_pins() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let first_changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('pin-first', 'First', NULL, \
                     '0000000001000-0000-pins', '2026-01-01')",
        ],
    )
    .await;
    let first = storage
        .publish_changeset("pins", 1, &first_changeset, SCHEMA_VERSION)
        .await
        .expect("publish first replay-pin fixture");
    let second_changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('pin-second', 'Second', NULL, \
                     '0000000002000-0000-pins', '2026-01-01')",
        ],
    )
    .await;
    let second = storage
        .publish_changeset("pins", 2, &second_changeset, SCHEMA_VERSION)
        .await
        .expect("publish second replay-pin fixture");
    let target = open_test_db();
    pull_into(&target, &storage, &temp_store_dir().1).await;

    let (_first_owner, first_package, first_remote) =
        retained_store_package_pin(&target, &first).await;
    let (second_owner, second_package, second_remote) =
        retained_store_package_pin(&target, &second).await;

    let mut missing = second_remote.clone();
    let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut missing else {
        unreachable!("retained package is shared")
    };
    let crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &mut record.state
    else {
        unreachable!("retained package is activated")
    };
    assert!(ownership.activated.remove(
        &crate::sync::remote_object::SharedObjectOwner::RetainedReplay(second_owner.clone())
    ));
    replace_stored_remote_object(&target, &second_package.object, &missing).await;
    assert!(store_database(&target)
        .materialized_frontier()
        .await
        .expect_err("missing replay pin must invalidate materialization")
        .to_string()
        .contains("retained-replay ownership index"));
    replace_stored_remote_object(&target, &second_package.object, &second_remote).await;

    let mut tampered = missing;
    let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut tampered
    else {
        unreachable!("retained package is shared")
    };
    let crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &mut record.state
    else {
        unreachable!("retained package is activated")
    };
    let crate::sync::remote_object::RetainedReplayOwner::Commit { commit, .. } = &second_owner;
    ownership.activated.insert(
        crate::sync::remote_object::SharedObjectOwner::RetainedReplay(
            crate::sync::remote_object::RetainedReplayOwner::Commit {
                commit: commit.clone(),
                input_hash: crate::sync::store_commit::ObjectHash::digest(b"tampered input"),
            },
        ),
    );
    replace_stored_remote_object(&target, &second_package.object, &tampered).await;
    assert!(store_database(&target)
        .materialized_frontier()
        .await
        .expect_err("tampered replay pin must invalidate materialization")
        .to_string()
        .contains("retained-replay ownership index"));
    replace_stored_remote_object(&target, &second_package.object, &second_remote).await;

    let mut invented = first_remote;
    let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record) = &mut invented
    else {
        unreachable!("retained package is shared")
    };
    let crate::sync::remote_object::OwnedObjectState::UploadedVerified { ownership } =
        &mut record.state
    else {
        unreachable!("retained package is activated")
    };
    ownership.activated.insert(
        crate::sync::remote_object::SharedObjectOwner::RetainedReplay(second_owner.clone()),
    );
    replace_stored_remote_object(&target, &first_package.object, &invented).await;
    let crate::sync::remote_object::RetainedReplayOwner::Commit { commit, input_hash } =
        &second_owner;
    let crate::sync::store_commit::StoreCommitCoord {
        stream_id,
        sequence,
    } = &commit.coord;
    let stream_id = stream_id.to_string();
    let sequence = i64::try_from(*sequence).expect("invented replay sequence fits SQLite");
    let commit_ref = serde_json::to_string(commit).expect("serialize invented replay owner");
    let input_hash = input_hash.to_string();
    let first_object_id =
        crate::sync::remote_object::remote_object_id(&first_package.object).to_string();
    target
        .call(move |conn| {
            conn.execute(
                "INSERT INTO retained_replay_objects
                 (device_id, seq, commit_ref, input_hash, object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![stream_id, sequence, commit_ref, input_hash, first_object_id],
            )
            .map_err(crate::database::DbError::from)?;
            Ok(())
        })
        .await
        .expect("invent replay ownership index row");
    assert!(
        crate::sync::store::store_package_is_retained_for_replay_for_test(
            &target,
            first_package,
            first,
        )
        .await
        .expect_err("invented replay pin must block reclamation validation")
        .to_string()
        .contains("ownership differs from its exact object closure")
    );
    assert!(store_database(&target)
        .materialized_frontier()
        .await
        .expect_err("invented replay pin must invalidate materialization")
        .to_string()
        .contains("ownership differs from its exact object closure"));
}

#[tokio::test]
async fn retained_input_collision_rolls_back_remote_rows_and_materialization() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('rollback-row', 'Must roll back', NULL, \
                     '0000000001000-0000-rollback', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("rollback", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish retained-input rollback fixture");
    let stream_id = commit_stream_id(&commit);
    let target_dir = tempfile::tempdir().expect("create retained collision database directory");
    let target_path = target_dir.path().join("store.sqlite");
    let copied_path = target_path.clone();
    source
        .call(move |conn| {
            conn.execute("VACUUM INTO ?1", [copied_path.to_string_lossy().as_ref()])
                .map(|_| ())
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("copy the locally-authored retained input");
    let (target, _stamper) = crate::database::Database::open(
        &target_path,
        test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "test-device".to_string(),
        &test_migrations(),
    )
    .expect("open copied retained collision database");
    let conflicting_stream = stream_id.clone();
    target
        .call(move |conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            tx.execute(
                "DELETE FROM materialized_commits WHERE device_id = ?1 AND seq = 1",
                [&conflicting_stream],
            )
            .map_err(crate::database::DbError::from)?;
            tx.execute("DELETE FROM notes WHERE id = 'rollback-row'", [])
                .map_err(crate::database::DbError::from)?;
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("remove the locally materialized outcome while retaining its exact input");

    let error = pull_into_result(&target, &storage, &temp_store_dir().1)
        .await
        .expect_err("retained input collision must fail the pull transaction");
    assert!(
        error
            .to_string()
            .contains("already contains different exact input"),
        "unexpected pull error: {error:?}"
    );
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'rollback-row'").await);
    let checked_stream = stream_id.clone();
    let materialized = target
        .call(move |conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM materialized_commits \
                 WHERE device_id = ?1 AND seq = 1)",
                [checked_stream],
                |row| row.get::<_, bool>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("read rolled-back materialization state");
    assert!(!materialized);
}

#[tokio::test]
async fn empty_package_materializes_its_exact_commit_position() {
    let source = open_test_db();
    let target = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let commit = storage
        .publish_changeset("dev1", 1, &[], SCHEMA_VERSION)
        .await
        .expect("publish empty exact Store changeset");
    let stream_id = commit_stream_id(&commit);
    let (_tmp, store_dir) = temp_store_dir();

    let (updated, result) = pull_into(&target, &storage, &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get(&stream_id), Some(&1));
    assert_eq!(
        materialized_sequences(&target).await.get(&stream_id),
        Some(&1),
    );
}

#[tokio::test]
async fn host_write_after_remote_apply_observes_the_matching_position() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('remote', 'Remote', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("dev1", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, &store_dir).await;

    let tables = target.synced_tables().to_vec();
    let write_id = target.new_write_id();
    target
        .call(move |conn| {
            crate::sync::store::StoreDatabase::run_internal_store_write_transaction_on(
                conn,
                &tables,
                None,
                write_id,
                |tx| {
                    let remote_row: bool = tx
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM notes WHERE id = 'remote')",
                            [],
                            |row| row.get(0),
                        )
                        .map_err(crate::database::DbError::from)?;
                    let materialized: Option<u64> = tx
                        .query_row(
                            "SELECT seq FROM materialized_commits WHERE device_id = ?1",
                            [&stream_id],
                            |row| row.get::<_, i64>(0).map(|seq| seq as u64),
                        )
                        .optional()
                        .map_err(crate::database::DbError::from)?;
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
                    .map_err(crate::database::DbError::from)
                },
            )
        })
        .await
        .expect("host write after remote apply");

    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'local'").await);
}

/// A changeset whose object was reclaimed (deleted as superseded) past this
/// device's position surfaces a `MissingChangeset` held reason and holds the
/// position at the gap — the host reports reclaimed history rather than a generic
/// stall, and the device stream never advances over a changeset it did not apply.
#[tokio::test]
async fn pull_holds_and_names_a_reclaimed_changeset_gap() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // The source device's head advertises seq 1, but the changeset object is
    // gone: reclamation deleted it as superseded. `store_changeset` both writes
    // the object and advances the head to seq 1; deleting the object leaves the
    // head pointing past a hole.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);
    let graph = load_exact_published_commit(&storage, commit).await;
    let package = graph
        .commit
        .store_package()
        .expect("Store commit carries a Store package");
    storage
        .storage
        .delete_protocol_object(&package.object)
        .await
        .expect("delete exact Store package");

    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, &ld).await;

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
        HeldStorePositionReason::ObjectUnreadable { key, detail }
            if key == "exact Store object" && detail.contains("object not found")
    ));
    // The position holds at the gap: dev1 never advances over the unapplied seq.
    assert_eq!(updated.get(&stream_id).copied().unwrap_or(0), 0);
}

#[tokio::test]
async fn uniqueness_conflict_rolls_back_the_entire_changeset_and_position() {
    let db1 = unique_note_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('would-partially-land', 'free-slug', 'First row', \
                     '0000000000900-0000-dev1', '2026-01-01')",
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('remote', 'same-slug', 'Remote', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let commit = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&commit);

    let db2 = unique_note_db();
    exec(
        &db2,
        "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
         VALUES ('local', 'same-slug', 'Local', '0000000002000-0000-dev2', '2026-01-01')",
    )
    .await;
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, &ld).await;

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
        materialized_sequences(&db2).await.get(&stream_id),
        None,
        "a rejected changeset has no durable position",
    );
    assert!(row_exists(&db2, "SELECT 1 FROM unique_notes WHERE id = 'local'").await);
    assert!(!row_exists(&db2, "SELECT 1 FROM unique_notes WHERE id = 'remote'").await);
    assert!(
        !row_exists(
            &db2,
            "SELECT 1 FROM unique_notes WHERE id = 'would-partially-land'",
        )
        .await,
        "rows before the constraint conflict roll back with the rejected changeset",
    );
}

#[tokio::test]
async fn non_retryable_constraint_is_reported_even_when_the_changeset_also_violates_a_foreign_key()
{
    let source = mixed_constraint_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    exec(
        &source,
        "INSERT INTO constraint_parents (id, _updated_at) \
         VALUES ('missing-on-target', '0000000001000-0000-dev1'); \
         INSERT INTO constraint_parents (id, _updated_at) \
         VALUES ('present-on-target', '0000000001000-0000-dev1')",
    )
    .await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO constraint_items (id, parent_id, slug, _updated_at) \
             VALUES ('fk-row', 'missing-on-target', 'free-slug', \
                     '0000000002000-0000-dev1')",
            "INSERT INTO constraint_items (id, parent_id, slug, _updated_at) \
             VALUES ('unique-row', 'present-on-target', 'duplicate-slug', \
                     '0000000002001-0000-dev1')",
        ],
    )
    .await;
    storage
        .publish_changeset("dev1", 1, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");

    let target = mixed_constraint_db();
    exec(
        &target,
        "INSERT INTO constraint_parents (id, _updated_at) \
         VALUES ('present-on-target', '0000000001000-0000-dev2'); \
         INSERT INTO constraint_items (id, parent_id, slug, _updated_at) \
         VALUES ('local-row', 'present-on-target', 'duplicate-slug', \
                 '0000000003000-0000-dev2')",
    )
    .await;
    let (_tmp, store_dir) = temp_store_dir();

    let (updated, result) = pull_into(&target, &storage, &store_dir).await;

    assert_eq!(result.changesets_applied, 0);
    let conflicts = constraint_conflicts(&result);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        conflicts[0].reason,
        HeldStorePositionReason::ConstraintConflict(vec!["constraint_items".to_string()])
    );
    assert_eq!(updated.get("dev1"), None);
    assert_eq!(materialized_sequences(&target).await.get("dev1"), None);
    assert!(
        !row_exists(
            &target,
            "SELECT 1 FROM constraint_items WHERE id = 'fk-row'"
        )
        .await
    );
    assert!(
        !row_exists(
            &target,
            "SELECT 1 FROM constraint_items WHERE id = 'unique-row'"
        )
        .await
    );
    assert!(
        row_exists(
            &target,
            "SELECT 1 FROM constraint_items WHERE id = 'local-row'"
        )
        .await
    );
}

#[tokio::test]
async fn fk_violation_still_retries_and_resolves() {
    let child_source = open_test_db();
    let storage = create_store(&child_source, UserKeypair::generate()).await;
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
    exec(
        &child_source,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Parent', NULL, '0000000001000-0000-parent', '2026-01-01')",
    )
    .await;
    let child_cs = capture_bytes(
        &child_source,
        &[
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('t1', 'n1', 'green', '0000000001001-0000-child', '2026-01-01')",
        ],
    )
    .await;
    let child_commit = storage
        .publish_changeset("dev-child", child_sequence, &child_cs, SCHEMA_VERSION)
        .await
        .expect("publish child exact Store changeset");

    let parent_source = open_test_db();
    let parent_cs = capture_bytes(
        &parent_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Parent', NULL, '0000000001000-0000-parent', '2026-01-01')",
        ],
    )
    .await;
    let parent_commit = storage
        .publish_changeset("dev-parent", parent_sequence, &parent_cs, SCHEMA_VERSION)
        .await
        .expect("publish parent exact Store changeset");

    let target = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (_, first) = pull_into(&target, &storage, &ld).await;
    assert!(first
        .held_positions
        .iter()
        .any(|held| held.reason == HeldStorePositionReason::ForeignKeyDependency));
    let (updated, result) = pull_into(&target, &storage, &ld).await;

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
        query_text(&target, "SELECT tag FROM note_tags WHERE id = 't1'").await,
        "green"
    );
}

#[tokio::test]
async fn pull_skips_changeset_from_newer_schema() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;

    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Future', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let commit = resign_exact_commit(
        &storage,
        &graph,
        SCHEMA_VERSION + 1,
        graph.commit.membership_authority.clone(),
    )
    .await;
    replace_exact_commit_bytes(
        &storage,
        &graph,
        commit.to_bytes(),
        commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(newer_schema_positions(&result).len(), 1);
    // The position must NOT advance past a genuine newer-schema changeset: it
    // becomes applicable once this app updates, and an already-running device
    // never re-bootstraps, so advancing would strand its rows forever. Leaving
    // the position put re-fetches seq 1 after the upgrade.
    assert_eq!(updated.get("dev1"), None);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

/// The schema-version gate reads `env.schema_version` to classify a held stream
/// as routine version skew (the peer upgraded past us), so it must run only on an
/// authenticated envelope. A forged object carrying a large `schema_version` and
/// an invalid signature must surface as tamper — an invalid signature — not be
/// laundered into the benign `skipped_schema` count, where a host waits for an
/// upgrade that will never resolve it while the real signal is never raised.
#[tokio::test]
async fn a_forged_newer_schema_changeset_reports_tamper_not_a_schema_skip() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;
    // A changeset stamped one schema version above the local db, signed at its own
    // position so the position check passes and the loop reaches the signature and
    // schema checks.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'ForgedFuture', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let commit = resign_exact_commit(
        &storage,
        &graph,
        SCHEMA_VERSION + 1,
        graph.commit.membership_authority.clone(),
    )
    .await;
    let mut forged: serde_json::Value = serde_json::from_slice(&commit.to_bytes()).unwrap();
    forged["signature"] = serde_json::Value::String("0".repeat(128));
    let commit_ref = replace_exact_commit_bytes_before_commit_validation(
        &storage,
        &graph,
        serde_json::to_vec(&forged).unwrap(),
        commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;
    let expected_stream_id = commit_stream_id(&graph.reference);

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, &temp_store_dir().1)
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
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(
        materialized_sequences(&db2).await.get(&expected_stream_id),
        None,
    );
}

/// A genuine newer-schema changeset is signed, so verifying the signature before
/// the schema gate does not change its handling: it still verifies, still counts
/// as a schema skip, still holds the position, and applies once the local schema
/// catches up. The reorder rejects only forgeries, never an authentic upgrade.
#[tokio::test]
async fn a_signed_newer_schema_changeset_still_counts_as_a_schema_skip() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'SignedFuture', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let commit = resign_exact_commit(
        &storage,
        &graph,
        SCHEMA_VERSION + 1,
        graph.commit.membership_authority.clone(),
    )
    .await;
    replace_exact_commit_bytes(
        &storage,
        &graph,
        commit.to_bytes(),
        commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(newer_schema_positions(&result).len(), 1);
    assert!(invalid_changeset_positions(&result).is_empty());
    assert_eq!(result.changesets_applied, 0);
    // Held, not advanced: it becomes applicable once this app upgrades.
    assert_eq!(updated.get("dev1"), None);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

/// The pull gate compares an incoming changeset's `schema_version` against the
/// opened db's [`Database::schema_version`], not a hand-bumped constant: a peer at
/// version N applies a changeset stamped N and skips one stamped N+1 without
/// advancing its position. The peer's own version is derived from the db, so this
/// fails if the gate stops tracking the schema that actually exists on disk. (The
/// push side — that an *outgoing* changeset is stamped with the db's version — is
/// covered by `push_stamps_the_dbs_schema_version`, which drives the real producer.)
#[tokio::test]
async fn pull_gate_tracks_the_dbs_schema_version() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;

    let n = db1.schema_version();

    // seq 1 stamped at exactly the peer's schema version: applies.
    let cs1 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'At N', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let first_reference = storage
        .publish_changeset("dev1", 1, &cs1, n)
        .await
        .expect("publish first exact Store changeset");
    let stream_id = commit_stream_id(&first_reference);

    // seq 2 stamped one above the peer's schema version: skipped, position held.
    let cs2 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n2', 'Above N', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev1", 2, &cs2, n)
        .await
        .expect("publish second exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let commit = resign_exact_commit(
        &storage,
        &graph,
        n + 1,
        graph.commit.membership_authority.clone(),
    )
    .await;
    replace_exact_commit_bytes(
        &storage,
        &graph,
        commit.to_bytes(),
        commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;

    let db2 = open_test_db();
    assert_eq!(
        db2.schema_version(),
        n,
        "both peers open the same migration ladder, so they share the wire version"
    );
    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

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
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n2'").await);
}

/// The push side stamps an outgoing changeset with the db's
/// [`Database::schema_version`], driven through the real producer
/// (`service::sync`) and read back off the produced envelope — so a regression
/// that stamped a constant instead would fail here. Paired with
/// `pull_gate_tracks_the_dbs_schema_version`, which covers the receiver gate.
#[tokio::test]
async fn push_stamps_the_dbs_schema_version() {
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;
    let tables = test_synced_tables();
    let (_tmp, ld1) = temp_store_dir();

    // `shared = 1` so the gated `notes` root survives the push gate and there is an
    // outgoing changeset to inspect.
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('n1', 'One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    let keypair = storage.protocol_founder_keypair();
    let result = sync_for_test(
        &db1,
        &tables,
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld1,
    )
    .await
    .expect("sync push");

    let position = result.expect("an outgoing Store commit");
    let stream_id = commit_stream_id(&position);
    let (_commit_ref, commit) = load_exact_materialized_commit(
        &db1,
        &storage.storage,
        &stream_id,
        position.coord.sequence(),
    )
    .await
    .expect("load exact Store commit")
    .expect("Store commit slot");
    assert_eq!(
        commit
            .value
            .store_package()
            .expect("outgoing Store commit carries a Store package")
            .schema_version,
        db1.schema_version(),
        "the outgoing Store package is stamped with the database schema version",
    );
}

#[tokio::test]
async fn sync_reuses_opened_schema_models() {
    crate::sync::gate::reset_from_tables_call_count();
    crate::blob::decl::reset_from_tables_call_count();

    let db = open_test_db();
    let storage = create_store(&db, UserKeypair::generate()).await;
    assert_eq!(crate::sync::gate::from_tables_call_count(), 1);
    assert_eq!(crate::blob::decl::from_tables_call_count(), 1);

    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'One', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    let keypair = storage.protocol_founder_keypair();
    let (_tmp, store_dir) = temp_store_dir();
    sync_for_test(
        &db,
        db.synced_tables(),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &store_dir,
    )
    .await
    .expect("sync");

    assert_eq!(crate::sync::gate::from_tables_call_count(), 1);
    assert_eq!(crate::blob::decl::from_tables_call_count(), 1);
}

#[tokio::test]
async fn pull_does_not_advance_position_past_a_blob_failed_changeset() {
    let db1 = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // Source dev1: seq 1 references a photo blob; seq 2 is a plain note.
    capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'One', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'attach', 11, '{}', '0000000001001-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"PHOTO-BYTES"),
            ),
        ],
    )
    .await;
    let (_source_tmp, source_store_dir) = temp_store_dir();
    store_local(&source_store_dir, "ph1", b"PHOTO-BYTES").await;
    make_test_root_remote(&db1, &storage, &source_store_dir, "n1").await;
    let first_commit = crate::sync::store::StoreDatabase::new(&db1)
        .latest_local_store_position()
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
    storage
        .storage
        .delete_blob_object(&stored)
        .await
        .expect("remove exact remote blob fixture");
    let cs2 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n2', 'Two', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    publish_blob_changeset(&db1, &storage, &source_store_dir, cs2, 1).await;

    // The puller declares note_photos blob-bearing, so seq 1's missing blob fails
    // while seq 2 (no blob) would succeed.
    let db2 = open_test_db_with_blob(photo_decl());
    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

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
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "seq 1's row must not be applied when its blob download fails",
    );
    // seq 2 is never reached -- the pull stops this device at the failed seq 1.
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n2'").await,
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
    let db1 = open_test_db();
    let storage = create_store(&db1, UserKeypair::generate()).await;

    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Corrupt', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev1", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let package = graph
        .commit
        .store_package()
        .expect("Store commit carries a Store package");
    let expected_package_hash = package.content_hash;
    let expected_stream_id = commit_stream_id(&graph.reference);
    replace_exact_package_bytes(&storage, &graph, cs[..cs.len() - 1].to_vec()).await;

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, &temp_store_dir().1)
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
                reason: HeldStorePositionReason::ObjectUnreadable { key, detail },
            } if device_id == &expected_stream_id
                && *package_hash == expected_package_hash
                && key == "exact Store object"
                && detail.contains("does not match stored size/hash")
        ),
        "unexpected held position: {:#?}",
        result.held_positions[0]
    );
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a size-mismatched changeset must not be applied",
    );
    assert_eq!(
        materialized_sequences(&db2).await.get(&expected_stream_id),
        None,
    );
}

/// A Store commit is signed for one exact sequence. Copying its bytes beneath a
/// different immutable slot is an object collision and cannot materialize rows.
#[tokio::test]
async fn a_store_commit_replayed_at_another_sequence_is_rejected() {
    let src = open_test_db();
    let storage = create_store(&src, UserKeypair::generate()).await;

    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Replayed', NULL, '0000000005000-0000-dev', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let crate::sync::store_commit::StoreCommitCoord { stream_id, .. } = &graph.reference.coord;
    let relocated_coord = crate::sync::store_commit::StoreCommitCoord {
        stream_id: *stream_id,
        sequence: 2,
    };
    let expected_stream_id = stream_id.to_string();
    let relocated_object = create_exact_protocol_object(
        &storage.storage,
        &crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            storage.root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        ),
        &crate::sync::store_commit::commit_semantic_prefix(
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
    let relocated_ref = crate::sync::store_commit::StoreBatchCommitRef {
        coord: relocated_coord,
        commit_hash: graph.commit.commit_hash(),
        object: relocated_object,
    };
    replace_exact_head(
        &storage,
        &graph,
        relocated_ref,
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;
    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, &temp_store_dir().1)
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
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a Store commit relocated to another sequence must not be applied",
    );
    assert_eq!(
        materialized_sequences(&db2).await.get(&expected_stream_id),
        None
    );
}

/// The signed Store slot includes the device id as well as the sequence.
#[tokio::test]
async fn a_store_commit_relocated_to_another_device_is_rejected() {
    let src = open_test_db();
    let storage = create_store(&src, UserKeypair::generate()).await;

    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Relocated', NULL, '0000000001000-0000-devVictim', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("devVictim", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let relocated_stream = crate::sync::membership::AuthorStreamId::from_bytes([99; 32]);
    let relocated_object = create_exact_protocol_object(
        &storage.storage,
        &crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            storage.root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        ),
        &crate::sync::store_commit::commit_semantic_prefix(
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
    let relocated_ref = crate::sync::store_commit::StoreBatchCommitRef {
        coord: crate::sync::store_commit::StoreCommitCoord {
            stream_id: relocated_stream,
            sequence: 1,
        },
        commit_hash: graph.commit.commit_hash(),
        object: relocated_object,
    };
    replace_exact_head(
        &storage,
        &graph,
        relocated_ref,
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;
    let expected_stream_id = commit_stream_id(&graph.reference);

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, &temp_store_dir().1)
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
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a Store commit relocated to another device must not be applied",
    );
    assert_eq!(
        materialized_sequences(&db2).await.get(&expected_stream_id),
        None
    );
}

/// A signed changeset sitting at the exact position its envelope declares is
/// untouched by the position binding — it applies normally. The check rejects
/// relocation, not authorship.
#[tokio::test]
async fn a_changeset_at_its_own_position_still_applies() {
    let src = open_test_db();
    let storage = create_store(&src, UserKeypair::generate()).await;
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'InPlace', NULL, '0000000001000-0000-dev', '2026-01-01')",
        ],
    )
    .await;
    let reference = storage
        .publish_changeset("dev", 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get(&stream_id), Some(&1));
    assert!(result.held_positions.is_empty());
}

#[tokio::test]
async fn corrupt_local_register_fails_without_materializing_the_remote_commit() {
    let good_source = open_test_db();
    let storage = create_store(&good_source, UserKeypair::generate()).await;
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

    let good_cs = capture_bytes(
        &good_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-good', 'Good', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    let good_commit = storage
        .publish_changeset("devA", good_sequence, &good_cs, SCHEMA_VERSION)
        .await
        .expect("publish valid exact Store changeset");

    let bad_source = open_test_db();
    // The base row exists (so the UPDATE below is an UPDATE, not an insert), but
    // through raw `exec`, so only the UPDATE enters the captured changeset.
    exec(
        &bad_source,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-bad', 'Base', NULL, '0000000001000-0000-devB', '2026-01-01')",
    )
    .await;
    let bad_cs = capture_bytes(
        &bad_source,
        &[
            "UPDATE notes SET title = 'Bad', _updated_at = '0000000002000-0000-devB' \
             WHERE id = 'n-bad'",
        ],
    )
    .await;
    let bad_commit = storage
        .publish_changeset("devB", bad_sequence, &bad_cs, SCHEMA_VERSION)
        .await
        .expect("publish invalid exact Store changeset bytes");

    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-bad', 'Local', NULL, 'not-a-stamp', '2026-01-01')",
    )
    .await;
    let (_tmp, ld) = temp_store_dir();
    let (_, first) = pull_into(&target, &storage, &ld).await;
    assert!(first.held_positions.is_empty());
    let error = pull_into_result(&target, &storage, &ld)
        .await
        .expect_err("an invalid local register must fail loudly");

    assert!(matches!(error, StorePullError::Database(_)));
    assert!(
        row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n-good'").await,
        "independent commit did not apply",
    );
    let good_stream_id = commit_stream_id(&good_commit);
    let bad_stream_id = commit_stream_id(&bad_commit);
    assert_eq!(
        materialized_sequences(&target).await.get(&good_stream_id),
        Some(&good_commit.coord.sequence()),
        "the independent commit completed before the corrupt local register was read",
    );
    assert_eq!(
        materialized_sequences(&target).await.get(&bad_stream_id),
        None,
        "the failing commit never materializes",
    );
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n-bad'").await,
        "Local",
        "the failing commit rolls back its row mutation",
    );
}

/// A signed Store commit whose package is not a SQLite changeset holds only its
/// own chain. An independent device's valid commit still materializes.
#[tokio::test]
async fn malformed_store_package_isolates_to_one_device() {
    let bad_source = open_test_db();
    let storage = create_store(&bad_source, UserKeypair::generate()).await;
    storage
        .device_id("founder")
        .await
        .expect("reserve the founder producer");
    storage
        .device_id("devB")
        .await
        .expect("activate malformed-package producer");
    let target = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (_, activation_result) = pull_into_result(&target, &storage, &ld)
        .await
        .expect("materialize device activations before publishing device commits");
    assert!(activation_result.held_positions.is_empty());

    let bad_seed = capture_bytes(
        &bad_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-bad', 'Bad', NULL, '0000000001000-0000-devB', '2026-01-01')",
        ],
    )
    .await;
    let bad_reference = storage
        .publish_changeset("devB", 1, &bad_seed, SCHEMA_VERSION)
        .await
        .expect("publish valid seed Store package");

    let good_source = open_test_db();
    let good_cs = capture_bytes(
        &good_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-good', 'Good', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
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
    let bad_reference = replace_store_package_with_malformed_bytes(&storage, bad_reference).await;
    let bad_stream_id = commit_stream_id(&bad_reference);

    let (updated, result) = pull_into_result(&target, &storage, &ld)
        .await
        .expect("a malformed Store package must not fail the whole pull");

    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n-good'").await);
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
        result.held_positions[0].reason,
        HeldStorePositionReason::InvalidChangeset(_)
    ));
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let db1 = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // Source: a note + a cover photo. The blob id is ≥4 chars so it forms the
    // `{ab}/{cd}` cache shard.
    capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('p1ab', 'n1', 'cover', 10, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"PHOTOBYTES"),
            ),
        ],
    )
    .await;

    let (_source_tmp, source_store_dir) = temp_store_dir();
    store_local(&source_store_dir, "p1ab", b"PHOTOBYTES").await;
    make_test_root_remote(&db1, &storage, &source_store_dir, "n1").await;
    let commit = crate::sync::store::StoreDatabase::new(&db1)
        .latest_local_store_position()
        .await
        .expect("read blob commit position")
        .expect("blob write has a Store commit");

    // Destination pulls. A `CacheEager` photo lands in the store dir's evictable
    // locator-keyed cache on pull.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_t, ld) = temp_store_dir();
    let (_updated, result) = pull_into(&db2, &storage, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let blob_row = row_blob_ref(&db2, "note_photos", "p1ab").await;
    let downloaded = std::fs::read(exact_cache_path(&ld, &blob_row)).expect("downloaded photo");
    assert_eq!(downloaded, b"PHOTOBYTES");
    let stored = blob_row
        .stored()
        .expect("pulled blob row carries exact storage")
        .clone();
    let remote = stored_remote_object(&db2, stored.object()).await;
    let stream_id = commit_stream_id(&commit);
    let sequence = commit.coord.sequence() as i64;
    let input_hash: crate::sync::store_commit::ObjectHash = db2
        .call(move |conn| {
            let hash: String = conn
                .query_row(
                    "SELECT input_hash FROM retained_merge_materializations \
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![stream_id, sequence],
                    |row| row.get(0),
                )
                .map_err(crate::database::DbError::from)?;
            hash.parse().map_err(|error| {
                crate::database::DbError::Message(format!(
                    "parse retained blob input hash: {error}"
                ))
            })
        })
        .await
        .expect("load retained blob input hash");
    assert!(matches!(
        remote,
        crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.state,
                crate::sync::remote_object::OwnedObjectState::UploadedVerified {
                    ownership
                } if ownership.activated.contains(
                    &crate::sync::remote_object::SharedObjectOwner::StoreCommit(commit.clone())
                ) && ownership.activated.contains(
                    &crate::sync::remote_object::SharedObjectOwner::RetainedReplay(
                        crate::sync::remote_object::RetainedReplayOwner::Commit {
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
    let db1 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // Source: a shared note + an audio row, declared user-provided · CacheLazy.
    capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;
    let (source_tmp, ld1) = temp_store_dir();
    let source = source_tmp.path().join("audio1.flac");
    std::fs::write(&source, b"AUDIO-PAYLOAD").expect("write exact external audio fixture");
    let reference = db1
        .row_blob_ref("note_photos", "audio1")
        .await
        .expect("load exact external audio row");
    db1.call(move |conn| {
        crate::database::Database::register_external_blob_on(conn, &reference, &source)
    })
    .await
    .expect("register exact external audio fixture");
    make_test_root_remote(&db1, &storage, &ld1, "n1").await;
    let audio_blob = db1
        .row_blob_ref("note_photos", "audio1")
        .await
        .expect("load exact published audio row")
        .stored()
        .cloned()
        .expect("published audio row carries exact blob authority");

    assert_eq!(
        read_exact_blob(&storage, &audio_blob).await,
        b"AUDIO-PAYLOAD",
        "the transition publishes the exact user-provided bytes",
    );

    // A failed exact read rejects the row and leaves the commit available for retry.
    let db2 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let (_t, ld) = temp_store_dir();
    let membership = storage
        .open_into(&db2)
        .await
        .expect("open exact Store before failed lazy verification");
    let failing = FaultingStorage::blob(&storage.storage);
    let error = crate::sync::store::pull_store_commits(
        &store_database(&db2),
        db2.synced_tables(),
        &failing,
        storage.root.store_root_hash,
        &ld,
        &membership,
        None,
    )
    .await
    .expect_err("lazy blob verification failure rejects the Store commit");
    assert!(
        matches!(&error, crate::sync::store::StorePullError::BlobDownloads(_)),
        "unexpected lazy verification error: {error:?}"
    );
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);

    // The same commit applies once the exact blob can be opened and verified.
    let (updated, result) = pull_into(&db2, &storage, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.values().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithAudio",
        "the row carrying the CacheLazy blob still reaches the peer",
    );
    // Verification used an unpublished temporary file, so the plaintext remains
    // absent from both cache locations until an application read requests it.
    let reference = row_blob_ref(&db2, "note_photos", "audio1").await;
    assert!(
        !exact_pinned_path(&ld, &reference).exists()
            && !exact_cache_path(&ld, &reference).exists(),
        "a CacheLazy blob must NOT be downloaded on pull — it stays in the cloud for on-demand fetch",
    );
}

fn open_scoped_circle_test_db() -> crate::database::Database {
    open_test_db_schema(
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::IndependentUuid)
                .scoped_by("audience"),
            SyncedTable::new(
                "comments",
                crate::sync::session::RowIdentity::IndependentUuid,
            ),
        ],
        vec![crate::migration::Migration::sql(
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
    let source = open_scoped_circle_test_db();
    let storage = create_store(&source, owner.clone()).await;
    let _source_membership = storage
        .open_into(&source)
        .await
        .expect("open scoped source Store");
    let device_id = source
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read scoped source device")
        .expect("scoped source device exists");
    let circle_id = storage
        .loaded_store(&source)
        .await
        .expect("load scoped source Store")
        .create_circle(&device_id, "0000000001000-0000-owner", "Readers", &owner)
        .await
        .expect("create exact Circle");
    let note_id = "01890a5d-ac96-774b-bcce-b302099c3f74";
    let comment_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    let sql = format!(
        "INSERT INTO notes VALUES ('{note_id}', '{circle_id}', 'private', '0000000002000-0000-owner');
         INSERT INTO comments VALUES ('{comment_id}', '{note_id}', 'child', '0000000002001-0000-owner');"
    );
    let tables = source.synced_tables().to_vec();
    let gates = source.gates();
    let blob_decls = source.blob_decls();
    let write_id = source.new_write_id();
    source
        .call(move |conn| {
            let routing = EncryptionService::from_key([42; 32]);
            crate::sync::store::StoreDatabase::run_store_write_transaction_on(
                conn,
                &tables,
                &gates,
                &blob_decls,
                Some(&routing),
                write_id,
                |tx| {
                    tx.execute_batch(&sql)
                        .map_err(crate::database::DbError::from)
                },
            )
        })
        .await
        .expect("commit scoped host transaction");
    let (_source_temp, source_dir) = temp_store_dir();
    storage
        .publish_pending(&source, &source_dir)
        .await
        .expect("publish Circle-scoped rows");

    let target = open_scoped_circle_test_db();
    let target_membership = storage
        .open_into(&target)
        .await
        .expect("open scoped target Store");
    let (_target_temp, target_dir) = temp_store_dir();
    let result = crate::sync::store::pull_store_commits(
        &store_database(&target),
        target.synced_tables(),
        &storage.storage,
        storage.root.store_root_hash,
        &target_dir,
        &target_membership,
        Some(&owner),
    )
    .await
    .expect("pull Circle-scoped rows");

    assert!(result.changesets_applied >= 1);
    assert!(
        row_exists(
            &target,
            &format!("SELECT 1 FROM notes WHERE id = '{note_id}'")
        )
        .await,
        "Circle root was not applied: {:?}",
        result.held_positions
    );
    assert!(
        row_exists(
            &target,
            &format!("SELECT 1 FROM comments WHERE id = '{comment_id}'")
        )
        .await
    );
    let (routes, mirrors): (i64, i64) = target
        .call(move |conn| {
            let routes = conn.query_row("SELECT COUNT(*) FROM _coven_row_routes", [], |row| {
                row.get(0)
            })?;
            let mirrors = conn.query_row(
                "SELECT COUNT(*) FROM _coven_audience WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )?;
            Ok((routes, mirrors))
        })
        .await
        .expect("read pulled routing state");
    assert_eq!((routes, mirrors), (2, 2));
    assert!(
        result
            .row_changes
            .iter()
            .all(|change| !crate::sync::gate::is_routing_table(&change.table)),
        "host-visible row changes must not expose Coven routing tables"
    );
    assert!(
        stored_remote_objects(&target)
            .await
            .iter()
            .any(|remote| is_external_circle_package(remote, true)),
        "pulled Merge Circle package must carry external exact and replay ownership"
    );
}

#[tokio::test]
async fn merge_pull_applies_a_circle_activation_before_its_reversed_order_successor() {
    let owner = UserKeypair::generate();
    let observer = open_scoped_circle_test_db();
    let storage = TestStore::create(&observer, "circle-activation-order", owner.clone())
        .await
        .expect("create Store for Circle activation ordering");
    storage.home.sort_listings();
    let first = open_scoped_circle_test_db();
    let second = open_scoped_circle_test_db();
    let receiver = open_scoped_circle_test_db();
    for participant in [&first, &second, &receiver] {
        install_active_device_fixture(
            &storage,
            &observer,
            participant,
            &owner,
            "2026-07-19T00:00:00Z",
        )
        .await
        .expect("install active Circle test device");
    }
    for participant in [&first, &second, &receiver] {
        let (_temp, store_dir) = temp_store_dir();
        pull_into(participant, &storage, &store_dir).await;
    }
    let first_stream = local_announcement_stream(&first).await;
    let second_stream = local_announcement_stream(&second).await;
    let (activator, successor) = if first_stream > second_stream {
        (&first, &second)
    } else {
        (&second, &first)
    };
    assert!(
        local_announcement_stream(successor).await < local_announcement_stream(activator).await
    );
    let activator_device = activator
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read Circle activator device")
        .expect("Circle activator device exists");
    let circle_id = storage
        .loaded_store(activator)
        .await
        .expect("load Circle activator Store")
        .create_circle(
            &activator_device,
            "0000000001000-0000-owner",
            "Readers",
            &owner,
        )
        .await
        .expect("create Circle on the later-sorted stream");

    let (_successor_temp, successor_dir) = temp_store_dir();
    let successor_membership = storage
        .open_into(successor)
        .await
        .expect("open Store before pulling Circle activation");
    crate::sync::store::pull_store_commits(
        &store_database(successor),
        successor.synced_tables(),
        &storage.storage,
        storage.root.store_root_hash,
        &successor_dir,
        &successor_membership,
        Some(&owner),
    )
    .await
    .expect("pull Circle activation before authoring successor");
    let successor_device = successor
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read Circle successor device")
        .expect("Circle successor device exists");
    storage
        .loaded_store(successor)
        .await
        .expect("load Circle successor Store")
        .rename_circle(
            &successor_device,
            "0000000002000-0000-owner",
            circle_id,
            "Renamed readers",
            &owner,
        )
        .await
        .expect("publish Circle successor from the earlier-sorted stream");

    let (_receiver_temp, receiver_dir) = temp_store_dir();
    let receiver_membership = storage
        .open_into(&receiver)
        .await
        .expect("open Store before ordered Circle pull");
    let result = crate::sync::store::pull_store_commits(
        &store_database(&receiver),
        receiver.synced_tables(),
        &storage.storage,
        storage.root.store_root_hash,
        &receiver_dir,
        &receiver_membership,
        Some(&owner),
    )
    .await
    .expect("pull Circle activation and successor in one pass");

    assert!(result.held_positions.is_empty(), "{result:?}");
    assert_eq!(
        store_database(&receiver)
            .get_circles(&crate::keys::public_key_hex(&owner))
            .await
            .expect("read ordered Circle result")
            .into_iter()
            .find(|circle| circle.id == circle_id)
            .expect("Circle exists after ordered pull")
            .name,
        "Renamed readers"
    );
}

#[tokio::test]
async fn local_user_provided_blob_does_not_block_changeset_publish() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = create_store(&db, UserKeypair::generate()).await;
    let (tmp, ld) = temp_store_dir();
    let external = tmp.path().join("audio.flac");
    std::fs::write(&external, b"local audio").expect("write external file");
    exec(
        &db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', 11, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            crate::blob::content_hash(b"local audio"),
        ),
    )
    .await;
    let reference = db
        .row_blob_ref("note_photos", "audio1")
        .await
        .expect("load exact external row blob reference");
    let external = external.clone();
    db.call(move |conn| {
        crate::database::Database::register_external_blob_on(conn, &reference, &external)
    })
    .await
    .expect("register exact external blob reference");
    let outgoing = capture_bytes(
        &db,
        &["UPDATE notes SET title = 'Changed', \
           _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
    )
    .await;

    let result = sync_for_test(
        &db,
        &test_synced_tables_with_blob(BlobDecl::new(
            "audio",
            Provenance::UserProvided,
            CacheFill::CacheLazy,
        )),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &storage.protocol_founder_keypair(),
        &ld,
    )
    .await
    .expect("a Local blob does not require remote object authority");
    assert!(result.is_some(), "the changeset publishes a Store commit");
    assert!(
        crate::sync::store::StoreDatabase::new(&db)
            .latest_local_store_position()
            .await
            .expect("read exact local Store position")
            .is_some(),
        "the publish advances the local Store position",
    );
}

#[tokio::test]
async fn missing_remote_user_provided_blob_aborts_before_changeset_publish() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = create_store(&db, UserKeypair::generate()).await;
    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;
    let (_tmp, ld) = temp_store_dir();

    let result = sync_for_test(
        &db,
        &test_synced_tables_with_blob(BlobDecl::new(
            "audio",
            Provenance::UserProvided,
            CacheFill::CacheLazy,
        )),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &storage.protocol_founder_keypair(),
        &ld,
    )
    .await;
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("missing remote user-provided blob must abort publish"),
    };

    assert!(
        err.contains("audio/audio1") && err.contains("absent"),
        "the error must name the absent remote blob: {err}",
    );
    assert!(
        crate::sync::store::StoreDatabase::new(&db)
            .latest_local_store_position()
            .await
            .expect("read exact local Store position")
            .is_none(),
        "failed publish created no Store commit",
    );
}

#[tokio::test]
async fn present_remote_user_provided_blob_can_publish_changeset() {
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = create_store(&db, UserKeypair::generate()).await;
    capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;
    let (tmp, store_dir) = temp_store_dir();
    let source = tmp.path().join("audio1.flac");
    std::fs::write(&source, b"AUDIO-PAYLOAD").expect("write exact external audio fixture");
    let reference = db
        .row_blob_ref("note_photos", "audio1")
        .await
        .expect("load exact external row blob reference");
    db.call(move |conn| {
        crate::database::Database::register_external_blob_on(conn, &reference, &source)
    })
    .await
    .expect("register exact external audio fixture");
    make_test_root_remote(&db, &storage, &store_dir, "n1").await;
    let result = crate::sync::store::StoreDatabase::new(&db)
        .latest_local_store_position()
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
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let storage = create_store(&db, UserKeypair::generate()).await;
    exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01');
         INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    // The rows above are seed (raw `exec`, unjournaled), so the captured changeset
    // is just the DELETE under test.
    let outgoing = capture_bytes(&db, &["DELETE FROM note_photos WHERE id = 'audio1'"]).await;
    let (_tmp, ld) = temp_store_dir();

    let result = sync_for_test(
        &db,
        &test_synced_tables_with_blob(BlobDecl::new(
            "audio",
            Provenance::UserProvided,
            CacheFill::CacheLazy,
        )),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &storage.protocol_founder_keypair(),
        &ld,
    )
    .await
    .expect("delete does not require the removed blob to exist remotely");
    assert!(result.is_some(), "the delete publishes a Store commit");

    assert_eq!(
        result
            .expect("published exact Store commit")
            .coord
            .sequence(),
        1,
        "delete publishes even when the removed blob is absent remotely",
    );
}

/// A changeset that references absent blob bytes becomes durably blocked instead
/// of publishing a row that every puller would fail to materialize.
#[tokio::test]
async fn sync_aborts_when_a_referenced_blob_file_is_missing() {
    let db1 = open_test_db_with_blob(photo_decl());
    let keypair = UserKeypair::generate();
    let storage = create_store(&db1, keypair.clone()).await;

    // A shared note + a host-provided cover row, but the cover is deliberately never
    // stored in the local store, so the inline push finds nothing in either the local
    // store or the cache.
    let missing_blob = format!(
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('p1ab', 'n1', 'cover', 7, '{}', \
                 '0000000001000-0000-dev1', '2026-01-01')",
        crate::blob::content_hash(b"missing"),
    );
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &missing_blob,
        ],
    )
    .await;

    let (_t1, ld1) = temp_store_dir();
    let result = sync_for_test(
        &db1,
        &test_synced_tables_with_blob(photo_decl()),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld1,
    )
    .await;
    let err = result.expect_err("missing blob blocks Store publication");
    assert!(
        err.contains("outbound blob photos/p1ab is absent from storage"),
        "an absent blob must abort Store publication, got {err:?}",
    );
    let pending = db1.pending_writes().await.expect("read blocked write");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].status,
        crate::WriteStatus::Blocked(crate::WriteBlock::MissingBlob {
            namespace: "photos".to_string(),
            id: "p1ab".to_string(),
        }),
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
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );

    let bytes = b"COVER-BYTES";
    let db = open_test_db_with_blob(readable_photo_decl());
    let tables = test_synced_tables_with_blob(readable_photo_decl());
    let (_t, ld) = temp_store_dir();
    store_local(&ld, "p1cover", bytes).await;
    let rows = [
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        &format!(
            "INSERT INTO note_photos \
             (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
             VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', \
             '0000000001000-0000-dev1', '2026-01-01')",
            bytes.len(),
            crate::blob::content_hash(bytes),
        ),
    ];
    let outgoing = capture_bytes(&db, &rows).await;
    push_cycle(&db, &tables, &storage, outgoing.clone(), 0, &keypair, &ld).await;
    let cover_key = row_blob_object_key(&db, "note_photos", "p1cover").await;
    assert_eq!(
        home.get(&cover_key).as_deref(),
        Some(bytes.as_slice()),
        "the first push uploads the cover",
    );

    // This device now holds no copy of the blob at all: the push moved the local-store
    // copy into the cache, and the cache copy is then evicted.
    local_files::drop_blob(&ld, "photos", "p1cover")
        .await
        .expect("drop any local-store copy");
    let cached = exact_cache_path(&ld, &row_blob_ref(&db, "note_photos", "p1cover").await);
    if cached.exists() {
        std::fs::remove_file(&cached).expect("evict the cached copy");
    }

    // The row is re-emitted. The blob has no local bytes to upload — and needs none.
    let result = sync_for_test(
        &db,
        &tables,
        outgoing,
        1,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld,
    )
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
/// path, end to end over a real `CloudSyncStorage` in `BlobPathScheme::Plain`.
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
    // Driven through the real `service::sync` + `push_changeset` so the
    // production blob-upload path keys the blob from its `cloud_path`.
    let plaintext = b"COVERART";

    let db1 = open_test_db_with_blob(readable_photo_decl());
    let (_t1, ld1) = temp_store_dir();
    // The cover's readable key lives in the row's `cloud_path` column.
    // The host stages the cover into the cache before the inline push reads it.
    store_local(&ld1, "p1cover", plaintext).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', 8, '{}', 'n1/cover-p1cover.jpg', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(plaintext),
            ),
        ],
    )
    .await;

    let result = sync_for_test(
        &db1,
        &test_synced_tables_with_blob(readable_photo_decl()),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld1,
    )
    .await
    .expect("sync");
    assert!(
        result.is_some(),
        "the readable blob row publishes a Store commit"
    );

    // The blob lands at an immutable version below its readable path.
    let blob_key = row_blob_object_key(&db1, "note_photos", "p1cover").await;
    assert!(
        blob_key.starts_with("photos/readable/n1/cover-p1cover.jpg/.coven-versions/"),
        "the exact object stays grouped below its readable path: {blob_key}",
    );
    assert!(
        storage
            .cloud_home()
            .exists(&blob_key)
            .await
            .expect("exists at exact readable version"),
        "the exact readable blob version exists",
    );
    assert!(
        !storage
            .cloud_home()
            .exists("photos/n1/cover-p1cover.jpg")
            .await
            .expect("check obsolete mutable readable key"),
        "no mutable object occupies the bare readable path",
    );

    // Device B: a fresh DB and its own store dir, same cloud + plain scheme,
    // pulls and downloads the cover from the readable key.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld) = temp_store_dir();
    let (_updated, result) = pull_exact_store_into(&db2, &db1, &storage, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(exact_cache_path(
        &ld,
        &row_blob_ref(&db2, "note_photos", "p1cover").await,
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
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );

    let bytes = b"COVER-BYTES";
    let db = open_test_db_with_blob(readable_photo_decl());
    let tables = test_synced_tables_with_blob(readable_photo_decl());
    let (_t, ld) = temp_store_dir();
    store_local(&ld, "p1cover", bytes).await;
    // `n1/cover.jpg` names no blob: it would key p1cover today and its replacement
    // tomorrow at one and the same cloud object.
    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover.jpg', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                bytes.len(),
                crate::blob::content_hash(bytes),
            ),
        ],
    )
    .await;

    let err = sync_for_test(
        &db,
        &tables,
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld,
    )
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
    tokio::spawn(run_plain_scheme_distinct_blobs_write_objects_at_their_own_keys())
        .await
        .expect("distinct browsable blob orchestration task");
}

async fn run_plain_scheme_distinct_blobs_write_objects_at_their_own_keys() {
    // A browsable home: readable keys, objects stored in the clear (the two are one
    // choice), so the test reads the cloud object back as plaintext.
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1 = open_test_db_with_blob(readable_photo_decl());
    let tables = test_synced_tables_with_blob(readable_photo_decl());
    let (_t1, ld1) = temp_store_dir();
    store_local(&ld1, "p1cover", old_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                old_bytes.len(),
                crate::blob::content_hash(old_bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 0, &keypair, &ld1).await;
    let old_key = row_blob_object_key(&db1, "note_photos", "p1cover").await;
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the first push puts the cover at the key its path names",
    );

    // Device B takes the cover before the replacement, so it is a peer holding the
    // replaced blob when the new one arrives.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    pull_exact_store_into(&db2, &db1, &storage, &ld2).await;

    // Add another blob whose readable path names it.
    store_local(&ld1, "p2cover", new_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[&format!(
            "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p2cover', 'n1', 'cover', {}, '{}', 'n1/cover-p2cover.jpg', \
                 '0000000002000-0000-dev1', '2026-01-01')",
            new_bytes.len(),
            crate::blob::content_hash(new_bytes),
        )],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 1, &keypair, &ld1).await;
    let new_key = row_blob_object_key(&db1, "note_photos", "p2cover").await;

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
    let (_updated, result) = pull_exact_store_into(&db2, &db1, &storage, &ld2).await;

    assert!(
        !result.asset_downloads_failed,
        "device B downloads blobs matching their row hashes",
    );
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(exact_cache_path(
        &ld2,
        &row_blob_ref(&db2, "note_photos", "p2cover").await,
    ))
    .expect("device B cached the replacement cover");
    assert_eq!(
        cached,
        new_bytes.as_slice(),
        "device B serves the second blob's bytes",
    );
}

/// Sequential replacements write separate immutable objects.
#[tokio::test]
async fn plain_scheme_two_replacements_write_two_objects() {
    tokio::spawn(run_plain_scheme_two_replacements_write_two_objects())
        .await
        .expect("sequential browsable blob replacement orchestration task");
}

async fn run_plain_scheme_two_replacements_write_two_objects() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let tables = test_synced_tables_with_blob(replaceable_photo_decl());

    let original = b"ORIGINAL-COVER";
    let from_a = b"COVER-FROM-A";
    let from_b = b"COVER-FROM-B-BYTES";

    // The source publishes the original cover.
    let db_a = open_test_db_with_blob(replaceable_photo_decl());
    let (_ta, ld_a) = temp_store_dir();
    store_local(&ld_a, "p0cover", original).await;
    let outgoing = capture_bytes(
        &db_a,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'cover', {}, '{}', 'n1/cover-p0cover.jpg', 'p0cover', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                original.len(),
                crate::blob::content_hash(original),
            ),
        ],
    )
    .await;
    push_cycle(&db_a, &tables, &storage, outgoing, 0, &keypair, &ld_a).await;

    // Each replacement uses a fresh blob id and readable path.
    store_local(&ld_a, "pAcover", from_a).await;
    let outgoing_a = capture_bytes(
        &db_a,
        &[&format!(
            "UPDATE note_photos SET blob_id = 'pAcover', cloud_path = 'n1/cover-pAcover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
            from_a.len(),
            crate::blob::content_hash(from_a),
        )],
    )
    .await;
    push_cycle(&db_a, &tables, &storage, outgoing_a, 1, &keypair, &ld_a).await;
    let from_a_key = row_blob_object_key(&db_a, "note_photos", "ph1").await;

    store_local(&ld_a, "pBcover", from_b).await;
    let outgoing_b = capture_bytes(
        &db_a,
        &[&format!(
            "UPDATE note_photos SET blob_id = 'pBcover', cloud_path = 'n1/cover-pBcover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000003000-0000-dev2' WHERE id = 'ph1'",
            from_b.len(),
            crate::blob::content_hash(from_b),
        )],
    )
    .await;
    push_cycle(&db_a, &tables, &storage, outgoing_b, 2, &keypair, &ld_a).await;
    let from_b_key = row_blob_object_key(&db_a, "note_photos", "ph1").await;

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
    let db_c = open_test_db_with_blob(replaceable_photo_decl());
    let (_tc, ld_c) = temp_store_dir();
    let (_updated, result) = pull_exact_store_into(&db_c, &db_a, &storage, &ld_c).await;
    assert!(
        !result.asset_downloads_failed,
        "every row the third device applies names an object that holds its bytes",
    );
    assert_eq!(
        result.changesets_applied, 3,
        "the original and both replacements all apply",
    );

    let winner: String = db_c
        .call(|conn| {
            conn.query_row(
                "SELECT blob_id FROM note_photos WHERE id = 'ph1'",
                [],
                |r| r.get::<_, String>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("the cover row");
    assert_eq!(winner, "pBcover");
    let expected = from_b.as_slice();
    let cached = std::fs::read(exact_cache_path(
        &ld_c,
        &row_blob_ref(&db_c, "note_photos", "ph1").await,
    ))
    .expect("the third device cached the cover its row names");
    assert_eq!(
        cached, expected,
        "the latest row names the second replacement's bytes",
    );
}

/// A device replaying two blob-bearing changesets can fetch each immutable object.
#[tokio::test]
async fn plain_scheme_a_laggard_finds_blobs_from_each_changeset() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let tables = test_synced_tables_with_blob(readable_photo_decl());

    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1 = open_test_db_with_blob(readable_photo_decl());
    let (_t1, ld1) = temp_store_dir();
    store_local(&ld1, "p1cover", old_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                old_bytes.len(),
                crate::blob::content_hash(old_bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 0, &keypair, &ld1).await;

    // Another blob is published while the laggard is away.
    store_local(&ld1, "p2cover", new_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[&format!(
            "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p2cover', 'n1', 'cover', {}, '{}', 'n1/cover-p2cover.jpg', \
                 '0000000002000-0000-dev1', '2026-01-01')",
            new_bytes.len(),
            crate::blob::content_hash(new_bytes),
        )],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 1, &keypair, &ld1).await;

    // The laggard pulls from zero: it applies the pre-replacement changeset first, whose
    // row names the replaced blob. Its bytes are still at their own key.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    let (_positions, result) = pull_exact_store_into(&db2, &db1, &storage, &ld2).await;

    assert!(
        !result.asset_downloads_failed,
        "each changeset finds the exact blob object it names",
    );
    assert_eq!(result.changesets_applied, 2, "both changesets apply",);
    let cached = std::fs::read(exact_cache_path(
        &ld2,
        &row_blob_ref(&db2, "note_photos", "p2cover").await,
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
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    // A readable name with no blob id anywhere in it.
    let bytes = b"AUDIO-BYTES";
    let db1 = open_test_db_with_blob(write_once_photo_decl());
    let tables = test_synced_tables_with_blob(write_once_photo_decl());
    let (_t1, ld1) = temp_store_dir();
    store_local(&ld1, "f1audio", bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'audio', {}, '{}', 'n1/Sonata No. 3.flac', 'f1audio', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                bytes.len(),
                crate::blob::content_hash(bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 0, &keypair, &ld1).await;
    let audio_key = row_blob_object_key(&db1, "note_photos", "ph1").await;

    assert_eq!(
        home.get(&audio_key).as_deref(),
        Some(bytes.as_slice()),
        "the blob lands at the consumer's own readable name, with no blob id in it",
    );

    // A peer pulls it off that readable key and verifies it against the row's hash.
    let db2 = open_test_db_with_blob(write_once_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    let (_positions, result) = pull_exact_store_into(&db2, &db1, &storage, &ld2).await;
    assert!(!result.asset_downloads_failed);
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(exact_cache_path(
        &ld2,
        &row_blob_ref(&db2, "note_photos", "ph1").await,
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
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let first = b"FIRST-AUDIO";
    let second = b"SECOND-AUDIO-BYTES";

    let db = open_test_db_with_blob(write_once_photo_decl());
    let tables = test_synced_tables_with_blob(write_once_photo_decl());
    let (_t, ld) = temp_store_dir();
    store_local(&ld, "f1audio", first).await;
    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'audio', {}, '{}', 'n1/Sonata No. 3.flac', 'f1audio', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                first.len(),
                crate::blob::content_hash(first),
            ),
        ],
    )
    .await;
    push_cycle(&db, &tables, &storage, outgoing, 0, &keypair, &ld).await;
    let audio_key = row_blob_object_key(&db, "note_photos", "ph1").await;

    // Repoint the write-once row at a second blob — the move that would rewrite the object
    // the first blob occupies.
    store_local(&ld, "f2audio", second).await;
    let outgoing = capture_bytes(
        &db,
        &[&format!(
            "UPDATE note_photos SET blob_id = 'f2audio', size = {}, hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
            second.len(),
            crate::blob::content_hash(second),
        )],
    )
    .await;
    let err = sync_for_test(
        &db,
        &tables,
        outgoing,
        1,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld,
    )
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
    tokio::spawn(run_plain_scheme_repointing_a_row_moves_its_blob_to_a_new_key())
        .await
        .expect("browsable blob repointing orchestration task");
}

async fn run_plain_scheme_repointing_a_row_moves_its_blob_to_a_new_key() {
    // A browsable home: readable keys, objects stored in the clear (the two are one
    // choice), so the test reads the cloud object back as plaintext.
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1 = open_test_db_with_blob(replaceable_photo_decl());
    let tables = test_synced_tables_with_blob(replaceable_photo_decl());
    let (_t1, ld1) = temp_store_dir();
    store_local(&ld1, "p1cover", old_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', 'p1cover', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                old_bytes.len(),
                crate::blob::content_hash(old_bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 0, &keypair, &ld1).await;
    let old_key = row_blob_object_key(&db1, "note_photos", "ph1").await;
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the first push puts the cover at the key its path names",
    );

    // Device B takes the cover before the replacement, so it is a peer holding the
    // replaced blob when the new one arrives.
    let db2 = open_test_db_with_blob(replaceable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    pull_exact_store_into(&db2, &db1, &storage, &ld2).await;
    let old_cache_path = exact_cache_path(&ld2, &row_blob_ref(&db2, "note_photos", "ph1").await);

    // Repoint the row at a new blob: same primary key, new blob id, and the cloud path
    // moves with it because it names the blob. The replaced blob's local copy goes away.
    store_local(&ld1, "p2cover", new_bytes).await;
    local_files::drop_blob(&ld1, "photos", "p1cover")
        .await
        .expect("drop the replaced blob's local copy");
    let outgoing = capture_bytes(
        &db1,
        &[&format!(
            "UPDATE note_photos SET blob_id = 'p2cover', cloud_path = 'n1/cover-p2cover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
            new_bytes.len(),
            crate::blob::content_hash(new_bytes),
        )],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 1, &keypair, &ld1).await;
    let new_key = row_blob_object_key(&db1, "note_photos", "ph1").await;

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
    let (_updated, result) = pull_exact_store_into(&db2, &db1, &storage, &ld2).await;

    assert!(
        !result.asset_downloads_failed,
        "device B must download a cover matching the row's hash",
    );
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(exact_cache_path(
        &ld2,
        &row_blob_ref(&db2, "note_photos", "ph1").await,
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
}

/// Repointing a row at a new blob while HOLDING its cloud path is the shape the rule
/// exists to refuse, and it is the one a changeset cannot show on its own: an UPDATE
/// reports only the columns whose values changed, so it carries the new blob id and not
/// the (unchanged) path. coven reads the path from the row that owns the blob — which is
/// where it catches that the path names the blob the row no longer points at.
#[tokio::test]
async fn plain_scheme_repointing_a_row_without_moving_its_cloud_path_is_refused() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1 = open_test_db_with_blob(replaceable_photo_decl());
    let tables = test_synced_tables_with_blob(replaceable_photo_decl());
    let (_t1, ld1) = temp_store_dir();
    store_local(&ld1, "p1cover", old_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithCover', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, blob_id, _updated_at, created_at) \
                 VALUES ('ph1', 'n1', 'cover', {}, '{}', 'n1/cover-p1cover.jpg', 'p1cover', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                old_bytes.len(),
                crate::blob::content_hash(old_bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 0, &keypair, &ld1).await;
    let old_key = row_blob_object_key(&db1, "note_photos", "ph1").await;

    // The repointing leaves `cloud_path` naming the blob it replaced, so the new blob
    // would be keyed at the old blob's object.
    store_local(&ld1, "p2cover", new_bytes).await;
    let outgoing = capture_bytes(
        &db1,
        &[&format!(
            "UPDATE note_photos SET blob_id = 'p2cover', size = {}, hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'ph1'",
            new_bytes.len(),
            crate::blob::content_hash(new_bytes),
        )],
    )
    .await;
    let err = sync_for_test(
        &db1,
        &tables,
        outgoing,
        1,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld1,
    )
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

/// Push one cycle's captured changeset the way the sync loop does: `service::sync`
/// prepares (and uploads the host-provided blobs of) the gated changeset, then
/// publishes the resulting immutable Store objects, as `device`.
async fn push_cycle_as<S: TestStoreStorage>(
    db: &crate::database::Database,
    tables: &[SyncedTable],
    storage: &S,
    outgoing: Vec<u8>,
    local_seq: u64,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) {
    let result = sync_for_test(
        db,
        tables,
        outgoing,
        local_seq,
        storage,
        "2026-01-01T00:00:00Z",
        "",
        keypair,
        store_dir,
    )
    .await
    .expect("sync");
    assert!(result.is_some(), "the captured rows publish a Store commit");
}

/// [`push_cycle_as`] for tests backed directly by [`CloudSyncStorage`].
async fn push_cycle(
    db: &crate::database::Database,
    tables: &[SyncedTable],
    storage: &CloudSyncStorage,
    outgoing: Vec<u8>,
    local_seq: u64,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) {
    push_cycle_as(db, tables, storage, outgoing, local_seq, keypair, store_dir).await;
}

/// Full encrypted blob round-trip through `CloudSyncStorage` (encrypted) over a
/// shared `CloudHome`. Device A publishes a note plus its cover photo via the real
/// `service::sync`; the blob lands ciphertext at rest. Device B — a fresh DB
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
        BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
            .with_scope(crate::blob::BlobScope::Derived("covers".to_string()))
    };

    let db1 = open_test_db_with_blob(decl());
    let (_t1, ld1) = temp_store_dir();
    // The host stages the cover into the cache before the inline push reads it.
    store_local(&ld1, "p1cover", plaintext).await;
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('p1cover', 'n1', 'cover', 15, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(plaintext),
            ),
        ],
    )
    .await;

    let result = sync_for_test(
        &db1,
        &test_synced_tables_with_blob(decl()),
        outgoing,
        0,
        &storage,
        "2026-01-01T00:00:00Z",
        "",
        &keypair,
        &ld1,
    )
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
        .cloud_home()
        .read(blob_key)
        .await
        .expect("blob present in cloud");
    assert_ne!(
        at_rest, plaintext,
        "blob must be encrypted at rest in the cloud"
    );

    // Device B: a fresh DB and its own store dir, same cloud + key + declaration.
    let db2 = open_test_db_with_blob(decl());
    let (_t, ld) = temp_store_dir();
    let (updated, result) = pull_exact_store_into(&db2, &db1, &storage, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.values().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithPhoto"
    );
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(exact_cache_path(
        &ld,
        &row_blob_ref(&db2, "note_photos", "p1cover").await,
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
        uploader.author_pubkey,
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
    let db1 = open_test_db_with_user_and_host_blobs(eager_decl(), lazy_decl());
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // Both children host-provided, differing only in fill: the photo is CacheEager,
    // the cover CacheLazy. Both inherit the `notes` gate, so a shared note carries
    // both through the inline push in one cycle.
    let (_t1, ld1) = temp_store_dir();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithBlobs', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        &format!(
            "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
             VALUES ('peager01', 'n1', 'cover', 11, '{}', '0000000001000-0000-dev1', '2026-01-01')",
            crate::blob::content_hash(b"EAGER-BYTES"),
        ),
    )
    .await;
    exec(
        &db1,
        &format!(
            "INSERT INTO note_covers (id, note_id, size, hash, _updated_at, created_at) \
             VALUES ('clazy001', 'n1', 10, '{}', '0000000001001-0000-dev1', '2026-01-01')",
            crate::blob::content_hash(b"LAZY-BYTES"),
        ),
    )
    .await;
    // The host stores both blobs in the local store (their Local home) before the
    // inline push reads them to upload.
    local_files::store(&ld1, "photos", "peager01", b"EAGER-BYTES")
        .await
        .expect("store eager blob in local store");
    local_files::store(&ld1, "covers", "clazy001", b"LAZY-BYTES")
        .await
        .expect("store lazy blob in local store");
    make_test_root_remote(&db1, &storage, &ld1, "n1").await;

    // Both blobs reached the cloud — the inline push uploads regardless of fill.
    let eager = db1
        .row_blob_ref("note_photos", "peager01")
        .await
        .expect("load exact eager row blob reference");
    storage
        .verify_blob_object(eager.stored().expect("eager blob was published"))
        .await
        .expect("verify exact eager blob object");
    let lazy = db1
        .row_blob_ref("note_covers", "clazy001")
        .await
        .expect("load exact lazy row blob reference");
    storage
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
    let db1 = open_test_db_with_blob(photo_decl());
    let storage = create_store(&db1, UserKeypair::generate()).await;

    // Source dev1: a note + a CacheEager cover row, the cover present in the cloud.
    capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('pdel1234', 'n1', 'cover', {}, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                b"COVERBYTES".len(),
                crate::blob::content_hash(b"COVERBYTES"),
            ),
        ],
    )
    .await;
    let (_source_tmp, source_store_dir) = temp_store_dir();
    store_local(&source_store_dir, "pdel1234", b"COVERBYTES").await;
    make_test_root_remote(&db1, &storage, &source_store_dir, "n1").await;

    // dev2 pulls → the CacheEager cover lands in the evictable cache.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_t, ld) = temp_store_dir();
    pull_into(&db2, &storage, &ld).await;
    let deleted_reference = row_blob_ref(&db2, "note_photos", "pdel1234").await;
    let deleted_cache_path = exact_cache_path(&ld, &deleted_reference);
    let deleted_pinned_path = exact_pinned_path(&ld, &deleted_reference);
    assert!(
        deleted_cache_path.exists(),
        "the cover lands in the evictable cache after the first pull",
    );

    // The source makes the root Local again. Its gate retraction carries the child
    // DELETE through the real transition publication path.
    let (_cancel_tx, cancel) = tokio::sync::watch::channel(false);
    crate::blob::transition::make_local(
        &store_database(&db1),
        &storage.storage,
        &source_store_dir,
        &crate::sync::hlc::Hlc::new("delete-fixture".to_string()),
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
    let (_positions, result) = pull_into(&db2, &storage, &ld).await;
    assert_eq!(result.changesets_applied, 1, "the DELETE changeset applied");
    assert!(
        !deleted_pinned_path.exists() && !deleted_cache_path.exists(),
        "applying the blob-bearing DELETE drops the cache copies",
    );
}

#[tokio::test]
async fn local_blob_cleanup_intent_survives_restart_after_position_commit() {
    let cleanup_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy);
    let source = open_test_db_with_blob(cleanup_decl());
    let storage = create_store(&source, UserKeypair::generate()).await;
    capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('cleanup01', 'n1', 'cover', 7, '{}', \
                         '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"cleanup"),
            ),
        ],
    )
    .await;
    let (_source_tmp, source_store_dir) = temp_store_dir();
    store_local(&source_store_dir, "cleanup01", b"cleanup").await;
    make_test_root_remote(&source, &storage, &source_store_dir, "n1").await;

    let database_dir = tempfile::tempdir().expect("database temp dir");
    let database_path = database_dir.path().join("store.db");
    let target = open_blob_test_db_at(&database_path, cleanup_decl());
    let (_store_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, &store_dir).await;
    let deleted_locator_hash = target
        .row_blob_ref("note_photos", "cleanup01")
        .await
        .expect("load exact blob before deletion")
        .stored()
        .expect("pulled blob has exact storage")
        .locator()
        .locator_hash()
        .to_string();
    let deletion =
        capture_bytes(&source, &["DELETE FROM note_photos WHERE id = 'cleanup01'"]).await;
    publish_blob_changeset(&source, &storage, &source_store_dir, deletion, 1).await;
    if store_dir.storage_dir().exists() {
        std::fs::remove_dir_all(store_dir.storage_dir()).expect("remove storage directory");
    }
    let obstructing_file = store_dir.as_ref().join("storage");
    std::fs::write(&obstructing_file, b"not a directory").expect("obstruct cleanup paths");

    let error = pull_into_result(&target, &storage, &store_dir)
        .await
        .expect_err("post-commit filesystem cleanup failure fails the pull");
    assert!(error.to_string().contains("local blob cleanup"), "{error}");
    assert!(!row_exists(&target, "SELECT 1 FROM note_photos WHERE id = 'cleanup01'").await);
    let pending_before_restart = target
        .call(|conn| {
            let mut statement = conn
                .prepare("SELECT copy_identity FROM local_cleanup_intents ORDER BY copy_identity")
                .map_err(crate::database::DbError::from)?;
            let identities = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(crate::database::DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(crate::database::DbError::from)?;
            Ok(identities)
        })
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

    let restarted = open_blob_test_db_at(&database_path, cleanup_decl());
    let (_updated, second) = pull_into(&restarted, &storage, &store_dir).await;
    assert_eq!(second.changesets_applied, 0);
    assert!(!second.asset_downloads_failed);
    assert!(!second.local_blob_cleanup_pending);
    let pending_after_restart: i64 = restarted
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM local_cleanup_intents", [], |row| {
                row.get(0)
            })
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(pending_after_restart, 0);
}

#[tokio::test]
async fn host_write_cannot_make_a_blob_live_during_its_filesystem_cleanup() {
    let decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let target = open_test_db_with_blob(decl);
    let storage = std::sync::Arc::new(create_store(&target, UserKeypair::generate()).await);
    exec(
        &target,
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
        .call(|conn| {
            conn.execute(
                "INSERT INTO local_cleanup_intents (namespace, blob_id, copy_identity) \
                 VALUES ('photos', 'cleanup-race', 'local')",
                [],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();

    let (_tmp, store_dir) = temp_store_dir();
    store_local(&store_dir, "cleanup-race", b"old bytes").await;
    let (reached_filesystem, resume_cleanup) = target.arm_test_pause(
        crate::database::DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
            namespace: "photos".to_string(),
            blob_id: "cleanup-race".to_string(),
        },
    );
    let pull_db = target.clone();
    let pull_storage = storage.clone();
    let pull_store_dir = store_dir.clone();
    let cleanup =
        tokio::spawn(
            async move { pull_into(&pull_db, pull_storage.as_ref(), &pull_store_dir).await },
        );

    reached_filesystem.notified().await;
    let tables = target.synced_tables().to_vec();
    let update_tables = tables.clone();
    let insert_write_id = target.new_write_id();
    let update_write_id = target.new_write_id();
    let host_write = target
        .call(move |conn| {
            crate::sync::store::StoreDatabase::run_internal_store_write_transaction_on(
                conn,
                &tables,
                None,
                insert_write_id,
                |tx| {
                    tx.execute(
                        "INSERT INTO note_photos \
                         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                         VALUES ('new-row', 'n1', 'cover', 9, NULL, 'cleanup-race', \
                                 '0000000002000-0000-dev2', '2026-01-01')",
                        [],
                    )
                    .map(|_| ())
                    .map_err(crate::database::DbError::from)
                },
            )
        })
        .await;
    let host_update = target
        .call(move |conn| {
            crate::sync::store::StoreDatabase::run_internal_store_write_transaction_on(
                conn,
                &update_tables,
                None,
                update_write_id,
                |tx| {
                    tx.execute(
                        "UPDATE note_photos SET blob_id = 'cleanup-race', \
                         _updated_at = '0000000002001-0000-dev2' \
                         WHERE id = 'existing-row'",
                        [],
                    )
                    .map(|_| ())
                    .map_err(crate::database::DbError::from)
                },
            )
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
    assert!(!row_exists(&target, "SELECT 1 FROM note_photos WHERE id = 'new-row'").await);
    assert!(
        row_exists(
            &target,
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
    use crate::database::DatabaseTestPoint;

    let decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let target = open_test_db_with_blob(decl);
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Cleanup parent', NULL, \
                 '0000000001000-0000-dev2', '2026-01-01');",
    )
    .await;
    target
        .call(|conn| {
            conn.execute(
                "INSERT INTO local_cleanup_intents (namespace, blob_id, copy_identity) \
                 VALUES ('photos', 'shared-intent', 'local')",
                [],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();

    let (_tmp, store_dir) = temp_store_dir();
    store_local(&store_dir, "shared-intent", b"old bytes").await;
    let mut points = target.observe_test_points();
    let before_filesystem = DatabaseTestPoint::LocalBlobCleanupBeforeFilesystem {
        namespace: "photos".to_string(),
        blob_id: "shared-intent".to_string(),
    };
    let (first_reached_filesystem, resume_first) = target.arm_test_pause(before_filesystem.clone());

    let first_db = target.clone();
    let first_store_dir = store_dir.clone();
    let first = tokio::spawn(async move {
        crate::blob::local_cleanup::drain(&first_db, &first_store_dir).await
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

    let tables = target.synced_tables().to_vec();
    let write_id = target.new_write_id();
    let host_re_reference = target
        .call(move |conn| {
            crate::sync::store::StoreDatabase::run_internal_store_write_transaction_on(
                conn,
                &tables,
                None,
                write_id,
                |tx| {
                    tx.execute(
                        "INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                     VALUES ('blocked-row', 'n1', 'cover', 9, NULL, 'shared-intent', \
                             '0000000002000-0000-dev2', '2026-01-01')",
                        [],
                    )
                    .map(|_| ())
                    .map_err(crate::database::DbError::from)
                },
            )
        })
        .await;
    assert!(
        host_re_reference.is_err(),
        "the cleanup intent rejects a host row re-reference"
    );

    let second_db = target.clone();
    let second_store_dir = store_dir.clone();
    let second = tokio::spawn(async move {
        crate::blob::local_cleanup::drain(&second_db, &second_store_dir).await
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

    exec(
        &target,
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
         VALUES ('live-row', 'n1', 'cover', 9, NULL, 'shared-intent', \
                 '0000000003000-0000-dev2', '2026-01-01')",
    )
    .await;
    store_local(&store_dir, "shared-intent", b"recreated bytes").await;
    assert!(!crate::blob::local_cleanup::drain(&target, &store_dir)
        .await
        .unwrap());
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
    let db1 = open_test_db_with_blob(decl.clone());
    let storage = create_store(&db1, UserKeypair::generate()).await;

    capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'SharedBlob', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('photo-a', 'n1', 'cover', 12, '{h}', 'sharedblob', '0000000001000-0000-dev1', '2026-01-01')",
                h = crate::blob::content_hash(b"SHARED-BYTES"),
            ),
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('photo-b', 'n1', 'cover', 12, '{h}', 'sharedblob', '0000000001000-0000-dev1', '2026-01-01')",
                h = crate::blob::content_hash(b"SHARED-BYTES"),
            ),
        ],
    )
    .await;
    let (_source_tmp, source_store_dir) = temp_store_dir();
    store_local(&source_store_dir, "sharedblob", b"SHARED-BYTES").await;
    make_test_root_remote(&db1, &storage, &source_store_dir, "n1").await;

    let db2 = open_test_db_with_blob(decl);
    let (_tmp, ld) = temp_store_dir();
    let (_positions, result) = pull_into(&db2, &storage, &ld).await;
    assert_eq!(result.changesets_applied, 1);
    let shared_reference = row_blob_ref(&db2, "note_photos", "photo-b").await;
    let shared_cache_path = exact_cache_path(&ld, &shared_reference);
    assert!(
        shared_cache_path.exists(),
        "the shared CacheEager blob lands in the cache",
    );

    store_local(&source_store_dir, "newblob", b"NEW-BYTES").await;
    let cs2 = capture_bytes(
        &db1,
        &[&format!(
            "UPDATE note_photos \
             SET cloud_path = 'newblob', size = 9, hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'photo-a'",
            crate::blob::content_hash(b"NEW-BYTES"),
        )],
    )
    .await;
    publish_blob_changeset(&db1, &storage, &source_store_dir, cs2, 1).await;

    let (_updated, result) = pull_into(&db2, &storage, &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(
        row_exists(
            &db2,
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
        exact_cache_path(&ld, &row_blob_ref(&db2, "note_photos", "photo-a").await).exists(),
        "the replacement blob lands in the cache",
    );
}

#[tokio::test]
async fn pull_rejects_store_commit_missing_its_signature_when_chain_exists() {
    let founder = UserKeypair::generate();
    let db1 = open_test_db();
    let storage = create_store(&db1, founder.clone()).await;
    let founder_pk = hex::encode(founder.public_key());

    let chain = exact_membership_chain(&storage).await;

    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "dev1",
        1,
        &cs,
        Some(membership_coord(&chain, &founder_pk, 1)),
    )
    .await;
    let graph = load_exact_published_commit(&storage, reference).await;
    let mut unsigned: serde_json::Value = serde_json::from_slice(&graph.commit.to_bytes()).unwrap();
    unsigned
        .as_object_mut()
        .expect("Store commit is a JSON object")
        .remove("signature");
    let commit_ref = replace_exact_commit_bytes_before_commit_validation(
        &storage,
        &graph,
        serde_json::to_vec(&unsigned).unwrap(),
        graph.commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;
    let expected_stream_id = commit_stream_id(&commit_ref);

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, &temp_store_dir().1)
        .await
        .expect("a Store commit without its required signature is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(
        matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Commit { device_id, commit },
                reason: HeldStorePositionReason::ObjectUnreadable { key, detail },
            } if device_id == &expected_stream_id
                && commit == &commit_ref
                && key == commit_ref.object.slot().logical_key()
                && detail.contains("missing field `signature`")
        ),
        "unexpected held position: {:#?}",
        result.held_positions[0]
    );
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(
        materialized_sequences(&db2).await.get(&expected_stream_id),
        None,
    );
}

/// Owner anchoring (issue #95/#102): a puller with a pinned owner refuses a chain
/// whose founder is a different key — the wipe-and-refound takeover — rather than
/// adopting it and authorizing the attacker.
#[tokio::test]
async fn pull_refuses_a_chain_not_anchored_to_the_pinned_owner() {
    let source = open_test_db();
    let storage = create_store(&source, UserKeypair::generate()).await;
    let db2 = open_test_db();
    storage
        .open_into(&db2)
        .await
        .expect("open exact Store before replacing the owner pin");

    // The puller has a different owner pinned from the exact root authority.
    let owner = UserKeypair::generate();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = crate::sync::store::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::new(&db2),
    )
    .await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
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
    let source = open_test_db();
    let storage = create_store(&source, owner).await;
    let db2 = open_test_db();
    let chain = storage
        .open_into(&db2)
        .await
        .expect("open exact Store before removing its membership head");
    let founder_head = chain
        .head_refs()
        .iter()
        .find(|head| head.coord.author_pubkey == owner_pubkey)
        .expect("founder has an exact membership head")
        .clone();

    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();
    storage
        .storage
        .delete_protocol_object(&founder_head.object)
        .await
        .expect("remove exact founder membership head");

    let result = crate::sync::store::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::new(&db2),
    )
    .await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
        "an empty chain with a pinned owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

struct PersistedCycleRemoval {
    storage: TestStore,
    db: crate::database::Database,
    founder_pubkey: String,
    second_owner_head: crate::sync::membership::MembershipHeadRef,
    removed_member_pubkey: String,
}

async fn persisted_cycle_removal(pin_owner: bool) -> PersistedCycleRemoval {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let founder_pubkey = hex::encode(founder.public_key());
    let second_owner_pubkey = hex::encode(second_owner.public_key());
    let removed_member_pubkey = hex::encode(removed_member.public_key());
    let db = open_test_db();
    let storage = create_store(&db, founder.clone()).await;
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::store::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("persisted-cycle-second-owner".to_string()),
        &second_owner_pubkey,
        None,
        MemberRole::Member,
        &encryption,
        "test-lib",
        "Test Store",
        &store_database(&db),
    )
    .await
    .expect("invite second Owner as a Member");
    let second_owner_db = open_test_db();
    install_active_device_fixture(
        &storage,
        &db,
        &second_owner_db,
        &second_owner,
        "2026-03-01T00:00:45Z",
    )
    .await
    .expect("activate second Owner device");
    promote_active_member_fixture(
        &storage,
        &db,
        &second_owner_db,
        &founder,
        &second_owner,
        &encryption,
    )
    .await
    .expect("promote active second Owner");
    let mut chain = storage
        .open_into(&db)
        .await
        .expect("load membership after second Owner promotion");
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
    publish_exact_membership_entry(&storage, &mut chain, add_member, &founder).await;
    let remove_member = chain
        .signed_remove_member_in_stream(
            &second_owner,
            second_owner_stream,
            pubkey_hex(&removed_member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    publish_exact_membership_entry(&storage, &mut chain, remove_member, &second_owner).await;
    let second_owner_head = chain
        .head_refs()
        .iter()
        .find(|head| head.coord.author_pubkey == second_owner_pubkey)
        .expect("second Owner has an exact membership head")
        .clone();

    if pin_owner {
        db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &founder_pubkey)
            .await
            .unwrap();
    } else {
        db.delete_protocol_state(OWNER_PUBKEY_STATE_KEY)
            .await
            .expect("clear persisted-cycle owner pin");
    }

    let initial = crate::sync::store::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::new(&db),
    )
    .await
    .expect("accept and persist the complete multi-author chain");
    assert!(!initial
        .chain
        .expect("listed membership chain")
        .can_write_now(&removed_member_pubkey));

    PersistedCycleRemoval {
        storage,
        db,
        founder_pubkey,
        second_owner_head,
        removed_member_pubkey,
    }
}

#[tokio::test]
async fn pinned_cycle_recovers_persisted_authors_when_membership_listing_is_empty() {
    let fixture = persisted_cycle_removal(true).await;

    let recovered = crate::sync::store::load_cycle_membership(
        &fixture.storage.storage,
        &crate::sync::store::StoreDatabase::new(&fixture.db),
    )
    .await
    .expect("empty LIST must use the persisted author floors");

    assert_eq!(
        recovered.pinned_owner.as_deref(),
        Some(fixture.founder_pubkey.as_str())
    );
    assert!(!recovered
        .chain
        .expect("persisted membership chain")
        .can_write_now(&fixture.removed_member_pubkey));
}

#[tokio::test]
async fn cycle_pins_persisted_authors_when_membership_listing_is_empty() {
    let fixture = persisted_cycle_removal(false).await;

    let recovered = crate::sync::store::load_cycle_membership(
        &fixture.storage.storage,
        &crate::sync::store::StoreDatabase::new(&fixture.db),
    )
    .await
    .expect("an unpinned prior chain must not fall open on an empty LIST");

    assert_eq!(
        recovered.pinned_owner.as_deref(),
        Some(fixture.founder_pubkey.as_str())
    );
    assert!(!recovered
        .chain
        .expect("persisted membership chain")
        .can_write_now(&fixture.removed_member_pubkey));
}

#[tokio::test]
async fn cycle_rejects_missing_state_required_by_a_persisted_floor() {
    let fixture = persisted_cycle_removal(false).await;
    fixture
        .storage
        .storage
        .delete_protocol_object(&fixture.second_owner_head.object)
        .await
        .expect("delete exact persisted membership head");

    let error = match crate::sync::store::load_cycle_membership(
        &fixture.storage.storage,
        &crate::sync::store::StoreDatabase::new(&fixture.db),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("a persisted author floor requires its signed head"),
    };

    assert!(
        matches!(&error, PullError::MembershipTampered(message) if message.contains("durable cursor")),
        "missing persisted-author state must be membership tamper: {error}"
    );
}

#[tokio::test]
async fn mid_cycle_empty_membership_listing_loads_an_advanced_head_from_the_floor() {
    let owner = UserKeypair::generate();
    let owner_pubkey = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let source = open_test_db();
    let storage = create_store(&source, owner.clone()).await;
    let target = open_test_db();
    let mut chain = storage
        .open_into(&target)
        .await
        .expect("bind mid-cycle fixture to its exact Store root");
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();

    let cycle_membership = crate::sync::store::load_cycle_membership(
        &storage.storage,
        &crate::sync::store::StoreDatabase::new(&target),
    )
    .await
    .expect("load founder at cycle start");

    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'AdvancedHead', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devM",
        1,
        &changeset,
        Some(membership_coord(&chain, &owner_pubkey, 1)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let (_tmp, store_dir) = temp_store_dir();
    let result = crate::sync::store::pull_store_commits(
        &store_database(&target),
        target.synced_tables(),
        &storage.storage,
        storage.store_root_hash(),
        &store_dir,
        cycle_membership
            .chain
            .as_ref()
            .expect("opened Store has membership"),
        None,
    )
    .await
    .expect("pull with an empty mid-cycle membership LIST");
    let updated: HashMap<_, _> = result
        .frontier
        .iter()
        .map(|(device_id, position)| (device_id.clone(), position.coord.sequence()))
        .collect();

    assert_eq!(result.changesets_applied, 1);
    assert!(unauthorized_positions(&result).is_empty());
    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
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
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    // A founder entry + a changeset the owner authored: without the fail-closed
    // guard the cycle would (fail to list, drop to chain=None, then) apply this.
    let _chain = exact_membership_chain(&storage).await;
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'X', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    storage
        .publish_changeset(&owner_pk, 1, &cs, SCHEMA_VERSION)
        .await
        .expect("publish owner exact Store changeset");

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    storage
        .open_into(&db2)
        .await
        .expect("open exact Store before fault injection");
    let failing = FaultingStorage::membership(&storage.storage, 1);
    let result = crate::sync::store::load_cycle_membership(
        &failing,
        &crate::sync::store::StoreDatabase::new(&db2),
    )
    .await;
    assert!(
        matches!(result, Err(PullError::MembershipLoad(_))),
        "an exact membership read failure on an owner-pinned store must abort the cycle",
    );
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
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
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    let chain = exact_membership_chain(&storage).await;
    // The owner authors a signed changeset.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromOwner', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devOwner",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 1)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get(&stream_id), Some(&1));
}

#[tokio::test]
async fn pull_authorizes_merge_operations_at_their_exact_predecessor_membership() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let second_owner = UserKeypair::generate();
    let source = open_test_db();
    let storage = create_store(&source, owner.clone()).await;
    storage
        .device_id("founder")
        .await
        .expect("reserve founder Store producer");
    storage
        .device_id("devOwner")
        .await
        .expect("activate a separate founder-identity Store producer");
    let encryption = EncryptionService::from_key([42; 32]);
    crate::sync::store::invite_member(
        &storage.storage,
        storage.home.as_ref(),
        &owner,
        &crate::sync::hlc::Hlc::new("exact-predecessor-second-owner".to_string()),
        &pubkey_hex(&second_owner),
        None,
        MemberRole::Member,
        &encryption,
        "test-store",
        "Test Store",
        &store_database(&source),
    )
    .await
    .expect("invite second Owner as a Member");
    let second_owner_db = open_test_db();
    install_active_device_fixture(
        &storage,
        &source,
        &second_owner_db,
        &second_owner,
        "2026-03-01T00:00:45Z",
    )
    .await
    .expect("activate second Owner device");
    promote_active_member_fixture(
        &storage,
        &source,
        &second_owner_db,
        &owner,
        &second_owner,
        &encryption,
    )
    .await
    .expect("promote active second Owner");
    let chain = storage
        .open_into(&source)
        .await
        .expect("load membership after second Owner promotion");
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'BeforeDemotion', NULL, '0000000002000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devOwner",
        1,
        &changeset,
        Some(membership_coord(&chain, &owner_pk, 1)),
    )
    .await;

    let second_owner_custody = TestCustody::default();
    second_owner_custody.set_initial_key(encryption.key_bytes());
    let second_owner_cipher = std::sync::RwLock::new(CloudCipher::Encrypted(encryption.clone()));
    crate::sync::store::remove_member(
        &storage.storage,
        storage.home.as_ref(),
        &second_owner,
        &crate::sync::hlc::Hlc::new("exact-predecessor-founder-removal".to_string()),
        &owner_pk,
        &encryption,
        &second_owner_custody,
        &second_owner_cipher,
        &crate::sync::cloud_storage::PendingRotation::none(),
        &store_database(&second_owner_db),
    )
    .await
    .expect("successor Owner removes founder with exact recovery state");

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    pull_into(&target, &storage, &temp_store_dir().1).await;
    let (_, result) = pull_into(&target, &storage, &temp_store_dir().1).await;

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
    let source = open_test_db();
    let storage = create_store(&source, owner.clone()).await;

    let _chain = exact_membership_chain(&storage).await;

    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'MissingGrant', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    let reference =
        publish_exact_changeset_with_authority(&storage, "devOwner", 1, &changeset, None).await;
    let stream_id = commit_stream_id(&reference);

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (updated, result) = pull_into(&target, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get(&stream_id), None);
}

/// A signed device head commits to its registration's exact Store stream. A
/// commit from another stream cannot be replayed through that head.
#[tokio::test]
async fn pull_rejects_a_head_that_names_another_device_stream() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let source = open_test_db();
    let storage = create_store(&source, owner.clone()).await;
    storage
        .device_id("devOwner")
        .await
        .expect("reserve founder producer");
    storage
        .device_id("other-device")
        .await
        .expect("activate second device");

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (_pull_tmp, pull_store_dir) = temp_store_dir();
    let (_, activation_result) = pull_into_result(&target, &storage, &pull_store_dir)
        .await
        .expect("materialize device activation before replacing heads");
    assert!(activation_result.held_positions.is_empty());

    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WrongStreamSigner', NULL, '0000000002000-0000-devOwner', '2026-01-01')",
        ],
    )
    .await;
    let owner_sequence = storage
        .next_commit_sequence("devOwner")
        .await
        .expect("read founder producer sequence");
    let reference = storage
        .publish_changeset("devOwner", owner_sequence, &changeset, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let graph = load_exact_published_commit(&storage, reference).await;
    let other_sequence = storage
        .next_commit_sequence("other-device")
        .await
        .expect("read second device sequence");
    let other_reference = storage
        .publish_changeset("other-device", other_sequence, &[], SCHEMA_VERSION)
        .await
        .expect("publish second exact device graph");
    let other = load_exact_published_commit(&storage, other_reference).await;
    storage
        .storage
        .delete_protocol_object(&graph.head_object)
        .await
        .expect("remove original stream head");
    replace_exact_head(
        &storage,
        &other,
        graph.reference.clone(),
        other.head.author_registration.clone(),
        &other.device_signer,
    )
    .await;
    let expected_stream_id = commit_stream_id(&other.reference);

    let (_, result) = pull_into_result(&target, &storage, &pull_store_dir)
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
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(
        materialized_sequences(&target)
            .await
            .get(&expected_stream_id),
        None
    );
}

/// Issue #84 — the membership-propagation lag, the core bug. A member's signed
/// changeset is pulled BEFORE the LIST that rebuilds the chain shows the Add that
/// authorizes them (membership entries and changesets are separate, unordered
/// object streams). The cycle-start chain does not authorize the member, so the
/// old code skipped the changeset and advanced the position — losing it forever.
/// Now the changeset carries the coordinate of its authorizing entry; a direct,
/// read-after-write-consistent GET resolves that entry even though the LIST lags,
/// and the changeset applies. It must NOT be lost.
#[tokio::test]
async fn pull_resolves_a_changeset_whose_authorizing_entry_lags_the_listing() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    // The mock signs every head with the owner key, so the member's device head is
    // owner-authored — a current member — and passes the head-authorization check
    // even while the member's own Add is still invisible to the LIST.
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    // Founder at (owner, 1); the owner adds the member as a Member at (owner, 2).
    let mut chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;
    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add that is lagging the LIST.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devM",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 1)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    // The lagging entry was fetched by coordinate and the changeset applied — not
    // dropped as non-member, and not surfaced as a rejection.
    assert_eq!(result.changesets_applied, 1);
    assert!(unauthorized_positions(&result).is_empty());
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get(&stream_id), Some(&1));
}

/// Issue #84 — the other side of the split: a genuinely unauthorized changeset
/// (here authored by a key that is NOT in the chain at all, with a grant
/// coordinate that resolves to an entry that doesn't authorize it) is judged
/// against the exact entry it names, found wanting, and SKIPPED — position advanced
/// so the device isn't stuck — and surfaced for a UI warning. The grant points at
/// the founder entry (owner, 1), which authorizes the owner, not the outsider, so
/// merging it still leaves the outsider unauthorized.
#[tokio::test]
async fn pull_skips_and_surfaces_a_forged_changeset_whose_grant_does_not_authorize_it() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let outsider = UserKeypair::generate();
    // Head signed by the owner (a current member) so the head passes its check and
    // pull reaches the changeset-level judgment.
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    let mut chain = exact_membership_chain(&storage).await;
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
    publish_exact_membership_entry(&storage, &mut chain, add_outsider, &owner).await;

    // The outsider authors a signed changeset but, lacking any Add of their own,
    // names the founder entry (owner, 1) as their grant. The signature is valid
    // (it's their own key) but the named entry authorizes the owner, not them.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-devX', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devX",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 2)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    // Nothing applies and the durable frontier remains before the forged commit.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    let unauthorized = unauthorized_positions(&result);
    assert_eq!(unauthorized.len(), 1);
    assert_eq!(
        unauthorized[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id: stream_id.clone(),
            commit: reference,
        }
    );
    assert_eq!(updated.get(&stream_id), None);
    assert_eq!(materialized_sequences(&db2).await.get(&stream_id), None,);
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
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    let chain = exact_membership_chain(&storage).await;

    // The owner (a current member) authors a changeset that WOULD be authorized,
    // then its signature is corrupted. The signature check must reject it before
    // authorization is even considered.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Tampered', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "dev1",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 1)),
    )
    .await;
    let graph = load_exact_published_commit(&storage, reference).await;
    let mut forged: serde_json::Value = serde_json::from_slice(&graph.commit.to_bytes()).unwrap();
    forged["signature"] = serde_json::Value::String("0".repeat(128));
    let commit_ref = replace_exact_commit_bytes_before_commit_validation(
        &storage,
        &graph,
        serde_json::to_vec(&forged).unwrap(),
        graph.commit.commit_hash(),
        graph.head.author_registration.clone(),
        &graph.device_signer,
    )
    .await;
    let expected_stream_id = commit_stream_id(&graph.reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (_, result) = pull_into_result(&db2, &storage, &temp_store_dir().1)
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
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(
        materialized_sequences(&db2).await.get(&expected_stream_id),
        None
    );
}

/// Issue #84 — a removed member's changeset is skipped, not applied. The owner
/// added the member at (owner, 2) then removed them at (owner, 3); the member's
/// changeset names its (still-valid-looking) Add grant (owner, 2), but the puller
/// already holds the Remove, so merging the grant into the full chain still leaves
/// the author unauthorized. Surfaced and position-advanced, like any forged write.
#[tokio::test]
async fn pull_skips_a_removed_members_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    let mut chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;
    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    publish_exact_membership_entry(&storage, &mut chain, remove_member, &owner).await;
    // The removed member authors a changeset stamping their old grant (owner, 2).
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromRemoved', NULL, '0000000004000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devM",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 2)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert_eq!(updated.get(&stream_id), None);
}

#[tokio::test]
async fn removed_member_candidate_cleanup_verifies_the_exact_revocation_witness() {
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let owner_db = open_test_db();
    let storage = create_store(&owner_db, owner.clone()).await;
    let mut chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;

    let member_db = open_test_db();
    install_active_device_fixture(
        &storage,
        &owner_db,
        &member_db,
        &member,
        "2026-03-01T00:02:00Z",
    )
    .await
    .expect("activate member device");
    let member_changeset = capture_bytes(
        &member_db,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('member-candidate', 'Member candidate', NULL, \
                   '0000000003000-0000-member', '2026-01-01')",
        ],
    )
    .await;
    let (_member_temp, member_store_dir) = temp_store_dir();
    let candidate = sync_for_test(
        &member_db,
        member_db.synced_tables(),
        member_changeset,
        0,
        &storage,
        "2026-03-01T00:03:00Z",
        "",
        &member,
        &member_store_dir,
    )
    .await
    .expect("publish member candidate")
    .expect("member candidate produces a Store commit");
    let candidate_graph = load_exact_published_commit_as(&storage, candidate, &member).await;
    let write_id = candidate_graph.commit.write_id.clone();

    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:04:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    publish_exact_membership_entry(&storage, &mut chain, remove_member, &owner).await;
    let owner_changeset = capture_bytes(
        &owner_db,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('revocation-witness', 'Revocation witness', NULL, \
                   '0000000005000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    let (_owner_temp, owner_store_dir) = temp_store_dir();
    let owner_sequence = crate::sync::store::StoreDatabase::new(&owner_db)
        .latest_local_store_position()
        .await
        .expect("read Owner witness predecessor")
        .map_or(0, |reference| reference.coord.sequence());
    sync_for_test(
        &owner_db,
        owner_db.synced_tables(),
        owner_changeset,
        owner_sequence,
        &storage,
        "2026-03-01T00:05:00Z",
        "",
        &owner,
        &owner_store_dir,
    )
    .await
    .expect("publish accepted revocation witness")
    .expect("revocation witness produces a Store commit");

    let (_pull_temp, pull_store_dir) = temp_store_dir();
    storage.home.fail_exact_delete_on_call(1);
    pull_into_result(&member_db, &storage, &pull_store_dir)
        .await
        .expect_err("interrupted cleanup retains the verified retraction journal");
    crate::sync::store::cleanup_merge_candidate(
        &store_database(&member_db),
        &storage.storage,
        write_id.clone(),
    )
    .await
    .expect("verify and resume removed-member candidate cleanup");
    crate::sync::store::StoreDatabase::new(&member_db)
        .finish_retracted_merge_candidate_cleanup(write_id.clone())
        .await
        .expect("finalize removed-member candidate cleanup");
    assert!(storage
        .home
        .get(candidate_graph.reference.object.slot().logical_key())
        .is_none());
    assert!(storage
        .home
        .get(candidate_graph.head_object.slot().logical_key())
        .is_none());
    assert!(matches!(
        member_db
            .write_status(&write_id)
            .await
            .expect("read retracted member write"),
        crate::WriteStatus::Resolved(crate::WriteResolution::Retracted { witness })
            if witness.original_position().commit() == &candidate_graph.reference
    ));
    assert!(!crate::sync::store::StoreDatabase::new(&member_db)
        .merge_candidate_cleanup_pending(&write_id)
        .await
        .expect("read completed member cleanup"));
    assert!(crate::sync::store::StoreDatabase::new(&member_db)
        .protocol_inert_object(candidate_graph.head_object)
        .await
        .expect("read terminal member head")
        .is_some());
}

/// A hash-linked membership chain detects a missing MIDDLE entry via `previous_hash`,
/// but nothing points forward to a missing TAIL entry, so a listing that omits a
/// committed `Remove` still hash-links cleanly and reads the removed member as
/// current. The owner removes the member at (owner, 3) and publishes a head
/// covering it, but the LIST that rebuilds the chain omits that key while a keyed
/// GET still serves it — the same eventual-consistency gap a legitimate lagging
/// Add exploits, except here the lagging object is the one that revokes, not the
/// one that grants. The removed member's changeset, naming its now-superseded
/// (owner, 2) Add as its grant, must still be judged against the full,
/// head-committed chain (which the keyed GET recovers) and refused — not against
/// whatever a bare listing happens to hash-link into.
#[tokio::test]
async fn removed_member_is_not_re_admitted_by_a_lagging_listing() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    let mut chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;
    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    publish_exact_membership_entry(&storage, &mut chain, remove_member, &owner).await;

    // The removed member authors a changeset stamping their old grant (owner, 2),
    // which looks like a legitimate lagging Add if the reload is judged against a
    // plain listing instead of the committed chain.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromRemoved', NULL, '0000000004000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devM",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 2)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    // Not applied: the removed member is not re-admitted by the lagging listing.
    // Surfaced as rejected-unauthorized and the position advances so the device is
    // not stuck on it.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert_eq!(updated.get(&stream_id), None);
}

/// A membership entry is not authoritative until its author publishes a signed
/// head covering it. A changeset cannot turn a stored-but-uncommitted Add into an
/// authorization grant merely by naming that entry's coordinate.
#[tokio::test]
async fn pull_rejects_a_changeset_naming_a_grant_no_head_covers() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    // The owner publishes a head covering only the founder entry (seq 1) before
    // adding the member, so the Add at seq 2 is uploaded but no head certifies it
    // yet — genuinely uncommitted, not just list-lagging.
    let chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    let grant = add_member.coord();
    let (prepared, _) = crate::sync::store_objects::prepare_membership_entry(
        &storage.storage,
        storage.root.store_root_hash,
        &add_member,
    )
    .await
    .expect("prepare uncommitted exact membership entry");
    crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("publish uncommitted exact membership entry");

    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add no head covers yet.
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromUncommittedMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let reference =
        publish_exact_changeset_with_authority(&storage, "devM", 1, &cs, Some(grant)).await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get(&stream_id), None);
}

#[tokio::test]
async fn relocated_membership_grant_cannot_authorize_a_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let relocated_author = hex::encode(UserKeypair::generate().public_key());
    let source = open_test_db();
    let storage = create_store(&source, owner.clone()).await;

    let mut chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    let grant_bytes = serde_json::to_vec(&add_member).expect("serialize exact membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;

    let owner_grant = membership_coord(&chain, &owner_pk, 2);
    let relocated_prefix = crate::sync::store_commit::membership_entry_semantic_prefix(
        &relocated_author,
        &owner_grant.author_owner_grant,
        owner_grant.stream_id,
        2,
        owner_grant.entry_hash,
    );
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        crate::sync::storage::ProtocolObjectDomain::StoreMembershipEntry,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &relocated_prefix, ".json")
        .await
        .expect("allocate relocated exact membership grant slot");
    let prepared = storage
        .prepare_protocol_object(&context, slot, &relocated_prefix, grant_bytes)
        .expect("prepare relocated exact membership grant");
    crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("relocate the grant to another author's coordinate");

    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'RelocatedGrant', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
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

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (_, result) = pull_into_result(&target, &storage, &temp_store_dir().1)
        .await
        .expect("a relocated membership grant holds its Store stream");

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&target).await.get(&stream_id), None);
}

/// A storage read failure while resolving a grant holds the affected stream at
/// the undecided commit. The pull must not replace an unavailable committed-chain
/// read with a bare keyed entry or abort independent streams.
#[tokio::test]
async fn pull_holds_the_position_when_the_mid_cycle_membership_list_fails() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    // Capture the cycle's founder-only membership view before committing the
    // member Add and activating that member's device.
    let mut chain = exact_membership_chain(&storage).await;

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let loaded = storage
        .open_into(&db2)
        .await
        .expect("load exact committed membership prefix");

    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member, &owner).await;
    let member_db = open_test_db();
    install_active_device_fixture(&storage, &db1, &member_db, &member, "2026-03-01T00:02:00Z")
        .await
        .expect("activate member device");

    let cs = capture_bytes(
        &member_db,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    let (_publish_tmp, publish_store_dir) = temp_store_dir();
    let reference = sync_for_test(
        &member_db,
        member_db.synced_tables(),
        cs,
        0,
        &storage,
        "2026-03-01T00:03:00Z",
        "",
        &member,
        &publish_store_dir,
    )
    .await
    .expect("publish member Store changeset")
    .expect("member Store changeset produces a commit");
    let stream_id = commit_stream_id(&reference);

    let failing = FaultingStorage::membership(&storage.storage, 1);
    let result = crate::sync::store::pull_store_commits(
        &store_database(&db2),
        db2.synced_tables(),
        &failing,
        storage.root.store_root_hash,
        &temp_store_dir().1,
        &loaded,
        None,
    )
    .await
    .expect("a failed membership reload holds only the affected stream");

    // The failed read leaves authorization undecided and the position unchanged.
    assert!(result.held_positions.iter().any(|held| matches!(
        &held.reason,
        HeldStorePositionReason::InvalidObject(detail)
            if detail.contains("forced exact membership read failure")
    )));
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&db2).await.get(&stream_id), None);
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
    let db2 = open_test_db();
    let storage = create_store(&db2, owner.clone()).await;

    let mut chain = exact_membership_chain(&storage).await;
    let add_member = chain
        .signed_set_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    publish_exact_membership_entry(&storage, &mut chain, add_member.clone(), &owner).await;
    let remove_member = chain
        .signed_remove_member_in_stream(
            &owner,
            membership_author_stream(&chain, &owner),
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    publish_exact_membership_entry(&storage, &mut chain, remove_member, &owner).await;
    let remove_coord = membership_coord(&chain, &owner_pk, 3);
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
    pull_into(&db2, &storage, &temp_store_dir().1).await;

    storage
        .delete_protocol_object(&remove_head.object)
        .await
        .expect("hide exact remove head to serve the predecessor as terminal");

    let result = pull_into_result(&db2, &storage, &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(StorePullError::Membership(_))),
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
    let db2 = open_test_db();
    let storage = create_store(&db2, UserKeypair::generate()).await;
    let chain = exact_membership_chain(&storage).await;
    let founder = chain.entries().first().expect("exact founder entry");
    let coord = founder.coord();
    let head_ref = chain
        .head_ref_for_stream(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
        )
        .expect("exact founder head reference");
    let head =
        crate::sync::store::load_exact_membership_head(&storage.storage, &storage.root, head_ref)
            .await
            .expect("load exact founder head");
    let mut bad = founder.clone();
    bad.signature = "00".to_string();
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        storage.root.store_root_hash,
        crate::sync::storage::ProtocolObjectDomain::StoreMembershipEntry,
    );
    let prefix = crate::sync::store_commit::membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
        coord.entry_hash,
    );
    storage
        .delete_protocol_object(&head.body.entry.object)
        .await
        .expect("delete exact founder entry before corruption");
    let prepared = storage
        .prepare_protocol_object(
            &context,
            head.body.entry.object.slot().clone(),
            &prefix,
            serde_json::to_vec(&bad).expect("serialize corrupt founder"),
        )
        .expect("prepare corrupt exact founder entry");
    crate::sync::store_objects::create_exact_object(&storage.storage, &prepared)
        .await
        .expect("publish corrupt exact founder entry");

    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &pubkey_hex(&storage.signer))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(StorePullError::Membership(_))),
        "a malformed chain on a pinned-owner store must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// A verified head whose signer is not a current member is examined so a newly
/// added member can resolve a committed grant that appeared after cycle start.
/// When the envelope's named grant does not authorize the signer, the changeset
/// is rejected and the position advances instead of holding on attacker content.
#[tokio::test]
async fn pull_rejects_a_stream_authored_by_a_non_member() {
    // The mock signs every head it publishes with `outsider`, who is not in the
    // chain — so the head it writes for `dev1` fails the membership check.
    let owner = UserKeypair::generate();
    let outsider = UserKeypair::generate();
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;
    let owner_pk = hex::encode(owner.public_key());
    let mut chain = exact_membership_chain(&storage).await;
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
    publish_exact_membership_entry(&storage, &mut chain, add_outsider, &owner).await;

    // dev1 has a changeset in the bucket (its head is published by the mock,
    // signed by the non-member `outsider`).
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromForgedHead', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "dev1",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 2)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert_eq!(updated.get(&stream_id), None);
    assert!(result
        .visible_heads
        .iter()
        .any(|head| head.head.commit == reference));
}

/// The honored case: a head authored by a current member (here a second device
/// whose head and changeset the owner signs) is kept, and its changeset applies.
#[tokio::test]
async fn pull_honors_a_head_authored_by_a_current_member() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    // The mock is the owner's device, so the head it publishes for `devA` is
    // owner-signed — a current member.
    let db1 = open_test_db();
    let storage = create_store(&db1, owner.clone()).await;

    let chain = exact_membership_chain(&storage).await;

    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromMember', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    let reference = publish_exact_changeset_with_authority(
        &storage,
        "devA",
        1,
        &cs,
        Some(membership_coord(&chain, &owner_pk, 1)),
    )
    .await;
    let stream_id = commit_stream_id(&reference);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
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
        let db1 = open_test_db_with_blob(photo_decl());
        create_store(&db1, UserKeypair::generate()).await;

        // The attacker's blob bytes, planted in the cloud under the malicious id's
        // flat mock key (the same key the puller's `get_blob` computes for it). No
        // local file is written on the source side, so nothing escapes here.
        // The source's changeset adds a note + a photo row whose id is the
        // traversal string. (The mock stored the blob above; this is the row that
        // references it.)
        capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES ('x/../../../PWNED', 'n1', 'cover', 5, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                    crate::blob::content_hash(b"PWNED"),
                ),
            ],
        )
        .await;
        let (_tmp, store_dir) = temp_store_dir();
        let error = crate::blob::transition::make_remote(
            &store_database(&db1),
            &store_dir,
            &crate::sync::hlc::Hlc::new("traversal-test".to_string()),
            "notes",
            "n1",
            false,
        )
        .await
        .expect_err("a traversal blob id cannot enter the upload journal");
        assert!(matches!(
            error,
            crate::blob::transition::MakeRemoteError::Source { ref blob_id, .. }
                if blob_id == "x/../../../PWNED"
        ));
    }

    /// A blob id too short to form the `{ab}/{cd}` partition prefix (the
    /// dash-stripped id is under four chars, or splits a multi-byte char) cannot
    /// index the prefix's byte slice, so the path builder refuses it. End to end
    /// it is bad data: the row does not apply and the position holds. (The slice
    /// itself is proven non-panicking by the `hashed_path` unit tests in
    /// `store_dir`.)
    #[tokio::test]
    async fn unindexable_id_is_refused_not_panicked() {
        let db1 = open_test_db_with_blob(photo_decl());
        create_store(&db1, UserKeypair::generate()).await;

        capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                // `id = "a"` dash-strips to "a", too short for the `&hex[..2]`
                // prefix slice, so the path builder refuses it.
                &format!(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES ('a', 'n1', 'cover', 1, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                    crate::blob::content_hash(b"A"),
                ),
            ],
        )
        .await;
        let (_tmp, store_dir) = temp_store_dir();
        let error = crate::blob::transition::make_remote(
            &store_database(&db1),
            &store_dir,
            &crate::sync::hlc::Hlc::new("short-id-test".to_string()),
            "notes",
            "n1",
            false,
        )
        .await
        .expect_err("an unindexable blob id cannot enter the upload journal");
        assert!(matches!(
            error,
            crate::blob::transition::MakeRemoteError::Source { ref blob_id, .. }
                if blob_id == "a"
        ));
    }

    /// A normal blob id still round-trips: the boundary check rejects only ids that
    /// could escape the cache or can't be partitioned, and a well-formed id writes
    /// its blob into the pinned cache at its partitioned `{ab}/{cd}/<id>` path.
    #[tokio::test]
    async fn normal_id_still_writes_under_the_blob_dir() {
        let db1 = open_test_db_with_blob(photo_decl());
        let storage = create_store(&db1, UserKeypair::generate()).await;

        capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                &format!(
                    "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                     VALUES ('p1ab', 'n1', 'attach', 10, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                    crate::blob::content_hash(b"PHOTOBYTES"),
                ),
            ],
        )
        .await;
        let (_source_tmp, source_store_dir) = temp_store_dir();
        store_local(&source_store_dir, "p1ab", b"PHOTOBYTES").await;
        make_test_root_remote(&db1, &storage, &source_store_dir, "n1").await;

        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_store_dir();
        let (updated, result) = pull_into(&db2, &storage, &ld).await;

        assert_eq!(result.changesets_applied, 1, "a well-formed row applies");
        assert!(!result.asset_downloads_failed);
        assert_eq!(updated.values().copied().collect::<Vec<_>>(), vec![1]);
        let written = std::fs::read(exact_cache_path(
            &ld,
            &row_blob_ref(&db2, "note_photos", "p1ab").await,
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
    let observer = open_test_db();
    let storage = TestStore::create(&observer, "test-lib", keypair.clone())
        .await
        .expect("create exact Store for dependency-order test");
    storage.home.sort_listings();
    let tables = test_synced_tables();

    let first = open_test_db();
    let second = open_test_db();
    let receiver = open_test_db();
    for participant in [&first, &second, &receiver] {
        install_active_device_fixture(
            &storage,
            &observer,
            participant,
            &keypair,
            "2026-01-01T00:00:00Z",
        )
        .await
        .expect("install active test device");
    }
    for participant in [&first, &second, &receiver] {
        let (_activation_temp, activation_store_dir) = temp_store_dir();
        pull_into(participant, &storage, &activation_store_dir).await;
    }
    let first_stream = local_announcement_stream(&first).await;
    let second_stream = local_announcement_stream(&second).await;
    let (db_ins, db_upd, insert_stream, update_stream) = if first_stream > second_stream {
        (&first, &second, first_stream, second_stream)
    } else {
        (&second, &first, second_stream, first_stream)
    };
    assert!(update_stream < insert_stream);

    let (_ti, ld_ins) = temp_store_dir();
    let insert = capture_bytes(
        db_ins,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('n1', 'orig', NULL, 1, '0000000001000-0000-ins', '2026-01-01')",
        ],
    )
    .await;
    push_cycle_as(db_ins, &tables, &storage, insert, 0, &keypair, &ld_ins).await;
    let insert_position = crate::sync::store::StoreDatabase::new(db_ins)
        .latest_local_store_position()
        .await
        .expect("read inserter position")
        .expect("inserter published one Store commit");

    let (_tu, ld_upd) = temp_store_dir();
    pull_into(db_upd, &storage, &ld_upd).await;
    assert_eq!(
        store_database(db_upd)
            .materialized_frontier()
            .await
            .expect("read updater materialized frontier")
            .get(&insert_stream.to_string()),
        Some(&insert_position),
        "the updater durably materializes the exact insert before capturing its update",
    );
    let update = capture_bytes(
        db_upd,
        &[
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-upd' \
           WHERE id = 'n1'",
        ],
    )
    .await;
    push_cycle_as(db_upd, &tables, &storage, update, 0, &keypair, &ld_upd).await;
    let (_, update_commit) =
        load_exact_materialized_commit(db_upd, &storage.storage, &update_stream.to_string(), 1)
            .await
            .expect("load updater Store commit")
            .expect("updater Store commit is materialized");
    assert_eq!(
        update_commit.value.order.dependencies().get(&insert_stream),
        Some(&insert_position),
        "the update commit captures the exact insert dependency",
    );

    let (_tc, ld_c) = temp_store_dir();
    pull_into(&receiver, &storage, &ld_c).await;

    assert_eq!(
        query_text(&receiver, "SELECT title FROM notes WHERE id = 'n1'").await,
        "updated",
        "the dependent UPDATE must wait for its exact INSERT dependency",
    );
}

#[tokio::test]
async fn provider_blob_download_failure_remains_typed() {
    let (db, _) = crate::database::Database::open(
        std::path::Path::new(":memory:"),
        test_synced_tables_with_blob(BlobDecl::new(
            "photos",
            Provenance::HostProvided,
            CacheFill::CacheEager,
        )),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        "download-provider-failure".to_string(),
        &test_migrations(),
    )
    .expect("open blob database");
    let bytes = b"provider-down";
    let hash = crate::blob::content_hash(bytes);
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('download-root', 'download', NULL, 1, \
                     '0000000001000-0000-source', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        &format!(
            "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('download-blob', 'download-root', 'cover', {}, '{}', \
                         '0000000001000-0000-source', '2026-01-01')",
            bytes.len(),
            hash,
        ),
    )
    .await;
    let storage = create_store(&db, UserKeypair::generate()).await;
    let stored = create_exact_blob(&db, &storage, "photos", "download-blob", None, bytes).await;
    let local = db
        .row_blob_ref("note_photos", "download-blob")
        .await
        .expect("load exact download row blob reference");
    let remote = crate::blob::RowBlobRef::new(
        local.table().to_string(),
        local.row_id().to_string(),
        local.row_stamp().to_string(),
        local.column().to_string(),
        local.blob().clone(),
        local.plaintext_size(),
        local.plaintext_hash(),
        crate::blob::RowBlobAuthority::Remote(
            crate::sync::audience_package::PackageAudience::Store,
        ),
        Some(stored),
    )
    .expect("attach exact stored blob publication to row");
    let failing = FaultingStorage::blob(&storage.storage);
    let (_temp, store_dir) = temp_store_dir();

    let failures =
        crate::sync::store::download_blobs(
            &store_database(&db),
            vec![crate::sync::store::BlobDownload::from_row(remote)
                .expect("build exact blob download")],
            &failing,
            &store_dir,
        )
        .await
        .expect_err("provider download failure remains a typed batch failure");
    assert_eq!(failures.failures().len(), 1);
    assert!(failures.has_transport_failure());
    assert!(
        crate::sync::cycle::SyncCycleFailure::operation("pull Store commits", failures,)
            .is_offline()
    );
}
