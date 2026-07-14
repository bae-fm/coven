//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
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
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::cycle;
use crate::sync::membership::{founder_entry, MemberRole, MembershipChain, MembershipCoord};
use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;
use crate::sync::pull::PullError;
use crate::sync::store_commit::StoreDeviceHead;
use crate::sync::store_pull::{
    HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason, StorePullError,
};
/// The synthetic test db opens with a single migration, so its
/// [`crate::database::Database::schema_version`] is 1. Changesets are stored at
/// that version; a newer peer's changeset or floor uses `SCHEMA_VERSION + 1`.
const SCHEMA_VERSION: u32 = 1;
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

#[async_trait]
trait TestStoreStorage: SyncStorage {
    async fn bind_for_test_publish(
        &self,
        db: &crate::database::Database,
        device_id: &str,
        keypair: &UserKeypair,
    ) -> Result<(), String>;
}

#[async_trait]
impl TestStoreStorage for MockSyncStorage {
    async fn bind_for_test_publish(
        &self,
        db: &crate::database::Database,
        device_id: &str,
        _keypair: &UserKeypair,
    ) -> Result<(), String> {
        bind_mock_store_protocol(db, self, device_id).await;
        Ok(())
    }
}

#[async_trait]
impl TestStoreStorage for CloudSyncStorage {
    async fn bind_for_test_publish(
        &self,
        db: &crate::database::Database,
        device_id: &str,
        keypair: &UserKeypair,
    ) -> Result<(), String> {
        if db
            .get_protocol_state(crate::database::PROTOCOL_GENESIS_HASH_STATE_KEY)
            .await
            .map_err(|error| error.to_string())?
            .is_none()
        {
            let genesis = crate::sync::store_genesis::create_store(
                db,
                self,
                self.store_id(),
                "0000000000001-0000-test-publish",
                keypair,
            )
            .await
            .map_err(|error| error.to_string())?;
            crate::sync::cycle::ensure_owner_anchored_chain(self, db, &genesis, keypair).await?;
        }
        db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
            .await
            .map_err(|error| error.to_string())
    }
}

fn cloud_test_storage(
    home: std::sync::Arc<dyn CloudHome>,
    cipher: CloudCipher,
    blob_paths: BlobPathScheme,
    store_id: &str,
    keypair: UserKeypair,
) -> CloudSyncStorage {
    CloudSyncStorage::new(home, cipher, blob_paths, store_id, keypair).with_copy_ids(
        std::sync::Arc::new(crate::storage::cloud::RandomCopyIdGenerator),
    )
}

/// Stage and publish exact package bytes through the durable Store outbox.
async fn sync_for_test<S: TestStoreStorage>(
    device_id: &str,
    db: &crate::database::Database,
    tables: &[SyncedTable],
    outgoing: Vec<u8>,
    local_seq: u64,
    storage: &S,
    timestamp: &str,
    message: &str,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) -> Result<Option<crate::sync::store_commit::CommitPosition>, String> {
    let configured_tables: Vec<_> = db.synced_tables().iter().map(SyncedTable::name).collect();
    let supplied_tables: Vec<_> = tables.iter().map(SyncedTable::name).collect();
    assert_eq!(configured_tables, supplied_tables);
    assert!(
        message.is_empty(),
        "Store commits carry no arbitrary message"
    );
    storage
        .bind_for_test_publish(db, device_id, keypair)
        .await?;
    let before = db
        .latest_local_store_position()
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(
        before.as_ref().map_or(0, |position| position.seq),
        local_seq
    );
    db.enqueue_store_changeset_for_test(outgoing)
        .await
        .map_err(|error| error.to_string())?;
    let membership = crate::sync::pull::load_cycle_membership(storage, db)
        .await
        .map_err(|error| error.to_string())?;
    let staged = crate::sync::store_outbound::stage_pending_store_batch(
        db,
        storage,
        device_id,
        timestamp,
        keypair,
        store_dir,
        membership.chain.as_ref(),
        None,
    )
    .await
    .map_err(|error| error.to_string())?;
    if !staged {
        return Ok(None);
    }
    crate::sync::store_outbound::drain_outbound_store_batches(db, storage)
        .await
        .map_err(|error| error.to_string())?;
    db.latest_local_store_position()
        .await
        .map_err(|error| error.to_string())
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
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        "restart-test-device".to_string(),
        &test_migrations(),
    )
    .expect("open file-backed blob test database")
    .0
}

/// Store `bytes` into `ld`'s local store under blob id `id`, the way a host stores a
/// host-provided cover (its Local home) before the inline push reads it to upload.
async fn store_local(ld: &crate::store_dir::StoreDir, id: &str, bytes: &[u8]) {
    local_files::store(ld, "photos", id, bytes)
        .await
        .expect("store host-provided blob in the local store");
}

async fn remove_protocol_prefix(storage: &MockSyncStorage, prefix: &str) {
    let listing = storage
        .list_protocol_objects(prefix)
        .await
        .expect("list protocol candidates for removal");
    assert!(
        !listing.objects.is_empty(),
        "protocol removal prefix {prefix:?} must name at least one candidate",
    );
    for object in listing.objects {
        storage
            .delete_protocol_object(&object)
            .await
            .expect("remove protocol candidate");
    }
}

async fn materialized_sequences(db: &crate::database::Database) -> HashMap<String, u64> {
    db.materialized_frontier()
        .await
        .expect("read materialized Store frontier")
        .into_iter()
        .map(|(device_id, position)| (device_id, position.seq))
        .collect()
}

fn constraint_conflicts(
    result: &crate::sync::store_pull::StorePullResult,
) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| matches!(held.reason, HeldStorePositionReason::ConstraintConflict(_)))
        .collect()
}

fn newer_schema_positions(
    result: &crate::sync::store_pull::StorePullResult,
) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| matches!(held.reason, HeldStorePositionReason::NewerSchema { .. }))
        .collect()
}

fn unauthorized_positions(
    result: &crate::sync::store_pull::StorePullResult,
) -> Vec<&HeldStorePosition> {
    result
        .held_positions
        .iter()
        .filter(|held| held.reason == HeldStorePositionReason::Unauthorized)
        .collect()
}

fn invalid_changeset_positions(
    result: &crate::sync::store_pull::StorePullResult,
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

#[tokio::test]
async fn pull_applies_remote_changeset_and_surfaces_row_changes() {
    let storage = MockSyncStorage::new();

    // Source device records a note as changeset seq 1.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Second device pulls.
    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        materialized_sequences(&db2).await.get("dev1"),
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
    let storage = MockSyncStorage::new();
    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Remote', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &changeset, SCHEMA_VERSION);

    let target = open_test_db();
    exec(
        &target,
        "CREATE TRIGGER reject_materialized_insert BEFORE INSERT ON materialized_commits \
         BEGIN SELECT RAISE(ABORT, 'injected materialized-position write failure'); END;",
    )
    .await;
    let (_tmp, store_dir) = temp_store_dir();
    let error = pull_into_result(&target, &storage, "dev2", &store_dir)
        .await
        .expect_err("materialized-position failure aborts the pull");
    assert!(matches!(error, StorePullError::Database(_)));
    assert!(
        !row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "the row cannot commit when its position write fails",
    );
    assert!(materialized_sequences(&target).await.is_empty());
}

#[tokio::test]
async fn ordinary_pull_starts_from_its_durable_position() {
    let storage = MockSyncStorage::new();
    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('stale-row', 'Remote', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &changeset, SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_positions.is_empty());
    assert_eq!(materialized_sequences(&target).await.get("dev1"), Some(&1),);
    assert!(
        row_exists(&target, "SELECT 1 FROM notes WHERE id = 'stale-row'").await,
        "ordinary pull derives coverage from durable rows, not caller input",
    );
}

#[tokio::test]
async fn ordinary_pull_uses_its_durable_position_on_every_call() {
    let storage = MockSyncStorage::new();
    let source = open_test_db();
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
    storage.store_changeset("dev1", 1, &first, SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, "dev2", &store_dir).await;
    storage.store_changeset("dev1", 2, &second, SCHEMA_VERSION);

    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_positions.is_empty());
    assert_eq!(updated.get("dev1"), Some(&2));
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'position-row'").await,
        "Two",
    );
}

#[tokio::test]
async fn ordinary_pull_applies_the_change_immediately_after_its_durable_position() {
    let storage = MockSyncStorage::new();
    let source = open_test_db();
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
    storage.store_changeset("dev1", 1, &first, SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, "dev2", &store_dir).await;
    storage.store_changeset("dev1", 2, &second, SCHEMA_VERSION);

    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get("dev1"), Some(&2));
    assert_eq!(materialized_sequences(&target).await.get("dev1"), Some(&2),);
}

#[tokio::test]
async fn invalid_materialized_positions_are_rejected_at_the_database_boundary() {
    let target = open_test_db();
    let invalid_insert = target
        .call(|conn| {
            conn.execute(
                "INSERT INTO materialized_commits (device_id, seq, commit_hash) \
                 VALUES ('bad-device', -1, \
                         'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
                [],
            )
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        })
        .await;
    assert!(invalid_insert.is_err());
    assert!(target.materialized_frontier().await.unwrap().is_empty());
    let overflow = std::collections::BTreeMap::from([(
        "overflow-device".to_string(),
        crate::sync::store_commit::CommitPosition {
            seq: u64::MAX,
            commit_hash: crate::sync::store_commit::ObjectHash::digest(b"overflow"),
        },
    )]);
    assert!(target
        .install_bootstrap_state(
            &overflow,
            crate::sync::store_commit::ObjectHash::digest(b"snapshot"),
            crate::sync::store_commit::ObjectHash::digest(b"genesis"),
        )
        .await
        .is_err());
    assert!(target
        .snapshot_coverage_frontier()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn empty_package_materializes_its_exact_commit_position() {
    let storage = MockSyncStorage::new();
    storage.store_changeset("dev1", 1, &[], SCHEMA_VERSION);
    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();

    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(materialized_sequences(&target).await.get("dev1"), Some(&1),);
}

#[tokio::test]
async fn host_write_after_remote_apply_observes_the_matching_position() {
    let storage = MockSyncStorage::new();
    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('remote', 'Remote', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &changeset, SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, "dev2", &store_dir).await;

    let tables = target.synced_tables().to_vec();
    target
        .call(move |conn| {
            crate::database::Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                let remote_row: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM notes WHERE id = 'remote')",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(crate::database::DbError::from)?;
                let materialized: Option<u64> = tx
                    .query_row(
                        "SELECT seq FROM materialized_commits WHERE device_id = 'dev1'",
                        [],
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
            })
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
    let storage = MockSyncStorage::new();

    // The source device's head advertises seq 1, but the changeset object is
    // gone: reclamation deleted it as superseded. `store_changeset` both writes
    // the object and advances the head to seq 1; deleting the object leaves the
    // head pointing past a hole.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'First', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        1,
    )
    .await
    .expect("load Store commit")
    .expect("Store commit exists");
    remove_protocol_prefix(&storage, &format!("{}/", commit.value.package.object_key)).await;

    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0].coordinate,
        HeldStoreCoordinate::Package {
            device_id,
            seq: 1,
            ..
        } if device_id == "dev1"
    ));
    assert_eq!(
        result.held_positions[0].reason,
        HeldStorePositionReason::MissingPackage,
    );
    // The position holds at the gap: dev1 never advances over the unapplied seq.
    assert_eq!(updated.get("dev1").copied().unwrap_or(0), 0);
}

#[tokio::test]
async fn uniqueness_conflict_rolls_back_the_entire_changeset_and_position() {
    let storage = MockSyncStorage::new();

    let db1 = unique_note_db();
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
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = unique_note_db();
    exec(
        &db2,
        "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
         VALUES ('local', 'same-slug', 'Local', '0000000002000-0000-dev2', '2026-01-01')",
    )
    .await;
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    let conflicts = constraint_conflicts(&result);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        conflicts[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id: "dev1".to_string(),
            position: storage.store_commit_position("dev1", 1),
        }
    );
    assert_eq!(
        conflicts[0].reason,
        HeldStorePositionReason::ConstraintConflict(vec!["unique_notes".to_string()])
    );
    assert_eq!(updated.get("dev1"), None);
    assert_eq!(
        materialized_sequences(&db2).await.get("dev1"),
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
    let storage = MockSyncStorage::new();
    let source = mixed_constraint_db();
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
    storage.store_changeset("dev1", 1, &changeset, SCHEMA_VERSION);

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

    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

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
    let storage = MockSyncStorage::new();

    let child_source = open_test_db();
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
    storage.store_changeset("dev-child", 1, &child_cs, SCHEMA_VERSION);

    let parent_source = open_test_db();
    let parent_cs = capture_bytes(
        &parent_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Parent', NULL, '0000000001000-0000-parent', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev-parent", 1, &parent_cs, SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&target, &storage, "dev-target", &ld).await;

    assert_eq!(updated.get("dev-child"), Some(&1));
    assert_eq!(updated.get("dev-parent"), Some(&1));
    assert_eq!(result.changesets_applied, 2);
    assert!(constraint_conflicts(&result).is_empty());
    assert_eq!(
        query_text(&target, "SELECT tag FROM note_tags WHERE id = 't1'").await,
        "green"
    );
}

#[tokio::test]
async fn pull_skips_changeset_from_newer_schema() {
    let storage = MockSyncStorage::new();

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Future', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION + 1);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

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
    let storage = MockSyncStorage::new();
    let forger = UserKeypair::generate();

    // A changeset stamped one schema version above the local db, signed at its own
    // position so the position check passes and the loop reaches the signature and
    // schema checks.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'ForgedFuture', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("dev1", 1, &cs, SCHEMA_VERSION + 1, None, &forger, &forger);
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    let prefix =
        crate::sync::store_commit::commit_semantic_prefix("dev1", 1, commit.value.commit_hash());
    remove_protocol_prefix(&storage, &format!("{prefix}/")).await;
    let mut forged: serde_json::Value = serde_json::from_slice(&commit.bytes).unwrap();
    forged["signature"] = serde_json::Value::String("0".repeat(128));
    storage
        .append_protocol_object(&prefix, ".json", serde_json::to_vec(&forged).unwrap())
        .await
        .unwrap();

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a forged Store commit is held before schema classification");
    assert_eq!(result.held_positions.len(), 1);
    assert_eq!(
        result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit {
                device_id: "dev1".to_string(),
                position: commit.value.position(),
            },
            reason: HeldStorePositionReason::InvalidSignature,
        }
    );
    assert!(newer_schema_positions(&result).is_empty());
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&db2).await.get("dev1"), None);
}

/// A genuine newer-schema changeset is signed, so verifying the signature before
/// the schema gate does not change its handling: it still verifies, still counts
/// as a schema skip, still holds the position, and applies once the local schema
/// catches up. The reorder rejects only forgeries, never an authentic upgrade.
#[tokio::test]
async fn a_signed_newer_schema_changeset_still_counts_as_a_schema_skip() {
    let storage = MockSyncStorage::new();
    let author = UserKeypair::generate();

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'SignedFuture', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("dev1", 1, &cs, SCHEMA_VERSION + 1, None, &author, &author);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

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
    let storage = MockSyncStorage::new();

    let db1 = open_test_db();
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
    storage.store_changeset("dev1", 1, &cs1, n);

    // seq 2 stamped one above the peer's schema version: skipped, position held.
    let cs2 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n2', 'Above N', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 2, &cs2, n + 1);

    let db2 = open_test_db();
    assert_eq!(
        db2.schema_version(),
        n,
        "both peers open the same migration ladder, so they share the wire version"
    );
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

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
        updated.get("dev1"),
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
    let storage = MockSyncStorage::new();
    let tables = test_synced_tables();
    let db1 = open_test_db();
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

    let keypair = UserKeypair::generate();
    let result = sync_for_test(
        "dev1",
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
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        position.seq,
    )
    .await
    .expect("load Store commit")
    .expect("Store commit slot");
    assert_eq!(
        commit.value.package.schema_version,
        db1.schema_version(),
        "the outgoing Store package is stamped with the database schema version",
    );
}

#[tokio::test]
async fn sync_reuses_opened_schema_models() {
    crate::sync::gate::reset_from_tables_call_count();
    crate::blob::decl::reset_from_tables_call_count();

    let storage = MockSyncStorage::new();
    let db = open_test_db();
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

    let keypair = UserKeypair::generate();
    let (_tmp, store_dir) = temp_store_dir();
    sync_for_test(
        "dev1",
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
    let storage = MockSyncStorage::new();

    // Source dev1: seq 1 references a photo blob; seq 2 is a plain note.
    let db1 = open_test_db();
    let cs1 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'One', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
             VALUES ('ph1', 'n1', 'attach', '0000000001001-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs1, SCHEMA_VERSION);
    // The photo blob is intentionally never uploaded, so seq 1's blob download
    // fails on the puller (a transient cloud unavailability, in the real world).
    let cs2 = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n2', 'Two', NULL, '0000000002000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 2, &cs2, SCHEMA_VERSION);

    // The puller declares note_photos blob-bearing, so seq 1's missing blob fails
    // while seq 2 (no blob) would succeed.
    let db2 = open_test_db_with_blob(photo_decl());
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert!(
        result.asset_downloads_failed,
        "seq 1's blob download must fail"
    );
    // The position must NOT jump to 2 past the blob-failed seq 1 — otherwise seq 1's
    // blob would never be re-fetched. It stays before seq 1 so the next cycle
    // resumes there.
    assert_ne!(
        updated.get("dev1"),
        Some(&2),
        "position must not advance past the blob-failed seq",
    );
    assert_eq!(
        updated.get("dev1"),
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
    let author = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(author.clone());

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Corrupt', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("dev1", 1, &cs, SCHEMA_VERSION, None, &author, &author);
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    remove_protocol_prefix(&storage, &format!("{}/", commit.value.package.object_key)).await;
    storage
        .append_protocol_object(
            &commit.value.package.object_key,
            ".pkg",
            cs[..cs.len() - 1].to_vec(),
        )
        .await
        .unwrap();

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a Store package that differs from its descriptor is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Package {
                device_id,
                seq: 1,
                package_hash,
            },
            reason: HeldStorePositionReason::InvalidObject(detail),
        } if device_id == "dev1"
            && *package_hash == commit.value.package.content_hash
            && detail.contains("Store package length")
    ));
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a size-mismatched changeset must not be applied",
    );
    assert_eq!(materialized_sequences(&db2).await.get("dev1"), None);
}

/// A Store commit is signed for one exact sequence. Copying its bytes beneath a
/// different immutable slot is an object collision and cannot materialize rows.
#[tokio::test]
async fn a_store_commit_replayed_at_another_sequence_is_rejected() {
    let storage = MockSyncStorage::new();
    let victim = UserKeypair::generate();

    let src = open_test_db();
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Replayed', NULL, '0000000005000-0000-dev', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("dev", 1, &cs, SCHEMA_VERSION, None, &victim, &victim);
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    remove_protocol_prefix(&storage, "store-v1/heads/dev/").await;
    let relocated_position = crate::sync::store_commit::CommitPosition {
        seq: 2,
        commit_hash: commit.value.commit_hash(),
    };
    let relocated_commit_prefix =
        crate::sync::store_commit::commit_semantic_prefix("dev", 2, relocated_position.commit_hash);
    storage
        .append_protocol_object(&relocated_commit_prefix, ".json", commit.bytes)
        .await
        .unwrap();
    let relocated_head = StoreDeviceHead::signed(
        storage.protocol_genesis_hash(),
        "dev".to_string(),
        Some(relocated_position.clone()),
        "2026-03-01T00:05:00Z".to_string(),
        &victim,
    )
    .unwrap();
    storage
        .append_protocol_object(
            &crate::sync::store_commit::head_semantic_prefix("dev", 2, relocated_head.head_hash()),
            ".json",
            relocated_head.to_bytes(),
        )
        .await
        .unwrap();

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a relocated Store commit is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit { device_id, position },
            reason: HeldStorePositionReason::WrongSlot(_),
        } if device_id == "dev" && *position == relocated_position
    ));
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a Store commit relocated to another sequence must not be applied",
    );
    assert_eq!(materialized_sequences(&db2).await.get("dev"), None);
}

/// The signed Store slot includes the device id as well as the sequence.
#[tokio::test]
async fn a_store_commit_relocated_to_another_device_is_rejected() {
    let storage = MockSyncStorage::new();
    let victim = UserKeypair::generate();

    let src = open_test_db();
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Relocated', NULL, '0000000001000-0000-devVictim', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("devVictim", 1, &cs, SCHEMA_VERSION, None, &victim, &victim);
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "devVictim",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    remove_protocol_prefix(&storage, "store-v1/heads/devVictim/").await;
    storage
        .append_protocol_object(
            &crate::sync::store_commit::commit_semantic_prefix(
                "devAttacker",
                1,
                commit.value.commit_hash(),
            ),
            ".json",
            commit.bytes,
        )
        .await
        .unwrap();
    let relocated_head = StoreDeviceHead::signed(
        storage.protocol_genesis_hash(),
        "devAttacker".to_string(),
        Some(commit.value.position()),
        "2026-03-01T00:01:00Z".to_string(),
        &victim,
    )
    .unwrap();
    storage
        .append_protocol_object(
            &crate::sync::store_commit::head_semantic_prefix(
                "devAttacker",
                1,
                relocated_head.head_hash(),
            ),
            ".json",
            relocated_head.to_bytes(),
        )
        .await
        .unwrap();

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a relocated Store commit is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit { device_id, position },
            reason: HeldStorePositionReason::WrongSlot(_),
        } if device_id == "devAttacker" && *position == commit.value.position()
    ));
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a Store commit relocated to another device must not be applied",
    );
    assert_eq!(materialized_sequences(&db2).await.get("devAttacker"), None);
}

/// A signed changeset sitting at the exact position its envelope declares is
/// untouched by the position binding — it applies normally. The check rejects
/// relocation, not authorship.
#[tokio::test]
async fn a_changeset_at_its_own_position_still_applies() {
    let storage = MockSyncStorage::new();
    let author = UserKeypair::generate();

    let src = open_test_db();
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'InPlace', NULL, '0000000001000-0000-dev', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("dev", 1, &cs, SCHEMA_VERSION, None, &author, &author);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("dev"), Some(&1));
    assert!(result.held_positions.is_empty());
}

#[tokio::test]
async fn corrupt_local_register_fails_without_materializing_the_remote_commit() {
    let storage = MockSyncStorage::new();

    let good_source = open_test_db();
    let good_cs = capture_bytes(
        &good_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-good', 'Good', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("devA", 1, &good_cs, SCHEMA_VERSION);

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
    storage.store_changeset("devB", 1, &bad_cs, SCHEMA_VERSION);

    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-bad', 'Local', NULL, 'not-a-stamp', '2026-01-01')",
    )
    .await;
    let (_tmp, ld) = temp_store_dir();
    let error = pull_into_result(&target, &storage, "devTarget", &ld)
        .await
        .expect_err("an invalid local register must fail loudly");

    assert!(matches!(error, StorePullError::Database(_)));
    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n-good'").await);
    assert_eq!(
        materialized_sequences(&target).await.get("devA"),
        Some(&1),
        "the independent commit completed before the corrupt local register was read",
    );
    assert_eq!(
        materialized_sequences(&target).await.get("devB"),
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
    let storage = MockSyncStorage::new();

    let good_source = open_test_db();
    let good_cs = capture_bytes(
        &good_source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n-good', 'Good', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("devA", 1, &good_cs, SCHEMA_VERSION);

    storage.store_changeset("devB", 1, b"not a SQLite changeset", SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into_result(&target, &storage, "devTarget", &ld)
        .await
        .expect("a malformed Store package must not fail the whole pull");

    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n-good'").await);
    assert_eq!(updated.get("devA"), Some(&1));
    assert_eq!(
        updated.get("devB"),
        None,
        "the malformed device's position is not materialized",
    );
    assert_eq!(result.changesets_applied, 1);
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id,
            position,
        } if device_id == "devB" && position.seq == 1
    ));
    assert!(matches!(
        result.held_positions[0].reason,
        HeldStorePositionReason::InvalidChangeset(_)
    ));
}

/// Repair replaces every bad physical object in the held slot before publishing
/// one valid immutable package, commit, and head for that sequence.
#[tokio::test]
async fn repaired_store_slot_resumes_the_held_device() {
    let storage = MockSyncStorage::new();

    storage.store_changeset("dev1", 1, b"not a SQLite changeset", SCHEMA_VERSION);

    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (held_positions, first) = pull_into_result(&db2, &storage, "dev2", &ld)
        .await
        .expect("a malformed Store package must not fail the whole pull");
    assert_eq!(first.changesets_applied, 0);
    assert_eq!(held_positions.get("dev1"), None);
    assert_eq!(first.held_positions.len(), 1);

    let bad_commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    remove_protocol_prefix(&storage, "store-v1/heads/dev1/").await;
    remove_protocol_prefix(&storage, "store-v1/commits/dev1/1/").await;
    remove_protocol_prefix(
        &storage,
        &format!("{}/", bad_commit.value.package.object_key),
    )
    .await;

    // The repaired slot publishes one valid Store state at the held sequence.
    let source = open_test_db();
    let cs = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Recovered', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let (updated, second) = pull_into_result(&db2, &storage, "dev2", &ld)
        .await
        .expect("resume pull");
    assert_eq!(second.changesets_applied, 1);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert!(second.held_positions.is_empty());
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "Recovered"
    );
}

#[tokio::test]
async fn blob_round_trips_through_storage_via_blob_plan() {
    let storage = MockSyncStorage::new();

    // Source: a note + a cover photo. The blob id is ≥4 chars so it forms the
    // `{ab}/{cd}` cache shard.
    let db1 = open_test_db_with_blob(photo_decl());
    let cs = capture_bytes(
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

    // The cover blob is in the cloud (uploaded when the row was first written),
    // keyed `photos/p1ab` master-scoped as the declaration maps it.
    storage
        .put_blob(
            "photos",
            "p1ab",
            crate::blob::BlobScope::Master,
            None,
            b"PHOTOBYTES".to_vec(),
        )
        .await
        .expect("put_blob");
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    // Destination pulls. A `CacheEager` photo lands in the store dir's evictable
    // cache (`storage/cache/<id>`) on pull — which coven builds from the validated id.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_t, ld) = temp_store_dir();
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    let downloaded = std::fs::read(ld.cache_blob_path("photos", "p1ab").expect("cache path"))
        .expect("downloaded photo");
    assert_eq!(downloaded, b"PHOTOBYTES");
}

#[tokio::test]
async fn update_uploads_and_downloads_new_blob_id_and_drops_old_local_copy() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let decl = photo_decl_with_blob_id_column();
    let tables = test_synced_tables_with_blob(decl.clone());

    let db1 = open_test_db_with_blob(decl.clone());
    let (_tmp1, ld1) = temp_store_dir();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', 8, '0000000001000-0000-dev1', '2026-01-01', 'oldaaaa')",
    )
    .await;
    // The rows above are seed (raw `exec`, unjournaled), so the captured changeset
    // is just the UPDATE — the update-blob-id path under test.
    store_local(&ld1, "newaaaa", b"NEW-BLOB").await;
    let outgoing = capture_bytes(
        &db1,
        &[&format!(
            "UPDATE note_photos SET cloud_path = 'newaaaa', hash = '{}', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'p-row'",
            crate::blob::content_hash(b"NEW-BLOB"),
        )],
    )
    .await;

    let result = sync_for_test(
        "dev1",
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
    .expect("sync update");
    assert!(result.is_some(), "the update publishes a Store commit");
    assert!(
        storage.exists("photos/newaaaa").await.unwrap(),
        "push uploads the UPDATE's new blob id"
    );
    assert!(
        !storage.exists("photos/oldaaaa").await.unwrap(),
        "push must not upload the UPDATE's old blob id"
    );

    let db2 = open_test_db_with_blob(decl);
    let (_tmp2, ld2) = temp_store_dir();
    exec(
        &db2,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev2', '2026-01-01')",
    )
    .await;
    exec(
        &db2,
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', 8, '0000000001000-0000-dev2', '2026-01-01', 'oldaaaa')",
    )
    .await;
    crate::local_blob::write_atomic(
        &ld2.cache_blob_path("photos", "oldaaaa")
            .expect("old cache path"),
        b"OLD-BLOB",
    )
    .await
    .expect("seed old cache");

    let (_updated, pull) = pull_into(&db2, &storage, "dev2", &ld2).await;
    assert_eq!(pull.changesets_applied, 1);
    assert!(
        ld2.cache_blob_path("photos", "newaaaa")
            .expect("new cache path")
            .exists(),
        "pull downloads the UPDATE's new blob id"
    );
    assert!(
        !ld2.cache_blob_path("photos", "oldaaaa")
            .expect("old cache path")
            .exists(),
        "pull cleanup drops the UPDATE's old blob id"
    );
}

#[tokio::test]
async fn update_to_null_drops_old_local_blob_copy() {
    let storage = MockSyncStorage::new();
    let decl = photo_decl_with_blob_id_column();
    let db1 = open_test_db_with_blob(decl.clone());
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01', 'oldnull')",
    )
    .await;
    // The rows above are seed (raw `exec`, unjournaled), so the captured changeset
    // is just the UPDATE-to-NULL under test.
    let cs = capture_bytes(
        &db1,
        &[
            "UPDATE note_photos SET cloud_path = NULL, _updated_at = '0000000002000-0000-dev1' \
          WHERE id = 'p-row'",
        ],
    )
    .await;
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db_with_blob(decl);
    let (_tmp, ld) = temp_store_dir();
    exec(
        &db2,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev2', '2026-01-01')",
    )
    .await;
    exec(
        &db2,
        "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', '0000000001000-0000-dev2', '2026-01-01', 'oldnull')",
    )
    .await;
    crate::local_blob::write_atomic(
        &ld.cache_blob_path("photos", "oldnull")
            .expect("old cache path"),
        b"OLD-BLOB",
    )
    .await
    .expect("seed old cache");

    let (_updated, pull) = pull_into(&db2, &storage, "dev2", &ld).await;
    assert_eq!(pull.changesets_applied, 1);
    assert!(
        !ld.cache_blob_path("photos", "oldnull")
            .expect("old cache path")
            .exists(),
        "pull cleanup drops the old blob when UPDATE removes the blob id"
    );
}

/// A `CacheLazy` blob's row still crosses to the puller, but its bytes are NOT
/// downloaded on pull (it streams on demand, fetched on first read) — the opposite
/// pull outcome from the `CacheEager` round-trip above. The split is declared:
/// `note_photos` carries a user-provided · `CacheLazy` blob here.
#[tokio::test]
async fn user_provided_blob_is_not_pushed_inline_and_not_downloaded_on_pull() {
    let keypair = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(keypair.clone());
    let audio_tables = || {
        test_synced_tables_with_blob(BlobDecl::new(
            "audio",
            Provenance::UserProvided,
            CacheFill::CacheLazy,
        ))
    };

    // Source: a shared note + an audio row, declared user-provided · CacheLazy.
    let db1 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    storage
        .put_blob(
            "audio",
            "audio1",
            crate::blob::BlobScope::Master,
            None,
            b"AUDIO-PAYLOAD".to_vec(),
        )
        .await
        .expect("plant audio blob before publish");

    // Drive the real push path. The inline push uploads only host-provided blobs, so
    // the user-provided audio is NOT uploaded here — it goes via the durable outbox in
    // the make_remote flow, not this changeset-blob upload.
    let (_t1, ld1) = temp_store_dir();
    let result = sync_for_test(
        "dev1",
        &db1,
        &audio_tables(),
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
    assert!(result.is_some(), "the audio row publishes a Store commit");

    // The user-provided blob was NOT uploaded by the inline push.
    assert_eq!(
        storage
            .get_blob(
                "audio",
                None,
                "audio1",
                crate::blob::BlobScope::Master,
                None
            )
            .await
            .expect("audio blob remains present"),
        b"AUDIO-PAYLOAD",
        "the inline push must not rewrite a user-provided blob",
    );

    // Destination pulls.
    let db2 = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let (_t, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    // The row applied and the position advanced — the CacheLazy blob never blocks the
    // apply, and its absence is not a download failure.
    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithAudio",
        "the row carrying the CacheLazy blob still reaches the peer",
    );
    // ...but the blob was NOT downloaded to the puller's cache: CacheLazy is fetched
    // on first read, not eagerly on pull.
    assert!(
        !ld.pinned_blob_path("audio", "audio1").unwrap().exists()
            && !ld.cache_blob_path("audio", "audio1").unwrap().exists(),
        "a CacheLazy blob must NOT be downloaded on pull — it stays in the cloud for on-demand fetch",
    );
}

#[tokio::test]
async fn user_provided_blob_with_external_ref_aborts_before_changeset_publish() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let (tmp, ld) = temp_store_dir();
    let external = tmp.path().join("audio.flac");
    std::fs::write(&external, b"local audio").expect("write external file");
    db.register_external_blob("audio1", "audio", &external, 11)
        .await
        .expect("register external ref");
    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    let result = sync_for_test(
        "dev1",
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
        &UserKeypair::generate(),
        &ld,
    )
    .await;
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("local user-provided blob must abort publish"),
    };

    assert!(
        err.contains("local") && err.contains("audio/audio1"),
        "the error must name the local user-provided blob: {err}",
    );
    assert!(
        crate::sync::store_objects::list_visible_heads(&storage, storage.protocol_genesis_hash(),)
            .await
            .expect("list Store heads")
            .heads
            .is_empty(),
        "failed publish created no Store head",
    );
}

#[tokio::test]
async fn missing_remote_user_provided_blob_aborts_before_changeset_publish() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let (_tmp, ld) = temp_store_dir();

    let result = sync_for_test(
        "dev1",
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
        &UserKeypair::generate(),
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
        crate::sync::store_objects::list_visible_heads(&storage, storage.protocol_genesis_hash(),)
            .await
            .expect("list Store heads")
            .heads
            .is_empty(),
        "failed publish created no Store head",
    );
}

#[tokio::test]
async fn present_remote_user_provided_blob_can_publish_changeset() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
    storage
        .put_blob(
            "audio",
            "audio1",
            crate::blob::BlobScope::Master,
            None,
            b"AUDIO-PAYLOAD".to_vec(),
        )
        .await
        .expect("plant remote blob");
    let outgoing = capture_bytes(
        &db,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('audio1', 'n1', 'audio', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let (_tmp, ld) = temp_store_dir();

    let result = sync_for_test(
        "dev1",
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
        &UserKeypair::generate(),
        &ld,
    )
    .await
    .expect("remote user-provided blob is publishable");
    assert!(
        result.is_some(),
        "the remote blob row publishes a Store commit"
    );

    assert_eq!(
        crate::sync::store_objects::list_visible_heads(&storage, storage.protocol_genesis_hash())
            .await
            .expect("list Store heads")
            .heads[0]
            .value
            .position
            .as_ref()
            .expect("active Store head")
            .seq,
        1,
        "publish advances the head after the remote blob exists",
    );
}

#[tokio::test]
async fn delete_ref_does_not_require_remote_blob_to_publish_changeset() {
    let storage = MockSyncStorage::new();
    let db = open_test_db_with_blob(BlobDecl::new(
        "audio",
        Provenance::UserProvided,
        CacheFill::CacheLazy,
    ));
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
        "dev1",
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
        &UserKeypair::generate(),
        &ld,
    )
    .await
    .expect("delete does not require the removed blob to exist remotely");
    assert!(result.is_some(), "the delete publishes a Store commit");

    assert_eq!(
        crate::sync::store_objects::list_visible_heads(&storage, storage.protocol_genesis_hash())
            .await
            .expect("list Store heads")
            .heads[0]
            .value
            .position
            .as_ref()
            .expect("active Store head")
            .seq,
        1,
        "delete publishes even when the removed blob is absent remotely",
    );
}

/// A changeset that references a blob whose local file is missing must abort the
/// cycle, not skip the upload and publish the row anyway. `sync` returns the
/// outgoing changeset for the caller to push; aborting here (Err) is what keeps
/// the caller from publishing a row whose blob was never uploaded — every puller
/// would 404 on that blob forever.
#[tokio::test]
async fn sync_aborts_when_a_referenced_blob_file_is_missing() {
    let storage = MockSyncStorage::new();

    // A shared note + a host-provided cover row, but the cover is deliberately never
    // stored in the local store, so the inline push finds nothing in either the local
    // store or the cache.
    let db1 = open_test_db_with_blob(photo_decl());
    let outgoing = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1ab', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;

    let keypair = UserKeypair::generate();
    let (_t1, ld1) = temp_store_dir();
    let result = sync_for_test(
        "dev1",
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
    let err = result.err();
    assert!(
        err.as_deref()
            .is_some_and(|error| error.contains("p1ab") && error.contains("blob missing")),
        "an unstaged blob must abort Store publication, got {err:?}",
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
    const COVER_KEY: &str = "photos/n1/cover-p1cover.jpg";

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
    assert_eq!(
        home.get(COVER_KEY).as_deref(),
        Some(bytes.as_slice()),
        "the first push uploads the cover",
    );

    // This device now holds no copy of the blob at all: the push moved the local-store
    // copy into the cache, and the cache copy is then evicted.
    local_files::drop_blob(&ld, "photos", "p1cover")
        .await
        .expect("drop any local-store copy");
    let cached = ld.cache_blob_path("photos", "p1cover").expect("cache path");
    if cached.exists() {
        std::fs::remove_file(&cached).expect("evict the cached copy");
    }

    // The row is re-emitted. The blob has no local bytes to upload — and needs none.
    let result = sync_for_test(
        "dev1",
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
        home.get(COVER_KEY).as_deref(),
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
        CloudCipher::Encrypted(EncryptionService::from_key([5u8; 32])),
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
        "dev1",
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

    // The blob lands at the readable key, not the hashed shard.
    assert!(
        storage
            .cloud_home()
            .exists("photos/n1/cover-p1cover.jpg")
            .await
            .expect("exists at readable key"),
        "the blob must land at the readable cloud_path key",
    );
    let hashed = CloudSyncStorage::blob_key(
        BlobPathScheme::Hashed,
        "photos",
        Some(&storage.self_uploader()),
        "p1cover",
        None,
    )
    .expect("hashed key");
    assert!(
        !storage
            .cloud_home()
            .exists(&hashed)
            .await
            .expect("exists at hashed key"),
        "the hashed shard key must be absent under the plain scheme",
    );

    // Device B: a fresh DB and its own store dir, same cloud + plain scheme,
    // pulls and downloads the cover from the readable key.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld) = temp_store_dir();
    let (_updated, result) = pull_cloud_into(&db2, &db1, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(ld.cache_blob_path("photos", "p1cover").expect("cache path"))
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
        "dev1",
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

/// Replacing a blob-bearing row on a browsable home writes a NEW cloud object; it never
/// overwrites the one it replaces.
///
/// A blob id names one immutable byte-string, and a host-provided blob's `cloud_path`
/// must name its blob — so the replacement's fresh blob id carries a fresh path, hence a
/// fresh key. The object the replaced blob occupies is a different object, and it stands
/// at its own key until its tombstone is collected.
///
/// Device A publishes a cover and device B pulls it; A then replaces the row with a fresh
/// one — new blob id, new bytes in the local store, a path naming the new blob — and B
/// pulls again. Both objects stand, each holding its own blob's bytes, and B serves the
/// replacement.
#[tokio::test]
async fn plain_scheme_replacing_a_blob_writes_a_new_object_at_its_own_key() {
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
    const OLD_KEY: &str = "photos/n1/cover-p1cover.jpg";
    const NEW_KEY: &str = "photos/n1/cover-p2cover.jpg";

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
    assert_eq!(
        home.get(OLD_KEY).as_deref(),
        Some(old_bytes.as_slice()),
        "the first push puts the cover at the key its path names",
    );

    // Device B takes the cover before the replacement, so it is a peer holding the
    // replaced blob when the new one arrives.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    pull_cloud_into(&db2, &db1, &storage, "dev2", &ld2).await;

    // Replace the cover: a new blob, whose bytes the host stages in the local store,
    // carried by a fresh row whose readable path names it; the replaced row and its local
    // copy go away.
    store_local(&ld1, "p2cover", new_bytes).await;
    local_files::drop_blob(&ld1, "photos", "p1cover")
        .await
        .expect("drop the replaced blob's local copy");
    let outgoing = capture_bytes(
        &db1,
        &[
            "DELETE FROM note_photos WHERE id = 'p1cover'",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p2cover', 'n1', 'cover', {}, '{}', 'n1/cover-p2cover.jpg', \
                 '0000000002000-0000-dev1', '2026-01-01')",
                new_bytes.len(),
                crate::blob::content_hash(new_bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 1, &keypair, &ld1).await;

    assert_eq!(
        home.get(NEW_KEY).as_deref(),
        Some(new_bytes.as_slice()),
        "the replacement writes its own cloud object",
    );
    assert_eq!(
        home.get(OLD_KEY).as_deref(),
        Some(old_bytes.as_slice()),
        "the replaced blob's object is untouched — it is tombstoned, not overwritten, so a \
         device that has not yet pulled the replacement still reads the bytes its row names",
    );

    // Device B pulls the replacement. Its download verifies the object against the new
    // row's content hash, so an object holding the replaced bytes would fail the pull.
    let (_updated, result) = pull_cloud_into(&db2, &db1, &storage, "dev2", &ld2).await;

    assert!(
        !result.asset_downloads_failed,
        "device B must download a cover matching the row's hash",
    );
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(
        ld2.cache_blob_path("photos", "p2cover")
            .expect("cache path"),
    )
    .expect("device B cached the replacement cover");
    assert_eq!(
        cached,
        new_bytes.as_slice(),
        "device B serves the replacement bytes, not the cover it replaced",
    );
}

/// Two devices replacing one blob at once do not contend for a cloud object.
///
/// Each device mints its own blob id for the bytes it stored, and a blob's path names its
/// blob — so the two replacements carry two different keys and write two different
/// objects. There is no key both devices write, so the bucket cannot end up holding one
/// device's bytes while the row names the other's. The row is a genuine last-write-wins
/// conflict (both devices repoint the same primary key), and whichever repointing wins,
/// the object it names is in the bucket, unoverwritten, for every peer to verify and read.
#[tokio::test]
async fn plain_scheme_two_devices_replacing_one_blob_write_two_objects() {
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

    // Device A publishes the original cover; device B pulls it. Both now hold row `ph1`.
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

    let db_b = open_test_db_with_blob(replaceable_photo_decl());
    let (_tb, ld_b) = temp_store_dir();
    pull_cloud_into(&db_b, &db_a, &storage, "dev2", &ld_b).await;

    // Both devices repoint `ph1` before seeing the other's change — the same row, two new
    // blobs. Each blob id is fresh, so each path names a different blob and keys a
    // different object. Device B's write carries the later `_updated_at`, so it is the
    // row's last-write-wins winner.
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
    store_local(&ld_b, "pBcover", from_b).await;
    let outgoing_b = capture_bytes(
        &db_b,
        &[&format!(
            "UPDATE note_photos SET blob_id = 'pBcover', cloud_path = 'n1/cover-pBcover.jpg', \
             size = {}, hash = '{}', _updated_at = '0000000003000-0000-dev2' WHERE id = 'ph1'",
            from_b.len(),
            crate::blob::content_hash(from_b),
        )],
    )
    .await;
    push_cycle(&db_a, &tables, &storage, outgoing_a, 1, &keypair, &ld_a).await;
    // Device B's own first published changeset, so its local sequence starts at zero.
    push_cycle_as(
        "dev2", &db_b, &tables, &storage, outgoing_b, 0, &keypair, &ld_b,
    )
    .await;

    // Neither replacement overwrote the other: both objects stand, each holding the bytes
    // of the blob its key names. Under a key that did not name its blob, these two writes
    // would have been one object, and its bytes would be whichever device the bucket saw
    // last — not necessarily the device the row's conflict resolved to.
    assert_eq!(
        home.get("photos/n1/cover-pAcover.jpg").as_deref(),
        Some(from_a.as_slice()),
        "device A's replacement is at its own key",
    );
    assert_eq!(
        home.get("photos/n1/cover-pBcover.jpg").as_deref(),
        Some(from_b.as_slice()),
        "device B's replacement is at its own key",
    );

    // A third device pulls every changeset. Whichever repointing the row converges to, the
    // object that row names is in the bucket holding that blob's bytes — so every download
    // verifies against its row's hash and nothing is left unsatisfiable. Under a key that
    // did not name its blob, the surviving row could name bytes the other device had
    // already overwritten, and no retry would ever resolve it.
    let db_c = open_test_db_with_blob(replaceable_photo_decl());
    let (_tc, ld_c) = temp_store_dir();
    let (_updated, result) = pull_cloud_into(&db_c, &db_a, &storage, "dev3", &ld_c).await;
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
    let expected = match winner.as_str() {
        "pAcover" => from_a.as_slice(),
        "pBcover" => from_b.as_slice(),
        other => panic!("the row must converge to one of the two replacements, got {other:?}"),
    };
    let cached = std::fs::read(ld_c.cache_blob_path("photos", &winner).expect("cache path"))
        .expect("the third device cached the cover its row names");
    assert_eq!(
        cached, expected,
        "the bytes the surviving row names are the bytes in the bucket — the two \
         replacements never contended for one object",
    );
}

/// A device replaying a changeset written BEFORE a replacement can still fetch that
/// changeset's blob.
///
/// The replacement writes a new key, so the superseded blob's object is still standing at
/// its own key — tombstoned, and held for the deletion grace, which is exactly the
/// convergence window coven promises a device that has been away. The old changeset's
/// content hash is therefore satisfiable, and the device applies it and then the
/// replacement, rather than wedging on a changeset whose bytes were overwritten.
#[tokio::test]
async fn plain_scheme_a_changeset_older_than_a_replacement_still_finds_its_blob() {
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

    // The replacement is published while the laggard is away, so the laggard has never
    // seen either changeset when it finally pulls.
    store_local(&ld1, "p2cover", new_bytes).await;
    local_files::drop_blob(&ld1, "photos", "p1cover")
        .await
        .expect("drop the replaced blob's local copy");
    let outgoing = capture_bytes(
        &db1,
        &[
            "DELETE FROM note_photos WHERE id = 'p1cover'",
            &format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, cloud_path, _updated_at, created_at) \
                 VALUES ('p2cover', 'n1', 'cover', {}, '{}', 'n1/cover-p2cover.jpg', \
                 '0000000002000-0000-dev1', '2026-01-01')",
                new_bytes.len(),
                crate::blob::content_hash(new_bytes),
            ),
        ],
    )
    .await;
    push_cycle(&db1, &tables, &storage, outgoing, 1, &keypair, &ld1).await;

    // The laggard pulls from zero: it applies the pre-replacement changeset first, whose
    // row names the replaced blob. Its bytes are still at their own key.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    let (_positions, result) = pull_cloud_into(&db2, &db1, &storage, "dev2", &ld2).await;

    assert!(
        !result.asset_downloads_failed,
        "the changeset written before the replacement must still find the blob it names — \
         the replacement wrote a different key and left this object standing",
    );
    assert_eq!(
        result.changesets_applied, 2,
        "both changesets apply: the laggard is not wedged at the one it cannot satisfy",
    );
    let cached = std::fs::read(
        ld2.cache_blob_path("photos", "p2cover")
            .expect("cache path"),
    )
    .expect("the laggard cached the current cover");
    assert_eq!(
        cached,
        new_bytes.as_slice(),
        "having caught up, the laggard serves the replacement",
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
    const AUDIO_KEY: &str = "photos/n1/Sonata No. 3.flac";

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

    assert_eq!(
        home.get(AUDIO_KEY).as_deref(),
        Some(bytes.as_slice()),
        "the blob lands at the consumer's own readable name, with no blob id in it",
    );

    // A peer pulls it off that readable key and verifies it against the row's hash.
    let db2 = open_test_db_with_blob(write_once_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    let (_positions, result) = pull_cloud_into(&db2, &db1, &storage, "dev2", &ld2).await;
    assert!(!result.asset_downloads_failed);
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(
        ld2.cache_blob_path("photos", "f1audio")
            .expect("cache path"),
    )
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
    const AUDIO_KEY: &str = "photos/n1/Sonata No. 3.flac";

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
        "dev1",
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
        home.get(AUDIO_KEY).as_deref(),
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
    const OLD_KEY: &str = "photos/n1/cover-p1cover.jpg";
    const NEW_KEY: &str = "photos/n1/cover-p2cover.jpg";

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
    assert_eq!(
        home.get(OLD_KEY).as_deref(),
        Some(old_bytes.as_slice()),
        "the first push puts the cover at the key its path names",
    );

    // Device B takes the cover before the replacement, so it is a peer holding the
    // replaced blob when the new one arrives.
    let db2 = open_test_db_with_blob(replaceable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    pull_cloud_into(&db2, &db1, &storage, "dev2", &ld2).await;

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

    assert_eq!(
        home.get(NEW_KEY).as_deref(),
        Some(new_bytes.as_slice()),
        "the repointed row's blob writes its own cloud object",
    );
    assert_eq!(
        home.get(OLD_KEY).as_deref(),
        Some(old_bytes.as_slice()),
        "the replaced blob's object is not overwritten — it is tombstoned and stands until \
         the GC collects it",
    );

    // Device B pulls the repointing. Its download verifies the object against the new
    // row's content hash, so serving it the replaced bytes would fail the pull outright.
    let (_updated, result) = pull_cloud_into(&db2, &db1, &storage, "dev2", &ld2).await;

    assert!(
        !result.asset_downloads_failed,
        "device B must download a cover matching the row's hash",
    );
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(
        ld2.cache_blob_path("photos", "p2cover")
            .expect("cache path"),
    )
    .expect("device B cached the replacement cover");
    assert_eq!(
        cached,
        new_bytes.as_slice(),
        "device B serves the replacement bytes, not the cover it replaced",
    );
    assert!(
        !ld2.cache_blob_path("photos", "p1cover")
            .expect("cache path")
            .exists(),
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
    const OLD_KEY: &str = "photos/n1/cover-p1cover.jpg";

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
        "dev1",
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
        home.get(OLD_KEY).as_deref(),
        Some(old_bytes.as_slice()),
        "the replaced blob's object is untouched — the cycle aborted before any upload",
    );
}

/// Push one cycle's captured changeset the way the sync loop does: `service::sync`
/// prepares (and uploads the host-provided blobs of) the gated changeset, then
/// publishes the resulting immutable Store objects, as `device`.
async fn push_cycle_as(
    device: &str,
    db: &crate::database::Database,
    tables: &[SyncedTable],
    storage: &CloudSyncStorage,
    outgoing: Vec<u8>,
    local_seq: u64,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) {
    let result = sync_for_test(
        device,
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

/// [`push_cycle_as`] for the single-device tests, which all push as `dev1`.
async fn push_cycle(
    db: &crate::database::Database,
    tables: &[SyncedTable],
    storage: &CloudSyncStorage,
    outgoing: Vec<u8>,
    local_seq: u64,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) {
    push_cycle_as(
        "dev1", db, tables, storage, outgoing, local_seq, keypair, store_dir,
    )
    .await;
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
        "dev1",
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
    let blob_key = CloudSyncStorage::blob_key(
        BlobPathScheme::Hashed,
        "photos",
        Some(&storage.self_uploader()),
        "p1cover",
        None,
    )
    .expect("hashed key");
    let at_rest = storage
        .cloud_home()
        .read(&blob_key)
        .await
        .expect("blob present in cloud");
    assert_ne!(
        at_rest, plaintext,
        "blob must be encrypted at rest in the cloud"
    );

    // Device B: a fresh DB and its own store dir, same cloud + key + declaration.
    let db2 = open_test_db_with_blob(decl());
    let (_t, ld) = temp_store_dir();
    let (updated, result) = pull_cloud_into(&db2, &db1, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'").await,
        "WithPhoto"
    );
    // A `CacheEager` cover lands in B's evictable cache on pull.
    let downloaded = std::fs::read(ld.cache_blob_path("photos", "p1cover").expect("cache path"))
        .expect("device B downloaded photo");
    assert_eq!(
        downloaded, plaintext,
        "device B must recover the source bytes after decrypting with the shared key"
    );

    // The pull recorded, atomically with applying the row, that A uploaded this
    // blob — so a later read (after a cache eviction) keys it under A's prefix
    // without a listing scan.
    assert_eq!(
        db2.blob_uploader("photos", "p1cover")
            .await
            .expect("read uploader index"),
        Some(hex::encode(keypair.public_key())),
        "device B's uploader index names A as the blob's uploader",
    );
}

/// The inline push, after uploading a host-provided blob, decides what to do with
/// the local-store copy by the blob's `CacheFill`, not its provenance: `CacheEager`
/// warms the evictable cache (the first read is a local hit), while `CacheLazy`
/// drops the local copy outright (the cloud has the bytes; a later read fetches
/// them). Either way the local store must NOT keep a Remote blob's bytes — that
/// would read as Local. Two host-provided blobs in one subtree, one of each fill,
/// prove the split is driven by fill alone.
#[tokio::test]
async fn inline_push_warms_cache_for_eager_and_drops_local_for_lazy() {
    let storage = MockSyncStorage::new();

    // Both children host-provided, differing only in fill: the photo is CacheEager,
    // the cover CacheLazy. Both inherit the `notes` gate, so a shared note carries
    // both through the inline push in one cycle.
    let eager_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager);
    let lazy_decl = || BlobDecl::new("covers", Provenance::HostProvided, CacheFill::CacheLazy);

    let db1 = open_test_db_with_user_and_host_blobs(eager_decl(), lazy_decl());
    let (_t1, ld1) = temp_store_dir();
    exec(
        &db1,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'WithBlobs', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
         VALUES ('peager01', 'n1', 'cover', 11, '0000000001000-0000-dev1', '2026-01-01')",
    )
    .await;
    exec(
        &db1,
        "INSERT INTO note_covers (id, note_id, size, _updated_at, created_at) \
         VALUES ('clazy001', 'n1', 10, '0000000001001-0000-dev1', '2026-01-01')",
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
    let keypair = UserKeypair::generate();
    let hlc = crate::sync::hlc::Hlc::new("dev1".to_string());
    let cipher = std::sync::RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [31u8; 32],
    )));
    bind_mock_store_protocol(&db1, &storage, "dev1").await;
    cycle::run_single_sync_cycle(
        &storage,
        "test-lib",
        "dev1",
        &hlc,
        &crate::clock::SystemClock,
        &db1,
        &cipher,
        &PendingRotation::none(),
        &keypair,
        None,
        &ld1,
        None,
        None,
    )
    .await
    .expect("cycle");

    // Both blobs reached the cloud — the inline push uploads regardless of fill.
    assert!(
        storage
            .get_blob(
                "photos",
                None,
                "peager01",
                crate::blob::BlobScope::Master,
                None
            )
            .await
            .is_ok(),
        "the eager blob must be uploaded",
    );
    assert!(
        storage
            .get_blob(
                "covers",
                None,
                "clazy001",
                crate::blob::BlobScope::Master,
                None
            )
            .await
            .is_ok(),
        "the lazy blob must be uploaded",
    );

    // CacheEager: warmed into the cache, gone from the local store. The first read
    // is a local cache hit.
    assert!(
        ld1.cache_blob_path("photos", "peager01").unwrap().exists(),
        "an eager blob's local copy is moved into the cache",
    );
    assert!(
        !ld1.local_blob_path("photos", "peager01").unwrap().exists(),
        "a Remote blob's bytes must not stay in the local store (would read as Local)",
    );

    // CacheLazy: dropped from the local store, NOT placed in the cache. A later read
    // fetches it from the cloud.
    assert!(
        !ld1.local_blob_path("covers", "clazy001").unwrap().exists(),
        "a lazy blob's local copy is dropped after upload",
    );
    assert!(
        !ld1.cache_blob_path("covers", "clazy001").unwrap().exists(),
        "a lazy blob is not pre-primed into the cache — it streams on first read",
    );
}

/// When a peer applies a changeset that DELETEs a blob-bearing row (a gate retract
/// or a genuine delete), it drops that blob's local copy — both cache folders and the
/// local store — or it would leak forever once the row is gone. The peer drops only
/// its own local copy; it never writes a cloud tombstone.
#[tokio::test]
async fn applying_a_blob_bearing_delete_drops_the_local_copy() {
    let storage = MockSyncStorage::new();

    // Source dev1: a note + a CacheEager cover row, the cover present in the cloud.
    let db1 = open_test_db_with_blob(photo_decl());
    let cs1 = capture_bytes(
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
    storage
        .put_blob(
            "photos",
            "pdel1234",
            crate::blob::BlobScope::Master,
            None,
            b"COVERBYTES".to_vec(),
        )
        .await
        .expect("plant cover");
    storage.store_changeset("dev1", 1, &cs1, SCHEMA_VERSION);

    // dev2 pulls → the CacheEager cover lands in the evictable cache.
    let db2 = open_test_db_with_blob(photo_decl());
    let (_t, ld) = temp_store_dir();
    pull_into(&db2, &storage, "dev2", &ld).await;
    assert!(
        ld.cache_blob_path("photos", "pdel1234").unwrap().exists(),
        "the cover lands in the evictable cache after the first pull",
    );

    // dev1 deletes the cover row; dev2 pulls the DELETE.
    let cs2 = capture_bytes(&db1, &["DELETE FROM note_photos WHERE id = 'pdel1234'"]).await;
    storage.store_changeset("dev1", 2, &cs2, SCHEMA_VERSION);
    let (_positions, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 1, "the DELETE changeset applied");
    assert!(
        !ld.pinned_blob_path("photos", "pdel1234").unwrap().exists()
            && !ld.cache_blob_path("photos", "pdel1234").unwrap().exists(),
        "applying the blob-bearing DELETE drops the cache copies",
    );
}

#[tokio::test]
async fn local_blob_cleanup_intent_survives_restart_after_position_commit() {
    let storage = MockSyncStorage::new();
    let cleanup_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy);
    let source = open_test_db_with_blob(cleanup_decl());
    exec(
        &source,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('cleanup01', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01');",
    )
    .await;
    let deletion =
        capture_bytes(&source, &["DELETE FROM note_photos WHERE id = 'cleanup01'"]).await;
    storage.store_changeset("dev1", 1, &deletion, SCHEMA_VERSION);

    let database_dir = tempfile::tempdir().expect("database temp dir");
    let database_path = database_dir.path().join("store.db");
    let target = open_blob_test_db_at(&database_path, cleanup_decl());
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev2', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('cleanup01', 'n1', 'cover', '0000000001000-0000-dev2', '2026-01-01');",
    )
    .await;

    let (_store_tmp, store_dir) = temp_store_dir();
    let obstructing_file = store_dir.as_ref().join("storage");
    std::fs::write(&obstructing_file, b"not a directory").expect("obstruct cleanup paths");

    let (updated, first) = pull_into(&target, &storage, "dev2", &store_dir).await;
    assert_eq!(first.changesets_applied, 1, "first pull: {first:?}");
    assert!(
        !first.asset_downloads_failed,
        "post-commit cleanup does not mean a pre-apply blob download failed",
    );
    assert!(first.local_blob_cleanup_pending);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        materialized_sequences(&target).await.get("dev1"),
        Some(&1),
        "filesystem cleanup does not hold the materialized position",
    );
    assert!(!row_exists(&target, "SELECT 1 FROM note_photos WHERE id = 'cleanup01'").await);
    let pending_before_restart: i64 = target
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM local_cleanup_intents", [], |row| {
                row.get(0)
            })
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(pending_before_restart, 1);

    tokio::task::spawn_blocking(move || drop(target))
        .await
        .expect("close database before restart");
    std::fs::remove_file(&obstructing_file).expect("restore cleanup paths");

    let restarted = open_blob_test_db_at(&database_path, cleanup_decl());
    assert_eq!(
        materialized_sequences(&restarted).await.get("dev1"),
        Some(&1),
    );
    let (_updated, second) = pull_into(&restarted, &storage, "dev2", &store_dir).await;
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
    let storage = std::sync::Arc::new(MockSyncStorage::new());
    let decl = BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy)
        .with_id_column("blob_id");
    let target = open_test_db_with_blob(decl);
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
                "INSERT INTO local_cleanup_intents (namespace, blob_id) \
                 VALUES ('photos', 'cleanup-race')",
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
    let cleanup = tokio::spawn(async move {
        pull_into(&pull_db, pull_storage.as_ref(), "dev2", &pull_store_dir).await
    });

    reached_filesystem.notified().await;
    let tables = target.synced_tables().to_vec();
    let update_tables = tables.clone();
    let host_write = target
        .call(move |conn| {
            crate::database::Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                tx.execute(
                    "INSERT INTO note_photos \
                         (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                         VALUES ('new-row', 'n1', 'cover', 9, NULL, 'cleanup-race', \
                                 '0000000002000-0000-dev2', '2026-01-01')",
                    [],
                )
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
        })
        .await;
    let host_update = target
        .call(move |conn| {
            crate::database::Database::run_pending_journaled_transaction_on(
                conn,
                &update_tables,
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
                "INSERT INTO local_cleanup_intents (namespace, blob_id) \
                 VALUES ('photos', 'shared-intent')",
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
    let host_re_reference = target
        .call(move |conn| {
            crate::database::Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                tx.execute(
                    "INSERT INTO note_photos \
                     (id, note_id, kind, size, hash, blob_id, _updated_at, created_at) \
                     VALUES ('blocked-row', 'n1', 'cover', 9, NULL, 'shared-intent', \
                             '0000000002000-0000-dev2', '2026-01-01')",
                    [],
                )
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
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
    let storage = MockSyncStorage::new();
    let decl = photo_decl_with_blob_id_column();

    let db1 = open_test_db_with_blob(decl.clone());
    let cs1 = capture_bytes(
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
    storage
        .put_blob(
            "photos",
            "sharedblob",
            crate::blob::BlobScope::Master,
            None,
            b"SHARED-BYTES".to_vec(),
        )
        .await
        .expect("plant shared blob");
    storage.store_changeset("dev1", 1, &cs1, SCHEMA_VERSION);

    let db2 = open_test_db_with_blob(decl);
    let (_tmp, ld) = temp_store_dir();
    let (_positions, result) = pull_into(&db2, &storage, "dev2", &ld).await;
    assert_eq!(result.changesets_applied, 1);
    assert!(
        ld.cache_blob_path("photos", "sharedblob").unwrap().exists(),
        "the shared CacheEager blob lands in the cache",
    );

    storage
        .put_blob(
            "photos",
            "newblob",
            crate::blob::BlobScope::Master,
            None,
            b"NEW-BYTES".to_vec(),
        )
        .await
        .expect("plant replacement blob");
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
    storage.store_changeset("dev1", 2, &cs2, SCHEMA_VERSION);

    let (_updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

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
        ld.cache_blob_path("photos", "sharedblob").unwrap().exists(),
        "a blob-changing update must not drop a copy another live row still references",
    );
    assert!(
        ld.cache_blob_path("photos", "newblob").unwrap().exists(),
        "the replacement blob lands in the cache",
    );
}

#[tokio::test]
async fn pull_rejects_store_commit_missing_its_signature_when_chain_exists() {
    let founder = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(founder.clone());
    let founder_pk = hex::encode(founder.public_key());

    let entry = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &founder_pk, 1, entry).await;
    publish_membership_chain_head(&storage, &chain, &founder).await;

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_with_grant(
        "dev1",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &founder_pk, 1)),
    );
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    let prefix =
        crate::sync::store_commit::commit_semantic_prefix("dev1", 1, commit.value.commit_hash());
    remove_protocol_prefix(&storage, &format!("{prefix}/")).await;
    let mut unsigned: serde_json::Value = serde_json::from_slice(&commit.bytes).unwrap();
    unsigned
        .as_object_mut()
        .expect("Store commit is a JSON object")
        .remove("signature");
    storage
        .append_protocol_object(&prefix, ".json", serde_json::to_vec(&unsigned).unwrap())
        .await
        .unwrap();

    let db2 = open_test_db();
    let (_, result) = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a Store commit without its required signature is held");
    assert_eq!(result.held_positions.len(), 1);
    assert!(matches!(
        &result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit { device_id, position },
            reason: HeldStorePositionReason::InvalidObject(detail),
        } if device_id == "dev1"
            && *position == commit.value.position()
            && detail.contains("missing field `signature`")
    ));
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&db2).await.get("dev1"), None,);
}

/// Owner anchoring (issue #95/#102): a puller with a pinned owner refuses a chain
/// whose founder is a different key — the wipe-and-refound takeover — rather than
/// adopting it and authorizing the attacker.
#[tokio::test]
async fn pull_refuses_a_chain_not_anchored_to_the_pinned_owner() {
    let storage = MockSyncStorage::new();

    // The attacker wiped membership/* and refounded themselves as Owner.
    let attacker = UserKeypair::generate();
    let attacker_pk = hex::encode(attacker.public_key());
    let forged = founder_entry("test-store", &attacker, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &attacker_pk, 1, forged).await;
    publish_membership_chain_head(&storage, &chain, &attacker).await;

    // The puller has the real owner pinned (a different key).
    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(StorePullError::Membership(_))),
        "a chain founded by a non-owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// Owner anchoring (issue #104/#102): a puller with a pinned owner refuses an
/// empty membership listing — the chain was wiped — rather than falling open to
/// "no chain, accept everything."
#[tokio::test]
async fn pull_refuses_wiped_membership_when_owner_pinned() {
    let storage = MockSyncStorage::new();

    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(StorePullError::Membership(_))),
        "an empty chain with a pinned owner must be refused, got {:?}",
        result.map(|_| ()),
    );
}

struct PersistedCycleRemoval {
    storage: MockSyncStorage,
    db: crate::database::Database,
    founder_pubkey: String,
    second_owner_pubkey: String,
    removed_member_pubkey: String,
}

async fn persisted_cycle_removal(pin_owner: bool) -> PersistedCycleRemoval {
    let founder = UserKeypair::generate();
    let second_owner = UserKeypair::generate();
    let removed_member = UserKeypair::generate();
    let founder_pubkey = hex::encode(founder.public_key());
    let second_owner_pubkey = hex::encode(second_owner.public_key());
    let removed_member_pubkey = hex::encode(removed_member.public_key());
    let storage = MockSyncStorage::with_keypair(founder.clone());
    let db = open_test_db();
    if pin_owner {
        db.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &founder_pubkey)
            .await
            .unwrap();
    }

    let mut chain = MembershipChain::new();
    let founder_entry = storage.protocol_genesis().founder;
    append_membership_entry(&storage, &mut chain, &founder_pubkey, 1, founder_entry).await;
    let add_owner = chain
        .signed_set_member(
            &founder,
            pubkey_hex(&second_owner),
            None,
            MemberRole::Owner,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &founder_pubkey, 2, add_owner).await;
    let add_member = chain
        .signed_set_member(
            &founder,
            pubkey_hex(&removed_member),
            None,
            MemberRole::Member,
            "2026-03-01T00:02:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &founder_pubkey, 3, add_member).await;
    let remove_member = chain
        .signed_remove_member(
            &second_owner,
            pubkey_hex(&removed_member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    append_membership_entry(&storage, &mut chain, &second_owner_pubkey, 1, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &founder).await;
    publish_membership_chain_head(&storage, &chain, &second_owner).await;

    let initial = crate::sync::pull::load_cycle_membership(&storage, &db)
        .await
        .expect("accept and persist the complete multi-author chain");
    assert!(!initial
        .chain
        .expect("listed membership chain")
        .can_write_now(&removed_member_pubkey));

    for seq in 1..=3 {
        storage.hide_membership_from_listing(&founder_pubkey, seq);
    }
    storage.hide_membership_from_listing(&second_owner_pubkey, 1);

    PersistedCycleRemoval {
        storage,
        db,
        founder_pubkey,
        second_owner_pubkey,
        removed_member_pubkey,
    }
}

#[tokio::test]
async fn pinned_cycle_recovers_persisted_authors_when_membership_listing_is_empty() {
    let fixture = persisted_cycle_removal(true).await;

    let recovered = crate::sync::pull::load_cycle_membership(&fixture.storage, &fixture.db)
        .await
        .expect("empty LIST must use the persisted author floors");

    assert_eq!(
        recovered.pinned_owner.as_deref(),
        Some(fixture.founder_pubkey.as_str())
    );
    assert!(recovered.listed_entries.is_empty());
    assert!(!recovered
        .chain
        .expect("persisted membership chain")
        .can_write_now(&fixture.removed_member_pubkey));
}

#[tokio::test]
async fn unpinned_cycle_recovers_persisted_authors_when_membership_listing_is_empty() {
    let fixture = persisted_cycle_removal(false).await;

    let recovered = crate::sync::pull::load_cycle_membership(&fixture.storage, &fixture.db)
        .await
        .expect("an unpinned prior chain must not fall open on an empty LIST");

    assert!(recovered.pinned_owner.is_none());
    assert!(recovered.listed_entries.is_empty());
    assert!(!recovered
        .chain
        .expect("persisted membership chain")
        .can_write_now(&fixture.removed_member_pubkey));
}

#[tokio::test]
async fn unpinned_cycle_rejects_missing_state_required_by_a_persisted_floor() {
    let fixture = persisted_cycle_removal(false).await;
    fixture
        .storage
        .remove_membership_head(&fixture.second_owner_pubkey);

    let error = match crate::sync::pull::load_cycle_membership(&fixture.storage, &fixture.db).await
    {
        Err(error) => error,
        Ok(_) => panic!("a persisted author floor requires its signed head"),
    };

    assert!(
        matches!(&error, PullError::MembershipTampered(message) if message.contains(&fixture.second_owner_pubkey)),
        "missing persisted-author state must be membership tamper: {error}"
    );
}

#[tokio::test]
async fn mid_cycle_empty_membership_listing_loads_an_advanced_head_from_the_floor() {
    let owner = UserKeypair::generate();
    let owner_pubkey = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();

    let mut chain = MembershipChain::new();
    let founder = storage.protocol_genesis().founder;
    append_membership_entry(&storage, &mut chain, &owner_pubkey, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let cycle_membership = crate::sync::pull::load_cycle_membership(&storage, &target)
        .await
        .expect("load founder at cycle start");

    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pubkey, 2, add_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    storage.hide_membership_from_listing(&owner_pubkey, 1);
    storage.hide_membership_from_listing(&owner_pubkey, 2);

    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'AdvancedHead', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &changeset,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pubkey, 2)),
        &member,
        &member,
    );

    let (_tmp, store_dir) = temp_store_dir();
    bind_mock_store_protocol(&target, &storage, "dev2").await;
    let result = crate::sync::store_pull::pull_store_commits(
        &target,
        target.synced_tables(),
        &storage,
        storage.protocol_genesis_hash(),
        "dev2",
        &store_dir,
        cycle_membership.chain.as_ref(),
    )
    .await
    .expect("pull with an empty mid-cycle membership LIST");
    let updated: HashMap<_, _> = result
        .frontier
        .iter()
        .map(|(device_id, position)| (device_id.clone(), position.seq))
        .collect();

    assert_eq!(result.changesets_applied, 1);
    assert!(unauthorized_positions(&result).is_empty());
    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), Some(&1));
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // A founder entry + a changeset the owner authored: without the fail-closed
    // guard the cycle would (fail to list, drop to chain=None, then) apply this.
    let founder = founder_entry("test-store", &owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'X', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset(&owner_pk, 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // Membership can't even be listed: the cycle must abort rather than continue
    // with authorization silently disabled.
    storage.fail_membership_listing();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(StorePullError::Membership(_))),
        "a membership-list failure on an owner-pinned store must abort the cycle, got {:?}",
        result.map(|_| ()),
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // The owner authors a signed changeset.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromOwner', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devOwner",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 1)),
        &owner,
        &owner,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devOwner"), Some(&1));
}

/// Every changeset in an initialized store names the exact committed membership
/// entry that grants its signer write access. Being a current member does not
/// make an absent grant acceptable.
#[tokio::test]
async fn pull_rejects_a_current_owner_changeset_without_a_membership_grant() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'MissingGrant', NULL, '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devOwner",
        1,
        &changeset,
        SCHEMA_VERSION,
        None,
        &owner,
        &owner,
    );

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (updated, result) = pull_into(&target, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devOwner"), None);
}

/// A signed device head commits a stream to one member identity. An authentic
/// changeset signed by another current member cannot be replayed into that
/// stream, even when its membership grant is otherwise valid.
#[tokio::test]
async fn pull_rejects_a_changeset_whose_signer_differs_from_the_device_head() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WrongStreamSigner', NULL, '0000000002000-0000-devOwner', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devOwner",
        1,
        &changeset,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 2)),
        &member,
        &owner,
    );

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (_, result) = pull_into_result(&target, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a head signer mismatch holds only that device");

    assert!(result.held_positions.iter().any(|held| matches!(
        (&held.coordinate, &held.reason),
        (
            HeldStoreCoordinate::Head { device_id, .. },
            HeldStorePositionReason::HeadAuthorMismatch { .. }
        ) if device_id == "devOwner"
    )));
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&target).await.get("devOwner"), None);
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // Founder at (owner, 1); the owner adds the member as a Member at (owner, 2).
    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    // ...but the LIST hasn't caught up to the member's Add yet. A keyed GET of
    // (owner, 2) still resolves it — the eventual-consistency gap issue #84 closes.
    storage.hide_membership_from_listing(&owner_pk, 2);

    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add that is lagging the LIST.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 2)),
        &member,
        &member,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // The lagging entry was fetched by coordinate and the changeset applied — not
    // dropped as non-member, and not surfaced as a rejection.
    assert_eq!(result.changesets_applied, 1);
    assert!(unauthorized_positions(&result).is_empty());
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), Some(&1));
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // The outsider authors a signed changeset but, lacking any Add of their own,
    // names the founder entry (owner, 1) as their grant. The signature is valid
    // (it's their own key) but the named entry authorizes the owner, not them.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Forged', NULL, '0000000001000-0000-devX', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devX",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 1)),
        &outsider,
        &outsider,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // Nothing applies and the durable frontier remains before the forged commit.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    let unauthorized = unauthorized_positions(&result);
    assert_eq!(unauthorized.len(), 1);
    assert_eq!(
        unauthorized[0].coordinate,
        HeldStoreCoordinate::Commit {
            device_id: "devX".to_string(),
            position: storage.store_commit_position("devX", 1),
        }
    );
    assert_eq!(updated.get("devX"), None);
    assert_eq!(materialized_sequences(&db2).await.get("devX"), None,);
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // The owner (a current member) authors a changeset that WOULD be authorized,
    // then its signature is corrupted. The signature check must reject it before
    // authorization is even considered.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Tampered', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "dev1",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 1)),
        &owner,
        &owner,
    );
    let commit = crate::sync::store_objects::load_commit_slot(
        &storage,
        storage.protocol_genesis_hash(),
        "dev1",
        1,
    )
    .await
    .unwrap()
    .unwrap();
    let prefix =
        crate::sync::store_commit::commit_semantic_prefix("dev1", 1, commit.value.commit_hash());
    remove_protocol_prefix(&storage, &format!("{prefix}/")).await;
    let mut forged: serde_json::Value = serde_json::from_slice(&commit.bytes).unwrap();
    forged["signature"] = serde_json::Value::String("0".repeat(128));
    storage
        .append_protocol_object(&prefix, ".json", serde_json::to_vec(&forged).unwrap())
        .await
        .unwrap();

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (_, result) = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect("a Store commit with an invalid signature is held");

    // Nothing applied; surfaced as an invalid signature (NOT unauthorized) and the
    // position holds at the bad object.
    assert_eq!(result.held_positions.len(), 1);
    assert_eq!(
        result.held_positions[0],
        HeldStorePosition {
            coordinate: HeldStoreCoordinate::Commit {
                device_id: "dev1".to_string(),
                position: commit.value.position(),
            },
            reason: HeldStorePositionReason::InvalidSignature,
        }
    );
    assert!(unauthorized_positions(&result).is_empty());
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&db2).await.get("dev1"), None);
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    let remove_member = chain
        .signed_remove_member(
            &owner,
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // The removed member authors a changeset stamping their old grant (owner, 2).
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromRemoved', NULL, '0000000004000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 2)),
        &member,
        &member,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert_eq!(updated.get("devM"), None);
}

/// A hash-linked membership chain detects a missing MIDDLE entry via `prev_hash`,
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
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    let remove_member = chain
        .signed_remove_member(
            &owner,
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // The LIST omits the committed Remove; a keyed GET of (owner, 3) still serves
    // it, exactly as the cycle-start load already recovers it via the head.
    storage.hide_membership_from_listing(&owner_pk, 3);

    // The removed member authors a changeset stamping their old grant (owner, 2),
    // which looks like a legitimate lagging Add if the reload is judged against a
    // plain listing instead of the committed chain.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromRemoved', NULL, '0000000004000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 2)),
        &member,
        &member,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // Not applied: the removed member is not re-admitted by the lagging listing.
    // Surfaced as rejected-unauthorized and the position advances so the device is
    // not stuck on it.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert_eq!(updated.get("devM"), None);
}

/// A membership entry is not authoritative until its author publishes a signed
/// head covering it. A changeset cannot turn a stored-but-uncommitted Add into an
/// authorization grant merely by naming that entry's coordinate.
#[tokio::test]
async fn pull_rejects_a_changeset_naming_a_grant_no_head_covers() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // The owner publishes a head covering only the founder entry (seq 1) before
    // adding the member, so the Add at seq 2 is uploaded but no head certifies it
    // yet — genuinely uncommitted, not just list-lagging.
    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;

    // The member authors a signed changeset, stamping the grant coordinate of the
    // entry that authorizes them: (owner, 2), the Add no head covers yet.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromUncommittedMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 2)),
        &member,
        &member,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // The Add is visible by keyed GET but absent from the signed committed prefix.
    assert_eq!(storage.membership_list_count(), 2);
    assert_eq!(result.changesets_applied, 0);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), None);
}

#[tokio::test]
async fn relocated_membership_grant_cannot_authorize_a_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let relocated_author = hex::encode(UserKeypair::generate().public_key());
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;

    let grant_bytes = storage
        .read_membership_entry_bytes(&owner_pk, 2)
        .await
        .expect("owner's uncommitted grant");
    let owner_grant = membership_coord(&chain, &owner_pk, 2);
    let relocated_prefix = crate::sync::store_commit::membership_entry_semantic_prefix(
        &relocated_author,
        &owner_grant.author_owner_grant,
        2,
        owner_grant.entry_hash,
    );
    storage
        .append_protocol_object(&relocated_prefix, ".json", grant_bytes)
        .await
        .expect("relocate the grant to another author's coordinate");

    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'RelocatedGrant', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &changeset,
        SCHEMA_VERSION,
        Some(MembershipCoord {
            author_pubkey: relocated_author,
            author_owner_grant: owner_grant.author_owner_grant,
            seq: 2,
            entry_hash: owner_grant.entry_hash,
        }),
        &member,
        &member,
    );

    let target = open_test_db();
    target
        .set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let result = pull_into_result(&target, &storage, "dev2", &temp_store_dir().1).await;

    assert!(matches!(result, Err(StorePullError::Membership(_))));
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&target).await.get("devM"), None);
}

/// A storage read failure while resolving a grant leaves the position at the
/// undecided changeset. The pull must not replace an unavailable committed-chain
/// read with a bare keyed entry.
#[tokio::test]
async fn pull_holds_the_position_when_the_mid_cycle_membership_list_fails() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // The owner publishes a head covering only the founder entry (seq 1) before
    // adding the member, so the Add at seq 2 is uploaded but no head certifies it
    // yet — genuinely uncommitted, not just list-lagging.
    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;

    // Only the SECOND `list_membership_entries` call fails: the first (cycle
    // start, inside `load_cycle_membership`) succeeds, and the second (the
    // mid-cycle reload inside `resolve_membership_authorization`) hits a storage
    // error.
    storage.fail_membership_list_on_call(2);

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromLaggingMember', NULL, '0000000002000-0000-devM', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devM",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 2)),
        &member,
        &member,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let error = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1)
        .await
        .expect_err("a failed membership reload must abort Store pull");

    // The failed read leaves authorization undecided and the position unchanged.
    assert_eq!(storage.membership_list_count(), 2);
    assert!(matches!(error, StorePullError::Membership(_)));
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(materialized_sequences(&db2).await.get("devM"), None);
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
    use crate::sync::membership::{entry_hash, AuthorHead};

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = chain
        .signed_set_member(
            &owner,
            pubkey_hex(&member),
            None,
            MemberRole::Member,
            "2026-03-01T00:01:00Z".to_string(),
        )
        .expect("active Owner signs membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member.clone()).await;
    let remove_member = chain
        .signed_remove_member(
            &owner,
            pubkey_hex(&member),
            "2026-03-01T00:03:00Z".to_string(),
        )
        .expect("active Owner removes membership grant");
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // First cycle: accepts the head at seq 3 (member removed), persisting the
    // reader's watermark at 3.
    pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // A stale replica serves the head from before the Remove (seq 2, signed over
    // the Add's tip hash).
    let stale = AuthorHead::signed(
        "test-store".to_string(),
        add_member.author_owner_grant.clone(),
        2,
        entry_hash(&add_member),
        &owner,
    );
    storage.remove_membership_head(&owner_pk);
    storage
        .append_membership_head_bytes(&owner_pk, serde_json::to_vec(&stale).unwrap())
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
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
    let storage = MockSyncStorage::new();

    // A non-empty listing whose entry won't validate (broken founder signature).
    let attacker = UserKeypair::generate();
    let mut bad = founder_entry("test-store", &attacker, "2026-03-01T00:00:00Z");
    bad.signature = "00".to_string();
    storage
        .append_membership_entry_bytes(
            &hex::encode(attacker.public_key()),
            1,
            serde_json::to_vec(&bad).unwrap(),
        )
        .await
        .unwrap();

    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
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
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let owner_pk = hex::encode(owner.public_key());
    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // dev1 has a changeset in the bucket (its head is published by the mock,
    // signed by the non-member `outsider`).
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromForgedHead', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as("dev1", 1, &cs, SCHEMA_VERSION, None, &outsider, &outsider);

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(unauthorized_positions(&result).len(), 1);
    assert_eq!(updated.get("dev1"), None);
    assert!(result.visible_heads.iter().any(|h| h.device_id == "dev1"));
}

/// The honored case: a head authored by a current member (here a second device
/// whose head and changeset the owner signs) is kept, and its changeset applies.
#[tokio::test]
async fn pull_honors_a_head_authored_by_a_current_member() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    // The mock is the owner's device, so the head it publishes for `devA` is
    // owner-signed — a current member.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = storage.protocol_genesis().founder;
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'FromMember', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    storage.store_changeset_signed_as(
        "devA",
        1,
        &cs,
        SCHEMA_VERSION,
        Some(membership_coord(&chain, &owner_pk, 1)),
        &owner,
        &owner,
    );

    let db2 = open_test_db();
    db2.set_protocol_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devA"), Some(&1));
}

/// A pulled blob's `id` is the primary key of a row authored by any write-capable
/// member (or anyone with the bucket credential). It is interpolated into the
/// blob's local file path, so an unconstrained `id` lets a member's row direct a
/// blob write to an attacker-chosen file outside the store directory — an
/// arbitrary file write that clobbers config/rc/binaries on every pulling device.
/// The pull must treat an `id` (or namespace/cloud_path) that could escape the
/// store directory, or that can't form a partition prefix, as bad data: refuse
/// the write, skip the row, surface it — never write outside, never panic.
mod blob_path_traversal {
    use super::*;
    use crate::blob::BlobScope;

    /// A blob whose `id` climbs out of the cache directory with `..` must NOT have
    /// its bytes written outside it. coven builds the destination from the id under
    /// its store cache; without the boundary check the id would resolve to a path
    /// above the cache and the downloaded bytes land there (an arbitrary-file-write
    /// RCE); the check refuses such a row as bad data, so nothing is written outside
    /// the cache and the apply is held.
    #[tokio::test]
    async fn traversal_id_does_not_write_outside_the_blob_dir() {
        let storage = MockSyncStorage::new();

        // The attacker's blob bytes, planted in the cloud under the malicious id's
        // flat mock key (the same key the puller's `get_blob` computes for it). No
        // local file is written on the source side, so nothing escapes here.
        let evil_bytes = b"OWNED".to_vec();
        storage
            .put_blob(
                "photos",
                "x/../../../PWNED",
                BlobScope::Master,
                None,
                evil_bytes,
            )
            .await
            .expect("plant evil blob in the cloud");

        // The source's changeset adds a note + a photo row whose id is the
        // traversal string. (The mock stored the blob above; this is the row that
        // references it.)
        let db1 = open_test_db();
        let cs = capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('x/../../../PWNED', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        )
        .await;
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        // The puller builds the blob's destination from the validated id under its
        // own store dir. `download_blobs` rejects the traversal id (it is not a
        // safe path token) before building any path, so nothing is written — the
        // `dir.join(id)` escape is structurally unreachable (the id validation is
        // proven by the `store_dir` unit tests).
        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_store_dir();
        let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

        // It is bad data, so the row that carries it is not applied and the position
        // does not advance — the same posture as any other failed-blob changeset.
        assert!(
            result.asset_downloads_failed,
            "a refused blob fails the changeset's downloads",
        );
        assert_eq!(result.changesets_applied, 0, "the bad row is not applied");
        assert_eq!(updated.get("dev1"), None, "the position is held for retry");
    }

    /// A blob id too short to form the `{ab}/{cd}` partition prefix (the
    /// dash-stripped id is under four chars, or splits a multi-byte char) cannot
    /// index the prefix's byte slice, so the path builder refuses it. End to end
    /// it is bad data: the row does not apply and the position holds. (The slice
    /// itself is proven non-panicking by the `hashed_path` unit tests in
    /// `store_dir`.)
    #[tokio::test]
    async fn unindexable_id_is_refused_not_panicked() {
        let storage = MockSyncStorage::new();

        let db1 = open_test_db();
        let cs = capture_bytes(
            &db1,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
                // `id = "a"` dash-strips to "a", too short for the `&hex[..2]`
                // prefix slice, so the path builder refuses it.
                "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('a', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        )
        .await;
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_store_dir();
        // The pull completes (no panic); the unindexable row is refused.
        let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

        assert!(
            result.asset_downloads_failed,
            "an unindexable blob id fails the changeset's downloads instead of panicking",
        );
        assert_eq!(result.changesets_applied, 0, "the bad row is not applied");
        assert_eq!(updated.get("dev1"), None, "the position is held for retry");
    }

    /// A normal blob id still round-trips: the boundary check rejects only ids that
    /// could escape the cache or can't be partitioned, and a well-formed id writes
    /// its blob into the pinned cache at its partitioned `{ab}/{cd}/<id>` path.
    #[tokio::test]
    async fn normal_id_still_writes_under_the_blob_dir() {
        let storage = MockSyncStorage::new();

        storage
            .put_blob(
                "photos",
                "p1ab",
                BlobScope::Master,
                None,
                b"PHOTOBYTES".to_vec(),
            )
            .await
            .expect("plant blob");

        let db1 = open_test_db();
        let cs = capture_bytes(
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
        storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_store_dir();
        let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

        assert_eq!(result.changesets_applied, 1, "a well-formed row applies");
        assert!(!result.asset_downloads_failed);
        assert_eq!(updated.get("dev1"), Some(&1));
        let written = std::fs::read(ld.cache_blob_path("photos", "p1ab").expect("cache path"))
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
/// was deleted locally, delete wins" and OMITs — dropping the UPDATE and advancing the
/// The updater's commit captures the inserter's exact position. Reversed head discovery
/// therefore holds the UPDATE until the INSERT is durable, independent of listing order.
#[tokio::test]
async fn update_applied_before_its_insert_diverges_notfound_omit() {
    let home = InMemoryCloudHome::new();
    // Force a deterministic cross-device apply order so the bug reproduces every run —
    // a real bucket LIST is unordered, which is why the live test raced ~50/50.
    home.sort_listings();
    let keypair = UserKeypair::generate();
    let storage = cloud_test_storage(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let tables = test_synced_tables();

    // The inserter's device id (`dev-z`) sorts LAST, the updater's (`dev-a`) FIRST, so
    // the third device processes the UPDATE stream before the INSERT stream.
    let db_ins = open_test_db();
    let (_ti, ld_ins) = temp_store_dir();
    let insert = capture_bytes(
        &db_ins,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('n1', 'orig', NULL, 1, '0000000001000-0000-ins', '2026-01-01')",
        ],
    )
    .await;
    push_cycle_as(
        "dev-z", &db_ins, &tables, &storage, insert, 0, &keypair, &ld_ins,
    )
    .await;

    let db_upd = open_test_db();
    let (_tu, ld_upd) = temp_store_dir();
    pull_cloud_into(&db_upd, &db_ins, &storage, "dev-a", &ld_upd).await;
    let update = capture_bytes(
        &db_upd,
        &[
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-upd' \
           WHERE id = 'n1'",
        ],
    )
    .await;
    push_cycle_as(
        "dev-a", &db_upd, &tables, &storage, update, 0, &keypair, &ld_upd,
    )
    .await;

    let db_c = open_test_db();
    let (_tc, ld_c) = temp_store_dir();
    pull_cloud_into(&db_c, &db_ins, &storage, "dev-c", &ld_c).await;

    assert_eq!(
        query_text(&db_c, "SELECT title FROM notes WHERE id = 'n1'").await,
        "updated",
        "the UPDATE applied before its INSERT must not be dropped as a local delete",
    );
}
