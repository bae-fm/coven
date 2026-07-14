//! Tests for the pull path and blob sync, on the synthetic schema.
//!
//! A source device captures changesets into a `MockSyncStorage`; a second device
//! pulls and applies them through a real [`crate::database::Database`], exercising
//! the real `pull_changes` + blob plumbing.

use std::collections::HashMap;

use rusqlite::OptionalExtension;

use crate::blob::{local_files, CacheFill, Provenance};
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::migration::Migration;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage, PendingRotation};
use crate::sync::cycle;
use crate::sync::envelope;
use crate::sync::membership::{MemberRole, MembershipAction, MembershipChain, MembershipCoord};
use crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY;
use crate::sync::pull::{HeldChangesetReason, PullError};
/// The synthetic test db opens with a single migration, so its
/// [`crate::database::Database::schema_version`] is 1. Changesets are stored at
/// that version; a newer peer's changeset or floor uses `SCHEMA_VERSION + 1`.
const SCHEMA_VERSION: u32 = 1;
use crate::sync::service::{self, SyncCycleError};
use crate::sync::session::{BlobDecl, SyncedTable};
use crate::sync::storage::SyncStorage;
use crate::sync::test_helpers::*;

/// Drive `service::sync` the way the cycle does: load the cycle's membership chain
/// once and hand it in. Test call sites pass the same positional args they always
/// did — this is the non-cycle standalone entry that loads the membership itself.
async fn sync_for_test(
    device_id: &str,
    db: &crate::database::Database,
    tables: &[SyncedTable],
    outgoing: Vec<u8>,
    local_seq: u64,
    storage: &dyn SyncStorage,
    timestamp: &str,
    message: &str,
    keypair: &UserKeypair,
    store_dir: &crate::store_dir::StoreDir,
) -> Result<service::SyncResult, SyncCycleError> {
    let membership = crate::sync::pull::load_cycle_membership(storage, db)
        .await
        .map_err(SyncCycleError::Pull)?;
    service::sync(
        device_id,
        db,
        tables,
        outgoing,
        local_seq,
        storage,
        timestamp,
        message,
        keypair,
        store_dir,
        membership.chain.as_ref(),
        membership.pinned_owner.as_deref(),
        false,
    )
    .await
}

async fn blob_exists_at_recorded_location(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    namespace: &str,
    id: &str,
    cloud_path: Option<&str>,
) -> bool {
    let Some(location) = db
        .blob_location(namespace, id)
        .await
        .expect("read blob location")
    else {
        return false;
    };
    storage
        .blob_exists(namespace, &location, id, cloud_path)
        .await
        .expect("exists check")
}

async fn plant_cloud_blob(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    namespace: &str,
    id: &str,
    cloud_path: Option<&str>,
    bytes: Vec<u8>,
    counter: u64,
) -> crate::blob::CloudBlobLocation {
    let location = test_blob_location(
        &storage
            .own_uploader()
            .expect("test storage has an uploader identity"),
        counter,
    );
    storage
        .put_blob(
            namespace,
            &location,
            id,
            crate::blob::BlobScope::Master,
            cloud_path,
            bytes,
        )
        .await
        .expect("plant cloud blob");
    db.record_blob_location(namespace, id, &location)
        .await
        .expect("record cloud blob location");
    location
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
        vec![SyncedTable::new("unique_notes")],
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
            SyncedTable::new("constraint_parents"),
            SyncedTable::new("constraint_items"),
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

fn remove_envelope_field(packed: &[u8], field: &str) -> Vec<u8> {
    let separator = packed.iter().position(|byte| *byte == 0).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&packed[..separator]).unwrap();
    json.as_object_mut().unwrap().remove(field);
    let mut malformed = serde_json::to_vec(&json).unwrap();
    malformed.push(0);
    malformed.extend_from_slice(&packed[separator + 1..]);
    malformed
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
        db2.get_all_sync_cursors().await.unwrap().get("dev1"),
        Some(&1),
        "the row and its durable cursor commit in the pull that applies it",
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
async fn cursor_write_failure_rolls_back_the_remote_rows() {
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
        "CREATE TRIGGER reject_cursor_insert BEFORE INSERT ON sync_cursors \
         BEGIN SELECT RAISE(ABORT, 'injected cursor write failure'); END;",
    )
    .await;
    let (_tmp, store_dir) = temp_store_dir();
    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(updated.get("dev1"), None);
    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_changesets.len(), 1);
    assert!(matches!(
        result.held_changesets[0].reason,
        HeldChangesetReason::ApplyFailed { .. }
    ));
    assert!(
        !row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "the row cannot commit when its cursor write fails",
    );
    assert!(target.get_all_sync_cursors().await.unwrap().is_empty());
}

#[tokio::test]
async fn ordinary_pull_starts_from_its_durable_cursor() {
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
    assert!(result.held_changesets.is_empty());
    assert_eq!(
        target.get_all_sync_cursors().await.unwrap().get("dev1"),
        Some(&1),
    );
    assert!(
        row_exists(&target, "SELECT 1 FROM notes WHERE id = 'stale-row'").await,
        "ordinary pull derives coverage from durable rows, not caller input",
    );
}

#[tokio::test]
async fn ordinary_pull_uses_its_durable_cursor_on_every_call() {
    let storage = MockSyncStorage::new();
    let source = open_test_db();
    let first = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('cursor-row', 'One', NULL, \
                     '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let second = capture_bytes(
        &source,
        &["UPDATE notes SET title = 'Two', \
             _updated_at = '0000000002000-0000-dev1' WHERE id = 'cursor-row'"],
    )
    .await;
    storage.store_changeset("dev1", 1, &first, SCHEMA_VERSION);

    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();
    pull_into(&target, &storage, "dev2", &store_dir).await;
    storage.store_changeset("dev1", 2, &second, SCHEMA_VERSION);

    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(result.held_changesets.is_empty());
    assert_eq!(updated.get("dev1"), Some(&2));
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'cursor-row'").await,
        "Two",
    );
}

#[tokio::test]
async fn ordinary_pull_applies_the_change_immediately_after_its_durable_cursor() {
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
    assert_eq!(
        target.get_all_sync_cursors().await.unwrap().get("dev1"),
        Some(&2),
    );
}

#[tokio::test]
async fn invalid_durable_cursor_values_are_rejected_at_the_database_boundary() {
    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO sync_cursors (device_id, last_seq) VALUES ('bad-device', -1)",
    )
    .await;

    assert!(target.get_all_sync_cursors().await.is_err());
    assert!(target
        .advance_sync_cursor("bad-device", 0, 1)
        .await
        .is_err());
    assert!(
        crate::database::Database::validate_sync_cursors(&HashMap::from([(
            "overflow-device".to_string(),
            u64::MAX,
        )]))
        .is_err()
    );
}

#[tokio::test]
async fn empty_changeset_advances_the_durable_cursor() {
    let storage = MockSyncStorage::new();
    storage.store_changeset("dev1", 1, &[], SCHEMA_VERSION);
    let target = open_test_db();
    let (_tmp, store_dir) = temp_store_dir();

    let (updated, result) = pull_into(&target, &storage, "dev2", &store_dir).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert_eq!(
        target.get_all_sync_cursors().await.unwrap().get("dev1"),
        Some(&1),
    );
}

#[tokio::test]
async fn host_write_after_remote_apply_observes_the_matching_cursor() {
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
                let cursor: Option<u64> = tx
                    .query_row(
                        "SELECT last_seq FROM sync_cursors WHERE device_id = 'dev1'",
                        [],
                        |row| row.get::<_, i64>(0).map(|seq| seq as u64),
                    )
                    .optional()
                    .map_err(crate::database::DbError::from)?;
                assert!(remote_row, "the host transaction observes the remote row");
                assert_eq!(
                    cursor,
                    Some(1),
                    "the same database cut observes the row's materialized cursor",
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
/// device's cursor surfaces a `MissingChangeset` held reason and holds the
/// cursor at the gap — the host reports reclaimed history rather than a generic
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
    storage
        .delete_changeset("dev1", 1)
        .await
        .expect("reclaim the changeset object");

    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_changesets.len(), 1);
    assert_eq!(result.held_changesets[0].device_id, "dev1");
    assert_eq!(result.held_changesets[0].seq, 1);
    assert_eq!(
        result.held_changesets[0].reason,
        HeldChangesetReason::MissingChangeset,
    );
    // The cursor holds at the gap: dev1 never advances over the unapplied seq.
    assert_eq!(updated.get("dev1").copied().unwrap_or(0), 0);
}

#[tokio::test]
async fn uniqueness_conflict_rolls_back_the_entire_changeset_and_cursor() {
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
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('other', 'other-slug', 'Other', '0000000001001-0000-dev1', '2026-01-01')",
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

    assert_eq!(result.constraint_conflicts.len(), 1);
    assert_eq!(result.constraint_conflicts[0].device_id, "dev1");
    assert_eq!(result.constraint_conflicts[0].seq, 1);
    assert_eq!(result.constraint_conflicts[0].table, "unique_notes");
    assert_eq!(updated.get("dev1"), None);
    assert_eq!(
        db2.get_all_sync_cursors().await.unwrap().get("dev1"),
        None,
        "a rejected changeset has no durable cursor",
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
    assert_eq!(result.constraint_conflicts.len(), 1);
    assert_eq!(result.constraint_conflicts[0].table, "constraint_items");
    assert_eq!(updated.get("dev1"), None);
    assert_eq!(
        target.get_all_sync_cursors().await.unwrap().get("dev1"),
        None
    );
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
    assert_eq!(result.skipped_schema, 1);
    // The cursor must NOT advance past a genuine newer-schema changeset: it
    // becomes applicable once this app updates, and an already-running device
    // never re-bootstraps, so advancing would strand its rows forever. Leaving
    // the cursor put re-fetches seq 1 after the upgrade.
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
    let packed = envelope::pack_signed(
        "dev1",
        1,
        SCHEMA_VERSION + 1,
        "",
        "2026-03-01T00:01:00Z",
        &forger,
        None,
        &cs,
    );
    // Corrupt the signature: a well-formed but never-verifying 64-byte Ed25519
    // signature, leaving the rest of the envelope (including the newer
    // schema_version) intact so the object reaches the signature and schema gates.
    let (mut env, changeset_bytes) = envelope::unpack(&packed).unwrap();
    env.signature = "0".repeat(128);
    storage.put_changeset_packed("dev1", 1, envelope::pack(&env, &changeset_bytes));

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // Reported as tamper, not routine version skew: skipped_schema untouched.
    assert_eq!(result.skipped_schema, 0);
    assert_eq!(result.invalid_signatures.len(), 1);
    assert_eq!(result.invalid_signatures[0].device_id, "dev1");
    assert_eq!(result.invalid_signatures[0].seq, 1);
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    // The cursor holds at the tampered object, the same as any invalid signature.
    assert_eq!(updated.get("dev1"), None);
}

/// A genuine newer-schema changeset is signed, so verifying the signature before
/// the schema gate does not change its handling: it still verifies, still counts
/// as a schema skip, still holds the cursor, and applies once the local schema
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
    let packed = envelope::pack_signed(
        "dev1",
        1,
        SCHEMA_VERSION + 1,
        "",
        "2026-03-01T00:01:00Z",
        &author,
        None,
        &cs,
    );
    storage.put_changeset_packed("dev1", 1, packed);
    storage.publish_head_as("dev1", 1, &author);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.skipped_schema, 1);
    assert!(result.invalid_signatures.is_empty());
    assert_eq!(result.changesets_applied, 0);
    // Held, not advanced: it becomes applicable once this app upgrades.
    assert_eq!(updated.get("dev1"), None);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

/// The pull gate compares an incoming changeset's `schema_version` against the
/// opened db's [`Database::schema_version`], not a hand-bumped constant: a peer at
/// version N applies a changeset stamped N and skips one stamped N+1 without
/// advancing its cursor. The peer's own version is derived from the db, so this
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

    // seq 2 stamped one above the peer's schema version: skipped, cursor held.
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
        "the N-stamped changeset applies"
    );
    assert_eq!(
        result.skipped_schema, 1,
        "the N+1-stamped changeset is skipped"
    );
    assert_eq!(
        updated.get("dev1"),
        Some(&1),
        "cursor stops at the applied seq, never past the skipped one"
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

    let packed = result.outgoing.expect("an outgoing changeset").packed;
    let (env, _changeset) = envelope::unpack(&packed).expect("unpack outgoing envelope");
    assert_eq!(
        env.schema_version,
        db1.schema_version(),
        "the outgoing changeset is stamped with the db's applied schema version",
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
async fn pull_does_not_advance_cursor_past_a_blob_failed_changeset() {
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
    // The cursor must NOT jump to 2 past the blob-failed seq 1 — otherwise seq 1's
    // blob would never be re-fetched. It stays before seq 1 so the next cycle
    // resumes there.
    assert_ne!(
        updated.get("dev1"),
        Some(&2),
        "cursor must not advance past the blob-failed seq",
    );
    assert_eq!(
        updated.get("dev1"),
        None,
        "cursor stays before the blob-failed seq 1",
    );
    // The blob-bearing row must NOT have been applied: with download-before-apply
    // (#111), seq 1's failed blob means seq 1 is skipped whole -- "row present,
    // blob missing" never exists. (Before #111 the row was applied and only the
    // cursor held back, so n1 was visible with no photo file on disk.)
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
/// of the fields the signature covers, and the byte-count check identifies the
/// corrupt object before its payload is used. The row must never land. The cursor is held back so the
/// next cycle re-examines the same object instead of suppressing the seq.
#[tokio::test]
async fn pull_rejects_changeset_whose_declared_size_mismatches_actual_bytes() {
    let author = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(author.clone());

    // A real changeset from dev1, packed into an envelope whose `changeset_size`
    // is deliberately wrong (one byte short of the actual payload), as a truncated
    // or tampered download would be.
    let db1 = open_test_db();
    let cs = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Corrupt', NULL, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    let mut env = envelope::ChangesetEnvelope::signed(
        "dev1",
        1,
        SCHEMA_VERSION,
        "",
        "2026-02-10T00:00:00Z",
        None,
        Vec::new(),
        &author,
        &cs,
    );
    // The lie: the envelope claims one fewer byte than the payload carries.
    env.changeset_size = cs.len() - 1;
    storage.put_changeset_packed("dev1", 1, envelope::pack(&env, &cs));

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a size-mismatched changeset must not be applied",
    );
    assert_eq!(
        updated.get("dev1"),
        None,
        "cursor holds at the size-mismatched changeset",
    );
    assert_eq!(result.held_changesets.len(), 1);
    assert_eq!(result.held_changesets[0].device_id, "dev1");
    assert_eq!(result.held_changesets[0].seq, 1);
    assert_eq!(
        result.held_changesets[0].reason,
        HeldChangesetReason::SizeMismatch {
            expected: cs.len() - 1,
            actual: cs.len(),
        }
    );
}

/// The changeset signature covers the envelope's self-declared position
/// (`device_id`, `seq`). A member holding the store key can decrypt a peer's
/// changeset and re-seal the identical bytes under the authenticated data for a
/// different storage key — the victim's own signature still authorizes the
/// content. If the puller trusted the signature without binding it to the
/// location it fetched from, it would apply the relocated changeset in the new
/// slot (replaying rows that may have been deleted since, with no last-writer
/// opponent to lose to) and, by advancing the cursor, suppress the real occupant
/// of that slot. The puller must instead hold the cursor and surface the
/// relocation as tamper.
#[tokio::test]
async fn a_changeset_replayed_at_another_seq_is_held_not_applied() {
    let storage = MockSyncStorage::new();
    let victim = UserKeypair::generate();

    // The victim authors a valid, signed changeset for its own position (dev, 5).
    let src = open_test_db();
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Replayed', NULL, '0000000005000-0000-dev', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "dev",
        5,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:05:00Z",
        &victim,
        None,
        &cs,
    );

    // A member re-seals those exact bytes at (dev, 9). In the mock the sealed
    // object is the packed envelope itself, so relocating is writing the same
    // bytes under the new key; the envelope still declares its authored position
    // (dev, 5). The head advances to 9.
    storage.put_changeset_packed("dev", 9, packed);

    // The reader has already applied this device's stream through seq 8, so its
    // pull legitimately begins at the relocated object's slot, seq 9.
    let db2 = open_test_db();
    exec(
        &db2,
        "INSERT INTO sync_cursors (device_id, last_seq) VALUES ('dev', 8)",
    )
    .await;

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // The relocated seq-5 content is not applied at seq 9, the cursor holds at 8
    // (never advances past the tampered slot), and the relocation is surfaced.
    assert_eq!(result.changesets_applied, 0);
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a changeset relocated to another seq must not be applied",
    );
    assert_eq!(
        updated.get("dev"),
        Some(&8),
        "cursor holds at the tampered slot, does not advance past it",
    );
    assert_eq!(result.held_changesets.len(), 1);
    assert_eq!(result.held_changesets[0].device_id, "dev");
    assert_eq!(result.held_changesets[0].seq, 9);
    assert_eq!(
        result.held_changesets[0].reason,
        HeldChangesetReason::PositionMismatch {
            declared_device_id: "dev".to_string(),
            declared_seq: 5,
        }
    );
}

/// The position the signature covers includes the device prefix, not only the
/// seq: an object authentic for one device's stream, relocated under another
/// device's prefix, is held rather than applied. Same binding, other coordinate.
#[tokio::test]
async fn a_changeset_relocated_to_another_device_is_held() {
    let storage = MockSyncStorage::new();
    let victim = UserKeypair::generate();

    // The victim authors a valid, signed changeset for (devVictim, 1).
    let src = open_test_db();
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Relocated', NULL, '0000000001000-0000-devVictim', '2026-01-01')",
        ],
    )
    .await;
    let packed = envelope::pack_signed(
        "devVictim",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:01:00Z",
        &victim,
        None,
        &cs,
    );

    // A member writes those exact bytes under a different device's prefix. The
    // envelope still declares device_id "devVictim".
    storage.put_changeset_packed("devAttacker", 1, packed);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(
        !row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await,
        "a changeset relocated to another device must not be applied",
    );
    assert_eq!(
        updated.get("devAttacker"),
        None,
        "cursor holds at the relocated slot",
    );
    assert_eq!(result.held_changesets.len(), 1);
    assert_eq!(result.held_changesets[0].device_id, "devAttacker");
    assert_eq!(result.held_changesets[0].seq, 1);
    assert_eq!(
        result.held_changesets[0].reason,
        HeldChangesetReason::PositionMismatch {
            declared_device_id: "devVictim".to_string(),
            declared_seq: 1,
        }
    );
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
    let packed = envelope::pack_signed(
        "dev",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:01:00Z",
        &author,
        None,
        &cs,
    );
    storage.put_changeset_packed("dev", 1, packed);
    storage.publish_head_as("dev", 1, &author);

    let db2 = open_test_db();
    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("dev"), Some(&1));
    assert!(result.held_changesets.is_empty());
}

#[tokio::test]
async fn apply_failure_isolates_to_one_device() {
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
    let (updated, result) = pull_into_result(&target, &storage, "devTarget", &ld)
        .await
        .expect("pull isolates apply failure");

    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n-good'").await);
    assert_eq!(updated.get("devA"), Some(&1));
    assert_eq!(
        updated.get("devB"),
        None,
        "the failed device cursor is held",
    );
    assert_eq!(result.held_changesets.len(), 1);
    assert_eq!(result.held_changesets[0].device_id, "devB");
    assert_eq!(result.held_changesets[0].seq, 1);
    assert!(matches!(
        result.held_changesets[0].reason,
        HeldChangesetReason::ApplyFailed { .. }
    ));
    assert_eq!(result.changesets_applied, 1);
}

/// An object at `changes/{device}/{seq}` that decrypts but whose bytes are not a
/// valid envelope (here: no null separator between the JSON and the changeset)
/// holds only its own device's cursor. A second device's changesets still apply,
/// and the malformed object surfaces as a held changeset naming the device and
/// seq. Before this, an unpack failure propagated out and failed the whole pull
/// every cycle, permanently stopping sync for every member.
#[tokio::test]
async fn malformed_envelope_isolates_to_one_device() {
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

    // devB's seq 1 is bytes with no null separator, so `envelope::unpack` fails.
    storage.put_changeset_packed("devB", 1, b"not a valid envelope".to_vec());

    let target = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (updated, result) = pull_into_result(&target, &storage, "devTarget", &ld)
        .await
        .expect("a malformed envelope must not fail the whole pull");

    assert!(row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n-good'").await);
    assert_eq!(updated.get("devA"), Some(&1));
    assert_eq!(
        updated.get("devB"),
        None,
        "the malformed device's cursor is held",
    );
    assert_eq!(result.changesets_applied, 1);
    assert_eq!(result.held_changesets.len(), 1);
    assert_eq!(result.held_changesets[0].device_id, "devB");
    assert_eq!(result.held_changesets[0].seq, 1);
    assert!(matches!(
        result.held_changesets[0].reason,
        HeldChangesetReason::MalformedEnvelope { .. }
    ));
}

#[tokio::test]
async fn solo_store_holds_envelopes_missing_required_signing_fields() {
    let storage = MockSyncStorage::new();
    let author = UserKeypair::generate();
    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'MissingSigningField', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;

    for (device, field) in [("devA", "author_pubkey"), ("devB", "signature")] {
        let packed = envelope::pack_signed(
            device,
            1,
            SCHEMA_VERSION,
            "",
            "2026-03-01T00:01:00Z",
            &author,
            None,
            &changeset,
        );
        storage.put_changeset_packed(device, 1, remove_envelope_field(&packed, field));
    }

    let target = open_test_db();
    let (updated, result) = pull_into_result(&target, &storage, "devTarget", &temp_store_dir().1)
        .await
        .expect("malformed signing fields isolate to their device streams");

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_changesets.len(), 2);
    assert!(result
        .held_changesets
        .iter()
        .all(|held| matches!(held.reason, HeldChangesetReason::MalformedEnvelope { .. })));
    assert_eq!(updated.get("devA"), None);
    assert_eq!(updated.get("devB"), None);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

#[tokio::test]
async fn chained_store_holds_envelopes_missing_required_signing_fields() {
    let owner = UserKeypair::generate();
    let owner_pubkey = hex::encode(owner.public_key());
    let storage = MockSyncStorage::with_keypair(owner.clone());
    let mut chain = MembershipChain::new();
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    append_membership_entry(&storage, &mut chain, &owner_pubkey, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let source = open_test_db();
    let changeset = capture_bytes(
        &source,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'MissingSigningField', NULL, '0000000001000-0000-devA', '2026-01-01')",
        ],
    )
    .await;
    let grant = Some(MembershipCoord {
        author_pubkey: owner_pubkey.clone(),
        seq: 1,
    });
    for (device, field) in [("devA", "author_pubkey"), ("devB", "signature")] {
        let packed = envelope::pack_signed(
            device,
            1,
            SCHEMA_VERSION,
            "",
            "2026-03-01T00:01:00Z",
            &owner,
            grant.clone(),
            &changeset,
        );
        storage.put_changeset_packed(device, 1, remove_envelope_field(&packed, field));
    }

    let target = open_test_db();
    target
        .set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();
    let (updated, result) = pull_into_result(&target, &storage, "devTarget", &temp_store_dir().1)
        .await
        .expect("malformed signing fields isolate to their device streams");

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.held_changesets.len(), 2);
    assert!(result
        .held_changesets
        .iter()
        .all(|held| matches!(held.reason, HeldChangesetReason::MalformedEnvelope { .. })));
    assert_eq!(updated.get("devA"), None);
    assert_eq!(updated.get("devB"), None);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

/// The malformed-envelope hold is a bounded stall, not a permanent skip: once a
/// valid object is re-pushed at the same seq, the next cycle resumes that
/// device's stream from where the cursor was held.
#[tokio::test]
async fn re_pushed_valid_envelope_resumes_the_held_device() {
    let storage = MockSyncStorage::new();

    storage.put_changeset_packed("dev1", 1, b"not a valid envelope".to_vec());

    let db2 = open_test_db();
    let (_tmp, ld) = temp_store_dir();
    let (held_cursors, first) = pull_into_result(&db2, &storage, "dev2", &ld)
        .await
        .expect("a malformed envelope must not fail the whole pull");
    assert_eq!(first.changesets_applied, 0);
    assert_eq!(held_cursors.get("dev1"), None);
    assert_eq!(first.held_changesets.len(), 1);

    // The device re-pushes a valid changeset at the held seq.
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
    assert!(second.held_changesets.is_empty());
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
    let location = plant_cloud_blob(
        &db1,
        &storage,
        "photos",
        "p1ab",
        None,
        b"PHOTOBYTES".to_vec(),
        1000,
    )
    .await;
    storage.store_changeset_with_blob_locations(
        "dev1",
        1,
        &cs,
        SCHEMA_VERSION,
        vec![test_blob_location_record(
            "photos",
            "p1ab",
            location,
            b"PHOTOBYTES",
        )],
    );

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
async fn stale_same_length_cache_is_replaced_before_the_cursor_advances() {
    let storage = MockSyncStorage::new();
    let db1 = open_test_db_with_blob(photo_decl());
    let current = b"CURRENT-BYTES";
    let stale = b"STALE--BYTES!";
    assert_eq!(current.len(), stale.len());
    let changeset = capture_bytes(
        &db1,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01')",
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('stale001', 'n1', 'cover', {}, '{}', \
                 '0000000001000-0000-dev1', '2026-01-01')",
                current.len(),
                crate::blob::content_hash(current),
            ),
        ],
    )
    .await;
    let location = plant_cloud_blob(
        &db1,
        &storage,
        "photos",
        "stale001",
        None,
        current.to_vec(),
        1000,
    )
    .await;
    storage.store_changeset_with_blob_locations(
        "dev1",
        1,
        &changeset,
        SCHEMA_VERSION,
        vec![test_blob_location_record(
            "photos", "stale001", location, current,
        )],
    );

    let db2 = open_test_db_with_blob(photo_decl());
    let (_tmp, store_dir) = temp_store_dir();
    let cache_path = store_dir.cache_blob_path("photos", "stale001").unwrap();
    crate::local_blob::write_atomic(&cache_path, stale)
        .await
        .unwrap();
    let (cursors, result) = pull_into(&db2, &storage, "dev2", &store_dir).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(!result.asset_downloads_failed);
    assert_eq!(cursors.get("dev1"), Some(&1));
    assert_eq!(std::fs::read(cache_path).unwrap(), current);
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
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', 8, \
                 'f3f47f35e084260cb147209c1985f34a9b9b07cda169516f91b30be0fdf3c6f7', \
                 '0000000001000-0000-dev1', '2026-01-01', 'oldaaaa')",
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
    let outgoing = result.outgoing.expect("outgoing update");
    assert!(
        blob_exists_at_recorded_location(&db1, &storage, "photos", "newaaaa", None).await,
        "push uploads the UPDATE's new blob id"
    );
    assert!(
        !blob_exists_at_recorded_location(&db1, &storage, "photos", "oldaaaa", None).await,
        "push must not upload the UPDATE's old blob id"
    );

    cycle::push_changeset(
        &storage,
        &db1,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("publish update");

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
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', 8, \
                 'f3f47f35e084260cb147209c1985f34a9b9b07cda169516f91b30be0fdf3c6f7', \
                 '0000000001000-0000-dev2', '2026-01-01', 'oldaaaa')",
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
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', 8, \
                 'f3f47f35e084260cb147209c1985f34a9b9b07cda169516f91b30be0fdf3c6f7', \
                 '0000000001000-0000-dev1', '2026-01-01', 'oldnull')",
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
        "INSERT INTO note_photos \
         (id, note_id, kind, size, hash, _updated_at, created_at, cloud_path) \
         VALUES ('p-row', 'n1', 'cover', 8, \
                 'f3f47f35e084260cb147209c1985f34a9b9b07cda169516f91b30be0fdf3c6f7', \
                 '0000000001000-0000-dev2', '2026-01-01', 'oldnull')",
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
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('audio1', 'n1', 'audio', 13, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"AUDIO-PAYLOAD"),
            ),
        ],
    )
    .await;

    let audio_location = plant_cloud_blob(
        &db1,
        &storage,
        "audio",
        "audio1",
        None,
        b"AUDIO-PAYLOAD".to_vec(),
        1000,
    )
    .await;

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
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        &db1,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");

    // The user-provided blob was NOT uploaded by the inline push.
    assert_eq!(
        storage
            .get_blob(
                "audio",
                &audio_location,
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

    // The row applied and the cursor advanced — the CacheLazy blob never blocks the
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
        matches!(err, SyncCycleError::BlobMissing(_)),
        "local user-provided blob must fail as BlobMissing: {err:?}",
    );
    assert!(
        storage
            .list_heads()
            .await
            .expect("list heads")
            .heads
            .is_empty(),
        "sync returned no outgoing changeset for a caller to publish"
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
        matches!(err, SyncCycleError::BlobMissing(_)),
        "missing remote user-provided blob must fail as BlobMissing: {err:?}",
    );
    assert!(
        storage
            .list_heads()
            .await
            .expect("list heads")
            .heads
            .is_empty(),
        "sync returned no outgoing changeset for a caller to publish"
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
    plant_cloud_blob(
        &db,
        &storage,
        "audio",
        "audio1",
        None,
        b"AUDIO-PAYLOAD".to_vec(),
        1000,
    )
    .await;
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
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        &db,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push changeset");

    assert_eq!(
        storage.list_heads().await.expect("list heads").heads[0].seq,
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
    let outgoing = result.outgoing.expect("outgoing delete changeset");
    cycle::push_changeset(
        &storage,
        &db,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push delete changeset");

    assert_eq!(
        storage.list_heads().await.expect("list heads").heads[0].seq,
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
            &format!(
                "INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('p1ab', 'n1', 'cover', 7, '{}', '0000000001000-0000-dev1', '2026-01-01')",
                crate::blob::content_hash(b"missing"),
            ),
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
    // `SyncResult` is not Debug; inspect only the error side for the assert message.
    let err = result.err();
    assert!(
        matches!(err, Some(SyncCycleError::BlobMissing(_))),
        "an unstaged blob must abort the cycle, got {err:?}",
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
    let storage = CloudSyncStorage::new(
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
    let cover_key = current_blob_key(
        &db,
        &storage,
        "photos",
        "p1cover",
        Some("n1/cover-p1cover.jpg"),
    )
    .await;
    assert_eq!(
        home.get(&cover_key).as_deref(),
        Some(bytes.as_slice()),
        "the first push uploads the cover",
    );

    // This device now holds no copy of the blob at all: the push moved the local-store
    // copy into the cache, and the cache copy is then evicted.
    local_files::drop_blob(&ld, "photos", "p1cover", &crate::blob::content_hash(bytes))
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
        home.get(&cover_key).as_deref(),
        Some(bytes.as_slice()),
        "the cloud object is left exactly as it stands",
    );
}

/// The `note_photos` declaration for the plain (browsable) scheme: the blob's
/// readable suffix comes from the row's `cloud_path` column.
fn readable_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_cloud_path_column("cloud_path")
}

/// A plain-scheme home stores a changeset-driven blob below its generated identity,
/// with `n1/cover-p1cover.jpg` as the readable suffix, and a second device with the
/// same declaration pulls it from that generated readable key and recovers the
/// bytes. This is the changeset-push / changeset-pull half of the blob path, end to
/// end over a real `CloudSyncStorage` in `BlobPathScheme::Plain`.
#[tokio::test]
async fn plain_scheme_blob_round_trips_at_the_readable_key() {
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
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
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        &db1,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");
    let readable_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p1cover",
        Some("n1/cover-p1cover.jpg"),
    )
    .await;

    // The blob lands at the readable key, not the hashed shard.
    assert!(
        storage
            .cloud_home()
            .exists(&readable_key)
            .await
            .expect("exists at readable key"),
        "the blob must land at the readable cloud_path key",
    );
    let hashed = CloudSyncStorage::blob_key(
        BlobPathScheme::Hashed,
        "photos",
        &db1.blob_location("photos", "p1cover")
            .await
            .unwrap()
            .unwrap(),
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
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

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

/// A plain generated key carries the blob id independently of the readable suffix, so a
/// host-provided blob may use the consumer's stable readable name without a redundant id
/// in that name.
#[tokio::test]
async fn plain_scheme_host_blob_accepts_a_stable_readable_path() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
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

    push_cycle(&db, &tables, &storage, outgoing, 0, &keypair, &ld).await;
    let key = current_blob_key(&db, &storage, "photos", "p1cover", Some("n1/cover.jpg")).await;
    assert_eq!(
        home.get(&key).as_deref(),
        Some(bytes.as_slice()),
        "the blob id in the generated prefix makes the readable suffix collision-safe",
    );
}

/// Replacing a blob-bearing row on a browsable home writes a NEW cloud object; it never
/// overwrites the one it replaces.
///
/// A blob id names one immutable byte-string, and every generated key carries that id.
/// The replacement therefore gets a fresh key even if its readable suffix is reused. The
/// object it replaced stands at its own key until its tombstone is collected.
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
    let storage = CloudSyncStorage::new(
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
    let old_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p1cover",
        Some("n1/cover-p1cover.jpg"),
    )
    .await;
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the first push puts the cover at the key its path names",
    );

    // Device B takes the cover before the replacement, so it is a peer holding the
    // replaced blob when the new one arrives.
    let db2 = open_test_db_with_blob(readable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    pull_into(&db2, &storage, "dev2", &ld2).await;

    // Replace the cover: a new blob, whose bytes the host stages in the local store,
    // carried by a fresh row whose readable path names it; the replaced row and its local
    // copy go away.
    store_local(&ld1, "p2cover", new_bytes).await;
    local_files::drop_blob(
        &ld1,
        "photos",
        "p1cover",
        &crate::blob::content_hash(old_bytes),
    )
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
    let new_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p2cover",
        Some("n1/cover-p2cover.jpg"),
    )
    .await;

    assert_eq!(
        home.get(&new_key).as_deref(),
        Some(new_bytes.as_slice()),
        "the replacement writes its own cloud object",
    );
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the replaced blob's object is untouched — it is tombstoned, not overwritten, so a \
         device that has not yet pulled the replacement still reads the bytes its row names",
    );

    // Device B pulls the replacement. Its download verifies the object against the new
    // row's content hash, so an object holding the replaced bytes would fail the pull.
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &ld2).await;
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
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    let tables = test_synced_tables_with_blob(repointable_photo_decl());

    let original = b"ORIGINAL-COVER";
    let from_a = b"COVER-FROM-A";
    let from_b = b"COVER-FROM-B-BYTES";

    // Device A publishes the original cover; device B pulls it. Both now hold row `ph1`.
    let db_a = open_test_db_with_blob(repointable_photo_decl());
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

    let db_b = open_test_db_with_blob(repointable_photo_decl());
    let (_tb, ld_b) = temp_store_dir();
    pull_into(&db_b, &storage, "dev2", &ld_b).await;

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
    let key_a = current_blob_key(
        &db_a,
        &storage,
        "photos",
        "pAcover",
        Some("n1/cover-pAcover.jpg"),
    )
    .await;
    let key_b = current_blob_key(
        &db_b,
        &storage,
        "photos",
        "pBcover",
        Some("n1/cover-pBcover.jpg"),
    )
    .await;

    // Neither replacement overwrote the other: both objects stand, each holding the bytes
    // of the blob its key names. Under a key that did not name its blob, these two writes
    // would have been one object, and its bytes would be whichever device the bucket saw
    // last — not necessarily the device the row's conflict resolved to.
    assert_eq!(
        home.get(&key_a).as_deref(),
        Some(from_a.as_slice()),
        "device A's replacement is at its own key",
    );
    assert_eq!(
        home.get(&key_b).as_deref(),
        Some(from_b.as_slice()),
        "device B's replacement is at its own key",
    );

    // A third device pulls every changeset. Whichever repointing the row converges to, the
    // object that row names is in the bucket holding that blob's bytes — so every download
    // verifies against its row's hash and nothing is left unsatisfiable. Under a key that
    // did not name its blob, the surviving row could name bytes the other device had
    // already overwritten, and no retry would ever resolve it.
    let db_c = open_test_db_with_blob(repointable_photo_decl());
    let (_tc, ld_c) = temp_store_dir();
    let (_updated, result) = pull_into(&db_c, &storage, "dev3", &ld_c).await;
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
    let storage = CloudSyncStorage::new(
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
    local_files::drop_blob(
        &ld1,
        "photos",
        "p1cover",
        &crate::blob::content_hash(old_bytes),
    )
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
    let (_cursors, result) = pull_into(&db2, &storage, "dev2", &ld2).await;

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

/// A browsable declaration whose readable suffix is independent of its blob id.
fn stable_path_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("blob_id")
        .with_cloud_path_column("cloud_path")
}

/// A blob keeps a stable, fully readable suffix with no blob id in the name and
/// round-trips through the cloud. The generated prefix supplies its immutable identity.
#[tokio::test]
async fn plain_scheme_blob_keeps_a_stable_readable_path() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );
    // A readable name with no blob id anywhere in it.

    let bytes = b"AUDIO-BYTES";
    let db1 = open_test_db_with_blob(stable_path_photo_decl());
    let tables = test_synced_tables_with_blob(stable_path_photo_decl());
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
    let audio_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "f1audio",
        Some("n1/Sonata No. 3.flac"),
    )
    .await;

    assert_eq!(
        home.get(&audio_key).as_deref(),
        Some(bytes.as_slice()),
        "the blob lands at the consumer's own readable name, with no blob id in it",
    );

    // A peer pulls it off that readable key and verifies it against the row's hash.
    let db2 = open_test_db_with_blob(stable_path_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    let (_cursors, result) = pull_into(&db2, &storage, "dev2", &ld2).await;
    assert!(!result.asset_downloads_failed);
    assert_eq!(result.changesets_applied, 1);
    let cached = std::fs::read(
        ld2.cache_blob_path("photos", "f1audio")
            .expect("cache path"),
    )
    .expect("device B cached the audio");
    assert_eq!(cached, bytes.as_slice());
}

/// Repointing a row while retaining its readable suffix writes a distinct generated key
/// because the key carries both the publication generation and blob id.
#[tokio::test]
async fn plain_scheme_repointing_a_stable_readable_path_writes_a_distinct_object() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );

    let first = b"FIRST-AUDIO";
    let second = b"SECOND-AUDIO-BYTES";

    let db = open_test_db_with_blob(stable_path_photo_decl());
    let tables = test_synced_tables_with_blob(stable_path_photo_decl());
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
    let audio_key = current_blob_key(
        &db,
        &storage,
        "photos",
        "f1audio",
        Some("n1/Sonata No. 3.flac"),
    )
    .await;

    // Repoint the row at a second blob without changing its readable suffix.
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
    push_cycle(&db, &tables, &storage, outgoing, 1, &keypair, &ld).await;
    let second_key = current_blob_key(
        &db,
        &storage,
        "photos",
        "f2audio",
        Some("n1/Sonata No. 3.flac"),
    )
    .await;
    assert_ne!(audio_key, second_key);
    assert_eq!(
        home.get(&audio_key).as_deref(),
        Some(first.as_slice()),
        "the first generated object remains immutable",
    );
    assert_eq!(
        home.get(&second_key).as_deref(),
        Some(second.as_slice()),
        "the replacement lands at its own generated key",
    );
}

/// A browsable home's `note_photos` declaration: the blob id is its own column, apart
/// from the primary key, so a row can be repointed at a new blob while keeping its
/// identity; `cloud_path` supplies the readable suffix.
fn repointable_photo_decl() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("blob_id")
        .with_cloud_path_column("cloud_path")
}

/// Repointing a row at a new blob moves its cloud key, and the new bytes land there.
///
/// The row keeps its primary key and gets a new blob id and readable suffix. The generated
/// key includes the new blob id, so the repointing writes a new cloud object and leaves the
/// one it replaced standing at its own key.
///
/// Device A publishes a cover and device B pulls it; A then repoints the row at a fresh
/// blob and pushes; B pulls again and must serve the new bytes and drop the old.
#[tokio::test]
async fn plain_scheme_repointing_a_row_moves_its_blob_to_a_new_key() {
    // A browsable home: readable keys, objects stored in the clear (the two are one
    // choice), so the test reads the cloud object back as plaintext.
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );

    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1 = open_test_db_with_blob(repointable_photo_decl());
    let tables = test_synced_tables_with_blob(repointable_photo_decl());
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
    let old_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p1cover",
        Some("n1/cover-p1cover.jpg"),
    )
    .await;
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the first push puts the cover at the key its path names",
    );

    // Device B takes the cover before the replacement, so it is a peer holding the
    // replaced blob when the new one arrives.
    let db2 = open_test_db_with_blob(repointable_photo_decl());
    let (_t2, ld2) = temp_store_dir();
    pull_into(&db2, &storage, "dev2", &ld2).await;

    // Repoint the row at a new blob: same primary key, new blob id, and the cloud path
    // moves with it because it names the blob. The replaced blob's local copy goes away.
    store_local(&ld1, "p2cover", new_bytes).await;
    local_files::drop_blob(
        &ld1,
        "photos",
        "p1cover",
        &crate::blob::content_hash(old_bytes),
    )
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
    let new_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p2cover",
        Some("n1/cover-p2cover.jpg"),
    )
    .await;

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
    let (_updated, result) = pull_into(&db2, &storage, "dev2", &ld2).await;

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

/// Repointing a row at a new blob while retaining its cloud path is safe because the
/// generated key carries the new blob id independently of that readable suffix.
#[tokio::test]
async fn plain_scheme_repointing_a_row_without_moving_its_cloud_path_is_distinct() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
        std::sync::Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "test-lib",
        keypair.clone(),
    );

    let old_bytes = b"OLD-COVER-BYTES";
    let new_bytes = b"NEW-COVER-BYTES";

    let db1 = open_test_db_with_blob(repointable_photo_decl());
    let tables = test_synced_tables_with_blob(repointable_photo_decl());
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
    let old_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p1cover",
        Some("n1/cover-p1cover.jpg"),
    )
    .await;

    // The repointing leaves `cloud_path` unchanged.
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
    push_cycle(&db1, &tables, &storage, outgoing, 1, &keypair, &ld1).await;
    let new_key = current_blob_key(
        &db1,
        &storage,
        "photos",
        "p2cover",
        Some("n1/cover-p1cover.jpg"),
    )
    .await;
    assert_ne!(old_key, new_key);
    assert_eq!(
        home.get(&old_key).as_deref(),
        Some(old_bytes.as_slice()),
        "the replaced blob's generated object is untouched",
    );
    assert_eq!(
        home.get(&new_key).as_deref(),
        Some(new_bytes.as_slice()),
        "the new blob lands at a distinct generated key despite reusing the readable suffix",
    );
}

/// Push one cycle's captured changeset the way the sync loop does: `service::sync`
/// prepares (and uploads the host-provided blobs of) the gated changeset, then
/// `push_changeset` publishes it, as `device`.
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
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        storage,
        db,
        device,
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");
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

async fn current_blob_key(
    db: &crate::database::Database,
    storage: &CloudSyncStorage,
    namespace: &str,
    id: &str,
    cloud_path: Option<&str>,
) -> String {
    let location = db.blob_location(namespace, id).await.unwrap().unwrap();
    CloudSyncStorage::blob_key(
        storage.blob_path_scheme(),
        namespace,
        &location,
        id,
        cloud_path,
    )
    .unwrap()
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
    let storage = CloudSyncStorage::new(
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
    let outgoing = result.outgoing.expect("outgoing changeset");
    cycle::push_changeset(
        &storage,
        &db1,
        "dev1",
        outgoing.seq,
        outgoing.packed,
        "2026-01-01T00:00:00Z",
    )
    .await
    .expect("push_changeset");

    // At rest the cover photo is ciphertext, not the source bytes.
    let blob_key = current_blob_key(&db1, &storage, "photos", "p1cover", None).await;
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
    let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

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
        db2.blob_location("photos", "p1cover")
            .await
            .expect("read blob location")
            .map(|location| location.uploader),
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
    let keypair = UserKeypair::generate();
    let hlc = crate::sync::hlc::Hlc::new("dev1".to_string());
    let cipher = std::sync::RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
        [31u8; 32],
    )));
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
        blob_exists_at_recorded_location(&db1, &storage, "photos", "peager01", None).await,
        "the eager blob must be uploaded",
    );
    assert!(
        blob_exists_at_recorded_location(&db1, &storage, "covers", "clazy001", None).await,
        "the lazy blob must be uploaded",
    );

    // CacheEager: warmed into the cache, gone from the local store. The first read
    // is a local cache hit.
    assert!(
        ld1.cache_blob_path("photos", "peager01").unwrap().exists(),
        "an eager blob's local copy is moved into the cache",
    );
    assert!(
        !ld1.local_blob_path(
            "photos",
            "peager01",
            &crate::blob::content_hash(b"EAGER-BYTES"),
        )
        .unwrap()
        .exists(),
        "a Remote blob's bytes must not stay in the local store (would read as Local)",
    );

    // CacheLazy: dropped from the local store, NOT placed in the cache. A later read
    // fetches it from the cloud.
    assert!(
        !ld1.local_blob_path(
            "covers",
            "clazy001",
            &crate::blob::content_hash(b"LAZY-BYTES"),
        )
        .unwrap()
        .exists(),
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
    let location = plant_cloud_blob(
        &db1,
        &storage,
        "photos",
        "pdel1234",
        None,
        b"COVERBYTES".to_vec(),
        1000,
    )
    .await;
    storage.store_changeset_with_blob_locations(
        "dev1",
        1,
        &cs1,
        SCHEMA_VERSION,
        vec![test_blob_location_record(
            "photos",
            "pdel1234",
            location,
            b"COVERBYTES",
        )],
    );

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
    let (_cursors, result) = pull_into(&db2, &storage, "dev2", &ld).await;

    assert_eq!(result.changesets_applied, 1, "the DELETE changeset applied");
    assert!(
        !ld.pinned_blob_path("photos", "pdel1234").unwrap().exists()
            && !ld.cache_blob_path("photos", "pdel1234").unwrap().exists(),
        "applying the blob-bearing DELETE drops the cache copies",
    );
}

#[tokio::test]
async fn local_blob_cleanup_intent_survives_restart_after_cursor_commit() {
    let storage = MockSyncStorage::new();
    let cleanup_decl = || BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheLazy);
    let source = open_test_db_with_blob(cleanup_decl());
    exec(
        &source,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'WithPhoto', NULL, '0000000001000-0000-dev1', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('cleanup01', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', '0000000001000-0000-dev1', '2026-01-01');",
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
         INSERT INTO note_photos (id, note_id, kind, size, hash, _updated_at, created_at) \
         VALUES ('cleanup01', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', '0000000001000-0000-dev2', '2026-01-01');",
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
        target.get_all_sync_cursors().await.unwrap().get("dev1"),
        Some(&1),
        "filesystem cleanup does not hold the materialized cursor",
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
        restarted.get_all_sync_cursors().await.unwrap().get("dev1"),
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
         VALUES ('existing-row', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', 'other-blob', \
                 '0000000001000-0000-dev2', '2026-01-01')",
    )
    .await;
    target
        .call(|conn| {
            conn.execute(
                "INSERT INTO local_cleanup_intents (namespace, blob_id, content_hash) \
                 VALUES ('photos', 'cleanup-race', 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17')",
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
                         VALUES ('new-row', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', 'cleanup-race', \
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
            .local_blob_path(
                "photos",
                "cleanup-race",
                &crate::blob::content_hash(b"old bytes"),
            )
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
                "INSERT INTO local_cleanup_intents (namespace, blob_id, content_hash) \
                 VALUES ('photos', 'shared-intent', 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17')",
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
                     VALUES ('blocked-row', 'n1', 'cover', 9, 'b4dddecf813201f4a83f2ae71f6fa1a03ea961c3738e3da7fff94859c5ad1c17', 'shared-intent', \
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
         VALUES ('live-row', 'n1', 'cover', 15, 'e81a30ccb96fde2c2355782144d45064e8d62ce37a73e5a509198f3d9c528bce', 'shared-intent', \
                 '0000000003000-0000-dev2', '2026-01-01')",
    )
    .await;
    store_local(&store_dir, "shared-intent", b"recreated bytes").await;
    assert!(!crate::blob::local_cleanup::drain(&target, &store_dir)
        .await
        .unwrap());
    assert!(
        store_dir
            .local_blob_path(
                "photos",
                "shared-intent",
                &crate::blob::content_hash(b"recreated bytes"),
            )
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
    let shared_location = plant_cloud_blob(
        &db1,
        &storage,
        "photos",
        "sharedblob",
        None,
        b"SHARED-BYTES".to_vec(),
        1000,
    )
    .await;
    storage.store_changeset_with_blob_locations(
        "dev1",
        1,
        &cs1,
        SCHEMA_VERSION,
        vec![test_blob_location_record(
            "photos",
            "sharedblob",
            shared_location,
            b"SHARED-BYTES",
        )],
    );

    let db2 = open_test_db_with_blob(decl);
    let (_tmp, ld) = temp_store_dir();
    let (_cursors, result) = pull_into(&db2, &storage, "dev2", &ld).await;
    assert_eq!(result.changesets_applied, 1);
    assert!(
        ld.cache_blob_path("photos", "sharedblob").unwrap().exists(),
        "the shared CacheEager blob lands in the cache",
    );

    let new_location = plant_cloud_blob(
        &db1,
        &storage,
        "photos",
        "newblob",
        None,
        b"NEW-BYTES".to_vec(),
        2000,
    )
    .await;
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
    storage.store_changeset_with_blob_locations(
        "dev1",
        2,
        &cs2,
        SCHEMA_VERSION,
        vec![test_blob_location_record(
            "photos",
            "newblob",
            new_location,
            b"NEW-BYTES",
        )],
    );

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

/// Owner anchoring (issue #95/#102): a puller with a pinned owner refuses a chain
/// whose founder is a different key — the wipe-and-refound takeover — rather than
/// adopting it and authorizing the attacker.
#[tokio::test]
async fn pull_refuses_a_chain_not_anchored_to_the_pinned_owner() {
    let storage = MockSyncStorage::new();

    // The attacker wiped membership/* and refounded themselves as Owner.
    let attacker = UserKeypair::generate();
    let attacker_pk = hex::encode(attacker.public_key());
    let forged = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &attacker_pk, 1, forged).await;
    publish_membership_chain_head(&storage, &chain, &attacker).await;

    // The puller has the real owner pinned (a different key).
    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
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
    let storage = MockSyncStorage::new();

    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
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
    let storage = MockSyncStorage::new();
    let db = open_test_db();
    if pin_owner {
        db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &founder_pubkey)
            .await
            .unwrap();
    }

    let mut chain = MembershipChain::new();
    let founder_entry = founder_entry(&founder, "2026-03-01T00:00:00Z");
    append_membership_entry(&storage, &mut chain, &founder_pubkey, 1, founder_entry).await;
    let add_owner = make_linked_entry(
        &chain,
        &founder,
        MembershipAction::Add,
        &second_owner,
        MemberRole::Owner,
        "2026-03-01T00:01:00Z",
    );
    append_membership_entry(&storage, &mut chain, &founder_pubkey, 2, add_owner).await;
    let add_member = make_linked_entry(
        &chain,
        &founder,
        MembershipAction::Add,
        &removed_member,
        MemberRole::Member,
        "2026-03-01T00:02:00Z",
    );
    append_membership_entry(&storage, &mut chain, &founder_pubkey, 3, add_member).await;
    let remove_member = make_linked_entry(
        &chain,
        &second_owner,
        MembershipAction::Remove,
        &removed_member,
        MemberRole::Member,
        "2026-03-01T00:03:00Z",
    );
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
        .set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pubkey)
        .await
        .unwrap();

    let mut chain = MembershipChain::new();
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    append_membership_entry(&storage, &mut chain, &owner_pubkey, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let cycle_membership = crate::sync::pull::load_cycle_membership(&storage, &target)
        .await
        .expect("load founder at cycle start");

    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pubkey.clone(),
            seq: 2,
        }),
        &changeset,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let (_tmp, store_dir) = temp_store_dir();
    let (updated, result) = crate::sync::pull::pull_changes(
        &target,
        target.synced_tables(),
        &storage,
        "dev2",
        &store_dir,
        cycle_membership.chain,
        cycle_membership.pinned_owner,
    )
    .await
    .expect("pull with an empty mid-cycle membership LIST");

    assert_eq!(result.changesets_applied, 1);
    assert!(result.rejected_unauthorized.is_empty());
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
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // Membership can't even be listed: the cycle must abort rather than continue
    // with authorization silently disabled.
    storage.fail_membership_listing();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(PullError::Storage(_))),
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    let packed = envelope::pack_signed(
        "devOwner",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &owner,
        // The founder entry at (owner, 1) is what authorizes the owner to write.
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    storage.put_changeset_packed("devOwner", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    let packed = envelope::pack_signed(
        "devOwner",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:01:00Z",
        &owner,
        None,
        &changeset,
    );
    storage.put_changeset_packed("devOwner", 1, packed);

    let target = open_test_db();
    target
        .set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (updated, result) = pull_into(&target, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devOwner"), Some(&1));
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devOwner",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &changeset,
    );
    // `put_changeset_packed` advances the stream head with the mock's owner key,
    // while the envelope above is signed by the member.
    storage.put_changeset_packed("devOwner", 1, packed);

    let target = open_test_db();
    target
        .set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (updated, result) = pull_into(&target, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devOwner"), Some(&1));
}

/// Issue #84 — the membership-propagation lag, the core bug. A member's signed
/// changeset is pulled BEFORE the LIST that rebuilds the chain shows the Add that
/// authorizes them (membership entries and changesets are separate, unordered
/// object streams). The cycle-start chain does not authorize the member, so the
/// old code skipped the changeset and advanced the cursor — losing it forever.
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
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // The lagging entry was fetched by coordinate and the changeset applied — not
    // dropped as non-member, and not surfaced as a rejection.
    assert_eq!(result.changesets_applied, 1);
    assert!(result.rejected_unauthorized.is_empty());
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), Some(&1));
}

/// Issue #84 — the other side of the split: a genuinely unauthorized changeset
/// (here authored by a key that is NOT in the chain at all, with a grant
/// coordinate that resolves to an entry that doesn't authorize it) is judged
/// against the exact entry it names, found wanting, and SKIPPED — cursor advanced
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    let packed = envelope::pack_signed(
        "devX",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &outsider,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    storage.put_changeset_packed("devX", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // Nothing applied; the changeset is surfaced as rejected-unauthorized and the
    // cursor advances past it (the device must not stall on forged content).
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(result.rejected_unauthorized[0].device_id, "devX");
    assert_eq!(result.rejected_unauthorized[0].seq, 1);
    assert_eq!(
        result.rejected_unauthorized[0].author,
        hex::encode(outsider.public_key())
    );
    assert_eq!(updated.get("devX"), Some(&1));
    assert_eq!(
        db2.get_all_sync_cursors().await.unwrap().get("devX"),
        Some(&1),
    );
}

/// Issue #86 — a changeset whose signature does not verify (forged or corrupt in
/// transit) is rejected, logged at error, and surfaced as `invalid_signatures` so
/// the host can warn; the cursor holds at it. The signature check runs before the
/// authorization judgment, so a corrupt signature is reported as an invalid
/// signature, not as unauthorized.
#[tokio::test]
async fn pull_holds_and_surfaces_a_changeset_with_an_invalid_signature() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    let packed = envelope::pack_signed(
        "dev1",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &owner,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    // Corrupt the signature: an all-zero 64-byte Ed25519 signature is well-formed
    // but never verifies. Repack without re-signing so the envelope is otherwise
    // intact (the changeset_size still matches, so this reaches the signature check
    // rather than failing envelope parsing).
    let (mut env, changeset_bytes) = envelope::unpack(&packed).unwrap();
    env.signature = "0".repeat(128);
    storage.put_changeset_packed("dev1", 1, envelope::pack(&env, &changeset_bytes));

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // Nothing applied; surfaced as an invalid signature (NOT unauthorized) and the
    // cursor holds at the bad object.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert!(result.rejected_unauthorized.is_empty());
    assert_eq!(result.invalid_signatures.len(), 1);
    assert_eq!(result.invalid_signatures[0].device_id, "dev1");
    assert_eq!(result.invalid_signatures[0].seq, 1);
    assert_eq!(updated.get("dev1"), None);
}

/// Issue #84 — a removed member's changeset is skipped, not applied. The owner
/// added the member at (owner, 2) then removed them at (owner, 3); the member's
/// changeset names its (still-valid-looking) Add grant (owner, 2), but the puller
/// already holds the Remove, so merging the grant into the full chain still leaves
/// the author unauthorized. Surfaced and cursor-advanced, like any forged write.
#[tokio::test]
async fn pull_skips_a_removed_members_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    let remove_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Remove,
        &member,
        MemberRole::Member,
        "2026-03-01T00:03:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:04:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(
        result.rejected_unauthorized[0].author,
        hex::encode(member.public_key())
    );
    assert_eq!(updated.get("devM"), Some(&1));
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    let remove_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Remove,
        &member,
        MemberRole::Member,
        "2026-03-01T00:03:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:04:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // Not applied: the removed member is not re-admitted by the lagging listing.
    // Surfaced as rejected-unauthorized and the cursor advances so the device is
    // not stuck on it.
    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(
        result.rejected_unauthorized[0].author,
        hex::encode(member.public_key())
    );
    assert_eq!(updated.get("devM"), Some(&1));
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
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // The Add is visible by keyed GET but absent from the signed committed prefix.
    assert_eq!(storage.membership_list_count(), 2);
    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), Some(&1));
}

#[tokio::test]
async fn relocated_membership_grant_cannot_authorize_a_changeset() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let relocated_author = hex::encode(UserKeypair::generate().public_key());
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;

    let grant_bytes = storage
        .get_membership_entry(&owner_pk, 2)
        .await
        .expect("owner's uncommitted grant");
    storage
        .put_membership_entry(&relocated_author, 2, grant_bytes)
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: relocated_author,
            seq: 2,
        }),
        &changeset,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let target = open_test_db();
    target
        .set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();
    let (updated, result) = pull_into(&target, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), Some(&1));
}

/// A storage read failure while resolving a grant leaves the cursor at the
/// undecided changeset. The pull must not replace an unavailable committed-chain
/// read with a bare keyed entry.
#[tokio::test]
async fn pull_holds_the_cursor_when_the_mid_cycle_membership_list_fails() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(owner.clone());

    // The owner publishes a head covering only the founder entry (seq 1) before
    // adding the member, so the Add at seq 2 is uploaded but no head certifies it
    // yet — genuinely uncommitted, not just list-lagging.
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
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
    let packed = envelope::pack_signed(
        "devM",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:02:00Z",
        &member,
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 2,
        }),
        &cs,
    );
    storage.put_changeset_packed("devM", 1, packed);
    storage.publish_head_as("devM", 1, &member);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // The failed read leaves authorization undecided and the cursor unchanged.
    assert_eq!(storage.membership_list_count(), 2);
    assert_eq!(result.changesets_applied, 0);
    assert!(result.rejected_unauthorized.is_empty());
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devM"), None);
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member.clone()).await;
    let remove_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Remove,
        &member,
        MemberRole::Member,
        "2026-03-01T00:03:00Z",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // First cycle: accepts the head at seq 3 (member removed), persisting the
    // reader's watermark at 3.
    pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    // A stale replica serves the head from before the Remove (seq 2, signed over
    // the Add's tip hash).
    let stale = AuthorHead::signed(2, entry_hash(&add_member), &owner);
    storage
        .put_membership_head(&owner_pk, serde_json::to_vec(&stale).unwrap())
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
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
    let mut bad = founder_entry(&attacker, "2026-03-01T00:00:00Z");
    bad.signature = "00".to_string();
    storage
        .put_membership_entry(
            &hex::encode(attacker.public_key()),
            1,
            serde_json::to_vec(&bad).unwrap(),
        )
        .await
        .unwrap();

    let owner = UserKeypair::generate();
    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &hex::encode(owner.public_key()))
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(result, Err(PullError::MembershipTampered(_))),
        "a malformed chain on a pinned-owner store must be refused, got {:?}",
        result.map(|_| ()),
    );
}

/// A verified head whose signer is not a current member is examined so a newly
/// added member can resolve a committed grant that appeared after cycle start.
/// When the envelope's named grant does not authorize the signer, the changeset
/// is rejected and the cursor advances instead of holding on attacker content.
#[tokio::test]
async fn pull_rejects_a_stream_authored_by_a_non_member() {
    // The mock signs every head it publishes with `outsider`, who is not in the
    // chain — so the head it writes for `dev1` fails the membership check.
    let outsider = UserKeypair::generate();
    let storage = MockSyncStorage::with_keypair(outsider);

    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    storage.store_changeset("dev1", 1, &cs, SCHEMA_VERSION);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 0);
    assert!(!row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(result.rejected_unauthorized.len(), 1);
    assert_eq!(updated.get("dev1"), Some(&1));
    assert!(result.remote_heads.iter().any(|h| h.device_id == "dev1"));
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

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
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
    let packed = envelope::pack_signed(
        "devA",
        1,
        SCHEMA_VERSION,
        "",
        "2026-03-01T00:00:00Z",
        &owner,
        // The founder entry at (owner, 1) is what authorizes the owner to write.
        Some(MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 1,
        }),
        &cs,
    );
    storage.put_changeset_packed("devA", 1, packed);

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let (updated, result) = pull_into(&db2, &storage, "dev2", &temp_store_dir().1).await;

    assert_eq!(result.changesets_applied, 1);
    assert!(row_exists(&db2, "SELECT 1 FROM notes WHERE id = 'n1'").await);
    assert_eq!(updated.get("devA"), Some(&1));
}

/// A `min_schema_version` floor signed by a non-owner is ignored: it must NOT trip
/// `SchemaVersionTooOld` even when its version exceeds ours. The floor is an
/// owner-only control; a Member- or Follower-signed (or bucket-planted) one is a
/// freeze attempt.
#[tokio::test]
async fn pull_ignores_min_schema_version_from_a_non_owner() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    let member = UserKeypair::generate();

    // The mock signs the floor with `member` (a current member, but not an owner).
    let storage = MockSyncStorage::with_keypair(member.clone());

    // Chain: owner founds, then adds `member` as a Member.
    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    let add_member = make_linked_entry(
        &chain,
        &owner,
        MembershipAction::Add,
        &member,
        MemberRole::Member,
        "2026-03-01T00:01:00Z",
    );
    append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    // A member-signed floor above our version.
    storage
        .set_min_schema_version(SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    // The pull must succeed (the non-owner floor is ignored), not error with
    // SchemaVersionTooOld.
    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        result.is_ok(),
        "a non-owner floor must be ignored, not trip SchemaVersionTooOld; got {:?}",
        result.map(|_| ()),
    );
}

/// A `min_schema_version` floor signed by a current owner IS honored: a version
/// above ours trips `SchemaVersionTooOld`, the same refuse-to-sync the owner
/// intends after a breaking migration.
#[tokio::test]
async fn pull_honors_min_schema_version_from_a_current_owner() {
    let owner = UserKeypair::generate();
    let owner_pk = hex::encode(owner.public_key());
    // The mock is the owner's device, so the floor it sets is owner-signed.
    let storage = MockSyncStorage::with_keypair(owner.clone());

    let founder = founder_entry(&owner, "2026-03-01T00:00:00Z");
    let mut chain = MembershipChain::new();
    append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
    publish_membership_chain_head(&storage, &chain, &owner).await;

    storage
        .set_min_schema_version(SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let db2 = open_test_db();
    db2.set_sync_state(OWNER_PUBKEY_STATE_KEY, &owner_pk)
        .await
        .unwrap();

    let result = pull_into_result(&db2, &storage, "dev2", &temp_store_dir().1).await;
    assert!(
        matches!(
            result,
            Err(PullError::SchemaVersionTooOld {
                min_version,
                ..
            }) if min_version == SCHEMA_VERSION + 1
        ),
        "an owner-signed floor above our version must trip SchemaVersionTooOld; got {:?}",
        result.map(|_| ()),
    );
}

mod schema_version_too_old_display {
    use crate::sync::pull::PullError;

    #[test]
    fn names_app_and_versions_and_recovery() {
        let err = PullError::SchemaVersionTooOld {
            local_version: 3,
            min_version: 5,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("Update the app"),
            "missing recovery verb: {msg}"
        );
        assert!(msg.contains("v5"), "missing required version: {msg}");
        assert!(msg.contains("v3"), "missing current version: {msg}");
    }
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

    /// A blob whose `id` climbs out of the cache directory with `..` must NOT have
    /// its bytes written outside it. coven builds the destination from the id under
    /// its store cache; without the boundary check the id would resolve to a path
    /// above the cache and the downloaded bytes land there (an arbitrary-file-write
    /// RCE); the check refuses such a row as bad data, so nothing is written outside
    /// the cache and the apply is held.
    #[tokio::test]
    async fn traversal_id_does_not_write_outside_the_blob_dir() {
        let storage = MockSyncStorage::new();
        let db1 = open_test_db();

        // The attacker's blob bytes, planted in the cloud under the malicious id's
        // flat mock key (the same key the puller's `get_blob` computes for it). No
        // local file is written on the source side, so nothing escapes here.
        let evil_bytes = b"OWNED".to_vec();
        let location = test_blob_location(&storage.own_uploader().unwrap(), 1000);

        // The source's changeset adds a note + a photo row whose id is the
        // traversal string. (The mock stored the blob above; this is the row that
        // references it.)
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
        storage.store_changeset_with_blob_locations(
            "dev1",
            1,
            &cs,
            SCHEMA_VERSION,
            vec![test_blob_location_record(
                "photos",
                "x/../../../PWNED",
                location,
                &evil_bytes,
            )],
        );

        // The puller builds the blob's destination from the validated id under its
        // own store dir. `download_blobs` rejects the traversal id (it is not a
        // safe path token) before building any path, so nothing is written — the
        // `dir.join(id)` escape is structurally unreachable (the id validation is
        // proven by the `store_dir` unit tests).
        let db2 = open_test_db_with_blob(photo_decl());
        let (_t, ld) = temp_store_dir();
        let (updated, result) = pull_into(&db2, &storage, "dev2", &ld).await;

        // It is bad data, so the row that carries it is not applied and the cursor
        // does not advance — the same posture as any other failed-blob changeset.
        assert!(
            result.asset_downloads_failed,
            "a refused blob fails the changeset's downloads",
        );
        assert_eq!(result.changesets_applied, 0, "the bad row is not applied");
        assert_eq!(updated.get("dev1"), None, "the cursor is held for retry");
    }

    /// A blob id too short to form the `{ab}/{cd}` partition prefix (the
    /// dash-stripped id is under four chars, or splits a multi-byte char) cannot
    /// index the prefix's byte slice, so the path builder refuses it. End to end
    /// it is bad data: the row does not apply and the cursor holds. (The slice
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
        assert_eq!(updated.get("dev1"), None, "the cursor is held for retry");
    }

    /// A normal blob id still round-trips: the boundary check rejects only ids that
    /// could escape the cache or can't be partitioned, and a well-formed id writes
    /// its blob into the pinned cache at its partitioned `{ab}/{cd}/<id>` path.
    #[tokio::test]
    async fn normal_id_still_writes_under_the_blob_dir() {
        let storage = MockSyncStorage::new();
        let db1 = open_test_db();

        let location = plant_cloud_blob(
            &db1,
            &storage,
            "photos",
            "p1ab",
            None,
            b"PHOTOBYTES".to_vec(),
            1000,
        )
        .await;

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
        storage.store_changeset_with_blob_locations(
            "dev1",
            1,
            &cs,
            SCHEMA_VERSION,
            vec![test_blob_location_record(
                "photos",
                "p1ab",
                location,
                b"PHOTOBYTES",
            )],
        );

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

/// Deterministic repro (ignored) — the NOTFOUND→OMIT causal-ordering divergence.
///
/// A device that applies an UPDATE of a row before that row's INSERT (authored by
/// another device) hits `SQLITE_CHANGESET_NOTFOUND`, which the apply reads as "the row
/// was deleted locally, delete wins" and OMITs — dropping the UPDATE and advancing the
/// cursor. That device converges to the bare INSERT while the authors converge to the
/// UPDATE: permanent divergence, decided by the order `list_heads()` happened to return
/// (it is not a causally-consistent snapshot). The same OMIT is also wrong after a
/// DELETE, which coven leaves no durable tombstone for, so absence cannot be told apart
/// from not-yet-inserted.
///
/// The repro forces the bad order: `sort_listings` makes head order lexicographic and
/// the updater's device id (`dev-a`) sorts before the inserter's (`dev-z`), so the third
/// device processes the UPDATE stream first every run. It asserts the correct converged
/// title, which fails today.
///
/// Ignored because the fix belongs in circles' `BatchCommit` + audience-mirror apply
/// model, not a standalone apply-path change that model would tear out. Un-ignore when
/// that lands. See `plans/notfound-omit-for-circles.md`.
#[tokio::test]
#[ignore = "NOTFOUND→OMIT causal-ordering divergence: an UPDATE applied before its INSERT (or after a tombstoneless DELETE) is dropped as a local delete and diverges permanently. Fix lands with circles' BatchCommit/mirror apply model — see plans/notfound-omit-for-circles.md"]
async fn update_applied_before_its_insert_diverges_notfound_omit() {
    let home = InMemoryCloudHome::new();
    // Force a deterministic cross-device apply order so the bug reproduces every run —
    // a real bucket LIST is unordered, which is why the live test raced ~50/50.
    home.sort_listings();
    let keypair = UserKeypair::generate();
    let storage = CloudSyncStorage::new(
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
    pull_into(&db_upd, &storage, "dev-a", &ld_upd).await;
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
    pull_into(&db_c, &storage, "dev-c", &ld_c).await;

    assert_eq!(
        query_text(&db_c, "SELECT title FROM notes WHERE id = 'n1'").await,
        "updated",
        "the UPDATE applied before its INSERT must not be dropped as a local delete",
    );
}
