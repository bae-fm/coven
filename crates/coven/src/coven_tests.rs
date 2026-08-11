use super::*;

use crate::{WriteId, WriteReceipt};
use async_trait::async_trait;
use coven_foundation::config::Config;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::test_keyring;
use coven_protocol::blob::{BlobRef, BlobScope, CacheFill, Provenance};
use coven_protocol::objects::ObjectSlot;
use coven_protocol::synced_schema::BlobDecl;
use coven_replication::sync::test_helpers::TestStore;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::cloud::{
    BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError,
    ExactCreateOutcome, ExactSlotStorage, ExactUpload, UploadProgress,
};
use coven_storage::CloudCipher;
use rusqlite::{params, OptionalExtension};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Notify;

fn builder(dir: StoreDir) -> CovenBuilder {
    Coven::builder(
        dir,
        Config::with_defaults(
            "lib-test".to_string(),
            "device-test".to_string(),
            "Test".to_string(),
        ),
    )
}

async fn query_handle_text(handle: &CovenHandle, sql: &str) -> String {
    let sql = sql.to_string();
    handle
        .read(move |connection| {
            connection
                .query_row(&sql, [], |row| row.get(0))
                .map_err(CovenError::from)
        })
        .await
        .expect("query text through the host read capability")
}

async fn handle_row_exists(handle: &CovenHandle, sql: &str) -> bool {
    let sql = sql.to_string();
    handle
        .read(move |connection| {
            connection
                .query_row(&sql, [], |_| Ok(true))
                .optional()
                .map(|value| value.unwrap_or(false))
                .map_err(CovenError::from)
        })
        .await
        .expect("query row existence through the host read capability")
}

fn media_files_decl() -> BlobDecl {
    BlobDecl::new(
        "media-files",
        Provenance::HostProvided,
        CacheFill::CacheLazy,
    )
    .with_id_column("blob_id")
}

fn files_table() -> SyncedTable {
    SyncedTable::new("files", crate::RowIdentity::SharedKey).carries_blob(media_files_decl())
}

fn remote_root_files_table() -> SyncedTable {
    SyncedTable::new("files", crate::RowIdentity::SharedKey)
        .remote_root()
        .carries_blob(media_files_decl())
}

fn scoped_files_table() -> SyncedTable {
    SyncedTable::new("files", crate::RowIdentity::SharedKey)
        .scoped_by("audience")
        .carries_blob(media_files_decl())
}

fn files_migration() -> Migration {
    Migration::sql(
        1,
        "test-schema",
        "CREATE TABLE files (
                id TEXT PRIMARY KEY,
                blob_id TEXT,
                size INTEGER NOT NULL,
                hash TEXT,
                _updated_at TEXT NOT NULL
            ) STRICT;",
    )
}

fn scoped_files_migration() -> Migration {
    Migration::sql(
        1,
        "scoped-files",
        "CREATE TABLE files (
                id TEXT PRIMARY KEY,
                blob_id TEXT,
                size INTEGER NOT NULL,
                hash TEXT,
                audience TEXT,
                _updated_at TEXT NOT NULL
            ) STRICT;",
    )
}

fn gated_roots_table() -> SyncedTable {
    SyncedTable::new("roots", crate::RowIdentity::SharedKey).gated_by("shared")
}

fn gated_roots_migration() -> Migration {
    Migration::sql(
        1,
        "gated-roots",
        "CREATE TABLE roots (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
    )
}

fn open_gated_roots_handle() -> (tempfile::TempDir, CovenHandle) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let handle = builder(StoreDir::new_ephemeral(tmp.path()))
        .synced_tables(vec![gated_roots_table()])
        .migrations(vec![gated_roots_migration()])
        .open()
        .expect("open gated handle");
    (tmp, handle)
}

fn open_gated_roots_at(dir: StoreDir) -> CovenResult<CovenHandle> {
    builder(dir)
        .synced_tables(vec![gated_roots_table()])
        .migrations(vec![gated_roots_migration()])
        .open()
}

fn precreate_database(dir: &StoreDir, sql: &str) {
    let database =
        coven_database::DatabaseImageTest::open(&dir.db_path()).expect("precreate store database");
    database
        .execute_batch(sql)
        .expect("seed pre-existing database state");
}

#[test]
fn precreated_empty_sqlite_file_initializes_coven_metadata() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    precreate_database(&dir, "");

    open_gated_roots_at(dir).expect("initialize Coven in an empty SQLite database");
}

#[test]
fn existing_host_tables_without_a_coven_marker_initialize_coven_metadata() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    precreate_database(
        &dir,
        "CREATE TABLE roots (
                 id TEXT PRIMARY KEY,
                 title TEXT NOT NULL,
                 shared INTEGER NOT NULL,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             PRAGMA user_version = 1;",
    );

    open_gated_roots_at(dir).expect("initialize Coven beside an existing host schema");
}

#[test]
fn interrupted_coven_schema_without_a_marker_is_rejected() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    coven_database::DatabaseImageTest::open(&dir.db_path())
        .expect("precreate interrupted Coven database")
        .create_interrupted_coven_schema()
        .expect("seed interrupted Coven schema");

    let error = match open_gated_roots_at(dir) {
        Ok(_) => panic!("partial Coven schema has no valid initialization commit"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        CovenError::Database(DbError::Message(reason))
            if reason.contains("without the required initialization marker")
    ));
}

#[tokio::test]
async fn host_sql_cannot_discover_or_mutate_the_gate_baseline() {
    let (_tmp, handle) = open_gated_roots_handle();

    let discovery = handle
        .read(|sql| {
            sql.query("PRAGMA database_list", [], |row| row.get::<_, String>(1))
                .map_err(CovenError::from)
        })
        .await;
    assert!(
        discovery.is_err(),
        "host SQL must not enumerate coven's attached gate baseline",
    );

    let mutation = handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO coven_gate_empty.roots \
                     (id, title, shared, _updated_at) \
                     VALUES ('root-1', 'Private', 1, '0000000002000-0000-device-test')",
                [],
            )?;
            Ok(())
        })
        .await;
    assert!(
        mutation.is_err(),
        "host SQL must not address coven's attached gate baseline",
    );

    handle
        .write(|sql| {
            sql.execute_batch(
                "CREATE TABLE host_local (id TEXT PRIMARY KEY, value TEXT) STRICT; \
                     INSERT INTO host_local VALUES ('local-1', 'kept'); \
                     INSERT INTO roots (id, title, shared, _updated_at) \
                     VALUES ('root-1', 'Private', 0, \
                             '0000000001000-0000-device-test');",
            )?;
            Ok(())
        })
        .await
        .expect("arbitrary host-schema SQL remains available");
    let published = handle
        .write(|sql| {
            sql.execute(
                "UPDATE roots SET shared = 1, \
                     _updated_at = '0000000002000-0000-device-test' \
                     WHERE id = 'root-1'",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("flip root visible");
    let write_id = published.write_id.clone();
    let changeset = handle
        .store_write_partition_for_test(&write_id)
        .await
        .expect("load gated changeset");
    let rows = coven_database::walk_changeset(&changeset).expect("walk gated changeset");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].op, coven_foundation::changeset::ChangeOp::Insert);
    assert_eq!(rows[0].pk(), Some("root-1"));
    assert_eq!(rows[0].col(1), Some("Private"));
    assert_eq!(rows[0].col(2), Some("1"));
    assert_eq!(rows[0].col(3), Some("0000000002000-0000-device-test"));
}

fn open_files_handle() -> (tempfile::TempDir, CovenHandle) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let handle = builder(dir)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open handle");
    (tmp, handle)
}

#[tokio::test]
async fn configured_clock_is_the_hlc_wall_source() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let clock = chrono::DateTime::from_timestamp_millis(1_234).expect("valid clock instant");
    let handle = builder(StoreDir::new_ephemeral(tmp.path()))
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .clock(Arc::new(coven_foundation::clock::FixedClock(clock)))
        .open()
        .expect("open handle");

    let receipt = handle
        .write(|sql| {
            let stamp = sql.stamp();
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('configured-clock', NULL, 0, ?1)",
                [&stamp],
            )?;
            Ok(stamp)
        })
        .await
        .expect("write with configured clock");

    assert_eq!(receipt.value, "0000000001234-0000-device-test");
}

#[tokio::test]
async fn second_open_of_one_store_is_refused_until_the_first_handle_drops() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let first = builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("first open succeeds");
    let clone = first.clone();

    let second = builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open();
    assert!(matches!(
        second,
        Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
    ));

    drop(first);

    let still_locked = builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open();
    assert!(matches!(
        still_locked,
        Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
    ));

    drop(clone);

    builder(dir)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open succeeds after the first handle drops");
}

#[tokio::test]
async fn a_zero_or_negative_blob_tombstone_grace_is_refused_at_open() {
    for grace in [chrono::Duration::zero(), chrono::Duration::seconds(-1)] {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new_ephemeral(tmp.path());
        let result = builder(dir)
            .synced_tables(vec![files_table()])
            .migrations(vec![files_migration()])
            .blob_tombstone_grace(grace)
            .open();
        assert!(
            matches!(result, Err(CovenError::InvalidBlobTombstoneGrace)),
            "grace {grace:?} must be refused at open",
        );
    }
}

#[tokio::test]
async fn a_positive_blob_tombstone_grace_opens() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    builder(dir)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .blob_tombstone_grace(chrono::Duration::hours(1))
        .open()
        .expect("a positive grace opens");
}

fn open_remote_root_files_handle() -> (tempfile::TempDir, CovenHandle) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let handle = builder(dir)
        .synced_tables(vec![remote_root_files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open handle");
    (tmp, handle)
}

fn open_files_handle_in(dir: StoreDir) -> CovenHandle {
    try_open(&dir).expect("open handle")
}

async fn merge_test_storage(
    handle: &CovenHandle,
    keypair: &coven_keys::keys::UserKeypair,
    home: std::sync::Arc<crate::InMemoryCloudHome>,
) -> std::sync::Arc<TestStore> {
    handle
        .create_test_store("lib-test", keypair.clone(), home)
        .await
        .expect("create exact test Store")
}

/// Publishes the handle's pending Store writes through a fresh test Store, and
/// returns that Store so a peer can pull the same history back out of it.
async fn publish_pending_through_a_test_store(handle: &CovenHandle) -> std::sync::Arc<TestStore> {
    let storage = merge_test_storage(
        handle,
        &coven_keys::keys::UserKeypair::generate(),
        coven_replication::sync::test_helpers::test_cloud_home(),
    )
    .await;
    handle
        .publish_test_store(&storage)
        .await
        .expect("publish pending Store write");
    storage
}

trait CovenHandleWriteTestOps {
    async fn publish_current_writes(&self);
}

impl CovenHandleWriteTestOps for CovenHandle {
    async fn publish_current_writes(&self) {
        let keypair = coven_keys::keys::UserKeypair::generate();
        let storage = merge_test_storage(
            self,
            &keypair,
            coven_replication::sync::test_helpers::test_cloud_home(),
        )
        .await;
        self.publish_test_store(&storage)
            .await
            .expect("publish pending Store write");
    }
}

#[tokio::test]
async fn write_survives_reopen_before_sync_cycle() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let handle = open_files_handle_in(dir.clone());
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-before-reopen', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("write before reopen");
    drop(handle);

    let reopened = open_files_handle_in(dir);
    let keypair = coven_keys::keys::UserKeypair::generate();
    let storage = merge_test_storage(
        &reopened,
        &keypair,
        coven_replication::sync::test_helpers::test_cloud_home(),
    )
    .await;
    reopened
        .publish_test_store(&storage)
        .await
        .expect("publish pending Store write");

    let (_peer_tmp, peer) = open_files_handle();
    peer.pull_test_store(&storage).await;
    assert_eq!(
        query_handle_text(
            &peer,
            "SELECT id FROM files WHERE id = 'file-before-reopen'"
        )
        .await,
        "file-before-reopen",
    );
}

#[tokio::test]
async fn separate_host_transactions_publish_as_separate_store_commits_after_restart() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let handle = open_files_handle_in(dir.clone());
    let mut write_ids = Vec::new();
    for id in ["file-pending-a", "file-pending-b"] {
        let receipt = handle
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                    (id, sql.stamp()),
                )?;
                Ok(())
            })
            .await
            .expect("write before reopen");
        assert_eq!(receipt.status, crate::WriteStatus::Pending);
        write_ids.push(receipt.write_id);
    }
    drop(handle);

    let reopened = open_files_handle_in(dir);
    let pending = reopened.pending_writes().await.expect("pending writes");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].write_id, write_ids[0]);
    assert_eq!(pending[1].write_id, write_ids[1]);
    let mut first_status = reopened
        .subscribe_write_status(&write_ids[0])
        .await
        .expect("subscribe after restart");
    assert_eq!(*first_status.borrow(), crate::WriteStatus::Pending);
    let keypair = coven_keys::keys::UserKeypair::generate();
    let storage = merge_test_storage(
        &reopened,
        &keypair,
        coven_replication::sync::test_helpers::test_cloud_home(),
    )
    .await;
    reopened
        .publish_test_store(&storage)
        .await
        .expect("publish first pending Store write");

    first_status.changed().await.expect("published status");
    let first_sequence = match &*first_status.borrow() {
        crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
        status => panic!("first host transaction is not published: {status:?}"),
    };
    assert_eq!(
        reopened
            .write_status(&write_ids[1])
            .await
            .expect("second status after first publication"),
        crate::WriteStatus::Pending,
    );
    reopened
        .publish_test_store(&storage)
        .await
        .expect("publish second pending Store write");
    let second_sequence = match reopened
        .write_status(&write_ids[1])
        .await
        .expect("second status")
    {
        crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
        status => panic!("second host transaction is not published: {status:?}"),
    };
    assert_eq!(second_sequence, first_sequence + 1);
    assert!(reopened
        .pending_writes()
        .await
        .expect("published writes are not pending")
        .is_empty());

    let (_peer_tmp, peer) = open_files_handle();
    peer.pull_test_store(&storage).await;
    assert!(handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-pending-a'").await);
    assert!(handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-pending-b'").await);
}

#[tokio::test]
async fn device_local_transaction_is_local_only_and_never_pending() {
    let (_tmp, handle) = open_files_handle();
    let receipt = handle
        .write(|sql| {
            sql.execute_batch(
                "CREATE TABLE local_notes (id TEXT PRIMARY KEY, body TEXT) STRICT;
                     INSERT INTO local_notes VALUES ('local-1', 'private');",
            )?;
            Ok("saved")
        })
        .await
        .expect("local transaction");

    assert_eq!(receipt.value, "saved");
    assert_eq!(receipt.status, crate::WriteStatus::LocalOnly);
    assert_eq!(
        handle
            .write_status(&receipt.write_id)
            .await
            .expect("durable local status"),
        crate::WriteStatus::LocalOnly
    );
    assert!(handle
        .pending_writes()
        .await
        .expect("pending writes")
        .is_empty());
    let lease_count = handle
        .write_blob_lease_count_for_test(&receipt.write_id)
        .await
        .expect("count local-only blob leases");
    assert_eq!(lease_count, 0);

    publish_pending_through_a_test_store(&handle).await;
    assert_eq!(
        handle
            .write_status(&receipt.write_id)
            .await
            .expect("local status after sync"),
        crate::WriteStatus::LocalOnly
    );
    assert!(handle
        .pending_writes()
        .await
        .expect("pending writes after sync")
        .is_empty());
}

#[tokio::test]
async fn mixed_transaction_tracks_and_publishes_only_shared_rows() {
    let (_tmp, handle) = open_files_handle();
    let receipt = handle
        .write(|sql| {
            sql.execute_batch(
                "CREATE TABLE local_notes (id TEXT PRIMARY KEY, body TEXT) STRICT;
                     INSERT INTO local_notes VALUES ('local-1', 'private');",
            )?;
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at)
                     VALUES ('shared-1', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("mixed transaction");

    assert_eq!(receipt.status, crate::WriteStatus::Pending);
    let pending = handle.pending_writes().await.expect("pending mixed write");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].write_id, receipt.write_id);
    assert_eq!(
        pending[0].affected_rows,
        vec![crate::AffectedRow {
            table: "files".to_string(),
            primary_key: "shared-1".to_string(),
        }]
    );

    let storage = publish_pending_through_a_test_store(&handle).await;
    assert!(matches!(
        handle
            .write_status(&receipt.write_id)
            .await
            .expect("published mixed write"),
        crate::WriteStatus::Published(_)
    ));

    let (_peer_tmp, peer) = open_files_handle();
    peer.pull_test_store(&storage).await;
    assert!(handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'shared-1'").await);
    assert!(
        !handle_row_exists(
            &peer,
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'local_notes'"
        )
        .await
    );
    assert!(handle_row_exists(&handle, "SELECT 1 FROM local_notes WHERE id = 'local-1'").await);
}

#[tokio::test]
async fn delete_survives_reopen_before_sync_cycle() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let handle = open_files_handle_in(dir.clone());
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-delete-reopen', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("insert before first cycle");
    let storage = publish_pending_through_a_test_store(&handle).await;

    let (_peer_tmp, peer) = open_files_handle();
    peer.pull_test_store(&storage).await;
    assert!(
        handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-delete-reopen'").await,
        "the peer receives the insert before the delete",
    );

    handle
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id = 'file-delete-reopen'", [])?;
            Ok(())
        })
        .await
        .expect("delete before reopen");
    drop(handle);

    let reopened = open_files_handle_in(dir);
    reopened
        .publish_test_store(&storage)
        .await
        .expect("publish pending Store write");

    peer.pull_test_store(&storage).await;
    assert!(
        !handle_row_exists(&peer, "SELECT 1 FROM files WHERE id = 'file-delete-reopen'").await,
        "the delete changeset reaches the peer after reopening",
    );
}

#[tokio::test]
async fn pending_write_drains_only_after_changeset_push() {
    let (_tmp, handle) = open_files_handle();
    let receipt = handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-retry-publish', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("write before failed push");
    let keypair = coven_keys::keys::UserKeypair::generate();
    let home = coven_replication::sync::test_helpers::test_cloud_home();
    let storage = merge_test_storage(&handle, &keypair, home.clone()).await;
    home.fail_exact_create_before_call(1);

    let first = handle.publish_test_store(&storage).await;
    assert!(
        first
            .as_ref()
            .is_err_and(|error| error.contains("forced failure before exact create")),
        "the first cycle must report the append failure while preserving its outbox: {first:?}",
    );
    assert_eq!(
        handle
            .write_status(&receipt.write_id)
            .await
            .expect("write status after failed append"),
        crate::WriteStatus::Publishing,
    );

    handle
        .publish_test_store(&storage)
        .await
        .expect("publish pending Store write");
    assert!(
        matches!(
            handle
                .write_status(&receipt.write_id)
                .await
                .expect("write status after retry"),
            crate::WriteStatus::Published(_)
        ),
        "the pending write is published as an immutable Store commit after retry",
    );
}

#[tokio::test]
async fn builder_open_runs_coven_and_host_migrations() {
    let (_tmp, handle) = open_files_handle();
    let has_coven_table = handle
        .coven_table_exists_for_test(coven_database::DatabaseTestTable::named("protocol_state"))
        .await
        .expect("query coven table");
    let has_host_table: i64 = handle
        .read(|sql| {
            sql.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'files'",
                [],
                |row| row.get(0),
            )
            .map_err(CovenError::from)
        })
        .await
        .expect("query host table");
    assert!(has_coven_table);
    assert_eq!(has_host_table, 1);
}

#[tokio::test]
async fn open_of_a_too_new_db_yields_the_matchable_migration_variant() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());

    // Open with a two-step ladder so the db lands at synced-schema version 2.
    let ahead = builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![
            files_migration(),
            Migration::sql(2, "add-extra", "CREATE TABLE extra (id TEXT PRIMARY KEY)"),
        ])
        .open()
        .expect("open at version 2");
    drop(ahead);

    // Reopen with only the first step: an older binary meeting a db a newer one
    // already migrated. The remedy is "update the app", so the host must be able
    // to match the specific variant rather than string-scrape a DbError.
    let reopened = builder(dir)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open();
    assert!(matches!(
        reopened,
        Err(CovenError::Migration(MigrationError::SchemaTooNew {
            current: 2,
            supported: 1
        }))
    ));
}

#[tokio::test]
async fn sql_reads_writes_and_stamps() {
    let (_tmp, handle) = open_files_handle();
    let id = "file-sql".to_string();
    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                params![id, sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("insert through sql");
    let count: i64 = handle
        .read(|sql| {
            sql.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                .map_err(CovenError::from)
        })
        .await
        .expect("count rows");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn sql_surfaces_sqlite_constraint_typed() {
    let (_tmp, handle) = open_files_handle();
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                params!["duplicate-id", sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("seed row");

    let result: CovenResult<WriteReceipt<()>> = handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) VALUES (?1, NULL, 0, ?2)",
                params!["duplicate-id", sql.stamp()],
            )?;
            Ok(())
        })
        .await;

    assert!(matches!(result, Err(CovenError::Sqlite(_))));
}

#[tokio::test]
async fn read_sees_a_committed_write() {
    let (_tmp, handle) = open_files_handle();
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('file-read-your-write', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("insert through sql");

    // The read runs on the separate read-only connection; it must observe the
    // just-committed write (WAL read-your-writes for committed data).
    let id: String = handle
        .read(|conn| {
            conn.query_row(
                "SELECT id FROM files WHERE id = 'file-read-your-write'",
                [],
                |row| row.get(0),
            )
            .map_err(CovenError::from)
        })
        .await
        .expect("read the committed row back through the read path");
    assert_eq!(id, "file-read-your-write");
}

#[tokio::test]
async fn open_on_a_fresh_store_serves_reads() {
    // A fresh (empty) directory: `open` runs the writer's migrations, then opens
    // the read connection against the schema they created. A read over the
    // host table then succeeds rather than failing on a missing table — proof the
    // read connection opened after, not before, the schema exists.
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let handle = builder(dir)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open on an empty directory");
    let count: i64 = handle
        .read(|conn| {
            conn.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                .map_err(CovenError::from)
        })
        .await
        .expect("read on a fresh store");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn open_removes_orphaned_local_blob_temps() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let final_path = dir
        .local_blob_path("media-files", "tempaaaa")
        .expect("local path");
    let mut staged = dir
        .stage_atomic_file(&final_path)
        .await
        .expect("allocate local blob stage");
    staged
        .write_bytes(b"interrupted write")
        .await
        .expect("write interrupted stage");
    let temp = staged.leave_unpublished_for_test();

    let _handle = builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open handle");

    assert!(
        !temp.exists(),
        "open removes orphaned local blob staging temps"
    );
    assert!(
        !dir.local_blob_path("media-files", "tempaaaa")
            .expect("local path")
            .exists(),
        "the interrupted blob has no committed final file"
    );
}

#[tokio::test]
async fn write_inserts_row_and_host_provided_blob() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let bytes = b"piece-bytes".to_vec();
    let hash = coven_protocol::blob::content_hash(&bytes);
    handle
        .write_with_blobs(
            {
                let bytes = bytes.clone();
                move |w| {
                    w.put_blob("media-files", "blobaaaa", bytes);
                    Ok(())
                }
            },
            move |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    params!["file-1", "blobaaaa", bytes.len() as i64, hash, sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("write row and blob");
    let path = dir
        .local_blob_path("media-files", "blobaaaa")
        .expect("local path");
    assert_eq!(
        std::fs::read(path).expect("read local blob"),
        b"piece-bytes"
    );
}

#[tokio::test]
async fn orphaned_final_blob_is_replaced_by_next_write() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let path = dir
        .local_blob_path("media-files", "orphaaaa")
        .expect("local path");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&path, b"orphaned bytes")
        .await
        .expect("write orphaned file");

    handle
        .write_with_blobs(
            |w| {
                w.put_blob("media-files", "orphaaaa", b"committed bytes".to_vec());
                Ok(())
            },
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        "file-orphan",
                        "orphaaaa",
                        15i64,
                        coven_protocol::blob::content_hash(b"committed bytes"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            },
        )
        .await
        .expect("write replaces orphaned final blob");

    assert_eq!(
        std::fs::read(path).expect("read committed blob"),
        b"committed bytes"
    );
}

#[tokio::test]
async fn put_blob_rejects_id_already_referenced_by_a_row() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    handle
        .write_with_blobs(
            |w| {
                w.put_blob("media-files", "dupeaaaa", b"original".to_vec());
                Ok(())
            },
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        "file-original",
                        "dupeaaaa",
                        8i64,
                        coven_protocol::blob::content_hash(b"original"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            },
        )
        .await
        .expect("seed original blob");

    let result: CovenResult<WriteReceipt<()>> = handle
        .write_with_blobs(
            |w| {
                w.put_blob("media-files", "dupeaaaa", b"replacement".to_vec());
                Ok(())
            },
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        "file-replacement",
                        "dupeaaaa",
                        11i64,
                        coven_protocol::blob::content_hash(b"replacement"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            },
        )
        .await;

    assert!(matches!(
        result,
        Err(CovenError::BlobAlreadyReferenced { .. })
    ));
    let path = dir
        .local_blob_path("media-files", "dupeaaaa")
        .expect("dupe path");
    assert_eq!(
        std::fs::read(path).expect("read original blob"),
        b"original"
    );
    let replacement_rows: i64 = handle
        .read(|sql| {
            sql.query_row(
                "SELECT count(*) FROM files WHERE id = 'file-replacement'",
                [],
                |row| row.get(0),
            )
            .map_err(CovenError::from)
        })
        .await
        .expect("count replacement rows");
    assert_eq!(replacement_rows, 0);
}

#[tokio::test]
async fn remote_root_host_provided_write_reads_staging_through_handle_before_upload() {
    let (_tmp, handle) = open_remote_root_files_handle();
    let expected = b"remote-root-host-provided-staging-bytes".to_vec();
    let bytes = expected.clone();
    let hash = coven_protocol::blob::content_hash(&bytes);

    handle
        .write_with_blobs(
            {
                let bytes = bytes.clone();
                move |w| {
                    w.put_blob("media-files", "rrhpaaaa", bytes);
                    Ok(())
                }
            },
            move |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        "file-remote-root",
                        "rrhpaaaa",
                        bytes.len() as i64,
                        hash,
                        sql.stamp()
                    ],
                )?;
                Ok(())
            },
        )
        .await
        .expect("write remote-root row and host-provided blob");
    let blob = handle
        .row_blob_ref("files", "file-remote-root")
        .await
        .expect("capture remote-root blob row");

    let whole = handle
        .read_blob(&blob)
        .await
        .expect("read_blob serves upload staging before sync upload");
    assert_eq!(
        whole, expected,
        "read_blob returns the bytes written through handle.write",
    );

    let (offset, len) = (12u64, 19u64);
    let stream = handle
        .open_blob_stream(&blob)
        .await
        .expect("open_blob_stream serves upload staging before sync upload");
    assert_eq!(stream.plaintext_size(), expected.len() as u64);
    let range = stream
        .read_at(offset, len)
        .await
        .expect("read a range from the opened stream");
    assert_eq!(
        range,
        &expected[offset as usize..(offset + len) as usize],
        "the stream returns the requested slice of the staged bytes",
    );
}

struct RemoteOnlyStoreBlob {
    _tmp: tempfile::TempDir,
    dir: StoreDir,
    handle: CovenHandle,
    store: std::sync::Arc<TestStore>,
    home: std::sync::Arc<crate::InMemoryCloudHome>,
    encryption: crate::EncryptionService,
    destination_circle: crate::CircleId,
    source_object: ObjectSlot,
}

impl RemoteOnlyStoreBlob {
    /// Moves the scoped `circle-file` row into the fixture's destination Circle.
    async fn move_circle_file_to_its_destination(
        &self,
    ) -> crate::CovenResult<crate::WriteReceipt<()>> {
        let destination_circle_value = self.destination_circle.to_string();
        self.handle
            .write(move |sql| {
                sql.execute(
                    "UPDATE files SET audience = ?1, _updated_at = ?2
                     WHERE id = 'circle-file'",
                    params![destination_circle_value, sql.stamp()],
                )?;
                Ok(())
            })
            .await
    }

    /// The audience the scoped `circle-file` row currently carries.
    async fn circle_file_audience(&self) -> crate::CovenResult<Option<String>> {
        self.handle
            .read(|conn| {
                conn.query_row(
                    "SELECT audience FROM files WHERE id = 'circle-file'",
                    [],
                    |row| row.get::<_, Option<String>>(0),
                )
                .map_err(CovenError::from)
            })
            .await
    }
    async fn create() -> Self {
        let tmp = tempfile::tempdir().expect("temp dir");
        let dir = StoreDir::new_ephemeral(tmp.path());
        let signer = coven_keys::keys::UserKeypair::generate();
        let encryption = crate::EncryptionService::from_key([42; 32]);
        let handle = builder(dir.clone())
            .synced_tables(vec![scoped_files_table()])
            .migrations(vec![scoped_files_migration()])
            .key_custody(crate::KeyCustody::InMemory(encryption.clone().into()))
            .identity_custody(crate::IdentityCustody::InMemory(signer.clone()))
            .open()
            .expect("open scoped blob store");
        let home = coven_replication::sync::test_helpers::test_cloud_home();
        let store = handle
            .create_test_store("lib-test", signer, home.clone())
            .await
            .expect("create exact test Store");
        let destination_circle = handle
            .install_test_active_circle("blob-circle")
            .await
            .expect("install Circle authority");

        let bytes = b"remote-only-circle-blob".to_vec();
        let hash = coven_protocol::blob::content_hash(&bytes);
        handle
            .write_with_blobs(
                {
                    let bytes = bytes.clone();
                    move |batch| {
                        batch.put_blob("media-files", "circleblob", bytes);
                        Ok(())
                    }
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO files
                             (id, blob_id, size, hash, audience, _updated_at)
                             VALUES ('circle-file', 'circleblob', ?1, ?2, ?3, ?4)",
                        params![
                            bytes.len() as i64,
                            hash,
                            Option::<String>::None,
                            sql.stamp()
                        ],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("write Store blob");
        handle
            .publish_test_store(&store)
            .await
            .expect("publish Store blob");
        let source_object = handle
            .row_blob_ref("files", "circle-file")
            .await
            .expect("capture published Store blob")
            .stored()
            .expect("published Store blob has exact storage")
            .object()
            .slot()
            .clone();
        std::fs::remove_file(
            dir.local_blob_path("media-files", "circleblob")
                .expect("local blob path"),
        )
        .expect("remove local plaintext to leave a remote-only source");

        Self {
            _tmp: tmp,
            dir,
            handle,
            store,
            home,
            encryption,
            destination_circle,
            source_object,
        }
    }
}

fn outbound_blob_spools(dir: &StoreDir) -> std::collections::BTreeSet<PathBuf> {
    let path = dir.storage_dir().join("outbound-blobs");
    match std::fs::read_dir(path) {
        Ok(entries) => entries
            .map(|entry| entry.expect("read outbound blob spool entry").path())
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::collections::BTreeSet::new()
        }
        Err(error) => panic!("read outbound blob spools: {error}"),
    }
}

#[tokio::test]
async fn audience_move_requires_remote_only_blob_before_committing_sql() {
    let fixture = RemoteOnlyStoreBlob::create().await;
    ExactSlotStorage::delete_at(fixture.home.as_ref(), &fixture.source_object)
        .await
        .expect("remove the only remote source");
    fixture
        .handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect the exact test Store");

    let result = fixture.move_circle_file_to_its_destination().await;

    assert!(
        result.is_err(),
        "a missing source blob must abort the audience move before SQLite commit",
    );
    let audience = fixture
        .circle_file_audience()
        .await
        .expect("read rolled-back audience");
    assert_eq!(audience, None);
}

#[tokio::test]
async fn blob_audience_move_without_staging_rejects_and_rolls_back_sql() {
    let fixture = RemoteOnlyStoreBlob::create().await;
    let destination_circle_value = fixture.destination_circle.to_string();
    let result = fixture
        .handle
        .execute_sql_with_blob_staging_for_test(
            None,
            format!(
                "UPDATE files SET audience = '{destination_circle_value}',
                 _updated_at = '0000000009000-0000-device-test'
                 WHERE id = 'circle-file'"
            ),
        )
        .await;

    let error = result.expect_err("a blob audience move cannot omit materialization");
    assert!(
        error
            .to_string()
            .contains("BlobMoveRequiresMaterialization"),
        "{error}",
    );
    let audience = fixture
        .circle_file_audience()
        .await
        .expect("read rolled-back audience");
    assert_eq!(audience, None);
}

#[tokio::test]
async fn missing_authorized_store_only_blocks_a_move_that_needs_it() {
    let fixture = RemoteOnlyStoreBlob::create().await;

    fixture
        .handle
        .execute_sql_with_blob_staging_for_test(
            None,
            "UPDATE files SET _updated_at = '0000000009000-0000-device-test'
                 WHERE id = 'circle-file'"
                .to_string(),
        )
        .await
        .expect("a write that does not move an audience needs no authorized Store");

    let destination_circle_value = fixture.destination_circle.to_string();
    let error = fixture
        .handle
        .execute_sql_with_blob_staging_for_test(
            None,
            format!(
                "UPDATE files SET audience = '{destination_circle_value}',
                     _updated_at = '0000000010000-0000-device-test'
                     WHERE id = 'circle-file'"
            ),
        )
        .await
        .expect_err("a remote-only move must surface its adapter error");
    assert!(
        error
            .to_string()
            .contains("audience move staging is unavailable"),
        "{error}",
    );
    let audience = fixture
        .circle_file_audience()
        .await
        .expect("read audience after adapter failure");
    assert_eq!(audience, None);
}

#[tokio::test]
async fn journal_failure_removes_only_the_audience_move_spool_and_rolls_back_sql() {
    let fixture = RemoteOnlyStoreBlob::create().await;
    fixture
        .handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect the exact test Store");
    let before = outbound_blob_spools(&fixture.dir);
    fixture
        .handle
        .install_store_write_failure_trigger_for_test()
        .await
        .expect("install Store write journal fault");

    let result = fixture.move_circle_file_to_its_destination().await;

    assert!(result.is_err(), "the injected journal failure must surface");
    assert_eq!(
        outbound_blob_spools(&fixture.dir),
        before,
        "rollback removes the destination spool created by this attempt",
    );
    let audience = fixture
        .circle_file_audience()
        .await
        .expect("read rolled-back audience");
    assert_eq!(audience, None);
}

#[tokio::test]
async fn local_audience_move_rolls_back_its_file_and_reuses_an_exact_leftover() {
    let fixture = RemoteOnlyStoreBlob::create().await;
    fixture
        .handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect the exact test Store");
    let destination = fixture
        .dir
        .local_blob_path("media-files", "circleblob")
        .expect("resolve Local destination");
    let sibling = destination
        .parent()
        .expect("Local destination has a parent")
        .join("unrelated");
    std::fs::create_dir_all(sibling.parent().expect("sibling has a parent"))
        .expect("create Local blob directory");
    std::fs::write(&sibling, b"unrelated").expect("write unrelated Local file");
    fixture
        .handle
        .install_store_write_failure_trigger_for_test()
        .await
        .expect("install Local Store write journal fault");

    let result = fixture
        .handle
        .write(|sql| {
            sql.execute(
                "UPDATE files SET audience = 'local', _updated_at = ?1
                     WHERE id = 'circle-file'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await;
    assert!(result.is_err(), "the injected journal failure must surface");
    assert!(
        !destination.exists(),
        "rollback removes the Local file created by this attempt",
    );
    assert_eq!(
        std::fs::read(&sibling).expect("read unrelated Local file"),
        b"unrelated",
    );
    let audience = fixture
        .circle_file_audience()
        .await
        .expect("read rolled-back Local audience");
    assert_eq!(audience, None);
    assert!(matches!(
        fixture
            .handle
            .row_blob_ref("files", "circle-file")
            .await
            .expect("load blob after failed Local move")
            .authority(),
        coven_protocol::blob::RowBlobAuthority::Remote(_)
    ));

    fixture
        .handle
        .remove_store_write_failure_trigger_for_test()
        .await
        .expect("remove Local Store write journal fault");
    std::fs::write(&destination, b"remote-only-circle-blob")
        .expect("model an exact file left by failed cleanup");
    fixture
        .handle
        .write(|sql| {
            sql.execute(
                "UPDATE files SET audience = 'local', _updated_at = ?1
                     WHERE id = 'circle-file'",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("retry accepts the exact already-materialized Local file");
    assert_eq!(
        std::fs::read(&destination).expect("read retried Local destination"),
        b"remote-only-circle-blob",
    );
    assert_eq!(
        std::fs::read(&sibling).expect("read unrelated file after retry"),
        b"unrelated",
    );
}

#[tokio::test]
async fn audience_move_publishes_from_precommit_spool_after_source_disappears() {
    let fixture = RemoteOnlyStoreBlob::create().await;
    fixture
        .handle
        .connect_sync_with_test_home(
            fixture.home.clone(),
            CloudCipher::Encrypted(fixture.encryption.clone()),
        )
        .await
        .expect("connect the exact test Store");

    // The fabricated destination Circle names its fixed owner in its roster;
    // that identity must be an active Store member or the Circle would be
    // rotation-required and reject new content.
    fixture
        .handle
        .invite_member(
            &coven_keys::keys::public_key_hex(
                &coven_protocol::circle_activation_test_fixtures::test_circle_owner_keypair(),
            ),
            None,
            crate::MemberRole::Member,
        )
        .await
        .expect("register the fabricated Circle roster owner as a Store member");

    let destination_circle_value = fixture.destination_circle.to_string();
    let receipt = fixture
        .handle
        .write(move |sql| {
            sql.execute(
                "UPDATE files SET audience = ?1, _updated_at = ?2
                     WHERE id = 'circle-file'",
                params![destination_circle_value, sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("commit audience move after staging its destination blob");
    let blob_facts = fixture
        .handle
        .write_blob_facts_for_test(receipt.write_id.clone())
        .await
        .expect("read durable move blob facts");
    let blob_facts: serde_json::Value =
        serde_json::from_str(&blob_facts).expect("decode move blob facts");
    let spool_path = blob_facts["blobs"][0]["audience_move"]["remote"]["spool_path"]
        .as_str()
        .expect("move fact records its exact destination spool");
    assert!(
        std::path::Path::new(spool_path).is_file(),
        "the destination spool is durable before the SQL write returns",
    );

    ExactSlotStorage::delete_at(fixture.home.as_ref(), &fixture.source_object)
        .await
        .expect("remove source after the move commits");
    fixture
        .handle
        .publish_test_store(&fixture.store)
        .await
        .expect("publish the move from its durable destination spool");
    assert!(
        !std::path::Path::new(spool_path).exists(),
        "prepared-object completion retires the durable precommit spool",
    );
    assert!(
        !fixture
            .handle
            .publish_test_store(&fixture.store)
            .await
            .expect("retry completed move publication"),
        "a completed move has nothing left to publish",
    );
    let published = fixture
        .handle
        .row_blob_ref("files", "circle-file")
        .await
        .expect("capture published destination Circle blob");
    assert_eq!(
        published.authority().audience(),
        crate::Audience::Circle(fixture.destination_circle),
    );
}

#[tokio::test]
async fn public_materialization_survives_store_reopen_without_a_cloud_connection() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                let tmp = tempfile::tempdir().expect("temp dir");
                let dir = StoreDir::new_ephemeral(tmp.path());
                let signer = coven_keys::keys::UserKeypair::generate();
                let keyring =
                    crate::MasterKeyring::from(crate::EncryptionService::from_key([42; 32]));
                let open = || {
                    builder(dir.clone())
                        .synced_tables(vec![remote_root_files_table()])
                        .migrations(vec![files_migration()])
                        .key_custody(crate::KeyCustody::InMemory(keyring.clone()))
                        .identity_custody(crate::IdentityCustody::InMemory(signer.clone()))
                        .open()
                        .expect("open remote-root store")
                };
                let handle = open();
                let home = coven_replication::sync::test_helpers::test_cloud_home();
                handle
                    .create_test_store("lib-test", signer.clone(), home.clone())
                    .await
                    .expect("create exact test Store");
                handle
                    .connect_sync_with_test_home(
                        home,
                        CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
                    )
                    .await
                    .expect("connect exact test Store");

                let expected = b"public materialized blob".to_vec();
                let bytes = expected.clone();
                let hash = coven_protocol::blob::content_hash(&bytes);
                let receipt = handle
                    .write_with_blobs(
                        {
                            let bytes = bytes.clone();
                            move |batch| {
                                batch.put_blob("media-files", "materialized-blob", bytes);
                                Ok(())
                            }
                        },
                        move |sql| {
                            sql.execute(
                                "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![
                                    "materialized-row",
                                    "materialized-blob",
                                    bytes.len() as i64,
                                    hash,
                                    sql.stamp()
                                ],
                            )?;
                            Ok(())
                        },
                    )
                    .await
                    .expect("write remote-root blob");
                let mut status = handle
                    .subscribe_write_status(&receipt.write_id)
                    .await
                    .expect("subscribe to materialized write");
                handle.sync_now();
                tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        let current = status.borrow().clone();
                        match current {
                            crate::WriteStatus::Published(_) => break,
                            crate::WriteStatus::Pending | crate::WriteStatus::Publishing => {
                                status.changed().await.expect("write status remains open")
                            }
                            other => panic!("materialized write did not publish: {other:?}"),
                        }
                    }
                })
                .await
                .expect("materialized write publishes");
                let reference = handle
                    .row_blob_ref("files", "materialized-row")
                    .await
                    .expect("capture published row blob");
                handle
                    .materialize_row_blob(&reference)
                    .await
                    .expect("materialize through the public handle");
                let locator_hash = reference
                    .stored()
                    .expect("published row has exact storage")
                    .locator()
                    .locator_hash();
                let cached = dir
                    .cache_blob_path("media-files", locator_hash)
                    .expect("exact cache path");
                assert_eq!(std::fs::read(&cached).expect("read exact cache"), expected);

                handle.disconnect_sync();
                drop(handle);
                let reopened = open();
                let reopened_reference = reopened
                    .row_blob_ref("files", "materialized-row")
                    .await
                    .expect("capture reopened row blob");
                reopened
                    .materialize_row_blob(&reopened_reference)
                    .await
                    .expect("reopen verifies the materialized cache without cloud storage");
            })
            .await
            .expect("public materialization task");
        })
        .await;
}

#[tokio::test]
async fn sql_failure_removes_staged_blob() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let err = handle
        .write_with_blobs(
            |w| {
                w.put_blob("media-files", "blobbbbb", b"staged".to_vec());
                Ok(())
            },
            |_sql| Err::<(), CovenError>(CovenError::Blob("sql failed".to_string())),
        )
        .await
        .expect_err("write fails");
    assert!(err.to_string().contains("sql failed"));
    let path = dir
        .local_blob_path("media-files", "blobbbbb")
        .expect("local path");
    assert!(!path.exists());
}

#[tokio::test]
async fn blob_stage_failure_does_not_run_sql() {
    let (_tmp, handle) = open_files_handle();
    let result: CovenResult<WriteReceipt<()>> = handle
        .write_with_blobs(
            |w| {
                w.put_blob("media-files", "..", b"bad".to_vec());
                Ok(())
            },
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('should-not-exist', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await;
    // A blob id that can't form a safe path is its own typed error, not the
    // generic blob catch-all.
    assert!(matches!(result, Err(CovenError::UnsafeBlobPath(_))));
    let count: i64 = handle
        .read(|sql| {
            sql.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                .map_err(CovenError::from)
        })
        .await
        .expect("count rows");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn replacement_deletes_old_blob_after_sql_drops_reference() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    coven_foundation::store_dir::StoreDir::store_local_blob(&dir, "media-files", "oldaaaa", b"old")
        .await
        .expect("store old");
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                params![
                    "file-1",
                    "oldaaaa",
                    coven_protocol::blob::content_hash(b"old"),
                    sql.stamp()
                ],
            )?;
            Ok(())
        })
        .await
        .expect("seed row");
    handle.publish_current_writes().await;
    coven_foundation::store_dir::StoreDir::store_local_blob(&dir, "media-files", "oldaaaa", b"old")
        .await
        .expect("restore published blob locally");
    let old_ref = BlobRef {
        namespace: "media-files".to_string(),
        id: "oldaaaa".to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheLazy,
    };
    handle
        .write_with_blobs(
            move |w| {
                w.put_blob("media-files", "newaaaa", b"new".to_vec());
                w.delete_blob(old_ref);
                Ok(())
            },
            move |sql| {
                sql.execute(
                    "UPDATE files SET blob_id = ?1, size = 3, hash = ?2, \
                         _updated_at = ?3 WHERE id = 'file-1'",
                    params![
                        "newaaaa",
                        coven_protocol::blob::content_hash(b"new"),
                        sql.stamp()
                    ],
                )?;
                Ok(())
            },
        )
        .await
        .expect("replace blob");
    assert!(!dir
        .local_blob_path("media-files", "oldaaaa")
        .expect("old path")
        .exists());
    assert!(dir
        .local_blob_path("media-files", "newaaaa")
        .expect("new path")
        .exists());
}

struct PendingReplacement {
    first_write: WriteId,
    second_write: WriteId,
    first_path: std::path::PathBuf,
}

impl PendingReplacement {
    async fn queue(handle: &CovenHandle, store_dir: &StoreDir) -> Self {
        let first = handle
            .write_with_blobs(
                |batch| {
                    batch.put_blob("media-files", "ownedaaa", b"first".to_vec());
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES ('owned-file', 'ownedaaa', 5, ?1, ?2)",
                        params![coven_protocol::blob::content_hash(b"first"), sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("queue first blob write");
        let first_path = store_dir
            .local_blob_path("media-files", "ownedaaa")
            .expect("first blob path");
        let first_blob = BlobRef {
            namespace: "media-files".to_string(),
            id: "ownedaaa".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            provenance: Provenance::HostProvided,
            fill: CacheFill::CacheLazy,
        };
        let second = handle
            .write_with_blobs(
                move |batch| {
                    batch.put_blob("media-files", "ownedbbb", b"second".to_vec());
                    batch.delete_blob(first_blob);
                    Ok(())
                },
                |sql| {
                    sql.execute(
                        "UPDATE files SET blob_id = 'ownedbbb', size = 6, hash = ?1, \
                         _updated_at = ?2 \
                         WHERE id = 'owned-file'",
                        params![coven_protocol::blob::content_hash(b"second"), sql.stamp()],
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("queue replacement write");
        Self {
            first_write: first.write_id,
            second_write: second.write_id,
            first_path,
        }
    }

    async fn assert_publishes_in_order(&self, handle: &CovenHandle) {
        let keypair = coven_keys::keys::UserKeypair::generate();
        let home = coven_replication::sync::test_helpers::test_cloud_home();
        let storage = merge_test_storage(handle, &keypair, home.clone()).await;
        handle
            .publish_test_store(&storage)
            .await
            .expect("publish pending Store write");

        let first_sequence = match handle
            .write_status(&self.first_write)
            .await
            .expect("first status")
        {
            crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
            status => panic!("first replacement write is not published: {status:?}"),
        };
        assert_eq!(
            handle
                .write_status(&self.second_write)
                .await
                .expect("second status after first publication"),
            crate::WriteStatus::Pending,
            "one Store publication consumes one pending host transaction",
        );

        handle
            .publish_test_store(&storage)
            .await
            .expect("publish pending Store write");
        let second_sequence = match handle
            .write_status(&self.second_write)
            .await
            .expect("second status")
        {
            crate::WriteStatus::Published(position) => position.commit().coord.sequence(),
            status => panic!("second replacement write is not published: {status:?}"),
        };
        assert_eq!(
            second_sequence,
            first_sequence + 1,
            "replacement writes publish in their host transaction order"
        );
        let blob_objects = home
            .keys()
            .into_iter()
            .filter(|key| key.starts_with("media-files/opaque/"))
            .count();
        assert_eq!(blob_objects, 2, "both row versions upload their blob bytes");
        assert!(
            !self.first_path.exists(),
            "the first write releases its local bytes after publication"
        );
    }
}

#[tokio::test]
async fn pending_write_owns_blob_bytes_until_its_publication() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                let (tmp, handle) = open_files_handle();
                let dir = StoreDir::new_ephemeral(tmp.path());
                let replacement = PendingReplacement::queue(&handle, &dir).await;

                assert_eq!(
                    std::fs::read(&replacement.first_path)
                        .expect("first write still owns its bytes"),
                    b"first"
                );
                let overwrite: CovenResult<WriteReceipt<()>> = handle
                    .write_with_blobs(
                        |batch| {
                            batch.put_blob("media-files", "ownedaaa", b"overwritten".to_vec());
                            Ok(())
                        },
                        |sql| {
                            sql.execute(
                                "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES ('overwrite', 'ownedaaa', 11, ?1, ?2)",
                                params![
                                    coven_protocol::blob::content_hash(b"overwritten"),
                                    sql.stamp()
                                ],
                            )?;
                            Ok(())
                        },
                    )
                    .await;
                assert!(matches!(
                    overwrite,
                    Err(CovenError::BlobOwnedByPendingWrite { .. })
                ));
                assert_eq!(
                    std::fs::read(&replacement.first_path).expect("lease prevents overwrite"),
                    b"first"
                );
                replacement.assert_publishes_in_order(&handle).await;
            })
            .await
            .expect("pending-write blob ownership test task");
        })
        .await;
}

#[tokio::test]
async fn pending_write_blob_ownership_survives_restart() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            tokio::task::spawn_local(async {
                let tmp = tempfile::tempdir().expect("temp dir");
                let dir = StoreDir::new_ephemeral(tmp.path());
                let handle = open_files_handle_in(dir.clone());
                let replacement = PendingReplacement::queue(&handle, &dir).await;
                drop(handle);

                let reopened = open_files_handle_in(dir);
                assert_eq!(
                    std::fs::read(&replacement.first_path)
                        .expect("reopened first write still owns its bytes"),
                    b"first"
                );
                replacement.assert_publishes_in_order(&reopened).await;
            })
            .await
            .expect("reopened pending-write blob ownership test task");
        })
        .await;
}

#[tokio::test]
async fn author_delete_drops_all_local_blob_copies() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    coven_foundation::store_dir::StoreDir::store_local_blob(&dir, "media-files", "oldcccc", b"old")
        .await
        .expect("store old");
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                params![
                    "file-1",
                    "oldcccc",
                    coven_protocol::blob::content_hash(b"old"),
                    sql.stamp()
                ],
            )?;
            Ok(())
        })
        .await
        .expect("seed row");
    handle.publish_current_writes().await;
    let published = handle
        .row_blob_ref("files", "file-1")
        .await
        .expect("capture exact published blob");
    let locator_hash = published
        .stored()
        .expect("published row has exact storage")
        .locator()
        .locator_hash();
    let pinned = dir
        .pinned_blob_path("media-files", locator_hash)
        .expect("pinned path");
    let cached = dir
        .cache_blob_path("media-files", locator_hash)
        .expect("cache path");
    coven_foundation::store_dir::StoreDir::store_local_blob(&dir, "media-files", "oldcccc", b"old")
        .await
        .expect("restore published blob locally");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&pinned, b"old")
        .await
        .expect("write pinned blob");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&cached, b"old")
        .await
        .expect("write cached blob");
    let old_ref = BlobRef {
        namespace: "media-files".to_string(),
        id: "oldcccc".to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheLazy,
    };

    handle
        .write_with_blobs(
            move |w| {
                w.delete_blob(old_ref);
                Ok(())
            },
            move |sql| {
                sql.execute(
                    "UPDATE files SET blob_id = NULL, size = 0, hash = NULL, _updated_at = ?1 \
                         WHERE id = 'file-1'",
                    [sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("delete blob");

    assert!(!dir
        .local_blob_path("media-files", "oldcccc")
        .expect("local path")
        .exists());
    assert!(!pinned.exists(), "pinned copy is removed");
    assert!(!cached.exists(), "cache copy is removed");
}

#[tokio::test]
async fn failed_local_blob_cleanup_keeps_intent_for_later_drain() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &dir,
        "media-files",
        "oldddddd",
        b"old",
    )
    .await
    .expect("store old");
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                params![
                    "file-1",
                    "oldddddd",
                    coven_protocol::blob::content_hash(b"old"),
                    sql.stamp()
                ],
            )?;
            Ok(())
        })
        .await
        .expect("seed row");
    handle.publish_current_writes().await;
    let published = handle
        .row_blob_ref("files", "file-1")
        .await
        .expect("capture exact published blob");
    let locator_hash = published
        .stored()
        .expect("published row has exact storage")
        .locator()
        .locator_hash();
    let pinned = dir
        .pinned_blob_path("media-files", locator_hash)
        .expect("pinned path");
    coven_foundation::store_dir::StoreDir::store_local_blob(
        &dir,
        "media-files",
        "oldddddd",
        b"old",
    )
    .await
    .expect("restore published blob locally");
    std::fs::create_dir_all(&pinned).expect("create pinned blocker");
    let old_ref = BlobRef {
        namespace: "media-files".to_string(),
        id: "oldddddd".to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheLazy,
    };

    handle
        .write_with_blobs(
            move |w| {
                w.delete_blob(old_ref);
                Ok(())
            },
            |sql| {
                sql.execute(
                    "UPDATE files SET blob_id = NULL, size = 0, hash = NULL, _updated_at = ?1 \
                         WHERE id = 'file-1'",
                    [sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("row delete commits despite cleanup failure");

    assert_eq!(
        handle
            .cleanup_intent_count_for_test("media-files", "oldddddd")
            .await
            .expect("count cleanup intents"),
        2
    );
    assert!(dir
        .local_blob_path("media-files", "oldddddd")
        .expect("local path")
        .exists());

    std::fs::remove_dir_all(&pinned).expect("remove pinned blocker");
    handle
        .write_with_blobs(
            |_| Ok(()),
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES (?1, NULL, 0, ?2)",
                    params!["drain-trigger", sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("later committed write drains pending cleanup");

    assert_eq!(
        handle
            .cleanup_intent_count_for_test("media-files", "oldddddd")
            .await
            .expect("count cleanup intents"),
        0
    );
    assert!(!dir
        .local_blob_path("media-files", "oldddddd")
        .expect("local path")
        .exists());
}

#[tokio::test]
async fn write_drain_separates_live_local_source_from_deleted_exact_cache() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let blob_id = "shared01";
    handle
        .write(move |sql| {
            let hash = coven_protocol::blob::content_hash(b"live");
            for id in ["remote-deletes", "still-live"] {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES (?1, 'shared01', 4, ?2, \
                                 '0000000001000-0000-dev-remote')",
                    params![id, &hash],
                )?;
            }
            Ok(())
        })
        .await
        .expect("seed two rows sharing the blob");

    let local = dir
        .local_blob_path("media-files", blob_id)
        .expect("local path");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&local, b"live")
        .await
        .expect("write local blob");

    let (_source_tmp, source) = open_files_handle();
    let storage = Arc::new(
        source
            .create_test_store(
                "lib-test",
                coven_keys::keys::UserKeypair::generate(),
                coven_replication::sync::test_helpers::test_cloud_home(),
            )
            .await
            .expect("create remote exact test Store"),
    );
    source
        .write_with_blobs(
            |batch| {
                batch.put_blob("media-files", "shared01", b"live".to_vec());
                Ok(())
            },
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                         VALUES ('remote-deletes', 'shared01', 4, ?1, ?2)",
                    params![coven_protocol::blob::content_hash(b"live"), sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("insert remote row");
    source
        .publish_test_store(storage.as_ref())
        .await
        .expect("publish remote insert");
    let remote_reference = source
        .row_blob_ref("files", "remote-deletes")
        .await
        .expect("capture exact remote blob");
    let locator_hash = remote_reference
        .stored()
        .expect("published row has exact storage")
        .locator()
        .locator_hash();
    let pinned = dir
        .pinned_blob_path("media-files", locator_hash)
        .expect("pinned path");
    let cached = dir
        .cache_blob_path("media-files", locator_hash)
        .expect("cache path");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&pinned, b"live")
        .await
        .expect("write pinned blob");
    coven_foundation::local_file::AtomicStagedFile::write_for_test(&cached, b"live")
        .await
        .expect("write cached blob");
    let delete = source
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id = 'remote-deletes'", [])?;
            Ok(())
        })
        .await
        .expect("delete remote row");
    source
        .publish_test_store(storage.as_ref())
        .await
        .expect("publish remote delete");

    assert!(matches!(
        source
            .write_status(&delete.write_id)
            .await
            .expect("remote delete status"),
        crate::WriteStatus::Published(_)
    ));
    let (device_id, sequence) = source
        .latest_materialized_commit_coordinate_for_test()
        .await
        .expect("load remote delete stream coordinate");

    let (commit_reached, resume_pull) =
        handle.arm_pull_after_remote_commit_for_test(device_id, sequence);
    let pull_handle = handle.clone();
    let pull_storage = storage.clone();
    let pull =
        tokio::spawn(async move { pull_handle.pull_test_store(pull_storage.as_ref()).await });

    commit_reached.notified().await;
    handle
        .write_with_blobs(
            |_| Ok(()),
            |sql| {
                sql.execute(
                    "INSERT INTO files (id, blob_id, size, _updated_at) \
                         VALUES ('drain-trigger', NULL, 0, ?1)",
                    [sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await
        .expect("write drains cleanup queue");
    resume_pull.notify_one();
    pull.await.expect("pull task");

    assert!(!handle_row_exists(&handle, "SELECT 1 FROM files WHERE id = 'remote-deletes'").await);
    assert!(handle_row_exists(&handle, "SELECT 1 FROM files WHERE id = 'still-live'").await);
    assert!(local.exists(), "the live blob's local copy survives");
    assert!(
        !pinned.exists(),
        "the deleted locator's pinned copy is removed"
    );
    assert!(
        !cached.exists(),
        "the deleted locator's cache copy is removed"
    );
    assert_eq!(
        handle
            .cleanup_intent_count_for_test("media-files", blob_id)
            .await
            .expect("count cleanup intents"),
        0,
    );
}

#[tokio::test]
async fn replacement_is_rejected_while_sql_still_references_old_blob() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    coven_foundation::store_dir::StoreDir::store_local_blob(&dir, "media-files", "oldbbbb", b"old")
        .await
        .expect("store old");
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                     VALUES (?1, ?2, 3, ?3, ?4)",
                params![
                    "file-1",
                    "oldbbbb",
                    coven_protocol::blob::content_hash(b"old"),
                    sql.stamp()
                ],
            )?;
            Ok(())
        })
        .await
        .expect("seed row");
    let old_ref = BlobRef {
        namespace: "media-files".to_string(),
        id: "oldbbbb".to_string(),
        scope: BlobScope::Master,
        cloud_path: None,
        provenance: Provenance::HostProvided,
        fill: CacheFill::CacheLazy,
    };
    let result: CovenResult<WriteReceipt<()>> = handle
        .write_with_blobs(
            move |w| {
                w.put_blob("media-files", "newbbbb", b"new".to_vec());
                w.delete_blob(old_ref);
                Ok(())
            },
            move |sql| {
                sql.execute(
                    "UPDATE files SET _updated_at = ?1 WHERE id = 'file-1'",
                    [sql.stamp()],
                )?;
                Ok(())
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(CovenError::BlobStillReferenced { .. })
    ));
    assert!(dir
        .local_blob_path("media-files", "oldbbbb")
        .expect("old path")
        .exists());
    assert!(!dir
        .local_blob_path("media-files", "newbbbb")
        .expect("new path")
        .exists());
}

#[tokio::test]
async fn sql_panic_removes_moved_blob() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let result: CovenResult<WriteReceipt<()>> = handle
        .write_with_blobs(
            |w| {
                w.put_blob("media-files", "panicccc", b"new".to_vec());
                Ok(())
            },
            |_sql| panic!("boom"),
        )
        .await;
    assert!(matches!(result, Err(CovenError::WriteClosurePanicked)));
    assert!(!dir
        .local_blob_path("media-files", "panicccc")
        .expect("panic path")
        .exists());
}

#[tokio::test]
async fn write_surfaces_a_failed_installed_blob_rollback() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let final_path = dir
        .local_blob_path("media-files", "rollback-failure")
        .expect("rollback failure path");
    let obstructed_path = final_path.clone();

    let result: CovenResult<WriteReceipt<()>> = handle
        .write_with_blobs(
            |write| {
                write.put_blob("media-files", "rollback-failure", b"new".to_vec());
                Ok(())
            },
            move |_sql| {
                std::fs::remove_file(&obstructed_path)
                    .expect("replace installed blob with rollback obstruction");
                std::fs::create_dir(&obstructed_path)
                    .expect("create rollback obstruction directory");
                Err(CovenError::Blob("force rollback".to_string()))
            },
        )
        .await;

    let error = result.expect_err("the write and its blob rollback both fail");
    assert!(error.to_string().contains("force rollback"), "{error}");
    assert!(
        error
            .to_string()
            .contains("failed to remove installed local blobs during rollback"),
        "{error}"
    );
    assert!(final_path.is_dir(), "rollback obstruction remains visible");
}

#[tokio::test]
async fn concurrent_duplicate_blob_write_does_not_delete_committed_blob() {
    let (tmp, handle) = open_files_handle();
    let dir = StoreDir::new_ephemeral(tmp.path());
    let winner = handle.clone();
    let loser = handle.clone();

    let write_winner = tokio::spawn(async move {
        winner
            .write_with_blobs(
                move |w| {
                    w.put_blob("media-files", "raceblob", b"committed".to_vec());
                    Ok(())
                },
                move |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            "winner",
                            "raceblob",
                            9i64,
                            coven_protocol::blob::content_hash(b"committed"),
                            sql.stamp()
                        ],
                    )?;
                    Ok(())
                },
            )
            .await
    });

    let write_loser = tokio::spawn(async move {
        loser
            .write_with_blobs(
                move |w| {
                    w.put_blob("media-files", "raceblob", b"rolled-back".to_vec());
                    Ok(())
                },
                |_sql| Err::<(), CovenError>(CovenError::Blob("force rollback".to_string())),
            )
            .await
    });

    let winner_result = write_winner.await.expect("winner task");
    let loser_result = write_loser.await.expect("loser task");
    assert!(winner_result.is_ok() || loser_result.is_ok());
    assert!(winner_result.is_err() || loser_result.is_err());

    let path = dir
        .local_blob_path("media-files", "raceblob")
        .expect("race path");
    assert_eq!(std::fs::read(path).expect("read race blob"), b"committed");
    let rows: i64 = handle
        .read(|sql| {
            sql.query_row(
                "SELECT count(*) FROM files WHERE id = 'winner'",
                [],
                |row| row.get(0),
            )
            .map_err(CovenError::from)
        })
        .await
        .expect("count winner row");
    assert_eq!(rows, 1);
}

/// A [`CloudHome`] that blocks the sync loop inside a cycle on demand. It
/// delegates every call to an inner [`InMemoryCloudHome`], but once `armed`,
/// the first cloud operation of a cycle signals `entered` and parks on
/// `release` until the test wakes it — holding the loop mid-cycle so the test
/// can observe the store-directory lock while a cycle is in flight.
///
/// Arming happens only after `connect_sync_with_test_home` returns, so the
/// connect-time bootstrap runs unblocked and only a loop cycle is gated.
struct GateCloudHome {
    inner: InMemoryCloudHome,
    armed: Arc<AtomicBool>,
    gated: AtomicBool,
    entered: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

impl GateCloudHome {
    async fn gate(&self) {
        if self.armed.load(Ordering::Acquire) && !self.gated.swap(true, Ordering::AcqRel) {
            self.entered
                .send(())
                .expect("test observes the loop entering a cycle");
            self.release.notified().await;
        }
    }
}

#[async_trait]
impl CloudHome for GateCloudHome {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        self.gate().await;
        self.inner.put_object(key, data).await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError> {
        self.gate().await;
        self.inner.open_multipart(key, total_len).await
    }

    fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold()
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.gate().await;
        self.inner.read(key).await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        self.gate().await;
        self.inner.read_range(key, start, end).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        self.gate().await;
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        self.gate().await;
        self.inner.delete(key).await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        self.gate().await;
        self.inner.exists(key).await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        self.gate().await;
        self.inner.set_access(desired).await
    }
}

#[async_trait]
impl ExactSlotStorage for GateCloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        self.gate().await;
        ExactSlotStorage::provider_binding(&self.inner).await
    }

    async fn allocate_slot(&self, key: &str) -> Result<ObjectSlot, CloudHomeError> {
        self.gate().await;
        ExactSlotStorage::allocate_slot(&self.inner, key).await
    }

    async fn create_at(
        &self,
        upload: &ExactUpload<'_>,
        progress: &UploadProgress<'_>,
    ) -> Result<ExactCreateOutcome, CloudHomeError> {
        self.gate().await;
        ExactSlotStorage::create_at(&self.inner, upload, progress).await
    }

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        self.gate().await;
        ExactSlotStorage::read_at(&self.inner, slot).await
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        self.gate().await;
        ExactSlotStorage::read_range_at(&self.inner, slot, start, end).await
    }

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), coven_storage::cloud::CloudFileReadError> {
        self.gate().await;
        ExactSlotStorage::read_at_to_file(&self.inner, slot, destination).await
    }

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        self.gate().await;
        ExactSlotStorage::delete_at(&self.inner, slot).await
    }
}

fn try_open(dir: &StoreDir) -> CovenResult<CovenHandle> {
    builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open()
}

/// Reopen `dir`, retrying while a still-running sync loop holds the lock.
/// Retries only the lock refusal; any other open error fails immediately.
/// Fails the calling test if the lock is not released within the budget.
async fn open_when_lock_released(dir: &StoreDir) -> CovenHandle {
    for _ in 0..100 {
        match try_open(dir) {
            Ok(handle) => return handle,
            Err(CovenError::AlreadyOpen { .. }) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(other) => panic!("reopen failed for a reason other than the lock: {other}"),
        }
    }
    panic!("store lock never released: reopen kept failing");
}

#[tokio::test]
async fn lock_is_held_until_the_sync_loop_exits_its_cycle() {
    test_keyring::install();

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let encryption = crate::EncryptionService::from_key([42; 32]);
    // A store id distinct from every other test's — `try_open`/
    // `open_when_lock_released` below reopen the same directory (the
    // lock is path-scoped, not store-id-scoped) but this test's own
    // identity establishment must not collide with a concurrently
    // running test that also establishes one under the shared default id.
    let handle = Coven::builder(
        dir.clone(),
        Config::with_defaults(
            "lock-held-until-cycle-exits".to_string(),
            "device-test".to_string(),
            "Test".to_string(),
        ),
    )
    .synced_tables(vec![files_table()])
    .migrations(vec![files_migration()])
    .key_custody(crate::KeyCustody::InMemory(encryption.clone().into()))
    .open()
    .expect("open handle");
    handle
        .initialize_identity()
        .expect("establish this store's identity before connecting");

    let armed = Arc::new(AtomicBool::new(false));
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let home = Arc::new(GateCloudHome {
        inner: InMemoryCloudHome::new(),
        armed: armed.clone(),
        gated: AtomicBool::new(false),
        entered: entered_tx,
        release: release.clone(),
    });
    let connect_handle = handle.clone();
    tokio::spawn(async move {
        connect_handle
            .connect_sync_with_test_home(home, CloudCipher::Encrypted(encryption))
            .await
    })
    .await
    .expect("gating test connection task completes")
    .expect("connect over the gating test home");

    // Bootstrap is done; gate the loop's next cycle and wait until it parks
    // mid-cycle inside the cloud home.
    armed.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(30), entered_rx.recv())
        .await
        .expect("sync loop reached a cycle within the timeout")
        .expect("gating home signalled the cycle entry");

    // Drop the last handle while the loop is still mid-cycle. The loop owns a
    // guard clone, so the lock must stay held: a concurrent open is refused.
    drop(handle);
    assert!(
        matches!(
            try_open(&dir),
            Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
        ),
        "a concurrent open must be refused while the sync loop is mid-cycle",
    );

    // Release the cycle; the loop finishes, sees its channels closed, exits,
    // and drops the last guard clone — the lock frees and a reopen succeeds.
    release.notify_one();
    let _reopened = open_when_lock_released(&dir).await;
}

#[tokio::test]
async fn normal_shutdown_releases_the_lock_for_reopen() {
    test_keyring::install();

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let encryption = crate::EncryptionService::from_key([42; 32]);
    // A store id distinct from every other test's — see the identical
    // note in `lock_is_held_until_the_sync_loop_exits_its_cycle`.
    let handle = Coven::builder(
        dir.clone(),
        Config::with_defaults(
            "normal-shutdown-releases-lock".to_string(),
            "device-test".to_string(),
            "Test".to_string(),
        ),
    )
    .synced_tables(vec![files_table()])
    .migrations(vec![files_migration()])
    .key_custody(crate::KeyCustody::InMemory(encryption.clone().into()))
    .open()
    .expect("open handle");
    handle
        .initialize_identity()
        .expect("establish this store's identity before connecting");
    let connect_handle = handle.clone();
    tokio::spawn(async move {
        connect_handle
            .connect_sync_with_test_home(
                Arc::new(InMemoryCloudHome::new()),
                CloudCipher::Encrypted(encryption),
            )
            .await
    })
    .await
    .expect("shutdown test connection task completes")
    .expect("connect over the test home");

    drop(handle);

    let _reopened = open_when_lock_released(&dir).await;
}

// ========================================================================
// Read-only opens (CovenReadHandle)
// ========================================================================

fn try_open_read_only(dir: &StoreDir) -> CovenResult<crate::read_handle::CovenReadHandle> {
    builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open_read_only()
}

async fn read_files_count(handle: &crate::read_handle::CovenReadHandle) -> i64 {
    handle
        .read(|conn| {
            conn.query_row("SELECT count(*) FROM files", [], |row| row.get(0))
                .map_err(CovenError::from)
        })
        .await
        .expect("count files through the read handle")
}

/// The requirement's failing baseline made to pass: a second FULL open is still
/// refused while the first holds the store, but a read-only open succeeds
/// against the same held store — it takes no writer lock.
#[tokio::test]
async fn read_only_open_succeeds_while_a_full_open_holds_the_store() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let writer = open_files_handle_in(dir.clone());

    // A second full open is refused (the invariant the lock protects).
    assert!(matches!(
        try_open(&dir),
        Err(CovenError::AlreadyOpen { store_dir }) if store_dir == tmp.path()
    ));

    // A read-only open succeeds against the very same held store.
    let reader = try_open_read_only(&dir).expect("read-only open succeeds under a full open");
    assert_eq!(read_files_count(&reader).await, 0);

    drop(writer);
}

/// Multiple read-only opens coexist with each other and with the writer.
#[tokio::test]
async fn multiple_read_only_opens_coexist_with_a_writer() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let writer = open_files_handle_in(dir.clone());

    let reader_a = try_open_read_only(&dir).expect("first read-only open");
    let reader_b = try_open_read_only(&dir).expect("second read-only open");

    assert_eq!(read_files_count(&reader_a).await, 0);
    assert_eq!(read_files_count(&reader_b).await, 0);
    drop(writer);
}

/// A read-only handle sees rows the writer committed — both before the reader
/// opened and after (WAL cross-connection visibility on the one db file).
#[tokio::test]
async fn read_only_open_sees_committed_writer_data() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let writer = open_files_handle_in(dir.clone());

    writer
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('row-before-reader', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("writer inserts before the reader opens");

    let reader = try_open_read_only(&dir).expect("read-only open");
    assert_eq!(
        read_files_count(&reader).await,
        1,
        "the reader sees the row committed before it opened",
    );

    // A commit after the reader is open is visible on its next read: coven runs
    // each read as its own transaction, so it never pins an old WAL snapshot.
    writer
        .write(|sql| {
            sql.execute(
                "INSERT INTO files (id, blob_id, size, _updated_at) \
                     VALUES ('row-after-reader', NULL, 0, ?1)",
                [sql.stamp()],
            )?;
            Ok(())
        })
        .await
        .expect("writer inserts after the reader opened");
    assert_eq!(
        read_files_count(&reader).await,
        2,
        "the reader sees a row the writer committed after the reader opened",
    );
    drop(writer);
}

/// A read-only open runs no migration ladder, but refuses a db a newer binary
/// migrated past what this binary knows — the writer's `SchemaTooNew` policy.
#[tokio::test]
async fn read_only_open_refuses_a_too_new_schema() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());

    // A newer binary migrates the db to synced-schema version 2.
    let ahead = builder(dir.clone())
        .synced_tables(vec![files_table()])
        .migrations(vec![
            files_migration(),
            Migration::sql(2, "add-extra", "CREATE TABLE extra (id TEXT PRIMARY KEY)"),
        ])
        .open()
        .expect("open at version 2");
    drop(ahead);

    // An older binary opens the same db read-only with only the version-1 ladder:
    // it cannot understand the schema, so it refuses with the matchable variant.
    let reopened = builder(dir)
        .synced_tables(vec![files_table()])
        .migrations(vec![files_migration()])
        .open_read_only();
    assert!(matches!(
        reopened,
        Err(CovenError::Migration(MigrationError::SchemaTooNew {
            current: 2,
            supported: 1
        }))
    ));
}

/// End-to-end blob read through the read handle: the writer stores a
/// host-provided blob on a remote-root table (its bytes land in the local store
/// as upload staging); the read-only handle resolves the blob's locality and
/// serves those bytes — no cloud, no writer lock.
#[tokio::test]
async fn read_only_handle_reads_a_host_provided_blob() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let writer = builder(dir.clone())
        .synced_tables(vec![remote_root_files_table()])
        .migrations(vec![files_migration()])
        .open()
        .expect("open writer");

    let bytes = b"read-only-handle-serves-these-blob-bytes".to_vec();
    let hash = coven_protocol::blob::content_hash(&bytes);
    writer
        .write_with_blobs(
            {
                let bytes = bytes.clone();
                move |w| {
                    w.put_blob("media-files", "roblob01", bytes);
                    Ok(())
                }
            },
            {
                let len = bytes.len() as i64;
                move |sql| {
                    sql.execute(
                        "INSERT INTO files (id, blob_id, size, hash, _updated_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                        params!["file-ro", "roblob01", len, hash, sql.stamp()],
                    )?;
                    Ok(())
                }
            },
        )
        .await
        .expect("writer stores the row and blob");

    let reader = builder(dir)
        .synced_tables(vec![remote_root_files_table()])
        .migrations(vec![files_migration()])
        .open_read_only()
        .expect("read-only open");

    let blob = reader
        .row_blob_ref("files", "file-ro")
        .await
        .expect("capture read-only blob row");
    let read = reader
        .read_blob(&blob)
        .await
        .expect("read blob via read handle");
    assert_eq!(
        read, bytes,
        "the read handle serves the blob the writer stored"
    );

    // A ranged read through the same handle serves the requested slice.
    let (offset, len) = (5u64, 10u64);
    let range = reader
        .open_blob_stream(&blob)
        .await
        .expect("open a stream via read handle")
        .read_at(offset, len)
        .await
        .expect("ranged read via read handle");
    assert_eq!(range, &bytes[offset as usize..(offset + len) as usize]);
    drop(writer);
}

/// Concurrent same-path cache populate by two producers (the writer's and the
/// reader's fetch both writing the same blob into `cache/`) is safe: the atomic
/// temp-then-rename write means the destination is always one producer's whole
/// file, never a torn interleave. This is the primitive requirement 6 rests on.
#[tokio::test]
async fn concurrent_same_blob_cache_writes_never_tear() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = StoreDir::new_ephemeral(tmp.path());
    let dest = dir
        .cache_blob_path(
            "media-files",
            coven_protocol::store_commit::ObjectHash::digest(b"raceblob"),
        )
        .expect("cache path");
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).expect("create cache shard dir");
    }

    // Two full-length payloads that differ everywhere, so any interleave would be
    // detectable as a mixed file. In production both producers write the same
    // blob's bytes; the distinct payloads only make tearing observable.
    let a = vec![b'A'; 64 * 1024];
    let b = vec![b'B'; 64 * 1024];
    let (ra, rb) = tokio::join!(
        coven_foundation::local_file::AtomicStagedFile::write_for_test(&dest, &a),
        coven_foundation::local_file::AtomicStagedFile::write_for_test(&dest, &b),
    );
    ra.expect("first concurrent cache write");
    rb.expect("second concurrent cache write");

    let final_bytes = tokio::fs::read(&dest).await.expect("read cache file");
    assert!(
        final_bytes == a || final_bytes == b,
        "the cache file is exactly one producer's whole payload, never a torn mix",
    );
}
