use crate::sync::test_helpers::{
    test_cloud_home, test_migrations, test_store_dir, test_synced_tables, TestDevice, TestStore,
};
use coven_database::{Database, StoreDatabase};
use coven_keys::keys::UserKeypair;
use std::sync::Arc;

#[path = "write_rebase_blob_source_tests.rs"]
mod blob_sources;
#[path = "write_rebase_blob_tests.rs"]
mod blobs;
#[path = "write_rebase_circle_tests.rs"]
mod circles;
#[path = "write_rebase_completion_tests.rs"]
mod completion;
#[path = "write_rebase_constraint_tests.rs"]
mod constraints;
#[path = "write_rebase_covered_circle_tests.rs"]
mod covered_circles;
#[path = "write_rebase_private_tests.rs"]
mod private;

struct RebaseFixture {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
    source_dir: coven_foundation::store_dir::StoreDir,
    source: Database,
    target: Database,
    store: Arc<TestStore>,
    storage: Arc<coven_storage::CloudSyncConnection>,
    home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    owner: TestDevice,
    peer: TestDevice,
    signer: UserKeypair,
    routing: coven_keys::encryption::EncryptionService,
}

impl RebaseFixture {
    async fn new() -> Self {
        Self::with_tables(test_synced_tables()).await
    }

    async fn with_tables(tables: Vec<coven_protocol::synced_schema::SyncedTable>) -> Self {
        Self::with_schema(tables, test_migrations()).await
    }

    async fn with_schema(
        tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        migrations: Vec<coven_database::Migration>,
    ) -> Self {
        let directory = tempfile::tempdir().expect("database directory");
        let path = directory.path().join("source.db");
        let source_dir = test_store_dir();
        let source = Self::open_schema(&path, source_dir.clone(), tables.clone(), &migrations);
        let target_dir = test_store_dir();
        let target = coven_database::synthetic_store::open_test_db_schema(
            target_dir.clone(),
            tables,
            migrations,
        );
        let signer = UserKeypair::generate();
        let home = test_cloud_home();
        let (store, storage) = TestStore::create_with_connection(
            &source,
            source_dir.clone(),
            "reserved-write-rebase",
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create Store");
        let peer = store
            .activate_joined_device(
                &source,
                source_dir.clone(),
                &target,
                target_dir,
                &signer,
                "2026-01-01T00:00:00Z",
            )
            .await
            .expect("activate same-principal peer");
        let owner = store
            .bind_device_in(&source, source_dir.clone(), &signer)
            .await
            .expect("bind owner");
        owner.pull_store().await.expect("observe device activation");
        let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        Self::capture_host_edit(&source, &routing,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) VALUES \
             ('shared', 'Original title', 'Original body', 1, '0000000001000-0000-owner', '2026-01-01')",
        ).await;
        Self::publish(&owner).await;
        peer.pull_store().await.expect("peer installs original row");
        Self {
            _directory: directory,
            path,
            source_dir,
            source,
            target,
            store,
            storage,
            home,
            owner,
            peer,
            signer,
            routing,
        }
    }

    async fn capture_host_edit(
        database: &Database,
        routing: &coven_keys::encryption::EncryptionService,
        sql: &str,
    ) {
        let sql = sql.to_string();
        StoreDatabase::new(database)
            .run_host_store_write_for_test(Some(routing.clone()), None, move |transaction| {
                transaction
                    .execute_batch(&sql)
                    .map_err(coven_database::DbError::from)
            })
            .await
            .expect("capture host edit with the Store routing key");
    }

    fn open(path: &std::path::Path, store_dir: coven_foundation::store_dir::StoreDir) -> Database {
        Self::open_with_tables(path, store_dir, test_synced_tables())
    }

    fn open_with_tables(
        path: &std::path::Path,
        store_dir: coven_foundation::store_dir::StoreDir,
        tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    ) -> Database {
        Self::open_schema(path, store_dir, tables, &test_migrations())
    }

    fn open_schema(
        path: &std::path::Path,
        store_dir: coven_foundation::store_dir::StoreDir,
        tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        migrations: &[coven_database::Migration],
    ) -> Database {
        Database::open_synthetic_for_test(
            path,
            store_dir,
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "rebase-source".to_string(),
            Arc::new(coven_foundation::clock::SystemClock),
            migrations,
        )
        .expect("open file-backed source")
    }

    async fn publish(device: &TestDevice) {
        let mut writer = device.authorize_writer().await.expect("authorize writer");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare write"));
        assert_eq!(writer.drain_store_writes().await.expect("publish write"), 1);
    }

    async fn prepare_local(&self) -> coven_database::ActiveStorePublication {
        self.source.execute_test_host_write(
            "UPDATE notes SET title = 'Recorded local title', _updated_at = '0000000002000-0000-owner' WHERE id = 'shared'; \
             INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('private', 'Private local effect', 0, '0000000002000-0000-owner', '2026-01-01')",
        ).await;
        let mut writer = self
            .owner
            .authorize_writer()
            .await
            .expect("authorize local write");
        assert!(writer
            .prepare_pending_store_write()
            .await
            .expect("prepare original candidate"));
        StoreDatabase::new(&self.source)
            .active_store_publication()
            .await
            .expect("read reserved candidate")
            .expect("local write owns author turn")
    }

    async fn snapshot_peer_edit(&self, conflicting: bool) {
        let sql = if conflicting {
            "UPDATE notes SET title = 'Peer changed title', _updated_at = '0000000003000-0000-peer' WHERE id = 'shared'"
        } else {
            "UPDATE notes SET body = 'Peer changed body', _updated_at = '0000000003000-0000-peer' WHERE id = 'shared'"
        };
        Self::capture_host_edit(&self.target, &self.routing, sql).await;
        Self::publish(&self.peer).await;
        self.peer
            .publish_snapshot_generation_for_test()
            .await
            .expect("peer publishes snapshot");
    }

    async fn assert_rows(&self, database: &Database) {
        assert_eq!(
            database
                .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
                .await,
            "Recorded local title"
        );
        assert_eq!(
            database
                .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
                .await,
            "Peer changed body"
        );
        assert_eq!(
            database
                .query_test_text("SELECT title FROM notes WHERE id = 'private'")
                .await,
            "Private local effect"
        );
    }
}

#[tokio::test]
async fn snapshot_rebase_preserves_recorded_columns_and_reserved_write() {
    exercise_reserved_rebase(false, false).await;
}

#[tokio::test]
async fn snapshot_rebase_awaiting_preparation_survives_database_reopen() {
    exercise_reserved_rebase(true, false).await;
}

#[tokio::test]
async fn snapshot_rebase_can_cross_another_snapshot_before_preparation() {
    exercise_reserved_rebase(true, true).await;
}

async fn exercise_reserved_rebase(reopen: bool, second_snapshot: bool) {
    let fixture = RebaseFixture::new().await;
    let original = fixture.prepare_local().await;
    let (write_id, registration, coord) = original.commit_reservation().expect("reserved commit");
    let original_capture = StoreDatabase::new(&fixture.source)
        .store_write_capture_for_test(write_id.clone())
        .await
        .expect("read original captured edit");
    fixture.snapshot_peer_edit(false).await;
    let (_, pulled) = fixture
        .owner
        .pull_store()
        .await
        .expect("install snapshot and rebase atomically");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let database = StoreDatabase::new(&fixture.source);
    let awaiting = database
        .active_store_publication()
        .await
        .expect("read replacement phase")
        .expect("reserved author turn survives snapshot");
    assert!(awaiting.is_awaiting_preparation());
    assert_eq!(
        awaiting.commit_reservation(),
        Some((write_id, registration, coord))
    );
    assert!(database
        .oldest_prepared_store_write()
        .await
        .expect("read signed candidate")
        .is_none());
    assert_eq!(
        database
            .store_write_capture_for_test(write_id.clone())
            .await
            .expect("read retained captured edit"),
        original_capture,
        "recorded observations and edit remain immutable",
    );
    fixture.assert_rows(&fixture.source).await;
    if second_snapshot {
        fixture
            .peer
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish second snapshot");
        let (_, pulled) = fixture
            .owner
            .pull_store()
            .await
            .expect("rebase awaiting work again");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        fixture.assert_rows(&fixture.source).await;
    }
    let resumed_source = if reopen {
        RebaseFixture::open(&fixture.path, fixture.source_dir.clone())
    } else {
        fixture.source.clone()
    };
    let resumed_owner = fixture
        .store
        .bind_device_in(&resumed_source, fixture.source_dir.clone(), &fixture.signer)
        .await
        .expect("bind reserved writer from durable state");
    let mut writer = resumed_owner
        .authorize_writer()
        .await
        .expect("authorize resumed writer");
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("prepare and publish rebased write"),
        1
    );
    drop(writer);
    fixture.assert_rows(&resumed_source).await;
    let resumed_database = StoreDatabase::new(&resumed_source);
    assert!(resumed_database
        .active_store_publication()
        .await
        .expect("completed reservation")
        .is_none());
    let status = resumed_database
        .write_status(write_id)
        .await
        .expect("same logical receipt");
    let coven_protocol::write::WriteStatus::Published(receipt) = status else {
        panic!("write remains unfinished: {status:?}");
    };
    assert_eq!(
        &receipt
            .exact_commit()
            .expect("replacement has an exact commit receipt")
            .coord,
        coord
    );
    assert_ne!(
        coven_protocol::store_commit::StorePublicationPayload::Commit(
            receipt
                .exact_commit()
                .expect("replacement has an exact commit receipt")
                .clone()
        ),
        original.attempt().expect("original attempt").entry.payload,
    );
    let (_, pulled) = fixture
        .peer
        .pull_store()
        .await
        .expect("peer installs replacement");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(
        fixture
            .target
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        "Recorded local title"
    );
    assert!(
        !fixture
            .target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'private'")
            .await
    );
}

#[tokio::test]
async fn snapshot_rebase_conflict_preserves_the_entire_previous_state() {
    let fixture = RebaseFixture::new().await;
    let original = fixture.prepare_local().await;
    let database = StoreDatabase::new(&fixture.source);
    let before = database
        .retained_store_publication()
        .await
        .expect("original accepted boundary");
    let baseline = database
        .installed_replay_baseline()
        .await
        .expect("original baseline");
    fixture.snapshot_peer_edit(true).await;
    let error = fixture
        .owner
        .pull_store()
        .await
        .expect_err("conflicting recorded column stops adoption");
    assert!(
        format!("{error:?}").contains("WriteRebaseConflict"),
        "{error:?}"
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("preserved reservation"),
        Some(original)
    );
    let after = database
        .retained_store_publication()
        .await
        .expect("preserved observed history");
    assert_eq!(after.0, before.0);
    let entries = |values: &[coven_database::ExactProtocolObject<
        coven_protocol::store_commit::StorePublicationEntry,
    >]| {
        values
            .iter()
            .map(|entry| (entry.prepared.reference().clone(), entry.value.to_bytes()))
            .collect::<Vec<_>>()
    };
    assert_eq!(entries(&after.1), entries(&before.1));
    let unchanged = database
        .installed_replay_baseline()
        .await
        .expect("preserved baseline");
    assert_eq!(unchanged.coverage(), baseline.coverage());
    assert_eq!(
        unchanged.snapshot().map(|snapshot| &snapshot.reference),
        baseline.snapshot().map(|snapshot| &snapshot.reference)
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        "Recorded local title"
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
            .await,
        "Original body"
    );
}

#[tokio::test]
async fn snapshot_rebase_parent_delete_cannot_erase_an_unobserved_peer_child() {
    let fixture = RebaseFixture::new().await;
    fixture
        .source
        .execute_test_host_write("DELETE FROM notes WHERE id = 'shared'")
        .await;
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("authorize recorded deletion");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare recorded deletion"));
    drop(writer);
    let database = StoreDatabase::new(&fixture.source);
    let original = database
        .active_store_publication()
        .await
        .expect("read reserved deletion");
    let boundary = database
        .store_current_publication()
        .await
        .expect("original boundary");
    fixture
        .target
        .execute_test_host_write(
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) VALUES \
         ('peer-child', 'shared', 'Peer child', '0000000003000-0000-peer', '2026-01-01')",
        )
        .await;
    RebaseFixture::publish(&fixture.peer).await;
    fixture
        .peer
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish parent and child snapshot");
    let error = fixture
        .owner
        .pull_store()
        .await
        .expect_err("recorded parent delete cannot consume peer child");
    let described = format!("{error:?}");
    assert!(described.contains("WriteRebaseConflict"), "{described}");
    assert!(
        described.contains("primary_key: \"peer-child\""),
        "{described}"
    );
    assert!(described.contains("table: \"note_tags\""), "{described}");
    assert!(
        described.contains("without a parent in notes"),
        "{described}"
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged observed boundary"),
        boundary
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("unchanged reserved deletion"),
        original
    );
    assert!(
        !fixture
            .source
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'shared'")
            .await
    );
    assert!(
        fixture
            .target
            .test_row_exists("SELECT 1 FROM note_tags WHERE id = 'peer-child'")
            .await
    );
}

#[tokio::test]
async fn snapshot_rebase_cleanup_failure_keeps_the_replacement_reserved() {
    let fixture = RebaseFixture::new().await;
    let original = fixture.prepare_local().await;
    let (write_id, registration, coord) =
        original.commit_reservation().expect("reserved candidate");
    let database = StoreDatabase::new(&fixture.source);
    let pending = database
        .oldest_prepared_store_write()
        .await
        .expect("read candidate")
        .expect("prepared candidate");
    let old_commit = pending.commit.value.reference().clone();
    let (uploaded, _resume) = fixture.source.arm_test_pause(
        coven_database::DatabaseTestPoint::StoreWriteCommitUploaded {
            write_id: write_id.clone(),
        },
    );
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("authorize upload");
    {
        let upload = writer.drain_store_writes();
        tokio::pin!(upload);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = uploaded.notified() => {},
                result = &mut upload => panic!("upload returned before interruption: {result:?}"),
            }
        })
        .await
        .expect("upload original package and commit before acceptance");
    }
    drop(writer);
    fixture.snapshot_peer_edit(false).await;
    fixture
        .owner
        .pull_store()
        .await
        .expect("rebase uploaded candidate");
    let boundary = database
        .store_current_publication()
        .await
        .expect("snapshot boundary");
    fixture
        .store
        .fail_nth_exact_delete_of(&[old_commit.object.slot()], 1);
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume replacement preparation");
    let error = writer
        .drain_store_writes()
        .await
        .expect_err("old exact cleanup failure stops publication");
    assert!(
        error.to_string().contains("forced exact delete failure"),
        "{error}"
    );
    drop(writer);
    let retained = database
        .active_store_publication()
        .await
        .expect("read retained cleanup")
        .expect("same reservation remains");
    assert!(
        !retained.is_awaiting_preparation(),
        "replacement prepared before retiring reusable objects"
    );
    assert_eq!(
        retained.commit_reservation(),
        Some((write_id, registration, coord))
    );
    assert_eq!(retained.retired_candidates().len(), 1);
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("cleanup cannot publish"),
        boundary
    );
    fixture.assert_rows(&fixture.source).await;
    let reopened = RebaseFixture::open(&fixture.path, fixture.source_dir.clone());
    let resumed = fixture
        .store
        .bind_device_in(&reopened, fixture.source_dir.clone(), &fixture.signer)
        .await
        .expect("reopen cleanup owner");
    let mut writer = resumed.authorize_writer().await.expect("retry cleanup");
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("finish exact cleanup and publication"),
        1
    );
    assert!(StoreDatabase::new(&reopened)
        .active_store_publication()
        .await
        .expect("completed reservation")
        .is_none());
    assert!(
        !reopened
            .remote_object_exists_for_test(old_commit.object.clone())
            .await
            .expect("read retired candidate ownership"),
        "obsolete candidate bytes have no lifetime owner after verified deletion"
    );
    fixture.assert_rows(&reopened).await;
}

#[tokio::test]
async fn snapshot_rebase_does_not_replace_a_write_accepted_before_the_snapshot() {
    let fixture = RebaseFixture::new().await;
    let original = fixture.prepare_local().await;
    let (write_id, _, coord) = original.commit_reservation().expect("reserved write");
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume writer");
    assert_eq!(
        writer.drain_store_writes().await.expect("publish original"),
        1
    );
    drop(writer);
    let database = StoreDatabase::new(&fixture.source);
    let receipt = database
        .write_status(write_id)
        .await
        .expect("accepted write");
    let coven_protocol::write::WriteStatus::Published(position) = &receipt else {
        panic!("expected published write: {receipt:?}");
    };
    assert_eq!(
        &position
            .exact_commit()
            .expect("accepted write retains its exact commit")
            .coord,
        coord
    );
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer installs original acceptance");
    fixture.snapshot_peer_edit(false).await;
    let (_, pulled) = fixture
        .owner
        .pull_store()
        .await
        .expect("install covering snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_eq!(
        database
            .write_status(write_id)
            .await
            .expect("preserved receipt"),
        receipt
    );
    assert!(database
        .active_store_publication()
        .await
        .expect("released reservation")
        .is_none());
    assert!(!database
        .has_rebased_store_writes_for_test()
        .await
        .expect("read replacement inputs"));
    fixture.assert_rows(&fixture.source).await;
}

#[tokio::test]
async fn snapshot_rebase_blocks_the_conflicting_suffix_write_during_another_publication() {
    let fixture = RebaseFixture::new().await;
    let prepared = fixture.prepare_local().await;
    let (first_id, _, _) = prepared.commit_reservation().expect("first reservation");
    fixture.source.execute_test_host_write(
        "UPDATE notes SET body = 'Recorded second body', _updated_at = '0000000002500-0000-owner' WHERE id = 'shared'",
    ).await;
    let database = StoreDatabase::new(&fixture.source);
    let suffix = database
        .pending_writes()
        .await
        .expect("read actual captured suffix");
    let second = suffix
        .iter()
        .find(|write| &write.write_id != first_id)
        .expect("second captured write")
        .write_id
        .clone();
    fixture.snapshot_peer_edit(false).await;
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume first publication");
    let error = writer
        .drain_store_writes()
        .await
        .expect_err("second recorded write conflicts during first publication");
    drop(writer);
    assert!(
        format!("{error:?}").contains("WriteRebaseConflict"),
        "{error:?}"
    );
    let first = database.write_status(first_id).await.expect("first status");
    assert_eq!(
        first,
        coven_protocol::write::WriteStatus::Publishing,
        "first write did not conflict"
    );
    let blocked = database.write_status(&second).await.expect("second status");
    assert!(
        matches!(blocked,
            coven_protocol::write::WriteStatus::Blocked(coven_protocol::write::WriteBlock::RebaseConflict(ref conflict))
            if conflict.write_id == second
        ),
        "wrong suffix write was blocked: {blocked:?}"
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("preserved first reservation"),
        Some(prepared)
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
            .await,
        "Recorded second body"
    );
}
