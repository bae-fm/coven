use super::*;

fn open_preparation(path: std::path::PathBuf) -> DatabaseConnection {
    let directory = SnapshotPreparationDirectory::create(path.clone()).expect("create preparation");
    let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(&path);
    let (mut core, _created_payload_files) = DatabaseCore::open_unseeded(
        &store_dir.db_path(),
        store_dir.clone(),
        crate::connection_io::ConnectionDurability::Full,
        Vec::new(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        Arc::new(
            Hlc::try_new(
                "snapshot-preparation".into(),
                Arc::new(coven_foundation::clock::SystemClock),
            )
            .expect("prepare clock"),
        ),
        CovenMigrationPolicy::ApplyPending,
        &[Migration::sql(
            1,
            "prepared-row",
            "CREATE TABLE prepared_rows (value TEXT NOT NULL); \
             INSERT INTO prepared_rows VALUES ('initial')",
        )],
        crate::database_open::CovenMetadataOpen::Detect,
        false,
    )
    .expect("open preparation");
    core.snapshot_preparation = Some(SnapshotPreparation::Directory(directory));
    DatabaseConnection::start(core, "snapshot-preparation-test").expect("start preparation worker")
}

#[tokio::test]
async fn sealed_snapshot_retains_final_rows_and_payloads_without_its_worker() {
    let temporary = tempfile::tempdir().expect("preparation parent");
    let path = temporary.path().join("preparation");
    let connection = open_preparation(path.clone());
    let context = Arc::downgrade(&connection.context);
    connection
        .on_connection_thread(|core| {
            core.conn
                .execute("UPDATE prepared_rows SET value = 'prepared'", [])?;
            std::fs::write(core.context.store_dir.as_ref().join("payload"), b"payload")?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("finish preparation");

    let sealed = connection
        .into_prepared_snapshot()
        .await
        .expect("seal preparation");
    assert!(
        context.upgrade().is_none(),
        "the artifact retains no worker context"
    );
    assert_eq!(
        std::fs::read(path.join("payload")).expect("retained payload"),
        b"payload"
    );
    sealed.assert_prepared_row_for_test("prepared");
    sealed.discard().expect("release sealed preparation");
    assert!(
        !path.exists(),
        "image and payload ownership retire together"
    );
}

#[tokio::test]
async fn sealed_snapshot_reads_committed_pages_retained_in_the_wal() {
    let temporary = tempfile::tempdir().expect("preparation parent");
    let path = temporary.path().join("preparation");
    let connection = open_preparation(path.clone());
    let database_path = coven_foundation::store_dir::StoreDir::new_ephemeral(&path).db_path();
    let reader = rusqlite::Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open reader holding the initial snapshot");
    let transaction = reader.unchecked_transaction().expect("begin read snapshot");
    let initial: String = transaction
        .query_row("SELECT value FROM prepared_rows", [], |row| row.get(0))
        .expect("pin initial rows in the reader");
    assert_eq!(initial, "initial");
    connection
        .on_connection_thread(|core| {
            core.conn
                .execute("UPDATE prepared_rows SET value = 'prepared'", [])?;
            Ok::<_, DbError>(())
        })
        .await
        .expect("commit while the reader holds older rows");
    let sealed = connection
        .into_prepared_snapshot()
        .await
        .expect("seal preparation");
    assert!(
        std::fs::metadata(database_path.with_extension("db-wal"))
            .expect("the reader prevents retiring the WAL")
            .len()
            > 0
    );
    sealed.assert_prepared_row_for_test("prepared");
    drop(transaction);
    drop(reader);
    sealed
        .discard()
        .expect("release preparation and WAL together");
    assert!(!path.exists());
}

#[tokio::test]
async fn discarded_snapshot_preparation_releases_its_database_and_payloads() {
    let temporary = tempfile::tempdir().expect("preparation parent");
    let path = temporary.path().join("preparation");
    let connection = open_preparation(path.clone());
    let context = Arc::downgrade(&connection.context);
    std::fs::write(path.join("payload"), b"payload").expect("write preparation payload");

    connection
        .discard_snapshot_preparation()
        .await
        .expect("discard preparation");
    assert!(context.upgrade().is_none());
    assert!(!path.exists());
}

#[tokio::test]
async fn cancelled_snapshot_sealing_releases_the_closed_image_and_payloads() {
    let temporary = tempfile::tempdir().expect("preparation parent");
    let path = temporary.path().join("preparation");
    let connection = open_preparation(path.clone());
    let context = Arc::downgrade(&connection.context);
    let jobs = connection.thread.jobs.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    assert!(
        jobs.send(DbJob::Run(Box::new(move |_| {
            entered.send(()).expect("observe worker barrier");
            wait.recv().expect("release worker barrier");
        })))
        .is_ok(),
        "enqueue worker barrier"
    );
    started.await.expect("worker reached barrier");
    let mut sealing = Box::pin(connection.into_prepared_snapshot());
    std::future::poll_fn(|context| {
        assert!(std::future::Future::poll(sealing.as_mut(), context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(sealing);

    struct WorkerStopped(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for WorkerStopped {
        fn drop(&mut self) {
            // Closing the worker drops its queued jobs after the sealed artifact.
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (stopped, finished) = tokio::sync::oneshot::channel();
    let stopped = WorkerStopped(Some(stopped));
    assert!(
        jobs.send(DbJob::Run(Box::new(move |_| {
            drop(stopped);
            panic!("a sealed worker executed a later query");
        })))
        .is_ok(),
        "observe worker shutdown"
    );
    release.send(()).expect("release preparation");
    tokio::time::timeout(std::time::Duration::from_secs(5), finished)
        .await
        .expect("worker shutdown deadline")
        .expect("worker stopped");
    assert!(context.upgrade().is_none());
    assert!(!path.exists(), "cancelled sealing retained its files");
}
