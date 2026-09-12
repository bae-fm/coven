use super::*;

/// The files one unpublished snapshot preparation owns, and what releasing them
/// means. Both kinds hold their database private until the preparation
/// finishes.
pub(crate) enum SnapshotPreparation {
    /// Warm adoption: a disposable directory holding the copy and its payloads.
    /// Finishing it removes the directory whole.
    Directory(SnapshotPreparationDirectory),
    /// Cold restore: the destination database itself, unpublished until it can
    /// serve. Finishing it removes the database files and the payload files the
    /// restore created, and leaves every other file in the store directory.
    Destination(ColdSnapshotDestination),
}

impl SnapshotPreparation {
    fn finish<T>(self, outcome: Result<T, DbError>) -> Result<T, DbError> {
        match self {
            Self::Directory(directory) => directory.finish(outcome),
            Self::Destination(destination) => destination.finish(outcome),
        }
    }
}

/// The destination database of a cold restore while it is still private: its
/// image files, and every payload spool file this restore put beside them.
///
/// A spool file the install found already there and reused is not one of these,
/// so releasing the destination never removes a file the restore did not write.
pub(crate) struct ColdSnapshotDestination {
    image: crate::SnapshotDatabaseImage,
    created_payload_files: DestinationPayloadFiles,
}

impl ColdSnapshotDestination {
    fn new(
        image: crate::SnapshotDatabaseImage,
        store_dir: coven_foundation::store_dir::StoreDir,
        created_payload_files: Vec<PathBuf>,
    ) -> Self {
        Self {
            image,
            created_payload_files: DestinationPayloadFiles {
                store_dir,
                files: created_payload_files,
            },
        }
    }

    fn record(&mut self, created: Vec<PathBuf>) {
        self.created_payload_files.files.extend(created);
    }

    /// Publish the destination: the database is the store's now, and its payload
    /// files belong to the committed rows that name them.
    fn commit(self) {
        let Self {
            image,
            mut created_payload_files,
        } = self;
        created_payload_files.files.clear();
        image.commit();
    }

    /// Remove everything the restore attempt created. The caller closes SQLite
    /// first, so the image's sidecars are gone rather than written again after.
    fn finish<T>(self, outcome: Result<T, DbError>) -> Result<T, DbError> {
        let Self {
            image,
            mut created_payload_files,
        } = self;
        let cleanup = created_payload_files.remove();
        match image.finish_operation(outcome.and_then(|value| cleanup.map(|()| value))) {
            Ok(value) => Ok(value),
            Err(crate::SnapshotImageOperationError::Operation(cause)) => Err(cause),
            Err(crate::SnapshotImageOperationError::Cleanup { path, cleanup }) => {
                Err(crate::SnapshotImageError::Cleanup { path, cleanup }.into())
            }
            Err(crate::SnapshotImageOperationError::CleanupAfterFailure {
                path,
                cleanup,
                cause,
            }) => Err(crate::SnapshotImageError::CleanupAfterFailure {
                path,
                cleanup,
                cause: Box::new(crate::SnapshotImageError::from(cause)),
            }
            .into()),
        }
    }
}

/// Payload spool files an unfinished restore created. Removing them is the
/// terminal act; an abandoned guard removes them too, but can only log.
struct DestinationPayloadFiles {
    store_dir: coven_foundation::store_dir::StoreDir,
    files: Vec<PathBuf>,
}

impl DestinationPayloadFiles {
    fn remove(&mut self) -> Result<(), DbError> {
        crate::store::remove_created_payload_files(&self.store_dir, std::mem::take(&mut self.files))
            .map_err(DbError::StagedBlobRollback)
    }
}

impl Drop for DestinationPayloadFiles {
    fn drop(&mut self) {
        if self.files.is_empty() {
            return;
        }
        if let Err(error) = self.remove() {
            error!(%error, "could not remove an abandoned snapshot restore's payload files");
        }
    }
}

impl DatabaseCore {
    pub(super) fn discard_snapshot(self) -> Result<(), DbError> {
        self.close_snapshot()?.finish(Ok(()))
    }

    pub(crate) fn close_snapshot(self) -> Result<SnapshotPreparation, DbError> {
        let Self {
            conn,
            verified_store_authority,
            context,
            snapshot_preparation,
        } = self;
        let preparation = snapshot_preparation.ok_or_else(|| {
            DbError::Message("only a prepared checkpoint can finish its image preparation".into())
        })?;
        let closed = conn.close().map_err(|(_, error)| DbError::from(error));
        drop(verified_store_authority);
        drop(context);
        match closed {
            Ok(()) => Ok(preparation),
            Err(error) => preparation.finish(Err(error)),
        }
    }

    /// Close a warm adoption's preparation, whose sealed artifact is its
    /// directory. A cold restore destination has no artifact to hand over.
    pub(crate) fn close_snapshot_directory(self) -> Result<SnapshotPreparationDirectory, DbError> {
        match self.close_snapshot()? {
            SnapshotPreparation::Directory(directory) => Ok(directory),
            SnapshotPreparation::Destination(destination) => destination.finish(Err(
                DbError::Message("a cold restore destination has no prepared snapshot".into()),
            )),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn injected_circle_restore_failure(&self) -> Result<(), DbError> {
        if self
            .context
            .fail_circle_restore
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(DbError::Message(
                "injected Circle install failure after Store install".to_string(),
            ));
        }
        Ok(())
    }
}

/// Every call below the terminal ones borrows the preparation worker;
/// [`Database::finish_cold_snapshot`] and [`ColdSnapshotPreparation::discard`]
/// take it, and a later drop then has nothing left to release.
const WORKER_HELD: &str = "a cold snapshot preparation holds its worker until it finishes";

/// The destination of a cold snapshot restore while it is still private: the
/// verified Store image is installed, the restoring identity's Circle images
/// are not yet, and no [`Database`] handle to it exists outside this value.
pub struct ColdSnapshotPreparation {
    /// Taken by the terminal call, so a later drop releases nothing twice.
    connection: Option<DatabaseConnection>,
}

impl ColdSnapshotPreparation {
    fn new(connection: DatabaseConnection) -> Self {
        Self {
            connection: Some(connection),
        }
    }

    /// Install the Circle images the restoring identity selects.
    ///
    /// `select` resolves that selection against this destination's installed
    /// Store image. It is lent the database for exactly as long as the selection
    /// runs, so the restore keeps the only lasting handle to it. The spool files
    /// the install then writes join the destination's own, so abandoning the
    /// restore removes them whether or not the install committed.
    pub async fn restore_snapshot_circles<Select, Selection, E>(
        &self,
        select: Select,
    ) -> Result<(), E>
    where
        Select: FnOnce(crate::StoreDatabase) -> Selection,
        Selection: std::future::Future<Output = Result<crate::StagedCircleRestore, E>>,
        E: From<DbError>,
    {
        let connection = self.connection.as_ref().expect(WORKER_HELD);
        let restoring =
            crate::StoreDatabase::from_database(Database::from_connection(connection.clone()));
        let selection = select(restoring).await?;
        connection
            .restore_cold_snapshot_circles(selection)
            .await
            .map_err(E::from)
    }

    /// Release the destination: close SQLite, then remove its database files and
    /// every payload file this attempt created.
    pub async fn discard(mut self) -> Result<(), DbError> {
        let connection = self
            .connection
            .take()
            .expect("a cold snapshot preparation finishes once");
        connection.discard_snapshot_preparation().await
    }

    /// Fail the next Circle restore after its payload files are written — a
    /// test's stand-in for a crash between the Store and Circle installs.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_circle_restore_for_test(&self) {
        self.connection
            .as_ref()
            .expect(WORKER_HELD)
            .context
            .fail_circle_restore
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Database {
    /// Publish a finished cold snapshot restore: seed the destination's register
    /// clock from the rows now installed, start its serving worker, and only
    /// then release the image — the database becomes visible once it can serve.
    pub async fn finish_cold_snapshot(
        mut preparation: ColdSnapshotPreparation,
    ) -> Result<Self, DbError> {
        let connection = preparation
            .connection
            .take()
            .expect("a cold snapshot preparation finishes once");
        let (reply, result) = tokio::sync::oneshot::channel();
        let mut core = connection
            .finish_snapshot(DbJob::TakeCore(reply), result)
            .await?;
        let Some(SnapshotPreparation::Destination(destination)) = core.snapshot_preparation.take()
        else {
            return Err(DbError::Message(
                "only a cold restore destination can finish a cold restore".into(),
            ));
        };
        // The core is off every worker here, so its SQLite work runs on a
        // blocking thread rather than on the caller's executor.
        let seeded = tokio::task::spawn_blocking(move || match core.seed_clock() {
            Ok(()) => Ok(core),
            Err(error) => {
                drop(core);
                Err(error)
            }
        })
        .await
        .map_err(|error| {
            DbError::context(
                "seed the restored database clock",
                std::io::Error::other(error),
            )
        })?;
        let core = match seeded {
            Ok(core) => core,
            Err(error) => return destination.finish(Err(error)),
        };
        match Self::from_core(core, "coven-db") {
            Ok(database) => {
                destination.commit();
                Ok(database)
            }
            Err(error) => destination.finish(Err(error)),
        }
    }
}

impl Drop for ColdSnapshotPreparation {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        // An abandoned restore must leave nothing behind before the next attempt
        // opens the same path, so this waits for the worker rather than
        // detaching it. A drop has nobody to report cleanup failure to.
        if let Err(error) = connection.discard_snapshot_preparation_blocking() {
            error!(%error, "could not release an abandoned cold snapshot restore");
        }
    }
}

impl DatabaseConnection {
    pub(crate) async fn prepare_received_snapshot_circles(
        &self,
        selection: crate::StagedCircleRestore,
        receiver_wall_ms: u64,
    ) -> Result<(), DbError> {
        self.on_connection_thread(move |core| {
            if core.snapshot_preparation.is_none() {
                return Err(DbError::Message(
                    "recipient Circle preparation requires a disposable checkpoint database".into(),
                ));
            }
            store_session(core).restore_snapshot_circles(
                &selection,
                receiver_wall_ms,
                crate::payload_store::CreatedPayloadFiles::untracked(),
            )
        })
        .await
    }

    async fn restore_cold_snapshot_circles(
        &self,
        selection: crate::StagedCircleRestore,
    ) -> Result<(), DbError> {
        self.on_connection_thread(move |core| {
            let receiver_wall_ms = core.context.hlc.wall_now_ms();
            if !matches!(
                &core.snapshot_preparation,
                Some(SnapshotPreparation::Destination(_))
            ) {
                return Err(DbError::Message(
                    "recipient Circle restoration requires a cold restore destination".into(),
                ));
            }
            let created = std::cell::RefCell::new(Vec::new());
            let restored = store_session(core).restore_snapshot_circles(
                &selection,
                receiver_wall_ms,
                crate::payload_store::CreatedPayloadFiles::tracked(&created),
            );
            #[cfg(any(test, feature = "test-utils"))]
            let restored = restored.and_then(|()| core.injected_circle_restore_failure());
            let created = created.into_inner();
            match restored {
                Ok(()) => {
                    let Some(SnapshotPreparation::Destination(destination)) =
                        &mut core.snapshot_preparation
                    else {
                        unreachable!("the destination was checked before the restore ran");
                    };
                    destination.record(created);
                    Ok(())
                }
                Err(operation) => Err(
                    match crate::store::remove_created_payload_files(
                        &core.context.store_dir,
                        created,
                    ) {
                        Ok(()) => operation,
                        Err(rollback) => DbError::AudienceBlobRollbackFailed {
                            operation: Box::new(operation),
                            rollback,
                        },
                    },
                ),
            }
        })
        .await
    }

    /// Open the database `image` names as the destination of a cold snapshot
    /// restore: install the verified Store image, and hold the database private
    /// to the returned preparation until it finishes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_cold_snapshot(
        image: crate::SnapshotDatabaseImage,
        install: &VerifiedSnapshotBootstrapInstall,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        hlc: Arc<Hlc>,
        coven_migration_policy: CovenMigrationPolicy,
        migrations: &[Migration],
    ) -> Result<ColdSnapshotPreparation, OpenError> {
        let store_dir = crate::database_runtime::store_dir_of(image.path());
        // The open removes the payload files it created if it fails, and the
        // image goes with them — nothing is installed to keep them for.
        let opened = DatabaseCore::open_unseeded(
            image.path(),
            store_dir.clone(),
            crate::connection_io::ConnectionDurability::Full,
            synced_tables,
            blob_tombstone_grace,
            transfer_limits,
            hlc,
            coven_migration_policy,
            migrations,
            crate::database_open::CovenMetadataOpen::VerifiedSnapshot(install),
            true,
        );
        let (mut core, created_payload_files) = match opened {
            Ok(opened) => opened,
            Err(operation) => {
                return Err(match image.finish_operation(Err(operation)) {
                    Ok(()) => unreachable!("a failed open finishes as a failure"),
                    Err(crate::SnapshotImageOperationError::Operation(operation)) => operation,
                    Err(crate::SnapshotImageOperationError::Cleanup { path, cleanup }) => {
                        OpenError::from(DbError::from(crate::SnapshotImageError::Cleanup {
                            path,
                            cleanup,
                        }))
                    }
                    Err(crate::SnapshotImageOperationError::CleanupAfterFailure {
                        path,
                        cleanup,
                        cause,
                    }) => OpenError::PreparationCleanup {
                        operation: Box::new(cause),
                        cleanup: Box::new(DbError::from(crate::SnapshotImageError::Cleanup {
                            path,
                            cleanup,
                        })),
                    },
                });
            }
        };
        core.snapshot_preparation = Some(SnapshotPreparation::Destination(
            ColdSnapshotDestination::new(image, store_dir, created_payload_files),
        ));
        Self::start(core, "coven-snapshot-preparation")
            .map(ColdSnapshotPreparation::new)
            .map_err(OpenError::from)
    }

    pub(crate) async fn prepare_snapshot_database(
        &self,
        plaintext: Vec<u8>,
        install: VerifiedSnapshotBootstrapInstall,
    ) -> Result<Self, OpenError> {
        let core = self
            .on_connection_thread(move |receiver| {
                if ObjectHash::digest(&plaintext) != install.snapshot.meta.image.image_hash {
                    return Err(OpenError::from(DbError::Message(
                        "snapshot preparation image differs from its authenticated plaintext hash"
                            .into(),
                    )));
                }
                let parent = receiver
                    .context
                    .store_dir
                    .as_ref()
                    .join("snapshot-preparations");
                std::fs::create_dir_all(&parent).map_err(DbError::from)?;
                let preparation_path = parent.join(receiver.context.ids.new_id());
                let directory = SnapshotPreparationDirectory::create(preparation_path.clone())?;
                let store_dir =
                    coven_foundation::store_dir::StoreDir::new_ephemeral(preparation_path);
                let path = store_dir.db_path();
                let prepared = (|| {
                    crate::SnapshotDatabaseImage::create(path.clone(), &plaintext)
                        .map_err(DbError::from)?
                        .commit();
                    DatabaseCore::open_unseeded(
                        &path,
                        store_dir,
                        crate::connection_io::ConnectionDurability::Full,
                        receiver.context.synced_tables.as_ref().clone(),
                        receiver.context.blob_tombstone_grace,
                        *receiver
                            .context
                            .transfer_limits
                            .lock()
                            .expect("transfer limits mutex poisoned"),
                        receiver.context.hlc.clone(),
                        receiver.context.coven_migration_policy,
                        &receiver.context.migrations,
                        crate::database_open::CovenMetadataOpen::VerifiedSnapshot(&install),
                        false,
                    )
                })();
                match prepared {
                    // The directory owns every file the preparation writes, its
                    // payloads included, so removing it covers the reported list.
                    Ok((mut core, _created_payload_files)) => {
                        core.snapshot_preparation = Some(SnapshotPreparation::Directory(directory));
                        Ok(core)
                    }
                    Err(operation) => match directory.finish(Ok(())) {
                        Ok(()) => Err(operation),
                        Err(cleanup) => Err(OpenError::PreparationCleanup {
                            operation: Box::new(operation),
                            cleanup: Box::new(cleanup),
                        }),
                    },
                }
            })
            .await?;
        Self::start(core, "coven-snapshot-preparation").map_err(OpenError::from)
    }

    pub(crate) async fn into_prepared_snapshot(self) -> Result<PreparedStoreSnapshot, DbError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        self.finish_snapshot(DbJob::SealSnapshot(reply), result)
            .await
    }

    pub(crate) async fn discard_snapshot_preparation(self) -> Result<(), DbError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        self.finish_snapshot(DbJob::DiscardSnapshot(reply), result)
            .await
    }

    /// Release an abandoned preparation with no runtime to await on. Joining the
    /// worker is what makes the release complete before this returns; the reply
    /// is already sent by then, so it is read without waiting.
    fn discard_snapshot_preparation_blocking(self) -> Result<(), DbError> {
        let (reply, mut result) = tokio::sync::oneshot::channel();
        let worker = self.stop_snapshot_worker(DbJob::DiscardSnapshot(reply))?;
        if let Err(panic) = worker.join() {
            std::panic::resume_unwind(panic);
        }
        result.try_recv().map_err(|error| {
            DbError::context(
                "receive closed snapshot preparation",
                std::io::Error::other(error),
            )
        })?
    }

    /// Queue `job` as the preparation worker's last, and hand back its thread.
    fn stop_snapshot_worker(self, job: DbJob) -> Result<std::thread::JoinHandle<()>, DbError> {
        let mut thread = Arc::try_unwrap(self.thread).map_err(|_| {
            DbError::Message("snapshot preparation still has outstanding query handles".into())
        })?;
        thread.jobs.send(job).map_err(|_| {
            DbError::Message("snapshot preparation worker stopped before completion".into())
        })?;
        let worker = thread
            .join
            .take()
            .expect("owned connection thread has its worker");
        drop(thread);
        drop(self.context);
        Ok(worker)
    }

    async fn finish_snapshot<T>(
        self,
        job: DbJob,
        result: tokio::sync::oneshot::Receiver<Result<T, DbError>>,
    ) -> Result<T, DbError> {
        let worker = self.stop_snapshot_worker(job)?;
        let joined = tokio::task::spawn_blocking(move || worker.join())
            .await
            .map_err(|error| {
                DbError::context(
                    "join snapshot preparation worker",
                    std::io::Error::other(error),
                )
            })?;
        if let Err(panic) = joined {
            std::panic::resume_unwind(panic);
        }
        result.await.map_err(|error| {
            DbError::context(
                "receive closed snapshot preparation",
                std::io::Error::other(error),
            )
        })?
    }
}

#[cfg(test)]
#[path = "snapshot_preparation_tests.rs"]
mod tests;
