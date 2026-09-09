use super::*;

impl DatabaseCore {
    pub(crate) fn serialize_and_close_snapshot(
        self,
    ) -> Result<(Vec<u8>, SnapshotPreparationDirectory), DbError> {
        let serialized = crate::connection_io::serialize_database_image(&self.conn);
        self.close_snapshot(serialized)
    }

    pub(super) fn discard_snapshot(self) -> Result<(), DbError> {
        let ((), directory) = self.close_snapshot(Ok(()))?;
        directory.finish(Ok(()))
    }

    fn close_snapshot<T>(
        self,
        outcome: Result<T, DbError>,
    ) -> Result<(T, SnapshotPreparationDirectory), DbError> {
        let Self {
            conn,
            verified_store_authority,
            context,
            snapshot_preparation,
        } = self;
        let directory = snapshot_preparation.ok_or_else(|| {
            DbError::Message("only a prepared checkpoint can finish its image preparation".into())
        })?;
        let closed = conn.close().map_err(|(_, error)| DbError::from(error));
        drop(verified_store_authority);
        drop(context);
        directory.after_close(outcome, closed)
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
            store_session(core).prepare_received_snapshot_circles(&selection, receiver_wall_ms)
        })
        .await
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
                    Ok(mut core) => {
                        core.snapshot_preparation = Some(directory);
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

    async fn finish_snapshot<T>(
        self,
        job: DbJob,
        result: tokio::sync::oneshot::Receiver<Result<T, DbError>>,
    ) -> Result<T, DbError> {
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
