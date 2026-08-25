use super::*;

impl StoreSync {
    pub(crate) fn host_write_blob_staging(
        &self,
    ) -> Option<coven_replication::sync::store::HostWriteBlobStaging> {
        Some(connected_sync!(self)?.host_write_blob_staging(tokio::runtime::Handle::current()))
    }

    /// The cloud object key `blob` has, or would have, under this store's home.
    ///
    /// The `{uploader}` segment is this store's established identity, read from
    /// custody in every connection state. A connected cloud storage holds a
    /// copy of that same keypair, taken when it was opened, so deriving the
    /// segment from the identity itself leaves one authority rather than two
    /// that agree.
    pub(crate) fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        let scheme = match connected_sync!(self) {
            Some(sync) => sync.blob_path_scheme(),
            None => BlobPathScheme::for_storage(self.config().cloud_home.storage),
        };
        let uploader = self
            .security
            .identity_public_key()
            .map_err(StorageError::Key)?
            .map(hex::encode);
        CloudSyncConnection::blob_key(
            scheme,
            &blob.namespace,
            uploader.as_deref(),
            &blob.id,
            blob.cloud_path.as_deref(),
        )
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        pin: bool,
        refs: Vec<coven_protocol::blob::RowBlobRef>,
    ) -> Result<(), MakeRemoteError> {
        active_sync!(self)
            .ok_or(MakeRemoteError::SyncNotReady)?
            .make_remote(root_table, root_id, root_label, pin, refs)
            .await?;
        self.trigger();
        Ok(())
    }

    /// Record that a transition is to be unwound, connected or not.
    ///
    /// Recording the cancel is a local write; carrying it out — taking any
    /// object already written back out of the cloud — is the drain's, and the
    /// drain needs the provider. Requiring one here made the *decision*
    /// unavailable offline, which is the one moment a person most wants it:
    /// the upload they are watching is not going anywhere either.
    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        match active_sync!(self) {
            Some(sync) => sync.cancel_make_remote(root_table, root_id).await?,
            None => self
                .database
                .cancel_make_remote(root_table, root_id)
                .await
                .map_err(MakeRemoteError::from)?,
        }
        self.trigger();
        Ok(())
    }

    pub(crate) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        active_sync!(self)
            .ok_or(MakeLocalError::SyncNotReady)?
            .make_local(root_table, root_id, dest, cancel)
            .await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn drain_uploads(
        &self,
    ) -> Result<coven_replication::blob::DrainOutcome, SyncError> {
        self.drain_uploads_with(active_sync!(self).ok_or(SyncError::LoopNotRunning)?)
            .await
    }

    pub(crate) async fn retry_uploads_now(
        &self,
    ) -> Result<coven_replication::blob::DrainOutcome, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        if self
            .observer
            .as_ref()
            .is_some_and(|observer| observer.should_skip_uploads())
        {
            return Ok(coven_replication::blob::DrainOutcome::Paused);
        }
        self.database.reset_outbox_backoff().await?;
        self.drain_uploads_with(sync).await
    }

    async fn drain_uploads_with(
        &self,
        sync: Arc<SyncLoopHandle>,
    ) -> Result<coven_replication::blob::DrainOutcome, SyncError> {
        sync.drain_uploads()
            .await
            .map_err(|error| SyncError::BlobUpload(Box::new(error)))
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .discard_blocked_write(write_id)
            .await
            .map_err(SyncError::from)
    }
}
