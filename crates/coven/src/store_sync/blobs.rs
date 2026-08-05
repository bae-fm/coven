use super::*;

impl StoreSync {
    pub(crate) fn host_write_blob_staging(
        &self,
    ) -> Option<crate::sync::store::HostWriteBlobStaging> {
        Some(
            self.connected()?
                .host_write_blob_staging(tokio::runtime::Handle::current()),
        )
    }

    /// The cloud object key `blob` has, or would have, under this store's home.
    ///
    /// The `{uploader}` segment is this store's established identity, read from
    /// custody in every connection state. A connected cloud storage holds a
    /// copy of that same keypair, taken when it was opened, so deriving the
    /// segment from the identity itself leaves one authority rather than two
    /// that agree.
    pub(crate) fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        let scheme = match self.connected() {
            Some(sync) => sync.blob_path_scheme(),
            None => BlobPathScheme::for_storage(self.config().cloud_home.storage),
        };
        let uploader = self
            .security
            .identity_public_key()
            .map_err(|error| StorageError::Storage(format!("read this store's identity: {error}")))?
            .map(hex::encode);
        CloudSyncStorage::blob_key(
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
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        self.active()
            .ok_or(MakeRemoteError::SyncNotReady)?
            .make_remote(root_table, root_id, pin)
            .await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        self.active()
            .ok_or(MakeRemoteError::SyncNotReady)?
            .cancel_make_remote(root_table, root_id)
            .await?;
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
        self.active()
            .ok_or(MakeLocalError::SyncNotReady)?
            .make_local(root_table, root_id, dest, cancel)
            .await?;
        self.trigger();
        Ok(())
    }

    pub(crate) async fn drain_uploads(
        &self,
    ) -> Result<crate::protocol::blob::DrainOutcome, SyncError> {
        self.active()
            .ok_or(SyncError::LoopNotRunning)?
            .drain_uploads()
            .await
            .map_err(SyncError::BlobUpload)
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, SyncError> {
        self.active()
            .ok_or(SyncError::LoopNotRunning)?
            .discard_blocked_write(write_id)
            .await
            .map_err(SyncError::from)
    }
}
