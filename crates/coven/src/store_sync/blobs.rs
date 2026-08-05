use super::*;

impl StoreSync {
    pub(crate) async fn read_blob(
        &self,
        reference: &crate::protocol::blob::RowBlobRef,
    ) -> Result<Vec<u8>, BlobCacheError> {
        self.blob_access.read(reference).await
    }

    pub(crate) async fn materialize_blob(
        &self,
        reference: &crate::protocol::blob::RowBlobRef,
    ) -> Result<(), BlobCacheError> {
        self.blob_access.materialize(reference).await
    }

    pub(crate) async fn open_blob_stream(
        &self,
        reference: &crate::protocol::blob::RowBlobRef,
    ) -> Result<crate::sync::BlobStream, BlobCacheError> {
        self.blob_access.open_stream(reference).await
    }

    pub(crate) async fn pin_blobs(
        &self,
        references: &[crate::protocol::blob::RowBlobRef],
    ) -> Result<(), BlobCacheError> {
        self.blob_access.pin(references).await
    }

    pub(crate) async fn unpin_blobs(
        &self,
        references: &[crate::protocol::blob::RowBlobRef],
    ) -> Result<(), BlobCacheError> {
        self.local_blob_access.unpin(references).await
    }

    pub(crate) async fn all_blobs_pinned(
        &self,
        references: &[crate::protocol::blob::RowBlobRef],
    ) -> Result<bool, BlobCacheError> {
        self.local_blob_access.all_pinned(references).await
    }

    pub(crate) async fn evict_blob(
        &self,
        reference: &crate::protocol::blob::RowBlobRef,
    ) -> Result<(), BlobCacheError> {
        self.local_blob_access.evict(reference).await
    }

    pub(crate) fn host_write_blob_staging(
        &self,
    ) -> Option<crate::sync::store::HostWriteBlobStaging> {
        Some(self.connected()?.host_write_blob_staging())
    }

    pub(crate) fn blob_cloud_key(&self, blob: &BlobRef) -> Result<String, StorageError> {
        let (scheme, uploader) = match self.connected() {
            Some(connection) => (connection.blob_path_scheme(), Some(connection.uploader())),
            None => {
                let scheme = BlobPathScheme::for_storage(self.config().cloud_home.storage);
                let uploader = self
                    .security
                    .identity_public_key()
                    .map_err(|error| {
                        StorageError::Storage(format!("read this store's identity: {error}"))
                    })?
                    .map(hex::encode);
                (scheme, uploader)
            }
        };
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
