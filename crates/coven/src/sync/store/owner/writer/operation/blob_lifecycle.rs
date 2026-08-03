use super::*;

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        self.database
            .validate_store_write_routing(routing_encryption)?;
        let registration = self.database.local_blob_write_authority().await?;
        let authority = crate::storage::BlobWriteAuthority::new(&registration);
        crate::blob::upload::BlobUploadQueue::new(
            &self.database,
            self.storage.as_ref(),
            authority,
            store_dir,
            clock,
            routing_encryption,
            observer,
        )
        .drain()
        .await
    }

    pub(crate) async fn drain_tombstones(
        &self,
        cipher: &dyn CloudCipherAccess,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        let store_id = self.store_root().store_root_id.to_string();
        crate::blob::delete::TombstoneDrain::new(
            &self.database,
            self.storage.as_ref(),
            cipher,
            pending_rotation,
            &store_id,
            self.writer.identity,
            clock,
        )
        .drain()
        .await
    }

    pub(crate) async fn gc_tombstones(
        &self,
        cipher: &dyn CloudCipherAccess,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        let store_id = self.store_root().store_root_id.to_string();
        let activated_uploaders = self
            .database
            .activated_store_device_registration_records()
            .await
            .map_err(|error| error.to_string())?;
        crate::blob::delete::TombstoneCollection::new(
            &self.database,
            self.storage.as_ref(),
            cipher,
            &store_id,
            &crate::keys::public_key_hex(self.writer.identity),
            &activated_uploaders,
            &self.membership,
            clock,
            self.database.blob_tombstone_grace(),
        )
        .collect()
        .await
    }

    pub(crate) async fn drain_local_blob_cleanup(
        &self,
        store_dir: &StoreDir,
    ) -> Result<bool, crate::database::DbError> {
        self.database.drain_local_blob_cleanup(store_dir).await
    }

    pub(crate) async fn persist_hlc_high_water(&self) -> Result<(), crate::database::DbError> {
        self.database.persist_hlc_high_water().await
    }
}
