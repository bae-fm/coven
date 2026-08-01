use super::*;

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        hlc: &crate::sync::hlc::Hlc,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        StoreDatabase::validate_store_write_routing(
            self.database.gates().as_ref(),
            routing_encryption,
        )?;
        let (registration_ref, registration) = self.database.local_blob_write_authority().await?;
        let authority = crate::storage::BlobWriteAuthority::new(&registration_ref, &registration)
            .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
        crate::blob::upload::BlobUploadQueue::new(
            &self.database,
            self.storage.as_ref(),
            authority,
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .drain()
        .await
    }

    pub(crate) async fn drain_tombstones(
        &self,
        cloud_home: &dyn CloudHome,
        cipher: &dyn CloudCipherAccess,
        pending_rotation: &crate::storage::PendingRotation,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        let store_id = self.store_root().store_root_id.to_string();
        crate::blob::delete::TombstoneDrain::new(
            &self.database,
            cloud_home,
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
        cloud_home: &dyn CloudHome,
        cipher: &dyn CloudCipherAccess,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        let store_id = self.store_root().store_root_id.to_string();
        let activated_uploaders = self
            .database
            .activated_store_device_registration_records()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .collect();
        crate::blob::delete::TombstoneCollection::new(
            &self.database,
            cloud_home,
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

    pub(crate) async fn persist_hlc_high_water(
        &self,
        hlc: &crate::sync::hlc::Hlc,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .set_protocol_state(
                crate::sync::hlc::HIGHWATER_STATE_KEY,
                &hlc.high_water().to_string(),
            )
            .await
    }
}
