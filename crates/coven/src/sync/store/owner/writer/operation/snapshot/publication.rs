use crate::protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::protocol::store_commit::{
    snapshot_image_semantic_prefix, snapshot_slot_prefix, CircleSnapshotMeta, SnapshotMeta,
};
use crate::storage::{SyncStorage, VerifiedObjectWrites};

use super::{remove_snapshot_spool, SnapshotError};

/// A snapshot object that opened to bytes other than the prepared ones is
/// invalid publication state, not a storage failure.
fn snapshot_readback_error(error: crate::protocol::objects::StorageError) -> SnapshotError {
    match error {
        crate::protocol::objects::StorageError::ReadbackMismatch(key) => {
            SnapshotError::PublicationState(format!(
                "exact readback of {key} differs from its prepared bytes"
            ))
        }
        error => SnapshotError::Bucket(error),
    }
}

/// One exclusive Store-or-Circle snapshot publication operation.
///
/// The permit keeps durable snapshot state, remote object publication, and
/// local spool cleanup serialized against every other snapshot publication
/// using the same Store database.
pub(crate) struct AuthorizedSnapshotPublication<'operation> {
    database: &'operation crate::database::StoreDatabase,
    storage: &'operation dyn SyncStorage,
    _permit: crate::database::SnapshotPublicationPermit,
}

impl<'operation> AuthorizedSnapshotPublication<'operation> {
    pub(crate) async fn begin(
        database: &'operation crate::database::StoreDatabase,
        storage: &'operation dyn SyncStorage,
    ) -> Self {
        let permit = database.snapshot_publication_permit().await;
        Self {
            database,
            storage,
            _permit: permit,
        }
    }

    pub(crate) async fn resume_store(&self) -> Result<Option<SnapshotMeta>, SnapshotError> {
        self.drain_spool_cleanup().await?;
        let Some(pending) = self
            .database
            .outbound_snapshot_publication()
            .await
            .map_err(SnapshotError::from)?
        else {
            return Ok(None);
        };
        self.publish_store(pending).await.map(Some)
    }

    pub(crate) async fn publish_store(
        &self,
        pending: crate::database::DurableSnapshotPublication,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let meta = &pending.meta.value;
        let device_id = meta.author_registration.device_id.to_string();
        let image_context = ProtocolObjectContext::store_encrypted(
            meta.store_root_hash,
            ProtocolObjectDomain::StoreSnapshotImage,
        );
        let image_prefix = snapshot_image_semantic_prefix(&device_id, meta.image.image_hash);
        for prepared in &pending.blobs {
            let blob = prepared.bindings[0].blob();
            let uploader = blob.locator().uploader().clone();
            let registration = self
                .database
                .activated_store_device_registration(uploader.clone())
                .await
                .map_err(SnapshotError::from)?;
            let authority = crate::protocol::objects::BlobWriteAuthority::new(&registration);
            if let Some(spool_path) = &prepared.spool_path {
                self.storage
                    .create_blob_object_from_file(
                        blob,
                        &authority,
                        spool_path,
                        &crate::storage::cloud::no_progress(),
                    )
                    .await
                    .map_err(SnapshotError::Bucket)?;
            }
            self.storage
                .verify_blob_object(blob)
                .await
                .map_err(SnapshotError::Bucket)?;
        }
        self.storage
            .create_protocol_object(&pending.image.prepared)
            .await
            .map_err(SnapshotError::Bucket)?;
        self.storage
            .verify_readback(
                &image_context,
                &meta.image.object,
                &image_prefix,
                &pending.image.bytes,
            )
            .await
            .map_err(snapshot_readback_error)?;

        let meta_context = ProtocolObjectContext::signed_plaintext(
            meta.store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let meta_prefix = snapshot_slot_prefix(&device_id, pending.reference.generation);
        self.storage
            .create_protocol_object(&pending.meta.prepared)
            .await
            .map_err(SnapshotError::Bucket)?;
        self.storage
            .verify_readback(
                &meta_context,
                &pending.reference.object,
                &meta_prefix,
                &pending.meta.bytes,
            )
            .await
            .map_err(snapshot_readback_error)?;
        self.database
            .complete_snapshot_publication(pending.reference)
            .await
            .map_err(SnapshotError::from)?;
        self.drain_spool_cleanup().await?;
        Ok(pending.meta.value)
    }

    pub(crate) async fn resume_circle(
        &self,
        pending: crate::database::DurableCircleSnapshotPublication,
    ) -> Result<CircleSnapshotMeta, SnapshotError> {
        self.drain_spool_cleanup().await?;
        self.publish_circle(pending).await
    }

    pub(crate) async fn publish_circle(
        &self,
        pending: crate::database::DurableCircleSnapshotPublication,
    ) -> Result<CircleSnapshotMeta, SnapshotError> {
        // The exact ciphertext and its plaintext binding were established when
        // the objects were prepared, so publication does not need the Circle key.
        self.storage
            .create_protocol_object(&pending.image.prepared)
            .await
            .map_err(SnapshotError::Bucket)?;
        self.storage
            .create_protocol_object(&pending.meta.prepared)
            .await
            .map_err(SnapshotError::Bucket)?;
        self.database
            .complete_circle_snapshot_publication(pending.reference)
            .await
            .map_err(SnapshotError::from)?;
        self.drain_spool_cleanup().await?;
        Ok(pending.meta.value)
    }

    pub(crate) async fn drain_spool_cleanup(&self) -> Result<(), SnapshotError> {
        for path in self
            .database
            .snapshot_blob_spool_cleanup_paths()
            .await
            .map_err(SnapshotError::from)?
        {
            remove_snapshot_spool(&path, false)
                .await
                .map_err(SnapshotError::PublicationState)?;
            self.database
                .complete_snapshot_blob_spool_cleanup(&path)
                .await
                .map_err(SnapshotError::from)?;
        }
        Ok(())
    }
}
