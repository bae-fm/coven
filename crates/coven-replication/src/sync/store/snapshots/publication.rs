use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{semantic_prefix_from_exact_object, CircleSnapshotMeta};
use coven_storage::CloudSyncObjectStorage;

use super::SnapshotError;

/// One exclusive Store-or-Circle snapshot publication operation.
///
/// The permit keeps durable snapshot state and remote object publication
/// serialized against every other snapshot publication
/// using the same Store database.
pub(crate) struct AuthorizedSnapshotPublication<'operation> {
    database: &'operation coven_database::StoreDatabase,
    storage: &'operation dyn CloudSyncObjectStorage,
    _permit: coven_database::SnapshotPublicationPermit,
}

impl<'operation> AuthorizedSnapshotPublication<'operation> {
    pub(crate) async fn begin(
        database: &'operation coven_database::StoreDatabase,
        storage: &'operation dyn CloudSyncObjectStorage,
    ) -> Self {
        let permit = database.snapshot_publication_permit().await;
        Self {
            database,
            storage,
            _permit: permit,
        }
    }

    pub(crate) async fn complete_candidate_cleanup(&self) -> Result<(), SnapshotError> {
        let Some(active) = self.database.active_store_publication().await? else {
            return Ok(());
        };
        if active.retired_snapshot_objects().is_empty() {
            return Ok(());
        }
        for object in active.retired_snapshot_objects() {
            self.storage
                .delete_protocol_object(object)
                .await
                .map_err(SnapshotError::Bucket)?;
        }
        self.database
            .complete_snapshot_candidate_cleanup(active)
            .await?;
        Ok(())
    }

    pub(crate) async fn upload_store(
        &self,
        pending: &coven_database::DurableSnapshotPublication,
    ) -> Result<(), SnapshotError> {
        let meta = &pending.meta.value;
        for prepared in &pending.blobs {
            let blob = prepared.bindings[0].blob();
            if !prepared.remote.records_verified_upload() {
                return Err(SnapshotError::PublicationState(format!(
                    "snapshot blob {}/{} has no accepted upload",
                    blob.locator().namespace(),
                    blob.locator().blob_id()
                )));
            }
            self.storage
                .verify_blob_object(blob)
                .await
                .map_err(SnapshotError::Bucket)?;
        }
        self.storage
            .create_verified_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    meta.store_root_hash,
                    ProtocolObjectDomain::StoreSnapshotImage,
                ),
                &pending.image.prepared,
                &semantic_prefix_from_exact_object(&meta.image.object, ".db")?,
                &pending.image.value,
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        // Before the metadata that names it: a published snapshot pointing at a
        // rollup nobody put at the provider would send every joining device to
        // a signed reference that does not open.
        self.storage
            .create_verified_protocol_object(
                &ProtocolObjectContext::signed_plaintext(
                    meta.store_root_hash,
                    ProtocolObjectDomain::StoreMembershipRollup,
                ),
                &pending.rollup.prepared,
                &semantic_prefix_from_exact_object(&meta.membership_rollup.object, ".json")?,
                &pending.rollup.bytes,
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        self.storage
            .create_verified_protocol_object(
                &ProtocolObjectContext::signed_plaintext(
                    meta.store_root_hash,
                    ProtocolObjectDomain::StoreSnapshotMeta,
                ),
                &pending.meta.prepared,
                &semantic_prefix_from_exact_object(&pending.reference.object, ".json")
                    .map_err(SnapshotError::from)?,
                &pending.meta.bytes,
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        Ok(())
    }

    pub(crate) async fn resume_pending_circles(&self) -> Result<(), SnapshotError> {
        for circle_id in self.database.pending_circle_snapshot_ids().await? {
            let pending = self
                .database
                .outbound_circle_snapshot_publication(circle_id)
                .await?
                .ok_or_else(|| {
                    SnapshotError::PublicationState(format!(
                        "pending Circle {circle_id} snapshot disappeared during publication"
                    ))
                })?;
            self.publish_circle(pending).await?;
        }
        Ok(())
    }

    pub(crate) async fn publish_circle(
        &self,
        pending: coven_database::DurableCircleSnapshotPublication,
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
        Ok(pending.meta.value)
    }
}
