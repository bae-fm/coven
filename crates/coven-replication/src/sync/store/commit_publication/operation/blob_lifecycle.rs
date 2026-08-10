use super::*;
use coven_database::StoredBlobReferenceState;
use tracing::{debug, warn};

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn drain_tombstones(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<usize, String> {
        let store_id = self.store_root().store_root_id.to_string();
        self.writer
            .drain_tombstones(&self.database, self.storage.as_ref(), &store_id, clock)
            .await
    }

    /// Reclaim blobs whose authentic tombstones have aged past the configured
    /// convergence grace. Each candidate must decrypt under this Store, bind its
    /// signed exact object to its slot, come from a current write-capable member,
    /// and name an activated uploader. A member reclaims its own objects; an Owner
    /// may sweep every member's objects.
    ///
    /// Past the grace, a live remote row cancels the tombstone. Otherwise the
    /// operation rechecks that the tombstone still exists, deletes only its signed
    /// immutable provider object, and then removes the tombstone. Invalid,
    /// unauthorized, foreign-Store, and within-grace objects remain non-actionable.
    pub(crate) async fn gc_tombstones(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<usize, String> {
        let store_id = self.store_root().store_root_id.to_string();
        let activated_uploaders = self
            .database
            .activated_store_device_registration_records()
            .await
            .map_err(|error| error.to_string())?;
        let self_pubkey = self.writer.author_pubkey();
        let grace = self.database.blob_tombstone_grace();
        let tombstones = self
            .storage
            .list_blob_tombstones()
            .await
            .map_err(|error| format!("Failed to list tombstones: {error}"))?;
        let is_owner = self.membership.is_owner_now(&self_pubkey);
        let now = clock.now();
        let mut deleted = 0;

        for listed in tombstones {
            let (key_object_id, decoded) = match listed {
                coven_storage::ListedBlobTombstone::Opened {
                    object_id,
                    plaintext,
                } => (object_id, plaintext),
                coven_storage::ListedBlobTombstone::Invalid {
                    provider_key,
                    reason,
                } => {
                    debug!("skipping invalid tombstone {provider_key}: {reason}");
                    continue;
                }
            };
            let key = key_object_id.to_string();
            let tombstone: crate::blob::delete::BlobTombstoneJson =
                match serde_json::from_slice(&decoded) {
                    Ok(tombstone) => tombstone,
                    Err(error) => {
                        warn!("skipping unparseable tombstone {key}: {error}");
                        continue;
                    }
                };

            let tombstone_cloud_key = crate::blob::delete::stored_cloud_key(&tombstone.stored);
            let tombstone_object_id = crate::blob::delete::tombstone_object_id(&tombstone.stored);
            if tombstone_object_id != key_object_id {
                warn!(
                    "skipping tombstone {key}: signed object {tombstone_object_id} does not match its slot"
                );
                continue;
            }
            if !tombstone.verify(&store_id) {
                warn!("skipping tombstone {key} with an invalid signature");
                continue;
            }
            if !self.membership.can_write_now(&tombstone.author_pubkey) {
                warn!(
                    "skipping tombstone {key}: author {} is not a current write-capable member",
                    tombstone.author_pubkey
                );
                continue;
            }

            let deleted_at = match chrono::DateTime::parse_from_rfc3339(&tombstone.deleted_at) {
                Ok(deleted_at) => deleted_at.with_timezone(&chrono::Utc),
                Err(error) => {
                    warn!(
                        "skipping tombstone {key} with unparseable deleted_at {:?}: {error}",
                        tombstone.deleted_at
                    );
                    continue;
                }
            };
            if now.signed_duration_since(deleted_at) <= grace {
                debug!(
                    tombstone = %key,
                    deleted_at = %tombstone.deleted_at,
                    "skipping tombstone still inside the grace",
                );
                continue;
            }

            match self
                .database
                .stored_blob_reference_state(tombstone.stored.clone())
                .await
                .map_err(|error| format!("Failed to check live blob references: {error}"))?
            {
                StoredBlobReferenceState::LiveRemote => {
                    self.storage
                        .delete_blob_tombstone(&tombstone.stored)
                        .await
                        .map_err(|error| {
                            format!("Failed to cancel stale tombstone {key}: {error}")
                        })?;
                    debug!(
                        cloud_key = %tombstone_cloud_key,
                        "canceled tombstone because a live row still references its blob",
                    );
                    continue;
                }
                StoredBlobReferenceState::Unresolved => {
                    return Err(format!(
                        "tombstone {key} has a live blob reference whose locality is unresolved"
                    ));
                }
                StoredBlobReferenceState::NotLiveRemote => {}
            }

            let uploader = activated_uploaders
                .iter()
                .find(|registration| {
                    registration.reference() == tombstone.stored.locator().uploader()
                })
                .ok_or_else(|| {
                    format!(
                        "tombstone {key} names an unactivated blob uploader {}",
                        tombstone.stored.locator().uploader().device_id
                    )
                })?;
            if uploader.value().author_pubkey != self_pubkey && !is_owner {
                debug!(
                    tombstone = %key,
                    uploader = %uploader.value().author_pubkey,
                    "skipping reclaim of an object uploaded by another member",
                );
                continue;
            }

            match self.storage.blob_tombstone_exists(&tombstone.stored).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!("tombstone {key} disappeared before reclaim; skipping");
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to re-check tombstone {key} before reclaim: {error}"
                    ));
                }
            }

            let blob_present = match self.storage.verify_blob_object(&tombstone.stored).await {
                Ok(()) => true,
                Err(coven_protocol::objects::StorageError::NotFound(_)) => false,
                Err(error) => {
                    return Err(format!(
                        "Failed to check blob presence for {tombstone_cloud_key} before reclaim: {error}"
                    ));
                }
            };
            if blob_present {
                self.storage
                    .delete_blob_object(&tombstone.stored)
                    .await
                    .map_err(|error| {
                        format!(
                            "Failed to delete blob {tombstone_cloud_key} past the grace: {error}"
                        )
                    })?;
                deleted += 1;
                debug!(
                    cloud_key = %tombstone_cloud_key,
                    "reclaimed blob past the tombstone grace",
                );
            } else {
                debug!(
                    cloud_key = %tombstone_cloud_key,
                    "tombstone's blob already gone; cleaning up the leftover tombstone",
                );
            }

            self.storage
                .delete_blob_tombstone(&tombstone.stored)
                .await
                .map_err(|error| {
                    format!("Failed to delete tombstone {key} after reclaim: {error}")
                })?;
        }

        Ok(deleted)
    }

    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, coven_database::DbError> {
        self.history.drain_local_blob_cleanup().await
    }

    pub(crate) async fn persist_hlc_high_water(&self) -> Result<(), coven_database::DbError> {
        self.database.persist_hlc_high_water().await
    }
}
