use std::path::{Path, PathBuf};

use crate::*;
use coven_protocol::store_commit::{
    snapshot_image_semantic_prefix, snapshot_slot_prefix, SnapshotMeta, StoreSnapshotRef,
};

use super::*;

impl StoreDatabase {
    pub async fn outbound_snapshot_publication(
        &self,
    ) -> Result<Option<DurableSnapshotPublication>, DbError> {
        let pending = self
            .connection
            .call(load_outbound_store_snapshot_on)
            .await?;
        if let Some(pending) = &pending {
            verify_snapshot_blob_spools(&pending.blobs, "prepared").await?;
        }
        Ok(pending)
    }

    pub async fn stage_snapshot_publication(
        &self,
        meta: SnapshotMeta,
        meta_prepared: PreparedExactObject,
        image_bytes: Vec<u8>,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<StoreSnapshotRef, DbError> {
        let synced_tables = self.synced_tables().to_vec();
        let gates = self.gates();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let authority = local_store_authority_on(&tx)?;
                let registration_ref = authority.reference();
                let registration = authority.value();
                validate_snapshot_author(&meta.author_registration, registration_ref, "Store")?;
                validate_snapshot_image(
                    &meta.image,
                    &image_prepared,
                    &image_bytes,
                    format!(
                        "{}.db",
                        snapshot_image_semantic_prefix(
                            &registration.device_id.to_string(),
                            meta.image.image_hash,
                        )
                    ),
                    "Store",
                )?;
                let reference = StoreSnapshotRef {
                    generation: meta.generation,
                    snapshot_hash: meta.snapshot_hash(),
                    object: meta_prepared.reference().clone(),
                };
                let verified = SnapshotMeta::parse_at(
                    &meta.to_bytes(),
                    registration.store_root.store_root_hash,
                    &reference,
                    registration,
                )
                .map_err(|error| {
                    DbError::context("verify staged Store snapshot metadata", error)
                })?;
                if verified != meta {
                    return Err(DbError::Message(
                        "staged Store snapshot changed during exact verification".to_string(),
                    ));
                }
                let previous = load_published_store_snapshot_on(&tx)?;
                let (expected_generation, expected_predecessor, expected_slot) = match &previous {
                    Some(previous) => (
                        previous
                            .reference
                            .generation
                            .checked_add(1)
                            .ok_or_else(|| {
                                DbError::Message("Store snapshot generation overflow".to_string())
                            })?,
                        Some(previous.reference.clone()),
                        previous.successor_slot.clone(),
                    ),
                    None => (0, None, store_snapshot_first_slot(registration)?.clone()),
                };
                if meta.generation != expected_generation
                    || meta.predecessor != expected_predecessor
                    || meta_prepared.reference().slot() != &expected_slot
                    || meta.successor.predecessor
                        != previous.as_ref().map(|value| value.reference.clone())
                {
                    return Err(DbError::Message(
                        "Store snapshot does not extend the exact local stream".to_string(),
                    ));
                }
                let next_generation = meta.generation.checked_add(1).ok_or_else(|| {
                    DbError::Message("Store snapshot generation overflow".to_string())
                })?;
                if meta.successor.activation
                    != registration
                        .store_snapshot_activation(registration_ref)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        .activation_id()
                    || meta.successor.next_slot.logical_key()
                        != format!(
                            "{}.json",
                            snapshot_slot_prefix(
                                &registration.device_id.to_string(),
                                next_generation
                            )
                        )
                {
                    return Err(DbError::Message(
                        "Store snapshot successor is outside its activated exact stream"
                            .to_string(),
                    ));
                }
                let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner {
                    activation: meta.successor.activation,
                    generation: meta.generation,
                };
                validate_snapshot_blob_plans_on(
                    conn,
                    &gates,
                    &synced_tables,
                    &snapshot_owner,
                    &blobs,
                )?;
                tx.execute(
                    "INSERT INTO outbound_store_snapshot \
                 (singleton, snapshot_ref, meta_prepared, image_ref, image_prepared, \
                  image_bytes, meta_bytes, blobs) \
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        serde_json::to_string(&reference).map_err(|error| DbError::Message(
                            format!("serialize exact Store snapshot ref: {error}")
                        ))?,
                        serde_json::to_string(&meta_prepared).map_err(|error| DbError::Message(
                            format!("serialize prepared Store snapshot metadata: {error}")
                        ))?,
                        serde_json::to_string(&meta.image).map_err(|error| DbError::Message(
                            format!("serialize exact Store snapshot image ref: {error}")
                        ))?,
                        serde_json::to_string(&image_prepared).map_err(
                            |error| DbError::context(
                                "serialize prepared Store snapshot image",
                                error
                            )
                        )?,
                        image_bytes,
                        meta.to_bytes(),
                        serde_json::to_string(&blobs).map_err(|error| DbError::Message(
                            format!("serialize prepared Store snapshot blobs: {error}")
                        ))?,
                    ],
                )
                .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)?;
                Ok(reference)
            })
            .await
    }

    pub async fn latest_local_store_snapshot(
        &self,
    ) -> Result<Option<PublishedStoreSnapshot>, DbError> {
        self.connection.call(load_published_store_snapshot_on).await
    }

    pub async fn complete_snapshot_publication(
        &self,
        accepted: StoreSnapshotRef,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let outbound = load_outbound_store_snapshot_on(&tx)?.ok_or_else(|| {
                    DbError::Message("outbound Store snapshot is absent".to_string())
                })?;
                if outbound.reference != accepted {
                    return Err(DbError::Message(
                        "accepted Store snapshot differs from the prepared exact object"
                            .to_string(),
                    ));
                }
                install_snapshot_blob_plans_on(&tx, &outbound.blobs)?;
                let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner {
                    activation: outbound.meta.value.successor.activation,
                    generation: outbound.meta.value.generation,
                };
                persist_snapshot_image_on(
                    &tx,
                    &outbound.meta.value.image,
                    snapshot_owner,
                    "Store snapshot image",
                )?;
                let deleted = tx
                    .execute(
                        "DELETE FROM outbound_store_snapshot \
                     WHERE singleton = 1 AND snapshot_ref = ?1",
                        [serde_json::to_string(&accepted).map_err(|error| {
                            DbError::context("serialize accepted Store snapshot ref", error)
                        })?],
                    )
                    .map_err(DbError::from)?;
                if deleted != 1 {
                    return Err(DbError::Message(
                        "outbound snapshot ownership row is absent or changed".to_string(),
                    ));
                }
                let accepted_generation =
                    snapshot_generation_as_i64(accepted.generation, "Store snapshot")?;
                tx.execute(
                    "INSERT INTO published_store_snapshot \
                 (generation, snapshot_ref, successor_slot, meta_bytes) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        accepted_generation,
                        serde_json::to_string(&accepted).map_err(|error| DbError::Message(
                            format!("serialize published Store snapshot ref: {error}")
                        ))?,
                        serde_json::to_string(&outbound.meta.value.successor.next_slot).map_err(
                            |error| DbError::context(
                                "serialize Store snapshot successor slot",
                                error
                            )
                        )?,
                        outbound.meta.bytes,
                    ],
                )
                .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn snapshot_blob_spool_cleanup_paths(&self) -> Result<Vec<PathBuf>, DbError> {
        self.connection
            .call(|conn| {
                let mut statement = conn
                    .prepare("SELECT path FROM snapshot_blob_spool_cleanup ORDER BY path")
                    .map_err(DbError::from)?;
                let paths = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(DbError::from)?
                    .map(|row| row.map(PathBuf::from).map_err(DbError::from))
                    .collect();
                paths
            })
            .await
    }

    pub async fn complete_snapshot_blob_spool_cleanup(&self, path: &Path) -> Result<(), DbError> {
        let path = path.to_string_lossy().into_owned();
        self.connection
            .call(move |conn| {
                let deleted = conn
                    .execute(
                        "DELETE FROM snapshot_blob_spool_cleanup WHERE path = ?1",
                        [&path],
                    )
                    .map_err(DbError::from)?;
                if deleted != 1 {
                    return Err(DbError::Message(
                        "snapshot blob spool cleanup ownership is absent".to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }
}
