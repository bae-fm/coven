use crate::database::*;
use crate::protocol::circle::CircleId;
use crate::protocol::store_commit::{
    circle_snapshot_image_semantic_prefix, circle_snapshot_slot_prefix, CircleSnapshotMeta,
    CircleSnapshotRef,
};
use rusqlite::OptionalExtension;

use super::*;

impl StoreDatabase {
    pub(crate) async fn outbound_circle_snapshot_publication(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<DurableCircleSnapshotPublication>, DbError> {
        let pending = self
            .connection
            .call(move |conn| load_outbound_circle_snapshot_on(conn, circle_id))
            .await?;
        if let Some(pending) = &pending {
            verify_snapshot_blob_spools(&pending.blobs, "prepared Circle").await?;
        }
        Ok(pending)
    }

    pub(crate) async fn latest_local_circle_snapshot(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleSnapshot>, DbError> {
        self.connection
            .call(move |conn| load_published_circle_snapshot_on(conn, circle_id))
            .await
    }

    pub(crate) async fn stage_circle_snapshot_publication(
        &self,
        meta: CircleSnapshotMeta,
        meta_prepared: PreparedExactObject,
        image_bytes: Vec<u8>,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<CircleSnapshotRef, DbError> {
        let synced_tables = self.synced_tables().to_vec();
        let gates = self.gates();
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let authority = local_store_authority_on(&tx)?;
                let registration_ref = authority.reference();
                let registration = authority.value();
                validate_snapshot_author(&meta.author_registration, registration_ref, "Circle")?;
                let device_id = registration.device_id.to_string();
                validate_snapshot_image(
                    &meta.bootstrap.image,
                    &image_prepared,
                    &image_bytes,
                    format!(
                        "{}.db",
                        circle_snapshot_image_semantic_prefix(
                            meta.circle_id,
                            &device_id,
                            meta.bootstrap.image.image_hash,
                        )
                    ),
                    "Circle",
                )?;
                let reference = CircleSnapshotRef {
                    generation: meta.generation,
                    snapshot_hash: meta.snapshot_hash(),
                    object: meta_prepared.reference().clone(),
                };
                let verified = CircleSnapshotMeta::parse_at(
                    &meta.to_bytes(),
                    registration.store_root.store_root_hash,
                    &reference,
                    registration,
                )
                .map_err(|error| {
                    DbError::context("verify staged Circle snapshot metadata", error)
                })?;
                if verified != meta {
                    return Err(DbError::Message(
                        "staged Circle snapshot changed during exact verification".to_string(),
                    ));
                }
                let previous = load_published_circle_snapshot_on(&tx, meta.circle_id)?;
                let (expected_generation, expected_slot) = match &previous {
                    Some(previous) => (
                        previous
                            .reference
                            .generation
                            .checked_add(1)
                            .ok_or_else(|| {
                                DbError::Message("Circle snapshot generation overflow".to_string())
                            })?,
                        previous.successor_slot.clone(),
                    ),
                    None => (
                        0,
                        crate::protocol::objects::ObjectSlot::logical(format!(
                            "{}.json",
                            circle_snapshot_slot_prefix(meta.circle_id, &device_id, 0)
                        ))
                        .map_err(|error| DbError::Message(error.to_string()))?,
                    ),
                };
                if meta.generation != expected_generation
                    || meta_prepared.reference().slot() != &expected_slot
                    || meta.successor.predecessor
                        != previous.as_ref().map(|value| value.reference.clone())
                {
                    return Err(DbError::Message(
                        "Circle snapshot does not extend the exact local stream".to_string(),
                    ));
                }
                let next_generation = meta.generation.checked_add(1).ok_or_else(|| {
                    DbError::Message("Circle snapshot generation overflow".to_string())
                })?;
                let activation = crate::protocol::store_commit::circle_snapshot_stream_activation(
                    registration.store_root.store_root_hash,
                    registration_ref,
                    meta.circle_id,
                    &device_id,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if meta.successor.activation != activation
                    || meta.successor.next_slot.logical_key()
                        != format!(
                            "{}.json",
                            circle_snapshot_slot_prefix(
                                meta.circle_id,
                                &device_id,
                                next_generation
                            )
                        )
                {
                    return Err(DbError::Message(
                        "Circle snapshot successor is outside its activated exact stream"
                            .to_string(),
                    ));
                }
                let snapshot_owner = crate::protocol::remote_object::SnapshotObjectOwner {
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
                    "INSERT INTO outbound_circle_snapshot \
                 (circle_id, snapshot_ref, meta_prepared, image_ref, image_prepared, \
                  image_bytes, meta_bytes, blobs) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        meta.circle_id.to_string(),
                        serde_json::to_string(&reference).map_err(|error| DbError::Message(
                            format!("serialize exact Circle snapshot ref: {error}")
                        ))?,
                        serde_json::to_string(&meta_prepared).map_err(|error| DbError::Message(
                            format!("serialize prepared Circle snapshot metadata: {error}")
                        ))?,
                        serde_json::to_string(&meta.bootstrap.image).map_err(|error| {
                            DbError::context("serialize exact Circle snapshot image ref", error)
                        })?,
                        serde_json::to_string(&image_prepared).map_err(
                            |error| DbError::context(
                                "serialize prepared Circle snapshot image",
                                error
                            )
                        )?,
                        image_bytes,
                        meta.to_bytes(),
                        serde_json::to_string(&blobs).map_err(|error| DbError::Message(
                            format!("serialize prepared Circle snapshot blobs: {error}")
                        ))?,
                    ],
                )
                .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)?;
                Ok(reference)
            })
            .await
    }

    pub(crate) async fn complete_circle_snapshot_publication(
        &self,
        accepted: CircleSnapshotRef,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let circle_id = {
                    let bytes: Vec<u8> = tx
                        .query_row(
                            "SELECT meta_bytes FROM outbound_circle_snapshot \
                             WHERE snapshot_ref = ?1",
                            [serde_json::to_string(&accepted).map_err(|error| {
                                DbError::context("serialize accepted Circle snapshot ref", error)
                            })?],
                            |row| row.get::<_, Vec<u8>>(0),
                        )
                        .optional()
                        .map_err(DbError::from)?
                        .ok_or_else(|| {
                            DbError::Message("outbound Circle snapshot is absent".to_string())
                        })?;
                    let meta: CircleSnapshotMeta =
                        serde_json::from_slice(&bytes).map_err(|error| {
                            DbError::context("accepted Circle snapshot metadata", error)
                        })?;
                    meta.circle_id
                };
                let outbound =
                    load_outbound_circle_snapshot_on(&tx, circle_id)?.ok_or_else(|| {
                        DbError::Message("outbound Circle snapshot is absent".to_string())
                    })?;
                if outbound.reference != accepted {
                    return Err(DbError::Message(
                        "accepted Circle snapshot differs from the prepared exact object"
                            .to_string(),
                    ));
                }
                install_snapshot_blob_plans_on(&tx, &outbound.blobs)?;
                let snapshot_owner = crate::protocol::remote_object::SnapshotObjectOwner {
                    activation: outbound.meta.value.successor.activation,
                    generation: outbound.meta.value.generation,
                };
                persist_snapshot_image_on(
                    &tx,
                    &outbound.meta.value.bootstrap.image,
                    snapshot_owner,
                    "Circle snapshot image",
                )?;
                let deleted = tx
                    .execute(
                        "DELETE FROM outbound_circle_snapshot WHERE circle_id = ?1",
                        [circle_id.to_string()],
                    )
                    .map_err(DbError::from)?;
                if deleted != 1 {
                    return Err(DbError::Message(
                        "outbound Circle snapshot ownership row is absent or changed".to_string(),
                    ));
                }
                let accepted_generation =
                    snapshot_generation_as_i64(accepted.generation, "Circle snapshot")?;
                tx.execute(
                    "INSERT INTO published_circle_snapshot \
                 (circle_id, generation, snapshot_ref, successor_slot, cut, meta_bytes) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        circle_id.to_string(),
                        accepted_generation,
                        serde_json::to_string(&accepted).map_err(|error| DbError::Message(
                            format!("serialize published Circle snapshot ref: {error}")
                        ))?,
                        serde_json::to_string(&outbound.meta.value.successor.next_slot).map_err(
                            |error| DbError::context(
                                "serialize Circle snapshot successor slot",
                                error
                            )
                        )?,
                        serde_json::to_string(&outbound.meta.value.bootstrap.coverage).map_err(
                            |error| DbError::context("serialize Circle snapshot cut", error)
                        )?,
                        outbound.meta.bytes,
                    ],
                )
                .map_err(DbError::from)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}
