use std::collections::BTreeSet;

use crate::*;
use coven_protocol::circle::CircleId;
use coven_protocol::store_commit::{
    circle_snapshot_image_semantic_prefix, circle_snapshot_slot_prefix, CircleSnapshotMeta,
    CircleSnapshotRef,
};
use rusqlite::OptionalExtension;

use super::*;

impl StoreSession<'_> {
    fn outbound_circle_snapshot_publication(
        &mut self,
        circle_id: CircleId,
    ) -> Result<Option<DurableCircleSnapshotPublication>, DbError> {
        let authority = self.local_store_authority()?;
        load_outbound_circle_snapshot_on(self.conn, self.store_dir, &authority, circle_id)
    }

    fn latest_local_circle_snapshot(
        &mut self,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleSnapshot>, DbError> {
        let authority = self.local_store_authority()?;
        load_published_circle_snapshot_on(self.conn, &authority, circle_id)
    }

    fn stage_circle_snapshot_publication(
        &mut self,
        meta: CircleSnapshotMeta,
        meta_prepared: PreparedExactObject,
        image: SnapshotDatabaseImage,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<CircleSnapshotRef, DbError> {
        let authority = self.local_store_authority()?;
        let image_facts =
            crate::payload_spool::write_payload_file_blocking(self.store_dir, image.path())
                .map_err(|error| {
                    SnapshotImageError::Projection(format!("spool Circle snapshot image: {error}"))
                });
        let (image_hash, _) = image.finish(image_facts).map_err(snapshot_image_db_error)?;
        let image_prepared_hash = crate::payload_spool::write_payload_blocking(
            self.store_dir,
            image_prepared.stored_bytes(),
        )
        .map_err(|error| DbError::context("spool prepared Circle snapshot image", error))?;
        let image_prepared_size = image_prepared.stored_bytes().len() as u64;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let registration_ref = authority.reference();
        let registration = authority.value();
        validate_snapshot_author(&meta.author_registration, registration_ref, "Circle")?;
        let device_id = registration.device_id.to_string();
        validate_snapshot_image(
            &meta.bootstrap.image,
            &image_prepared,
            image_hash,
            image_prepared_hash,
            image_prepared_size,
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
        .map_err(|error| DbError::context("verify staged Circle snapshot metadata", error))?;
        if verified != meta {
            return Err(DbError::Message(
                "staged Circle snapshot changed during exact verification".to_string(),
            ));
        }
        let previous = load_published_circle_snapshot_on(&tx, &authority, meta.circle_id)?;
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
                coven_protocol::objects::ObjectSlot::logical(format!(
                    "{}.json",
                    circle_snapshot_slot_prefix(meta.circle_id, &device_id, 0)
                ))
                .map_err(|error| DbError::Message(error.to_string()))?,
            ),
        };
        if meta.generation != expected_generation
            || meta_prepared.reference().slot() != &expected_slot
            || meta.successor.predecessor != previous.as_ref().map(|value| value.reference.clone())
        {
            return Err(DbError::Message(
                "Circle snapshot does not extend the exact local stream".to_string(),
            ));
        }
        let next_generation = meta
            .generation
            .checked_add(1)
            .ok_or_else(|| DbError::Message("Circle snapshot generation overflow".to_string()))?;
        let activation = coven_protocol::store_commit::circle_snapshot_stream_activation(
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
                    circle_snapshot_slot_prefix(meta.circle_id, &device_id, next_generation)
                )
        {
            return Err(DbError::Message(
                "Circle snapshot successor is outside its activated exact stream".to_string(),
            ));
        }
        let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner {
            activation: meta.successor.activation,
            generation: meta.generation,
        };
        validate_snapshot_blob_plans_on(
            self.conn,
            self.gates,
            self.synced_tables,
            &snapshot_owner,
            &blobs,
        )?;
        tx.execute(
            "INSERT INTO outbound_circle_snapshot \
             (circle_id, snapshot_ref, meta_prepared, image_ref, meta_bytes, blobs) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                meta.circle_id.to_string(),
                serde_json::to_string(&reference).map_err(|error| DbError::Message(format!(
                    "serialize exact Circle snapshot ref: {error}"
                )))?,
                serde_json::to_string(&meta_prepared).map_err(|error| DbError::Message(
                    format!("serialize prepared Circle snapshot metadata: {error}")
                ))?,
                serde_json::to_string(&meta.bootstrap.image).map_err(|error| {
                    DbError::context("serialize exact Circle snapshot image ref", error)
                })?,
                meta.to_bytes(),
                serde_json::to_string(&blobs).map_err(|error| DbError::Message(format!(
                    "serialize prepared Circle snapshot blobs: {error}"
                )))?,
            ],
        )
        .map_err(DbError::from)?;
        crate::payload_spool::set_payload_owner_claims_on(
            &tx,
            &crate::payload_spool::outbound_circle_snapshot_owner_key(meta.circle_id),
            &BTreeSet::from([image_hash, image_prepared_hash]),
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(reference)
    }

    fn complete_circle_snapshot_publication(
        &mut self,
        accepted: CircleSnapshotRef,
    ) -> Result<(), DbError> {
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
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
            let meta: CircleSnapshotMeta = serde_json::from_slice(&bytes)
                .map_err(|error| DbError::context("accepted Circle snapshot metadata", error))?;
            meta.circle_id
        };
        let outbound =
            load_outbound_circle_snapshot_on(&tx, self.store_dir, &authority, circle_id)?
                .ok_or_else(|| {
                    DbError::Message("outbound Circle snapshot is absent".to_string())
                })?;
        if outbound.reference != accepted {
            return Err(DbError::Message(
                "accepted Circle snapshot differs from the prepared exact object".to_string(),
            ));
        }
        install_snapshot_blob_plans_on(&tx, &outbound.blobs)?;
        let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner {
            activation: outbound.meta.value.successor.activation,
            generation: outbound.meta.value.generation,
        };
        persist_snapshot_image_on(
            &tx,
            self.store_dir,
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
        crate::payload_spool::release_payload_owner_on(
            &tx,
            &crate::payload_spool::outbound_circle_snapshot_owner_key(circle_id),
        )?;
        let accepted_generation =
            snapshot_generation_as_i64(accepted.generation, "Circle snapshot")?;
        tx.execute(
            "INSERT INTO published_circle_snapshot \
             (circle_id, generation, snapshot_ref, successor_slot, cut, meta_bytes) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                circle_id.to_string(),
                accepted_generation,
                serde_json::to_string(&accepted).map_err(|error| DbError::Message(format!(
                    "serialize published Circle snapshot ref: {error}"
                )))?,
                serde_json::to_string(&outbound.meta.value.successor.next_slot).map_err(
                    |error| DbError::context("serialize Circle snapshot successor slot", error)
                )?,
                serde_json::to_string(&outbound.meta.value.bootstrap.coverage)
                    .map_err(|error| DbError::context("serialize Circle snapshot cut", error))?,
                outbound.meta.bytes,
            ],
        )
        .map_err(DbError::from)?;
        tx.commit().map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn outbound_circle_snapshot_publication(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<DurableCircleSnapshotPublication>, DbError> {
        let pending = self
            .call_store(move |session| session.outbound_circle_snapshot_publication(circle_id))
            .await?;
        if let Some(pending) = &pending {
            verify_snapshot_blob_spools(&pending.blobs, "prepared Circle").await?;
        }
        Ok(pending)
    }

    pub async fn latest_local_circle_snapshot(
        &self,
        circle_id: CircleId,
    ) -> Result<Option<PublishedCircleSnapshot>, DbError> {
        self.call_store(move |session| session.latest_local_circle_snapshot(circle_id))
            .await
    }

    pub async fn stage_circle_snapshot_publication(
        &self,
        meta: CircleSnapshotMeta,
        meta_prepared: PreparedExactObject,
        image: SnapshotDatabaseImage,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<CircleSnapshotRef, DbError> {
        self.call_store(move |session| {
            session.stage_circle_snapshot_publication(
                meta,
                meta_prepared,
                image,
                image_prepared,
                blobs,
            )
        })
        .await
    }

    pub async fn complete_circle_snapshot_publication(
        &self,
        accepted: CircleSnapshotRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_circle_snapshot_publication(accepted))
            .await
    }
}
