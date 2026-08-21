use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::*;
use coven_protocol::store_commit::{
    snapshot_image_semantic_prefix, snapshot_slot_prefix, SnapshotMeta, StoreSnapshotRef,
};

use super::*;

impl StoreSession<'_> {
    fn outbound_snapshot_publication(
        &mut self,
    ) -> Result<Option<DurableSnapshotPublication>, DbError> {
        let authority = self.local_store_authority()?;
        load_outbound_store_snapshot_on(self.conn, self.store_dir, &authority)
    }

    fn stage_snapshot_publication(
        &mut self,
        meta: SnapshotMeta,
        meta_prepared: PreparedExactObject,
        rollup_bytes: Vec<u8>,
        rollup_prepared: PreparedExactObject,
        image: SnapshotDatabaseImage,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<StoreSnapshotRef, DbError> {
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let image_facts =
            crate::payload_store::write_payload_file_blocking(&tx, self.store_dir, image.path())
                .map_err(|source| SnapshotImageError::ProjectionPayloadStore {
                    operation: "spool Store snapshot image".to_string(),
                    source,
                });
        let (image_hash, _) = image.finish(image_facts).map_err(snapshot_image_db_error)?;
        let image_prepared_hash = crate::payload_store::write_payload_blocking(
            &tx,
            self.store_dir,
            image_prepared.stored_bytes(),
        )
        .map_err(|error| DbError::context("spool prepared Store snapshot image", error))?;
        let image_prepared_size = image_prepared.stored_bytes().len() as u64;
        let registration_ref = authority.reference();
        let registration = authority.value();
        validate_snapshot_author(&meta.author_registration, registration_ref, "Store")?;
        validate_snapshot_image(
            &meta.image,
            &image_prepared,
            image_hash,
            image_prepared_hash,
            image_prepared_size,
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
        .map_err(|error| DbError::context("verify staged Store snapshot metadata", error))?;
        if verified != meta {
            return Err(DbError::Message(
                "staged Store snapshot changed during exact verification".to_string(),
            ));
        }
        coven_protocol::store_commit::MembershipRollup::parse_at(
            &rollup_bytes,
            registration.store_root.store_root_hash,
            &meta.membership_rollup,
            registration,
        )
        .map_err(|error| DbError::context("verify staged membership rollup", error))?;
        if rollup_prepared.reference() != &meta.membership_rollup.object {
            return Err(DbError::Message(
                "staged membership rollup differs from the snapshot that names it".to_string(),
            ));
        }
        // Spooled beside the image rather than carried in the row: a rollup
        // holds every membership object the Store has, which is KB-class and
        // belongs in the payload store.
        let rollup_hash =
            crate::payload_store::write_payload_blocking(&tx, self.store_dir, &rollup_bytes)
                .map_err(|error| DbError::context("spool membership rollup", error))?;
        let rollup_prepared_hash = crate::payload_store::write_payload_blocking(
            &tx,
            self.store_dir,
            rollup_prepared.stored_bytes(),
        )
        .map_err(|error| DbError::context("spool prepared membership rollup", error))?;
        if rollup_hash != meta.membership_rollup.rollup_hash {
            return Err(DbError::Message(
                "staged membership rollup bytes differ from the hash the snapshot names"
                    .to_string(),
            ));
        }
        let previous = load_published_store_snapshot_on(&tx, &authority)?;
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
            || meta.successor.predecessor != previous.as_ref().map(|value| value.reference.clone())
        {
            return Err(DbError::Message(
                "Store snapshot does not extend the exact local stream".to_string(),
            ));
        }
        let next_generation = meta
            .generation
            .checked_add(1)
            .ok_or_else(|| DbError::Message("Store snapshot generation overflow".to_string()))?;
        if meta.successor.activation
            != registration
                .store_snapshot_activation(registration_ref)
                .map_err(DbError::from)?
                .activation_id()
            || meta.successor.next_slot.logical_key()
                != format!(
                    "{}.json",
                    snapshot_slot_prefix(&registration.device_id.to_string(), next_generation)
                )
        {
            return Err(DbError::Message(
                "Store snapshot successor is outside its activated exact stream".to_string(),
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
            "INSERT INTO outbound_store_snapshot \
             (singleton, snapshot_ref, meta_prepared, image_ref, rollup_ref, \
              meta_bytes, blobs) \
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                serde_json::to_string(&reference).map_err(|error| {
                    DbError::context("serialize exact Store snapshot ref", error)
                })?,
                serde_json::to_string(&meta_prepared).map_err(|error| {
                    DbError::context("serialize prepared Store snapshot metadata", error)
                })?,
                serde_json::to_string(&meta.image).map_err(|error| {
                    DbError::context("serialize exact Store snapshot image ref", error)
                })?,
                serde_json::to_string(&meta.membership_rollup).map_err(|error| {
                    DbError::context("serialize exact membership rollup ref", error)
                })?,
                meta.to_bytes(),
                serde_json::to_string(&blobs).map_err(|error| {
                    DbError::context("serialize prepared Store snapshot blobs", error)
                })?,
            ],
        )
        .map_err(DbError::from)?;
        crate::payload_store::set_payload_owner_claims_on(
            &tx,
            crate::payload_store::OUTBOUND_STORE_SNAPSHOT_OWNER_KEY,
            &BTreeSet::from([
                image_hash,
                image_prepared_hash,
                rollup_hash,
                rollup_prepared_hash,
            ]),
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(reference)
    }

    fn latest_local_store_snapshot(&mut self) -> Result<Option<PublishedStoreSnapshot>, DbError> {
        let authority = self.local_store_authority()?;
        load_published_store_snapshot_on(self.conn, &authority)
    }

    fn local_store_snapshots(&mut self) -> Result<Vec<PublishedStoreSnapshot>, DbError> {
        let authority = self.local_store_authority()?;
        load_published_store_snapshots_on(self.conn, &authority)
    }

    fn complete_snapshot_publication(&mut self, accepted: StoreSnapshotRef) -> Result<(), DbError> {
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let outbound = load_outbound_store_snapshot_on(&tx, self.store_dir, &authority)?
            .ok_or_else(|| DbError::Message("outbound Store snapshot is absent".to_string()))?;
        if outbound.reference != accepted {
            return Err(DbError::Message(
                "accepted Store snapshot differs from the prepared exact object".to_string(),
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
            &outbound.meta.value.image,
            snapshot_owner.clone(),
            "Store snapshot image",
        )?;
        crate::snapshot_objects::persist_membership_rollup_on(
            &tx,
            self.store_dir,
            &outbound.meta.value.membership_rollup,
            snapshot_owner,
            "Store membership rollup",
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
        crate::payload_store::release_payload_owner_on(
            &tx,
            crate::payload_store::OUTBOUND_STORE_SNAPSHOT_OWNER_KEY,
        )?;
        let accepted_generation =
            snapshot_generation_as_i64(accepted.generation, "Store snapshot")?;
        tx.execute(
            "INSERT INTO published_store_snapshot \
             (generation, snapshot_ref, successor_slot, meta_bytes) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                accepted_generation,
                serde_json::to_string(&accepted).map_err(|error| {
                    DbError::context("serialize published Store snapshot ref", error)
                })?,
                serde_json::to_string(&outbound.meta.value.successor.next_slot).map_err(
                    |error| DbError::context("serialize Store snapshot successor slot", error)
                )?,
                outbound.meta.bytes,
            ],
        )
        .map_err(DbError::from)?;
        tx.commit().map_err(DbError::from)
    }

    fn snapshot_blob_spool_cleanup_paths(&self) -> Result<Vec<PathBuf>, DbError> {
        let mut statement = self
            .conn
            .prepare("SELECT path FROM snapshot_blob_spool_cleanup ORDER BY path")
            .map_err(DbError::from)?;
        let paths = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .map(|row| row.map(PathBuf::from).map_err(DbError::from))
            .collect();
        paths
    }

    fn complete_snapshot_blob_spool_cleanup(&self, path: &str) -> Result<(), DbError> {
        let deleted = self
            .conn
            .execute(
                "DELETE FROM snapshot_blob_spool_cleanup WHERE path = ?1",
                [path],
            )
            .map_err(DbError::from)?;
        if deleted != 1 {
            return Err(DbError::Message(
                "snapshot blob spool cleanup ownership is absent".to_string(),
            ));
        }
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn outbound_snapshot_publication(
        &self,
    ) -> Result<Option<DurableSnapshotPublication>, DbError> {
        let pending = self
            .call_store(|session| session.outbound_snapshot_publication())
            .await?;
        if let Some(pending) = &pending {
            verify_snapshot_blob_spools(&pending.blobs, "prepared").await?;
        }
        Ok(pending)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn stage_snapshot_publication(
        &self,
        meta: SnapshotMeta,
        meta_prepared: PreparedExactObject,
        rollup_bytes: Vec<u8>,
        rollup_prepared: PreparedExactObject,
        image: SnapshotDatabaseImage,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<StoreSnapshotRef, DbError> {
        self.call_store(move |session| {
            session.stage_snapshot_publication(
                meta,
                meta_prepared,
                rollup_bytes,
                rollup_prepared,
                image,
                image_prepared,
                blobs,
            )
        })
        .await
    }

    pub async fn latest_local_store_snapshot(
        &self,
    ) -> Result<Option<PublishedStoreSnapshot>, DbError> {
        self.call_store(|session| session.latest_local_store_snapshot())
            .await
    }

    pub async fn local_store_snapshots(&self) -> Result<Vec<PublishedStoreSnapshot>, DbError> {
        self.call_store(|session| session.local_store_snapshots())
            .await
    }

    pub async fn complete_snapshot_publication(
        &self,
        accepted: StoreSnapshotRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_snapshot_publication(accepted))
            .await
    }

    pub async fn snapshot_blob_spool_cleanup_paths(&self) -> Result<Vec<PathBuf>, DbError> {
        self.call_store(|session| session.snapshot_blob_spool_cleanup_paths())
            .await
    }

    pub async fn complete_snapshot_blob_spool_cleanup(&self, path: &Path) -> Result<(), DbError> {
        let path = path.to_string_lossy().into_owned();
        self.call_store(move |session| session.complete_snapshot_blob_spool_cleanup(&path))
            .await
    }
}
