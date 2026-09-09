use super::verified_store_authority::VerifiedRegistrationLookup;
use super::*;
use coven_protocol::objects::{ExactObjectRef, ObjectSlot};
use coven_protocol::remote_object::{remote_object_id, SnapshotObjectOwner};
use coven_protocol::store_commit::{AcceptedStoreSnapshotRef, SnapshotMeta, StoreRootRef};
use std::collections::BTreeSet;

impl StoreTransaction<'_, '_> {
    /// The accepted successor owns every unfinished physical deletion. Release
    /// local leases in the same transaction as adoption, before any remote I/O.
    pub(super) fn retire_snapshot_artifact_ownership(
        self,
        lookup: &mut dyn VerifiedRegistrationLookup,
        root: &StoreRootRef,
        accepted: &AcceptedStoreSnapshotRef,
        metadata: &SnapshotMeta,
    ) -> Result<BTreeSet<ObjectSlot>, DbError> {
        if metadata.store_root_hash != root.store_root_hash
            || accepted.publication.store_root_hash != root.store_root_hash
            || metadata.snapshot_hash() != accepted.snapshot.snapshot_hash
            || metadata.publication_predecessor.next_position()? != accepted.publication.position
        {
            return Err(DbError::Message(
                "snapshot retirement differs from its accepted boundary".into(),
            ));
        }
        metadata
            .history_summary
            .reclaim
            .validate_before(&accepted.publication)?;
        let protected = metadata
            .history_summary
            .pending_device_join_snapshot_slots();
        let mut superseded = metadata
            .history_summary
            .reclaim
            .snapshots
            .values()
            .map(|snapshot| snapshot.accepted.snapshot.object.slot().clone())
            .filter(|slot| !protected.contains(slot))
            .collect::<BTreeSet<_>>();
        for old in StoreRecords::new(self.transaction, self.store_dir)
            .published_store_snapshots(root, lookup)?
        {
            let position = old.meta.publication_predecessor.next_position()?;
            if position >= accepted.publication.position
                || protected.contains(old.reference.object.slot())
            {
                continue;
            }
            superseded.insert(old.reference.object.slot().clone());
            let owner = SnapshotObjectOwner::Store {
                metadata_slot: old.reference.object.slot().clone(),
            };
            for (object, image) in [
                (&old.meta.image.object, true),
                (&old.meta.membership_rollup.object, false),
            ] {
                let object_id = remote_object_id(object);
                let exists: bool = self.transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                    [object_id.to_string()],
                    |row| row.get(0),
                )?;
                if !exists {
                    continue;
                }
                let remote = crate::remote_object_records::load_remote_object_on(
                    self.transaction,
                    object_id,
                )?;
                if image {
                    remote.validate_reclaimable_snapshot_image(&old.meta.image, &owner)
                } else {
                    remote
                        .validate_reclaimable_membership_rollup(&old.meta.membership_rollup, &owner)
                }
                .map_err(|error| {
                    DbError::context("retire superseded snapshot artifact lease", error)
                })?;
                if !crate::remote_object_records::delete_remote_object_on(
                    self.transaction,
                    object_id,
                )? {
                    return Err(DbError::Message(
                        "snapshot artifact lease disappeared during retirement".into(),
                    ));
                }
            }
            self.transaction.execute(
                "DELETE FROM published_store_snapshot WHERE publication_position = ?1 AND snapshot_ref = ?2",
                rusqlite::params![
                    i64::try_from(position.get()).map_err(|error| DbError::context("retired snapshot position", error))?,
                    serde_json::to_string(&old.reference).map_err(|error| DbError::context("retired snapshot reference", error))?,
                ],
            )?;
        }
        Ok(superseded)
    }

    pub(super) fn retire_store_blob_snapshot_ownership(
        self,
        metadata_slot: &ObjectSlot,
        superseded: &BTreeSet<ObjectSlot>,
    ) -> Result<(), DbError> {
        let mut statement = self
            .transaction
            .prepare("SELECT remote_object_id FROM blob_locators ORDER BY remote_object_id")?;
        let blobs = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for blob in blobs {
            let object_id = blob
                .parse()
                .map_err(|error| DbError::context("snapshot blob object id", error))?;
            let mut remote =
                crate::remote_object_records::load_remote_object_on(self.transaction, object_id)?;
            if !remote.is_activated_stored_blob()
                || !remote.snapshot_owners().any(|owner| {
                    matches!(owner, SnapshotObjectOwner::Store { metadata_slot }
                    if superseded.contains(metadata_slot))
                })
            {
                continue;
            }
            remote
                .retire_superseded_store_snapshot_ownership(metadata_slot, superseded)
                .map_err(|error| {
                    DbError::context("retire accepted snapshot blob ownership", error)
                })?;
            crate::remote_object_records::update_remote_object_on(
                self.transaction,
                object_id,
                &remote,
            )?;
        }
        Ok(())
    }
}

impl StoreDatabase {
    pub async fn verify_snapshot_artifacts_released(
        &self,
        objects: Vec<ExactObjectRef>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            for object in objects {
                let id = remote_object_id(&object);
                let exists: bool = session.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                    [id.to_string()],
                    |row| row.get(0),
                )?;
                if exists {
                    crate::remote_object_records::load_remote_object_on(session.conn, id)?;
                    return Err(DbError::Message(
                        "accepted snapshot artifact retains a live local owner".into(),
                    ));
                }
            }
            Ok(())
        })
        .await
    }
}
