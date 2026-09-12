use std::collections::BTreeSet;

use crate::*;
use coven_protocol::store_commit::{
    snapshot_image_semantic_prefix, SnapshotMeta, StoreSnapshotRef,
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
        stage: StoreSnapshotPublicationStage,
        meta: SnapshotMeta,
        meta_prepared: PreparedExactObject,
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
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
            crate::payload_store::CreatedPayloadFiles::untracked(),
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
                    meta_prepared.reference().slot(),
                    meta.image.image_hash,
                )
            ),
            "Store",
        )?;
        let reference = StoreSnapshotRef {
            snapshot_hash: meta.snapshot_hash(),
            object: meta_prepared.reference().clone(),
        };
        SnapshotMeta::parse_at(
            &meta.to_bytes(),
            registration.store_root.store_root_hash,
            &reference,
            registration,
        )
        .map_err(|error| DbError::context("verify staged Store snapshot metadata", error))?;
        publication
            .validate_snapshot_shape(&meta, &reference)
            .map_err(|error| DbError::context("verify staged Store snapshot publication", error))?;
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
        let rollup_hash = crate::payload_store::write_payload_blocking(
            &tx,
            self.store_dir,
            &rollup_bytes,
            crate::payload_store::CreatedPayloadFiles::untracked(),
        )
        .map_err(|error| DbError::context("spool membership rollup", error))?;
        let rollup_prepared_hash = crate::payload_store::write_payload_blocking(
            &tx,
            self.store_dir,
            rollup_prepared.stored_bytes(),
            crate::payload_store::CreatedPayloadFiles::untracked(),
        )
        .map_err(|error| DbError::context("spool prepared membership rollup", error))?;
        if rollup_hash != meta.membership_rollup.rollup_hash {
            return Err(DbError::Message(
                "staged membership rollup bytes differ from the hash the snapshot names"
                    .to_string(),
            ));
        }
        let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
            metadata_slot: reference.object.slot().clone(),
        };
        // The publication names this immutable image. Current rows may already
        // contain unpublished replacements, deletions, or audience changes.
        let image_bytes =
            crate::payload_store::read_verified_payload_blocking(&tx, self.store_dir, image_hash)
                .map_err(|error| DbError::context("read staged Store snapshot image", error))?;
        let mut captured = Connection::open_in_memory()?;
        crate::connection_io::deserialize_database_image_into(&mut captured, &image_bytes)?;
        let captured_gates = Gates::from_tables(&captured, self.synced_tables)?;
        validate_snapshot_blob_plans_on(
            &captured,
            &captured_gates,
            self.synced_tables,
            &snapshot_owner,
            &blobs,
        )?;
        drop(captured);
        drop(image_bytes);
        let mut active_publication = ActiveStorePublication::snapshot(publication)?;
        match stage {
            StoreSnapshotPublicationStage::Initial => {
                match super::active_store_publication::claim_active_store_publication_on(
                    &tx,
                    &active_publication,
                )? {
                    super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
                    super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                        return Err(DbError::Message(
                            "Store snapshot already owns publication before its journal"
                                .to_string(),
                        ));
                    }
                    super::active_store_publication::ActiveStorePublicationClaim::Occupied(
                        owner,
                    ) => {
                        return Err(DbError::Message(format!(
                            "another local Store operation owns publication: {owner:?}"
                        )));
                    }
                }
            }
            StoreSnapshotPublicationStage::Replacing { previous, accepted } => {
                let old = load_outbound_store_snapshot_on(&tx, self.store_dir, &authority)?
                    .ok_or_else(|| {
                        DbError::Message("replacement snapshot has no pending candidate".into())
                    })?;
                if old.reference != previous {
                    return Err(DbError::Message(
                        "snapshot candidate changed before replacement".into(),
                    ));
                }
                let active =
                    super::active_store_publication::load_active_store_publication_on(&tx)?
                        .ok_or_else(|| {
                            DbError::Message("snapshot replacement has no active owner".into())
                        })?;
                if active.owner() != &ActiveStorePublicationOwner::Snapshot
                    || active.attempt()? != &old.publication
                    || !active.retired_snapshot_objects().is_empty()
                {
                    return Err(DbError::Message(
                        "snapshot replacement differs from its active owner".into(),
                    ));
                }
                let observed =
                    super::observed_store_publication::load_store_current_publication_on(&tx)?;
                if observed.record() != accepted.interval().current()
                    || observed.observed_version() != accepted.current_version()
                    || active_publication.attempt()?.previous != *observed.record()
                    || Some(&active_publication.attempt()?.previous_version)
                        != observed.observed_version()
                {
                    return Err(DbError::Message(
                        "snapshot replacement does not extend its installed winner".into(),
                    ));
                }
                let old_entry = old.publication.reference()?;
                let winner = accepted
                    .interval()
                    .entries()
                    .iter()
                    .find(|entry| entry.reference().position == old_entry.position)
                    .ok_or_else(|| {
                        DbError::Message(
                            "snapshot replacement lacks its settled exact position".into(),
                        )
                    })?;
                if winner.entry().previous_state_hash != old.publication.previous.state_hash() {
                    return Err(DbError::Message(
                        "snapshot winner names another exact predecessor".into(),
                    ));
                }
                if winner.reference() == &old_entry || accepted.interval().entries().iter().any(|entry|
                    matches!(&entry.entry().payload, coven_protocol::store_commit::StorePublicationPayload::Snapshot(snapshot) if snapshot == &old.reference))
                {
                    return Err(DbError::Message("an accepted snapshot cannot be replaced as an unaccepted candidate".into()));
                }
                let retained = BTreeSet::from([
                    reference.object.clone(),
                    meta.image.object.clone(),
                    meta.membership_rollup.object.clone(),
                    active_publication.attempt()?.entry_object.clone(),
                ]);
                let cleanup = snapshot_candidate_cleanup_on(&tx, &old, &retained)?;
                active_publication.retain_snapshot_cleanup(cleanup)?;
                super::active_store_publication::update_active_store_publication_on(
                    &tx,
                    &active,
                    &active_publication,
                )?;
                tx.execute(
                    "DELETE FROM outbound_store_snapshot WHERE singleton = 1",
                    [],
                )?;
            }
        }
        tx.execute(
            "INSERT INTO outbound_store_snapshot \
             (singleton, snapshot_ref, meta_prepared, meta_bytes, blobs) \
             VALUES (1, ?1, ?2, ?3, ?4)",
            rusqlite::params![
                serde_json::to_string(&reference).map_err(|error| {
                    DbError::context("serialize exact Store snapshot ref", error)
                })?,
                serde_json::to_string(&meta_prepared).map_err(|error| {
                    DbError::context("serialize prepared Store snapshot metadata", error)
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

    fn supersede_snapshot_publication(
        &mut self,
        previous: StoreSnapshotRef,
        snapshot: PublishedStoreSnapshot,
        accepted: AcceptedStorePublicationInterval,
    ) -> Result<(), DbError> {
        let baseline = self.installed_replay_baseline()?;
        if baseline.snapshot() != Some(&snapshot) {
            return Err(DbError::Message(
                "superseding snapshot is not the installed verified baseline".into(),
            ));
        }
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction()?;
        let old = load_outbound_store_snapshot_on(&tx, self.store_dir, &authority)?
            .ok_or_else(|| DbError::Message("superseded snapshot has no pending request".into()))?;
        let active = super::active_store_publication::load_active_store_publication_on(&tx)?
            .ok_or_else(|| DbError::Message("superseded snapshot has no active owner".into()))?;
        if old.reference != previous || active.attempt()? != &old.publication {
            return Err(DbError::Message(
                "snapshot request changed before supersession".into(),
            ));
        }
        let observed = super::observed_store_publication::load_store_current_publication_on(&tx)?;
        if observed.record() != accepted.interval().current()
            || observed.observed_version() != accepted.current_version()
        {
            return Err(DbError::Message(
                "superseding snapshot has another installed publication interval".into(),
            ));
        }
        let entry = accepted.interval().entries().iter().find(|entry|
            matches!(&entry.entry().payload, coven_protocol::store_commit::StorePublicationPayload::Snapshot(reference) if reference == &snapshot.reference)
        ).ok_or_else(|| DbError::Message("superseding snapshot is absent from the accepted interval".into()))?;
        if entry.reference().position <= old.publication.reference()?.position
            || entry.entry().previous_state_hash
                != snapshot.meta.publication_predecessor.state_hash()
            || !snapshot.meta.coverage.covers(&old.meta.value.coverage)
        {
            return Err(DbError::Message(
                "accepted snapshot does not supersede the requested checkpoint".into(),
            ));
        }
        let retained = BTreeSet::from([
            snapshot.reference.object.clone(),
            snapshot.meta.image.object.clone(),
            snapshot.meta.membership_rollup.object.clone(),
            entry.reference().object.clone(),
        ]);
        let cleanup = snapshot_candidate_cleanup_on(&tx, &old, &retained)?;
        let superseded = active.supersede_snapshot(snapshot, cleanup)?;
        super::active_store_publication::update_active_store_publication_on(
            &tx,
            &active,
            &superseded,
        )?;
        tx.commit()?;
        Ok(())
    }

    fn complete_superseded_snapshot_publication(
        &mut self,
        expected: ActiveStorePublication,
    ) -> Result<SnapshotMeta, DbError> {
        let snapshot = expected.superseding_snapshot().ok_or_else(|| {
            DbError::Message("snapshot request has no verified superseding checkpoint".into())
        })?;
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction()?;
        let pending = load_outbound_store_snapshot_on(&tx, self.store_dir, &authority)?
            .ok_or_else(|| DbError::Message("superseded snapshot request is absent".into()))?;
        if expected.attempt()? != &pending.publication {
            return Err(DbError::Message(
                "superseded snapshot differs from its original request".into(),
            ));
        }
        super::active_store_publication::clear_active_store_publication_on(&tx, &expected)?;
        tx.execute(
            "DELETE FROM outbound_store_snapshot WHERE singleton = 1",
            [],
        )?;
        crate::payload_store::release_payload_owner_on(
            &tx,
            crate::payload_store::OUTBOUND_STORE_SNAPSHOT_OWNER_KEY,
        )?;
        tx.commit()?;
        Ok(snapshot.meta.clone())
    }

    fn complete_snapshot_candidate_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<(), DbError> {
        let mut replacement = expected.clone();
        replacement.complete_snapshot_cleanup()?;
        super::active_store_publication::update_active_store_publication_on(
            self.conn,
            &expected,
            &replacement,
        )
    }

    fn latest_local_store_snapshot(&mut self) -> Result<Option<PublishedStoreSnapshot>, DbError> {
        let root = self.required_root_authority()?;
        StoreRecords::new(self.conn, self.store_dir)
            .published_store_snapshot(&root, self.verified_store_authority)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn local_store_snapshots(&mut self) -> Result<Vec<PublishedStoreSnapshot>, DbError> {
        let root = self.required_root_authority()?;
        StoreRecords::new(self.conn, self.store_dir)
            .published_store_snapshots(&root, self.verified_store_authority)
    }

    fn complete_snapshot_publication(
        &mut self,
        accepted: crate::AcceptedStorePublicationInterval,
    ) -> Result<SnapshotMeta, DbError> {
        let authority = self.local_store_authority()?;
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let outbound = load_outbound_store_snapshot_on(&tx, self.store_dir, &authority)?
            .ok_or_else(|| DbError::Message("outbound Store snapshot is absent".to_string()))?;
        let accepted_entry = accepted
            .interval()
            .entries()
            .iter()
            .find(|entry| {
                matches!(
                    &entry.entry().payload,
                    coven_protocol::store_commit::StorePublicationPayload::Snapshot(reference)
                        if reference == &outbound.reference
                )
            })
            .ok_or_else(|| {
                DbError::Message(
                    "accepted Store publication does not contain the prepared snapshot".to_string(),
                )
            })?;
        if accepted.interval().previous() != &*outbound.publication.previous
            || accepted_entry.entry() != &outbound.publication.entry
            || accepted_entry.reference().object != outbound.publication.entry_object
        {
            return Err(DbError::Message(
                "accepted Store snapshot differs from the prepared publication".to_string(),
            ));
        }
        let superseded = StoreTransaction::new(&tx, self.store_dir)
            .retire_snapshot_artifact_ownership(
                self.verified_store_authority,
                &authority.value().store_root,
                &coven_protocol::store_commit::AcceptedStoreSnapshotRef {
                    snapshot: outbound.reference.clone(),
                    publication: accepted_entry.reference().clone(),
                },
                &outbound.meta.value,
            )?;
        install_snapshot_blob_plans_on(&tx, &outbound.blobs)?;
        let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
            metadata_slot: outbound.reference.object.slot().clone(),
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
        StoreTransaction::new(&tx, self.store_dir)
            .retire_store_blob_snapshot_ownership(outbound.reference.object.slot(), &superseded)?;
        let deleted = tx
            .execute(
                "DELETE FROM outbound_store_snapshot \
                 WHERE singleton = 1 AND snapshot_ref = ?1",
                [serde_json::to_string(&outbound.reference).map_err(|error| {
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
        let accepted_position =
            i64::try_from(accepted_entry.reference().position.get()).map_err(|_| {
                DbError::Message("Store snapshot position exceeds SQLite integer".into())
            })?;
        tx.execute(
            "INSERT INTO published_store_snapshot \
             (publication_position, snapshot_ref, meta_bytes) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                accepted_position,
                serde_json::to_string(&outbound.reference).map_err(|error| {
                    DbError::context("serialize published Store snapshot ref", error)
                })?,
                outbound.meta.bytes,
            ],
        )
        .map_err(DbError::from)?;
        let expected = super::observed_store_publication::load_store_current_publication_on(&tx)?;
        if expected.record() != accepted.interval().current() {
            super::observed_store_publication::install_store_publication_interval_on(
                &tx, &expected, &accepted,
            )?;
        } else if expected.observed_version() != accepted.current_version() {
            return Err(DbError::Message(
                "accepted Store snapshot revision differs from the installed boundary".into(),
            ));
        }
        let active_publication = ActiveStorePublication::snapshot(outbound.publication.clone())?;
        super::active_store_publication::clear_active_store_publication_on(
            &tx,
            &active_publication,
        )?;
        tx.commit().map_err(DbError::from)?;
        Ok(outbound.meta.value)
    }
}

impl StoreDatabase {
    pub async fn outbound_snapshot_publication(
        &self,
    ) -> Result<Option<DurableSnapshotPublication>, DbError> {
        self.call_store(|session| session.outbound_snapshot_publication())
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn stage_snapshot_publication(
        &self,
        stage: StoreSnapshotPublicationStage,
        meta: SnapshotMeta,
        meta_prepared: PreparedExactObject,
        publication: coven_protocol::prepared_commit::PreparedStorePublication,
        rollup_bytes: Vec<u8>,
        rollup_prepared: PreparedExactObject,
        image: SnapshotDatabaseImage,
        image_prepared: PreparedExactObject,
        blobs: Vec<PreparedSnapshotBlob>,
    ) -> Result<StoreSnapshotRef, DbError> {
        self.call_store(move |session| {
            session.stage_snapshot_publication(
                stage,
                meta,
                meta_prepared,
                publication,
                rollup_bytes,
                rollup_prepared,
                image,
                image_prepared,
                blobs,
            )
        })
        .await
    }

    pub async fn supersede_snapshot_publication(
        &self,
        previous: StoreSnapshotRef,
        snapshot: PublishedStoreSnapshot,
        accepted: AcceptedStorePublicationInterval,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.supersede_snapshot_publication(previous, snapshot, accepted)
        })
        .await
    }

    pub async fn complete_superseded_snapshot_publication(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<SnapshotMeta, DbError> {
        self.call_store(move |session| session.complete_superseded_snapshot_publication(expected))
            .await
    }

    pub async fn complete_snapshot_candidate_cleanup(
        &self,
        expected: ActiveStorePublication,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.complete_snapshot_candidate_cleanup(expected))
            .await
    }

    pub async fn latest_local_store_snapshot(
        &self,
    ) -> Result<Option<PublishedStoreSnapshot>, DbError> {
        self.call_store(|session| session.latest_local_store_snapshot())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn local_store_snapshots(&self) -> Result<Vec<PublishedStoreSnapshot>, DbError> {
        self.call_store(|session| session.local_store_snapshots())
            .await
    }

    pub async fn complete_snapshot_publication(
        &self,
        accepted: crate::AcceptedStorePublicationInterval,
    ) -> Result<SnapshotMeta, DbError> {
        self.call_store(move |session| session.complete_snapshot_publication(accepted))
            .await
    }
}

/// A pending snapshot has no installed image/rollup lease. Records already in
/// the accepted object graph retain their own retirement authority; this owner
/// retires only its otherwise unowned exact candidate objects.
fn snapshot_candidate_cleanup_on(
    connection: &Connection,
    old: &DurableSnapshotPublication,
    retained: &BTreeSet<ExactObjectRef>,
) -> Result<Vec<ExactObjectRef>, DbError> {
    let mut cleanup = Vec::new();
    for object in BTreeSet::from([
        old.reference.object.clone(),
        old.meta.value.image.object.clone(),
        old.meta.value.membership_rollup.object.clone(),
        old.publication.entry_object.clone(),
    ]) {
        if retained.contains(&object) {
            continue;
        }
        let object_id = coven_protocol::remote_object::remote_object_id(&object);
        let owned: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
            [object_id.to_string()],
            |row| row.get(0),
        )?;
        if owned {
            crate::load_remote_object_on(connection, object_id)?;
        } else {
            cleanup.push(object);
        }
    }
    Ok(cleanup)
}
