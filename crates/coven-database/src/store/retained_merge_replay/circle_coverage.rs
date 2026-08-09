use super::*;
use crate::payload_spool::{
    circle_bootstrap_coverage_owner_key, payload_owner_claims_on, release_payload_owner_on,
    set_payload_owner_claims_on, StoreRecordTransaction, StoreRecords,
};
use crate::query_mapped_rows;

impl StoreSession<'_> {
    fn prepare_circle_restore_selection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleRestoreSelectionIndex, DbError> {
        let records = self.records;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        StoreDatabase::seed_stream_activation_index_from_retained_on(
            StoreRecords::new(&tx, records.store_dir),
            self.verified_store_authority,
            root,
        )?;
        let rows = query_mapped_rows(
            &tx,
            "SELECT circle_id, control_coord FROM circle_control_activations
             ORDER BY circle_id, control_coord",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        let mut circles: Vec<(
            coven_protocol::circle::CircleId,
            Vec<coven_protocol::circle::CircleControlCoord>,
        )> = Vec::new();
        for (circle_id, control_coord) in rows {
            let circle_id: coven_protocol::circle::CircleId = circle_id
                .parse()
                .map_err(|error| DbError::context("parse retained Circle id", error))?;
            let control: coven_protocol::circle::CircleControlCoord =
                serde_json::from_str(&control_coord).map_err(|error| {
                    DbError::context("parse retained Circle control coordinate", error)
                })?;
            match circles.last_mut() {
                Some((last_circle, controls)) if *last_circle == circle_id => {
                    controls.push(control)
                }
                _ => circles.push((circle_id, vec![control])),
            }
        }
        let preserved_bootstraps = StoreDatabase::circle_bootstrap_coverage_refs_on(&tx)?;
        tx.commit().map_err(DbError::from)?;
        Ok(CircleRestoreSelectionIndex {
            circles,
            preserved_bootstraps,
        })
    }

    fn retained_merge_materialization_by_ref(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let retained = self
            .verified_store_authority
            .retained_materialization_by_ref_on(self.records, reference)?;
        if retained.root() != root {
            return Err(DbError::Message(
                "retained Merge materialization belongs to another Store root".to_string(),
            ));
        }
        Ok(retained)
    }

    fn circle_replay_epoch_index(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        self.verified_store_authority
            .retained_replay_inputs_on(self.records, root)?;
        self.verified_store_authority
            .circle_replay_epoch_index_on(self.records)
    }
}

impl StoreDatabase {
    pub async fn prepare_circle_restore_selection(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleRestoreSelectionIndex, DbError> {
        self.connection
            .call_store(move |session| session.prepare_circle_restore_selection(&root))
            .await
    }

    pub async fn retained_merge_materialization_by_ref(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        reference: StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        self.connection
            .call_store(move |session| {
                session.retained_merge_materialization_by_ref(&root, &reference)
            })
            .await
    }

    pub async fn circle_replay_epoch_index(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        self.connection
            .call_store(move |session| session.circle_replay_epoch_index(&root))
            .await
    }

    pub(crate) fn record_circle_bootstrap_coverage_on(
        records: StoreRecordTransaction<'_, '_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
        activations: &VerifiedCircleActivations,
    ) -> Result<(), DbError> {
        for bootstrap in activations.bootstraps() {
            let activation = activations
                .circles()
                .iter()
                .find(|activation| {
                    activation.circle_id == bootstrap.circle_id()
                        && activation.control.coord == *bootstrap.control()
                })
                .ok_or_else(|| {
                    DbError::Message(
                        "verified Circle bootstrap has no activating control".to_string(),
                    )
                })?;
            Self::record_one_circle_bootstrap_coverage_on(
                records,
                authority,
                root,
                activation_commit,
                bootstrap,
                &activation.control,
            )?;
        }
        Ok(())
    }

    /// Record (or replace) one Circle's coverage row. The replacement validation
    /// writes the durable image bytes, accepts a strictly newer coverage cut whose
    /// control lineage the retained controls prove, and refuses a regression — the
    /// same rule whether the image is a pull-installed recipient bootstrap or a
    /// restore-installed standalone snapshot. `activation_control` is the verified
    /// control that activates this image: the pull recorder resolves it from the
    /// in-flight activation set; the restore installer resolves it from the
    /// just-installed control indexes.
    pub(crate) fn record_one_circle_bootstrap_coverage_on(
        records: StoreRecordTransaction<'_, '_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
        bootstrap: &coven_protocol::circle_activation::VerifiedCircleImage,
        activation_control: &coven_protocol::circle::PreparedCircleControl,
    ) -> Result<(), DbError> {
        let conn = records.transaction;
        {
            let circle_id = bootstrap.circle_id().to_string();
            let control_coord = serde_json::to_string(bootstrap.control())
                .map_err(|error| DbError::context("serialize Circle bootstrap control", error))?;
            let encoded_commit = serde_json::to_string(activation_commit).map_err(|error| {
                DbError::context("serialize Circle bootstrap activation", error)
            })?;
            let encoded_cut = serde_json::to_string(&bootstrap.reference().coverage)
                .map_err(|error| DbError::context("serialize Circle bootstrap coverage", error))?;
            let encoded_ref = serde_json::to_vec(bootstrap.reference())
                .map_err(|error| DbError::context("serialize Circle bootstrap reference", error))?;
            let encoded_image_hash = bootstrap.reference().image.image_hash.to_string();
            let image_bytes = bootstrap.image_bytes();
            if bootstrap.reference().image.image_hash != ObjectHash::digest(image_bytes) {
                return Err(DbError::Message(
                    "Circle bootstrap image bytes differ from their exact image hash".to_string(),
                ));
            }
            let installed_hash = records.install_payload(image_bytes)?;
            if installed_hash != bootstrap.reference().image.image_hash {
                return Err(DbError::Message(
                    "Circle bootstrap image was installed under a different content hash"
                        .to_string(),
                ));
            }
            let owner_key = circle_bootstrap_coverage_owner_key(bootstrap.circle_id());
            let existing: Option<(String, String, String, String, Vec<u8>)> = conn
                .query_row(
                    "SELECT control_coord, activation_commit, exact_cut, image_hash, bootstrap_ref
                     FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                    [&circle_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(DbError::from)?;
            let actual_claims = payload_owner_claims_on(conn, &owner_key)?;
            if let Some((prior_control, prior_commit, prior_cut, prior_image_hash, prior_ref)) =
                existing
            {
                let prior_reference: coven_protocol::circle::CircleBootstrapRef =
                    serde_json::from_slice(&prior_ref).map_err(|error| {
                        DbError::context("parse prior Circle bootstrap reference", error)
                    })?;
                if serde_json::to_vec(&prior_reference).map_err(|error| {
                    DbError::context("serialize prior Circle bootstrap reference", error)
                })? != prior_ref
                    || serde_json::to_string(&prior_reference.coverage).map_err(|error| {
                        DbError::context("serialize prior Circle bootstrap coverage", error)
                    })? != prior_cut
                    || prior_reference.image.image_hash.to_string() != prior_image_hash
                {
                    return Err(DbError::Message(
                        "retained Circle bootstrap row differs from its exact reference"
                            .to_string(),
                    ));
                }
                let expected_claims =
                    BTreeSet::from([prior_image_hash.parse::<ObjectHash>().map_err(|error| {
                        DbError::context("parse retained Circle bootstrap image hash", error)
                    })?]);
                if actual_claims != expected_claims {
                    return Err(DbError::Message(format!(
                        "retained Circle {} bootstrap payload claims differ from its image hash",
                        bootstrap.circle_id()
                    )));
                }
                if (
                    prior_control.as_str(),
                    prior_commit.as_str(),
                    prior_cut.as_str(),
                    prior_image_hash.as_str(),
                    &prior_ref,
                ) == (
                    control_coord.as_str(),
                    encoded_commit.as_str(),
                    encoded_cut.as_str(),
                    encoded_image_hash.as_str(),
                    &encoded_ref,
                ) {
                    // Row already records this exact image — idempotent no-op.
                    return Ok(());
                }
                let prior_control: coven_protocol::circle::CircleControlCoord =
                    serde_json::from_str(&prior_control).map_err(|error| {
                        DbError::context("parse prior Circle bootstrap control", error)
                    })?;
                let prior_cut: CommitFrontier =
                    serde_json::from_str(&prior_cut).map_err(|error| {
                        DbError::context("parse prior Circle bootstrap coverage", error)
                    })?;
                let prior_activation = Self::verified_circle_activation_on(
                    StoreRecords::new(records.transaction, records.store_dir),
                    authority,
                    root,
                    bootstrap.circle_id(),
                    &prior_control,
                )?
                .ok_or_else(|| {
                    DbError::Message(
                        "prior Circle bootstrap activation is not retained".to_string(),
                    )
                })?;
                if !bootstrap.reference().coverage.covers(&prior_cut)
                    || !Self::verified_circle_control_covers_on(
                        StoreRecords::new(records.transaction, records.store_dir),
                        authority,
                        root,
                        bootstrap.circle_id(),
                        activation_control,
                        &prior_activation.control.coord,
                    )?
                {
                    return Err(DbError::Message(format!(
                        "Circle {} bootstrap conflicts with its retained predecessor",
                        bootstrap.circle_id()
                    )));
                }
            } else if !actual_claims.is_empty() {
                return Err(DbError::Message(format!(
                    "Circle {} bootstrap payload claims exist without their coverage row",
                    bootstrap.circle_id()
                )));
            }
            conn.execute(
                "INSERT INTO circle_bootstrap_coverage
                 (circle_id, control_coord, activation_commit, exact_cut, image_hash, bootstrap_ref)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(circle_id) DO UPDATE SET
                   control_coord = excluded.control_coord,
                   activation_commit = excluded.activation_commit,
                   exact_cut = excluded.exact_cut,
                   image_hash = excluded.image_hash,
                   bootstrap_ref = excluded.bootstrap_ref",
                rusqlite::params![
                    circle_id,
                    control_coord,
                    encoded_commit,
                    encoded_cut,
                    encoded_image_hash,
                    encoded_ref,
                ],
            )
            .map_err(DbError::from)?;
            set_payload_owner_claims_on(conn, &owner_key, &BTreeSet::from([installed_hash]))?;
        }
        Ok(())
    }

    /// Delete one retained coverage row and its payload claim in the same
    /// transaction. The row and claim must agree before either is removed.
    pub(crate) fn clear_circle_bootstrap_coverage_on(
        records: StoreRecordTransaction<'_, '_>,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<(), DbError> {
        let conn = records.transaction;
        let owner_key = circle_bootstrap_coverage_owner_key(circle_id);
        let image_hash = conn
            .query_row(
                "SELECT image_hash FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|hash| hash.parse::<ObjectHash>().map_err(DbError::from))
            .transpose()?;
        let expected_claims = image_hash.into_iter().collect::<BTreeSet<_>>();
        if payload_owner_claims_on(conn, &owner_key)? != expected_claims {
            return Err(DbError::Message(format!(
                "Circle {circle_id} bootstrap row and payload claims disagree"
            )));
        }
        let deleted = conn
            .execute(
                "DELETE FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
            )
            .map_err(DbError::from)?;
        if deleted != usize::from(!expected_claims.is_empty()) {
            return Err(DbError::Message(format!(
                "Circle {circle_id} bootstrap row changed while it was being removed"
            )));
        }
        release_payload_owner_on(conn, &owner_key)
    }

    /// Remove the cloud-reference-only coverage rows imported from a Store
    /// snapshot before the final restore installs locally owned images. A
    /// snapshot deliberately carries no payload claims; finding one here means
    /// the transport image contains live local ownership state.
    pub(crate) fn clear_imported_circle_bootstrap_coverage_on(
        records: StoreRecordTransaction<'_, '_>,
    ) -> Result<(), DbError> {
        let circle_ids = query_mapped_rows(
            records.transaction,
            "SELECT circle_id FROM circle_bootstrap_coverage ORDER BY circle_id",
            [],
            |row| row.get::<_, String>(0),
        )?;
        for encoded in circle_ids {
            let circle_id = encoded
                .parse()
                .map_err(|error| DbError::context("parse imported Circle bootstrap id", error))?;
            let owner_key = circle_bootstrap_coverage_owner_key(circle_id);
            if !payload_owner_claims_on(records.transaction, &owner_key)?.is_empty() {
                return Err(DbError::Message(format!(
                    "imported Circle {circle_id} bootstrap unexpectedly carries local payload claims"
                )));
            }
            let deleted = records
                .transaction
                .execute(
                    "DELETE FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                    [&encoded],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(format!(
                    "imported Circle {circle_id} bootstrap disappeared during restore"
                )));
            }
        }
        Ok(())
    }

    /// Rebuild the stream-activation index from the retained materializations. A
    /// device restored from a snapshot has the retained authority but not the
    /// per-cycle stream-activation index the pull writes; restore selection must
    /// resolve control-stream authority before any pull, so it seeds the index
    /// from the retained inputs it will otherwise replay from. Idempotent: the
    /// recorder re-verifies each activation against any existing row.
    pub(crate) fn seed_stream_activation_index_from_retained_on(
        records: StoreRecords<'_>,
        registrations: &mut dyn VerifiedRegistrationLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let conn = records.conn;
        let encoded_refs = query_mapped_rows(
            conn,
            "SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref",
            [],
            |row| row.get::<_, String>(0),
        )?;
        for encoded in encoded_refs {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::context("parse retained materialization commit ref", error)
                })?;
            let owned = Self::load_retained_merge_materialization_by_ref_on(
                records,
                root,
                registrations,
                &reference,
            )?;
            Self::record_verified_stream_activations_on(
                conn,
                owned.circle_activations().stream_activations(),
                &encoded,
            )?;
        }
        Ok(())
    }

    pub fn circle_bootstrap_coverage_ref_on(
        conn: &Connection,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        let row: Option<(String, String, String, String, Vec<u8>)> = conn
            .query_row(
                "SELECT control_coord, activation_commit, exact_cut, image_hash, bootstrap_ref
                 FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((control, activation_commit, exact_cut, image_hash, bootstrap_ref)) = row else {
            return Ok(None);
        };
        Self::decode_circle_bootstrap_coverage_ref(
            circle_id,
            control,
            activation_commit,
            exact_cut,
            image_hash,
            bootstrap_ref,
        )
        .map(Some)
    }

    pub fn circle_bootstrap_coverage_refs_on(
        conn: &Connection,
    ) -> Result<Vec<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        let rows = query_mapped_rows(
            conn,
            "SELECT circle_id, control_coord, activation_commit, exact_cut,
                        image_hash, bootstrap_ref
                 FROM circle_bootstrap_coverage ORDER BY circle_id",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                ))
            },
        )?;
        let mut bootstraps = Vec::with_capacity(rows.len());
        for (circle_id, control, activation_commit, exact_cut, image_hash, encoded_reference) in
            rows
        {
            let circle_id: coven_protocol::circle::CircleId = circle_id
                .parse()
                .map_err(|error| DbError::context("parse retained Circle bootstrap id", error))?;
            bootstraps.push(Self::decode_circle_bootstrap_coverage_ref(
                circle_id,
                control,
                activation_commit,
                exact_cut,
                image_hash,
                encoded_reference,
            )?);
        }
        Ok(bootstraps)
    }

    pub(crate) fn claimed_circle_bootstrap_coverage_refs_on(
        records: StoreRecords<'_>,
    ) -> Result<Vec<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        let coverage = Self::circle_bootstrap_coverage_refs_on(records.conn)?;
        for retained in &coverage {
            let owner_key = circle_bootstrap_coverage_owner_key(retained.circle_id);
            let expected = BTreeSet::from([retained.bootstrap.image.image_hash]);
            if payload_owner_claims_on(records.conn, &owner_key)? != expected {
                return Err(DbError::Message(format!(
                    "retained Circle {} bootstrap payload claims differ from its image hash",
                    retained.circle_id
                )));
            }
        }
        Ok(coverage)
    }

    fn decode_circle_bootstrap_coverage_ref(
        circle_id: coven_protocol::circle::CircleId,
        control: String,
        activation_commit: String,
        exact_cut: String,
        image_hash: String,
        encoded_reference: Vec<u8>,
    ) -> Result<coven_protocol::circle::CircleBootstrapCoverageRef, DbError> {
        let control = serde_json::from_str(&control)
            .map_err(|error| DbError::context("parse retained Circle bootstrap control", error))?;
        let activation_commit = serde_json::from_str(&activation_commit).map_err(|error| {
            DbError::context("parse retained Circle bootstrap activation", error)
        })?;
        let exact_cut: CommitFrontier = serde_json::from_str(&exact_cut)
            .map_err(|error| DbError::context("parse retained Circle bootstrap coverage", error))?;
        let bootstrap: coven_protocol::circle::CircleBootstrapRef =
            serde_json::from_slice(&encoded_reference).map_err(|error| {
                DbError::context("parse retained Circle bootstrap reference", error)
            })?;
        if serde_json::to_vec(&bootstrap).map_err(|error| {
            DbError::context("serialize retained Circle bootstrap reference", error)
        })? != encoded_reference
            || bootstrap.coverage != exact_cut
            || bootstrap.image.image_hash.to_string() != image_hash
        {
            return Err(DbError::Message(
                "retained Circle bootstrap row differs from its exact reference".to_string(),
            ));
        }
        Ok(coven_protocol::circle::CircleBootstrapCoverageRef {
            circle_id,
            control,
            activation_commit,
            bootstrap,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn circle_bootstrap_replay_inputs_on(
        records: StoreRecords<'_>,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        Self::claimed_circle_bootstrap_coverage_refs_on(records)?
            .into_iter()
            .map(|coverage| {
                let image_bytes = records.payload(coverage.bootstrap.image.image_hash)?;
                let image =
                    coven_protocol::circle_activation::VerifiedCircleImage::from_stored_image(
                        coverage.circle_id,
                        coverage.control,
                        coverage.bootstrap,
                        image_bytes,
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                Ok((coverage.activation_commit, image))
            })
            .collect()
    }
}
