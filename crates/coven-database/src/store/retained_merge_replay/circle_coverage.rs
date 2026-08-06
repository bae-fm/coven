use super::*;
use crate::query_mapped_rows;

impl StoreDatabase {
    pub async fn prepare_circle_restore_selection(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleRestoreSelectionIndex, DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                Self::seed_stream_activation_index_from_retained_on(&tx, &root)?;
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
                let preserved_images = Self::circle_bootstrap_replay_inputs_on(&tx)?;
                tx.commit().map_err(DbError::from)?;
                Ok(CircleRestoreSelectionIndex {
                    circles,
                    preserved_images,
                })
            })
            .await
    }

    pub async fn retained_merge_materialization_by_ref(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        reference: StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        self.connection
            .call(move |conn| {
                Self::load_retained_merge_materialization_by_ref_on(conn, &root, &reference)
            })
            .await
    }

    pub async fn circle_replay_epoch_index(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleReplayEpochIndex, DbError> {
        self.with_retained_merge_materializations(move |conn, cache| {
            cache.replay_inputs_on(conn, &root)?;
            cache.circle_replay_epoch_index_on(conn)
        })
        .await
    }

    pub fn record_circle_bootstrap_coverage_on(
        conn: &Connection,
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
                conn,
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
    pub fn record_one_circle_bootstrap_coverage_on(
        conn: &Connection,
        root: &coven_protocol::store_commit::StoreRootRef,
        activation_commit: &StoreBatchCommitRef,
        bootstrap: &coven_protocol::circle_activation::VerifiedCircleImage,
        activation_control: &coven_protocol::circle::PreparedCircleControl,
    ) -> Result<(), DbError> {
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
            // The stored image bytes are input to the replay verifier, not trusted
            // for being local: their digest binds the row's image hash at write and
            // again at read, so a corrupted or swapped image fails loud.
            let image_bytes = bootstrap.image_bytes();
            if bootstrap.reference().image.image_hash != ObjectHash::digest(image_bytes) {
                return Err(DbError::Message(
                    "Circle bootstrap image bytes differ from their exact image hash".to_string(),
                ));
            }
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
                    conn,
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
                        conn,
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
            }
            conn.execute(
                "INSERT INTO circle_bootstrap_coverage
                 (circle_id, control_coord, activation_commit, exact_cut, image_hash,
                  image_bytes, bootstrap_ref)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(circle_id) DO UPDATE SET
                   control_coord = excluded.control_coord,
                   activation_commit = excluded.activation_commit,
                   exact_cut = excluded.exact_cut,
                   image_hash = excluded.image_hash,
                   image_bytes = excluded.image_bytes,
                   bootstrap_ref = excluded.bootstrap_ref",
                rusqlite::params![
                    circle_id,
                    control_coord,
                    encoded_commit,
                    encoded_cut,
                    encoded_image_hash,
                    image_bytes,
                    encoded_ref,
                ],
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }

    /// Delete a Circle's preserved coverage row. Restore calls this for every
    /// Circle the restoring identity cannot decrypt: a leftover coverage row would
    /// reconstruct a replay image for a Circle the restorer has no access to,
    /// re-arming the replay for a removed member. Deleting a row that is already
    /// absent is not an error — the identity simply had no coverage to clear.
    pub fn clear_circle_bootstrap_coverage_on(
        conn: &Connection,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<(), DbError> {
        conn.execute(
            "DELETE FROM circle_bootstrap_coverage WHERE circle_id = ?1",
            [circle_id.to_string()],
        )
        .map(|_| ())
        .map_err(DbError::from)
    }

    /// Rebuild the stream-activation index from the retained materializations. A
    /// device restored from a snapshot has the retained authority but not the
    /// per-cycle stream-activation index the pull writes; restore selection must
    /// resolve control-stream authority before any pull, so it seeds the index
    /// from the retained inputs it will otherwise replay from. Idempotent: the
    /// recorder re-verifies each activation against any existing row.
    pub fn seed_stream_activation_index_from_retained_on(
        conn: &Connection,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
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
            let owned =
                Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
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
        let row: Option<(String, String, Vec<u8>)> = conn
            .query_row(
                "SELECT control_coord, activation_commit, bootstrap_ref
                 FROM circle_bootstrap_coverage WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DbError::from)?;
        let Some((control, activation_commit, bootstrap_ref)) = row else {
            return Ok(None);
        };
        let control: coven_protocol::circle::CircleControlCoord = serde_json::from_str(&control)
            .map_err(|error| DbError::context("parse Circle coverage control", error))?;
        let activation_commit: StoreBatchCommitRef = serde_json::from_str(&activation_commit)
            .map_err(|error| DbError::context("parse Circle coverage activation", error))?;
        let bootstrap: coven_protocol::circle::CircleBootstrapRef =
            serde_json::from_slice(&bootstrap_ref)
                .map_err(|error| DbError::context("parse Circle coverage bootstrap", error))?;
        Ok(Some(coven_protocol::circle::CircleBootstrapCoverageRef {
            circle_id,
            control,
            activation_commit,
            bootstrap,
        }))
    }

    pub fn circle_bootstrap_replay_inputs_on(
        conn: &Connection,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        let rows = query_mapped_rows(
            conn,
            "SELECT circle_id, control_coord, activation_commit, exact_cut,
                        image_hash, image_bytes, bootstrap_ref
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
                    row.get::<_, Vec<u8>>(6)?,
                ))
            },
        )?;
        let mut bootstraps = Vec::with_capacity(rows.len());
        for (
            circle_id,
            control,
            activation_commit,
            exact_cut,
            image_hash,
            image_bytes,
            encoded_reference,
        ) in rows
        {
            let circle_id: coven_protocol::circle::CircleId = circle_id
                .parse()
                .map_err(|error| DbError::context("parse retained Circle bootstrap id", error))?;
            let control = serde_json::from_str(&control).map_err(|error| {
                DbError::context("parse retained Circle bootstrap control", error)
            })?;
            let activation_commit: StoreBatchCommitRef = serde_json::from_str(&activation_commit)
                .map_err(|error| {
                DbError::context("parse retained Circle bootstrap activation", error)
            })?;
            let exact_cut: CommitFrontier = serde_json::from_str(&exact_cut).map_err(|error| {
                DbError::context("parse retained Circle bootstrap coverage", error)
            })?;
            let reference: coven_protocol::circle::CircleBootstrapRef =
                serde_json::from_slice(&encoded_reference).map_err(|error| {
                    DbError::context("parse retained Circle bootstrap reference", error)
                })?;
            if serde_json::to_vec(&reference).map_err(|error| {
                DbError::context("serialize retained Circle bootstrap reference", error)
            })? != encoded_reference
                || reference.coverage != exact_cut
                || reference.image.image_hash.to_string() != image_hash
                || ObjectHash::digest(&image_bytes).to_string() != image_hash
            {
                return Err(DbError::Message(
                    "retained Circle bootstrap row differs from its exact reference".to_string(),
                ));
            }
            // Reconstruct the replay input from the row's own durable bytes — the
            // one representation restore-installed snapshots and pull-installed
            // bootstraps both write. `from_stored_image` re-binds the digest; the
            // replay loop runs the full image verification against the retained
            // control and routing key.
            let bootstrap =
                coven_protocol::circle_activation::VerifiedCircleImage::from_stored_image(
                    circle_id,
                    control,
                    reference,
                    image_bytes,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
            bootstraps.push((activation_commit, bootstrap));
        }
        Ok(bootstraps)
    }
}
