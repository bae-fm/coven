use super::*;
use crate::query_mapped_rows;
use crate::store::StoreRecords;

impl StoreSession<'_> {
    fn prepare_circle_restore_selection(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<CircleRestoreSelectionIndex, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        StoreRecords::new(&tx, self.store_dir)
            .seed_stream_activation_index_from_retained(self.verified_store_authority, root)?;
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
        let preserved_bootstraps = circle_bootstrap_coverage_refs_on(&tx)?;
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
            .retained_materialization_by_ref_on(
                crate::store::StoreRecords::new(self.conn, self.store_dir),
                reference,
            )?;
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
        self.verified_store_authority.retained_replay_inputs_on(
            crate::store::StoreRecords::new(self.conn, self.store_dir),
            root,
        )?;
        self.verified_store_authority
            .circle_replay_epoch_index_on(crate::store::StoreRecords::new(
                self.conn,
                self.store_dir,
            ))
    }
}

pub(crate) fn circle_bootstrap_coverage_ref_on(
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
    decode_circle_bootstrap_coverage_ref(
        circle_id,
        control,
        activation_commit,
        exact_cut,
        image_hash,
        bootstrap_ref,
    )
    .map(Some)
}

pub(crate) fn circle_bootstrap_coverage_refs_on(
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
    for (circle_id, control, activation_commit, exact_cut, image_hash, encoded_reference) in rows {
        let circle_id: coven_protocol::circle::CircleId = circle_id
            .parse()
            .map_err(|error| DbError::context("parse retained Circle bootstrap id", error))?;
        bootstraps.push(decode_circle_bootstrap_coverage_ref(
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
    let activation_commit = serde_json::from_str(&activation_commit)
        .map_err(|error| DbError::context("parse retained Circle bootstrap activation", error))?;
    let exact_cut: CommitFrontier = serde_json::from_str(&exact_cut)
        .map_err(|error| DbError::context("parse retained Circle bootstrap coverage", error))?;
    let bootstrap: coven_protocol::circle::CircleBootstrapRef =
        serde_json::from_slice(&encoded_reference).map_err(|error| {
            DbError::context("parse retained Circle bootstrap reference", error)
        })?;
    if serde_json::to_vec(&bootstrap)
        .map_err(|error| DbError::context("serialize retained Circle bootstrap reference", error))?
        != encoded_reference
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
        records
            .claimed_circle_bootstrap_coverage_refs()?
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
