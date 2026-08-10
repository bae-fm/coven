use std::collections::BTreeSet;

use rusqlite::OptionalExtension;

use super::{StoreRecords, StoreTransaction};
use crate::payload_store::{
    circle_bootstrap_coverage_owner_key, payload_owner_claims_on, release_payload_owner_on,
    set_payload_owner_claims_on,
};
use crate::store::verified_store_authority::VerifiedStoreLookup;
use crate::{DbError, ObjectHash, StoreDatabase};

impl StoreRecords<'_> {
    pub(crate) fn retained_circle_activation_commit_ref(
        self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, DbError> {
        crate::store::circle_authority::retained_circle_activation_commit_ref_on(
            self.conn, circle_id, control,
        )
    }

    pub(crate) fn circle_controls(
        self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Vec<coven_protocol::circle::CircleControlCoord>, DbError> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT control_coord
                 FROM circle_control_activations
                 WHERE circle_id = ?1
                 ORDER BY control_coord",
            )
            .map_err(DbError::from)?;
        let rows = statement
            .query_map([circle_id.to_string()], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?;
        let mut controls = Vec::new();
        for encoded in rows {
            let encoded = encoded.map_err(DbError::from)?;
            controls.push(serde_json::from_str(&encoded).map_err(|error| {
                DbError::context(
                    format!("parse retained Circle {circle_id} control coordinate"),
                    error,
                )
            })?);
        }
        Ok(controls)
    }

    /// Rebuild the stream-activation index from the retained materializations
    /// before restore selection resolves control-stream authority.
    pub(crate) fn seed_stream_activation_index_from_retained(
        self,
        registrations: &mut dyn crate::store::verified_store_authority::VerifiedRegistrationLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let encoded_refs = crate::query_mapped_rows(
            self.conn,
            "SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref",
            [],
            |row| row.get::<_, String>(0),
        )?;
        for encoded in encoded_refs {
            let reference: coven_protocol::store_commit::StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::context("parse retained materialization commit ref", error)
                })?;
            let owned = StoreDatabase::load_retained_merge_materialization_by_ref_on(
                self,
                root,
                registrations,
                &reference,
            )?;
            crate::store::stream_activation_records::record_verified_stream_activations_on(
                self.conn,
                owned.circle_activations().stream_activations(),
                &encoded,
            )?;
        }
        Ok(())
    }

    pub(crate) fn claimed_circle_bootstrap_coverage_refs(
        self,
    ) -> Result<Vec<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        let coverage =
            crate::store::retained_merge_replay::circle_bootstrap_coverage_refs_on(self.conn)?;
        for retained in &coverage {
            let owner_key = circle_bootstrap_coverage_owner_key(retained.circle_id);
            let expected = BTreeSet::from([retained.bootstrap.image.image_hash]);
            if payload_owner_claims_on(self.conn, &owner_key)? != expected {
                return Err(DbError::Message(format!(
                    "retained Circle {} bootstrap payload claims differ from its image hash",
                    retained.circle_id
                )));
            }
        }
        Ok(coverage)
    }
}

impl StoreTransaction<'_, '_> {
    pub(crate) fn seed_stream_activation_index_from_retained(
        self,
        registrations: &mut dyn crate::store::verified_store_authority::VerifiedRegistrationLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir)
            .seed_stream_activation_index_from_retained(registrations, root)
    }

    pub(crate) fn record_circle_bootstrap_coverage(
        self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        activation_commit: &coven_protocol::store_commit::StoreBatchCommitRef,
        activations: &coven_protocol::circle_activation::VerifiedCircleActivations,
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
            self.record_one_circle_bootstrap_coverage(
                authority,
                root,
                activation_commit,
                bootstrap,
                &activation.control,
            )?;
        }
        Ok(())
    }

    pub(crate) fn record_one_circle_bootstrap_coverage(
        self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        activation_commit: &coven_protocol::store_commit::StoreBatchCommitRef,
        bootstrap: &coven_protocol::circle_activation::VerifiedCircleImage,
        activation_control: &coven_protocol::circle::PreparedCircleControl,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        let circle_id = bootstrap.circle_id().to_string();
        let control_coord = serde_json::to_string(bootstrap.control())
            .map_err(|error| DbError::context("serialize Circle bootstrap control", error))?;
        let encoded_commit = serde_json::to_string(activation_commit)
            .map_err(|error| DbError::context("serialize Circle bootstrap activation", error))?;
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
        let installed_hash = self.install_payload(image_bytes)?;
        if installed_hash != bootstrap.reference().image.image_hash {
            return Err(DbError::Message(
                "Circle bootstrap image was installed under a different content hash".to_string(),
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
                    "retained Circle bootstrap row differs from its exact reference".to_string(),
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
                return Ok(());
            }
            let prior_control: coven_protocol::circle::CircleControlCoord =
                serde_json::from_str(&prior_control).map_err(|error| {
                    DbError::context("parse prior Circle bootstrap control", error)
                })?;
            let prior_cut: coven_protocol::store_commit::CommitFrontier =
                serde_json::from_str(&prior_cut).map_err(|error| {
                    DbError::context("parse prior Circle bootstrap coverage", error)
                })?;
            let prior_activation = StoreDatabase::verified_circle_activation_on(
                crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
                authority,
                root,
                bootstrap.circle_id(),
                &prior_control,
            )?
            .ok_or_else(|| {
                DbError::Message("prior Circle bootstrap activation is not retained".to_string())
            })?;
            if !bootstrap.reference().coverage.covers(&prior_cut)
                || !StoreDatabase::verified_circle_control_covers_on(
                    crate::store::store_session::StoreRecords::new(
                        self.transaction,
                        self.store_dir,
                    ),
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
        set_payload_owner_claims_on(conn, &owner_key, &BTreeSet::from([installed_hash]))
    }

    pub(crate) fn clear_circle_bootstrap_coverage(
        self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
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

    pub(crate) fn clear_imported_circle_bootstrap_coverage(self) -> Result<(), DbError> {
        let circle_ids = crate::query_mapped_rows(
            self.transaction,
            "SELECT circle_id FROM circle_bootstrap_coverage ORDER BY circle_id",
            [],
            |row| row.get::<_, String>(0),
        )?;
        for encoded in circle_ids {
            let circle_id = encoded
                .parse()
                .map_err(|error| DbError::context("parse imported Circle bootstrap id", error))?;
            let owner_key = circle_bootstrap_coverage_owner_key(circle_id);
            if !payload_owner_claims_on(self.transaction, &owner_key)?.is_empty() {
                return Err(DbError::Message(format!(
                    "imported Circle {circle_id} bootstrap unexpectedly carries local payload claims"
                )));
            }
            let deleted = self
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
}
