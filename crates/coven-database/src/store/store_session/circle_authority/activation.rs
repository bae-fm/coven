//! Which accepted Store commit activated a Circle control.
//!
//! Every Circle control the local store indexes names the exact Store commit
//! that accepted it. These reads resolve that link in both directions — control
//! to commit, and commit to the activation it carried — and report a control
//! whose materialization was reclaimed as such rather than as absence.

use crate::store::store_session::verified_store_authority::VerifiedStoreLookup;
use crate::store::store_session::{StoreRecords, StoreSession};
use crate::*;
use coven_protocol::store_commit::StoreBatchCommitRef;
use rusqlite::{Connection, OptionalExtension};

/// The three states a Circle control's activating commit can be in when resolved
/// from the retained authority: not an activation at all, a known activation whose
/// materialization has been reclaimed, or a retained activation with its commit.
enum CircleActivationCommitLookup {
    Absent,
    Reclaimed { stream_id: String, sequence: u64 },
    Retained(StoreBatchCommitRef),
}

/// Resolve a Circle control's activating commit reference from retained
/// authority. A known activation whose materialization was reclaimed is an
/// error because strict callers require the current control to stay retained.
pub(crate) fn circle_activation_commit_ref_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
    control: &coven_protocol::circle::CircleControlCoord,
) -> Result<Option<StoreBatchCommitRef>, DbError> {
    match circle_activation_commit_lookup_on(conn, circle_id, control)? {
        CircleActivationCommitLookup::Absent => Ok(None),
        CircleActivationCommitLookup::Reclaimed {
            stream_id,
            sequence,
        } => Err(DbError::Message(format!(
            "Circle {circle_id} activation commit {stream_id}/{sequence} is not retained"
        ))),
        CircleActivationCommitLookup::Retained(reference) => Ok(Some(reference)),
    }
}

/// Resolve a Circle control's activating commit, reading a reclaimed
/// materialization as absence because its standalone snapshot is superseded.
pub(crate) fn retained_circle_activation_commit_ref_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
    control: &coven_protocol::circle::CircleControlCoord,
) -> Result<Option<StoreBatchCommitRef>, DbError> {
    Ok(
        match circle_activation_commit_lookup_on(conn, circle_id, control)? {
            CircleActivationCommitLookup::Retained(reference) => Some(reference),
            CircleActivationCommitLookup::Absent
            | CircleActivationCommitLookup::Reclaimed { .. } => None,
        },
    )
}

fn circle_activation_commit_lookup_on(
    conn: &Connection,
    circle_id: coven_protocol::circle::CircleId,
    control: &coven_protocol::circle::CircleControlCoord,
) -> Result<CircleActivationCommitLookup, DbError> {
    let control_coord = serde_json::to_string(control)
        .map_err(|error| DbError::context("serialize Circle control coordinate", error))?;
    let stored = conn
        .query_row(
            "SELECT stream_id, seq, commit_hash
             FROM circle_control_activations
             WHERE circle_id = ?1 AND control_coord = ?2",
            rusqlite::params![circle_id.to_string(), control_coord],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(DbError::from)?;
    let Some((stream_id, sequence_sql, commit_hash)) = stored else {
        return Ok(CircleActivationCommitLookup::Absent);
    };
    let sequence = Database::sequence_from_sqlite(&stream_id, sequence_sql)?;
    let stored_ref: Option<String> = conn
        .query_row(
            "SELECT commit_ref FROM retained_merge_materializations
             WHERE device_id = ?1 AND seq = ?2",
            rusqlite::params![&stream_id, sequence_sql],
            |row| row.get(0),
        )
        .optional()
        .map_err(DbError::from)?;
    let Some(stored_ref) = stored_ref else {
        return Ok(CircleActivationCommitLookup::Reclaimed {
            stream_id,
            sequence,
        });
    };
    let reference = crate::store::materialized_commit_index::parse_stored_commit_ref(
        &stream_id,
        sequence,
        &stored_ref,
    )?;
    if reference.commit_hash.to_string() != commit_hash {
        return Err(DbError::Message(format!(
            "Circle {circle_id} activation index differs from its retained commit"
        )));
    }
    Ok(CircleActivationCommitLookup::Retained(reference))
}

impl StoreSession<'_> {
    fn verified_circle_activation_context(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<(
            coven_protocol::circle_activation::VerifiedCircleReference,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        let Some(commit) = circle_activation_commit_ref_on(self.conn, circle_id, control)? else {
            return Ok(None);
        };
        let activation = StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            control,
        )?
        .ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} activation context lost control {control:?}"
            ))
        })?;
        Ok(Some((activation, commit)))
    }

    fn verified_circle_activation(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        StoreDatabase::verified_circle_activation_on(
            crate::store::store_session::StoreRecords::new(self.conn, self.store_dir),
            self.verified_store_authority,
            root,
            circle_id,
            control,
        )
    }

    fn retained_circle_activation_commit_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        retained_circle_activation_commit_ref_on(self.conn, circle_id, control)
    }
}

impl StoreDatabase {
    pub async fn verified_circle_activation_context(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<(
            coven_protocol::circle_activation::VerifiedCircleReference,
            StoreBatchCommitRef,
        )>,
        DbError,
    > {
        self.call_store(move |session| {
            session.verified_circle_activation_context(&root, circle_id, &control)
        })
        .await
    }

    /// See [`StoreDatabase::retained_circle_activation_on`].
    pub async fn retained_circle_activation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        activating_commit: StoreBatchCommitRef,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        self.call_store(move |session| {
            Self::retained_circle_activation_on(
                StoreRecords::new(session.conn, session.store_dir),
                session.verified_store_authority,
                &root,
                circle_id,
                &activating_commit,
            )
        })
        .await
    }

    pub async fn verified_circle_activation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        self.call_store(move |session| {
            session.verified_circle_activation(&root, circle_id, &control)
        })
        .await
    }

    pub async fn retained_circle_activation_commit_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<Option<StoreBatchCommitRef>, DbError> {
        self.call_store(move |session| {
            session.retained_circle_activation_commit_ref(circle_id, &control)
        })
        .await
    }

    /// The accepted Circle activation `activating_commit` carries for
    /// `circle_id`, when this device still retains that commit's
    /// materialization.
    ///
    /// This is how an inherited Circle roster or metadata entry resolves to the
    /// exact earlier accepted activation that introduced it without re-reading
    /// the commit from storage — the path a recipient restoration and a
    /// baseline-covered position both take. Absence is not an error: the caller
    /// then proves the activation through the candidate's predecessor history.
    pub(super) fn retained_circle_activation_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        circle_id: coven_protocol::circle::CircleId,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
        let stream_id = activating_commit.coord.stream_id.to_string();
        let sequence = Database::sequence_to_sqlite(&stream_id, activating_commit.coord.sequence)?;
        let Some(stored) = records.retained_materialization_ref_at(&stream_id, sequence)? else {
            return Ok(None);
        };
        let expected = serde_json::to_string(activating_commit).map_err(|error| {
            DbError::context("serialize Circle activating commit reference", error)
        })?;
        if stored != expected {
            return Ok(None);
        }
        let retained = authority.retained_materialization_by_ref_on(records, activating_commit)?;
        if retained.root() != root {
            return Err(DbError::Message(
                "Circle activation belongs to another Store root".to_string(),
            ));
        }
        Ok(retained
            .circle_activations()
            .circles()
            .iter()
            .find(|activation| activation.circle_id == circle_id)
            .cloned())
    }
}

/// The activation for one control, preferring a verified activation the caller
/// prepared but has not installed yet over the retained index.
pub(super) fn verified_circle_activation_with_prefix_on(
    records: StoreRecords<'_>,
    authority: &mut dyn VerifiedStoreLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    circle_id: coven_protocol::circle::CircleId,
    coordinate: &coven_protocol::circle::CircleControlCoord,
    activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
) -> Result<Option<coven_protocol::circle_activation::VerifiedCircleReference>, DbError> {
    let mut matching = activations.iter().filter(|activation| {
        activation.circle_id == circle_id && &activation.control.coord == coordinate
    });
    if let Some(activation) = matching.next() {
        if activation.control.value.store_root_hash != root.store_root_hash
            || activation.reference.circle_id() != circle_id
            || activation.reference.control() != coordinate
            || !activation.control.verify()
        {
            return Err(DbError::Message(
                "prepared Circle lineage differs from its Store or control reference".to_string(),
            ));
        }
        if matching.any(|other| other != activation) {
            return Err(DbError::Message(format!(
                "Circle {circle_id} prepared history has conflicting copies of control {coordinate:?}"
            )));
        }
        return Ok(Some(activation.clone()));
    }
    StoreDatabase::verified_circle_activation_on(records, authority, root, circle_id, coordinate)
}
