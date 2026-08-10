use super::*;
use crate::query_mapped_rows;
use crate::store::StoreRecords;

impl StoreDatabase {
    /// The `retained_merge_materializations` commit-refs a Store snapshot image
    /// keeps: author-exclusion activation commits (device-exclusion recovery),
    /// Circle bootstrap-coverage activation commits, and every retained
    /// materialization that still carries a Circle package no bootstrap cut
    /// covers. `StoreRecordTransaction::retain_snapshot_replay_inputs` keeps exactly this set, and
    /// `validate_snapshot_retained_inputs_on` expects exactly it, so the two
    /// share this one derivation.
    pub(crate) fn snapshot_required_retained_refs(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<BTreeSet<String>, DbError> {
        let conn = records.conn;
        let references = query_mapped_rows(
            conn,
            "SELECT DISTINCT activation_commit
                 FROM store_author_exclusion_activations
                 ORDER BY activation_commit",
            [],
            |row| row.get::<_, String>(0),
        )?;
        let mut required = references.into_iter().collect::<BTreeSet<_>>();
        let bootstrap_rows = query_mapped_rows(
            conn,
            "SELECT circle_id, activation_commit, exact_cut
                 FROM circle_bootstrap_coverage ORDER BY circle_id",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        let mut bootstrap_cuts = BTreeMap::new();
        for (circle_id, activation_commit, exact_cut) in bootstrap_rows {
            let circle_id: coven_protocol::circle::CircleId = circle_id
                .parse()
                .map_err(|error| DbError::context("snapshot Circle bootstrap id", error))?;
            let cut: CommitFrontier = serde_json::from_str(&exact_cut)
                .map_err(|error| DbError::context("snapshot Circle bootstrap coverage", error))?;
            if bootstrap_cuts.insert(circle_id, cut).is_some() {
                return Err(DbError::Message(
                    "snapshot has duplicate Circle bootstrap coverage".to_string(),
                ));
            }
            required.insert(activation_commit);
        }
        let materialization_refs = query_mapped_rows(
            conn,
            "SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref",
            [],
            |row| row.get::<_, String>(0),
        )?;
        for encoded in materialization_refs {
            let reference: StoreBatchCommitRef = serde_json::from_str(&encoded)
                .map_err(|error| DbError::context("snapshot retained Circle commit", error))?;
            let materialization =
                authority.retained_materialization_by_ref_on(records, &reference)?;
            if materialization.root() != root {
                return Err(DbError::Message(
                    "snapshot retained materialization belongs to another Store root".to_string(),
                ));
            }
            let has_uncovered_circle_package = materialization.packages().iter().any(|package| {
                let coven_protocol::audience_package::PackageAudience::Circle { circle_id, .. } =
                    package.audience()
                else {
                    return false;
                };
                bootstrap_cuts
                    .get(circle_id)
                    .is_none_or(|cut| !cut.covers_commit(&reference))
            });
            if has_uncovered_circle_package {
                required.insert(encoded);
            }
        }
        Ok(required)
    }

    pub(crate) fn validate_snapshot_retained_inputs_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let conn = records.conn;
        // Each recorded author-exclusion activation must still match its
        // exclusion locator, so the image's exclusion table is internally exact.
        let stored = query_mapped_rows(
            conn,
            "SELECT exclusion_ref, activation_commit
                 FROM store_author_exclusion_activations
                 ORDER BY exclusion_ref",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        for (encoded_exclusion, activation_commit) in stored {
            let exclusion = serde_json::from_str(&encoded_exclusion)
                .map_err(|error| DbError::context("snapshot author exclusion reference", error))?;
            let locator =
                load_author_exclusion_activation_locator_on(records, authority, root, &exclusion)?;
            let encoded_locator =
                serde_json::to_string(locator.activation_commit()).map_err(|error| {
                    DbError::context("serialize snapshot author exclusion activation", error)
                })?;
            if encoded_locator != activation_commit {
                return Err(DbError::Message(
                    "snapshot author exclusion activation changed during verification".to_string(),
                ));
            }
        }
        // The image's retained inputs must be exactly the set the retention rule
        // keeps — an extra row is unjustified replay baseline, a missing one is
        // coverage the Circle retained replay needs.
        let expected = Self::snapshot_required_retained_refs(records, authority, root)?;
        let mut statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let actual = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .map_err(DbError::from)?;
        if actual != expected {
            return Err(DbError::Message(
                "snapshot retained inputs differ from the retention rule".to_string(),
            ));
        }
        Ok(())
    }
}
