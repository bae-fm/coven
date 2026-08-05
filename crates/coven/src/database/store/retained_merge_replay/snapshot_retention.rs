use super::*;

impl StoreDatabase {
    /// The `retained_merge_materializations` commit-refs a Store snapshot image
    /// keeps: author-exclusion activation commits (device-exclusion recovery),
    /// Circle bootstrap-coverage activation commits, and every retained
    /// materialization that still carries a Circle package no bootstrap cut
    /// covers. `retain_snapshot_replay_inputs_on` keeps exactly this set, and
    /// `validate_snapshot_retained_inputs_on` expects exactly it, so the two
    /// share this one derivation.
    pub(crate) fn snapshot_required_retained_refs(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<BTreeSet<String>, DbError> {
        let mut statement = conn
            .prepare(
                "SELECT DISTINCT activation_commit
                 FROM store_author_exclusion_activations
                 ORDER BY activation_commit",
            )
            .map_err(DbError::from)?;
        let references = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        let mut required = references.into_iter().collect::<BTreeSet<_>>();
        let mut bootstrap_statement = conn
            .prepare(
                "SELECT circle_id, activation_commit, exact_cut
                 FROM circle_bootstrap_coverage ORDER BY circle_id",
            )
            .map_err(DbError::from)?;
        let bootstrap_rows = bootstrap_statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(bootstrap_statement);
        let mut bootstrap_cuts = BTreeMap::new();
        for (circle_id, activation_commit, exact_cut) in bootstrap_rows {
            let circle_id: crate::protocol::circle::CircleId =
                circle_id.parse().map_err(|error| {
                    DbError::Message(format!("snapshot Circle bootstrap id: {error}"))
                })?;
            let cut: CommitFrontier = serde_json::from_str(&exact_cut).map_err(|error| {
                DbError::Message(format!("snapshot Circle bootstrap coverage: {error}"))
            })?;
            if bootstrap_cuts.insert(circle_id, cut).is_some() {
                return Err(DbError::Message(
                    "snapshot has duplicate Circle bootstrap coverage".to_string(),
                ));
            }
            required.insert(activation_commit);
        }
        let mut materialization_statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let materialization_refs = materialization_statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(materialization_statement);
        for encoded in materialization_refs {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!("snapshot retained Circle commit: {error}"))
                })?;
            let materialization =
                Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            let has_uncovered_circle_package = materialization.packages().iter().any(|package| {
                let crate::protocol::audience_package::PackageAudience::Circle {
                    circle_id, ..
                } = package.audience()
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

    pub(crate) fn retain_snapshot_replay_inputs_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let required = Self::snapshot_required_retained_refs(conn, root)?;
        let mut retained = Vec::with_capacity(required.len());
        for encoded in required {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!(
                        "snapshot author exclusion activation commit: {error}"
                    ))
                })?;
            Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &reference.coord;
            let stream_id = stream_id.to_string();
            let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
            let (stored_ref, input_hash, canonical_input): (String, String, Vec<u8>) = conn
                .query_row(
                    "SELECT commit_ref, input_hash, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![&stream_id, sequence_sql],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(DbError::from)?;
            if stored_ref != encoded
                || input_hash != ObjectHash::digest(&canonical_input).to_string()
            {
                return Err(DbError::Message(
                    "snapshot retained replay activation differs from its retained input"
                        .to_string(),
                ));
            }
            let input: RetainedMergeMaterializationInput = serde_json::from_slice(&canonical_input)
                .map_err(|error| {
                    DbError::Message(format!("snapshot retained replay input: {error}"))
                })?;
            retained.push((reference, input_hash, canonical_input, input));
        }
        Self::remove_retained_replay_ownership_from_snapshot_on(conn)?;
        conn.execute("DELETE FROM retained_merge_materializations", [])
            .map_err(DbError::from)?;
        for (reference, input_hash, canonical_input, input) in retained {
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &reference.coord;
            let stream_id = stream_id.to_string();
            let sequence = Database::sequence_to_sqlite(&stream_id, *sequence)?;
            let encoded_ref = serde_json::to_string(&reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize snapshot author exclusion activation: {error}"
                ))
            })?;
            conn.execute(
                "INSERT INTO retained_merge_materializations
                 (device_id, seq, commit_ref, input_hash, canonical_input)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    &stream_id,
                    sequence,
                    &encoded_ref,
                    &input_hash,
                    &canonical_input
                ],
            )
            .map_err(DbError::from)?;
            let input_hash = input_hash.parse().map_err(|error| {
                DbError::Message(format!(
                    "snapshot author exclusion input hash {input_hash}: {error}"
                ))
            })?;
            let owner = RetainedReplayOwner::Commit {
                commit: reference,
                input_hash,
            };
            Self::pin_retained_merge_objects_on(conn, &input, &owner)?;
            Self::validate_retained_merge_pin_closure_on(conn, &input, &owner)?;
        }
        Ok(())
    }

    pub(crate) fn retain_snapshot_device_states_on(
        conn: &rusqlite::Transaction<'_>,
        root: &crate::protocol::store_commit::StoreRootRef,
        coverage: BTreeMap<String, StoreBatchCommitRef>,
    ) -> Result<(), DbError> {
        let mut required = coverage.into_values().collect::<BTreeSet<_>>();
        let mut statement = conn
            .prepare("SELECT commit_ref FROM retained_merge_materializations ORDER BY commit_ref")
            .map_err(DbError::from)?;
        let retained = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in retained {
            let reference: StoreBatchCommitRef =
                serde_json::from_str(&encoded).map_err(|error| {
                    DbError::Message(format!("snapshot retained device-state authority: {error}"))
                })?;
            let materialization =
                Self::load_retained_merge_materialization_by_ref_on(conn, root, &reference)?;
            required.insert(reference);
            required.extend(materialization.commit().order.predecessor.iter().cloned());
            required.extend(
                materialization
                    .commit()
                    .order
                    .dependencies
                    .values()
                    .cloned(),
            );
        }
        conn.execute_batch(
            "CREATE TEMP TABLE snapshot_required_device_states (
                 commit_ref TEXT PRIMARY KEY
             ) STRICT;",
        )
        .map_err(DbError::from)?;
        for reference in &required {
            let encoded = serde_json::to_string(reference).map_err(|error| {
                DbError::Message(format!(
                    "serialize snapshot device-state reference: {error}"
                ))
            })?;
            let present = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM store_device_state_snapshots WHERE commit_ref = ?1
                     )",
                    [&encoded],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(DbError::from)?;
            if !present {
                return Err(DbError::Message(
                    "snapshot device-state closure is incomplete".to_string(),
                ));
            }
            conn.execute(
                "INSERT INTO snapshot_required_device_states (commit_ref) VALUES (?1)",
                [&encoded],
            )
            .map_err(DbError::from)?;
        }
        conn.execute(
            "DELETE FROM store_device_state_snapshots
             WHERE NOT EXISTS (
                 SELECT 1 FROM snapshot_required_device_states required
                 WHERE required.commit_ref = store_device_state_snapshots.commit_ref
             )",
            [],
        )
        .map_err(DbError::from)?;
        let actual = conn
            .prepare("SELECT commit_ref FROM store_device_state_snapshots ORDER BY commit_ref")
            .map_err(DbError::from)?
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .map_err(DbError::from)?;
        let expected = required
            .iter()
            .map(|reference| {
                serde_json::to_string(reference).map_err(|error| {
                    DbError::Message(format!("serialize expected snapshot device state: {error}"))
                })
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        if actual != expected {
            return Err(DbError::Message(
                "snapshot device-state closure differs from its exact authority".to_string(),
            ));
        }
        conn.execute_batch("DROP TABLE snapshot_required_device_states")
            .map_err(DbError::from)?;
        Ok(())
    }

    pub(crate) fn validate_snapshot_retained_inputs_on(
        conn: &Connection,
        root: &crate::protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        // Each recorded author-exclusion activation must still match its
        // exclusion locator, so the image's exclusion table is internally exact.
        let mut statement = conn
            .prepare(
                "SELECT exclusion_ref, activation_commit
                 FROM store_author_exclusion_activations
                 ORDER BY exclusion_ref",
            )
            .map_err(DbError::from)?;
        let stored = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(DbError::from)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(DbError::from)?;
        drop(statement);
        for (encoded_exclusion, activation_commit) in stored {
            let exclusion = serde_json::from_str(&encoded_exclusion).map_err(|error| {
                DbError::Message(format!("snapshot author exclusion reference: {error}"))
            })?;
            let locator = load_author_exclusion_activation_locator_on(conn, root, &exclusion)?;
            let encoded_locator =
                serde_json::to_string(locator.activation_commit()).map_err(|error| {
                    DbError::Message(format!(
                        "serialize snapshot author exclusion activation: {error}"
                    ))
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
        let expected = Self::snapshot_required_retained_refs(conn, root)?;
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
