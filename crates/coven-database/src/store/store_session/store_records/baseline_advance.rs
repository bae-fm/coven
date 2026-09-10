//! Advancing a standing device's replay baseline over an acknowledged snapshot.
//!
//! Installing a baseline and advancing one are different operations on the same
//! row. A joining device installs once, into a database that *is* the snapshot;
//! a device that has been in the store all along already holds history past the
//! snapshot's cut and must keep it. What it may drop is the history at or under
//! the cut that the snapshot image restates in one object.
//!
//! "At or under the cut" is not the same as "droppable". The image itself keeps
//! a closure of retained materializations past its own coverage, because the
//! retained-access paths — historical Circle epoch access, author-exclusion
//! recovery — read those rows from the live database rather than from a replay.
//! A device installing that image ends up holding exactly that closure, so a
//! device advancing onto the same cut must end up holding it too. Retirement
//! here therefore asks the same question the image capture asked, through the
//! same derivation, and drops only what neither the closure nor the history
//! past the cut claims.
//!
//! Dropping it is the point. A retained materialization pins every package and
//! blob its commit needs, so history the device keeps for replay is history
//! reclaim may not delete. A device whose baseline never moves keeps its whole
//! past retained, and its pin set therefore covers every package ever written —
//! which is why a standing device's reclaim reports every target it considers as
//! retained for replay and deletes nothing, forever.

use std::collections::BTreeSet;

use super::retained_replay::PreparedRetainedReplayBaseline;
use super::{StoreRecords, StoreTransaction};
use crate::store::verified_store_authority::VerifiedStoreLookup;
use crate::{Database, DbError, ObjectHash, RetainedReplayOwner};
use coven_protocol::store_commit::{CommitFrontier, StoreBatchCommitRef, StoreRootRef};

/// What one advancement retired, for the reclaim report that follows it.
///
/// The counts are the acceptance evidence: a run that advanced the baseline and
/// released nothing did not do what it was for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvancedReplayBaseline {
    /// Retained materializations at or under the new cut, now retired.
    pub retired_commits: u64,
    /// Remote objects that lost a replay pin. One object can be pinned by
    /// several commits, so this counts pins released, not objects freed.
    pub released_pins: u64,
    /// Journalled writes the new baseline image now states, now stripped to
    /// their receipts or dropped outright.
    pub folded_writes: u64,
}

impl StoreTransaction<'_, '_> {
    /// Adopt `snapshot_authority`'s cut as this device's replay baseline and
    /// retire the history it supersedes.
    ///
    /// The image reconstructs the accepted cut from retained local history.
    /// It is validated from its bytes before anything is retired: the baseline is
    /// what replay rewinds to, so an image that will not open must fail while
    /// the old baseline is still the committed one.
    ///
    /// Returns `None` when the snapshot does not advance this device's cut,
    /// which is the ordinary case on every cycle after the first.
    ///
    /// The whole operation is one transaction on purpose. Advancing the cut is
    /// what licenses retiring the rows, and retiring the rows is what makes the
    /// cut worth advancing; committing either alone leaves a device that either
    /// cannot rewind or cannot reclaim.
    pub(crate) fn advance_snapshot_replay_baseline(
        self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &StoreRootRef,
        schema_version: u32,
        routing_hash: ObjectHash,
        snapshot_authority: coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
        prepared: crate::store::materialization_models::PreparedSnapshotReplayBaselineAdvance,
        blob_decls: &crate::BlobDecls,
        synced_tables: &[coven_protocol::synced_schema::SyncedTable],
    ) -> Result<Option<AdvancedReplayBaseline>, DbError> {
        let crate::store::materialization_models::PreparedSnapshotReplayBaselineAdvance {
            changes_publication_base: _,
            expected_current_cut,
            image,
            folded,
        } = prepared;
        let records = StoreRecords::new(self.transaction, self.store_dir);
        let installed_cut = CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(
                self.transaction,
                None,
            )?,
        )
        .map_err(DbError::from)?;
        if installed_cut != expected_current_cut {
            return Err(DbError::Message(
                "replay baseline capture is stale against the installed Store frontier".to_string(),
            ));
        }
        let cut = snapshot_authority.metadata.coverage.clone();
        let Some(current) =
            crate::store::retained_replay::load_replay_baseline_metadata_on(records)?
        else {
            return Err(DbError::Message(
                "advancing a replay baseline requires an installed baseline".to_string(),
            ));
        };
        if !advances(
            &snapshot_authority.snapshot,
            &cut,
            &current,
            !folded.is_empty(),
        ) {
            return Ok(None);
        }
        let snapshot_reference = snapshot_authority.snapshot.clone();
        let snapshot_hash = snapshot_reference.snapshot_hash;
        let prepared = PreparedRetainedReplayBaseline::new(
            cut.clone(),
            schema_version,
            routing_hash,
            crate::RetainedReplayAuthority::InstalledSnapshot(snapshot_authority),
            image,
        );
        let prepared =
            prepared.validate_and_retain_snapshot_blobs(self, blob_decls, synced_tables)?;

        let (retired_commits, mut released_pins) =
            self.retire_superseded_history(authority, root, &cut)?;
        let folded_writes = self.fold_settled_store_writes(&cut, &folded)?;
        self.rewrite_snapshot_coverage(&cut, snapshot_hash)?;

        let mut timings =
            coven_foundation::stage_timing::StageTimings::start("Retained replay baseline advance");
        self.transaction
            .execute("DELETE FROM retained_replay_baselines", [])
            .map_err(DbError::from)?;
        let installed = records.install_prepared_replay_baseline(prepared, &mut timings)?;
        timings.report();
        if installed.exact_cut != cut {
            return Err(DbError::Message(
                "advanced replay baseline cut differs from the snapshot it adopted".to_string(),
            ));
        }

        released_pins = released_pins
            .checked_add(self.replace_snapshot_replay_object_ownership(&installed)?)
            .ok_or_else(|| DbError::Message("released replay pin count exceeds u64".into()))?;

        self.retain_snapshot_device_states(authority, root, cut.clone().into_refs())?;
        Ok(Some(AdvancedReplayBaseline {
            retired_commits,
            released_pins,
            folded_writes,
        }))
    }

    /// Drop the retained materializations at or under `cut` that the baseline
    /// shape does not keep, releasing the replay pins they held.
    ///
    /// Being covered by the cut is not on its own a licence to drop a row. A
    /// baseline image keeps a closure past its own coverage —
    /// author-exclusion activations, Circle bootstrap activations, and every
    /// materialization still carrying a Circle package no bootstrap cut covers
    /// — because the retained-access paths read those rows out of the live
    /// database, not out of a replay. `snapshot_required_retained_refs` is that
    /// closure, and it is the same derivation the image capture just used, so
    /// what survives here is exactly what the new baseline restates plus the
    /// history past the cut that no snapshot supersedes.
    ///
    /// `materialized_commits` rows go with the rows that are dropped because
    /// they carry the foreign key into the retained row; the position they
    /// recorded is restated by the coverage row written afterwards.
    ///
    /// Returns the commits retired and the replay pins that released.
    fn retire_superseded_history(
        &self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &StoreRootRef,
        cut: &CommitFrontier,
    ) -> Result<(u64, u64), DbError> {
        let conn = self.transaction;
        let records = StoreRecords::new(conn, self.store_dir);
        let retained_by_baseline =
            crate::StoreDatabase::snapshot_required_retained_refs(records, authority, root, cut)?;
        self.retire_history_outside_baseline(cut, &retained_by_baseline)
    }

    pub(super) fn retire_history_outside_baseline(
        &self,
        cut: &CommitFrontier,
        retained_by_baseline: &BTreeSet<String>,
    ) -> Result<(u64, u64), DbError> {
        let conn = self.transaction;
        let records = StoreRecords::new(conn, self.store_dir);
        let mut retired_commits = 0u64;
        let mut released_pins = 0u64;
        for (stream_id, sequence, encoded_ref, input_hash) in
            records.retained_materialization_rows()?
        {
            let reference: StoreBatchCommitRef = serde_json::from_str(&encoded_ref)
                .map_err(|error| DbError::context("retained replay commit reference", error))?;
            if !cut.covers_commit(&reference) || retained_by_baseline.contains(&encoded_ref) {
                continue;
            }
            let owner = RetainedReplayOwner::Commit {
                commit: reference,
                input_hash: input_hash.parse().map_err(|error| {
                    DbError::context(format!("retained replay input hash {input_hash}"), error)
                })?,
            };
            released_pins = released_pins
                .checked_add(self.release_replay_pins(&stream_id, sequence, &owner)?)
                .ok_or_else(|| {
                    DbError::Message("released replay pin count exceeded u64".to_string())
                })?;
            conn.execute(
                "DELETE FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence],
            )
            .map_err(DbError::from)?;
            let deleted = conn
                .execute(
                    "DELETE FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    rusqlite::params![&stream_id, sequence],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "superseded retained materialization disappeared".to_string(),
                ));
            }
            retired_commits += 1;
        }
        Ok((retired_commits, released_pins))
    }

    /// Retire the write-journal prefix the new baseline image states.
    ///
    /// The journal does two jobs, and only one of them is history. It carries
    /// the partitions a write still owes — to the cloud, or to the local rows a
    /// canonical replay would otherwise lose — and it is also this device's
    /// record of where its own writes landed, which is deliberately the one
    /// answer that survives an advance now that `materialized_commits` does not.
    ///
    /// So a folded write loses its working material and keeps its receipt: the
    /// partitions and the payload claims on them go, and with them the
    /// changeset, the commit base, the affected rows and the blob facts — every
    /// one of which describes work the image has absorbed. A local-only write
    /// has no receipt worth keeping: local-only is the whole of what could ever
    /// be said about it, and its caller was told that when it committed, so its
    /// row goes as well. That is the one a device accumulates per host write
    /// rather than per published write, and dropping it is what stops the
    /// journal growing with the clock instead of with the work.
    ///
    /// `folded` is what the capture actually applied, and the prefix is derived
    /// again here, inside the transaction that adopts the image, because the two
    /// have to name the same writes: dropping a partition the image does not
    /// state loses its local rows, and keeping one the image does state replays
    /// them on top of themselves. They can only differ if a write landed between
    /// the capture and this transaction, which is not something to reconcile
    /// later — the advance fails and the next cycle captures against the newer
    /// journal.
    pub(super) fn fold_settled_store_writes(
        &self,
        cut: &CommitFrontier,
        folded: &[crate::SettledStoreWrite],
    ) -> Result<u64, DbError> {
        let conn = self.transaction;
        let records = StoreRecords::new(conn, self.store_dir);
        let derived = crate::StoreDatabase::settled_store_write_prefix_on(records, cut)?;
        if derived != folded {
            return Err(DbError::Message(format!(
                "the write journal moved under the replay baseline capture: \
                 it folded {} writes, {} are settled now",
                folded.len(),
                derived.len()
            )));
        }
        for settled in folded {
            let write_id = &settled.write_id;
            crate::payload_store::release_payload_owner_on(
                conn,
                &crate::payload_store::store_write_owner_key(write_id),
            )?;
            conn.execute(
                "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            conn.execute(
                "DELETE FROM store_write_partitions WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            let statement = if matches!(
                settled.status,
                coven_protocol::write::WriteStatus::LocalOnly
            ) {
                "DELETE FROM store_writes WHERE write_id = ?1"
            } else {
                "UPDATE store_writes
                 SET affected_rows = NULL, changeset_hash = NULL,
                     base = NULL, blob_facts = NULL, rebased = NULL
                 WHERE write_id = ?1"
            };
            let touched = conn
                .execute(statement, [write_id.as_str()])
                .map_err(DbError::from)?;
            if touched != 1 {
                return Err(DbError::Message(format!(
                    "folded Store write {write_id} disappeared"
                )));
            }
        }
        u64::try_from(folded.len())
            .map_err(|_| DbError::Message("folded write count exceeded u64".to_string()))
    }

    /// Remove one commit's replay ownership from every object it pinned.
    ///
    /// The objects keep the commit owner that activated them, so releasing a
    /// pin makes a package reclaimable rather than unowned.
    fn release_replay_pins(
        &self,
        stream_id: &str,
        sequence: i64,
        owner: &RetainedReplayOwner,
    ) -> Result<u64, DbError> {
        let conn = self.transaction;
        let object_ids = crate::query_mapped_rows(
            conn,
            "SELECT object_id FROM retained_replay_objects
             WHERE device_id = ?1 AND seq = ?2
             ORDER BY object_id",
            rusqlite::params![stream_id, sequence],
            |row| row.get::<_, String>(0),
        )?
        .into_iter()
        .map(|encoded| {
            encoded.parse::<ObjectHash>().map_err(|error| {
                DbError::context(format!("retained replay object id {encoded}"), error)
            })
        })
        .collect::<Result<BTreeSet<_>, DbError>>()?;
        for object_id in &object_ids {
            let mut remote = crate::load_remote_object_on(conn, *object_id)?;
            remote
                .remove_retained_replay_owner(owner)
                .map_err(|error| {
                    DbError::context(
                        format!("release superseded replay owner from {object_id}"),
                        error,
                    )
                })?;
            crate::update_remote_object_on(conn, *object_id, &remote)?;
        }
        conn.execute(
            "DELETE FROM retained_replay_objects WHERE device_id = ?1 AND seq = ?2",
            rusqlite::params![stream_id, sequence],
        )
        .map_err(DbError::from)?;
        u64::try_from(object_ids.len())
            .map_err(|_| DbError::Message("released replay pin count exceeded u64".to_string()))
    }

    /// Restate the advanced cut as this device's snapshot coverage.
    ///
    /// The frontier reads coverage beside `materialized_commits` and takes the
    /// later of the two, so this is what carries the position of the commits
    /// retired above.
    pub(super) fn rewrite_snapshot_coverage(
        &self,
        cut: &CommitFrontier,
        snapshot_hash: ObjectHash,
    ) -> Result<(), DbError> {
        let conn = self.transaction;
        conn.execute("DELETE FROM snapshot_coverage", [])
            .map_err(DbError::from)?;
        for (stream_id, reference) in cut.clone().into_refs() {
            let encoded = serde_json::to_string(&reference)
                .map_err(|error| DbError::context("serialize advanced snapshot coverage", error))?;
            conn.execute(
                "INSERT INTO snapshot_coverage
                 (device_id, seq, commit_ref, snapshot_hash) VALUES (?1, ?2, ?3, ?4)",
                (
                    &stream_id,
                    Database::sequence_to_sqlite(&stream_id, reference.coord.sequence())?,
                    encoded,
                    snapshot_hash.to_string(),
                ),
            )
            .map_err(DbError::from)?;
        }
        Ok(())
    }
}

/// Whether adopting the snapshot would change this device's replay baseline.
///
/// Asked before the image is rebuilt, because rebuilding it replays the whole
/// retained history. The transaction that adopts the result asks again, and
/// that answer is the authoritative one.
pub(crate) fn replay_baseline_advances_on(
    records: StoreRecords<'_>,
    snapshot: &coven_protocol::store_commit::StoreSnapshotRef,
    cut: &CommitFrontier,
) -> Result<bool, DbError> {
    let Some(current) = crate::store::retained_replay::load_replay_baseline_metadata_on(records)?
    else {
        return Ok(false);
    };
    let folded = crate::StoreDatabase::settled_store_write_prefix_on(records, cut)?;
    Ok(advances(snapshot, cut, &current, !folded.is_empty()))
}

/// Whether `cut` covers `current` and changes what the baseline represents.
///
/// Equal coverage advances for a new accepted snapshot boundary or when the
/// image absorbs a settled local write prefix. An empty first snapshot still
/// replaces genesis as the base required by later publications.
fn advances(
    snapshot: &coven_protocol::store_commit::StoreSnapshotRef,
    cut: &CommitFrontier,
    current: &crate::RetainedReplayBaseline,
    consumes_writes: bool,
) -> bool {
    let same_snapshot = matches!(&current.authority,
        crate::RetainedReplayAuthority::InstalledSnapshot(authority) if &authority.snapshot == snapshot);
    cut.covers(&current.exact_cut)
        && (!same_snapshot || cut != &current.exact_cut || consumes_writes)
}
