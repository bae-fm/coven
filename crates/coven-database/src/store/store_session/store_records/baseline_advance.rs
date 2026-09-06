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
    /// `image` is the published snapshot's plaintext, which the caller has
    /// already checked against the signed image reference. It is validated
    /// again here, from the bytes, before anything is retired: the baseline is
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
        proof: coven_protocol::store_commit::ReplayBaselineRetirementProof,
        image: Vec<u8>,
        folded: &[crate::SettledStoreWrite],
        blob_decls: &crate::BlobDecls,
    ) -> Result<Option<AdvancedReplayBaseline>, DbError> {
        let current_cut = proof.current_cut.frontier();
        let (installed_state_ref, installed_state) =
            crate::store::store_device_state::store_device_state_for_history_cut_on(
                self.transaction,
                &proof.current_cut,
            )?;
        if installed_state_ref != proof.current_state
            || installed_state != proof.current_device_state
        {
            return Err(DbError::Message(
                "replay retirement proof differs from the installed Store device authority"
                    .to_string(),
            ));
        }
        let snapshot_authority = proof.authority;
        let records = StoreRecords::new(self.transaction, self.store_dir);
        let installed_cut = CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(
                self.transaction,
                None,
            )?,
        )
        .map_err(DbError::from)?;
        if installed_cut != current_cut {
            return Err(DbError::Message(
                "replay retirement proof is stale against the installed Store frontier".to_string(),
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
        if !advances(&cut, &current.exact_cut, !folded.is_empty()) {
            return Ok(None);
        }
        self.validate_replay_retirement_membership_witness(
            authority,
            root,
            &current_cut,
            &proof.current_membership,
            &proof.membership_witness,
            &snapshot_authority,
        )?;
        let snapshot_hash = snapshot_authority.snapshot.snapshot_hash;
        let prepared = PreparedRetainedReplayBaseline::new(
            cut.clone(),
            schema_version,
            routing_hash,
            crate::RetainedReplayAuthority::InstalledSnapshot(snapshot_authority),
            image,
        );
        let prepared = prepared.validate_image(self.store_dir, blob_decls)?;

        let (retired_commits, released_pins) =
            self.retire_superseded_history(authority, root, &cut)?;
        let folded_writes = self.fold_settled_store_writes(&cut, folded)?;
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

        // Device states are deliberately left alone. Installing an image adopts
        // a closure the image defines, so the install prunes to it; an advance
        // keeps the database it already had, and every position it recorded a
        // state for is one its own history reached — an exclusion proposal
        // resolving an old cut still asks for them. Keeping them costs one
        // small row each; dropping them leaves a live device unable to answer a
        // question about its own past.
        Ok(Some(AdvancedReplayBaseline {
            retired_commits,
            released_pins,
            folded_writes,
        }))
    }

    fn validate_replay_retirement_membership_witness(
        &self,
        authority: &mut dyn VerifiedStoreLookup,
        root: &StoreRootRef,
        current_cut: &CommitFrontier,
        current_membership: &coven_protocol::circle_control::StoreMembershipStateRef,
        witness: &coven_protocol::store_commit::ReplayRetirementMembershipWitness,
        snapshot: &coven_protocol::store_commit::RetainedReplaySnapshotAuthority,
    ) -> Result<(), DbError> {
        match witness {
            coven_protocol::store_commit::ReplayRetirementMembershipWitness::Snapshot => {
                if current_membership != &snapshot.metadata.state.membership {
                    return Err(DbError::Message(
                        "replay retirement membership differs from its snapshot witness"
                            .to_string(),
                    ));
                }
            }
            coven_protocol::store_commit::ReplayRetirementMembershipWitness::StoreCommit(
                reference,
            ) => {
                if !current_cut.covers_commit(reference) {
                    return Err(DbError::Message(
                        "replay retirement membership witness lies beyond its current cut"
                            .to_string(),
                    ));
                }
                let stream_id = reference.coord.stream_id.to_string();
                let sequence = reference.coord.sequence();
                let records = StoreRecords::new(self.transaction, self.store_dir);
                if records.materialized_commit_ref(&stream_id, sequence)? != Some(reference.clone())
                {
                    return Err(DbError::Message(
                        "replay retirement membership witness is not installed accepted history"
                            .to_string(),
                    ));
                }
                let row = records
                    .retained_materialization_rows()?
                    .into_iter()
                    .find(|(stored_stream, stored_sequence, _, _)| {
                        stored_stream == &stream_id
                            && u64::try_from(*stored_sequence).ok() == Some(sequence)
                    })
                    .ok_or_else(|| {
                        DbError::Message(
                            "replay retirement membership witness has no retained materialization"
                                .to_string(),
                        )
                    })?;
                let (_, _, encoded_ref, input_hash) = row;
                let stored_ref: StoreBatchCommitRef =
                    serde_json::from_str(&encoded_ref).map_err(|error| {
                        DbError::context("membership witness commit reference", error)
                    })?;
                if &stored_ref != reference {
                    return Err(DbError::Message(
                        "replay retirement membership witness differs from its retained row"
                            .to_string(),
                    ));
                }
                let materialization = (*self).load_retained_materialization(
                    root,
                    authority,
                    &stream_id,
                    sequence,
                    reference,
                    &input_hash,
                    None,
                )?;
                if &materialization.commit().membership_state != current_membership {
                    return Err(DbError::Message(
                        "replay retirement Store witness names another membership state"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
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
            crate::StoreDatabase::snapshot_required_retained_refs(records, authority, root)?;
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
    fn fold_settled_store_writes(
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
            let statement = if settled.fold.keeps_receipt() {
                "UPDATE store_writes
                 SET affected_rows = NULL, changeset_hash = NULL,
                     base = NULL, blob_facts = NULL
                 WHERE write_id = ?1"
            } else {
                "DELETE FROM store_writes WHERE write_id = ?1"
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
    fn rewrite_snapshot_coverage(
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

/// Whether adopting `cut` would move this device's baseline forward.
///
/// Asked before the image is rebuilt, because rebuilding it replays the whole
/// retained history. The transaction that adopts the result asks again, and
/// that answer is the authoritative one.
pub(crate) fn replay_baseline_advances_on(
    records: StoreRecords<'_>,
    cut: &CommitFrontier,
) -> Result<bool, DbError> {
    let Some(current) = crate::store::retained_replay::load_replay_baseline_metadata_on(records)?
    else {
        return Ok(false);
    };
    let folded = crate::StoreDatabase::settled_store_write_prefix_on(records, cut)?;
    Ok(advances(cut, &current.exact_cut, !folded.is_empty()))
}

/// Whether `cut` covers `current` and changes what the baseline represents.
///
/// Equal coverage still advances when the image absorbs a nonempty settled
/// write prefix. With no writes to consume, adopting the same cut would rewrite
/// the same baseline and retire nothing.
fn advances(cut: &CommitFrontier, current: &CommitFrontier, consumes_writes: bool) -> bool {
    cut.covers(current) && (cut != current || consumes_writes)
}
