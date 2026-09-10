use super::*;
use crate::store::retained_replay::load_replay_baseline_on;
use crate::store::store_session::StoreRecords;
use coven_protocol::write::{PublishedWrite, SnapshotCoveredPosition};

fn snapshot_write_is_covered(position: &SnapshotCoveredPosition, cut: &CommitFrontier) -> bool {
    cut.commits()
        .get(&position.coord.stream_id)
        .is_some_and(|covered| covered.coord.sequence() >= position.coord.sequence())
}

fn validate_snapshot_write_baseline(
    position: &SnapshotCoveredPosition,
    baseline: &RetainedReplayBaseline,
) -> Result<(), DbError> {
    let RetainedReplayAuthority::InstalledSnapshot(authority) = &baseline.authority else {
        return Err(DbError::Message(
            "snapshot-covered write has no installed snapshot baseline".into(),
        ));
    };
    let root = authority.store_root.store_root_hash;
    let stream = coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
        root,
        &position.author_registration,
        coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let installed_position = authority.metadata.publication_predecessor.next_position()?;
    if position.snapshot.publication.store_root_hash != root
        || position.coord.sequence() == 0
        || position.coord.stream_id != stream
        || installed_position < position.snapshot.publication.position
        || (installed_position == position.snapshot.publication.position
            && authority.snapshot != position.snapshot.snapshot)
        || !snapshot_write_is_covered(position, baseline.coverage())
    {
        return Err(DbError::Message(
            "snapshot-covered write differs from its installed cumulative baseline".into(),
        ));
    }
    Ok(())
}

fn replay_write_is_covered(
    records: StoreRecords<'_>,
    baseline: &RetainedReplayBaseline,
    write_id: &WriteId,
    status: &WriteStatus,
    candidate: Option<&StoreBatchCommitRef>,
) -> Result<bool, DbError> {
    match status {
        WriteStatus::Published(published) => match published.as_ref() {
            PublishedWrite::Commit(position) => {
                Ok(baseline.coverage().covers_commit(position.commit()))
            }
            PublishedWrite::Snapshot(position) => {
                validate_snapshot_write_baseline(position, baseline)?;
                Ok(true)
            }
        },
        WriteStatus::Publishing | WriteStatus::Blocked(_) => {
            match (&baseline.authority, candidate) {
                (RetainedReplayAuthority::InstalledSnapshot(authority), Some(candidate)) => {
                    records.snapshot_covers_reserved_write(write_id, candidate, authority)
                }
                _ => Ok(false),
            }
        }
        WriteStatus::LocalOnly
        | WriteStatus::LocalOnlyBlocked(_)
        | WriteStatus::Pending
        | WriteStatus::Resolved(_) => Ok(false),
    }
}

fn prepared_state_commit_reference(
    encoded: &str,
    write_id: &WriteId,
) -> Result<StoreBatchCommitRef, DbError> {
    let prepared: crate::store::publication_state::PreparedStoreWriteState =
        serde_json::from_str(encoded)
            .map_err(|error| DbError::context("retained replay prepared Store write", error))?;
    let commit: StoreBatchCommit = serde_json::from_slice(prepared.commit.semantic_bytes())
        .map_err(|error| DbError::context("retained replay prepared Store commit", error))?;
    if &commit.write_id != write_id {
        return Err(DbError::Message(format!(
            "retained write {write_id} names another prepared logical write"
        )));
    }
    let coord = coven_protocol::store_commit::StoreCommitCoord {
        stream_id: coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            commit.store_root_hash,
            &commit.author_registration,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        ),
        sequence: commit.seq(),
    };
    StoreBatchCommitRef::from_commit(
        &commit,
        coord,
        prepared.commit.prepared().reference().clone(),
    )
    .map_err(DbError::from)
}

impl crate::store::store_session::StoreTransaction<'_, '_> {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn load_retained_merge_materialization_by_ref(
        self,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        StoreDatabase::load_retained_merge_materialization_by_ref_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            root,
            registrations,
            reference,
        )
    }
}

impl StoreDatabase {
    pub(crate) fn load_retained_merge_materialization_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        stream_id: &str,
        sequence: u64,
        commit_ref: &StoreBatchCommitRef,
        expected_input_hash: &str,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let baseline = crate::store::retained_replay::load_replay_baseline_metadata_on(records)?;
        records.open_retained_merge_materialization(
            root,
            registrations,
            stream_id,
            sequence,
            commit_ref,
            expected_input_hash,
            baseline.as_ref(),
        )
    }

    pub(crate) fn load_retained_merge_materialization_by_ref_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let baseline = crate::store::retained_replay::load_replay_baseline_metadata_on(records)?;
        Self::load_retained_merge_materialization_at_baseline_on(
            records,
            root,
            registrations,
            reference,
            baseline.as_ref(),
        )
    }

    pub(crate) fn load_retained_merge_materialization_at_baseline_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
        baseline: Option<&RetainedReplayBaseline>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let (stored_ref, input_hash) =
            records.retained_materialization_identity(&stream_id, sequence_sql)?;
        let stored_ref = crate::store::materialized_commit_index::parse_stored_commit_ref(
            &stream_id,
            *sequence,
            &stored_ref,
        )?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge materialization coordinate contains another commit".to_string(),
            ));
        }
        records.open_retained_merge_materialization(
            root,
            registrations,
            &stream_id,
            *sequence,
            reference,
            &input_hash,
            baseline,
        )
    }

    /// The checkpoint a reference resolves to.
    ///
    /// A position at or under the installed snapshot coverage resolves to the
    /// coverage itself. The snapshot's owner signed one state for its whole
    /// covered prefix and the image restates that prefix in one object, so the
    /// per-position states below the coverage are not merely unavailable — they
    /// have stopped existing, and the coverage answers in their place. Above the
    /// coverage a retained row holds the position and answers for itself.
    ///
    /// This is why a predecessor cut may name a position no retained row holds:
    /// a commit's cut can reach back to any earlier coordinate on another
    /// stream, and a device that has advanced its replay baseline retired
    /// everything the new cut covers.
    ///
    /// `baseline` is supplied rather than loaded because loading one deserializes
    /// the whole baseline database image into a fresh in-memory connection and
    /// revalidates it, about fifteen milliseconds a call. The connection already
    /// holds a verified baseline; this reads that.
    pub(crate) fn load_retained_merge_history_checkpoint_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
        baseline: &RetainedReplayBaseline,
        reference: &StoreBatchCommitRef,
    ) -> Result<crate::RetainedMergeHistoryCheckpoint, DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream, *sequence)?;
        let coverage = records
            .snapshot_coverage_position(&stream)?
            .filter(|(covered, _)| covered >= sequence);
        if let Some((covered, encoded_coverage)) = coverage {
            let snapshot_reference: StoreBatchCommitRef = serde_json::from_str(&encoded_coverage)
                .map_err(|error| {
                DbError::context("snapshot Merge checkpoint commit ref", error)
            })?;
            // At the tip the coverage names this exact commit, and disagreeing
            // there is a corrupted position rather than a superseded one. Below
            // the tip there is nothing to compare against: the coverage state is
            // the answer for every position it covers.
            if covered == *sequence && &snapshot_reference != reference {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint coordinate contains another commit".to_string(),
                ));
            }
            let opened = Self::open_installed_baseline_history_summary(records, baseline)?;
            if opened
                .summary
                .frontier()
                .map_err(|error| DbError::context("snapshot Merge checkpoint frontier", error))?
                .get(&snapshot_reference.coord.stream_id)
                != Some(&snapshot_reference)
            {
                return Err(DbError::Message(
                    "snapshot Merge checkpoint is absent from its signed frontier".to_string(),
                ));
            }
            return Ok(crate::RetainedMergeHistoryCheckpoint::Snapshot(opened));
        }
        let (stored_ref, input_hash) =
            records.retained_materialization_identity(&stream, sequence_sql)?;
        let stored_ref: StoreBatchCommitRef = serde_json::from_str(&stored_ref)
            .map_err(|error| DbError::context("retained Merge checkpoint commit ref", error))?;
        if &stored_ref != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint coordinate contains another commit".to_string(),
            ));
        }
        let retained = Self::load_retained_merge_materialization_on(
            records,
            root,
            registrations,
            &stream_id.to_string(),
            *sequence,
            reference,
            &input_hash,
        )?;
        Self::open_retained_merge_history_checkpoint_on(
            records,
            registrations,
            reference,
            &retained,
        )
    }

    /// The signed history summary the installed baseline rests on, opened
    /// against the device state this database holds at its coverage.
    ///
    /// This is the authority that stands in for everything under the coverage:
    /// a walk that stops at the baseline resumes its composition from here
    /// rather than from the retired commits behind it.
    pub(crate) fn open_installed_baseline_history_summary(
        records: StoreRecords<'_>,
        baseline: &RetainedReplayBaseline,
    ) -> Result<coven_protocol::store_commit::OpenedRetainedMergeHistorySummary, DbError> {
        let RetainedReplayAuthority::InstalledSnapshot(authority) = &baseline.authority else {
            return Err(DbError::Message(
                "snapshot Merge checkpoint has genesis replay authority".to_string(),
            ));
        };
        let summary = authority.metadata.history_summary.clone();
        summary
            .validate_snapshot_baseline()
            .map_err(|error| DbError::context("snapshot Merge checkpoint", error))?;
        let frontier = summary.frontier().map_err(DbError::from)?;
        // Every stream the coverage names, not just the one a caller asked
        // about: `post_state` is the merge across the whole frontier, so
        // comparing one stream's state against it only ever agreed because the
        // stores that reached here had one stream.
        let (expected_state, state) = records.store_device_state_for_history_cut(
            &coven_protocol::store_commit::StoreHistoryCut(frontier),
        )?;
        if summary.post_state != expected_state {
            return Err(DbError::Message(
                "snapshot Merge checkpoint state differs from its signed reference".to_string(),
            ));
        }
        Ok(
            coven_protocol::store_commit::OpenedRetainedMergeHistorySummary {
                post_state: state,
                summary,
            },
        )
    }

    pub(crate) fn open_retained_merge_history_checkpoint_on(
        records: StoreRecords<'_>,
        registrations: &mut dyn VerifiedRegistrationLookup,
        reference: &StoreBatchCommitRef,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<crate::RetainedMergeHistoryCheckpoint, DbError> {
        if retained.commit_ref() != reference {
            return Err(DbError::Message(
                "retained Merge checkpoint materialization names another commit".to_string(),
            ));
        }
        let state = records.store_device_snapshot(reference)?;
        state
            .validate_canonical()
            .map_err(|error| DbError::context("retained Merge checkpoint state", error))?;
        let derived = crate::store::merge_materialization_transaction::derive_materialized_store_device_state_on(
            records,
            registrations,
            retained.root(),
            retained.commit(),
            retained.device_operations(),
        )?;
        if state != derived {
            return Err(DbError::Message(
                "retained Merge checkpoint state differs from its verified commit application"
                    .to_string(),
            ));
        }
        Ok(crate::RetainedMergeHistoryCheckpoint::Commit(Box::new(
            retained.clone(),
        )))
    }

    /// The prefix of the write journal a baseline at `cut` absorbs.
    ///
    /// A write is settled at `cut` when nothing it did is still owed: a
    /// local-only write the moment it commits, a resolved write once its
    /// candidate is cleaned up, a published write once `cut` covers the commit
    /// that carries it. Everything a settled write said is therefore restated
    /// by an image captured at `cut`: the baseline replay schedules each write
    /// at its observed Store frontier, with its shared commit when one exists.
    /// What the journal still holds for the settled prefix is therefore dead
    /// weight, and the advance strips each row to its receipt or drops it.
    ///
    /// It is a *prefix* rather than a set because the journal is ordered and
    /// its local partitions are the only record of the local rows. Folding a
    /// later write while retaining an earlier one would reverse their journal
    /// order on the next replay, so the walk stops at the first write that is
    /// not settled and everything after it stays.
    pub(crate) fn settled_store_write_prefix_on(
        records: StoreRecords<'_>,
        cut: &CommitFrontier,
    ) -> Result<Vec<crate::SettledStoreWrite>, DbError> {
        let mut settled = Vec::new();
        for row in records.store_write_replay_rows()? {
            let status: WriteStatus = serde_json::from_str(&row.status).map_err(|error| {
                DbError::context(format!("settled write {} status", row.write_id), error)
            })?;
            match &status {
                WriteStatus::LocalOnly | WriteStatus::Resolved(_) => {}
                WriteStatus::Published(published) => {
                    let covered = match published.as_ref() {
                        PublishedWrite::Commit(position) => cut.covers_commit(position.commit()),
                        PublishedWrite::Snapshot(position) => {
                            snapshot_write_is_covered(position, cut)
                        }
                    };
                    if !covered {
                        break;
                    }
                }
                WriteStatus::Pending
                | WriteStatus::Publishing
                | WriteStatus::Blocked(_)
                | WriteStatus::LocalOnlyBlocked(_) => break,
            };
            let base = records.effective_store_write_base(
                &WriteId::from_generated(row.write_id.clone()),
                &row.base,
            )?;
            let observed = CommitFrontier::from_refs(base.dependencies.clone())
                .map_err(|error| DbError::context("settled write observed frontier", error))?;
            if matches!(status, WriteStatus::LocalOnly | WriteStatus::Published(_))
                && !cut.covers(&observed)
            {
                break;
            }
            let changeset_hash = row.changeset_hash.parse::<ObjectHash>().map_err(|error| {
                DbError::context(
                    format!("settled write {} changeset hash", row.write_id),
                    error,
                )
            })?;
            records.payload(changeset_hash)?;
            settled.push(crate::SettledStoreWrite {
                ordinal: row.ordinal,
                write_id: WriteId::from_generated(row.write_id),
                status,
                observed: base,
                changeset_hash,
                input_hash: row.input_hash,
            });
        }
        Ok(settled)
    }

    fn retained_write_effect_on(
        records: StoreRecords<'_>,
        write_id: WriteId,
        base: &str,
        changeset_hash: &str,
        local_only: bool,
    ) -> Result<(MergeReplayWriteEffect, CommitFrontier), DbError> {
        let changeset_hash = changeset_hash.parse::<ObjectHash>()?;
        records.payload(changeset_hash)?;
        let base = records.effective_store_write_base(&write_id, base)?;
        let observed = CommitFrontier::from_refs(base.dependencies)
            .map_err(|error| DbError::context("retained write observed frontier", error))?;
        let mut partitions = records.store_write_partitions(write_id.as_str())?;
        if local_only {
            partitions.store = None;
            partitions.circles.clear();
        }
        Ok((
            MergeReplayWriteEffect {
                write_id,
                partitions,
            },
            observed,
        ))
    }

    pub(crate) fn load_folded_replay_journal_on(
        records: StoreRecords<'_>,
        folded: &[crate::SettledStoreWrite],
    ) -> Result<Vec<MergeReplayWrite>, DbError> {
        let rows = records.store_write_replay_rows()?;
        if rows.len() < folded.len() {
            return Err(DbError::Message(
                "folded write prefix extends past the retained journal".to_string(),
            ));
        }
        let mut journal = Vec::with_capacity(folded.len());
        for (settled, row) in folded.iter().zip(rows) {
            if row.ordinal != settled.ordinal
                || row.write_id != settled.write_id.as_str()
                || row.input_hash != settled.input_hash
                || row.changeset_hash != settled.changeset_hash.to_string()
            {
                return Err(DbError::Message(
                    "folded write prefix differs from retained journal input".to_string(),
                ));
            }
            let status: WriteStatus = serde_json::from_str(&row.status).map_err(|error| {
                DbError::context(format!("folded write {} status", row.write_id), error)
            })?;
            let base = records.effective_store_write_base(
                &WriteId::from_generated(row.write_id.clone()),
                &row.base,
            )?;
            if status != settled.status || base != settled.observed {
                return Err(DbError::Message(format!(
                    "folded write {} changed during baseline capture",
                    row.write_id
                )));
            }
            let write = match status {
                WriteStatus::LocalOnly => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        settled.write_id.clone(),
                        &row.base,
                        &row.changeset_hash,
                        true,
                    )?;
                    MergeReplayWrite::LocalOnly { effect, observed }
                }
                WriteStatus::Published(published) => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        settled.write_id.clone(),
                        &row.base,
                        &row.changeset_hash,
                        matches!(published.as_ref(), PublishedWrite::Snapshot(_)),
                    )?;
                    match *published {
                        PublishedWrite::Commit(position) => MergeReplayWrite::Accepted {
                            effect,
                            observed,
                            commit: position.commit,
                        },
                        PublishedWrite::Snapshot(_) => {
                            MergeReplayWrite::LocalOnly { effect, observed }
                        }
                    }
                }
                WriteStatus::Resolved(_) => MergeReplayWrite::Consumed {
                    write_id: settled.write_id.clone(),
                },
                _ => {
                    return Err(DbError::Message(format!(
                        "folded write {} changed status during baseline capture",
                        row.write_id
                    )));
                }
            };
            journal.push(write);
        }
        Ok(journal)
    }

    pub(crate) fn covered_replay_suffix_on(
        records: StoreRecords<'_>,
        baseline: &RetainedReplayBaseline,
        folded: &[crate::SettledStoreWrite],
    ) -> Result<Vec<MergeReplayWriteEffect>, DbError> {
        let prefix = Self::load_folded_replay_journal_on(records, folded)?;
        let mut effects = Vec::new();
        for row in records
            .store_write_replay_rows()?
            .into_iter()
            .skip(prefix.len())
        {
            let write_id = WriteId::from_generated(row.write_id);
            let status: WriteStatus = serde_json::from_str(&row.status)?;
            let candidate = match &status {
                WriteStatus::Publishing | WriteStatus::Blocked(_) => row
                    .prepared
                    .as_deref()
                    .map(|prepared| prepared_state_commit_reference(prepared, &write_id))
                    .transpose()?,
                _ => None,
            };
            if replay_write_is_covered(records, baseline, &write_id, &status, candidate.as_ref())? {
                let (effect, _) = Self::retained_write_effect_on(
                    records,
                    write_id,
                    &row.base,
                    &row.changeset_hash,
                    false,
                )?;
                effects.push(effect);
            }
        }
        Ok(effects)
    }

    pub(crate) fn load_merge_replay_journal_on(
        records: StoreRecords<'_>,
        baseline: &RetainedReplayBaseline,
        active_accepted_writes: &std::collections::BTreeMap<WriteId, StoreBatchCommitRef>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWrite>, DbError> {
        if active_accepted_writes
            .keys()
            .any(|write_id| retracted_writes.contains(write_id))
        {
            return Err(DbError::Message(
                "retained replay classifies one write as active and retracted".to_string(),
            ));
        }
        let mut journal = Vec::new();
        for row in records.store_write_replay_rows()? {
            let encoded_id = row.write_id;
            let write_id = WriteId::from_generated(encoded_id.clone());
            let status: WriteStatus = serde_json::from_str(&row.status).map_err(|error| {
                DbError::context(format!("retained replay write {encoded_id} status"), error)
            })?;
            let active = active_accepted_writes.get(&write_id);
            let retracted = retracted_writes.contains(&write_id);
            let write = match status {
                WriteStatus::LocalOnly | WriteStatus::LocalOnlyBlocked(_) => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        write_id,
                        &row.base,
                        &row.changeset_hash,
                        false,
                    )?;
                    if effect.partitions.store.is_some() || !effect.partitions.circles.is_empty() {
                        return Err(DbError::Message(format!(
                            "Local-only write {encoded_id} carries a shared partition"
                        )));
                    }
                    MergeReplayWrite::LocalOnly { effect, observed }
                }
                WriteStatus::Pending => {
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        write_id,
                        &row.base,
                        &row.changeset_hash,
                        false,
                    )?;
                    MergeReplayWrite::Unaccepted { effect, observed }
                }
                WriteStatus::Publishing | WriteStatus::Blocked(_) => {
                    if retracted {
                        return Err(DbError::Message(format!(
                            "unresolved write {encoded_id} is already terminally retracted"
                        )));
                    }
                    let candidate = row
                        .prepared
                        .as_deref()
                        .map(|prepared| prepared_state_commit_reference(prepared, &write_id))
                        .transpose()?;
                    let covered = replay_write_is_covered(
                        records,
                        baseline,
                        &write_id,
                        &status,
                        candidate.as_ref(),
                    )?;
                    let accepted = match (active, candidate.as_ref()) {
                        (Some(reference), Some(candidate)) if reference == candidate => {
                            Some(reference.clone())
                        }
                        (Some(_), _) => {
                            return Err(DbError::Message(format!(
                                "unresolved write {encoded_id} differs from its accepted candidate"
                            )));
                        }
                        (None, _) => None,
                    };
                    let (effect, observed) = Self::retained_write_effect_on(
                        records,
                        write_id,
                        &row.base,
                        &row.changeset_hash,
                        covered,
                    )?;
                    if covered {
                        MergeReplayWrite::LocalOnly { effect, observed }
                    } else {
                        match accepted {
                            Some(commit) => MergeReplayWrite::Accepted {
                                effect,
                                observed,
                                commit,
                            },
                            None => MergeReplayWrite::Unaccepted { effect, observed },
                        }
                    }
                }
                WriteStatus::Published(published) => {
                    let covered = if !retracted
                        || matches!(published.as_ref(), PublishedWrite::Snapshot(_))
                    {
                        replay_write_is_covered(
                            records,
                            baseline,
                            &write_id,
                            &WriteStatus::Published(published.clone()),
                            None,
                        )?
                    } else {
                        false
                    };
                    match *published {
                        PublishedWrite::Snapshot(position) => {
                            if retracted
                                || active.is_some_and(|commit| commit.coord != position.coord)
                            {
                                return Err(DbError::Message(format!(
                                    "snapshot-covered write {encoded_id} has a conflicting replay association"
                                )));
                            }
                            let (effect, observed) = Self::retained_write_effect_on(
                                records,
                                write_id,
                                &row.base,
                                &row.changeset_hash,
                                true,
                            )?;
                            MergeReplayWrite::LocalOnly { effect, observed }
                        }
                        PublishedWrite::Commit(position) => {
                            if retracted {
                                MergeReplayWrite::Consumed { write_id }
                            } else if covered {
                                let (effect, observed) = Self::retained_write_effect_on(
                                    records,
                                    write_id,
                                    &row.base,
                                    &row.changeset_hash,
                                    true,
                                )?;
                                MergeReplayWrite::LocalOnly { effect, observed }
                            } else {
                                let active = active.ok_or_else(|| {
                                    DbError::Message(format!(
                                        "published write {encoded_id} has no retained replay input"
                                    ))
                                })?;
                                if active != position.commit() {
                                    return Err(DbError::Message(format!(
                                        "published write {encoded_id} is associated with another accepted commit"
                                    )));
                                }
                                let (effect, observed) = Self::retained_write_effect_on(
                                    records,
                                    write_id,
                                    &row.base,
                                    &row.changeset_hash,
                                    false,
                                )?;
                                MergeReplayWrite::Accepted {
                                    effect,
                                    observed,
                                    commit: active.clone(),
                                }
                            }
                        }
                    }
                }
                WriteStatus::Resolved(_) => MergeReplayWrite::Consumed { write_id },
            };
            journal.push(write);
        }
        Ok(journal)
    }

    pub(crate) fn load_merge_replay_associations_on(
        records: StoreRecords<'_>,
        baseline: &RetainedReplayBaseline,
        active_accepted_writes: &std::collections::BTreeMap<WriteId, StoreBatchCommitRef>,
        retracted_writes: &BTreeSet<WriteId>,
    ) -> Result<Vec<MergeReplayWrite>, DbError> {
        let associations = Self::load_merge_replay_journal_on(
            records,
            baseline,
            active_accepted_writes,
            retracted_writes,
        )?
        .into_iter()
        .filter_map(|write| match write {
            MergeReplayWrite::Accepted {
                mut effect,
                observed,
                commit,
            } => {
                effect.partitions.local = None;
                Some(MergeReplayWrite::Accepted {
                    effect,
                    observed,
                    commit,
                })
            }
            MergeReplayWrite::LocalOnly { .. }
            | MergeReplayWrite::Unaccepted { .. }
            | MergeReplayWrite::Consumed { .. } => None,
        })
        .collect::<Vec<_>>();
        Ok(associations)
    }

    pub(crate) fn load_replay_baseline_on(
        records: StoreRecords<'_>,
    ) -> Result<RetainedReplayBaseline, DbError> {
        load_replay_baseline_on(records)?
            .ok_or_else(|| DbError::Message("retained replay baseline is absent".to_string()))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn load_retained_merge_replay_inputs_on(
        records: StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        registrations: &mut dyn VerifiedRegistrationLookup,
    ) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
        let rows = records.retained_materialization_rows()?;
        rows.into_iter()
            .map(|(stream_id, sequence, encoded_ref, input_hash)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                let commit_ref = crate::store::materialized_commit_index::parse_stored_commit_ref(
                    &stream_id,
                    sequence,
                    &encoded_ref,
                )?;
                Self::load_retained_merge_materialization_on(
                    records,
                    root,
                    registrations,
                    &stream_id,
                    sequence,
                    &commit_ref,
                    &input_hash,
                )
            })
            .collect()
    }
}
