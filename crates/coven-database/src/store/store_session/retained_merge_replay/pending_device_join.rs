use super::*;
use coven_protocol::store_commit::{
    AcceptedStoreSnapshotRef, StorePublicationBase, StorePublicationPayload,
};

impl StoreDatabase {
    pub(crate) fn pending_device_join_retention_on(
        records: StoreRecords<'_>,
        authority: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        coverage: &CommitFrontier,
        baseline: &RetainedReplayBaseline,
    ) -> Result<BTreeMap<AcceptedStoreSnapshotRef, BTreeSet<StoreBatchCommitRef>>, DbError> {
        let (_, state) = records.store_device_state_for_history_cut(
            &coven_protocol::store_commit::StoreHistoryCut(coverage.commits().clone()),
        )?;
        let mut inputs = Vec::new();
        for encoded in records.retained_materialization_refs()? {
            let reference: StoreBatchCommitRef = serde_json::from_str(&encoded)?;
            if baseline.coverage().covers_commit(&reference) || !coverage.covers_commit(&reference)
            {
                continue;
            }
            let input = authority.retained_materialization_by_ref_on(records, &reference)?;
            if input.root() != root {
                return Err(DbError::Message(
                    "pending Join input belongs to another Store".into(),
                ));
            }
            inputs.push(input);
        }
        let summary = match &baseline.authority {
            RetainedReplayAuthority::InstalledSnapshot(snapshot) => {
                if snapshot.store_root != *root {
                    return Err(DbError::Message(
                        "pending Join baseline belongs to another Store".into(),
                    ));
                }
                Some(&snapshot.metadata.history_summary)
            }
            RetainedReplayAuthority::Genesis(_) => None,
        };
        let accepted = inputs
            .iter()
            .map(|input| input.commit())
            .chain(summary.into_iter().flat_map(|summary| {
                summary
                    .membership_proofs
                    .values()
                    .map(|proof| &proof.commit_value)
            }))
            .collect::<Vec<_>>();
        let mut retained = BTreeMap::<_, BTreeSet<_>>::new();
        if let Some(summary) = summary {
            for (activation, closure) in &summary.pending_device_joins {
                let opening = closure.verified_commit(activation)?;
                if !opening.has_pending_device_join_bootstrap(
                    &state,
                    coverage,
                    accepted.iter().copied(),
                )? {
                    continue;
                }
                let snapshot = closure
                    .publication
                    .current
                    .latest_snapshot()
                    .ok_or_else(|| {
                        DbError::Message("pending Join has no accepted snapshot".into())
                    })?;
                retained.entry(snapshot.clone()).or_default().extend(
                    closure
                        .commits
                        .iter()
                        .map(|commit| commit.reference.clone()),
                );
            }
        }
        let mut fresh = Vec::new();
        for input in &inputs {
            if !input.commit().has_pending_device_join_bootstrap(
                &state,
                coverage,
                accepted.iter().copied(),
            )? {
                continue;
            }
            let StorePublicationBase::Snapshot(snapshot) = input.commit().publication_base() else {
                return Err(DbError::Message(
                    "pending Join has no snapshot publication base".into(),
                ));
            };
            let exact = input.acceptance().exact_publication().ok_or_else(|| {
                DbError::Message("new pending Join has no exact accepted publication".into())
            })?;
            let mut terminal = exact.reference().clone();
            if input
                .commit()
                .pending_bootstrap_registration(&state, coverage)?
                .is_none()
            {
                // A separate Attempt is captured at the actual first snapshot
                // predecessor, so its plan includes the whole accepted suffix
                // from the original image through that capture.
                for covered in &inputs {
                    let accepted = covered.acceptance().exact_publication().ok_or_else(|| {
                        DbError::Message("pending Join suffix has no exact publication".into())
                    })?;
                    if accepted.reference().position > terminal.position {
                        terminal = accepted.reference().clone();
                    }
                }
            }
            fresh.push((
                snapshot.clone(),
                terminal,
                exact.reference().clone(),
                input.commit_ref().clone(),
            ));
        }
        if !fresh.is_empty() {
            let entries = records.store_publication_entries()?;
            for (snapshot, terminal, activation_publication, activation) in fresh {
                let includes_base = entries.iter().any(|entry| {
                    entry.prepared.reference() == &snapshot.publication.object
                        && entry.value.payload
                            == StorePublicationPayload::Snapshot(snapshot.snapshot.clone())
                });
                let includes_terminal = entries.iter().any(|entry| {
                    entry.prepared.reference() == &activation_publication.object
                        && entry.value.payload
                            == StorePublicationPayload::Commit(activation.clone())
                });
                if !includes_base || !includes_terminal {
                    return Err(DbError::Message(
                        "pending Join interval is absent from retained publication history".into(),
                    ));
                }
                let required = retained.entry(snapshot.clone()).or_default();
                for entry in &entries {
                    if entry.value.position > snapshot.publication.position
                        && entry.value.position <= terminal.position
                    {
                        if let StorePublicationPayload::Commit(reference) = &entry.value.payload {
                            required.insert(reference.clone());
                        }
                    }
                }
            }
        }
        Ok(retained)
    }
}
