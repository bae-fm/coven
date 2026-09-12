use super::verified_store_authority::verify_prepared_store_commit_on;
use super::*;
use coven_protocol::store_commit::{
    AcceptedStoreSnapshotRef, CommitFrontier, StorePublicationBase,
};
use coven_protocol::write::WriteId;

impl VerifiedStoreTransaction<'_, '_, '_, '_> {
    pub(super) fn rebase_unpublished_store_writes(
        &mut self,
        snapshot: &AcceptedStoreSnapshotRef,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        membership: coven_protocol::membership::LocalStoreMembership,
    ) -> Result<Vec<coven_foundation::changeset::RowChange>, DbError> {
        if super::active_store_publication::load_active_store_publication_on(
            self.store.transaction,
        )?
        .is_some_and(|active| active.is_discarding())
        {
            return Err(DbError::Message(
                "snapshot adoption must wait for the reserved write discard to finish".to_string(),
            ));
        }
        let frontier = CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(
                self.store.transaction,
                None,
            )?,
        )?;
        let mut replay = self.authority.replay_projection_result_on(
            self.store,
            self.blob_decls,
            self.gates,
            self.synced_tables,
            routing_key,
            Some(&frontier),
            crate::ReplayJournal::Rebase,
            membership,
        )?;
        let suffix = replay.take_unaccepted_journal();
        if replay.materialized_frontier()? != frontier {
            return Err(DbError::Message(
                "unpublished write rebase did not realize the accepted frontier".to_string(),
            ));
        }
        let base = StorePublicationBase::Snapshot(snapshot.clone());
        for write in suffix {
            let (effect, observed) = match write {
                crate::MergeReplayWrite::Unaccepted { effect, observed }
                | crate::MergeReplayWrite::LocalOnly { effect, observed } => (effect, observed),
                crate::MergeReplayWrite::Consumed { .. } => continue,
                crate::MergeReplayWrite::Accepted { .. } => {
                    return Err(DbError::Message(
                        "an accepted write remains behind the unresolved journal suffix"
                            .to_string(),
                    ));
                }
            };
            if !frontier.covers(&observed) {
                return Err(DbError::Message(format!(
                    "recorded write {} observes history absent from the accepted rebase frontier",
                    effect.write_id
                )));
            }
            let write_id = effect.write_id.clone();
            let candidate = self.prepared_rebase_candidate(&write_id)?;
            if candidate
                .as_ref()
                .is_some_and(|candidate| candidate.publication_base == base)
            {
                replay.restore_unaccepted_write(self, effect, routing_key)?;
                continue;
            }
            replay.rebase_write(self, effect, &base, routing_key)?;
            if let Some(candidate) = candidate {
                self.retire_rebased_candidate(&write_id, snapshot, candidate)?;
            }
        }
        replay.install_on(self)
    }

    fn prepared_rebase_candidate(
        &mut self,
        write_id: &WriteId,
    ) -> Result<Option<coven_protocol::store_commit::VerifiedStoreBatchCommit>, DbError> {
        let tx = self.store.transaction;
        let prepared: Option<String> = tx.query_row(
            "SELECT prepared FROM store_writes WHERE write_id = ?1",
            [write_id.as_str()],
            |row| row.get(0),
        )?;
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        let prepared: super::publication_state::PreparedStoreWriteState =
            serde_json::from_str(&prepared)
                .map_err(|error| DbError::context("rebased prepared Store write", error))?;
        let root = self.authority.root().clone();
        let commit = verify_prepared_store_commit_on(
            self.authority,
            StoreRecords::new(tx, self.store.store_dir),
            &root,
            &prepared,
        )?;
        Ok(Some(commit))
    }

    pub(super) fn snapshot_candidate_nonactivation(
        &mut self,
        snapshot: &AcceptedStoreSnapshotRef,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<coven_protocol::remote_object::CandidateNonactivation, DbError> {
        let tx = self.store.transaction;
        let baseline = super::retained_replay::load_replay_baseline_metadata_on(
            StoreRecords::new(tx, self.store.store_dir),
        )?
        .ok_or_else(|| {
            DbError::Message("candidate rebase has no installed snapshot".to_string())
        })?;
        if !matches!(&baseline.authority, crate::RetainedReplayAuthority::InstalledSnapshot(authority)
            if authority.snapshot == snapshot.snapshot)
        {
            return Err(DbError::Message(
                "candidate retirement names another installed snapshot".to_string(),
            ));
        }
        let entries = super::observed_store_publication::load_store_publication_entries_on(tx)?;
        let snapshot_entry = entries
            .iter()
            .find(|entry| {
                entry.value.payload
                    == coven_protocol::store_commit::StorePublicationPayload::Snapshot(
                        snapshot.snapshot.clone(),
                    )
                    && entry.value.position == snapshot.publication.position
            })
            .ok_or_else(|| {
                DbError::Message(
                    "candidate retirement has no retained accepted snapshot entry".to_string(),
                )
            })?;
        if coven_protocol::store_commit::StorePublicationRef::from_entry(
            &snapshot_entry.value,
            snapshot_entry.prepared.reference().clone(),
        )? != snapshot.publication
        {
            return Err(DbError::Message(
                "candidate retirement differs from the exact accepted snapshot publication"
                    .to_string(),
            ));
        }
        if entries.iter().any(|entry| {
            matches!(&entry.value.payload,
            coven_protocol::store_commit::StorePublicationPayload::Commit(accepted)
                if accepted.coord == commit.reference().coord)
        }) {
            return Err(DbError::Message(
                "accepted candidate must settle before snapshot rebase".to_string(),
            ));
        }
        coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
            commit.reference(),
            commit.value(),
            coven_protocol::remote_object::CandidateNonactivationProof::SnapshotRetirement {
                snapshot: snapshot.clone(),
                coverage: baseline.coverage().clone(),
            },
        )
        .map_err(DbError::from)
    }

    fn retire_rebased_candidate(
        &mut self,
        write_id: &WriteId,
        snapshot: &AcceptedStoreSnapshotRef,
        commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<(), DbError> {
        let tx = self.store.transaction;
        let active = super::active_store_publication::load_active_store_publication_on(tx)?
            .ok_or_else(|| {
                DbError::Message(
                    "prepared rebase candidate has no publication reservation".to_string(),
                )
            })?;
        if active.owner() != &crate::ActiveStorePublicationOwner::StoreWrite(write_id.clone())
            || active.commit_reservation()
                != Some((
                    write_id,
                    &commit.author_registration,
                    &commit.reference().coord,
                ))
        {
            return Err(DbError::Message(
                "rebase candidate differs from the retained author reservation".to_string(),
            ));
        }
        let proof = self.snapshot_candidate_nonactivation(snapshot, &commit)?;
        let blobs =
            crate::load_prepared_audience_objects_on(tx, self.store.store_dir, write_id)?.blobs;
        let mut publications = vec![active.attempt()?.reference()?];
        if let Some(prior) = active.superseded_entry() {
            publications.push(prior.clone());
        }
        publications.sort();
        publications.dedup();
        let replacement = active.await_preparation(crate::RetiredStoreCandidate {
            nonactivation: proof,
            inputs: crate::RetiredStoreCandidateInputs::Write(blobs),
            publications,
        })?;
        super::active_store_publication::update_active_store_publication_on(
            tx,
            &active,
            &replacement,
        )?;
        tx.execute(
            "DELETE FROM store_write_packages WHERE write_id = ?1",
            [write_id.as_str()],
        )?;
        tx.execute(
            "DELETE FROM store_write_blobs WHERE write_id = ?1",
            [write_id.as_str()],
        )?;
        tx.execute(
            "UPDATE store_writes SET prepared = NULL, status = '\"pending\"' WHERE write_id = ?1",
            [write_id.as_str()],
        )?;
        Ok(())
    }
}

impl StoreTransaction<'_, '_> {
    pub(super) fn replace_rebased_store_write(
        self,
        write_id: &WriteId,
        original_hash: crate::ObjectHash,
        original_facts: &crate::StoreWriteBlobFacts,
        rebased: &crate::write_models::RebasedStoreWrite,
        partitions: &[crate::AudiencePartition],
    ) -> Result<(), DbError> {
        let payloads = self.replace_store_write_partitions(
            write_id,
            partitions,
            [original_hash, rebased.changeset_hash]
                .into_iter()
                .chain(original_facts.captured_payloads())
                .chain(rebased.blob_facts.captured_payloads())
                .collect(),
        )?;
        crate::payload_store::set_payload_owner_claims_on(
            self.transaction,
            &crate::payload_store::store_write_owner_key(write_id),
            &payloads,
        )?;
        for fact in original_facts.blobs.iter().chain(&rebased.blob_facts.blobs) {
            if fact.blob.provenance == coven_protocol::blob::Provenance::HostProvided {
                self.transaction.execute(
                    "INSERT OR IGNORE INTO store_write_blob_leases (write_id, namespace, blob_id) VALUES (?1, ?2, ?3)",
                    (write_id.as_str(), &fact.blob.namespace, &fact.blob.id),
                )?;
            }
        }
        let encoded = serde_json::to_string(rebased)
            .map_err(|error| DbError::context("serialize rebased Store write", error))?;
        if self.transaction.execute(
            "UPDATE store_writes SET rebased = ?2 WHERE write_id = ?1 AND changeset_hash = ?3",
            (write_id.as_str(), encoded, original_hash.to_string()),
        )? != 1
        {
            return Err(DbError::Message(
                "recorded write changed during atomic rebase".to_string(),
            ));
        }
        Ok(())
    }
}
