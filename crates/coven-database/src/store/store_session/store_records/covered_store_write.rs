use super::StoreRecords;
use crate::{
    ActiveStorePublication, ActiveStorePublicationOwner, DbError, RetainedReplayAuthority,
};
use coven_protocol::store_commit::VerifiedStoreBatchCommit;
use coven_protocol::write::SnapshotCoveredPosition;

/// A reserved logical write included in the installed snapshot, without an
/// assertion about which of its exact candidates was accepted.
#[derive(Debug, Clone)]
pub struct CoveredStoreWrite {
    active: ActiveStorePublication,
    candidate: VerifiedStoreBatchCommit,
    position: SnapshotCoveredPosition,
    protected_objects: std::collections::BTreeSet<coven_protocol::objects::ExactObjectRef>,
}

impl StoreRecords<'_> {
    pub(crate) fn covered_store_write(
        self,
        candidate: &VerifiedStoreBatchCommit,
    ) -> Result<Option<CoveredStoreWrite>, DbError> {
        let Some(active) = crate::store::store_session::active_store_publication::load_active_store_publication_on(self.conn)? else {
            return Ok(None);
        };
        if active.owner() != &ActiveStorePublicationOwner::StoreWrite(candidate.write_id.clone()) {
            return Ok(None);
        }
        let Some(baseline) =
            crate::store::store_session::retained_replay::load_replay_baseline_metadata_on(self)?
        else {
            return Ok(None);
        };
        let RetainedReplayAuthority::InstalledSnapshot(authority) = &baseline.authority else {
            return Ok(None);
        };
        if !self.snapshot_covers_reserved_write(
            &candidate.write_id,
            candidate.reference(),
            authority,
        )? {
            return Ok(None);
        }
        if authority.store_root.store_root_hash != candidate.store_root_hash() {
            return Err(DbError::Message(
                "covered write belongs to another Store".into(),
            ));
        }
        active.attempt()?.verify_commit(candidate)?;
        if active.covered_write_position().is_none()
            && authority
                .metadata
                .history_summary
                .causal_cut
                .get(&candidate.reference().coord)
                == Some(candidate.reference())
        {
            // This snapshot authenticates the exact candidate, so completion
            // retains its commit receipt and activated object ownership. A
            // logical completion already begun must keep its durable outcome.
            return Ok(None);
        }
        let coven_protocol::store_commit::StoreCommitBody::Operations(operations) = &candidate.body
        else {
            return Err(DbError::Message(
                "covered row write contains a control operation".into(),
            ));
        };
        if operations.acknowledgement.is_some()
            || !operations.circle_acknowledgements.is_empty()
            || operations.control.is_some()
            || !operations.device_join_attempt_decisions.is_empty()
            || !operations.provider_access_grants.is_empty()
            || !operations.device_registrations.is_empty()
            || !operations.device_exclusion_proposals.is_empty()
            || !operations.device_exclusion_outcomes.is_empty()
            || !operations.stream_activations.is_empty()
            || !operations.circle_controls.is_empty()
        {
            return Err(DbError::Message(
                "covered row write contains authority operations".into(),
            ));
        }

        let current = crate::store::store_session::observed_store_publication::load_store_current_publication_on(self.conn)?;
        let snapshot = current
            .record()
            .latest_snapshot()
            .filter(|snapshot| snapshot.snapshot == authority.snapshot)
            .ok_or_else(|| {
                DbError::Message("covered write baseline differs from the accepted snapshot".into())
            })?;
        let position = match active.covered_write_position() {
            Some(position) => {
                if position.author_registration != candidate.author_registration
                    || position.coord != candidate.reference().coord
                    || position.snapshot.publication.position > snapshot.publication.position
                    || position.snapshot.publication.position == snapshot.publication.position
                        && position.snapshot != *snapshot
                {
                    return Err(DbError::Message(
                        "covered completion lost its accepted snapshot boundary".into(),
                    ));
                }
                position.clone()
            }
            None => SnapshotCoveredPosition {
                author_registration: candidate.author_registration.clone(),
                coord: candidate.reference().coord.clone(),
                snapshot: snapshot.clone(),
            },
        };
        Ok(Some(CoveredStoreWrite {
            active,
            candidate: candidate.clone(),
            position,
            protected_objects: authority
                .metadata
                .history_summary
                .pending_device_join_artifacts()?,
        }))
    }

    fn covered_write_preparation(
        self,
        proof: &CoveredStoreWrite,
    ) -> Result<crate::store::publication_state::PreparedStoreWriteState, DbError> {
        let (status, prepared): (String, String) = self.conn.query_row(
            "SELECT status, prepared FROM store_writes WHERE write_id = ?1 AND prepared IS NOT NULL",
            [proof.candidate.write_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let status: coven_protocol::write::WriteStatus = serde_json::from_str(&status)
            .map_err(|error| DbError::context("covered write status", error))?;
        let prepared: crate::store::publication_state::PreparedStoreWriteState =
            serde_json::from_str(&prepared)
                .map_err(|error| DbError::context("covered write preparation", error))?;
        if status != coven_protocol::write::WriteStatus::Publishing
            || prepared.commit.semantic_bytes() != proof.candidate.value().to_bytes()
            || prepared.commit.prepared().reference() != &proof.candidate.reference().object
        {
            return Err(DbError::Message(
                "covered write differs from its durable preparation".into(),
            ));
        }
        Ok(prepared)
    }

    fn covered_write_releases(
        self,
        proof: &CoveredStoreWrite,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::ObjectHash,
            coven_protocol::remote_object::PendingCandidateRelease,
        )>,
        DbError,
    > {
        let audiences = crate::load_prepared_audience_objects_on(
            self.conn,
            self.store_dir,
            &proof.candidate.write_id,
        )?;
        let mut objects = crate::candidate_graph_exact_objects(proof.candidate.value())?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        objects.insert(proof.candidate.reference().object.clone());
        objects.extend(
            audiences
                .blobs
                .iter()
                .map(|blob| blob.blob().object().clone()),
        );
        objects
            .into_iter()
            .map(|object| {
                let id = coven_protocol::remote_object::remote_object_id(&object);
                let record = crate::load_remote_object_on(self.conn, id)?;
                if record.object() != &object {
                    return Err(DbError::Message(
                        "covered write object differs from its ownership row".into(),
                    ));
                }
                let released = record.release_pending_candidate(proof.candidate.reference())?;
                Ok((id, released))
            })
            .collect()
    }

    fn begin_covered_write_completion(
        self,
        expected: CoveredStoreWrite,
    ) -> Result<CoveredStoreWriteCompletion, DbError> {
        self.transaction(|store| {
            let records = StoreRecords::new(store.transaction, store.store_dir);
            let mut proof = records.covered_store_write(&expected.candidate)?
                .ok_or_else(|| DbError::Message("reserved write is no longer covered by its installed snapshot".into()))?;
            if proof.active != expected.active || proof.position != expected.position {
                return Err(DbError::Message("covered write changed before completion began".into()));
            }
            records.covered_write_preparation(&proof)?;
            let releases = records.covered_write_releases(&proof)?;
            let mut publication_entries = std::collections::BTreeSet::from([proof.active.attempt()?.entry_object.clone()]);
            if let Some(superseded) = proof.active.superseded_entry() {
                publication_entries.insert(superseded.object.clone());
            }
            publication_entries.retain(|object| !proof.protected_objects.contains(object));
            if proof.active.covered_write_position().is_none() {
                let active = proof.active.begin_covered_write_completion(proof.position.clone())?;
                crate::store::store_session::active_store_publication::update_active_store_publication_on(
                    store.transaction, &proof.active, &active,
                )?;
                proof.active = active;
            }
            Ok(crate::store::store_session::StoreTransactionOutcome::Commit(CoveredStoreWriteCompletion { proof, releases, publication_entries }))
        })
    }

    fn complete_covered_write(
        self,
        completion: CoveredStoreWriteCompletion,
    ) -> Result<
        (
            coven_protocol::write::WriteId,
            coven_protocol::write::WriteStatus,
        ),
        DbError,
    > {
        self.transaction(|store| {
            let records = StoreRecords::new(store.transaction, store.store_dir);
            let proof = records.covered_store_write(&completion.proof.candidate)?
                .ok_or_else(|| DbError::Message("covered completion lost its installed snapshot".into()))?;
            if proof.active != completion.proof.active || proof.position != completion.proof.position
                || proof.active.covered_write_position() != Some(&proof.position)
            {
                return Err(DbError::Message("covered completion lost its durable owner".into()));
            }
            let prepared = records.covered_write_preparation(&proof)?;
            let releases = records.covered_write_releases(&proof)?;
            // Import may add or retire owners of objects this completion never
            // deletes. Persist their current ownership, while requiring every
            // physical deletion target to remain exactly the one prepared.
            if releases.len() != completion.releases.len()
                || releases.iter().zip(&completion.releases).any(|((id, current), (expected_id, expected))| {
                    id != expected_id || match (current, expected) {
                        (coven_protocol::remote_object::PendingCandidateRelease::Retained(_),
                         coven_protocol::remote_object::PendingCandidateRelease::Retained(_)) => false,
                        _ => current != expected,
                    }
                })
            {
                return Err(DbError::Message("covered write deletion targets changed during cleanup".into()));
            }
            let write_id = proof.candidate.write_id.clone();
            let audiences = crate::load_prepared_audience_objects_on(store.transaction, store.store_dir, &write_id)?;
            let mut completed_active = proof.active.clone();
            if completed_active.superseded_entry().is_some() {
                completed_active.complete_superseded_entry_cleanup()?;
                crate::store::store_session::active_store_publication::update_active_store_publication_on(
                    store.transaction, &proof.active, &completed_active,
                )?;
            }
            let status = crate::store::store_session::publication::finish_store_write_publication_on(
                store, &write_id, &audiences, prepared.local_cleanup,
                coven_protocol::write::PublishedWrite::Snapshot(proof.position), &completed_active,
            )?;
            for (id, release) in releases {
                match release {
                    coven_protocol::remote_object::PendingCandidateRelease::Retained(record) => {
                        crate::update_remote_object_on(store.transaction, id, &record)?;
                    }
                    coven_protocol::remote_object::PendingCandidateRelease::DeleteProtocol(_)
                    | coven_protocol::remote_object::PendingCandidateRelease::DeleteBlob(_) => {
                        crate::store::store_session::candidate_records::delete_remote_objects_on(
                            store.transaction, [id], "covered write cleanup",
                        )?;
                    }
                }
            }
            for blob in &audiences.blobs {
                if let Some(path) = blob.spool_path() {
                    if path != store.store_dir.outbound_blob_spool_path(blob.blob().locator().locator_hash()) {
                        return Err(DbError::Message("covered blob spool differs from its locator".into()));
                    }
                    if crate::store::store_session::active_store_publication::retained_blob_spool_has_claim_on(store.transaction, path, None)? {
                        continue;
                    }
                    match std::fs::remove_file(path) {
                        Ok(()) => {},
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
                        Err(source) => return Err(coven_foundation::atomic_file::FileError::Path {
                            operation: "remove covered write blob spool", path: path.to_path_buf(), source,
                        }.into()),
                    }
                    coven_foundation::atomic_file::sync_parent_dir_blocking(path)?;
                }
            }
            Ok(crate::store::store_session::StoreTransactionOutcome::Commit((write_id, status)))
        })
    }
}

impl crate::StoreDatabase {
    pub async fn covered_store_write(
        &self,
        candidate: VerifiedStoreBatchCommit,
    ) -> Result<Option<CoveredStoreWrite>, DbError> {
        self.call_store(move |session| {
            StoreRecords::new(session.conn, session.store_dir).covered_store_write(&candidate)
        })
        .await
    }

    pub async fn begin_covered_write_completion(
        &self,
        proof: CoveredStoreWrite,
    ) -> Result<CoveredStoreWriteCompletion, DbError> {
        self.call_store(move |session| {
            StoreRecords::new(session.conn, session.store_dir).begin_covered_write_completion(proof)
        })
        .await
    }

    pub async fn complete_covered_write(
        &self,
        completion: CoveredStoreWriteCompletion,
    ) -> Result<(), DbError> {
        let (write_id, status) = self
            .call_store(move |session| {
                StoreRecords::new(session.conn, session.store_dir)
                    .complete_covered_write(completion)
            })
            .await?;
        self.notify_write_status(write_id, status);
        Ok(())
    }
}

/// Cleanup still belongs to the reserved write until its exact remote objects
/// and local spools have been retired and the receipt commits atomically.
#[derive(Debug)]
pub struct CoveredStoreWriteCompletion {
    proof: CoveredStoreWrite,
    releases: Vec<(
        coven_protocol::store_commit::ObjectHash,
        coven_protocol::remote_object::PendingCandidateRelease,
    )>,
    publication_entries: std::collections::BTreeSet<coven_protocol::objects::ExactObjectRef>,
}

impl CoveredStoreWriteCompletion {
    pub fn protocol_objects(
        &self,
    ) -> impl Iterator<Item = &coven_protocol::objects::ExactObjectRef> {
        self.releases
            .iter()
            .filter_map(|(_, release)| match release {
                coven_protocol::remote_object::PendingCandidateRelease::DeleteProtocol(object) => {
                    Some(object)
                }
                _ => None,
            })
            .chain(self.publication_entries.iter())
    }

    pub fn blob_objects(
        &self,
    ) -> impl Iterator<Item = &coven_protocol::blob::locator::StoredBlobRef> {
        self.releases
            .iter()
            .filter_map(|(_, release)| match release {
                coven_protocol::remote_object::PendingCandidateRelease::DeleteBlob(blob) => {
                    Some(blob)
                }
                _ => None,
            })
    }
}
