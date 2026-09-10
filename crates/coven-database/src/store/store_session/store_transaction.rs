use coven_foundation::store_dir::StoreDir;
use coven_protocol::remote_object::{remote_object_id, RemoteObjectRecord};
use coven_protocol::store_commit::ObjectHash;
use coven_protocol::write::{AffectedRow, WriteId, WriteResolution, WriteStatus};

use super::payload_store::PayloadStoreError;
use super::publication_state::PreparedStoreWriteState;
use super::{StoreRecords, StoreTransaction};
use crate::{
    candidate_graph_exact_objects, is_routing_table, load_remote_object_on, AudiencePartition,
    CirclePartitionControl, Database, DbError, StoreWriteBase, StoreWriteBlobFacts,
};

struct UnpublishedWriteCleanup {
    removable: Vec<ObjectHash>,
    candidate: Option<coven_protocol::store_commit::StoreBatchCommitRef>,
    active_publication: Option<crate::ActiveStorePublication>,
}

impl<'store, 'connection> StoreTransaction<'store, 'connection> {
    pub(super) fn new(
        transaction: &'store rusqlite::Transaction<'connection>,
        store_dir: &'store StoreDir,
    ) -> Self {
        Self {
            transaction,
            store_dir,
        }
    }

    pub(super) fn require_accepted_membership(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        membership: &coven_protocol::membership::MembershipChain,
        publication: &coven_protocol::store_commit::StorePublicationRef,
    ) -> Result<coven_protocol::store_commit::CommitFrontier, DbError> {
        use coven_protocol::membership::MembershipFloor;
        use coven_protocol::store_commit::CommitFrontier;
        use std::collections::BTreeSet;
        let records = StoreRecords::new(self.transaction, self.store_dir);
        let root = authority.required_root_authority_on(records)?;
        let boundary =
            super::observed_store_publication::load_store_current_publication_on(self.transaction)?;
        let coverage = CommitFrontier::from_refs(records.materialized_frontier()?)?;
        if boundary.record().accepted() != Some(publication) {
            return Err(DbError::StorePublicationChanged);
        }
        if records.store_publication_entries()?.iter().any(|entry| {
            matches!(&entry.value.payload,
                coven_protocol::store_commit::StorePublicationPayload::Commit(commit)
                    if !coverage.covers_commit(commit))
        }) {
            return Err(DbError::Message(
                "accepted membership verification cannot pass an accepted held publication".into(),
            ));
        }
        let baseline = authority.retained_replay_baseline_on(records)?.clone();
        let inputs = authority.retained_replay_inputs_on(records, &root)?;
        let mut heads = Vec::new();
        let mut entries = BTreeSet::new();
        let mut proofs = Vec::new();
        if let crate::RetainedReplayAuthority::InstalledSnapshot(snapshot) = &baseline.authority {
            heads.extend(snapshot.metadata.state.membership.heads.iter().cloned());
            proofs.extend(snapshot.metadata.history_summary.membership_proofs.values());
        }
        for input in &inputs {
            if baseline.coverage().covers_commit(input.commit_ref()) {
                continue;
            }
            if !coverage.covers_commit(input.commit_ref()) {
                return Err(DbError::Message(
                    "accepted membership verification membership input is outside its accepted coverage".into(),
                ));
            }
            heads.extend(input.commit().membership_state.heads.iter().cloned());
            if let Some(proof) = &input.history_evidence().membership_proof {
                proofs.push(proof.as_ref());
            }
        }
        for proof in proofs {
            entries.insert(proof.entry.coord.clone());
            heads.push(proof.head.clone());
            if let Some(predecessor) = proof.head_value.body.predecessor_head() {
                // In particular, the first accepted successor authenticates
                // the creation-owned Founder entry without a receipt of its own.
                entries.insert(predecessor.coord.clone());
            }
        }
        entries.extend(heads.iter().map(|head| head.coord.clone()));
        let heads = MembershipFloor::from_heads(heads).map_err(|error| {
            DbError::Message(format!(
                "accepted membership verification membership heads: {error}"
            ))
        })?;
        let actual_entries = membership
            .entries()
            .iter()
            .map(|entry| entry.coord())
            .collect::<BTreeSet<_>>();
        if actual_entries != entries || membership.head_refs() != heads.0.as_slice() {
            return Err(DbError::Message(
                "accepted membership verification membership differs from accepted history".into(),
            ));
        }
        Ok(coverage)
    }

    pub(crate) fn install_payload(&self, bytes: &[u8]) -> Result<ObjectHash, PayloadStoreError> {
        StoreRecords::new(self.transaction, self.store_dir).install_payload(bytes)
    }

    pub(crate) fn payload(&self, hash: ObjectHash) -> Result<Vec<u8>, PayloadStoreError> {
        StoreRecords::new(self.transaction, self.store_dir).payload(hash)
    }

    pub(crate) fn store_write_partitions(
        self,
        write_id: &str,
    ) -> Result<crate::PreparedStoreWritePartitions, DbError> {
        StoreRecords::new(self.transaction, self.store_dir).store_write_partitions(write_id)
    }

    pub(super) fn ensure_founder_replay_baseline(
        self,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: crate::RetainedReplayGenesisAuthority,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        super::retained_replay::ensure_founder_replay_baseline_on(
            StoreRecords::new(self.transaction, self.store_dir),
            schema_version,
            routing_hash,
            authority,
        )
    }

    pub(super) fn install_generation_zero_replay_baseline(
        self,
        schema_version: u32,
        routing_hash: ObjectHash,
        authority: crate::RetainedReplayGenesisAuthority,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        super::retained_replay::install_generation_zero_replay_baseline_on(
            StoreRecords::new(self.transaction, self.store_dir),
            schema_version,
            routing_hash,
            authority,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn advance_owner_promotion_journal(
        self,
        journal_key: String,
        target_key: String,
        previous_value: String,
        next_value: String,
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    ) -> Result<(), DbError> {
        super::owner_promotion::advance_owner_promotion_journal_on(
            self.transaction,
            self.store_dir,
            journal_key,
            target_key,
            previous_value,
            next_value,
            remote_objects,
            None,
        )
    }

    pub(super) fn replace_tables_from_projection(
        self,
        source: &super::ReplayProjection,
        tables: &[String],
    ) -> Result<(), DbError> {
        super::replay_projection::replace_tables_from_projection_on(
            source,
            self.transaction,
            tables,
        )
    }

    pub(crate) fn insert_store_write(
        self,
        write_id: &WriteId,
        partitions: &[AudiencePartition],
        changeset_hash: ObjectHash,
        base: &StoreWriteBase,
        blob_facts: &StoreWriteBlobFacts,
    ) -> Result<WriteStatus, DbError> {
        let tx = self.transaction;
        let remote_partitions = partitions
            .iter()
            .filter(|partition| partition.audience != coven_protocol::circle::Audience::Local)
            .collect::<Vec<_>>();
        let affected_rows = if remote_partitions.is_empty() {
            Vec::new()
        } else {
            let mut affected = Vec::new();
            for partition in &remote_partitions {
                affected.extend(
                    crate::walk_changeset(&partition.changeset)
                        .map_err(DbError::from)?
                        .into_iter()
                        .filter(|row| !is_routing_table(&row.table))
                        .map(|row| {
                            let primary_key = row.pk().map(str::to_owned).ok_or_else(|| {
                                DbError::Message(format!(
                                    "shared write row in {:?} has no primary key",
                                    row.table
                                ))
                            })?;
                            Ok(AffectedRow {
                                table: row.table,
                                primary_key,
                            })
                        })
                        .collect::<Result<Vec<_>, DbError>>()?,
                );
            }
            affected.sort();
            affected.dedup();
            affected
        };
        let status = if remote_partitions.is_empty() {
            WriteStatus::LocalOnly
        } else {
            WriteStatus::Pending
        };
        let base = serde_json::to_string(base)
            .map_err(|error| DbError::context("serialize Store observed frontier", error))?;
        let status_json = serde_json::to_string(&status)
            .map_err(|error| DbError::context("serialize write status", error))?;
        let affected_rows = serde_json::to_string(&affected_rows)
            .map_err(|error| DbError::context("serialize affected rows", error))?;
        let blob_facts_json = serde_json::to_string(blob_facts)
            .map_err(|error| DbError::context("serialize Store write blob facts", error))?;
        tx.execute(
            "INSERT INTO store_writes
             (write_id, status, affected_rows, changeset_hash, base, blob_facts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                write_id.as_str(),
                status_json,
                affected_rows,
                changeset_hash.to_string(),
                Some(base),
                blob_facts_json,
            ],
        )
        .map_err(DbError::from)?;
        let payloads = self.replace_store_write_partitions(
            write_id,
            partitions,
            std::iter::once(changeset_hash)
                .chain(blob_facts.captured_payloads())
                .collect(),
        )?;
        crate::payload_store::set_payload_owner_claims_on(
            tx,
            &crate::payload_store::store_write_owner_key(write_id),
            &payloads,
        )?;
        for fact in &blob_facts.blobs {
            if fact.blob.provenance != coven_protocol::blob::Provenance::HostProvided {
                continue;
            }
            tx.execute(
                "INSERT OR IGNORE INTO store_write_blob_leases
                 (write_id, namespace, blob_id) VALUES (?1, ?2, ?3)",
                (write_id.as_str(), &fact.blob.namespace, &fact.blob.id),
            )
            .map_err(DbError::from)?;
        }
        Ok(status)
    }

    pub(super) fn replace_store_write_partitions(
        self,
        write_id: &WriteId,
        partitions: &[AudiencePartition],
        mut payloads: std::collections::BTreeSet<ObjectHash>,
    ) -> Result<std::collections::BTreeSet<ObjectHash>, DbError> {
        let tx = self.transaction;
        tx.execute(
            "DELETE FROM store_write_partitions WHERE write_id = ?1",
            [write_id.as_str()],
        )?;
        for partition in partitions {
            let audience = match partition.audience {
                coven_protocol::circle::Audience::Store => "store".to_string(),
                coven_protocol::circle::Audience::Local => "local".to_string(),
                coven_protocol::circle::Audience::Circle(circle_id) => circle_id.to_string(),
            };
            let control = partition
                .control
                .as_ref()
                .map(CirclePartitionControl::stored_json);
            let partition_hash = self.install_payload(&partition.changeset)?;
            payloads.insert(partition_hash);
            tx.execute(
                "INSERT INTO store_write_partitions
                 (write_id, audience, control_coord, changeset_hash)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    write_id.as_str(),
                    audience,
                    control,
                    partition_hash.to_string()
                ],
            )
            .map_err(DbError::from)?;
        }
        Ok(payloads)
    }

    fn unpublished_write_cleanup(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_id: &WriteId,
    ) -> Result<UnpublishedWriteCleanup, DbError> {
        let tx = self.transaction;
        let raw_prepared: Option<String> = tx
            .query_row(
                "SELECT prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let mut removable = Vec::new();
        let mut candidate = None;
        let mut active_publication =
            super::active_store_publication::load_active_store_publication_on(tx)?.filter(
                |active| {
                    active.owner()
                        == &crate::ActiveStorePublicationOwner::StoreWrite(write_id.clone())
                },
            );
        if let Some(raw_prepared) = raw_prepared.as_deref() {
            let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                .map_err(|error| DbError::context("resolved prepared write", error))?;
            let merge = authority.verified_prepared_store_commit_on(
                StoreRecords::new(self.transaction, self.store_dir),
                &prepared,
            )?;
            let reference = merge.reference().clone();
            removable.push(remote_object_id(&reference.object));
            let active = super::active_store_publication::load_active_store_publication_on(tx)?
                .ok_or_else(|| {
                    DbError::Message(format!(
                        "prepared write {write_id} has no active Store publication"
                    ))
                })?;
            if active.owner() != &crate::ActiveStorePublicationOwner::StoreWrite(write_id.clone())
                || active.commit_reservation()
                    != Some((write_id, &merge.author_registration, &reference.coord))
            {
                return Err(DbError::Message(format!(
                    "prepared write {write_id} differs from active Store publication {:?}",
                    active.owner()
                )));
            }
            removable.extend(
                candidate_graph_exact_objects(merge.value())?
                    .iter()
                    .map(remote_object_id),
            );
            candidate = Some(reference);
            active_publication = Some(active);
        }
        let mut statement = tx
            .prepare("SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1")
            .map_err(DbError::from)?;
        let indexed = statement
            .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in indexed {
            removable.push(
                encoded
                    .parse()
                    .map_err(|error| DbError::context("resolved remote object id", error))?,
            );
        }
        Ok(UnpublishedWriteCleanup {
            removable,
            candidate,
            active_publication,
        })
    }

    fn unpublished_write_cleanup_complete(
        self,
        cleanup: &UnpublishedWriteCleanup,
    ) -> Result<bool, DbError> {
        if cleanup.active_publication.as_ref().is_some_and(|active| {
            active.is_awaiting_preparation()
                || !active.retired_candidates().is_empty()
                || active.superseded_entry().is_some()
        }) {
            return Ok(false);
        }
        let Some(candidate) = &cleanup.candidate else {
            return Ok(true);
        };
        for object_id in &cleanup.removable {
            let remote = load_remote_object_on(self.transaction, *object_id)?;
            if !remote
                .candidate_cleanup_complete(candidate)
                .map_err(|error| {
                    DbError::context(format!("validate candidate cleanup for {object_id}"), error)
                })?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn unpublished_write_cleanup_is_complete(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_id: &WriteId,
    ) -> Result<bool, DbError> {
        let cleanup = self.unpublished_write_cleanup(authority, write_id)?;
        self.unpublished_write_cleanup_complete(&cleanup)
    }

    pub(super) fn resolve_unpublished_writes(
        self,
        authority: &mut super::VerifiedStoreAuthority,
        write_ids: &[WriteId],
        resolution: &WriteResolution,
    ) -> Result<(), DbError> {
        let tx = self.transaction;
        let status = WriteStatus::Resolved(resolution.clone());
        for write_id in write_ids {
            let cleanup = self.unpublished_write_cleanup(authority, write_id)?;
            if !self.unpublished_write_cleanup_complete(&cleanup)? {
                return Err(DbError::Message(format!(
                    "candidate cleanup for write {write_id} is incomplete"
                )));
            }
            tx.execute(
                "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM store_write_packages WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            tx.execute(
                "DELETE FROM store_write_blobs WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            for object_id in cleanup.removable {
                let remote = load_remote_object_on(tx, object_id)?;
                let absent = matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        coven_protocol::remote_object::CandidateCommitRecord {
                            state:
                                coven_protocol::remote_object::CandidateCommitState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::CandidateExclusive(
                        coven_protocol::remote_object::CandidateObjectRecord {
                            state:
                                coven_protocol::remote_object::CandidateObjectState::AbsentVerified { .. },
                            ..
                        }
                    )
                );
                if absent {
                    crate::remote_object_records::delete_remote_object_on(tx, object_id)?;
                }
            }
            tx.execute(
                "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            if let Some(active) = cleanup.active_publication.as_ref() {
                super::active_store_publication::clear_active_store_publication_on(tx, active)?;
            }
            Database::set_write_status_on(tx, write_id, &status)?;
        }
        Ok(())
    }
}
