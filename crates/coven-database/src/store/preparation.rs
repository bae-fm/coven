use super::{
    candidate_records::parse_prepared_merge_candidate_parts_on,
    publication_state::{
        MergeAbandonmentOutcome, MergeCandidateAbandonmentPreparation, PreparedStoreWriteState,
        StoreWritePreparation,
    },
    StoreDatabase, StoreSession,
};
use crate::{
    persist_exact_remote_object_on, DbError, DurablePreparedProtocolObject, StoreWriteBase,
    LOCAL_DEVICE_ID_STATE_KEY,
};
use coven_protocol::remote_object::RemoteObjectRecord;
use coven_protocol::store_commit::{
    CommitFrontier, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
};
use coven_protocol::write::WriteStatus;
use rusqlite::OptionalExtension;

impl StoreSession<'_> {
    fn table_schema_for_apply(&mut self) -> Result<crate::TableSchema, DbError> {
        crate::TableSchema::for_apply(self.records.conn, self.synced_tables, self.gates)
    }

    fn prepare_store_write_commit(&mut self, stage: StoreWritePreparation) -> Result<(), DbError> {
        let author = self.activated_registration(&stage.commit.value.author_registration)?;
        if author.value().store_root != stage.root {
            return Err(DbError::Message(
                "prepared Store write belongs to another verified Store root".to_string(),
            ));
        }
        let records = self.records;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let local_device_id = crate::required_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
        let registration_object: String = tx
            .query_row(
                "SELECT registration_object \
                 FROM store_device_registration_activations WHERE device_id = ?1",
                [&local_device_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let registration_ref: StoreDeviceRegistrationRef =
            serde_json::from_str(&registration_object).map_err(|error| {
                DbError::context("prepared write exact registration ref", error)
            })?;
        if registration_ref != stage.commit.value.author_registration
            || registration_ref != stage.head.value.author_registration
        {
            return Err(DbError::Message(
                "prepared Store commit/head author registration differs from local activation"
                    .to_string(),
            ));
        }
        let registration = stage.commit.value.author();
        if author.value() != registration {
            return Err(DbError::Message(
                "prepared write author registration differs from its activated bytes".to_string(),
            ));
        }
        let stream_id = coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            stage.root.store_root_hash,
            &registration_ref,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let expected_coord = StoreCommitCoord {
            stream_id,
            sequence: stage.commit.value.seq(),
        };
        if stage.commit.value.store_root_hash() != stage.root.store_root_hash
            || stage.commit.value.reference().coord != expected_coord
            || stage.commit.value.reference().object != *stage.commit.prepared.reference()
        {
            return Err(DbError::Message(
                "authenticated prepared Store commit differs from its current local authority"
                    .to_string(),
            ));
        }
        if stage.commit.value.write_id != stage.write_id {
            return Err(DbError::Message(
                "prepared write id differs from signed commit".to_string(),
            ));
        }
        let commit_ref = stage.commit.value.reference().clone();
        if stage.head.value.commit != commit_ref {
            return Err(DbError::Message(
                "prepared Store head does not activate the exact prepared commit".to_string(),
            ));
        }
        stage
            .history_evidence
            .validate_for(&commit_ref, stage.commit.value.value())
            .map_err(|error| DbError::context("prepared Store history evidence", error))?;
        StoreDeviceHead::parse_at(
            &stage.head.value.to_bytes(),
            stage.root.store_root_hash,
            registration,
            &commit_ref,
        )
        .map_err(|error| DbError::context("verify prepared Store head", error))?;
        let (stored_base, stored_status, stored_preparation): (String, String, Option<String>) = tx
            .query_row(
                "SELECT base, status, prepared
                 FROM store_writes WHERE write_id = ?1",
                [stage.write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)?;
        if stored_status != "\"pending\"" || stored_preparation.is_some() {
            return Err(DbError::Message(format!(
                "write {} is not an unprepared pending write",
                stage.write_id
            )));
        }
        let partitions = StoreDatabase::store_write_partitions_on(
            crate::payload_spool::StoreRecords::new(&tx, records.store_dir),
            stage.write_id.as_str(),
        )?;
        let stored_base: StoreWriteBase = serde_json::from_str(&stored_base)
            .map_err(|error| DbError::context(format!("write {} base", stage.write_id), error))?;
        let stored_dependencies = CommitFrontier::from_refs(stored_base.dependencies)
            .map_err(|error| DbError::context("stored write dependencies", error))?;
        if stored_dependencies.commits() != stage.commit.value.merge_dependencies() {
            return Err(DbError::Message(format!(
                "prepared commit dependencies differ from write {}",
                stage.write_id
            )));
        }
        let another_prepared: Option<String> = tx
            .query_row(
                "SELECT write_id FROM store_writes
                 WHERE prepared IS NOT NULL AND write_id != ?1
                 ORDER BY ordinal LIMIT 1",
                [stage.write_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(DbError::from)?;
        if let Some(other_write_id) = another_prepared {
            return Err(DbError::Message(format!(
                "write {other_write_id} already owns Store publication"
            )));
        }
        let durable_predecessor =
            StoreDatabase::latest_position_for_device_on(&tx, &stream_id.to_string())?;
        let expected_seq = durable_predecessor
            .as_ref()
            .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
        if stage.commit.value.seq() != expected_seq
            || stage.commit.value.order.predecessor() != durable_predecessor.as_ref()
        {
            return Err(DbError::Message(format!(
                "outbound Store commit exact predecessor differs from durable {durable_predecessor:?}"
            )));
        }

        let mut object_ids = std::collections::BTreeSet::new();
        for remote in &stage.remote_objects {
            remote
                .validate()
                .map_err(|error| DbError::context("prepared remote object", error))?;
            if !object_ids.insert(remote.object_id()) {
                return Err(DbError::Message(
                    "prepared write contains a duplicate remote object".to_string(),
                ));
            }
        }
        crate::validate_prepared_audience_blob_graph(&object_ids, &stage.audiences)?;
        for remote in &stage.remote_objects {
            crate::persist_prepared_remote_object_on(
                &tx,
                records.store_dir,
                remote,
                &commit_ref,
                "candidate audience object",
            )?;
        }
        let commit_remote = RemoteObjectRecord::candidate_commit(
            commit_ref.clone(),
            &stage.commit.value.to_bytes(),
            stage.commit.prepared.stored_bytes(),
        )
        .map_err(|error| DbError::context("prepared candidate commit", error))?;
        persist_exact_remote_object_on(&tx, records.store_dir, &commit_remote, "candidate commit")?;
        let expected_partition_count = usize::from(partitions.store.is_some())
            .checked_add(partitions.circles.len())
            .ok_or_else(|| DbError::Message("audience partition count overflow".to_string()))?;
        if stage.audiences.packages.len() != expected_partition_count {
            return Err(DbError::Message(
                "prepared audience packages do not cover every write partition".to_string(),
            ));
        }
        let mut indexed = std::collections::BTreeSet::new();
        for package in &stage.audiences.packages {
            let value = package.package();
            if value.store_root_hash() != stage.root.store_root_hash
                || value.write_id() != &stage.write_id
                || value.commit_coord() != &commit_ref.coord
                || value.candidate_family() != stage.commit.value.candidate_family()
            {
                return Err(DbError::Message(
                    "prepared audience package differs from its exact Store commit".to_string(),
                ));
            }
            match value.audience() {
                coven_protocol::audience_package::PackageAudience::Store => {
                    let partition = partitions.store.as_ref().ok_or_else(|| {
                        DbError::Message(
                            "prepared Store package has no Store partition".to_string(),
                        )
                    })?;
                    if value.changeset() != partition.changeset {
                        return Err(DbError::Message(
                            "prepared Store package changeset differs from its partition"
                                .to_string(),
                        ));
                    }
                    stage
                        .commit
                        .value
                        .verify_store_package(package.semantic_bytes())
                        .map_err(|error| DbError::Message(error.to_string()))?;
                }
                coven_protocol::audience_package::PackageAudience::Circle { circle_id, .. } => {
                    let partition = partitions
                        .circles
                        .iter()
                        .find(|partition| {
                            partition.audience
                                == coven_protocol::circle::Audience::Circle(*circle_id)
                        })
                        .ok_or_else(|| {
                            DbError::Message(format!(
                                "prepared Circle package {circle_id} has no partition"
                            ))
                        })?;
                    if value.changeset() != partition.changeset {
                        return Err(DbError::Message(format!(
                            "prepared Circle package {circle_id} changeset differs from its partition"
                        )));
                    }
                    stage
                        .commit
                        .value
                        .verify_circle_package(*circle_id, package.semantic_bytes())
                        .map_err(|error| DbError::Message(error.to_string()))?;
                }
            }
            indexed.insert(package.remote_object_id());
        }
        indexed.extend(
            stage
                .audiences
                .blobs
                .iter()
                .map(crate::PreparedAudienceBlob::remote_object_id),
        );
        debug_assert_eq!(indexed, object_ids);
        super::prepared_remote_objects::persist_prepared_audience_objects_on(
            &tx,
            records.store_dir,
            &stage.write_id,
            &stage.audiences.packages,
            &stage.audiences.blobs,
        )?;
        let head_ref = coven_protocol::store_commit::StoreDeviceHeadRef {
            head_hash: stage.head.value.head_hash(),
            object: stage.head.prepared.reference().clone(),
        };
        let head_remote = RemoteObjectRecord::candidate_activated_store_head(
            head_ref,
            &stage.head.value.to_bytes(),
            stage.head.prepared.stored_bytes(),
            commit_ref.clone(),
        )
        .map_err(|error| DbError::context("prepared Store head", error))?;
        persist_exact_remote_object_on(&tx, records.store_dir, &head_remote, "Store head")?;

        let prepared = PreparedStoreWriteState::Publication {
            commit: DurablePreparedProtocolObject::new(
                stage.commit.value.to_bytes(),
                stage.commit.prepared,
            ),
            head: DurablePreparedProtocolObject::new(
                stage.head.value.to_bytes(),
                stage.head.prepared,
            ),
            history_evidence: stage.history_evidence,
            local_cleanup: stage.local_cleanup,
            completion: stage.completion,
        };
        let prepared = serde_json::to_string(&prepared)
            .map_err(|error| DbError::context("serialize prepared Store write", error))?;
        let status = serde_json::to_string(&WriteStatus::Publishing)
            .map_err(|error| DbError::context("serialize write status", error))?;
        let updated = tx
            .execute(
                "UPDATE store_writes SET prepared = ?2, status = ?3
                 WHERE write_id = ?1 AND prepared IS NULL AND status = '\"pending\"'",
                rusqlite::params![stage.write_id.as_str(), prepared, status],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(format!(
                "write {} lost pending preparation ownership",
                stage.write_id
            )));
        }
        tx.commit().map_err(DbError::from)?;
        Ok(())
    }

    fn prepare_merge_candidate_abandonment(
        &mut self,
        stage: MergeCandidateAbandonmentPreparation,
    ) -> Result<(), DbError> {
        let records = self.records;
        let verified_authority = &mut *self.verified_store_authority;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let (raw_status, raw_prepared): (String, String) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [stage.write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("Merge abandonment status", error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(format!(
                "write {} is not blocked",
                stage.write_id
            )));
        }
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("prepared Merge candidate", error))?;
        let PreparedStoreWriteState::Publication {
            commit: candidate_commit,
            head: candidate_head,
            history_evidence: candidate_history_evidence,
            local_cleanup,
            completion,
        } = prepared
        else {
            return Err(DbError::Message(
                "Merge abandonment requires one prepared candidate".to_string(),
            ));
        };
        let candidate = parse_prepared_merge_candidate_parts_on(
            crate::payload_spool::StoreRecords::new(&tx, records.store_dir),
            verified_authority,
            candidate_commit.semantic_bytes(),
            candidate_commit.prepared().reference(),
            candidate_head.semantic_bytes(),
            candidate_head.prepared().reference(),
        )?;
        if candidate.commit.write_id != stage.write_id {
            return Err(DbError::Message(
                "prepared Merge candidate differs from its write identity".to_string(),
            ));
        }
        let tx_records = crate::payload_spool::StoreRecords::new(&tx, records.store_dir);
        let root = verified_authority.required_root_authority_on(tx_records)?;
        let registration = verified_authority.activated_registration_on(
            tx_records,
            &root,
            &candidate.commit.author_registration,
        )?;
        if stage.commit.value.store_root_hash() != root.store_root_hash
            || stage.commit.value.author() != &registration
            || stage.commit.value.reference().coord != candidate.reference.coord
            || stage.commit.value.reference().object != *stage.commit.prepared.reference()
        {
            return Err(DbError::Message(
                "authenticated Merge abandonment commit differs from its current local authority"
                    .to_string(),
            ));
        }
        if stage.commit.value.write_id != stage.write_id
            || stage.commit.value.abandoned_candidates()
                != [coven_protocol::store_commit::CandidateCleanupManifest {
                    candidate: coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
                        coord: candidate.reference.coord.clone(),
                        object: candidate.reference.object.clone(),
                        canonical_signed_bytes: candidate.canonical_signed_bytes.clone(),
                    },
                }]
        {
            return Err(DbError::Message(
                "Merge abandonment does not name its exact prepared candidate".to_string(),
            ));
        }
        let authority_ref = stage.commit.value.reference().clone();
        if stage.head.value.commit != authority_ref
            || stage.head.prepared.reference().slot() != candidate.head_object.slot()
            || stage.head.value.successor != candidate.head.successor
            || stage.head.value.author_registration != candidate.commit.author_registration
        {
            return Err(DbError::Message(
                "Merge abandonment head differs from the candidate competition point".to_string(),
            ));
        }
        stage
            .history_evidence
            .validate_for(&authority_ref, stage.commit.value.value())
            .map_err(|error| DbError::context("Merge abandonment history evidence", error))?;
        StoreDeviceHead::parse_at(
            &stage.head.value.to_bytes(),
            root.store_root_hash,
            &registration,
            &authority_ref,
        )
        .map_err(|error| DbError::context("verify Merge abandonment head", error))?;
        let authority_commit = RemoteObjectRecord::candidate_commit(
            authority_ref.clone(),
            &stage.commit.value.to_bytes(),
            stage.commit.prepared.stored_bytes(),
        )
        .map_err(|error| DbError::context("Merge abandonment commit", error))?;
        persist_exact_remote_object_on(
            &tx,
            records.store_dir,
            &authority_commit,
            "Merge abandonment commit",
        )?;
        let authority_head_ref = coven_protocol::store_commit::StoreDeviceHeadRef {
            head_hash: stage.head.value.head_hash(),
            object: stage.head.prepared.reference().clone(),
        };
        let authority_head = RemoteObjectRecord::candidate_activated_store_head(
            authority_head_ref,
            &stage.head.value.to_bytes(),
            stage.head.prepared.stored_bytes(),
            authority_ref,
        )
        .map_err(|error| DbError::context("Merge abandonment head", error))?;
        persist_exact_remote_object_on(
            &tx,
            records.store_dir,
            &authority_head,
            "Merge abandonment head",
        )?;
        let replacement = PreparedStoreWriteState::MergeAbandonment {
            candidate_commit,
            candidate_head,
            candidate_history_evidence,
            authority_commit: DurablePreparedProtocolObject::new(
                stage.commit.value.to_bytes(),
                stage.commit.prepared,
            ),
            authority_head: DurablePreparedProtocolObject::new(
                stage.head.value.to_bytes(),
                stage.head.prepared,
            ),
            authority_history_evidence: stage.history_evidence,
            outcome: MergeAbandonmentOutcome::Prepared,
            local_cleanup,
            completion,
        };
        let replacement = serde_json::to_string(&replacement)
            .map_err(|error| DbError::context("serialize Merge abandonment", error))?;
        let publishing = serde_json::to_string(&WriteStatus::Publishing)
            .map_err(|error| DbError::context("serialize Merge abandonment status", error))?;
        let updated = tx
            .execute(
                "UPDATE store_writes SET prepared = ?2, status = ?3
                 WHERE write_id = ?1 AND prepared = ?4
                   AND json_extract(status, '$.blocked') IS NOT NULL",
                rusqlite::params![
                    stage.write_id.as_str(),
                    replacement,
                    publishing,
                    raw_prepared
                ],
            )
            .map_err(DbError::from)?;
        if updated != 1 {
            return Err(DbError::Message(
                "blocked Merge candidate changed during abandonment preparation".to_string(),
            ));
        }
        tx.commit().map_err(DbError::from)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn enqueue_store_changeset_for_test(
        &mut self,
        write_id: coven_protocol::write::WriteId,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let records = self.records;
        let tx = records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let local_stream_id = self.verified_store_authority.local_merge_stream_id_on(
            crate::payload_spool::StoreRecords::new(&tx, records.store_dir),
        )?;
        let base = StoreWriteBase {
            dependencies: StoreDatabase::materialized_frontier_on(&tx, local_stream_id.as_deref())?,
        };
        let partitions = vec![crate::AudiencePartition {
            audience: coven_protocol::circle::Audience::Store,
            control: None,
            changeset: changeset.clone(),
        }];
        let blob_facts =
            StoreDatabase::capture_partition_blob_facts_on(&tx, &partitions, self.blob_decls)?;
        let changeset_hash =
            crate::payload_spool::write_payload_blocking(records.store_dir, &changeset)?;
        StoreDatabase::insert_store_write_on(
            crate::payload_spool::StoreRecordTransaction::new(&tx, records.store_dir),
            &write_id,
            &partitions,
            changeset_hash,
            &base,
            &blob_facts,
            1,
        )?;
        tx.commit().map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn table_schema_for_apply(&self) -> Result<crate::TableSchema, DbError> {
        self.connection
            .call_store(|session| session.table_schema_for_apply())
            .await
    }

    pub async fn prepare_store_write_commit(
        &self,
        stage: StoreWritePreparation,
    ) -> Result<(), DbError> {
        let write_id = stage.write_id.clone();
        self.connection
            .call_store(move |session| session.prepare_store_write_commit(stage))
            .await?;
        self.notify_write_status(write_id, WriteStatus::Publishing);
        Ok(())
    }

    pub async fn prepare_merge_candidate_abandonment(
        &self,
        stage: MergeCandidateAbandonmentPreparation,
    ) -> Result<(), DbError> {
        let notified_write_id = stage.write_id.clone();
        self.connection
            .call_store(move |session| session.prepare_merge_candidate_abandonment(stage))
            .await?;
        self.notify_write_status(notified_write_id, WriteStatus::Publishing);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn enqueue_store_changeset_for_test(
        &self,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let write_id = self.new_store_write_id();
        self.connection
            .call_store(move |session| {
                session.enqueue_store_changeset_for_test(write_id, changeset)
            })
            .await
    }
}
