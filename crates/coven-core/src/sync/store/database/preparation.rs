use super::{
    candidate_records::parse_prepared_merge_candidate_parts_on,
    publication_state::{
        MergeAbandonmentOutcome, MergeCandidateAbandonmentPreparation, PreparedStoreWriteState,
        StoreWritePreparation,
    },
    StoreDatabase,
};
use crate::database::{
    load_activated_registration_on, persist_exact_remote_object_on,
    required_store_root_authority_on, DbError, DurablePreparedProtocolObject, StoreWriteBase,
    LOCAL_DEVICE_ID_STATE_KEY,
};
use crate::sync::remote_object::RemoteObjectRecord;
use crate::sync::store_commit::{
    CommitFrontier, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistration,
    StoreDeviceRegistrationRef,
};
use crate::write::WriteStatus;
use rusqlite::OptionalExtension;

impl StoreDatabase {
    #[cfg(test)]
    pub(crate) async fn enqueue_store_changeset_for_test(
        &self,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let write_id = self.database.new_write_id();
        let blob_decls = self.database.blob_decls();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let local_stream_id = crate::database::local_merge_stream_id_on(&tx)?;
                let base = StoreWriteBase {
                    dependencies:
                        crate::sync::store::database::StoreDatabase::materialized_frontier_on(
                            &tx,
                            local_stream_id.as_deref(),
                        )?,
                };
                let inverse_changeset = StoreDatabase::invert_changeset(&changeset)?;
                let partitions = vec![crate::sync::gate::AudiencePartition {
                    audience: crate::sync::circle::Audience::Store,
                    control: None,
                    changeset,
                }];
                let blob_facts =
                    StoreDatabase::capture_partition_blob_facts_on(&tx, &partitions, &blob_decls)?;
                StoreDatabase::insert_store_write_on(
                    &tx,
                    &write_id,
                    &partitions,
                    &inverse_changeset,
                    &base,
                    &blob_facts,
                    1,
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn prepare_store_write_commit(
        &self,
        stage: StoreWritePreparation,
    ) -> Result<(), DbError> {
        let write_id = stage.write_id.clone();
        self.database.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id =
                crate::database::required_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
            let root = required_store_root_authority_on(&tx)?;
            let (registration_bytes, registration_object): (Vec<u8>, String) = tx.query_row(
                "SELECT registration_bytes, registration_object \
                 FROM store_device_registration_activations WHERE device_id = ?1",
                [&local_device_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).map_err(DbError::from)?;
            let registration_ref: StoreDeviceRegistrationRef =
                serde_json::from_str(&registration_object).map_err(|error| {
                    DbError::Message(format!("prepared write exact registration ref: {error}"))
                })?;
            if registration_ref != stage.commit.value.author_registration
                || registration_ref != stage.head.value.author_registration
            {
                return Err(DbError::Message(
                    "prepared Store commit/head author registration differs from local activation"
                        .to_string(),
                ));
            }
            let registration = StoreDeviceRegistration::parse_at(
                &registration_bytes,
                &root,
                registration_ref.device_id,
            ).map_err(|error| DbError::Message(format!(
                "prepared write author registration: {error}"
            )))?;
            registration_ref.verify_registration(&registration)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let stream_id = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &registration_ref,
                crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
            let expected_coord = StoreCommitCoord {
                stream_id,
                sequence: stage.commit.value.seq(),
            };
            if stage.commit.value.store_root_hash() != root.store_root_hash
                || stage.commit.value.author() != &registration
                || stage.commit.value.reference().coord != expected_coord
                || stage.commit.value.reference().object
                    != *stage.commit.prepared.reference()
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
            if stage.history_summary.digest() != stage.head.value.history_summary
                || stage.history_summary.causal_cut.get(&commit_ref.coord) != Some(&commit_ref)
            {
                return Err(DbError::Message(
                    "prepared Store history summary differs from its signed head".to_string(),
                ));
            }
            StoreDeviceHead::parse_at(
                &stage.head.value.to_bytes(),
                root.store_root_hash,
                &registration,
                &commit_ref,
            ).map_err(|error| DbError::Message(format!(
                "verify prepared Store head: {error}"
            )))?;
            let (stored_changeset, stored_base, stored_status, stored_preparation): (
                Vec<u8>,
                String,
                String,
                Option<String>,
            ) = tx
                .query_row(
                    "SELECT changeset, base, status, prepared
                     FROM store_writes WHERE write_id = ?1",
                    [stage.write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(DbError::from)?;
            if stored_status != "\"pending\"" || stored_preparation.is_some() {
                return Err(DbError::Message(format!(
                    "write {} is not an unprepared pending write",
                    stage.write_id
                )));
            }
            let partitions = StoreDatabase::store_write_partitions_on(
                &tx,
                stage.write_id.as_str(),
                &stored_changeset,
            )?;
            let stored_base: StoreWriteBase =
                serde_json::from_str(&stored_base).map_err(|error| {
                    DbError::Message(format!("write {} base: {error}", stage.write_id))
                })?;
            let stored_dependencies = CommitFrontier::from_refs(stored_base.dependencies)
                .map_err(|error| {
                    DbError::Message(format!("stored write dependencies: {error}"))
                })?;
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
                crate::sync::store::database::StoreDatabase::latest_position_for_device_on(&tx, &stream_id.to_string())?;
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

            Self::persist_closed_write_objects_on(
                &tx,
                &stage.write_id,
                root.store_root_hash,
                &commit_ref,
                stage.commit.value.value(),
                stage.commit.prepared.stored_bytes(),
                &partitions,
                &stage.remote_objects,
                &stage.audiences,
            )?;
            let head_ref = crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: stage.head.value.head_hash(),
                object: stage.head.prepared.reference().clone(),
            };
            let head_remote = RemoteObjectRecord::candidate_activated_store_head(
                head_ref,
                stage.head.value.to_bytes(),
                stage.head.prepared.stored_bytes().to_vec(),
                commit_ref.clone(),
            )
            .map_err(|error| DbError::Message(format!("prepared Store head: {error}")))?;
            persist_exact_remote_object_on(&tx, &head_remote, "Store head")?;

            let prepared = PreparedStoreWriteState::Publication {
                commit: DurablePreparedProtocolObject::new(
                    stage.commit.value.to_bytes(),
                    stage.commit.prepared,
                ),
                head: DurablePreparedProtocolObject::new(
                    stage.head.value.to_bytes(),
                    stage.head.prepared,
                ),
                history_summary: stage.history_summary,
                local_cleanup: stage.local_cleanup,
                completion: stage.completion,
            };
            let prepared = serde_json::to_string(&prepared).map_err(|error| {
                DbError::Message(format!("serialize prepared Store write: {error}"))
            })?;
            let status = serde_json::to_string(&WriteStatus::Publishing)
                .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
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
        })
        .await?;
        self.database
            .notify_write_status(write_id, WriteStatus::Publishing);
        Ok(())
    }

    pub(crate) async fn prepare_merge_candidate_abandonment(
        &self,
        stage: MergeCandidateAbandonmentPreparation,
    ) -> Result<(), DbError> {
        let notified_write_id = stage.write_id.clone();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [stage.write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("Merge abandonment status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(format!(
                        "write {} is not blocked",
                        stage.write_id
                    )));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("prepared Merge candidate: {error}"))
                    })?;
                let PreparedStoreWriteState::Publication {
                    commit: candidate_commit,
                    head: candidate_head,
                    history_summary: candidate_history_summary,
                    local_cleanup,
                    completion,
                } = prepared
                else {
                    return Err(DbError::Message(
                        "Merge abandonment requires one prepared candidate".to_string(),
                    ));
                };
                let candidate = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &candidate_commit,
                    &candidate_head,
                )?;
                if candidate.commit.write_id != stage.write_id {
                    return Err(DbError::Message(
                        "prepared Merge candidate differs from its write identity".to_string(),
                    ));
                }
                let root = required_store_root_authority_on(&tx)?;
                let registration = load_activated_registration_on(
                    &tx,
                    &root,
                    &candidate.commit.author_registration,
                )?;
                if stage.commit.value.store_root_hash() != root.store_root_hash
                    || stage.commit.value.author() != &registration
                    || stage.commit.value.reference().coord != candidate.reference.coord
                    || stage.commit.value.reference().object
                        != *stage.commit.prepared.reference()
                {
                    return Err(DbError::Message(
                        "authenticated Merge abandonment commit differs from its current local authority"
                            .to_string(),
                    ));
                }
                if stage.commit.value.write_id != stage.write_id
                    || stage.commit.value.abandoned_candidates()
                        != [crate::sync::store_commit::CandidateCleanupManifest {
                            candidate: crate::sync::store_commit::StoreBatchCommitDeletionTarget {
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
                    || stage.history_summary.digest() != stage.head.value.history_summary
                    || stage.history_summary.causal_cut.get(&authority_ref.coord)
                        != Some(&authority_ref)
                    || stage.head.prepared.reference().slot()
                        != candidate.head_prepared.reference().slot()
                    || stage.head.value.successor != candidate.head.successor
                    || stage.head.value.author_registration != candidate.commit.author_registration
                {
                    return Err(DbError::Message(
                        "Merge abandonment head differs from the candidate competition point"
                            .to_string(),
                    ));
                }
                StoreDeviceHead::parse_at(
                    &stage.head.value.to_bytes(),
                    root.store_root_hash,
                    &registration,
                    &authority_ref,
                )
                .map_err(|error| {
                    DbError::Message(format!("verify Merge abandonment head: {error}"))
                })?;
                let authority_commit = RemoteObjectRecord::candidate_commit(
                    authority_ref.clone(),
                    stage.commit.value.to_bytes(),
                    stage.commit.prepared.stored_bytes().to_vec(),
                )
                .map_err(|error| DbError::Message(format!("Merge abandonment commit: {error}")))?;
                persist_exact_remote_object_on(&tx, &authority_commit, "Merge abandonment commit")?;
                let authority_head_ref = crate::sync::store_commit::StoreDeviceHeadRef {
                    head_hash: stage.head.value.head_hash(),
                    object: stage.head.prepared.reference().clone(),
                };
                let authority_head = RemoteObjectRecord::candidate_activated_store_head(
                    authority_head_ref,
                    stage.head.value.to_bytes(),
                    stage.head.prepared.stored_bytes().to_vec(),
                    authority_ref,
                )
                .map_err(|error| DbError::Message(format!("Merge abandonment head: {error}")))?;
                persist_exact_remote_object_on(&tx, &authority_head, "Merge abandonment head")?;
                let replacement = PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    candidate_history_summary,
                    authority_commit: DurablePreparedProtocolObject::new(
                        stage.commit.value.to_bytes(),
                        stage.commit.prepared,
                    ),
                    authority_head: DurablePreparedProtocolObject::new(
                        stage.head.value.to_bytes(),
                        stage.head.prepared,
                    ),
                    authority_history_summary: stage.history_summary,
                    outcome: MergeAbandonmentOutcome::Prepared,
                    local_cleanup,
                    completion,
                };
                let replacement = serde_json::to_string(&replacement).map_err(|error| {
                    DbError::Message(format!("serialize Merge abandonment: {error}"))
                })?;
                let publishing =
                    serde_json::to_string(&WriteStatus::Publishing).map_err(|error| {
                        DbError::Message(format!("serialize Merge abandonment status: {error}"))
                    })?;
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
                        "blocked Merge candidate changed during abandonment preparation"
                            .to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)?;
                Ok(())
            })
            .await?;
        self.database
            .notify_write_status(notified_write_id, WriteStatus::Publishing);
        Ok(())
    }
}
