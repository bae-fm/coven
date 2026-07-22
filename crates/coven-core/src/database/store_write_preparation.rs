use crate::database::blob_records::load_activated_registration_on;
use crate::database::remote_object_records::persist_exact_remote_object_on;

#[cfg(test)]
use crate::database::local_store_identity::local_merge_stream_id_on;

use super::*;

impl Database {
    pub(crate) async fn reserve_serial_store_branch(
        &self,
    ) -> Result<Option<SerialStoreBranchPreparationWork>, DbError> {
        if self.write_policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial branch reservation requires the Serial write policy".to_string(),
            ));
        }
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, changeset, inverse_changeset, base, blob_facts, status, prepared
                     FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })
                .map_err(DbError::from)?;
            let mut records = Vec::new();
            for row in rows {
                records.push(row.map_err(DbError::from)?);
            }
            drop(statement);
            let Some(first) = records.first() else {
                tx.commit().map_err(DbError::from)?;
                return Ok(None);
            };
            let first_base: StoreWriteBase = serde_json::from_str(&first.3)
                .map_err(|error| DbError::Message(format!("pending Serial write base: {error}")))?;
            let StoreWriteBase::Serial { branch_id, base } = first_base else {
                return Err(DbError::Message(
                    "MergeConcurrent write exists in a Serial database".to_string(),
                ));
            };
            let preparing = serde_json::to_string(&PreparedStoreWriteState::SerialPreparing)
                .map_err(|error| DbError::Message(format!("serialize Serial reservation: {error}")))?;
            let publishing = serde_json::to_string(&WriteStatus::Publishing)
                .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
            let mut writes = Vec::new();
            let mut newly_reserved = Vec::new();
            for (write_id, changeset, inverse_changeset, raw_base, blob_facts, status, prepared) in
                records
            {
                let parsed_base: StoreWriteBase = serde_json::from_str(&raw_base)
                    .map_err(|error| DbError::Message(format!("pending Serial write base: {error}")))?;
                if parsed_base
                    != (StoreWriteBase::Serial {
                        branch_id: branch_id.clone(),
                        base: base.clone(),
                    })
                {
                    break;
                }
                match (status.as_str(), prepared.as_deref()) {
                    ("\"pending\"", None) => {
                        let updated = tx
                            .execute(
                                "UPDATE store_writes SET status = ?2, prepared = ?3
                                 WHERE write_id = ?1 AND status = '\"pending\"' AND prepared IS NULL",
                                rusqlite::params![&write_id, &publishing, &preparing],
                            )
                            .map_err(DbError::from)?;
                        if updated != 1 {
                            return Err(DbError::Message(format!(
                                "Serial write {write_id} lost branch reservation"
                            )));
                        }
                        newly_reserved.push(WriteId::from_generated(write_id.clone()));
                    }
                    ("\"publishing\"", Some(stored)) if stored == preparing => {}
                    ("\"publishing\"", Some(_)) => {
                        tx.commit().map_err(DbError::from)?;
                        return Ok(None);
                    }
                    _ => {
                        tx.commit().map_err(DbError::from)?;
                        return Ok(None);
                    }
                }
                let partitions = Self::store_write_partitions_on(
                    &tx,
                    &write_id,
                    &changeset,
                    WritePolicy::Serial,
                )?;
                writes.push(PreparedStoreWrite {
                    write_id: WriteId::from_generated(write_id),
                    changeset,
                    partitions,
                    inverse_changeset,
                    base: parsed_base,
                    blob_facts: serde_json::from_str(&blob_facts).map_err(|error| {
                        DbError::Message(format!("pending Serial write blob facts: {error}"))
                    })?,
                });
            }
            if writes.is_empty() {
                return Err(DbError::Message("Serial branch reservation is empty".to_string()));
            }
            tx.commit().map_err(DbError::from)?;
            for write_id in newly_reserved {
                Self::notify_write_status_in(&statuses, &write_id, WriteStatus::Publishing);
            }
            Ok(Some(SerialStoreBranchPreparationWork {
                branch_id,
                base,
                writes,
            }))
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn enqueue_store_changeset_for_test(
        &self,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let write_id = self.new_write_id();
        let blob_decls = self.blob_decls();
        let write_policy = self.write_policy();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_stream_id = local_merge_stream_id_on(&tx)?;
            let base = match write_policy {
                WritePolicy::MergeConcurrent => StoreWriteBase::MergeConcurrent {
                    dependencies: Self::materialized_frontier_on(&tx, local_stream_id.as_deref())?,
                },
                WritePolicy::Serial => StoreWriteBase::Serial {
                    branch_id: PendingBranchId::from_first_write(write_id.clone()),
                    base: Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)?,
                },
            };
            let inverse_changeset = Self::invert_changeset(&changeset)?;
            let partitions = vec![gate::AudiencePartition {
                audience: crate::sync::circle::Audience::Store,
                control: None,
                changeset,
            }];
            let blob_facts = Self::capture_partition_blob_facts_on(&tx, &partitions, &blob_decls)?;
            Self::insert_store_write_on(
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
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx.query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [LOCAL_DEVICE_ID_STATE_KEY],
                |row| row.get(0),
            ).map_err(DbError::from)?;
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
            let coord = StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence: stage.commit.value.seq(),
            };
            stage.commit.value.verify_at(root.store_root_hash, &coord, &registration)
                .map_err(|error| DbError::Message(format!(
                    "verify prepared Store commit: {error}"
                )))?;
            if stage.commit.value.write_id != stage.write_id {
                return Err(DbError::Message(
                    "prepared write id differs from signed commit".to_string(),
                ));
            }
            let commit_ref = StoreBatchCommitRef::from_commit(
                &stage.commit.value,
                coord,
                stage.commit.prepared.reference().clone(),
            ).map_err(|error| DbError::Message(error.to_string()))?;
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
            let partitions = Self::store_write_partitions_on(
                &tx,
                stage.write_id.as_str(),
                &stored_changeset,
                WritePolicy::MergeConcurrent,
            )?;
            let stored_base: StoreWriteBase =
                serde_json::from_str(&stored_base).map_err(|error| {
                    DbError::Message(format!("write {} base: {error}", stage.write_id))
                })?;
            let StoreWriteBase::MergeConcurrent {
                dependencies: stored_dependencies,
            } = stored_base
            else {
                return Err(DbError::Message(format!(
                    "serial write {} reached MergeConcurrent preparation",
                    stage.write_id
                )));
            };
            let stored_dependencies = CommitFrontier::from_refs(
                WritePolicy::MergeConcurrent,
                stored_dependencies,
            ).map_err(|error| DbError::Message(format!(
                "stored write dependencies: {error}"
            )))?;
            if stored_dependencies.merge_commits().map_err(|error| {
                DbError::Message(error.to_string())
            })? != stage.commit.value.merge_dependencies().map_err(|error| {
                DbError::Message(format!("prepared Store commit policy: {error}"))
            })? {
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
                Self::latest_position_for_device_on(&tx, &stream_id.to_string())?;
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
                &stage.commit.value,
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

            let prepared = PreparedStoreWriteState::MergeConcurrent {
                commit: DurablePreparedProtocolObject {
                    semantic_bytes: stage.commit.value.to_bytes(),
                    prepared: stage.commit.prepared,
                },
                head: DurablePreparedProtocolObject {
                    semantic_bytes: stage.head.value.to_bytes(),
                    prepared: stage.head.prepared,
                },
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
            Self::notify_write_status_in(&statuses, &write_id, WriteStatus::Publishing);
            Ok(())
        })
        .await
    }

    pub(crate) async fn prepare_merge_candidate_abandonment(
        &self,
        stage: MergeCandidateAbandonmentPreparation,
    ) -> Result<(), DbError> {
        if self.write_policy() != WritePolicy::MergeConcurrent {
            return Err(DbError::Message(
                "Merge candidate abandonment requires MergeConcurrent policy".to_string(),
            ));
        }
        let statuses = self.state.write_statuses.clone();
        let notified_write_id = stage.write_id.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let (raw_status, raw_prepared): (String, String) = tx
                .query_row(
                    "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                    [stage.write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("Merge abandonment status: {error}")))?;
            if !matches!(status, WriteStatus::Blocked(_)) {
                return Err(DbError::Message(format!(
                    "write {} is not blocked",
                    stage.write_id
                )));
            }
            let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                .map_err(|error| DbError::Message(format!("prepared Merge candidate: {error}")))?;
            let PreparedStoreWriteState::MergeConcurrent {
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
            let candidate =
                parse_prepared_merge_candidate_parts_on(&tx, &candidate_commit, &candidate_head)?;
            if candidate.commit.write_id != stage.write_id {
                return Err(DbError::Message(
                    "prepared Merge candidate differs from its write identity".to_string(),
                ));
            }
            let root = required_store_root_authority_on(&tx)?;
            let registration =
                load_activated_registration_on(&tx, &root, &candidate.commit.author_registration)?;
            stage
                .commit
                .value
                .verify_at(
                    root.store_root_hash,
                    &candidate.reference.coord,
                    &registration,
                )
                .map_err(|error| {
                    DbError::Message(format!("verify Merge abandonment commit: {error}"))
                })?;
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
            let authority_ref = StoreBatchCommitRef::from_commit(
                &stage.commit.value,
                candidate.reference.coord.clone(),
                stage.commit.prepared.reference().clone(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
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
            .map_err(|error| DbError::Message(format!("verify Merge abandonment head: {error}")))?;
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
                authority_commit: DurablePreparedProtocolObject {
                    semantic_bytes: stage.commit.value.to_bytes(),
                    prepared: stage.commit.prepared,
                },
                authority_head: DurablePreparedProtocolObject {
                    semantic_bytes: stage.head.value.to_bytes(),
                    prepared: stage.head.prepared,
                },
                authority_history_summary: stage.history_summary,
                outcome: MergeAbandonmentOutcome::Prepared,
                local_cleanup,
                completion,
            };
            let replacement = serde_json::to_string(&replacement).map_err(|error| {
                DbError::Message(format!("serialize Merge abandonment: {error}"))
            })?;
            let publishing = serde_json::to_string(&WriteStatus::Publishing).map_err(|error| {
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
                    "blocked Merge candidate changed during abandonment preparation".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)?;
            Self::notify_write_status_in(&statuses, &notified_write_id, WriteStatus::Publishing);
            Ok(())
        })
        .await
    }

    pub(crate) async fn prepare_serial_candidate_abandonment(
        &self,
        stage: SerialCandidateAbandonmentPreparation,
    ) -> Result<(), DbError> {
        if self.write_policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial candidate abandonment requires the Serial write policy".to_string(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let existing: Option<String> = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if existing.is_some() {
                return Err(DbError::Message(
                    "a Serial candidate abandonment is already prepared".to_string(),
                ));
            }
            let (raw_base, raw_prepared): (String, String) = tx
                .query_row(
                    "SELECT base, prepared FROM store_writes
                     WHERE prepared IS NOT NULL AND status = '\"publishing\"'
                     ORDER BY ordinal LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let StoreWriteBase::Serial { branch_id, base } = serde_json::from_str(&raw_base)
                .map_err(|error| {
                    DbError::Message(format!("Serial abandonment branch base: {error}"))
                })?
            else {
                return Err(DbError::Message(
                    "Merge branch reached Serial candidate abandonment".to_string(),
                ));
            };
            if branch_id != stage.branch_id {
                return Err(DbError::Message(
                    "Serial abandonment names another pending branch".to_string(),
                ));
            }
            let prepared: PreparedStoreWriteState =
                serde_json::from_str(&raw_prepared).map_err(|error| {
                    DbError::Message(format!("prepared Serial abandonment candidate: {error}"))
                })?;
            let PreparedStoreWriteState::Serial {
                base_head,
                commit: candidate_commit,
                ..
            } = &prepared
            else {
                return Err(DbError::Message(
                    "Serial abandonment requires an exact prepared branch".to_string(),
                ));
            };
            let base_head = base_head.clone();
            let candidate = parse_prepared_serial_candidate(&raw_prepared)?
                .expect("matched exact Serial candidate");
            if candidate.reference != stage.candidate {
                return Err(DbError::Message(
                    "Serial abandonment target differs from the branch first candidate".to_string(),
                ));
            }
            let root = required_store_root_authority_on(&tx)?;
            let registration =
                load_activated_registration_on(&tx, &root, &candidate.commit.author_registration)?;
            stage
                .commit
                .value
                .verify_at(
                    root.store_root_hash,
                    &candidate.reference.coord,
                    &registration,
                )
                .map_err(|error| {
                    DbError::Message(format!("verify Serial abandonment commit: {error}"))
                })?;
            if stage.commit.value.write_id != candidate.commit.write_id
                || stage.commit.value.order != candidate.commit.order
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
                    "Serial abandonment does not replace its exact first candidate".to_string(),
                ));
            }
            let authority_ref = StoreBatchCommitRef::from_commit(
                &stage.commit.value,
                candidate.reference.coord.clone(),
                stage.commit.prepared.reference().clone(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?;
            let parsed_head =
                StoreSerialHead::parse(&stage.head.to_bytes(), root.store_root_hash, &registration)
                    .map_err(|error| {
                        DbError::Message(format!("verify Serial abandonment head: {error}"))
                    })?;
            if parsed_head != stage.head
                || !matches!(
                    &stage.head.state,
                    StoreSerialHeadState::Commit {
                        author_registration,
                        commit,
                    } if author_registration == &candidate.commit.author_registration
                        && commit == &authority_ref
                )
            {
                return Err(DbError::Message(
                    "Serial abandonment head does not activate its exact authority".to_string(),
                ));
            }
            let (raw_tip_base, raw_tip_prepared): (String, String) = tx
                .query_row(
                    "SELECT base, prepared FROM store_writes
                     WHERE prepared IS NOT NULL AND status = '\"publishing\"'
                     ORDER BY ordinal DESC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let StoreWriteBase::Serial {
                branch_id: tip_branch_id,
                base: tip_base,
            } = serde_json::from_str(&raw_tip_base).map_err(|error| {
                DbError::Message(format!("Serial abandonment tip branch base: {error}"))
            })?
            else {
                return Err(DbError::Message(
                    "Merge tip reached Serial candidate abandonment".to_string(),
                ));
            };
            let tip_prepared: PreparedStoreWriteState = serde_json::from_str(&raw_tip_prepared)
                .map_err(|error| {
                    DbError::Message(format!("prepared Serial abandonment tip: {error}"))
                })?;
            let PreparedStoreWriteState::Serial {
                tip_head_bytes: Some(tip_head_bytes),
                ..
            } = tip_prepared
            else {
                return Err(DbError::Message(
                    "Serial abandonment branch has no activating tip head".to_string(),
                ));
            };
            if tip_branch_id != branch_id
                || tip_base != base
                || tip_head_bytes != stage.original_head_bytes
            {
                return Err(DbError::Message(
                    "Serial abandonment original head differs from its prepared branch".to_string(),
                ));
            }
            let authority_remote = RemoteObjectRecord::candidate_commit(
                authority_ref,
                stage.commit.value.to_bytes(),
                stage.commit.prepared.stored_bytes().to_vec(),
            )
            .map_err(|error| DbError::Message(format!("Serial abandonment commit: {error}")))?;
            persist_exact_remote_object_on(&tx, &authority_remote, "Serial abandonment commit")?;
            let durable = DurableSerialCandidateAbandonment {
                branch_id,
                base,
                base_head,
                candidate: candidate.reference,
                commit: DurablePreparedProtocolObject {
                    semantic_bytes: stage.commit.value.to_bytes(),
                    prepared: stage.commit.prepared,
                },
                head_bytes: stage.head.to_bytes(),
                original_head_bytes: stage.original_head_bytes,
            };
            let durable = serde_json::to_string(&durable).map_err(|error| {
                DbError::Message(format!("serialize Serial candidate abandonment: {error}"))
            })?;
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                (SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY, durable),
            )
            .map_err(DbError::from)?;
            if candidate_commit.prepared.reference() != &stage.candidate.object {
                return Err(DbError::Message(
                    "Serial abandonment candidate storage identity changed".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn prepare_serial_store_branch_commit(
        &self,
        stage: SerialStoreWritePreparation,
    ) -> Result<(), DbError> {
        if self.write_policy() != WritePolicy::Serial {
            return Err(DbError::Message(
                "Serial branch preparation requires the Serial write policy".to_string(),
            ));
        }
        if stage.writes.is_empty() {
            return Err(DbError::Message(
                "prepared Serial branch is empty".to_string(),
            ));
        }
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let local_device_id: String = tx
                .query_row(
                    "SELECT value FROM protocol_state WHERE key = ?1",
                    [LOCAL_DEVICE_ID_STATE_KEY],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let root = required_store_root_authority_on(&tx)?;
            let expected_base = StoreWriteBase::Serial {
                branch_id: stage.branch_id.clone(),
                base: stage.base.clone(),
            };
            {
                let bytes = stage.base_head.bytes.as_slice();
                let unverified: StoreSerialHead = serde_json::from_slice(bytes)
                    .map_err(|error| DbError::Message(format!("Serial base head: {error}")))?;
                let executor_ref = match &unverified.state {
                    StoreSerialHeadState::Genesis {
                        founder_registration,
                        ..
                    } => founder_registration,
                    StoreSerialHeadState::Commit {
                        author_registration,
                        ..
                    } => author_registration,
                };
                let executor = load_activated_registration_on(&tx, &root, executor_ref)?;
                let verified = StoreSerialHead::parse(bytes, root.store_root_hash, &executor)
                    .map_err(|error| {
                        DbError::Message(format!("verify Serial base head: {error}"))
                    })?;
                let base_ref = match verified.state {
                    StoreSerialHeadState::Genesis {
                        root: head_root,
                        founder_registration,
                    } => {
                        if head_root != root || founder_registration != executor_ref.clone() {
                            return Err(DbError::Message(
                                "Serial genesis head differs from exact Store authority"
                                    .to_string(),
                            ));
                        }
                        None
                    }
                    StoreSerialHeadState::Commit { commit, .. } => Some(commit),
                };
                if base_ref != stage.base {
                    return Err(DbError::Message(
                        "Serial branch base differs from the exact observed head".to_string(),
                    ));
                }
            }
            let head_bytes = stage.head.to_bytes();
            let mut predecessor = stage.base.clone();
            let mut tip_ref = None;
            let mut tip_registration = None;
            for (index, write) in stage.writes.iter().enumerate() {
                if write.commit.value.write_id != write.write_id
                    || write.commit.value.author_registration.device_id.to_string()
                        != local_device_id
                {
                    return Err(DbError::Message(format!(
                        "prepared Serial write {} identity differs from its commit",
                        write.write_id
                    )));
                }
                let expected_seq = predecessor
                    .as_ref()
                    .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
                let coord = StoreCommitCoord::Serial {
                    sequence: expected_seq,
                };
                let registration = load_activated_registration_on(
                    &tx,
                    &root,
                    &write.commit.value.author_registration,
                )?;
                write
                    .commit
                    .value
                    .verify_at(root.store_root_hash, &coord, &registration)
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Serial commit: {error}"))
                    })?;
                let order_matches = match (&predecessor, &write.commit.value.order) {
                    (
                        Some(expected),
                        crate::sync::store_commit::StoreCommitOrder::Serial {
                            predecessor: StoreSerialPredecessor::Commit(actual),
                            ..
                        },
                    ) => actual == expected,
                    (
                        None,
                        crate::sync::store_commit::StoreCommitOrder::Serial {
                            predecessor:
                                StoreSerialPredecessor::Genesis {
                                    root: commit_root,
                                    founder_registration,
                                },
                            ..
                        },
                    ) => {
                        commit_root == &root
                            && founder_registration == &write.commit.value.author_registration
                    }
                    _ => false,
                };
                if !order_matches {
                    return Err(DbError::Message(format!(
                        "prepared Serial commit {} has the wrong exact predecessor",
                        write.write_id
                    )));
                }
                let commit_ref = StoreBatchCommitRef::from_commit(
                    &write.commit.value,
                    coord,
                    write.commit.prepared.reference().clone(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                let (stored_changeset, stored_base, status, prepared): (
                    Vec<u8>,
                    String,
                    String,
                    String,
                ) = tx
                    .query_row(
                        "SELECT changeset, base, status, prepared FROM store_writes
                         WHERE write_id = ?1",
                        [write.write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .map_err(DbError::from)?;
                let stored_base: StoreWriteBase = serde_json::from_str(&stored_base)
                    .map_err(|error| DbError::Message(format!("stored Serial base: {error}")))?;
                let stored_prepared: PreparedStoreWriteState = serde_json::from_str(&prepared)
                    .map_err(|error| {
                        DbError::Message(format!("stored Serial reservation: {error}"))
                    })?;
                if stored_base != expected_base
                    || status != "\"publishing\""
                    || !matches!(stored_prepared, PreparedStoreWriteState::SerialPreparing)
                {
                    return Err(DbError::Message(format!(
                        "Serial write {} no longer owns its exact branch reservation",
                        write.write_id
                    )));
                }
                let partitions = Self::store_write_partitions_on(
                    &tx,
                    write.write_id.as_str(),
                    &stored_changeset,
                    WritePolicy::Serial,
                )?;
                Self::persist_closed_write_objects_on(
                    &tx,
                    &write.write_id,
                    root.store_root_hash,
                    &commit_ref,
                    &write.commit.value,
                    write.commit.prepared.stored_bytes(),
                    &partitions,
                    &write.remote_objects,
                    &write.audiences,
                )?;
                let tip_head_bytes = (index + 1 == stage.writes.len()).then(|| head_bytes.clone());
                let durable = PreparedStoreWriteState::Serial {
                    base_head: stage.base_head.clone(),
                    commit: DurablePreparedProtocolObject {
                        semantic_bytes: write.commit.value.to_bytes(),
                        prepared: write.commit.prepared.clone(),
                    },
                    tip_head_bytes,
                    local_cleanup: write.local_cleanup.clone(),
                    completion: write.completion.clone(),
                };
                let durable = serde_json::to_string(&durable).map_err(|error| {
                    DbError::Message(format!("serialize prepared Serial write: {error}"))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                         WHERE write_id = ?1 AND status = '\"publishing\"'",
                        rusqlite::params![write.write_id.as_str(), durable],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "Serial write {} lost exact preparation ownership",
                        write.write_id
                    )));
                }
                predecessor = Some(commit_ref.clone());
                tip_ref = Some(commit_ref);
                tip_registration = Some(registration);
            }
            let tip_ref = tip_ref.expect("nonempty checked above");
            let tip_registration = tip_registration.expect("nonempty checked above");
            let parsed_head =
                StoreSerialHead::parse(&head_bytes, root.store_root_hash, &tip_registration)
                    .map_err(|error| {
                        DbError::Message(format!("verify prepared Serial head: {error}"))
                    })?;
            let tip_author_ref = &stage
                .writes
                .last()
                .expect("nonempty checked above")
                .commit
                .value
                .author_registration;
            if parsed_head != stage.head
                || !matches!(
                    &stage.head.state,
                    StoreSerialHeadState::Commit {
                        author_registration,
                        commit,
                    } if author_registration == tip_author_ref
                        && commit == &tip_ref
                )
            {
                return Err(DbError::Message(
                    "prepared Serial head does not activate the exact branch tip".to_string(),
                ));
            }
            let reserved_count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM store_writes
                     WHERE status = '\"publishing\"' AND json_type(base, '$.serial') IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if reserved_count
                != i64::try_from(stage.writes.len()).map_err(|_| {
                    DbError::Message("Serial branch length exceeds SQLite integer".into())
                })?
            {
                return Err(DbError::Message(format!(
                    "prepared Serial branch contains {} writes but {reserved_count} are reserved",
                    stage.writes.len()
                )));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(super) fn write_ids_matching_serial_base<I>(
        rows: I,
        expected_base: &StoreWriteBase,
    ) -> Result<Vec<WriteId>, DbError>
    where
        I: IntoIterator<Item = rusqlite::Result<(String, String)>>,
    {
        let mut write_ids = Vec::new();
        for row in rows {
            let (write_id, raw_base) = row.map_err(DbError::from)?;
            let stored_base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("stored Serial base: {error}")))?;
            if &stored_base == expected_base {
                write_ids.push(WriteId::from_generated(write_id));
            }
        }
        Ok(write_ids)
    }

    pub(crate) async fn release_serial_store_branch_reservation(
        &self,
        branch_id: PendingBranchId,
        base: Option<StoreBatchCommitRef>,
        status: WriteStatus,
    ) -> Result<(), DbError> {
        if !matches!(status, WriteStatus::Pending | WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "Serial preparation can only return a reservation to pending or blocked"
                    .to_string(),
            ));
        }
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let expected_base = StoreWriteBase::Serial { branch_id, base };
            let preparing = serde_json::to_string(&PreparedStoreWriteState::SerialPreparing)
                .map_err(|error| {
                    DbError::Message(format!("serialize Serial reservation: {error}"))
                })?;
            let status_json = serde_json::to_string(&status)
                .map_err(|error| DbError::Message(format!("serialize write status: {error}")))?;
            let mut statement = tx
                .prepare(
                    "SELECT write_id, base FROM store_writes
                     WHERE status = '\"publishing\"' AND prepared = ?1
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([&preparing], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;
            let write_ids = Self::write_ids_matching_serial_base(rows, &expected_base)?;
            drop(statement);
            if write_ids.is_empty() {
                return Err(DbError::Message(
                    "reserved Serial branch disappeared during preparation".to_string(),
                ));
            }
            for write_id in &write_ids {
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET status = ?2, prepared = NULL
                         WHERE write_id = ?1 AND status = '\"publishing\"' AND prepared = ?3",
                        rusqlite::params![write_id.as_str(), &status_json, &preparing],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(format!(
                        "reserved Serial write {write_id} disappeared during release"
                    )));
                }
            }
            tx.commit().map_err(DbError::from)?;
            for write_id in write_ids {
                Self::notify_write_status_in(&statuses, &write_id, status.clone());
            }
            Ok(())
        })
        .await
    }
}
