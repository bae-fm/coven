use super::{
    candidate_records::*,
    publication_state::{MergeAbandonmentOutcome, PreparedStoreWriteState},
    StoreDatabase,
};
use crate::database::*;
use crate::sync::remote_object::{remote_object_id, RemoteObjectRecord};
use crate::sync::storage::PreparedExactObject;
use crate::sync::store_commit::{
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceRegistrationRef,
};
use crate::write::{WriteId, WriteResolution, WriteStatus};
use rusqlite::OptionalExtension;

impl StoreDatabase<'_> {
    pub(crate) async fn blocked_merge_candidate(
        &self,
        write_id: WriteId,
    ) -> Result<Option<BlockedMergeCandidate>, DbError> {
        self.database
            .call(move |conn| {
                let row: Option<(String, Option<String>)> = conn
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(DbError::from)?;
                let Some((raw_status, raw_prepared)) = row else {
                    return Err(DbError::Message(format!("write {write_id} is absent")));
                };
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("blocked Merge candidate status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(format!("write {write_id} is not blocked")));
                }
                let Some(raw_prepared) = raw_prepared else {
                    return Ok(None);
                };
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("blocked Merge candidate preparation: {error}"))
                    })?;
                let PreparedStoreWriteState::Publication { .. } = &prepared else {
                    return Ok(None);
                };
                let candidate = parse_prepared_merge_candidate_on(conn, &prepared)?;
                if candidate.commit.write_id != write_id {
                    return Err(DbError::Message(
                        "blocked Merge candidate differs from its write identity".to_string(),
                    ));
                }
                Ok(Some(BlockedMergeCandidate {
                    commit: ExactProtocolObject {
                        value: candidate.commit,
                        bytes: candidate.canonical_signed_bytes,
                        object: candidate.reference.object.clone(),
                        prepared: match &prepared {
                            PreparedStoreWriteState::Publication { commit, .. } => {
                                commit.prepared().clone()
                            }
                            _ => unreachable!("matched Merge preparation"),
                        },
                    },
                    head: ExactProtocolObject {
                        value: candidate.head,
                        bytes: match &prepared {
                            PreparedStoreWriteState::Publication { head, .. } => {
                                head.semantic_bytes().to_vec()
                            }
                            _ => unreachable!("matched Merge preparation"),
                        },
                        object: candidate.head_prepared.reference().clone(),
                        prepared: candidate.head_prepared,
                    },
                }))
            })
            .await
    }

    pub(crate) async fn blocked_merge_history_summary(
        &self,
        write_id: WriteId,
    ) -> Result<crate::sync::store_commit::RetainedVerifiedMergeHistorySummary, DbError> {
        self.database
            .call(move |conn| {
                let raw: String = conn
                    .query_row(
                        "SELECT prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let prepared: PreparedStoreWriteState =
                    serde_json::from_str(&raw).map_err(|error| {
                        DbError::Message(format!("blocked Merge history summary: {error}"))
                    })?;
                let PreparedStoreWriteState::Publication {
                    history_summary, ..
                } = prepared
                else {
                    return Err(DbError::Message(
                        "blocked Merge history summary has no original candidate".to_string(),
                    ));
                };
                Ok(history_summary)
            })
            .await
    }

    pub(crate) async fn prepared_merge_abandonment_candidates(
        &self,
        write_id: WriteId,
    ) -> Result<Option<PreparedMergeAbandonmentCandidates>, DbError> {
        self.database
            .call(move |conn| {
                let raw_prepared: Option<String> = conn
                    .query_row(
                        "SELECT prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let Some(raw_prepared) = raw_prepared else {
                    return Ok(None);
                };
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("prepared Merge abandonment: {error}"))
                    })?;
                let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    authority_commit,
                    authority_head,
                    outcome: MergeAbandonmentOutcome::Prepared,
                    ..
                } = &prepared
                else {
                    return Ok(None);
                };
                Ok(Some(PreparedMergeAbandonmentCandidates {
                    candidate: blocked_merge_candidate_from_prepared(
                        parse_prepared_merge_candidate_parts_on(
                            conn,
                            candidate_commit,
                            candidate_head,
                        )?,
                    ),
                    authority: blocked_merge_candidate_from_prepared(
                        parse_prepared_merge_candidate_parts_on(
                            conn,
                            authority_commit,
                            authority_head,
                        )?,
                    ),
                }))
            })
            .await
    }

    pub(crate) async fn author_exclusion_activation_for_candidate(
        &self,
        candidate: StoreBatchCommitRef,
        author: StoreDeviceRegistrationRef,
    ) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
        self.database
            .call(move |conn| {
                author_exclusion_activation_for_candidate_on(conn, &candidate, &author)
            })
            .await
    }

    pub(crate) async fn begin_blocked_merge_candidate_nonactivation(
        &self,
        write_id: WriteId,
        nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let nonactivation = blocked_merge_candidate_nonactivation(nonactivation)?;
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("blocked Merge candidate status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(format!(
                        "Merge candidate {write_id} is not blocked"
                    )));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("blocked Merge candidate preparation: {error}"))
                    })?;
                let PreparedStoreWriteState::Publication { .. } = &prepared else {
                    return Err(DbError::Message(
                        "author exclusion reached a non-candidate Merge preparation".to_string(),
                    ));
                };
                let candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?;
                begin_blocked_merge_candidate_nonactivation_on(
                    &tx,
                    &write_id,
                    &candidate,
                    &nonactivation,
                    true,
                )?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn begin_prepared_merge_abandonment_nonactivation(
        &self,
        write_id: WriteId,
        candidate_nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
        authority_nonactivation: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let candidate_nonactivation =
            blocked_merge_candidate_nonactivation(candidate_nonactivation)?;
        let authority_nonactivation =
            blocked_merge_candidate_nonactivation(authority_nonactivation)?;
        let notified_write_id = write_id.clone();
        let blocked = self
            .database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("blocked Merge abandonment status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Publishing | WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(
                        "Merge abandonment candidates are not publishing or blocked".to_string(),
                    ));
                }
                let mut prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("blocked Merge abandonment preparation: {error}"))
                    })?;
                let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    authority_commit,
                    authority_head,
                    outcome,
                    ..
                } = &mut prepared
                else {
                    return Err(DbError::Message(
                        "author exclusion reached a non-abandonment Merge preparation".to_string(),
                    ));
                };
                if !matches!(outcome, MergeAbandonmentOutcome::Prepared) {
                    return Err(DbError::Message(
                        "Merge abandonment already has a publication outcome".to_string(),
                    ));
                }
                let candidate =
                    parse_prepared_merge_candidate_parts_on(&tx, candidate_commit, candidate_head)?;
                let authority =
                    parse_prepared_merge_candidate_parts_on(&tx, authority_commit, authority_head)?;
                begin_blocked_merge_candidate_nonactivation_on(
                    &tx,
                    &write_id,
                    &candidate,
                    &candidate_nonactivation,
                    true,
                )?;
                begin_blocked_merge_candidate_nonactivation_on(
                    &tx,
                    &write_id,
                    &authority,
                    &authority_nonactivation,
                    false,
                )?;
                *outcome = MergeAbandonmentOutcome::AuthorExcluded;
                let replacement = serde_json::to_string(&prepared).map_err(|error| {
                    DbError::Message(format!(
                        "serialize excluded Merge abandonment preparation: {error}"
                    ))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                     WHERE write_id = ?1 AND status = ?3 AND prepared = ?4",
                        rusqlite::params![write_id.as_str(), replacement, raw_status, raw_prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "Merge abandonment changed during author-exclusion transition".to_string(),
                    ));
                }
                let device_id = candidate.commit.author_registration.device_id.to_string();
                let blocked = WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: format!(
                        "Store author {device_id} was excluded before candidate activation"
                    ),
                });
                Database::set_write_status_on(&tx, &write_id, &blocked)?;
                tx.commit().map_err(DbError::from)?;
                Ok(blocked)
            })
            .await?;
        self.database
            .notify_write_status(notified_write_id, blocked);
        Ok(())
    }

    pub(crate) async fn merge_abandonment_state(
        &self,
        write_id: &WriteId,
    ) -> Result<MergeAbandonmentState, DbError> {
        let write_id = write_id.clone();
        self.database
            .call(move |conn| {
                let raw_prepared: Option<String> = conn
                    .query_row(
                        "SELECT prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let Some(raw_prepared) = raw_prepared else {
                    return Ok(MergeAbandonmentState::None);
                };
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("prepared Merge abandonment: {error}"))
                    })?;
                let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    outcome,
                    ..
                } = &prepared
                else {
                    return Ok(MergeAbandonmentState::None);
                };
                let candidate = parse_prepared_merge_candidate_parts_on(
                    conn,
                    candidate_commit,
                    candidate_head,
                )?;
                Ok(match outcome {
                    MergeAbandonmentOutcome::Prepared => MergeAbandonmentState::Prepared,
                    MergeAbandonmentOutcome::Accepted { .. } => MergeAbandonmentState::Accepted,
                    MergeAbandonmentOutcome::Lost { winner_commit, .. }
                        if winner_commit == &candidate.reference =>
                    {
                        MergeAbandonmentState::CandidateWon
                    }
                    MergeAbandonmentOutcome::Lost { .. } => MergeAbandonmentState::OtherWon,
                    MergeAbandonmentOutcome::AuthorExcluded => {
                        MergeAbandonmentState::AuthorExcluded
                    }
                })
            })
            .await
    }

    pub(crate) async fn resume_winning_merge_candidate(
        &self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        let notified_write_id = write_id.clone();
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("winning Merge candidate status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(
                        "winning Merge candidate is not blocked".to_string(),
                    ));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("winning Merge candidate preparation: {error}"))
                    })?;
                let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    candidate_history_summary,
                    authority_commit,
                    authority_head,
                    authority_history_summary: _,
                    outcome: MergeAbandonmentOutcome::Lost { winner_commit, .. },
                    local_cleanup,
                    completion,
                } = prepared
                else {
                    return Err(DbError::Message(
                        "Merge abandonment did not lose to its prepared candidate".to_string(),
                    ));
                };
                let candidate = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &candidate_commit,
                    &candidate_head,
                )?;
                if winner_commit != candidate.reference {
                    return Err(DbError::Message(
                        "Merge abandonment winner is another candidate".to_string(),
                    ));
                }
                let authority = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &authority_commit,
                    &authority_head,
                )?;
                remove_cleaned_merge_authority_on(&tx, &authority)?;
                let replacement = PreparedStoreWriteState::Publication {
                    commit: candidate_commit,
                    head: candidate_head,
                    history_summary: candidate_history_summary,
                    local_cleanup,
                    completion,
                };
                let replacement = serde_json::to_string(&replacement).map_err(|error| {
                    DbError::Message(format!("serialize winning Merge candidate: {error}"))
                })?;
                let publishing =
                    serde_json::to_string(&WriteStatus::Publishing).map_err(|error| {
                        DbError::Message(format!("serialize winning Merge status: {error}"))
                    })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2, status = ?3
                     WHERE write_id = ?1 AND prepared = ?4",
                        rusqlite::params![write_id.as_str(), replacement, publishing, raw_prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "Merge abandonment changed while restoring its winner".to_string(),
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

    pub(crate) async fn finish_lost_merge_abandonment(
        &self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("lost Merge abandonment status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(
                        "lost Merge abandonment is not blocked".to_string(),
                    ));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("lost Merge abandonment preparation: {error}"))
                    })?;
                let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    candidate_history_summary,
                    authority_commit,
                    authority_head,
                    authority_history_summary: _,
                    outcome: MergeAbandonmentOutcome::Lost { winner_commit, .. },
                    local_cleanup,
                    completion,
                } = prepared
                else {
                    return Err(DbError::Message(
                        "Merge abandonment has no third-candidate winner".to_string(),
                    ));
                };
                let candidate = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &candidate_commit,
                    &candidate_head,
                )?;
                if winner_commit == candidate.reference {
                    return Err(DbError::Message(
                        "original candidate won the Merge abandonment race".to_string(),
                    ));
                }
                let authority = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &authority_commit,
                    &authority_head,
                )?;
                remove_cleaned_merge_authority_on(&tx, &authority)?;
                let replacement = PreparedStoreWriteState::Publication {
                    commit: candidate_commit,
                    head: candidate_head,
                    history_summary: candidate_history_summary,
                    local_cleanup,
                    completion,
                };
                let replacement = serde_json::to_string(&replacement).map_err(|error| {
                    DbError::Message(format!("serialize lost Merge candidate: {error}"))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                     WHERE write_id = ?1 AND status = ?3 AND prepared = ?4",
                        rusqlite::params![write_id.as_str(), replacement, raw_status, raw_prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "Merge abandonment changed while removing its losing authority".to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn finish_author_excluded_merge_abandonment(
        &self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("excluded Merge abandonment status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(
                        "excluded Merge abandonment is not blocked".to_string(),
                    ));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("excluded Merge abandonment preparation: {error}"))
                    })?;
                let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    candidate_history_summary,
                    authority_commit,
                    authority_head,
                    authority_history_summary: _,
                    outcome: MergeAbandonmentOutcome::AuthorExcluded,
                    local_cleanup,
                    completion,
                } = prepared
                else {
                    return Err(DbError::Message(
                        "Merge abandonment has no author-exclusion outcome".to_string(),
                    ));
                };
                let candidate = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &candidate_commit,
                    &candidate_head,
                )?;
                let authority = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    &authority_commit,
                    &authority_head,
                )?;
                if !merge_candidate_cleanup_targets_on(&tx, &write_id, &candidate, true)?.is_empty()
                    || !merge_candidate_cleanup_targets_on(&tx, &write_id, &authority, false)?
                        .is_empty()
                {
                    return Err(DbError::Message(
                        "excluded Merge abandonment cleanup is incomplete".to_string(),
                    ));
                }
                remove_cleaned_author_excluded_merge_authority_on(&tx, &authority)?;
                let replacement = PreparedStoreWriteState::Publication {
                    commit: candidate_commit,
                    head: candidate_head,
                    history_summary: candidate_history_summary,
                    local_cleanup,
                    completion,
                };
                let replacement = serde_json::to_string(&replacement).map_err(|error| {
                    DbError::Message(format!(
                        "serialize cleaned excluded Merge abandonment: {error}"
                    ))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                     WHERE write_id = ?1 AND status = ?3 AND prepared = ?4",
                        rusqlite::params![write_id.as_str(), replacement, raw_status, raw_prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "excluded Merge abandonment changed during cleanup".to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn merge_candidate_cleanup_pending(
        &self,
        write_id: &WriteId,
    ) -> Result<bool, DbError> {
        let write_id = write_id.clone();
        self.database.call(move |conn| {
            let (raw_status, raw_prepared): (String, Option<String>) = conn
                .query_row(
                    "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                DbError::Message(format!("Merge cleanup status: {error}"))
            })?;
            if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status {
                witness.validate().map_err(DbError::Message)?;
                let candidate = witness.original_position().commit();
                let StoreCommitCoord {
                    stream_id,
                    sequence,
                } = &candidate.coord;
                let exists: bool = conn
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM merge_retraction_cleanups
                             WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3
                         )",
                        rusqlite::params![
                            stream_id.to_string(),
                            Database::sequence_to_sqlite(&stream_id.to_string(), *sequence)?,
                            serde_json::to_string(candidate).map_err(|error| {
                                DbError::Message(format!(
                                    "serialize Merge retraction cleanup ref: {error}"
                                ))
                            })?,
                        ],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if exists {
                    Database::load_merge_retraction_cleanup_on(conn, candidate)?;
                }
                return Ok(exists);
            }
            let Some(raw_prepared) = raw_prepared else {
                return Ok(false);
            };
            let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                .map_err(|error| DbError::Message(format!("prepared Merge cleanup: {error}")))?;
            let candidate = parse_prepared_merge_candidate_on(conn, &prepared)?;
            let cleanup_pending = |candidate: &PreparedMergeCandidate| -> Result<bool, DbError> {
                let remote = load_remote_object_on(
                    conn,
                    remote_object_id(&candidate.reference.object),
                )?;
                Ok(matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        crate::sync::remote_object::CandidateCommitRecord {
                            state:
                                crate::sync::remote_object::CandidateCommitState::CleanupPending {
                                    proof: crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                                        | crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                                },
                            ..
                        }
                    )
                ))
            };
            match &prepared {
                PreparedStoreWriteState::Publication { .. } => cleanup_pending(&candidate),
                PreparedStoreWriteState::MergeAbandonment {
                    outcome,
                    authority_commit,
                    authority_head,
                    ..
                } => {
                    let authority = parse_prepared_merge_candidate_parts_on(
                        conn,
                        authority_commit,
                        authority_head,
                    )?;
                    match outcome {
                        MergeAbandonmentOutcome::Prepared => Ok(false),
                        MergeAbandonmentOutcome::Accepted { .. } => cleanup_pending(&candidate),
                        MergeAbandonmentOutcome::AuthorExcluded => Ok(
                            cleanup_pending(&candidate)? || cleanup_pending(&authority)?,
                        ),
                        MergeAbandonmentOutcome::Lost { winner_commit, .. } => Ok(
                            (winner_commit != &candidate.reference && cleanup_pending(&candidate)?)
                                || cleanup_pending(&authority)?,
                        ),
                    }
                }
            }
        })
        .await
    }

    pub(crate) async fn merge_candidate_cleanup_targets(
        &self,
        write_id: WriteId,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.database
            .call(move |conn| {
                let (raw_status, raw_prepared): (String, Option<String>) = conn
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::Message(format!("Merge cleanup status: {error}")))?;
                if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = &status {
                    witness.validate().map_err(DbError::Message)?;
                    let candidate = Database::load_merge_retraction_cleanup_on(
                        conn,
                        witness.original_position().commit(),
                    )?;
                    if candidate.commit.write_id != write_id {
                        return Err(DbError::Message(
                            "Merge retraction cleanup names another write".to_string(),
                        ));
                    }
                    return merge_candidate_cleanup_targets_on(conn, &write_id, &candidate, false);
                }
                if !matches!(status, WriteStatus::Blocked(_)) {
                    return Err(DbError::Message(format!(
                        "Merge cleanup write {write_id} is not blocked"
                    )));
                }
                let raw_prepared = raw_prepared.ok_or_else(|| {
                    DbError::Message("blocked Merge cleanup has no prepared candidate".to_string())
                })?;
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("prepared Merge cleanup: {error}"))
                    })?;
                let candidate = parse_prepared_merge_candidate_on(conn, &prepared)?;
                match &prepared {
                    PreparedStoreWriteState::Publication { .. } => {
                        merge_candidate_cleanup_targets_on(conn, &write_id, &candidate, true)
                    }
                    PreparedStoreWriteState::MergeAbandonment {
                        outcome,
                        authority_commit,
                        authority_head,
                        ..
                    } => {
                        let authority = parse_prepared_merge_candidate_parts_on(
                            conn,
                            authority_commit,
                            authority_head,
                        )?;
                        let mut targets = Vec::new();
                        match outcome {
                            MergeAbandonmentOutcome::Prepared => {
                                return Err(DbError::Message(
                                    "Merge abandonment has no accepted winner".to_string(),
                                ));
                            }
                            MergeAbandonmentOutcome::Accepted { .. } => {
                                targets.extend(merge_candidate_cleanup_targets_on(
                                    conn, &write_id, &candidate, true,
                                )?);
                            }
                            MergeAbandonmentOutcome::AuthorExcluded => {
                                targets.extend(merge_candidate_cleanup_targets_on(
                                    conn, &write_id, &candidate, true,
                                )?);
                                targets.extend(merge_candidate_cleanup_targets_on(
                                    conn, &write_id, &authority, false,
                                )?);
                            }
                            MergeAbandonmentOutcome::Lost { winner_commit, .. } => {
                                if winner_commit != &candidate.reference {
                                    targets.extend(merge_candidate_cleanup_targets_on(
                                        conn, &write_id, &candidate, true,
                                    )?);
                                }
                                targets.extend(merge_candidate_cleanup_targets_on(
                                    conn, &write_id, &authority, false,
                                )?);
                            }
                        }
                        Ok(targets)
                    }
                }
            })
            .await
    }

    pub(crate) async fn finish_retracted_merge_candidate_cleanup(
        &self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let raw_status: String = tx
                    .query_row(
                        "SELECT status FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("Merge retraction cleanup status: {error}"))
                })?;
                let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status else {
                    return Ok(());
                };
                witness.validate().map_err(DbError::Message)?;
                let candidate_ref = witness.original_position().commit().clone();
                let candidate = Database::load_merge_retraction_cleanup_on(&tx, &candidate_ref)?;
                if candidate.commit.write_id != write_id {
                    return Err(DbError::Message(
                        "Merge retraction cleanup names another write".to_string(),
                    ));
                }
                finish_merge_retraction_cleanup_on(&tx, &candidate)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn pending_merge_retraction_cleanups(
        &self,
    ) -> Result<Vec<StoreBatchCommitRef>, DbError> {
        self.database
            .call(|conn| {
                let mut statement = conn
                    .prepare(
                        "SELECT device_id, seq, commit_ref
                     FROM merge_retraction_cleanups
                     ORDER BY device_id, seq",
                    )
                    .map_err(DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(DbError::from)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(DbError::from)?;
                drop(statement);
                rows.into_iter()
                    .map(|(stream_id, sequence, encoded_ref)| {
                        let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                        let candidate =
                            Database::parse_stored_commit_ref(&stream_id, sequence, &encoded_ref)?;
                        Database::load_merge_retraction_cleanup_on(conn, &candidate)?;
                        Ok(candidate)
                    })
                    .collect()
            })
            .await
    }

    pub(crate) async fn merge_retraction_cleanup_verification(
        &self,
        candidate: StoreBatchCommitRef,
    ) -> Result<TerminalCandidateCleanupVerification, DbError> {
        self.database.call(move |conn| {
            let prepared = Database::load_merge_retraction_cleanup_on(conn, &candidate)?;
            let remote = load_remote_object_on(conn, remote_object_id(&candidate.object))?;
            let proof = remote
                .candidate_nonactivation_proof(&candidate)
                .map_err(|error| DbError::Message(error.to_string()))?
                .ok_or_else(|| {
                    DbError::Message(
                        "Merge retraction cleanup has no terminal proof".to_string(),
                    )
                })?;
            let authority = match proof {
                crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
                    exclusion,
                    ..
                } => TerminalCandidateAuthority::AuthorExclusion(
                    load_author_exclusion_activation_locator_on(conn, exclusion)?,
                ),
                crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
                    grant_id,
                    membership,
                    activation_commit,
                    activation_head,
                } => TerminalCandidateAuthority::MembershipGrantRevocation {
                    grant_id: grant_id.clone(),
                    membership: membership.clone(),
                    activation_commit: activation_commit.clone(),
                    activation_head: activation_head.clone(),
                },
                crate::sync::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
                    let durable = crate::sync::remote_object::CandidateNonactivation::from_durable_parts(
                        &candidate,
                        &prepared.commit,
                        proof.clone(),
                    )
                    .map_err(|error| DbError::Message(error.to_string()))?;
                    validate_terminal_nonactivation_authority_on(conn, &durable)?;
                    TerminalCandidateAuthority::DependencyRetraction(
                        crate::sync::remote_object::VerifiedDependencyRetractionAuthority::after_live_authority_check(durable)
                            .map_err(|error| DbError::Message(error.to_string()))?,
                    )
                }
                crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
                    return Err(DbError::Message(
                        "Merge retraction cleanup has nonterminal proof".to_string(),
                    ));
                }
            };
            Ok(TerminalCandidateCleanupVerification {
                authority,
                candidate: blocked_merge_candidate_from_prepared(prepared),
            })
        })
        .await
    }

    pub(crate) async fn merge_retraction_cleanup_targets(
        &self,
        candidate: StoreBatchCommitRef,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.database
            .call(move |conn| {
                let prepared = Database::load_merge_retraction_cleanup_on(conn, &candidate)?;
                merge_candidate_cleanup_targets_on(
                    conn,
                    &prepared.commit.write_id,
                    &prepared,
                    false,
                )
            })
            .await
    }

    pub(crate) async fn confirm_merge_retraction_cleanup_nonactivation(
        &self,
        candidate: StoreBatchCommitRef,
        verified: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let (durable, head_nonactivation) = verified
            .into_terminal_head_nonactivation()
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.database
            .call(move |conn| {
                let prepared = Database::load_merge_retraction_cleanup_on(conn, &candidate)?;
                if durable
                    .reference()
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != candidate
                    || head_nonactivation.head().object() != prepared.head_prepared.reference()
                {
                    return Err(DbError::Message(
                        "verified Merge retraction cleanup names another candidate".to_string(),
                    ));
                }
                if let crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
                    exclusion,
                    accepted_cut,
                    activation_head,
                } = durable.proof()
                {
                    let locator = load_author_exclusion_activation_locator_on(conn, exclusion)?;
                    if locator.accepted_cut() != accepted_cut
                        || locator.activation_head() != activation_head
                    {
                        return Err(DbError::Message(
                            "verified Merge retraction differs from durable exclusion authority"
                                .to_string(),
                        ));
                    }
                }
                let remote = load_remote_object_on(conn, remote_object_id(&candidate.object))?;
                if remote
                    .candidate_nonactivation_proof(&candidate)
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != Some(durable.proof())
                {
                    return Err(DbError::Message(
                        "verified Merge retraction differs from candidate ownership".to_string(),
                    ));
                }
                if !matches!(
                    load_merge_candidate_head_cleanup_on(
                        conn,
                        prepared.head_prepared.reference(),
                        &candidate,
                    )?,
                    MergeCandidateHeadCleanup::ProtocolInert
                ) {
                    return Err(DbError::Message(
                        "retracted Merge activation head is not retained as inert authority"
                            .to_string(),
                    ));
                }
                Ok(())
            })
            .await
    }

    pub(crate) async fn finish_merge_retraction_cleanup(
        &self,
        candidate: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let prepared = Database::load_merge_retraction_cleanup_on(&tx, &candidate)?;
                finish_merge_retraction_cleanup_on(&tx, &prepared)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn merge_candidate_terminal_verifications(
        &self,
        write_id: WriteId,
    ) -> Result<Vec<TerminalCandidateCleanupVerification>, DbError> {
        self.database.call(move |conn| {
            let (raw_status, raw_prepared): (String, Option<String>) = conn
                .query_row(
                    "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&raw_status)
                .map_err(|error| DbError::Message(format!("Merge cleanup status: {error}")))?;
            let mut candidates = Vec::new();
            if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status {
                witness.validate().map_err(DbError::Message)?;
                let candidate = Database::load_merge_retraction_cleanup_on(
                    conn,
                    witness.original_position().commit(),
                )?;
                if candidate.commit.write_id != write_id {
                    return Err(DbError::Message(
                        "Merge retraction cleanup names another write".to_string(),
                    ));
                }
                candidates.push(candidate);
            } else {
                let raw_prepared = raw_prepared.ok_or_else(|| {
                    DbError::Message("Merge cleanup has no prepared candidate".to_string())
                })?;
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("prepared Merge cleanup: {error}"))
                    })?;
                match &prepared {
                    PreparedStoreWriteState::Publication { .. } => {
                        candidates.push(parse_prepared_merge_candidate_on(conn, &prepared)?);
                    }
                    PreparedStoreWriteState::MergeAbandonment {
                        candidate_commit,
                        candidate_head,
                        authority_commit,
                        authority_head,
                        ..
                    } => {
                        candidates.push(parse_prepared_merge_candidate_parts_on(
                            conn,
                            candidate_commit,
                            candidate_head,
                        )?);
                        candidates.push(parse_prepared_merge_candidate_parts_on(
                            conn,
                            authority_commit,
                            authority_head,
                        )?);
                    }
                }
            }
            let mut verifications = Vec::new();
            for candidate in candidates {
                let remote =
                    load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
                let Some(proof) = remote
                    .candidate_nonactivation_proof(&candidate.reference)
                    .map_err(|error| DbError::Message(error.to_string()))?
                else {
                    continue;
                };
                let authority = match proof {
                    crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
                        exclusion,
                        ..
                    } => TerminalCandidateAuthority::AuthorExclusion(
                        load_author_exclusion_activation_locator_on(conn, exclusion)?,
                    ),
                    crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
                        grant_id,
                        membership,
                        activation_commit,
                        activation_head,
                    } => TerminalCandidateAuthority::MembershipGrantRevocation {
                        grant_id: grant_id.clone(),
                        membership: membership.clone(),
                        activation_commit: activation_commit.clone(),
                        activation_head: activation_head.clone(),
                    },
                    crate::sync::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
                        let durable = crate::sync::remote_object::CandidateNonactivation::from_durable_parts(
                            &candidate.reference,
                            &candidate.commit,
                            proof.clone(),
                        )
                        .map_err(|error| DbError::Message(error.to_string()))?;
                        validate_terminal_nonactivation_authority_on(conn, &durable)?;
                        TerminalCandidateAuthority::DependencyRetraction(
                            crate::sync::remote_object::VerifiedDependencyRetractionAuthority::after_live_authority_check(durable)
                                .map_err(|error| DbError::Message(error.to_string()))?,
                        )
                    }
                    crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { .. } => continue,
                };
                verifications.push(TerminalCandidateCleanupVerification {
                    authority,
                    candidate: blocked_merge_candidate_from_prepared(candidate),
                });
            }
            Ok(verifications)
        })
        .await
    }

    pub(crate) async fn reconcile_merge_candidate_terminal_head(
        &self,
        write_id: WriteId,
        verified: crate::sync::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        if !matches!(
            verified.proof(),
            crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                | crate::sync::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
        ) {
            return Err(DbError::Message(
                "terminal head reconciliation received another proof family".to_string(),
            ));
        }
        let (durable, head_nonactivation) = verified
            .into_terminal_head_nonactivation()
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, Option<String>) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let reference = durable
                    .reference()
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let mut candidates = Vec::new();
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::Message(format!("Merge cleanup status: {error}")))?;
                if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status {
                    witness.validate().map_err(DbError::Message)?;
                    if witness.original_position().commit() != &reference {
                        return Err(DbError::Message(
                        "fresh excluded-author head evidence differs from the retraction witness"
                            .to_string(),
                    ));
                    }
                    candidates.push(Database::load_merge_retraction_cleanup_on(&tx, &reference)?);
                } else {
                    let raw_prepared = raw_prepared.ok_or_else(|| {
                        DbError::Message("Merge cleanup has no prepared candidate".to_string())
                    })?;
                    let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                        .map_err(|error| {
                            DbError::Message(format!("prepared Merge cleanup: {error}"))
                        })?;
                    match &prepared {
                        PreparedStoreWriteState::Publication { commit, head, .. } => candidates
                            .push(parse_prepared_merge_candidate_parts_on(&tx, commit, head)?),
                        PreparedStoreWriteState::MergeAbandonment {
                            candidate_commit,
                            candidate_head,
                            authority_commit,
                            authority_head,
                            ..
                        } => {
                            candidates.push(parse_prepared_merge_candidate_parts_on(
                                &tx,
                                candidate_commit,
                                candidate_head,
                            )?);
                            candidates.push(parse_prepared_merge_candidate_parts_on(
                                &tx,
                                authority_commit,
                                authority_head,
                            )?);
                        }
                    }
                }
                let candidate = candidates
                    .into_iter()
                    .find(|candidate| candidate.reference == reference)
                    .ok_or_else(|| {
                        DbError::Message(
                            "fresh excluded-author head evidence names another write".to_string(),
                        )
                    })?;
                validate_terminal_candidate_authority_on(&tx, &candidate, &durable)?;
                let object_id = remote_object_id(candidate.head_prepared.reference());
                let remote_exists: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                        [object_id.to_string()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if !remote_exists {
                    let inert = load_protocol_inert_object_on(&tx, object_id)?;
                    if inert
                        .candidate_nonactivation_proof(&candidate.reference)
                        .map_err(|error| DbError::Message(error.to_string()))?
                        != Some(durable.proof())
                    {
                        return Err(DbError::Message(
                            "protocol-inert candidate head carries another proof".to_string(),
                        ));
                    }
                    return tx.commit().map_err(DbError::from);
                }
                let mut remote = load_remote_object_on(&tx, object_id)?;
                let inert = remote
                    .begin_candidate_nonactivation_with_verified_head_nonactivation(
                        durable,
                        &head_nonactivation,
                    )
                    .map_err(|error| {
                        DbError::Message(format!(
                            "reconcile excluded-author head {object_id}: {error}"
                        ))
                    })?;
                finish_remote_candidate_nonactivation_on(&tx, object_id, remote, inert)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub(crate) async fn adopt_alternate_merge_head(
        &self,
        write_id: WriteId,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        self.database
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("alternate Merge head status: {error}"))
                })?;
                if !matches!(status, WriteStatus::Publishing) {
                    return Err(DbError::Message(format!(
                        "Merge candidate {write_id} is not publishing"
                    )));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("prepared Merge candidate: {error}"))
                    })?;
                let publication = parse_prepared_merge_publication_on(&tx, &prepared)?;
                let root = required_store_root_authority_on(&tx)?;
                let registration = load_activated_registration_on(
                    &tx,
                    &root,
                    &publication.commit.author_registration,
                )?;
                let candidate = publication.reference;
                let verified_winner = StoreDeviceHead::parse_at(
                    &winner.to_bytes(),
                    root.store_root_hash,
                    &registration,
                    &candidate,
                )
                .map_err(|error| {
                    DbError::Message(format!("verify alternate Merge head: {error}"))
                })?;
                if verified_winner != winner || winner.commit != candidate {
                    return Err(DbError::Message(
                        "alternate Merge head does not activate the prepared commit".to_string(),
                    ));
                }
                replace_prepared_merge_head_remote_on(
                    &tx,
                    publication.head_prepared.reference(),
                    &winner,
                    &winner_prepared,
                    &candidate,
                )?;
                let replacement_head =
                    DurablePreparedProtocolObject::new(winner.to_bytes(), winner_prepared);
                let replacement = match prepared {
                    PreparedStoreWriteState::Publication {
                        commit,
                        history_summary,
                        local_cleanup,
                        completion,
                        ..
                    } => PreparedStoreWriteState::Publication {
                        commit,
                        head: replacement_head,
                        history_summary,
                        local_cleanup,
                        completion,
                    },
                    PreparedStoreWriteState::MergeAbandonment {
                        candidate_commit,
                        candidate_head,
                        candidate_history_summary,
                        authority_commit,
                        authority_history_summary,
                        outcome,
                        local_cleanup,
                        completion,
                        ..
                    } => PreparedStoreWriteState::MergeAbandonment {
                        candidate_commit,
                        candidate_head,
                        candidate_history_summary,
                        authority_commit,
                        authority_head: replacement_head,
                        authority_history_summary,
                        outcome,
                        local_cleanup,
                        completion,
                    },
                };
                let replacement = serde_json::to_string(&replacement).map_err(|error| {
                    DbError::Message(format!("serialize alternate Merge preparation: {error}"))
                })?;
                let updated = tx
                    .execute(
                        "UPDATE store_writes SET prepared = ?2
                     WHERE write_id = ?1 AND status = '\"publishing\"' AND prepared = ?3",
                        rusqlite::params![write_id.as_str(), replacement, raw_prepared],
                    )
                    .map_err(DbError::from)?;
                if updated != 1 {
                    return Err(DbError::Message(
                        "prepared Merge write changed during head replacement".to_string(),
                    ));
                }
                tx.commit().map_err(DbError::from)
            })
            .await
    }
}
