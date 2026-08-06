use super::*;

impl StoreDatabase {
    pub async fn merge_candidate_terminal_verifications(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        write_id: WriteId,
    ) -> Result<Vec<TerminalCandidateCleanupVerification>, DbError> {
        self.connection
            .call(move |conn| {
                let (raw_status, raw_prepared): (String, Option<String>) = conn
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::context("Merge cleanup status", error))?;
                let mut candidates = Vec::new();
                if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status {
                    witness.validate().map_err(DbError::Message)?;
                    let candidate = crate::StoreDatabase::load_merge_retraction_cleanup_on(
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
                        .map_err(|error| DbError::context("prepared Merge cleanup", error))?;
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
                    if let Some(verification) =
                        terminal_candidate_verification_on(conn, &root, candidate)?
                    {
                        verifications.push(verification);
                    }
                }
                Ok(verifications)
            })
            .await
    }

    pub async fn reconcile_merge_candidate_terminal_head(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        write_id: WriteId,
        verified: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        if !matches!(
            verified.proof(),
            coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                | coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
        ) {
            return Err(DbError::Message(
                "terminal head reconciliation received another proof family".to_string(),
            ));
        }
        let (durable, head_nonactivation) = verified
            .into_terminal_head_nonactivation()
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.connection
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
                    .map_err(|error| DbError::context("Merge cleanup status", error))?;
                if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status {
                    witness.validate().map_err(DbError::Message)?;
                    if witness.original_position().commit() != &reference {
                        return Err(DbError::Message(
                        "fresh excluded-author head evidence differs from the retraction witness"
                            .to_string(),
                    ));
                    }
                    candidates.push(crate::StoreDatabase::load_merge_retraction_cleanup_on(
                        &tx, &reference,
                    )?);
                } else {
                    let raw_prepared = raw_prepared.ok_or_else(|| {
                        DbError::Message("Merge cleanup has no prepared candidate".to_string())
                    })?;
                    let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                        .map_err(|error| DbError::context("prepared Merge cleanup", error))?;
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
                validate_terminal_candidate_authority_on(&tx, &root, &candidate, &durable)?;
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
                        DbError::context(
                            format!("reconcile excluded-author head {object_id}"),
                            error,
                        )
                    })?;
                finish_remote_candidate_nonactivation_on(&tx, object_id, remote, inert)?;
                tx.commit().map_err(DbError::from)
            })
            .await
    }

    pub async fn adopt_alternate_merge_head(
        &self,
        write_id: WriteId,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let (raw_status, raw_prepared): (String, String) = tx
                    .query_row(
                        "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(DbError::from)?;
                let status: WriteStatus = serde_json::from_str(&raw_status)
                    .map_err(|error| DbError::context("alternate Merge head status", error))?;
                if !matches!(status, WriteStatus::Publishing) {
                    return Err(DbError::Message(format!(
                        "Merge candidate {write_id} is not publishing"
                    )));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| DbError::context("prepared Merge candidate", error))?;
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
                .map_err(|error| DbError::context("verify alternate Merge head", error))?;
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
                    DbError::context("serialize alternate Merge preparation", error)
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
