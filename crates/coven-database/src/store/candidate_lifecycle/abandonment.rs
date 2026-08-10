use super::*;
use crate::store::StoreSession;

impl StoreSession<'_> {
    fn mark_candidate_cleanup_absent(&mut self, object: ExactObjectRef) -> Result<(), DbError> {
        let conn = self.records.conn;
        let object_id = remote_object_id(&object);
        let mut remote = load_remote_object_on(conn, object_id)?;
        if remote.cleanup_target() != Some(&object) {
            return Err(DbError::Message(format!(
                "remote object {object_id} is not awaiting exact cleanup"
            )));
        }
        remote.mark_absent_verified().map_err(|error| {
            DbError::context(format!("mark candidate {object_id} absent"), error)
        })?;
        update_remote_object_on(conn, object_id, &remote)
    }

    fn blocked_merge_candidate(
        &mut self,
        write_id: WriteId,
    ) -> Result<Option<BlockedMergeCandidate>, DbError> {
        let records = self.records;
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
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
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("blocked Merge candidate status", error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(format!("write {write_id} is not blocked")));
        }
        let Some(raw_prepared) = raw_prepared else {
            return Ok(None);
        };
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("blocked Merge candidate preparation", error))?;
        let PreparedStoreWriteState::Publication { .. } = &prepared else {
            return Ok(None);
        };
        let candidate = parse_prepared_merge_candidate_on(records, verified_authority, &prepared)?;
        if candidate.commit.write_id != write_id {
            return Err(DbError::Message(
                "blocked Merge candidate differs from its write identity".to_string(),
            ));
        }
        Ok(Some(BlockedMergeCandidate {
            commit: candidate.commit,
            commit_bytes: candidate.canonical_signed_bytes,
            commit_object: candidate.reference.object.clone(),
            head: candidate.head,
            head_object: candidate.head_object,
        }))
    }

    fn prepared_merge_abandonment_candidates(
        &mut self,
        write_id: WriteId,
    ) -> Result<Option<PreparedMergeAbandonmentCandidates>, DbError> {
        let records = self.records;
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
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
            .map_err(|error| DbError::context("prepared Merge abandonment", error))?;
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
                    records,
                    verified_authority,
                    candidate_commit.semantic_bytes(),
                    candidate_commit.prepared().reference(),
                    candidate_head.semantic_bytes(),
                    candidate_head.prepared().reference(),
                )?,
            ),
            authority: blocked_merge_candidate_from_prepared(
                parse_prepared_merge_candidate_parts_on(
                    records,
                    verified_authority,
                    authority_commit.semantic_bytes(),
                    authority_commit.prepared().reference(),
                    authority_head.semantic_bytes(),
                    authority_head.prepared().reference(),
                )?,
            ),
        }))
    }

    fn author_exclusion_activation_for_candidate(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        candidate: StoreBatchCommitRef,
        author: StoreDeviceRegistrationRef,
    ) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
        let records = self.records;
        author_exclusion_activation_for_candidate_on(
            records,
            self.verified_store_authority,
            &root,
            &candidate,
            &author,
        )
    }

    fn begin_blocked_merge_candidate_nonactivation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        write_id: WriteId,
        nonactivation: BlockedMergeCandidateNonactivation,
    ) -> Result<(), DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, raw_prepared): (String, String) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("blocked Merge candidate status", error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(format!(
                "Merge candidate {write_id} is not blocked"
            )));
        }
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("blocked Merge candidate preparation", error))?;
        let PreparedStoreWriteState::Publication { .. } = &prepared else {
            return Err(DbError::Message(
                "author exclusion reached a non-candidate Merge preparation".to_string(),
            ));
        };
        let store_transaction = crate::store::StoreTransaction::new(&tx, self.records.store_dir);
        let candidate =
            store_transaction.prepared_merge_candidate(verified_authority, &prepared)?;
        store_transaction.begin_blocked_merge_candidate_nonactivation(
            verified_authority,
            &root,
            &write_id,
            &candidate,
            &nonactivation,
            true,
            &[],
        )?;
        tx.commit().map_err(DbError::from)
    }

    fn begin_prepared_merge_abandonment_nonactivation(
        &mut self,
        root: coven_protocol::store_commit::StoreRootRef,
        write_id: WriteId,
        candidate_nonactivation: BlockedMergeCandidateNonactivation,
        authority_nonactivation: BlockedMergeCandidateNonactivation,
    ) -> Result<WriteStatus, DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, raw_prepared): (String, String) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("blocked Merge abandonment status", error))?;
        if !matches!(status, WriteStatus::Publishing | WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "Merge abandonment candidates are not publishing or blocked".to_string(),
            ));
        }
        let mut prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("blocked Merge abandonment preparation", error))?;
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
        let store_transaction = crate::store::StoreTransaction::new(&tx, self.records.store_dir);
        let candidate = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            candidate_commit.semantic_bytes(),
            candidate_commit.prepared().reference(),
            candidate_head.semantic_bytes(),
            candidate_head.prepared().reference(),
        )?;
        let authority = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            authority_commit.semantic_bytes(),
            authority_commit.prepared().reference(),
            authority_head.semantic_bytes(),
            authority_head.prepared().reference(),
        )?;
        store_transaction.begin_blocked_merge_candidate_nonactivation(
            verified_authority,
            &root,
            &write_id,
            &candidate,
            &candidate_nonactivation,
            true,
            &[],
        )?;
        store_transaction.begin_blocked_merge_candidate_nonactivation(
            verified_authority,
            &root,
            &write_id,
            &authority,
            &authority_nonactivation,
            false,
            &[],
        )?;
        *outcome = MergeAbandonmentOutcome::AuthorExcluded;
        let replacement = serde_json::to_string(&prepared).map_err(|error| {
            DbError::context("serialize excluded Merge abandonment preparation", error)
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
        let blocked =
            WriteStatus::Blocked(coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: format!(
                    "Store author {device_id} was excluded before candidate activation"
                ),
            });
        Database::set_write_status_on(&tx, &write_id, &blocked)?;
        tx.commit().map_err(DbError::from)?;
        Ok(blocked)
    }

    fn merge_abandonment_state(
        &mut self,
        write_id: WriteId,
    ) -> Result<MergeAbandonmentState, DbError> {
        let records = self.records;
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
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
            .map_err(|error| DbError::context("prepared Merge abandonment", error))?;
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
            records,
            verified_authority,
            candidate_commit.semantic_bytes(),
            candidate_commit.prepared().reference(),
            candidate_head.semantic_bytes(),
            candidate_head.prepared().reference(),
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
            MergeAbandonmentOutcome::AuthorExcluded => MergeAbandonmentState::AuthorExcluded,
        })
    }

    fn resume_winning_merge_candidate(&mut self, write_id: WriteId) -> Result<(), DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, raw_prepared): (String, String) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("winning Merge candidate status", error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "winning Merge candidate is not blocked".to_string(),
            ));
        }
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("winning Merge candidate preparation", error))?;
        let PreparedStoreWriteState::MergeAbandonment {
            candidate_commit,
            candidate_head,
            candidate_history_evidence,
            authority_commit,
            authority_head,
            authority_history_evidence: _,
            outcome: MergeAbandonmentOutcome::Lost { winner_commit, .. },
            local_cleanup,
            completion,
        } = prepared
        else {
            return Err(DbError::Message(
                "Merge abandonment did not lose to its prepared candidate".to_string(),
            ));
        };
        let store_transaction = crate::store::StoreTransaction::new(&tx, self.records.store_dir);
        let candidate = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            candidate_commit.semantic_bytes(),
            candidate_commit.prepared().reference(),
            candidate_head.semantic_bytes(),
            candidate_head.prepared().reference(),
        )?;
        if winner_commit != candidate.reference {
            return Err(DbError::Message(
                "Merge abandonment winner is another candidate".to_string(),
            ));
        }
        let authority = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            authority_commit.semantic_bytes(),
            authority_commit.prepared().reference(),
            authority_head.semantic_bytes(),
            authority_head.prepared().reference(),
        )?;
        remove_cleaned_merge_authority_on(&tx, &authority)?;
        let replacement = PreparedStoreWriteState::Publication {
            commit: candidate_commit,
            head: candidate_head,
            history_evidence: candidate_history_evidence,
            local_cleanup,
            completion,
        };
        let replacement = serde_json::to_string(&replacement)
            .map_err(|error| DbError::context("serialize winning Merge candidate", error))?;
        let publishing = serde_json::to_string(&WriteStatus::Publishing)
            .map_err(|error| DbError::context("serialize winning Merge status", error))?;
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
    }

    fn finish_lost_merge_abandonment(&mut self, write_id: WriteId) -> Result<(), DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, raw_prepared): (String, String) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("lost Merge abandonment status", error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "lost Merge abandonment is not blocked".to_string(),
            ));
        }
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("lost Merge abandonment preparation", error))?;
        let PreparedStoreWriteState::MergeAbandonment {
            candidate_commit,
            candidate_head,
            candidate_history_evidence,
            authority_commit,
            authority_head,
            authority_history_evidence: _,
            outcome: MergeAbandonmentOutcome::Lost { winner_commit, .. },
            local_cleanup,
            completion,
        } = prepared
        else {
            return Err(DbError::Message(
                "Merge abandonment has no third-candidate winner".to_string(),
            ));
        };
        let store_transaction = crate::store::StoreTransaction::new(&tx, self.records.store_dir);
        let candidate = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            candidate_commit.semantic_bytes(),
            candidate_commit.prepared().reference(),
            candidate_head.semantic_bytes(),
            candidate_head.prepared().reference(),
        )?;
        if winner_commit == candidate.reference {
            return Err(DbError::Message(
                "original candidate won the Merge abandonment race".to_string(),
            ));
        }
        let authority = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            authority_commit.semantic_bytes(),
            authority_commit.prepared().reference(),
            authority_head.semantic_bytes(),
            authority_head.prepared().reference(),
        )?;
        remove_cleaned_merge_authority_on(&tx, &authority)?;
        let replacement = PreparedStoreWriteState::Publication {
            commit: candidate_commit,
            head: candidate_head,
            history_evidence: candidate_history_evidence,
            local_cleanup,
            completion,
        };
        let replacement = serde_json::to_string(&replacement)
            .map_err(|error| DbError::context("serialize lost Merge candidate", error))?;
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
    }

    fn finish_author_excluded_merge_abandonment(
        &mut self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.records.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let (raw_status, raw_prepared): (String, String) = tx
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("excluded Merge abandonment status", error))?;
        if !matches!(status, WriteStatus::Blocked(_)) {
            return Err(DbError::Message(
                "excluded Merge abandonment is not blocked".to_string(),
            ));
        }
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("excluded Merge abandonment preparation", error))?;
        let PreparedStoreWriteState::MergeAbandonment {
            candidate_commit,
            candidate_head,
            candidate_history_evidence,
            authority_commit,
            authority_head,
            authority_history_evidence: _,
            outcome: MergeAbandonmentOutcome::AuthorExcluded,
            local_cleanup,
            completion,
        } = prepared
        else {
            return Err(DbError::Message(
                "Merge abandonment has no author-exclusion outcome".to_string(),
            ));
        };
        let store_transaction = crate::store::StoreTransaction::new(&tx, self.records.store_dir);
        let candidate = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            candidate_commit.semantic_bytes(),
            candidate_commit.prepared().reference(),
            candidate_head.semantic_bytes(),
            candidate_head.prepared().reference(),
        )?;
        let authority = store_transaction.prepared_merge_candidate_parts(
            verified_authority,
            authority_commit.semantic_bytes(),
            authority_commit.prepared().reference(),
            authority_head.semantic_bytes(),
            authority_head.prepared().reference(),
        )?;
        if !merge_candidate_cleanup_targets_on(&tx, &write_id, &candidate, true, &[])?.is_empty()
            || !merge_candidate_cleanup_targets_on(&tx, &write_id, &authority, false, &[])?
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
            history_evidence: candidate_history_evidence,
            local_cleanup,
            completion,
        };
        let replacement = serde_json::to_string(&replacement).map_err(|error| {
            DbError::context("serialize cleaned excluded Merge abandonment", error)
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
    }
}

impl StoreDatabase {
    pub async fn mark_candidate_cleanup_absent(
        &self,
        object: ExactObjectRef,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.mark_candidate_cleanup_absent(object))
            .await
    }

    pub async fn blocked_merge_candidate(
        &self,
        write_id: WriteId,
    ) -> Result<Option<BlockedMergeCandidate>, DbError> {
        self.call_store(move |session| session.blocked_merge_candidate(write_id))
            .await
    }

    pub async fn prepared_merge_abandonment_candidates(
        &self,
        write_id: WriteId,
    ) -> Result<Option<PreparedMergeAbandonmentCandidates>, DbError> {
        self.call_store(move |session| session.prepared_merge_abandonment_candidates(write_id))
            .await
    }

    pub async fn author_exclusion_activation_for_candidate(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        candidate: StoreBatchCommitRef,
        author: StoreDeviceRegistrationRef,
    ) -> Result<Option<AuthorExclusionActivationLocator>, DbError> {
        self.call_store(move |session| {
            session.author_exclusion_activation_for_candidate(root, candidate, author)
        })
        .await
    }

    pub async fn begin_blocked_merge_candidate_nonactivation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        write_id: WriteId,
        nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let nonactivation = blocked_merge_candidate_nonactivation(nonactivation)?;
        self.call_store(move |session| {
            session.begin_blocked_merge_candidate_nonactivation(root, write_id, nonactivation)
        })
        .await
    }

    pub async fn begin_prepared_merge_abandonment_nonactivation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        write_id: WriteId,
        candidate_nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
        authority_nonactivation: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let candidate_nonactivation =
            blocked_merge_candidate_nonactivation(candidate_nonactivation)?;
        let authority_nonactivation =
            blocked_merge_candidate_nonactivation(authority_nonactivation)?;
        let notified_write_id = write_id.clone();
        let blocked = self
            .call_store(move |session| {
                session.begin_prepared_merge_abandonment_nonactivation(
                    root,
                    write_id,
                    candidate_nonactivation,
                    authority_nonactivation,
                )
            })
            .await?;
        self.notify_write_status(notified_write_id, blocked);
        Ok(())
    }

    pub async fn merge_abandonment_state(
        &self,
        write_id: &WriteId,
    ) -> Result<MergeAbandonmentState, DbError> {
        let write_id = write_id.clone();
        self.call_store(move |session| session.merge_abandonment_state(write_id))
            .await
    }

    pub async fn resume_winning_merge_candidate(&self, write_id: WriteId) -> Result<(), DbError> {
        let notified_write_id = write_id.clone();
        self.call_store(move |session| session.resume_winning_merge_candidate(write_id))
            .await?;
        self.notify_write_status(notified_write_id, WriteStatus::Publishing);
        Ok(())
    }

    pub async fn finish_lost_merge_abandonment(&self, write_id: WriteId) -> Result<(), DbError> {
        self.call_store(move |session| session.finish_lost_merge_abandonment(write_id))
            .await
    }

    pub async fn finish_author_excluded_merge_abandonment(
        &self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        self.call_store(move |session| session.finish_author_excluded_merge_abandonment(write_id))
            .await
    }
}
