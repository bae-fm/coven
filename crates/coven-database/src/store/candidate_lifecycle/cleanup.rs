use super::*;
use crate::query_mapped_rows;
use crate::store::StoreSession;

impl StoreSession<'_> {
    fn merge_candidate_cleanup_pending(&mut self, write_id: &WriteId) -> Result<bool, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let (raw_status, raw_prepared): (String, Option<String>) = conn
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("Merge cleanup status", error))?;
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
                            DbError::context("serialize Merge retraction cleanup ref", error)
                        })?,
                    ],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if exists {
                crate::StoreDatabase::load_merge_retraction_cleanup_on(
                    records,
                    verified_authority,
                    candidate,
                )?;
            }
            return Ok(exists);
        }
        let Some(raw_prepared) = raw_prepared else {
            return Ok(false);
        };
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("prepared Merge cleanup", error))?;
        let candidate = parse_prepared_merge_candidate_on(records, verified_authority, &prepared)?;
        let cleanup_pending = |candidate: &PreparedMergeCandidate| -> Result<bool, DbError> {
            let remote =
                load_remote_object_on(conn, remote_object_id(&candidate.reference.object))?;
            Ok(matches!(
                remote,
                RemoteObjectRecord::CandidateCommit(
                    coven_protocol::remote_object::CandidateCommitRecord {
                        state:
                            coven_protocol::remote_object::CandidateCommitState::CleanupPending {
                                proof: coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. }
                                    | coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
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
                    records,
                    verified_authority,
                    authority_commit.semantic_bytes(),
                    authority_commit.prepared().reference(),
                    authority_head.semantic_bytes(),
                    authority_head.prepared().reference(),
                )?;
                match outcome {
                    MergeAbandonmentOutcome::Prepared => Ok(false),
                    MergeAbandonmentOutcome::Accepted { .. } => cleanup_pending(&candidate),
                    MergeAbandonmentOutcome::AuthorExcluded => {
                        Ok(cleanup_pending(&candidate)? || cleanup_pending(&authority)?)
                    }
                    MergeAbandonmentOutcome::Lost { winner_commit, .. } => Ok((winner_commit
                        != &candidate.reference
                        && cleanup_pending(&candidate)?)
                        || cleanup_pending(&authority)?),
                }
            }
        }
    }

    fn merge_candidate_cleanup_targets(
        &mut self,
        write_id: &WriteId,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let (raw_status, raw_prepared): (String, Option<String>) = conn
            .query_row(
                "SELECT status, prepared FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("Merge cleanup status", error))?;
        if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = &status {
            witness.validate().map_err(DbError::Message)?;
            let candidate = crate::StoreDatabase::load_merge_retraction_cleanup_on(
                records,
                verified_authority,
                witness.original_position().commit(),
            )?;
            if candidate.commit.write_id != *write_id {
                return Err(DbError::Message(
                    "Merge retraction cleanup names another write".to_string(),
                ));
            }
            return merge_candidate_cleanup_targets_on(conn, write_id, &candidate, false, &[]);
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
            .map_err(|error| DbError::context("prepared Merge cleanup", error))?;
        let candidate = parse_prepared_merge_candidate_on(records, verified_authority, &prepared)?;
        match &prepared {
            PreparedStoreWriteState::Publication { .. } => {
                merge_candidate_cleanup_targets_on(conn, write_id, &candidate, true, &[])
            }
            PreparedStoreWriteState::MergeAbandonment {
                outcome,
                authority_commit,
                authority_head,
                ..
            } => {
                let authority = parse_prepared_merge_candidate_parts_on(
                    records,
                    verified_authority,
                    authority_commit.semantic_bytes(),
                    authority_commit.prepared().reference(),
                    authority_head.semantic_bytes(),
                    authority_head.prepared().reference(),
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
                            conn,
                            write_id,
                            &candidate,
                            true,
                            &[],
                        )?);
                    }
                    MergeAbandonmentOutcome::AuthorExcluded => {
                        targets.extend(merge_candidate_cleanup_targets_on(
                            conn,
                            write_id,
                            &candidate,
                            true,
                            &[],
                        )?);
                        targets.extend(merge_candidate_cleanup_targets_on(
                            conn,
                            write_id,
                            &authority,
                            false,
                            &[],
                        )?);
                    }
                    MergeAbandonmentOutcome::Lost { winner_commit, .. } => {
                        if winner_commit != &candidate.reference {
                            targets.extend(merge_candidate_cleanup_targets_on(
                                conn,
                                write_id,
                                &candidate,
                                true,
                                &[],
                            )?);
                        }
                        targets.extend(merge_candidate_cleanup_targets_on(
                            conn,
                            write_id,
                            &authority,
                            false,
                            &[],
                        )?);
                    }
                }
                Ok(targets)
            }
        }
    }

    fn finish_retracted_merge_candidate_cleanup(
        &mut self,
        write_id: &WriteId,
    ) -> Result<(), DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let raw_status: String = tx
            .query_row(
                "SELECT status FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("Merge retraction cleanup status", error))?;
        let WriteStatus::Resolved(WriteResolution::Retracted { witness }) = status else {
            return Ok(());
        };
        witness.validate().map_err(DbError::Message)?;
        let candidate_ref = witness.original_position().commit().clone();
        let candidate = crate::StoreDatabase::load_merge_retraction_cleanup_on(
            crate::store::StoreRecords::new(&tx, self.store_dir),
            verified_authority,
            &candidate_ref,
        )?;
        if candidate.commit.write_id != *write_id {
            return Err(DbError::Message(
                "Merge retraction cleanup names another write".to_string(),
            ));
        }
        finish_merge_retraction_cleanup_on(&tx, &candidate)?;
        tx.commit().map_err(DbError::from)
    }

    fn pending_merge_retraction_cleanups(&mut self) -> Result<Vec<StoreBatchCommitRef>, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let rows = query_mapped_rows(
            conn,
            "SELECT device_id, seq, commit_ref
             FROM merge_retraction_cleanups
             ORDER BY device_id, seq",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        rows.into_iter()
            .map(|(stream_id, sequence, encoded_ref)| {
                let sequence = Database::sequence_from_sqlite(&stream_id, sequence)?;
                let candidate = crate::StoreDatabase::parse_stored_commit_ref(
                    &stream_id,
                    sequence,
                    &encoded_ref,
                )?;
                crate::StoreDatabase::load_merge_retraction_cleanup_on(
                    records,
                    verified_authority,
                    &candidate,
                )?;
                Ok(candidate)
            })
            .collect()
    }

    fn merge_retraction_cleanup_verification(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        candidate: &StoreBatchCommitRef,
    ) -> Result<TerminalCandidateCleanupVerification, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let prepared = crate::StoreDatabase::load_merge_retraction_cleanup_on(
            records,
            verified_authority,
            candidate,
        )?;
        let remote = load_remote_object_on(conn, remote_object_id(&candidate.object))?;
        let proof = remote
            .candidate_nonactivation_proof(candidate)
            .map_err(|error| DbError::Message(error.to_string()))?
            .ok_or_else(|| {
                DbError::Message("Merge retraction cleanup has no terminal proof".to_string())
            })?;
        let authority = match proof {
            coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                exclusion,
                ..
            } => TerminalCandidateAuthority::AuthorExclusion(
                load_author_exclusion_activation_locator_on(
                    records,
                    verified_authority,
                    root,
                    exclusion,
                )?,
            ),
            coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
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
            coven_protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
                let durable = coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
                    candidate,
                    &prepared.commit,
                    proof.clone(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                validate_terminal_nonactivation_authority_on(
                    records,
                    verified_authority,
                    root,
                    &durable,
                )?;
                TerminalCandidateAuthority::DependencyRetraction(
                    coven_protocol::remote_object::VerifiedDependencyRetractionAuthority::after_live_authority_check(durable)
                        .map_err(|error| DbError::Message(error.to_string()))?,
                )
            }
            coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
                return Err(DbError::Message(
                    "Merge retraction cleanup has nonterminal proof".to_string(),
                ));
            }
        };
        Ok(TerminalCandidateCleanupVerification {
            authority,
            candidate: blocked_merge_candidate_from_prepared(prepared),
        })
    }

    fn merge_retraction_cleanup_targets(
        &mut self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let prepared = crate::StoreDatabase::load_merge_retraction_cleanup_on(
            records,
            verified_authority,
            candidate,
        )?;
        merge_candidate_cleanup_targets_on(conn, &prepared.commit.write_id, &prepared, false, &[])
    }

    fn confirm_merge_retraction_cleanup_nonactivation(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        candidate: &StoreBatchCommitRef,
        durable: &coven_protocol::remote_object::CandidateNonactivation,
        head_nonactivation: &coven_protocol::remote_object::VerifiedCandidateHeadNonactivation,
    ) -> Result<(), DbError> {
        let records = crate::store::StoreRecords::new(self.conn, self.store_dir);
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let prepared = crate::StoreDatabase::load_merge_retraction_cleanup_on(
            records,
            verified_authority,
            candidate,
        )?;
        if durable
            .reference()
            .map_err(|error| DbError::Message(error.to_string()))?
            != *candidate
            || head_nonactivation.head().object() != &prepared.head_object
        {
            return Err(DbError::Message(
                "verified Merge retraction cleanup names another candidate".to_string(),
            ));
        }
        if let coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion,
            accepted_cut,
            activation_head,
        } = durable.proof()
        {
            let locator = load_author_exclusion_activation_locator_on(
                records,
                verified_authority,
                root,
                exclusion,
            )?;
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
            .candidate_nonactivation_proof(candidate)
            .map_err(|error| DbError::Message(error.to_string()))?
            != Some(durable.proof())
        {
            return Err(DbError::Message(
                "verified Merge retraction differs from candidate ownership".to_string(),
            ));
        }
        if !matches!(
            load_merge_candidate_head_cleanup_on(conn, &prepared.head_object, candidate)?,
            MergeCandidateHeadCleanup::ProtocolInert
        ) {
            return Err(DbError::Message(
                "retracted Merge activation head is not retained as inert authority".to_string(),
            ));
        }
        Ok(())
    }

    fn finish_merge_retraction_cleanup(
        &mut self,
        candidate: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        let verified_authority = &mut *self.verified_store_authority;
        let conn = self.conn;
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        let prepared = crate::StoreDatabase::load_merge_retraction_cleanup_on(
            crate::store::StoreRecords::new(&tx, self.store_dir),
            verified_authority,
            candidate,
        )?;
        finish_merge_retraction_cleanup_on(&tx, &prepared)?;
        tx.commit().map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn merge_candidate_cleanup_pending(
        &self,
        write_id: &WriteId,
    ) -> Result<bool, DbError> {
        let write_id = write_id.clone();
        self.connection
            .call_store(move |session| session.merge_candidate_cleanup_pending(&write_id))
            .await
    }

    pub async fn merge_candidate_cleanup_targets(
        &self,
        write_id: WriteId,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.connection
            .call_store(move |session| session.merge_candidate_cleanup_targets(&write_id))
            .await
    }

    pub async fn finish_retracted_merge_candidate_cleanup(
        &self,
        write_id: WriteId,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| session.finish_retracted_merge_candidate_cleanup(&write_id))
            .await
    }

    pub async fn pending_merge_retraction_cleanups(
        &self,
    ) -> Result<Vec<StoreBatchCommitRef>, DbError> {
        self.connection
            .call_store(|session| session.pending_merge_retraction_cleanups())
            .await
    }

    pub async fn merge_retraction_cleanup_verification(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        candidate: StoreBatchCommitRef,
    ) -> Result<TerminalCandidateCleanupVerification, DbError> {
        self.connection
            .call_store(move |session| {
                session.merge_retraction_cleanup_verification(&root, &candidate)
            })
            .await
    }

    pub async fn merge_retraction_cleanup_targets(
        &self,
        candidate: StoreBatchCommitRef,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        self.connection
            .call_store(move |session| session.merge_retraction_cleanup_targets(&candidate))
            .await
    }

    pub async fn confirm_merge_retraction_cleanup_nonactivation(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        candidate: StoreBatchCommitRef,
        verified: coven_protocol::remote_object::VerifiedCandidateNonactivation,
    ) -> Result<(), DbError> {
        let (durable, head_nonactivation) = verified
            .into_terminal_head_nonactivation()
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.connection
            .call_store(move |session| {
                session.confirm_merge_retraction_cleanup_nonactivation(
                    &root,
                    &candidate,
                    &durable,
                    &head_nonactivation,
                )
            })
            .await
    }

    pub async fn finish_merge_retraction_cleanup(
        &self,
        candidate: StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| session.finish_merge_retraction_cleanup(&candidate))
            .await
    }
}
