use super::*;

pub(super) fn complete_merge_retraction_closure(
    direct_dependencies: &BTreeMap<StoreBatchCommitRef, BTreeSet<StoreBatchCommitRef>>,
    mut closure: BTreeSet<StoreBatchCommitRef>,
) -> BTreeSet<StoreBatchCommitRef> {
    loop {
        let additions = direct_dependencies
            .iter()
            .filter(|(reference, _)| !closure.contains(*reference))
            .filter(|(_, dependencies)| {
                dependencies
                    .iter()
                    .any(|dependency| closure.contains(dependency))
            })
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        if additions.is_empty() {
            return closure;
        }
        closure.extend(additions);
    }
}

pub(super) fn require_exact_merge_retraction_closure(
    direct_dependencies: &BTreeMap<StoreBatchCommitRef, BTreeSet<StoreBatchCommitRef>>,
    roots: BTreeSet<StoreBatchCommitRef>,
    provided: &BTreeSet<StoreBatchCommitRef>,
) -> Result<(), DbError> {
    let required = complete_merge_retraction_closure(direct_dependencies, roots);
    if provided != &required {
        return Err(DbError::Message(
            "verified terminal retractions do not exactly cover excluded materializations"
                .to_string(),
        ));
    }
    Ok(())
}

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn insert_merge_retraction_cleanup(
        &self,
        retained: &OwnedVerifiedMergeMaterialization,
    ) -> Result<(), DbError> {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &retained.commit_ref().coord;
        let input = crate::database::store::materialization_models::MergeRetractionCleanupInput {
            commit: crate::protocol::objects::PreparedExactObject::new(
                retained.commit_ref().object.clone(),
                retained.commit().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
            activation_head: crate::protocol::objects::PreparedExactObject::new(
                retained.activation_head_object().clone(),
                retained.activation_head().to_bytes(),
            )
            .map_err(|error| DbError::Message(error.to_string()))?,
        };
        let canonical_cleanup = serde_json::to_vec(&input).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup: {error}"))
        })?;
        let cleanup_hash = ObjectHash::digest(&canonical_cleanup);
        let stream_id = stream_id.to_string();
        let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
        let encoded_ref = serde_json::to_string(&retained.commit_ref()).map_err(|error| {
            DbError::Message(format!("serialize Merge retraction cleanup ref: {error}"))
        })?;
        self.transaction
            .execute(
                "INSERT INTO merge_retraction_cleanups
                 (device_id, seq, commit_ref, cleanup_hash, canonical_cleanup)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    &stream_id,
                    sequence_sql,
                    &encoded_ref,
                    cleanup_hash.to_string(),
                    &canonical_cleanup,
                ],
            )
            .map_err(DbError::from)?;
        StoreDatabase::load_merge_retraction_cleanup_on(self.transaction, retained.commit_ref())?;
        Ok(())
    }

    pub(crate) fn retire_circle_bootstrap_coverage(
        &self,
        activation_commit: &StoreBatchCommitRef,
    ) -> Result<usize, DbError> {
        let encoded = serde_json::to_string(activation_commit).map_err(|error| {
            DbError::Message(format!(
                "serialize retracted Circle bootstrap activation: {error}"
            ))
        })?;
        self.transaction
            .execute(
                "DELETE FROM circle_bootstrap_coverage WHERE activation_commit = ?1",
                [encoded],
            )
            .map_err(DbError::from)
    }

    pub(crate) fn retract_verified_merge_materializations(
        &self,
        root: &crate::protocol::store_commit::StoreRootRef,
        retained_merge_materializations: &mut RetainedMergeMaterializationCache,
        retractions: Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>,
    ) -> Result<Vec<(WriteId, WriteStatus)>, DbError> {
        let conn = self.transaction;
        let provided = retractions
            .iter()
            .map(|retraction| {
                retraction
                    .candidate_reference()
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let retained = retained_merge_materializations.replay_inputs_on(conn, root)?;
        let mut required = BTreeSet::new();
        for retained in &retained {
            if author_exclusion_activation_for_candidate_on(
                conn,
                root,
                retained.commit_ref(),
                &retained.commit().author_registration,
            )?
            .is_some()
            {
                required.insert(retained.commit_ref().clone());
            }
        }
        for retraction in &retractions {
            if matches!(
                retraction.proof(),
                crate::protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. }
            ) {
                required.insert(
                    retraction
                        .candidate_reference()
                        .map_err(|error| DbError::Message(error.to_string()))?,
                );
            }
        }
        let direct_dependencies = retained
            .iter()
            .map(|retained| {
                let mut direct = retained
                    .commit()
                    .order
                    .dependencies()
                    .values()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if let Some(predecessor) = retained.commit().order.predecessor() {
                    direct.insert(predecessor.clone());
                }
                (retained.commit_ref().clone(), direct)
            })
            .collect::<BTreeMap<_, _>>();
        require_exact_merge_retraction_closure(&direct_dependencies, required, &provided)?;
        let mut notifications = Vec::new();
        for verified in retractions {
            let (nonactivation, head_nonactivation) =
                verified
                    .into_terminal_head_nonactivation()
                    .map_err(|error| DbError::Message(error.to_string()))?;
            let candidate = nonactivation
                .reference()
                .map_err(|error| DbError::Message(error.to_string()))?;
            validate_terminal_nonactivation_authority_on(conn, root, &nonactivation)?;
            match nonactivation.proof() {
                crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                    exclusion,
                    accepted_cut,
                    activation_head,
                } => {
                    let locator =
                        load_author_exclusion_activation_locator_on(conn, root, exclusion)?;
                    if locator.accepted_cut() != accepted_cut
                        || locator.activation_head() != activation_head
                    {
                        return Err(DbError::Message(
                            "terminal Merge retraction differs from its activated exclusion"
                                .to_string(),
                        ));
                    }
                }
                crate::protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. } => {}
                crate::protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {}
                crate::protocol::remote_object::CandidateNonactivationProof::MergeWinner { .. } => {
                    return Err(DbError::Message(
                        "terminal Merge retraction carries nonterminal evidence".to_string(),
                    ));
                }
            }
            let StoreCommitCoord {
                stream_id,
                sequence,
            } = &candidate.coord;
            let stream_id = stream_id.to_string();
            let sequence_sql = Database::sequence_to_sqlite(&stream_id, *sequence)?;
            let encoded_ref = serde_json::to_string(&candidate).map_err(|error| {
                DbError::Message(format!("serialize retracted Merge commit: {error}"))
            })?;
            let input_hash: String = conn
                .query_row(
                    "SELECT retained_input_hash FROM materialized_commits
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                    rusqlite::params![&stream_id, sequence_sql, &encoded_ref],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let retained = StoreDatabase::load_retained_merge_materialization_on(
                conn,
                root,
                &stream_id,
                *sequence,
                &candidate,
                &input_hash,
            )?;
            if retained.commit().to_bytes() != nonactivation.candidate().canonical_signed_bytes
                || retained.activation_head_object() != head_nonactivation.head().object()
            {
                return Err(DbError::Message(
                    "terminal retraction differs from its retained materialization".to_string(),
                ));
            }
            self.insert_merge_retraction_cleanup(&retained)?;
            let replay_owner = RetainedReplayOwner::Commit {
                commit: candidate.clone(),
                input_hash: retained.input_hash(),
            };
            let mut replay_statement = conn
                .prepare(
                    "SELECT object_id FROM retained_replay_objects
                     WHERE device_id = ?1 AND seq = ?2
                     ORDER BY object_id",
                )
                .map_err(DbError::from)?;
            let replay_object_ids = replay_statement
                .query_map(rusqlite::params![&stream_id, sequence_sql], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(DbError::from)?
                .map(|row| {
                    let encoded = row.map_err(DbError::from)?;
                    encoded.parse().map_err(|error| {
                        DbError::Message(format!(
                            "retracted Merge replay object id {encoded}: {error}"
                        ))
                    })
                })
                .collect::<Result<BTreeSet<ObjectHash>, DbError>>()?;
            drop(replay_statement);
            let head_object_id = remote_object_id(retained.activation_head_object());
            let mut activated_object_ids = candidate_graph_exact_objects(retained.commit())?
                .iter()
                .map(remote_object_id)
                .collect::<BTreeSet<_>>();
            activated_object_ids.extend(replay_object_ids.iter().copied());
            if let Some(membership_objects) = retained.membership_objects() {
                activated_object_ids.extend(membership_objects.object_ids());
            }
            activated_object_ids.insert(remote_object_id(&candidate.object));
            activated_object_ids.insert(head_object_id);
            for object_id in &replay_object_ids {
                let mut remote = load_remote_object_on(conn, *object_id)?;
                remote
                    .remove_retained_replay_owner(&replay_owner)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "remove retracted replay owner from {object_id}: {error}"
                        ))
                    })?;
                update_remote_object_on(conn, *object_id, &remote)?;
            }
            conn.execute(
                "DELETE FROM retained_replay_objects WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence_sql],
            )
            .map_err(DbError::from)?;
            for object_id in activated_object_ids {
                let mut remote = load_remote_object_on(conn, object_id)?
                    .into_observed_activated(&candidate)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "record observed Merge activation for {object_id}: {error}"
                        ))
                    })?;
                let inert = remote
                    .retract_activated_candidate(
                        nonactivation.clone(),
                        (object_id == head_object_id).then_some(&head_nonactivation),
                    )
                    .map_err(|error| {
                        DbError::Message(format!(
                            "retract activated Merge object {object_id}: {error}"
                        ))
                    })?;
                finish_remote_candidate_nonactivation_on(conn, object_id, remote, inert)?;
            }
            let deleted = conn
                .execute(
                    "DELETE FROM materialized_commits
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                    rusqlite::params![&stream_id, sequence_sql, &encoded_ref],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "retracted Merge materialization disappeared".to_string(),
                ));
            }
            let deleted = conn
                .execute(
                    "DELETE FROM store_device_state_snapshots WHERE commit_ref = ?1",
                    [&encoded_ref],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "retracted Merge device state disappeared".to_string(),
                ));
            }
            self.retire_circle_bootstrap_coverage(&candidate)?;
            let deleted = conn
                .execute(
                    "DELETE FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3 AND input_hash = ?4",
                    rusqlite::params![
                        &stream_id,
                        sequence_sql,
                        &encoded_ref,
                        retained.input_hash().to_string()
                    ],
                )
                .map_err(DbError::from)?;
            if deleted != 1 {
                return Err(DbError::Message(
                    "retracted Merge retained input disappeared".to_string(),
                ));
            }
            let raw_status: Option<String> = conn
                .query_row(
                    "SELECT status FROM store_writes WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(raw_status) = raw_status {
                let stored_status: WriteStatus =
                    serde_json::from_str(&raw_status).map_err(|error| {
                        DbError::Message(format!("retracted Merge write status: {error}"))
                    })?;
                let original = match stored_status {
                    WriteStatus::Published(original) if original.commit() == &candidate => {
                        *original
                    }
                    WriteStatus::Publishing | WriteStatus::Blocked(_) => PublishedPosition {
                        device_id: retained.commit().author_registration.device_id.to_string(),
                        commit: candidate.clone(),
                    },
                    WriteStatus::Resolved(WriteResolution::Retracted { witness })
                        if witness.original_position().commit() == &candidate =>
                    {
                        return Err(DbError::Message(
                            "retracted Merge write still owns an active materialization"
                                .to_string(),
                        ));
                    }
                    other => {
                        return Err(DbError::Message(format!(
                            "retracted Merge write has incompatible status {other:?}"
                        )));
                    }
                };
                let witness = crate::WriteRetractionWitness::new(original, nonactivation.clone())
                    .map_err(DbError::Message)?;
                let status = WriteStatus::Resolved(WriteResolution::Retracted { witness });
                conn.execute(
                    "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                )
                .map_err(DbError::from)?;
                conn.execute(
                    "DELETE FROM store_write_packages WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                )
                .map_err(DbError::from)?;
                conn.execute(
                    "DELETE FROM store_write_blobs WHERE write_id = ?1",
                    [retained.commit().write_id.as_str()],
                )
                .map_err(DbError::from)?;
                Database::set_write_status_on(conn, &retained.commit().write_id, &status)?;
                notifications.push((retained.commit().write_id.clone(), status));
            }
        }
        Ok(notifications)
    }
}
