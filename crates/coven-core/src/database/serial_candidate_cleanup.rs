use crate::database::blob_records::load_activated_registration_on;
use crate::database::remote_object_records::begin_remote_candidate_nonactivation_on;
use crate::database::remote_object_records::candidate_graph_exact_objects;
use crate::database::remote_object_records::load_remote_object_on;
use crate::database::remote_object_records::update_remote_object_on;

use super::*;

impl Database {
    pub(crate) async fn prepare_serial_candidate_cleanup(
        &self,
        branch_id: PendingBranchId,
        plan: &crate::sync::store_engine::SerialResolutionPlan,
    ) -> Result<Vec<CandidateCleanupObject>, DbError> {
        let accepted_refs = plan
            .commits()
            .iter()
            .map(|commit| commit.commit_ref.clone())
            .collect::<Vec<_>>();
        let accepted_commits = plan
            .commits()
            .iter()
            .map(|commit| commit.commit.clone())
            .collect::<Vec<_>>();
        let head = plan.head().clone();
        let head_bytes = plan.head_object().bytes.clone();
        let verified_suffix = plan
            .verified_suffix()
            .map_err(|error| DbError::Message(error.to_string()))?;
        self.call(move |conn| {
            if head.to_bytes() != head_bytes {
                return Err(DbError::Message(
                    "Serial cleanup head bytes differ from the verified head".to_string(),
                ));
            }
            let mut statement = conn
                .prepare(
                    "SELECT write_id, base, status, prepared FROM store_writes
                     WHERE status != '\"local_only\"'
                       AND json_extract(status, '$.published') IS NULL
                       AND json_extract(status, '$.resolved') IS NULL
                       AND json_type(base, '$.serial') IS NOT NULL
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            let mut branch_base = None;
            let mut prepared = Vec::new();
            for (write_id, raw_base, raw_status, raw_prepared) in rows {
                let base: StoreWriteBase = serde_json::from_str(&raw_base).map_err(|error| {
                    DbError::Message(format!("Serial cleanup branch base: {error}"))
                })?;
                let StoreWriteBase::Serial {
                    branch_id: stored_branch_id,
                    base,
                } = base
                else {
                    return Err(DbError::Message(
                        "MergeConcurrent base reached Serial candidate cleanup".to_string(),
                    ));
                };
                if stored_branch_id != branch_id {
                    return Err(DbError::Message(
                        "Serial database contains more than one unresolved branch".to_string(),
                    ));
                }
                let status: WriteStatus = serde_json::from_str(&raw_status).map_err(|error| {
                    DbError::Message(format!("Serial cleanup branch status: {error}"))
                })?;
                if !matches!(
                    status,
                    WriteStatus::Conflict(ref conflict) if conflict.branch_id == branch_id
                ) {
                    return Err(DbError::Message(format!(
                        "Serial cleanup write {write_id} is not conflicted"
                    )));
                }
                if branch_base.as_ref().is_some_and(|stored| stored != &base) {
                    return Err(DbError::Message(
                        "Serial cleanup branch has inconsistent bases".to_string(),
                    ));
                }
                branch_base.get_or_insert(base);
                let Some(raw_prepared) = raw_prepared else {
                    continue;
                };
                if let Some(candidate) = parse_prepared_serial_candidate(&raw_prepared)? {
                    prepared.push((
                        WriteId::from_generated(write_id),
                        candidate.commit,
                        candidate.reference,
                        candidate.canonical_signed_bytes,
                    ));
                }
            }
            if prepared.is_empty() {
                return Ok(Vec::new());
            }
            let branch_base = branch_base.expect("prepared branch has a base");
            if accepted_refs.is_empty() || accepted_refs.len() != accepted_commits.len() {
                return Err(DbError::Message(
                    "Serial candidate cleanup requires a nonempty accepted suffix".to_string(),
                ));
            }
            let mut accepted_predecessor = branch_base.clone();
            for (reference, commit) in accepted_refs.iter().zip(&accepted_commits) {
                reference
                    .verify_commit(commit)
                    .map_err(|error| DbError::Message(error.to_string()))?;
                if commit.order.predecessor() != accepted_predecessor.as_ref() {
                    return Err(DbError::Message(
                        "accepted Serial cleanup suffix has a broken exact predecessor"
                            .to_string(),
                    ));
                }
                accepted_predecessor = Some(reference.clone());
            }
            if !matches!(
                head.state,
                StoreSerialHeadState::Commit { ref commit, .. }
                    if Some(commit) == accepted_refs.last()
            ) {
                return Err(DbError::Message(
                    "Serial cleanup head does not name the accepted suffix tip".to_string(),
                ));
            }
            let mut losing_predecessor = branch_base.clone();
            let mut losing_targets = Vec::with_capacity(prepared.len());
            for (_, commit, reference, bytes) in &prepared {
                if commit.order.predecessor() != losing_predecessor.as_ref() {
                    return Err(DbError::Message(
                        "losing Serial candidates have a broken exact predecessor chain"
                            .to_string(),
                    ));
                }
                losing_targets.push(
                    crate::sync::remote_object::StoreBatchCommitDeletionTarget {
                        coord: reference.coord.clone(),
                        object: reference.object.clone(),
                        canonical_signed_bytes: bytes.clone(),
                    },
                );
                losing_predecessor = Some(reference.clone());
            }
            if accepted_refs.first() == prepared.first().map(|(_, _, reference, _)| reference) {
                return Err(DbError::Message(
                    "accepted Serial suffix begins with the losing candidate".to_string(),
                ));
            }
            if verified_suffix.durable().predecessor != branch_base
                || verified_suffix.durable().commits != accepted_refs
                || verified_suffix.durable().canonical_signed_head_bytes != head_bytes
            {
                return Err(DbError::Message(
                    "verified Serial suffix differs from the cleanup branch".to_string(),
                ));
            }
            let root = required_store_root_authority_on(conn)?;
            let verified_losing = losing_targets
                .iter()
                .zip(prepared.iter())
                .map(|(target, (_, commit, _, _))| {
                    let author = load_activated_registration_on(
                        conn,
                        &root,
                        &commit.author_registration,
                    )?;
                    Ok((target.clone(), author))
                })
                .collect::<Result<Vec<_>, DbError>>()?;
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut exclusive_cleanup = Vec::new();
            let mut commit_cleanup = Vec::new();
            for (index, (write_id, commit, reference, _)) in prepared.iter().enumerate() {
                let nonactivation = verified_suffix
                    .verify_candidate_nonactivation(verified_losing[..=index].to_vec())
                    .map_err(|error| DbError::Message(error.to_string()))?
                    .into_durable();
                let mut object_ids = candidate_graph_exact_objects(commit)?
                    .iter()
                    .map(|object| remote_object_id(object).to_string())
                    .collect::<Vec<_>>();
                let mut object_statement = tx
                    .prepare(
                        "SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1
                         ORDER BY remote_object_id",
                    )
                    .map_err(DbError::from)?;
                let indexed = object_statement
                    .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
                    .map_err(DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DbError::from)?;
                drop(object_statement);
                object_ids.extend(indexed);
                let mut manifest_cleanup = BTreeSet::new();
                for encoded in object_ids {
                    let object_id: ObjectHash = encoded.parse().map_err(|error| {
                        DbError::Message(format!("Serial cleanup remote object id: {error}"))
                    })?;
                    if let Some(object) = begin_remote_candidate_nonactivation_on(
                        &tx,
                        object_id,
                        nonactivation.clone(),
                    )? {
                        manifest_cleanup.insert(object);
                    }
                }
                for object in candidate_graph_exact_objects(commit)? {
                    if manifest_cleanup.remove(&object) {
                        exclusive_cleanup.push(CandidateCleanupObject {
                            object,
                        });
                    }
                }
                if !manifest_cleanup.is_empty() {
                    return Err(DbError::Message(format!(
                        "candidate cleanup for write {write_id} contains an object outside its signed manifest"
                    )));
                }
                let commit_object_id = remote_object_id(&reference.object);
                if let Some(object) = begin_remote_candidate_nonactivation_on(
                    &tx,
                    commit_object_id,
                    nonactivation,
                )? {
                    commit_cleanup.push(CandidateCleanupObject { object });
                }
            }
            tx.commit().map_err(DbError::from)?;
            exclusive_cleanup.extend(commit_cleanup);
            Ok(exclusive_cleanup)
        })
        .await
    }

    pub(crate) async fn prepare_serial_abandonment_authority_cleanup(
        &self,
        plan: &crate::sync::store_engine::SerialResolutionPlan,
    ) -> Result<Option<CandidateCleanupObject>, DbError> {
        let prepared = self
            .prepared_serial_candidate_abandonment()
            .await?
            .ok_or_else(|| {
                DbError::Message("Serial candidate abandonment is not prepared".to_string())
            })?;
        let accepted_refs = plan
            .commits()
            .iter()
            .map(|commit| commit.commit_ref.clone())
            .collect::<Vec<_>>();
        if accepted_refs.is_empty() {
            return Err(DbError::Message(
                "Serial abandonment cleanup requires an accepted successor".to_string(),
            ));
        }
        let authority_ref = StoreBatchCommitRef::from_commit(
            &prepared.authority.value,
            StoreCommitCoord::Serial {
                sequence: prepared.authority.value.seq(),
            },
            prepared.authority.object.clone(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        if accepted_refs.first() == Some(&authority_ref) {
            return Ok(None);
        }
        let head_bytes = plan.head_object().bytes.clone();
        if plan.head().to_bytes() != head_bytes
            || !matches!(
                &plan.head().state,
                StoreSerialHeadState::Commit { commit, .. }
                    if Some(commit) == accepted_refs.last()
            )
        {
            return Err(DbError::Message(
                "Serial abandonment cleanup head differs from its accepted suffix".to_string(),
            ));
        }
        let mut predecessor = prepared.base.clone();
        for commit in plan.commits() {
            commit
                .commit_ref
                .verify_commit(&commit.commit)
                .map_err(|error| DbError::Message(error.to_string()))?;
            if commit.commit.order.predecessor() != predecessor.as_ref() {
                return Err(DbError::Message(
                    "Serial abandonment accepted suffix has a broken predecessor".to_string(),
                ));
            }
            predecessor = Some(commit.commit_ref.clone());
        }
        let authority_target = crate::sync::store_commit::StoreBatchCommitDeletionTarget {
            coord: authority_ref.coord.clone(),
            object: authority_ref.object.clone(),
            canonical_signed_bytes: prepared.authority.bytes.clone(),
        };
        let verified_suffix = plan
            .verified_suffix()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if verified_suffix.durable().predecessor != prepared.base
            || verified_suffix.durable().commits != accepted_refs
            || verified_suffix.durable().canonical_signed_head_bytes != head_bytes
        {
            return Err(DbError::Message(
                "verified Serial suffix differs from the abandonment branch".to_string(),
            ));
        }
        let expected_state = prepared.durable_state;
        self.call(move |conn| {
            let state_matches: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM protocol_state WHERE key = ?1 AND value = ?2
                     )",
                    rusqlite::params![SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY, expected_state],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            if !state_matches {
                return Err(DbError::Message(
                    "Serial abandonment durable state changed before authority cleanup".to_string(),
                ));
            }
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let object_id = remote_object_id(&authority_ref.object);
            let root = required_store_root_authority_on(&tx)?;
            let author = load_activated_registration_on(
                &tx,
                &root,
                &prepared.authority.value.author_registration,
            )?;
            let nonactivation = verified_suffix
                .verify_candidate_nonactivation(vec![(authority_target, author)])
                .map_err(|error| DbError::Message(error.to_string()))?
                .into_durable();
            let target = begin_remote_candidate_nonactivation_on(&tx, object_id, nonactivation)?;
            tx.commit().map_err(DbError::from)?;
            Ok(target.map(|object| CandidateCleanupObject { object }))
        })
        .await
    }

    pub(crate) async fn discard_serial_branch_after_abandonment(
        &self,
        branch_id: PendingBranchId,
        plan: crate::sync::store_engine::SerialResolutionPlan,
    ) -> Result<(), DbError> {
        let prepared = self
            .prepared_serial_candidate_abandonment()
            .await?
            .ok_or_else(|| {
                DbError::Message("Serial candidate abandonment is not prepared".to_string())
            })?;
        if prepared.branch_id != branch_id {
            return Err(DbError::Message(
                "Serial abandonment belongs to another branch".to_string(),
            ));
        }
        let authority_ref = StoreBatchCommitRef::from_commit(
            &prepared.authority.value,
            StoreCommitCoord::Serial {
                sequence: prepared.authority.value.seq(),
            },
            prepared.authority.object.clone(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        let authority_accepted = plan
            .commits()
            .first()
            .is_some_and(|commit| commit.commit_ref == authority_ref);
        let expected_state = prepared.durable_state;
        let synced_tables = self.synced_tables().to_vec();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let write_ids =
                Self::apply_serial_resolution_on(&tx, &synced_tables, &branch_id, plan)?;
            let object_id = remote_object_id(&authority_ref.object);
            let remote = load_remote_object_on(&tx, object_id)?;
            if authority_accepted {
                let retained = remote.into_activated(&authority_ref).map_err(|error| {
                    DbError::Message(format!("activate Serial abandonment authority: {error}"))
                })?;
                update_remote_object_on(&tx, object_id, &retained)?;
            } else {
                if !remote
                    .candidate_cleanup_complete(&authority_ref)
                    .map_err(|error| {
                        DbError::Message(format!(
                            "validate Serial abandonment authority cleanup: {error}"
                        ))
                    })?
                {
                    return Err(DbError::Message(
                        "Serial abandonment authority cleanup is incomplete".to_string(),
                    ));
                }
                let removed = tx
                    .execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?;
                if removed != 1 {
                    return Err(DbError::Message(
                        "Serial abandonment authority disappeared during removal".to_string(),
                    ));
                }
            }
            let removed = tx
                .execute(
                    "DELETE FROM protocol_state WHERE key = ?1 AND value = ?2",
                    rusqlite::params![SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY, expected_state],
                )
                .map_err(DbError::from)?;
            if removed != 1 {
                return Err(DbError::Message(
                    "Serial abandonment durable state disappeared".to_string(),
                ));
            }
            let resolution = WriteResolution::Discarded;
            Self::resolve_unpublished_writes_on(&tx, &write_ids, &resolution)?;
            tx.commit().map_err(DbError::from)?;
            let status = WriteStatus::Resolved(resolution);
            for write_id in write_ids {
                Self::notify_write_status_in(&statuses, &write_id, status.clone());
            }
            Ok(())
        })
        .await
    }

    pub(crate) async fn remove_losing_serial_abandonment_authority(&self) -> Result<(), DbError> {
        let prepared = self
            .prepared_serial_candidate_abandonment()
            .await?
            .ok_or_else(|| {
                DbError::Message("Serial candidate abandonment is not prepared".to_string())
            })?;
        let authority_ref = StoreBatchCommitRef::from_commit(
            &prepared.authority.value,
            StoreCommitCoord::Serial {
                sequence: prepared.authority.value.seq(),
            },
            prepared.authority.object.clone(),
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        let expected_state = prepared.durable_state;
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let object_id = remote_object_id(&authority_ref.object);
            let remote = load_remote_object_on(&tx, object_id)?;
            if !remote
                .candidate_cleanup_complete(&authority_ref)
                .map_err(|error| {
                    DbError::Message(format!(
                        "validate losing Serial abandonment cleanup: {error}"
                    ))
                })?
            {
                return Err(DbError::Message(
                    "losing Serial abandonment cleanup is incomplete".to_string(),
                ));
            }
            let removed = tx
                .execute(
                    "DELETE FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                )
                .map_err(DbError::from)?;
            if removed != 1 {
                return Err(DbError::Message(
                    "losing Serial abandonment authority disappeared".to_string(),
                ));
            }
            let removed = tx
                .execute(
                    "DELETE FROM protocol_state WHERE key = ?1 AND value = ?2",
                    rusqlite::params![SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY, expected_state],
                )
                .map_err(DbError::from)?;
            if removed != 1 {
                return Err(DbError::Message(
                    "Serial abandonment durable state disappeared".to_string(),
                ));
            }
            tx.commit().map_err(DbError::from)
        })
        .await
    }

    pub(crate) async fn mark_candidate_cleanup_absent(
        &self,
        object: ExactObjectRef,
    ) -> Result<(), DbError> {
        self.call(move |conn| {
            let object_id = remote_object_id(&object);
            let mut remote = load_remote_object_on(conn, object_id)?;
            if remote.cleanup_target() != Some(&object) {
                return Err(DbError::Message(format!(
                    "remote object {object_id} is not awaiting exact cleanup"
                )));
            }
            remote.mark_absent_verified().map_err(|error| {
                DbError::Message(format!("mark candidate {object_id} absent: {error}"))
            })?;
            update_remote_object_on(conn, object_id, &remote)
        })
        .await
    }
}
