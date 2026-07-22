use crate::database::store_device_state::store_serial_predecessor_on;

use crate::database::blob_records::load_activated_registration_on;
use crate::database::remote_object_records::candidate_graph_exact_objects;
use crate::database::remote_object_records::load_remote_object_on;

use super::*;

impl Database {
    pub(crate) async fn exact_serial_predecessor(
        &self,
        commit: Option<StoreBatchCommitRef>,
    ) -> Result<StoreSerialPredecessor, DbError> {
        self.call(move |conn| store_serial_predecessor_on(conn, commit.as_ref()))
            .await
    }

    pub(crate) async fn complete_prepared_serial_branch(
        &self,
        accepted: VersionedObject,
    ) -> Result<u64, DbError> {
        let statuses = self.state.write_statuses.clone();
        let gates = self.state.gates.clone();
        let synced_tables = self.state.synced_tables.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let root = required_store_root_authority_on(&tx)?;
            let accepted_unverified: StoreSerialHead = serde_json::from_slice(&accepted.bytes)
                .map_err(|error| DbError::Message(format!("accepted Serial head: {error}")))?;
            let accepted_author_ref = match &accepted_unverified.state {
                StoreSerialHeadState::Commit {
                    author_registration,
                    ..
                } => author_registration,
                StoreSerialHeadState::Genesis { .. } => {
                    return Err(DbError::Message(
                        "prepared Serial branch cannot complete at a genesis head".to_string(),
                    ));
                }
            };
            let accepted_author = load_activated_registration_on(&tx, &root, accepted_author_ref)?;
            let accepted_head =
                StoreSerialHead::parse(&accepted.bytes, root.store_root_hash, &accepted_author)
                    .map_err(|error| {
                        DbError::Message(format!("verify accepted Serial head: {error}"))
                    })?;
            let accepted_tip = match &accepted_head.state {
                StoreSerialHeadState::Commit { commit, .. } => commit.clone(),
                StoreSerialHeadState::Genesis { .. } => unreachable!("rejected above"),
            };
            let mut statement = tx
                .prepare(
                    "SELECT write_id, prepared, base FROM store_writes
                     WHERE prepared IS NOT NULL AND status = '\"publishing\"'
                     ORDER BY ordinal",
                )
                .map_err(DbError::from)?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(DbError::from)?;
            drop(statement);
            let mut completed = Vec::new();
            let mut completed_base = None;
            let mut predecessor = None;
            let mut prepared_tip_head = None;
            for row in rows {
                let (stored_write_id, prepared, raw_base) = row;
                let stored_base: StoreWriteBase = serde_json::from_str(&raw_base)
                    .map_err(|error| DbError::Message(format!("prepared Serial base: {error}")))?;
                match &completed_base {
                    Some(expected) if expected != &stored_base => {
                        return Err(DbError::Message(
                            "prepared Serial branch contains inconsistent bases".to_string(),
                        ));
                    }
                    None => completed_base = Some(stored_base.clone()),
                    Some(_) => {}
                }
                let PreparedStoreWriteState::Serial {
                    commit,
                    tip_head_bytes,
                    local_cleanup,
                    ..
                } = serde_json::from_str(&prepared)
                    .map_err(|error| DbError::Message(format!("prepared Serial write: {error}")))?
                else {
                    return Err(DbError::Message(
                        "non-Serial write reached Serial completion".to_string(),
                    ));
                };
                let unverified: StoreBatchCommit = serde_json::from_slice(&commit.semantic_bytes)
                    .map_err(|error| {
                    DbError::Message(format!("prepared Serial commit: {error}"))
                })?;
                if predecessor.is_none() {
                    let StoreWriteBase::Serial { base, .. } = &stored_base else {
                        return Err(DbError::Message(
                            "Merge base reached Serial completion".to_string(),
                        ));
                    };
                    predecessor = base.clone();
                }
                let sequence = predecessor
                    .as_ref()
                    .map_or(1, |reference: &StoreBatchCommitRef| {
                        reference.coord.sequence().saturating_add(1)
                    });
                let coord = StoreCommitCoord::Serial { sequence };
                let registration =
                    load_activated_registration_on(&tx, &root, &unverified.author_registration)?;
                let commit_value = StoreBatchCommit::parse_at(
                    &commit.semantic_bytes,
                    root.store_root_hash,
                    &coord,
                    &registration,
                )
                .map_err(|error| DbError::Message(format!("outbound Serial commit: {error}")))?;
                let commit_ref = StoreBatchCommitRef::from_commit(
                    &commit_value,
                    coord,
                    commit.prepared.reference().clone(),
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                let order_matches = match (&predecessor, &commit_value.order) {
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
                            && founder_registration == &commit_value.author_registration
                    }
                    _ => false,
                };
                if !order_matches {
                    return Err(DbError::Message(
                        "prepared Serial completion chain has a different exact predecessor"
                            .to_string(),
                    ));
                }
                if commit_value.write_id.as_str() != stored_write_id {
                    return Err(DbError::Message(
                        "prepared Serial write id differs from signed commit".to_string(),
                    ));
                }
                let write_id = commit_value.write_id.clone();
                Self::activate_prepared_write_on(
                    &tx,
                    &root,
                    &gates,
                    &synced_tables,
                    &write_id,
                    &commit_value,
                    &commit_ref,
                    PreparedWriteMaterialization::Serial,
                    local_cleanup,
                    &[],
                )?;
                let cleared = tx
                    .execute(
                        "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                        [write_id.as_str()],
                    )
                    .map_err(DbError::from)?;
                if cleared != 1 {
                    return Err(DbError::Message(format!(
                        "prepared Serial write {write_id} disappeared during completion"
                    )));
                }
                let status = WriteStatus::Published(Box::new(PublishedPosition::Serial {
                    commit: commit_ref.clone(),
                }));
                Self::set_write_status_on(&tx, &write_id, &status)?;
                if let Some(bytes) = tip_head_bytes {
                    if prepared_tip_head.replace(bytes).is_some() {
                        return Err(DbError::Message(
                            "prepared Serial branch has multiple tip heads".to_string(),
                        ));
                    }
                }
                predecessor = Some(commit_ref.clone());
                completed.push((write_id, status, commit_ref));
            }
            let Some((_, _, final_ref)) = completed.last() else {
                return Err(DbError::Message(
                    "prepared Serial branch is absent".to_string(),
                ));
            };
            if final_ref != &accepted_tip
                || prepared_tip_head.as_deref() != Some(accepted.bytes.as_slice())
            {
                return Err(DbError::Message(
                    "accepted Serial head differs from the exact prepared branch tip".to_string(),
                ));
            }
            tx.execute(
                "INSERT INTO serial_head_receipt \
                 (singleton, head_bytes, version_token, commit_ref) VALUES (1, ?1, ?2, ?3) \
                 ON CONFLICT(singleton) DO UPDATE SET \
                   head_bytes = excluded.head_bytes, \
                   version_token = excluded.version_token, \
                   commit_ref = excluded.commit_ref",
                (
                    &accepted.bytes,
                    serde_json::to_string(&accepted.version).map_err(|error| {
                        DbError::Message(format!("serialize Serial version receipt: {error}"))
                    })?,
                    serde_json::to_string(&accepted_tip).map_err(|error| {
                        DbError::Message(format!("serialize accepted Serial commit ref: {error}"))
                    })?,
                ),
            )
            .map_err(DbError::from)?;
            let completed_base = completed_base.ok_or_else(|| {
                DbError::Message("prepared Serial branch base is absent".to_string())
            })?;
            let suffix_first: Option<String> = tx
                .query_row(
                    "SELECT write_id FROM store_writes
                     WHERE status = '\"pending\"' AND prepared IS NULL AND base = ?1
                     ORDER BY ordinal LIMIT 1",
                    [serde_json::to_string(&completed_base).map_err(|error| {
                        DbError::Message(format!("serialize Serial base: {error}"))
                    })?],
                    |row| row.get(0),
                )
                .optional()
                .map_err(DbError::from)?;
            if let Some(suffix_first) = suffix_first {
                let rebased = StoreWriteBase::Serial {
                    branch_id: PendingBranchId::from_first_write(WriteId::from_generated(
                        suffix_first,
                    )),
                    base: Some(accepted_tip.clone()),
                };
                tx.execute(
                    "UPDATE store_writes SET base = ?2
                     WHERE status = '\"pending\"' AND prepared IS NULL AND base = ?1",
                    rusqlite::params![
                        serde_json::to_string(&completed_base).map_err(
                            |error| DbError::Message(format!(
                                "serialize completed Serial base: {error}"
                            ))
                        )?,
                        serde_json::to_string(&rebased).map_err(|error| DbError::Message(
                            format!("serialize rebased Serial suffix: {error}")
                        ))?,
                    ],
                )
                .map_err(DbError::from)?;
            }
            let count = u64::try_from(completed.len())
                .map_err(|_| DbError::Message("Serial completion count exceeds u64".to_string()))?;
            tx.commit().map_err(DbError::from)?;
            for (write_id, status, _) in completed {
                Self::notify_write_status_in(&statuses, &write_id, status);
            }
            Ok(count)
        })
        .await
    }

    fn validate_serial_candidate_cleanup_on(
        conn: &Connection,
        write_id: &WriteId,
        raw_prepared: Option<&str>,
    ) -> Result<(), DbError> {
        let Some(raw_prepared) = raw_prepared else {
            return Ok(());
        };
        let Some(candidate) = parse_prepared_serial_candidate(raw_prepared)? else {
            return Ok(());
        };
        let mut object_ids = vec![remote_object_id(&candidate.reference.object)];
        object_ids.extend(
            candidate_graph_exact_objects(&candidate.commit)?
                .iter()
                .map(remote_object_id),
        );
        let mut statement = conn
            .prepare("SELECT remote_object_id FROM store_write_blobs WHERE write_id = ?1")
            .map_err(DbError::from)?;
        let indexed = statement
            .query_map([write_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        drop(statement);
        for encoded in indexed {
            object_ids.push(encoded.parse().map_err(|error| {
                DbError::Message(format!("Serial cleanup remote object id: {error}"))
            })?);
        }
        for object_id in object_ids {
            let remote = load_remote_object_on(conn, object_id)?;
            if !remote
                .candidate_cleanup_complete(&candidate.reference)
                .map_err(|error| {
                    DbError::Message(format!(
                        "validate candidate cleanup for {object_id}: {error}"
                    ))
                })?
            {
                return Err(DbError::Message(format!(
                    "candidate cleanup for remote object {object_id} is incomplete"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn apply_serial_resolution_on(
        tx: &rusqlite::Transaction<'_>,
        synced_tables: &[SyncedTable],
        branch_id: &PendingBranchId,
        plan: crate::sync::store_pull::SerialResolutionPlan,
    ) -> Result<Vec<WriteId>, DbError> {
        let (head, _head_object, commits) = plan.into_parts();
        let schema = Arc::new(crate::sync::conflict::TableSchema::from_db(
            tx,
            synced_tables,
        )?);
        let gates = Gates::from_tables(tx, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let blob_decls = BlobDecls::from_tables(tx, synced_tables)
            .map_err(|error| DbError::Message(error.to_string()))?;
        let mut statement = tx
            .prepare(
                "SELECT write_id, status, inverse_changeset, base, prepared FROM store_writes
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
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(DbError::from)?;
        let mut branch = Vec::new();
        let mut branch_base = None;
        let mut saw_conflict = false;
        for row in rows {
            let (write_id, status, inverse, raw_base, prepared) = row.map_err(DbError::from)?;
            let status: WriteStatus = serde_json::from_str(&status)
                .map_err(|error| DbError::Message(format!("Serial branch status: {error}")))?;
            let base: StoreWriteBase = serde_json::from_str(&raw_base)
                .map_err(|error| DbError::Message(format!("Serial branch base: {error}")))?;
            let StoreWriteBase::Serial {
                branch_id: stored_branch_id,
                base,
            } = base
            else {
                return Err(DbError::Message(
                    "MergeConcurrent base reached Serial resolution".to_string(),
                ));
            };
            if &stored_branch_id != branch_id {
                return Err(DbError::Message(
                    "Serial database contains more than one unresolved branch".to_string(),
                ));
            }
            match status {
                WriteStatus::Conflict(conflict) => {
                    if conflict.branch_id != stored_branch_id
                        || conflict.base != store_serial_predecessor_on(tx, base.as_ref())?
                    {
                        return Err(DbError::Message(
                            "Serial conflict status differs from its durable branch base"
                                .to_string(),
                        ));
                    }
                    saw_conflict = true;
                }
                WriteStatus::Pending if prepared.is_none() => {}
                status => {
                    return Err(DbError::Message(format!(
                        "conflicted Serial branch write {write_id} has non-resolvable status {status:?}"
                    )))
                }
            }
            Self::validate_serial_candidate_cleanup_on(
                tx,
                &WriteId::from_generated(write_id.clone()),
                prepared.as_deref(),
            )?;
            if branch_base.as_ref().is_some_and(|stored| stored != &base) {
                return Err(DbError::Message(
                    "Serial conflict branch has inconsistent bases".to_string(),
                ));
            }
            branch_base.get_or_insert(base);
            branch.push((WriteId::from_generated(write_id), inverse));
        }
        drop(statement);
        if branch.is_empty() || !saw_conflict {
            return Err(DbError::Message(format!(
                "Serial branch {} is not conflicted",
                branch_id.first_write_id()
            )));
        }
        let branch_base = branch_base.expect("nonempty branch has a base value");
        let durable_base = Self::latest_position_for_device_on(tx, SERIAL_STREAM_ID)?;
        if durable_base != branch_base {
            return Err(DbError::Message(format!(
                "local Serial position {durable_base:?} differs from branch base {branch_base:?}"
            )));
        }
        for (_, inverse) in branch.iter().rev() {
            let inverse = crate::sync::apply::ValidatedChangeset::new(inverse, schema.clone())
                .map_err(|error| DbError::Message(format!("invalid Serial inverse: {error}")))?;
            crate::sync::apply::apply_changeset_strict_on(tx, inverse)
                .map_err(|error| DbError::Message(format!("reverse Serial branch: {error}")))?;
        }
        let mut predecessor = branch_base;
        for resolution in commits {
            let expected_seq = match predecessor.as_ref() {
                Some(reference) => reference.coord.sequence().checked_add(1).ok_or_else(|| {
                    DbError::Message("Serial resolution sequence overflow".to_string())
                })?,
                None => 1,
            };
            if resolution.commit.seq() != expected_seq
                || resolution.commit_ref.coord
                    != (StoreCommitCoord::Serial {
                        sequence: expected_seq,
                    })
                || resolution.commit.order.predecessor() != predecessor.as_ref()
            {
                return Err(DbError::Message(format!(
                    "Serial resolution commit {} does not follow the branch base",
                    resolution.commit.seq()
                )));
            }
            crate::sync::store_pull::apply_prepared_serial_commit_on(
                tx,
                schema.clone(),
                &gates,
                synced_tables,
                &blob_decls,
                &resolution,
            )?;
            predecessor = Some(resolution.commit_ref);
        }
        let head_commit = match head.state {
            StoreSerialHeadState::Commit { commit, .. } => Some(commit),
            StoreSerialHeadState::Genesis { .. } => None,
        };
        if predecessor != head_commit {
            return Err(DbError::Message(
                "Serial resolution commits do not reach the verified global head".to_string(),
            ));
        }
        Ok(branch.into_iter().map(|(write_id, _)| write_id).collect())
    }

    pub(super) fn resolve_unpublished_writes_on(
        tx: &rusqlite::Transaction<'_>,
        write_ids: &[WriteId],
        resolution: &WriteResolution,
    ) -> Result<(), DbError> {
        let status = WriteStatus::Resolved(resolution.clone());
        for write_id in write_ids {
            let raw_prepared: Option<String> = tx
                .query_row(
                    "SELECT prepared FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(DbError::from)?;
            let mut removable = Vec::new();
            let mut candidate = None;
            if let Some(raw_prepared) = raw_prepared.as_deref() {
                let prepared: PreparedStoreWriteState = serde_json::from_str(raw_prepared)
                    .map_err(|error| {
                        DbError::Message(format!("resolved prepared write: {error}"))
                    })?;
                match &prepared {
                    PreparedStoreWriteState::MergeConcurrent { .. }
                    | PreparedStoreWriteState::MergeAbandonment { .. } => {
                        let merge = parse_prepared_merge_candidate_on(tx, &prepared)?
                            .expect("matched Merge preparation");
                        removable.push(remote_object_id(&merge.reference.object));
                        match load_merge_candidate_head_cleanup_on(
                            tx,
                            merge.head_prepared.reference(),
                            &merge.reference,
                        )? {
                            MergeCandidateHeadCleanup::Remote { .. } => {
                                removable.push(remote_object_id(merge.head_prepared.reference()))
                            }
                            MergeCandidateHeadCleanup::ProtocolInert => {}
                        }
                        removable.extend(
                            candidate_graph_exact_objects(&merge.commit)?
                                .iter()
                                .map(remote_object_id),
                        );
                        candidate = Some(merge.reference);
                    }
                    PreparedStoreWriteState::Serial { .. } => {
                        if let Some(serial) = parse_prepared_serial_candidate(raw_prepared)? {
                            removable.push(remote_object_id(&serial.reference.object));
                            removable.extend(
                                candidate_graph_exact_objects(&serial.commit)?
                                    .iter()
                                    .map(remote_object_id),
                            );
                            candidate = Some(serial.reference);
                        }
                    }
                    PreparedStoreWriteState::SerialPreparing => {}
                }
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
                removable.push(encoded.parse().map_err(|error| {
                    DbError::Message(format!("resolved remote object id: {error}"))
                })?);
            }
            if let Some(candidate) = &candidate {
                for object_id in &removable {
                    let remote = load_remote_object_on(tx, *object_id)?;
                    if !remote
                        .candidate_cleanup_complete(candidate)
                        .map_err(|error| {
                            DbError::Message(format!(
                                "validate candidate cleanup for {object_id}: {error}"
                            ))
                        })?
                    {
                        return Err(DbError::Message(format!(
                            "candidate cleanup for remote object {object_id} is incomplete"
                        )));
                    }
                }
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
            for object_id in removable {
                let remote = load_remote_object_on(tx, object_id)?;
                let absent = matches!(
                    remote,
                    RemoteObjectRecord::CandidateCommit(
                        crate::sync::remote_object::CandidateCommitRecord {
                            state:
                                crate::sync::remote_object::CandidateCommitState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::CandidateExclusive(
                        crate::sync::remote_object::CandidateObjectRecord {
                            state:
                                crate::sync::remote_object::CandidateObjectState::AbsentVerified { .. },
                            ..
                        }
                    ) | RemoteObjectRecord::RetainedAuthority(
                        crate::sync::remote_object::RetainedAuthorityRecord {
                            state:
                                crate::sync::remote_object::RetainedAuthorityObjectState::UncreatedVerified { .. },
                            ..
                        }
                    )
                );
                if absent {
                    tx.execute(
                        "DELETE FROM remote_objects WHERE object_id = ?1",
                        [object_id.to_string()],
                    )
                    .map_err(DbError::from)?;
                }
            }
            tx.execute(
                "UPDATE store_writes SET prepared = NULL WHERE write_id = ?1",
                [write_id.as_str()],
            )
            .map_err(DbError::from)?;
            Self::set_write_status_on(tx, write_id, &status)?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub async fn discard_pending_serial_branch(
        &self,
        branch_id: PendingBranchId,
        plan: crate::sync::store_pull::SerialResolutionPlan,
    ) -> Result<(), DbError> {
        let synced_tables = self.synced_tables().to_vec();
        let statuses = self.state.write_statuses.clone();
        self.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let write_ids =
                Self::apply_serial_resolution_on(&tx, &synced_tables, &branch_id, plan)?;
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

    #[doc(hidden)]
    pub async fn replace_pending_serial_branch<R, E, F>(
        &self,
        branch_id: PendingBranchId,
        plan: crate::sync::store_pull::SerialResolutionPlan,
        replacement_write_id: WriteId,
        f: F,
    ) -> Result<WriteReceipt<R>, E>
    where
        R: Send + 'static,
        E: From<DbError> + Send + 'static,
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<R, E> + Send + 'static,
    {
        let synced_tables = self.synced_tables().to_vec();
        let gates = self.gates();
        let blob_decls = self.blob_decls();
        let statuses = self.state.write_statuses.clone();
        let outcome = self
            .call(move |conn| {
                Ok((|| {
                    let tx = conn
                        .unchecked_transaction()
                        .map_err(DbError::from)
                        .map_err(E::from)?;
                    let old_write_ids =
                        Self::apply_serial_resolution_on(&tx, &synced_tables, &branch_id, plan)
                            .map_err(E::from)?;
                    let changes_before = tx.total_changes();
                    let (value, captured) =
                        Self::capture_host_changes_on(&tx, &synced_tables, || f(&tx))?;
                    let partitions = Self::partition_captured_write_on(
                        &tx,
                        &captured,
                        &gates,
                        WritePolicy::Serial,
                        StoreWriteRouting::SerialScoped,
                    )
                    .map_err(E::from)?;
                    let blob_facts =
                        Self::capture_partition_blob_facts_on(&tx, &partitions, &blob_decls)
                            .map_err(E::from)?;
                    let rows_changed = tx.total_changes().saturating_sub(changes_before);
                    let inverse_changeset = Self::invert_changeset(&captured).map_err(E::from)?;
                    let base = StoreWriteBase::Serial {
                        branch_id: PendingBranchId::from_first_write(replacement_write_id.clone()),
                        base: Self::latest_position_for_device_on(&tx, SERIAL_STREAM_ID)
                            .map_err(E::from)?,
                    };
                    let status = Self::insert_store_write_on(
                        &tx,
                        &replacement_write_id,
                        &partitions,
                        &inverse_changeset,
                        &base,
                        &blob_facts,
                        rows_changed,
                    )
                    .map_err(E::from)?;
                    let pending_branch_id = (!matches!(&status, WriteStatus::LocalOnly))
                        .then(|| PendingBranchId::from_first_write(replacement_write_id.clone()));
                    let resolution = WriteResolution::Replaced {
                        replacement: replacement_write_id.clone(),
                    };
                    Self::resolve_unpublished_writes_on(&tx, &old_write_ids, &resolution)
                        .map_err(E::from)?;
                    tx.commit().map_err(DbError::from).map_err(E::from)?;
                    let old_status = WriteStatus::Resolved(resolution);
                    for write_id in old_write_ids {
                        Self::notify_write_status_in(&statuses, &write_id, old_status.clone());
                    }
                    Ok(WriteReceipt {
                        value,
                        write_id: replacement_write_id,
                        status,
                        pending_branch_id,
                    })
                })())
            })
            .await
            .map_err(E::from)?;
        outcome
    }
}
