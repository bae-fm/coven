use super::{
    candidate_records::{
        author_exclusion_activation_for_candidate_on, begin_merge_candidate_nonactivation_on,
        parse_prepared_merge_candidate_on, parse_prepared_merge_candidate_parts_on,
        parse_prepared_merge_publication_on,
    },
    publication_state::{MergeAbandonmentOutcome, PreparedStoreWriteState},
    MergeMaterializationTransaction, StoreDatabase,
};
use crate::{
    candidate_graph_exact_objects, load_activated_registration_on,
    load_prepared_audience_objects_on, load_remote_object_on, required_store_root_authority_on,
    update_remote_object_on, BlobActivation, CloudOutboxRecords, CompletePreparedStoreWriteOutcome,
    Database, DbError, PreparedAudienceBlob, RetainedPackageApplication, LOCAL_DEVICE_ID_STATE_KEY,
};
use coven_protocol::remote_object::remote_object_id;
use coven_protocol::store_commit::{
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceHead, VerifiedStoreBatchCommit,
};
use coven_protocol::write::{PublishedPosition, WriteId, WriteResolution, WriteStatus};

impl StoreDatabase {
    pub async fn complete_prepared_store_write(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        accepted: StoreBatchCommitRef,
        nonactivations: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
    ) -> Result<CompletePreparedStoreWriteOutcome, DbError> {
        let nonactivations = nonactivations
            .into_iter()
            .map(|verified| {
                verified
                    .candidate_reference()
                    .map(|reference| (reference, verified.into_durable()))
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let gates = self.gates();
        let synced_tables = self.synced_tables().to_vec();
        let (outcome, notification) = self
            .connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                let local_device_id =
                    crate::required_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
                let prepared_count: i64 = tx
                    .query_row(
                        "SELECT COUNT(*) FROM store_writes WHERE prepared IS NOT NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if prepared_count != 1 {
                    return Err(DbError::Message(format!(
                        "Store publication expected one prepared write, found {prepared_count}"
                    )));
                }
                let (stored_write_id, raw_status, raw_prepared): (String, String, String) = tx
                    .query_row(
                        "SELECT write_id, status, prepared FROM store_writes
                     WHERE prepared IS NOT NULL ORDER BY ordinal LIMIT 1",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(DbError::from)?;
                let current_status: WriteStatus =
                    serde_json::from_str(&raw_status).map_err(|error| {
                        DbError::context("prepared Store write status", error)
                    })?;
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| DbError::context("prepared Store write", error))?;
                let exclusion_candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?;
                if author_exclusion_activation_for_candidate_on(
                    &tx,
                    &root,
                    &exclusion_candidate.reference,
                    &exclusion_candidate.commit.author_registration,
                )?
                .is_some()
                {
                    let device_id = exclusion_candidate.commit.author_registration.device_id;
                    let write_id = WriteId::from_generated(stored_write_id.clone());
                    if let WriteStatus::Resolved(WriteResolution::Retracted { witness }) =
                        &current_status
                    {
                        witness.validate().map_err(DbError::Message)?;
                        if witness.original_position().commit() != &exclusion_candidate.reference {
                            return Err(DbError::Message(
                                "terminal write retraction names another prepared candidate"
                                    .to_string(),
                            ));
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
                        let updated = tx
                            .execute(
                                "UPDATE store_writes SET prepared = NULL
                             WHERE write_id = ?1 AND status = ?2 AND prepared = ?3",
                                rusqlite::params![write_id.as_str(), &raw_status, &raw_prepared],
                            )
                            .map_err(DbError::from)?;
                        if updated != 1 {
                            return Err(DbError::Message(
                                "terminally retracted Store write changed during completion"
                                    .to_string(),
                            ));
                        }
                        tx.commit().map_err(DbError::from)?;
                        return Ok((
                            CompletePreparedStoreWriteOutcome::AuthorExcluded { device_id },
                            None,
                        ));
                    }
                    let status = WriteStatus::Blocked(coven_protocol::write::WriteBlock::InvalidProtocolState {
                        reason: format!(
                            "Store author {device_id} was excluded before candidate activation"
                        ),
                    });
                    Database::set_write_status_on(&tx, &write_id, &status)?;
                    tx.commit().map_err(DbError::from)?;
                    return Ok((
                        CompletePreparedStoreWriteOutcome::AuthorExcluded { device_id },
                        Some((write_id, status)),
                    ));
                }
                if let PreparedStoreWriteState::MergeAbandonment {
                    candidate_commit,
                    candidate_head,
                    authority_commit,
                    authority_head,
                    authority_history_summary,
                    ..
                } = &prepared
                {
                    let root = required_store_root_authority_on(&tx)?;
                    let candidate = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    candidate_commit.semantic_bytes(),
                    candidate_commit.prepared().reference(),
                    candidate_head.semantic_bytes(),
                    candidate_head.prepared().reference(),
                )?;
                    let authority = parse_prepared_merge_candidate_parts_on(
                    &tx,
                    authority_commit.semantic_bytes(),
                    authority_commit.prepared().reference(),
                    authority_head.semantic_bytes(),
                    authority_head.prepared().reference(),
                )?;
                    if authority.commit.write_id.as_str() != stored_write_id
                        || accepted != authority.reference
                        || !matches!(
                            &authority.commit.body,
                            coven_protocol::store_commit::StoreCommitBody::AbandonCandidates { .. }
                        )
                    {
                        return Err(DbError::Message(
                            "accepted Merge abandonment differs from its durable authority"
                                .to_string(),
                        ));
                    }
                    let registration = load_activated_registration_on(
                        &tx,
                        &root,
                        &authority.commit.author_registration,
                    )?;
                    StoreDeviceHead::parse_at(
                        &authority.head.to_bytes(),
                        root.store_root_hash,
                        &registration,
                        &accepted,
                    )
                    .map_err(|error| {
                        DbError::context("verify accepted Merge abandonment head", error)
                    })?;
                    for object in [
                        authority_commit.prepared().reference(),
                        authority_head.prepared().reference(),
                    ] {
                        let object_id = remote_object_id(object);
                        let remote = load_remote_object_on(&tx, object_id)?
                            .into_activated(&accepted)
                            .map_err(|error| {
                                DbError::context(format!("activate Merge abandonment object {object_id}"), error)
                            })?;
                        update_remote_object_on(&tx, object_id, &remote)?;
                    }
                    let nonactivation =
                        nonactivations.get(&candidate.reference).ok_or_else(|| {
                            DbError::Message(
                        "accepted Merge abandonment has no verified candidate nonactivation"
                            .to_string(),
                    )
                        })?;
                    begin_merge_candidate_nonactivation_on(
                        &tx,
                        &WriteId::from_generated(stored_write_id.clone()),
                        &candidate,
                        nonactivation,
                        true,
                        &[],
                    )?;
                    MergeMaterializationTransaction::new(&tx).record_materialized_merge_commit(
                        &root,
                        &authority.commit,
                        &[],
                        &authority.head,
                        &authority.head_object,
                        authority_history_summary,
                        &[],
                        None,
                    )?;
                    let mut completed_preparation = prepared.clone();
                    let PreparedStoreWriteState::MergeAbandonment { outcome, .. } =
                        &mut completed_preparation
                    else {
                        unreachable!("matched Merge abandonment")
                    };
                    *outcome = MergeAbandonmentOutcome::Accepted {
                        authority: accepted.clone(),
                    };
                    let completed_preparation = serde_json::to_string(&completed_preparation)
                        .map_err(|error| {
                            DbError::context("serialize accepted Merge abandonment", error)
                        })?;
                    let updated = tx
                        .execute(
                            "UPDATE store_writes SET prepared = ?2
                         WHERE write_id = ?1 AND prepared = ?3",
                            rusqlite::params![
                                stored_write_id.as_str(),
                                completed_preparation,
                                raw_prepared
                            ],
                        )
                        .map_err(DbError::from)?;
                    if updated != 1 {
                        return Err(DbError::Message(
                            "Merge abandonment changed during activation".to_string(),
                        ));
                    }
                    let blocked = WriteStatus::Blocked(coven_protocol::write::WriteBlock::InvalidProtocolState {
                        reason: format!(
                            "candidate abandonment {} is accepted; exact cleanup is pending",
                            authority.head.head_hash()
                        ),
                    });
                    let write_id = authority.commit.write_id.clone();
                    Database::set_write_status_on(&tx, &write_id, &blocked)?;
                    tx.commit().map_err(DbError::from)?;
                    return Ok((
                        CompletePreparedStoreWriteOutcome::Published,
                        Some((write_id, blocked)),
                    ));
                }
                let PreparedStoreWriteState::Publication {
                    commit,
                    head,
                    history_summary,
                    local_cleanup,
                    ..
                } = prepared
                else {
                    return Err(DbError::Message(
                        "Merge abandonment reached ordinary publication completion".to_string(),
                    ));
                };
                let root = required_store_root_authority_on(&tx)?;
                let unverified: StoreBatchCommit = serde_json::from_slice(commit.semantic_bytes())
                    .map_err(|error| DbError::context("prepared Store commit", error))?;
                let registration =
                    load_activated_registration_on(&tx, &root, &unverified.author_registration)?;
                let expected_stream =
                    coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                        root.store_root_hash,
                        &unverified.author_registration,
                        coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    );
                if accepted.coord.stream_id != expected_stream
                    || accepted.object != *commit.prepared().reference()
                {
                    return Err(DbError::Message(
                        "accepted Merge head differs from the exact prepared commit".to_string(),
                    ));
                }
                let commit_value = VerifiedStoreBatchCommit::parse(
                    commit.semantic_bytes(),
                    root.store_root_hash,
                    &accepted,
                    &registration,
                )
                .map_err(|error| DbError::context("outbound commit", error))?;
                let head_value = StoreDeviceHead::parse_at(
                    head.semantic_bytes(),
                    root.store_root_hash,
                    &registration,
                    &accepted,
                )
                .map_err(|error| DbError::context("outbound Store head", error))?;
                if commit_value.write_id.as_str() != stored_write_id {
                    return Err(DbError::Message(
                        "prepared write id differs from signed commit".to_string(),
                    ));
                }
                let write_id = commit_value.write_id.clone();
                let head_object_id = remote_object_id(head.prepared().reference());
                let commit = commit_value.value();
                let commit_ref = commit_value.reference();
                let remaining_spools: i64 = tx
                    .query_row(
                        "SELECT COUNT(*) FROM store_write_blobs
                         WHERE write_id = ?1 AND spool_path IS NOT NULL",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if remaining_spools != 0 {
                    return Err(DbError::Message(format!(
                        "prepared write {write_id} retains {remaining_spools} uploaded blob spool(s)"
                    )));
                }
                let audiences = load_prepared_audience_objects_on(&tx, &write_id)?;
                let retained_packages = audiences
                    .packages
                    .iter()
                    .map(|package| package.package().clone())
                    .collect::<Vec<_>>();
                for package in &audiences.packages {
                    package
                        .package()
                        .validate_blob_uploader(&commit.author_registration)
                        .map_err(|error| DbError::Message(error.to_string()))?;
                }
                let mut object_ids = std::collections::BTreeSet::new();
                object_ids.insert(remote_object_id(&commit_ref.object));
                object_ids.extend(
                    candidate_graph_exact_objects(commit)?
                        .iter()
                        .map(remote_object_id),
                );
                object_ids.extend(
                    audiences
                        .blobs
                        .iter()
                        .map(PreparedAudienceBlob::remote_object_id),
                );
                object_ids.insert(head_object_id);
                for object_id in object_ids {
                    let remote = load_remote_object_on(&tx, object_id)?
                        .into_activated(commit_ref)
                        .map_err(|error| {
                            DbError::context(format!("activate remote object {object_id}"), error)
                        })?;
                    let state = serde_json::to_string(&remote).map_err(|error| {
                        DbError::context("serialize activated remote object", error)
                    })?;
                    let updated = tx
                        .execute(
                            "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                            (object_id.to_string(), state),
                        )
                        .map_err(DbError::from)?;
                    if updated != 1 {
                        return Err(DbError::Message(format!(
                            "remote object {object_id} disappeared during activation"
                        )));
                    }
                }
                let activation = BlobActivation {
                    coord: commit_ref.coord.clone(),
                };
                let apply_schema =
                    crate::TableSchema::for_apply(&tx, &synced_tables, &gates)?;
                let store_transaction = MergeMaterializationTransaction::new(&tx);
                let cloud_outbox = CloudOutboxRecords::new(&tx);
                let mut consumed_uploads = 0;
                for package in &audiences.packages {
                    let winning_rows = store_transaction
                        .current_winning_rows(&apply_schema, package.package().changeset())?;
                    store_transaction.install_winning_blob_bindings(
                        &gates,
                        &synced_tables,
                        package.package(),
                        &activation,
                        &winning_rows,
                    )?;
                    for binding in package.package().blob_bindings() {
                        if cloud_outbox.consume_created_upload_handoff(package.package(), binding)? {
                            consumed_uploads += 1;
                        }
                    }
                }
                match Database::make_remote_publication_root_on(&tx, &write_id)? {
                    Some((root_table, root_id)) => {
                        if consumed_uploads == 0 {
                            return Err(DbError::Message(format!(
                                "make_remote publication {write_id} for {root_table:?}/{root_id:?} contains no Created upload handoff"
                            )));
                        }
                        let remaining: i64 = tx
                            .query_row(
                                "SELECT COUNT(*) FROM cloud_outbox
                                 WHERE operation = 'upload' AND root_table = ?1 AND root_id = ?2",
                                (&root_table, &root_id),
                                |row| row.get(0),
                            )
                            .map_err(DbError::from)?;
                        if remaining != 0 {
                            return Err(DbError::Message(format!(
                                "make_remote publication {write_id} left {remaining} upload handoff(s) for {root_table:?}/{root_id:?}"
                            )));
                        }
                        Database::complete_make_remote_publication_on(&tx, &write_id)?;
                    }
                    None if consumed_uploads != 0 => {
                        return Err(DbError::Message(format!(
                            "Store write {write_id} consumed Created upload handoffs without a make_remote publication intent"
                        )));
                    }
                    None => {}
                }
                for drop in local_cleanup.drops {
                    tx.execute(
                        "INSERT INTO published_blob_drop_intents
                         (seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                         ON CONFLICT(seq, namespace, blob_id, locator_hash) DO NOTHING",
                        rusqlite::params![
                            Database::sequence_to_sqlite(
                                &commit_ref.coord.stream_id.to_string(),
                                commit_ref.coord.sequence(),
                            )?,
                            drop.namespace,
                            drop.id,
                            i64::try_from(drop.size).map_err(|_| DbError::Message(
                                "outbound local cleanup size exceeds SQLite integer".to_string()
                            ))?,
                            drop.plaintext_hash.to_string(),
                            drop.locator_hash.to_string(),
                            drop.disposition.as_db(),
                        ],
                    )
                    .map_err(DbError::from)?;
                }
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
                tx.execute(
                    "DELETE FROM store_write_blob_leases WHERE write_id = ?1",
                    [write_id.as_str()],
                )
                .map_err(DbError::from)?;
                MergeMaterializationTransaction::new(&tx).record_materialized_merge_commit(
                    &root,
                    &commit_value,
                    &[],
                    &head_value,
                    head.prepared().reference(),
                    &history_summary,
                    &retained_packages,
                    (!retained_packages.is_empty())
                        .then_some(RetainedPackageApplication::LocallyAuthored),
                )?;
                let cleared = tx
                    .execute(
                        "UPDATE store_writes SET prepared = NULL
                     WHERE write_id = ?1 AND prepared IS NOT NULL",
                        [stored_write_id.as_str()],
                    )
                    .map_err(DbError::from)?;
                if cleared != 1 {
                    return Err(DbError::Message(
                        "prepared Store write disappeared".to_string(),
                    ));
                }
                let status = WriteStatus::Published(Box::new(PublishedPosition {
                    device_id: local_device_id,
                    commit: accepted.clone(),
                }));
                Database::set_write_status_on(&tx, &write_id, &status)?;
                tx.commit().map_err(DbError::from)?;
                Ok((
                    CompletePreparedStoreWriteOutcome::Published,
                    Some((write_id, status)),
                ))
            })
            .await?;
        if let Some((write_id, status)) = notification {
            self.notify_write_status(write_id, status);
        }
        Ok(outcome)
    }

    pub async fn mark_merge_candidate_conflict(
        &self,
        write_id: WriteId,
        nonactivations: Vec<coven_protocol::remote_object::VerifiedCandidateNonactivation>,
    ) -> Result<(), DbError> {
        let first = nonactivations.first().ok_or_else(|| {
            DbError::Message("Merge candidate conflict has no verified candidates".to_string())
        })?;
        let winner_commit = first
            .merge_winner_commit()
            .cloned()
            .map_err(|error| DbError::Message(error.to_string()))?;
        let winner_head = match first.proof() {
            coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                winner_head,
            } => winner_head.clone(),
            coven_protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. } => {
                return Err(DbError::Message(
                    "Merge slot conflict cannot carry author-exclusion evidence".to_string(),
                ));
            }
            coven_protocol::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation { .. } => {
                return Err(DbError::Message(
                    "Merge slot conflict cannot carry membership-grant revocation evidence"
                        .to_string(),
                ));
            }
            coven_protocol::remote_object::CandidateNonactivationProof::MergeDependencyRetraction { .. } => {
                return Err(DbError::Message(
                    "Merge slot conflict cannot carry dependent-retraction evidence".to_string(),
                ));
            }
        };
        let winner_proof = first.proof().clone();
        let nonactivations = nonactivations
            .into_iter()
            .map(|verified| {
                if verified
                    .merge_winner_commit()
                    .map_err(|error| DbError::Message(error.to_string()))?
                    != &winner_commit
                    || verified.proof() != &winner_proof
                {
                    return Err(DbError::Message(
                        "Merge candidate conflict observations name different winners".to_string(),
                    ));
                }
                verified
                    .candidate_reference()
                    .map(|reference| (reference, verified.into_durable()))
                    .map_err(|error| DbError::Message(error.to_string()))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let notified_write_id = write_id.clone();
        let blocked = self
            .connection
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
                    .map_err(|error| DbError::context("Merge candidate status", error))?;
                if !matches!(status, WriteStatus::Publishing) {
                    return Err(DbError::Message(format!(
                        "Merge candidate {write_id} is not publishing"
                    )));
                }
                let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
                    .map_err(|error| DbError::context("prepared Merge candidate", error))?;
                let prepared_candidate = parse_prepared_merge_candidate_on(&tx, &prepared)?;
                let publication = parse_prepared_merge_publication_on(&tx, &prepared)?;
                if winner_head.object.slot() != publication.head_object.slot()
                    || winner_head.object == publication.head_object
                {
                    return Err(DbError::Message(
                        "Merge winner does not replace the prepared exact head slot".to_string(),
                    ));
                }
                if prepared_candidate.commit.write_id != write_id {
                    return Err(DbError::Message(
                        "prepared Merge graph differs from its write identity".to_string(),
                    ));
                }
                if matches!(&prepared, PreparedStoreWriteState::MergeAbandonment { .. }) {
                    let publication_nonactivation =
                        nonactivations.get(&publication.reference).ok_or_else(|| {
                            DbError::Message(
                                "Merge abandonment authority has no verified nonactivation"
                                    .to_string(),
                            )
                        })?;
                    begin_merge_candidate_nonactivation_on(
                        &tx,
                        &write_id,
                        &publication,
                        publication_nonactivation,
                        false,
                        &[],
                    )?;
                    if winner_commit != prepared_candidate.reference {
                        let candidate_nonactivation = nonactivations
                            .get(&prepared_candidate.reference)
                            .ok_or_else(|| {
                                DbError::Message(
                                    "Merge abandonment candidate has no verified nonactivation"
                                        .to_string(),
                                )
                            })?;
                        begin_merge_candidate_nonactivation_on(
                            &tx,
                            &write_id,
                            &prepared_candidate,
                            candidate_nonactivation,
                            true,
                            &[],
                        )?;
                    }
                    let mut lost_preparation = prepared.clone();
                    let PreparedStoreWriteState::MergeAbandonment { outcome, .. } =
                        &mut lost_preparation
                    else {
                        unreachable!("matched Merge abandonment")
                    };
                    *outcome = MergeAbandonmentOutcome::Lost {
                        winner_commit: winner_commit.clone(),
                        winner_head: winner_head.clone(),
                    };
                    let lost_preparation =
                        serde_json::to_string(&lost_preparation).map_err(|error| {
                            DbError::context("serialize lost Merge abandonment", error)
                        })?;
                    let updated = tx
                        .execute(
                            "UPDATE store_writes SET prepared = ?2
                         WHERE write_id = ?1 AND prepared = ?3",
                            rusqlite::params![write_id.as_str(), lost_preparation, raw_prepared],
                        )
                        .map_err(DbError::from)?;
                    if updated != 1 {
                        return Err(DbError::Message(
                            "Merge abandonment changed while recording its winner".to_string(),
                        ));
                    }
                } else {
                    let candidate_nonactivation = nonactivations
                        .get(&prepared_candidate.reference)
                        .ok_or_else(|| {
                            DbError::Message(
                                "Merge candidate has no verified nonactivation".to_string(),
                            )
                        })?;
                    begin_merge_candidate_nonactivation_on(
                        &tx,
                        &write_id,
                        &prepared_candidate,
                        candidate_nonactivation,
                        true,
                        &[],
                    )?;
                }
                let blocked =
                    WriteStatus::Blocked(coven_protocol::write::WriteBlock::InvalidProtocolState {
                        reason: format!(
                            "Merge successor slot is occupied by signed head {}",
                            winner_head.head_hash
                        ),
                    });
                Database::set_write_status_on(&tx, &write_id, &blocked)?;
                tx.commit().map_err(DbError::from)?;
                Ok(blocked)
            })
            .await?;
        self.notify_write_status(notified_write_id, blocked);
        Ok(())
    }
}
