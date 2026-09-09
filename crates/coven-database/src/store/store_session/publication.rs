use super::{
    publication_state::PreparedStoreWriteState, MergeMaterializationTransaction, StoreDatabase,
    StoreSession, StoreTransactionOutcome, VerifiedStoreTransaction,
};
use crate::{
    candidate_graph_exact_objects, load_prepared_audience_objects_on, load_remote_object_on,
    CloudOutboxRecords, Database, DbError, OwnedVerifiedMergeMaterialization, PreparedAudienceBlob,
    RetainedPackageApplication, LOCAL_DEVICE_ID_STATE_KEY,
};
use coven_protocol::remote_object::remote_object_id;
use coven_protocol::store_commit::{StoreBatchCommit, VerifiedStoreBatchCommit};
use coven_protocol::write::{PublishedPosition, PublishedWrite, WriteId, WriteStatus};

impl VerifiedStoreTransaction<'_, '_, '_, '_> {
    fn complete_prepared_store_write(
        &mut self,
        accepted_publication: crate::StoreCommitPublicationOutcome,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    ) -> Result<
        (
            Option<OwnedVerifiedMergeMaterialization>,
            (WriteId, WriteStatus),
        ),
        DbError,
    > {
        let state = &mut *self.authority;
        let gates = self.gates;
        let synced_tables = self.synced_tables;
        let store_transaction = self.store;
        let tx = store_transaction.transaction;
        let local_device_id = crate::required_protocol_state_on(tx, LOCAL_DEVICE_ID_STATE_KEY)?;
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
        let current_status: WriteStatus = serde_json::from_str(&raw_status)
            .map_err(|error| DbError::context("prepared Store write status", error))?;
        if current_status != WriteStatus::Publishing {
            return Err(DbError::Message(format!(
                "prepared Store write has non-publishing status {current_status:?}"
            )));
        }
        let prepared: PreparedStoreWriteState = serde_json::from_str(&raw_prepared)
            .map_err(|error| DbError::context("prepared Store write", error))?;
        let PreparedStoreWriteState {
            commit,
            history_evidence,
            local_cleanup,
            ..
        } = prepared;
        let active = super::active_store_publication::load_active_store_publication_on(tx)?
            .ok_or_else(|| {
                DbError::Message(format!(
                    "publishing write {stored_write_id} has no active Store publication"
                ))
            })?;
        let publication = active.attempt()?.clone();
        let accepted = match &publication.entry.payload {
            coven_protocol::store_commit::StorePublicationPayload::Commit(reference) => {
                reference.clone()
            }
            coven_protocol::store_commit::StorePublicationPayload::Snapshot(_) => {
                return Err(DbError::Message(
                    "prepared Store write contains a snapshot publication".to_string(),
                ));
            }
        };
        let root = state.root().clone();
        let unverified: StoreBatchCommit = serde_json::from_slice(commit.semantic_bytes())
            .map_err(|error| DbError::context("prepared Store commit", error))?;
        let registration =
            super::verified_store_authority::VerifiedRegistrationLookup::activated_registration_on(
                state,
                crate::store::store_session::StoreRecords::new(
                    self.store.transaction,
                    self.store.store_dir,
                ),
                &root,
                &unverified.author_registration,
            )?;
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
                "accepted Store publication differs from the exact prepared commit".to_string(),
            ));
        }
        let commit_value = VerifiedStoreBatchCommit::parse(
            commit.semantic_bytes(),
            root.store_root_hash,
            &accepted,
            &registration,
        )
        .map_err(|error| DbError::context("outbound commit", error))?;
        if active.owner()
            != &crate::ActiveStorePublicationOwner::StoreWrite(commit_value.write_id.clone())
            || active.commit_reservation()
                != Some((
                    &commit_value.write_id,
                    &commit_value.author_registration,
                    &commit_value.reference().coord,
                ))
        {
            return Err(DbError::Message(format!(
                "prepared write {stored_write_id} differs from active Store publication {:?}",
                active.owner()
            )));
        }
        let accepted_publication =
            accepted_publication.resolve_installed_on(store_transaction, &commit_value)?;
        let materialize = accepted_publication.requires_materialization();
        let publication: crate::AcceptedStoreCommitEvidence = match &accepted_publication {
            crate::StoreCommitPublicationOutcome::Accepted { interval, .. } => {
                install_accepted_commit_publication_on(tx, &publication, &commit_value, interval)?
                    .into()
            }
            crate::StoreCommitPublicationOutcome::Installed(_) => {
                accepted_publication.install_on(store_transaction, &commit_value)?
            }
        };
        if commit_value.write_id.as_str() != stored_write_id {
            return Err(DbError::Message(
                "prepared write id differs from signed commit".to_string(),
            ));
        }
        let write_id = commit_value.write_id.clone();
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
        let audiences = load_prepared_audience_objects_on(tx, self.store.store_dir, &write_id)?;
        let retained_packages = audiences
            .packages
            .iter()
            .map(|package| package.package().clone())
            .collect::<Vec<_>>();
        for package in &audiences.packages {
            package
                .package()
                .validate_blob_uploader(&commit.author_registration)
                .map_err(DbError::from)?;
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
        for object_id in object_ids {
            let remote = load_remote_object_on(tx, object_id)?
                .into_activated(commit_ref)
                .map_err(|error| {
                    DbError::context(format!("activate remote object {object_id}"), error)
                })?;
            let state = serde_json::to_string(&remote)
                .map_err(|error| DbError::context("serialize activated remote object", error))?;
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
        for package in &retained_packages {
            for binding in package.blob_bindings() {
                crate::blob_records::record_stored_locator_on(tx, binding.blob())?;
            }
        }
        let retained = if materialize {
            let merge_transaction = MergeMaterializationTransaction::from_store(self.store);
            let retained = merge_transaction.record_materialized_merge_commit(
                state,
                &root,
                &commit_value,
                &[],
                &publication,
                &history_evidence,
                &retained_packages,
                (!retained_packages.is_empty())
                    .then_some(RetainedPackageApplication::LocallyAuthored),
            )?;
            state.insert_verified(retained.clone())?;
            let replayed = state.replay_projection_watching_on(
                store_transaction,
                self.blob_decls,
                gates,
                synced_tables,
                routing_key.as_ref(),
                &std::collections::BTreeSet::new(),
                crate::ReplayJournal::Owed,
                coven_protocol::membership::LocalStoreMembership::Current,
                commit_ref,
            )?;
            match replayed.watched_outcome() {
                Some(super::WatchedReplayOutcome::Applied) => {}
                Some(super::WatchedReplayOutcome::Held(reason)) => {
                    return Err(DbError::Message(format!(
                        "accepted local Store publication held during replay: {reason:?}"
                    )));
                }
                None => {
                    return Err(DbError::Message(
                        "accepted local Store publication was absent from replay".to_string(),
                    ));
                }
            }
            replayed.install_on(self)?;
            Some(retained)
        } else {
            None
        };
        let status = finish_store_write_publication_on(
            store_transaction,
            &write_id,
            &audiences,
            local_cleanup,
            PublishedWrite::Commit(PublishedPosition {
                device_id: local_device_id,
                commit: accepted,
            }),
            &active,
        )?;
        Ok((retained, (write_id, status)))
    }
}

pub(super) fn finish_store_write_publication_on(
    store: super::StoreTransaction<'_, '_>,
    write_id: &WriteId,
    audiences: &crate::PreparedAudienceObjects,
    local_cleanup: crate::StoreBatchLocalCleanup,
    published: PublishedWrite,
    active: &crate::ActiveStorePublication,
) -> Result<WriteStatus, DbError> {
    let tx = store.transaction;
    let coord = published.coord();
    let cloud_outbox = CloudOutboxRecords::new(tx);
    let mut consumed_uploads = 0;
    for package in &audiences.packages {
        for binding in package.package().blob_bindings() {
            if cloud_outbox.consume_created_upload_handoff(package.package(), binding)? {
                consumed_uploads += 1;
            }
        }
    }
    match Database::make_remote_publication_root_on(tx, write_id)? {
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
            Database::complete_make_remote_publication_on(tx, write_id)?;
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
                Database::sequence_to_sqlite(&coord.stream_id.to_string(), coord.sequence(),)?,
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
    retain_local_replay_blob_leases(tx, store.store_dir, write_id)?;
    let cleared = tx
        .execute(
            "UPDATE store_writes SET prepared = NULL
                 WHERE write_id = ?1 AND prepared IS NOT NULL",
            [write_id.as_str()],
        )
        .map_err(DbError::from)?;
    if cleared != 1 {
        return Err(DbError::Message(
            "prepared Store write disappeared".to_string(),
        ));
    }
    super::active_store_publication::clear_active_store_publication_on(tx, active)?;
    let status = WriteStatus::Published(Box::new(published));
    Database::set_write_status_on(tx, write_id, &status)?;
    Ok(status)
}

fn install_accepted_commit_publication_on(
    transaction: &rusqlite::Transaction<'_>,
    attempt: &coven_protocol::prepared_commit::PreparedStorePublication,
    commit: &VerifiedStoreBatchCommit,
    accepted: &crate::AcceptedStorePublicationInterval,
) -> Result<crate::AcceptedStoreCommitPublication, DbError> {
    let previous =
        super::observed_store_publication::load_store_current_publication_on(transaction)?;
    if previous.record() != &attempt.previous
        || previous.observed_version() != Some(&attempt.previous_version)
    {
        return Err(DbError::Message(
            "accepted Store commit extends a stale local publication boundary".to_string(),
        ));
    }
    if accepted.interval().previous() != &*attempt.previous
        || accepted.interval().current() != &attempt.replacement
    {
        return Err(DbError::Message(
            "accepted Store publication interval differs from its prepared boundaries".to_string(),
        ));
    }
    let unverified = attempt.entry.clone();
    let reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
        &unverified,
        attempt.entry_object.clone(),
    )
    .map_err(|error| DbError::context("accepted Store publication reference", error))?;
    let publication = accepted
        .accepted_commit(commit)
        .map_err(|error| DbError::context("accepted Store commit publication", error))?;
    if publication.entry() != &attempt.entry || publication.reference() != &reference {
        return Err(DbError::Message(
            "accepted Store publication differs from the prepared entry".to_string(),
        ));
    }
    super::observed_store_publication::install_store_publication_interval_on(
        transaction,
        &previous,
        accepted,
    )?;
    Ok(publication)
}

fn retain_local_replay_blob_leases(
    tx: &rusqlite::Transaction<'_>,
    store_dir: &coven_foundation::store_dir::StoreDir,
    write_id: &WriteId,
) -> Result<(), DbError> {
    let records = super::StoreRecords::new(tx, store_dir);
    let partitions = records.store_write_partitions(write_id.as_str())?;
    let local_rows = partitions
        .local
        .iter()
        .map(|partition| crate::walk_changeset(&partition.changeset))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .filter(|change| {
            !crate::is_routing_table(&change.table)
                && !matches!(change.op, coven_foundation::changeset::ChangeOp::Delete)
        })
        .filter_map(|change| {
            let row_id = change.pk()?.to_string();
            Some((change.table, row_id))
        })
        .collect::<std::collections::BTreeSet<_>>();
    let raw_facts: String = tx
        .query_row(
            "SELECT blob_facts FROM store_writes WHERE write_id = ?1",
            [write_id.as_str()],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    let facts: crate::StoreWriteBlobFacts = serde_json::from_str(&raw_facts)
        .map_err(|error| DbError::context("published Store write blob facts", error))?;
    let retained = facts
        .blobs
        .into_iter()
        .filter(|fact| {
            fact.blob.provenance == coven_protocol::blob::Provenance::HostProvided
                && local_rows.contains(&(fact.table.clone(), fact.row_id.clone()))
        })
        .map(|fact| (fact.blob.namespace, fact.blob.id))
        .collect::<std::collections::BTreeSet<_>>();
    let leases = crate::query_mapped_rows(
        tx,
        "SELECT namespace, blob_id FROM store_write_blob_leases
         WHERE write_id = ?1 ORDER BY namespace, blob_id",
        [write_id.as_str()],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    for (namespace, blob_id) in leases {
        if retained.contains(&(namespace.clone(), blob_id.clone())) {
            continue;
        }
        tx.execute(
            "DELETE FROM store_write_blob_leases
             WHERE write_id = ?1 AND namespace = ?2 AND blob_id = ?3",
            (write_id.as_str(), namespace, blob_id),
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

impl StoreSession<'_> {
    fn complete_prepared_store_write(
        &mut self,
        accepted_publication: crate::StoreCommitPublicationOutcome,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    ) -> Result<
        (
            Option<OwnedVerifiedMergeMaterialization>,
            (WriteId, WriteStatus),
        ),
        DbError,
    > {
        self.verified_store_transaction(move |transaction| {
            let result =
                transaction.complete_prepared_store_write(accepted_publication, routing_key)?;
            Ok(StoreTransactionOutcome::Commit(result))
        })
    }
}

impl StoreDatabase {
    pub async fn complete_prepared_store_write(
        &self,
        accepted_publication: crate::StoreCommitPublicationOutcome,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    ) -> Result<Option<OwnedVerifiedMergeMaterialization>, DbError> {
        let (materialization, (write_id, status)) = self
            .call_store(move |session| {
                session.complete_prepared_store_write(accepted_publication, routing_key)
            })
            .await?;
        self.notify_write_status(write_id, status);
        Ok(materialization)
    }
}
