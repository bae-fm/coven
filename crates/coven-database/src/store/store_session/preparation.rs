use super::{
    publication_state::{PreparedStoreWriteState, StoreWritePreparation},
    StoreDatabase, StoreSession,
};
#[cfg(any(test, feature = "test-utils"))]
use crate::StoreWriteBase;
use crate::{
    persist_exact_remote_object_on, ActiveStorePublication, ActiveStorePublicationOwner, DbError,
    DurablePreparedProtocolObject, LOCAL_DEVICE_ID_STATE_KEY,
};
use coven_protocol::remote_object::RemoteObjectRecord;
use coven_protocol::store_commit::{CommitFrontier, StoreCommitCoord, StoreDeviceRegistrationRef};
use coven_protocol::write::WriteStatus;

impl StoreSession<'_> {
    fn table_schema_for_apply(&mut self) -> Result<crate::TableSchema, DbError> {
        crate::TableSchema::for_apply(self.conn, self.synced_tables, self.gates)
    }

    fn prepare_store_write_commit(&mut self, stage: StoreWritePreparation) -> Result<(), DbError> {
        let author = self.activated_registration(&stage.commit.value.author_registration)?;
        if author.value().store_root != stage.root {
            return Err(DbError::Message(
                "prepared Store write belongs to another verified Store root".to_string(),
            ));
        }
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let local_device_id = crate::required_protocol_state_on(&tx, LOCAL_DEVICE_ID_STATE_KEY)?;
        let registration_object: String = tx
            .query_row(
                "SELECT registration_object \
                 FROM store_device_registration_activations WHERE device_id = ?1",
                [&local_device_id],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let registration_ref: StoreDeviceRegistrationRef =
            serde_json::from_str(&registration_object).map_err(|error| {
                DbError::context("prepared write exact registration ref", error)
            })?;
        if registration_ref != stage.commit.value.author_registration {
            return Err(DbError::Message(
                "prepared Store commit author registration differs from local activation"
                    .to_string(),
            ));
        }
        let registration = stage.commit.value.author();
        if author.value() != registration {
            return Err(DbError::Message(
                "prepared write author registration differs from its activated bytes".to_string(),
            ));
        }
        let stream_id = coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            stage.root.store_root_hash,
            &registration_ref,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let expected_coord = StoreCommitCoord {
            stream_id,
            sequence: stage.commit.value.seq(),
        };
        if stage.commit.value.store_root_hash() != stage.root.store_root_hash
            || stage.commit.value.reference().coord != expected_coord
            || stage.commit.value.reference().object != *stage.commit.prepared.reference()
        {
            return Err(DbError::Message(
                "authenticated prepared Store commit differs from its current local authority"
                    .to_string(),
            ));
        }
        if stage.commit.value.write_id != stage.write_id {
            return Err(DbError::Message(
                "prepared write id differs from signed commit".to_string(),
            ));
        }
        let commit_ref = stage.commit.value.reference().clone();
        stage
            .history_evidence
            .validate_for(&commit_ref, stage.commit.value.value())
            .map_err(|error| DbError::context("prepared Store history evidence", error))?;
        let installed_publication =
            super::observed_store_publication::load_store_current_publication_on(&tx)?;
        if installed_publication.record() != &stage.publication.previous
            || installed_publication.observed_version() != Some(&stage.publication.previous_version)
        {
            return Err(DbError::Message(
                "prepared Store write extends a stale publication boundary".to_string(),
            ));
        }
        let publication_reference = coven_protocol::store_commit::StorePublicationRef::from_entry(
            &stage.publication.entry,
            stage.publication.entry_object.clone(),
        )
        .map_err(|error| DbError::context("prepared Store publication entry", error))?;
        stage
            .publication
            .replacement
            .verify_commit_transition(
                &stage.publication.previous,
                &stage.publication.entry,
                &publication_reference,
                &stage.commit.value,
                &registration.device_signing_pubkey,
            )
            .map_err(|error| DbError::context("prepared Store publication transition", error))?;
        let (stored_base, stored_status, stored_preparation): (
            Option<String>,
            String,
            Option<String>,
        ) = tx
            .query_row(
                "SELECT base, status, prepared
                 FROM store_writes WHERE write_id = ?1",
                [stage.write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)?;
        if stored_status != "\"pending\"" || stored_preparation.is_some() {
            return Err(DbError::Message(format!(
                "write {} is not an unprepared pending write",
                stage.write_id
            )));
        }
        let stored_base = stored_base.ok_or_else(|| {
            DbError::Message(format!(
                "pending write {} carries no commit base",
                stage.write_id
            ))
        })?;
        let partitions = crate::store::store_session::StoreTransaction::new(&tx, self.store_dir)
            .store_write_partitions(stage.write_id.as_str())?;
        let records = super::StoreRecords::new(&tx, self.store_dir);
        let stored_base = records.effective_store_write_base(&stage.write_id, &stored_base)?;
        if let Some(rebased) = records.rebased_store_write(&stage.write_id)? {
            if rebased.publication_base != stage.commit.value.publication_base {
                return Err(DbError::Message(
                    "rebased write requires the validated snapshot base".to_string(),
                ));
            }
        }
        let mut stored_dependencies = CommitFrontier::from_refs(stored_base.dependencies)
            .map_err(|error| DbError::context("stored write dependencies", error))?;
        let observed_predecessor = stored_dependencies.0.remove(&stream_id);
        if stored_dependencies.commits() != stage.commit.value.merge_dependencies() {
            return Err(DbError::Message(format!(
                "prepared commit dependencies differ from write {}",
                stage.write_id
            )));
        }
        let durable_predecessor =
            crate::store::materialized_commit_index::latest_position_for_device_on(
                &tx,
                &stream_id.to_string(),
            )?;
        if observed_predecessor.as_ref().is_some_and(|observed| {
            durable_predecessor.as_ref().is_none_or(|current| {
                current.coord.sequence() < observed.coord.sequence()
                    || current.coord.sequence() == observed.coord.sequence() && current != observed
            })
        }) {
            return Err(DbError::Message(format!(
                "outbound Store commit predecessor does not cover write {} capture frontier",
                stage.write_id
            )));
        }
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

        let active_publication = ActiveStorePublication::commit(
            ActiveStorePublicationOwner::StoreWrite(stage.write_id.clone()),
            stage.write_id.clone(),
            registration_ref,
            commit_ref.coord.clone(),
            stage.publication.clone(),
        )?;
        let reserved = super::active_store_publication::load_active_store_publication_on(&tx)?;
        if let Some(reserved) = reserved
            .as_ref()
            .filter(|active| active.is_awaiting_preparation())
        {
            if reserved.owner() != active_publication.owner()
                || reserved.commit_reservation() != active_publication.commit_reservation()
            {
                return Err(DbError::Message(
                    "candidate preparation differs from its retained reservation".to_string(),
                ));
            }
            let replacement = reserved.replace_attempt(stage.publication.clone())?;
            super::active_store_publication::update_active_store_publication_on(
                &tx,
                reserved,
                &replacement,
            )?;
        } else {
            match super::active_store_publication::claim_active_store_publication_on(
                &tx,
                &active_publication,
            )? {
                super::active_store_publication::ActiveStorePublicationClaim::Acquired => {}
                super::active_store_publication::ActiveStorePublicationClaim::AlreadyOwned => {
                    return Err(DbError::Message(
                        "pending Store write already owns an active publication".to_string(),
                    ));
                }
                super::active_store_publication::ActiveStorePublicationClaim::Occupied(source) => {
                    return Err(DbError::Message(format!(
                        "another local Store operation owns publication: {source:?}"
                    )));
                }
            }
        }

        let mut object_ids = std::collections::BTreeSet::new();
        for remote in &stage.remote_objects {
            remote
                .validate()
                .map_err(|error| DbError::context("prepared remote object", error))?;
            if !object_ids.insert(remote.object_id()) {
                return Err(DbError::Message(
                    "prepared write contains a duplicate remote object".to_string(),
                ));
            }
        }
        crate::validate_prepared_audience_blob_graph(&object_ids, &stage.audiences)?;
        for remote in &stage.remote_objects {
            crate::persist_prepared_remote_object_on(
                &tx,
                self.store_dir,
                remote,
                &commit_ref,
                "candidate audience object",
            )?;
        }
        let commit_remote = RemoteObjectRecord::candidate_commit(
            commit_ref.clone(),
            &stage.commit.value.to_bytes(),
            stage.commit.prepared.stored_bytes(),
        )
        .map_err(|error| DbError::context("prepared candidate commit", error))?;
        persist_exact_remote_object_on(&tx, self.store_dir, &commit_remote, "candidate commit")?;
        let expected_partition_count = usize::from(partitions.store.is_some())
            .checked_add(partitions.circles.len())
            .ok_or_else(|| DbError::Message("audience partition count overflow".to_string()))?;
        if stage.audiences.packages.len() != expected_partition_count {
            return Err(DbError::Message(
                "prepared audience packages do not cover every write partition".to_string(),
            ));
        }
        let mut indexed = std::collections::BTreeSet::new();
        for package in &stage.audiences.packages {
            let value = package.package();
            if value.store_root_hash() != stage.root.store_root_hash
                || value.write_id() != &stage.write_id
                || value.commit_coord() != &commit_ref.coord
                || value.candidate_family() != stage.commit.value.candidate_family()
            {
                return Err(DbError::Message(
                    "prepared audience package differs from its exact Store commit".to_string(),
                ));
            }
            match value.audience() {
                coven_protocol::audience_package::PackageAudience::Store => {
                    let partition = partitions.store.as_ref().ok_or_else(|| {
                        DbError::Message(
                            "prepared Store package has no Store partition".to_string(),
                        )
                    })?;
                    if value.changeset() != partition.changeset {
                        return Err(DbError::Message(
                            "prepared Store package changeset differs from its partition"
                                .to_string(),
                        ));
                    }
                    stage
                        .commit
                        .value
                        .verify_store_package(package.semantic_bytes())
                        .map_err(DbError::from)?;
                }
                coven_protocol::audience_package::PackageAudience::Circle { circle_id, .. } => {
                    let partition = partitions
                        .circles
                        .iter()
                        .find(|partition| {
                            partition.audience
                                == coven_protocol::circle::Audience::Circle(*circle_id)
                        })
                        .ok_or_else(|| {
                            DbError::Message(format!(
                                "prepared Circle package {circle_id} has no partition"
                            ))
                        })?;
                    if value.changeset() != partition.changeset {
                        return Err(DbError::Message(format!(
                            "prepared Circle package {circle_id} changeset differs from its partition"
                        )));
                    }
                    stage
                        .commit
                        .value
                        .verify_circle_package(*circle_id, package.semantic_bytes())
                        .map_err(DbError::from)?;
                }
            }
            indexed.insert(package.remote_object_id());
        }
        indexed.extend(
            stage
                .audiences
                .blobs
                .iter()
                .map(crate::PreparedAudienceBlob::remote_object_id),
        );
        debug_assert_eq!(indexed, object_ids);
        super::prepared_remote_objects::persist_prepared_audience_objects_on(
            &tx,
            self.store_dir,
            &stage.write_id,
            &stage.audiences.packages,
            &stage.audiences.blobs,
        )?;
        let prepared = PreparedStoreWriteState {
            commit: DurablePreparedProtocolObject::new(
                stage.commit.value.to_bytes(),
                stage.commit.prepared,
            ),
            history_evidence: stage.history_evidence,
            local_cleanup: stage.local_cleanup,
            completion: stage.completion,
        };
        let prepared = serde_json::to_string(&prepared)
            .map_err(|error| DbError::context("serialize prepared Store write", error))?;
        let status = serde_json::to_string(&WriteStatus::Publishing)
            .map_err(|error| DbError::context("serialize write status", error))?;
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
        if let Some(reserved) = reserved {
            for retired in reserved.retired_candidates() {
                super::candidate_records::begin_candidate_nonactivation_targets_on(
                    &tx,
                    &retired.candidate()?,
                    &retired.objects()?,
                    &retired.nonactivation,
                )?;
            }
        }
        tx.commit().map_err(DbError::from)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn enqueue_store_changeset_for_test(
        &mut self,
        write_id: coven_protocol::write::WriteId,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        let base = StoreWriteBase {
            dependencies: crate::store::materialized_commit_index::materialized_frontier_on(
                &tx, None,
            )?,
        };
        let partitions = vec![crate::AudiencePartition {
            audience: coven_protocol::circle::Audience::Store,
            control: None,
            changeset: changeset.clone(),
        }];
        let blob_facts = super::host_write_capture::capture_partition_blob_facts_on(
            &tx,
            &partitions,
            self.blob_decls,
        )?;
        let changeset_hash =
            crate::payload_store::write_payload_blocking(&tx, self.store_dir, &changeset)?;
        crate::store::store_session::StoreTransaction::new(&tx, self.store_dir)
            .insert_store_write(&write_id, &partitions, changeset_hash, &base, &blob_facts)?;
        tx.commit().map_err(DbError::from)
    }
}

impl StoreDatabase {
    pub async fn table_schema_for_apply(&self) -> Result<crate::TableSchema, DbError> {
        self.call_store(|session| session.table_schema_for_apply())
            .await
    }

    pub async fn prepare_store_write_commit(
        &self,
        stage: StoreWritePreparation,
    ) -> Result<(), DbError> {
        let write_id = stage.write_id.clone();
        self.call_store(move |session| session.prepare_store_write_commit(stage))
            .await?;
        self.notify_write_status(write_id, WriteStatus::Publishing);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn enqueue_store_changeset_for_test(
        &self,
        changeset: Vec<u8>,
    ) -> Result<(), DbError> {
        let write_id = self.new_store_write_id();
        self.call_store(move |session| {
            session.enqueue_store_changeset_for_test(write_id, changeset)
        })
        .await
    }
}
