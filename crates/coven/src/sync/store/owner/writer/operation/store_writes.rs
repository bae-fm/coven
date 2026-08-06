use super::*;
use crate::storage::VerifiedObjectWrites;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(super) fn local_device_id(&self) -> &coven_protocol::store_commit::StoreDeviceId {
        self.writer.device_id()
    }

    pub(crate) fn announcement_stream_id(&self) -> coven_protocol::membership::AuthorStreamId {
        self.writer
            .announcement_stream_id(self.store_root().store_root_hash)
    }

    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, coven_database::DbError>
    {
        self.database
            .latest_local_store_position(self.announcement_stream_id())
            .await
    }

    pub(super) async fn drain_prepared_store_writes(&mut self) -> Result<u64, StoreError> {
        let operation = self;
        let database = &operation.database;
        // Each candidate here takes a position on this device's own stream by
        // publishing its head, so this waits its turn behind any operation composing
        // against that same position. Queued writes are the one composer that can
        // lose a position safely — they re-prepare against the winner — but nothing
        // else can, so they must not be the ones to take it out from under an
        // operation that is mid-activation.
        let _authorship = database.author_own_stream().await;
        database.retire_uploaded_blob_spools().await?;
        let Some(first) = database.oldest_prepared_store_write().await? else {
            return Ok(0);
        };
        let database = operation.database.clone();
        let storage = operation.storage.as_ref();
        #[cfg(test)]
        let db = &database;
        let mut published = 0_u64;
        let mut next = Some(first);
        while let Some(batch) = next {
            let root = operation.store_root().clone();
            let write_id = batch.commit.value.write_id.clone();
            database
                .set_write_status(&write_id, crate::WriteStatus::Publishing)
                .await?;
            let attempt = async {
                Box::pin(operation.reject_excluded_merge_candidate(
                    &batch.head.value.commit,
                    &batch.commit.value.author_registration,
                ))
                .await?;
                let store_root_hash = root.store_root_hash;
                let commit = &batch.commit.value;
                if !matches!(
                    commit.body,
                    coven_protocol::store_commit::StoreCommitBody::AbandonCandidates { .. }
                ) {
                    operation.publish_prepared_remote_objects(&write_id).await?;
                    database.retire_uploaded_blob_spools().await?;
                }
                let head = &batch.head.value;
                let stream_id = head.commit.coord.stream_id.to_string();
                let commit_context = ProtocolObjectContext::signed_plaintext(
                    store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                );
                let commit_prefix = commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id,
                    commit.seq(),
                    commit.commit_hash(),
                );
                storage
                    .create_protocol_object(&batch.commit.prepared)
                    .await
                    .map_err(StoreObjectError::from)?;
                storage
                    .verify_readback(
                        &commit_context,
                        &batch.commit.object,
                        &commit_prefix,
                        &batch.commit.bytes,
                    )
                    .await
                    .map_err(StoreError::readback)?;
                database
                    .mark_candidate_commit_uploaded(head.commit.clone())
                    .await?;
                #[cfg(test)]
                db.reach_test_point(
                    coven_database::DatabaseTestPoint::StoreWriteCommitUploaded {
                        write_id: write_id.clone(),
                    },
                )
                .await;
                Box::pin(
                    operation
                        .reject_excluded_merge_candidate(&head.commit, &commit.author_registration),
                )
                .await?;
                let head_prefix = head_slot_prefix(
                    &head.author_registration.device_id.to_string(),
                    commit.seq(),
                );
                if let Err(error) = storage.create_protocol_object(&batch.head.prepared).await {
                    if !matches!(error, StorageError::SlotCollision(_)) {
                        return Err(StoreObjectError::from(error).into());
                    }
                    let observation = operation
                        .observe_occupied_merge_head(
                            head,
                            commit,
                            batch.head.object.slot(),
                            &head_prefix,
                        )
                        .await?;
                    if observation.winner().commit == head.commit {
                        let registration = database
                            .activated_store_device_registration(head.author_registration.clone())
                            .await?;
                        let nonactivations = observation.verified_nonactivations(
                            commit
                                .abandoned_candidates()
                                .iter()
                                .map(|manifest| manifest.candidate.clone()),
                            registration.value(),
                        )?;
                        let (winner, winner_prepared) = observation.into_head();
                        database
                            .adopt_alternate_merge_head(write_id.clone(), winner, winner_prepared)
                            .await?;
                        #[cfg(test)]
                        db.reach_test_point(
                            coven_database::DatabaseTestPoint::StoreWriteHeadReadBack {
                                write_id: write_id.clone(),
                            },
                        )
                        .await;
                        match database
                            .complete_prepared_store_write(
                                root.clone(),
                                head.commit.clone(),
                                nonactivations,
                            )
                            .await?
                        {
                            coven_database::CompletePreparedStoreWriteOutcome::Published => {}
                            coven_database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                                device_id,
                            } => return Err(StoreError::AuthorExcluded { device_id }),
                        }
                        return Ok::<bool, StoreError>(true);
                    }
                    let registration = database
                        .activated_store_device_registration(head.author_registration.clone())
                        .await?;
                    let nonactivations = observation.verified_nonactivations(
                        std::iter::once(StoreBatchCommitDeletionTarget {
                            coord: head.commit.coord.clone(),
                            object: head.commit.object.clone(),
                            canonical_signed_bytes: commit.to_bytes(),
                        })
                        .chain(
                            commit
                                .abandoned_candidates()
                                .iter()
                                .map(|manifest| manifest.candidate.clone()),
                        ),
                        registration.value(),
                    )?;
                    database
                        .mark_merge_candidate_conflict(write_id.clone(), nonactivations)
                        .await?;
                    return Ok::<bool, StoreError>(false);
                }
                let observation = operation
                    .observe_occupied_merge_head(
                        head,
                        commit,
                        batch.head.object.slot(),
                        &head_prefix,
                    )
                    .await?;
                if observation.winner() != head
                    || observation.winner_prepared().reference() != &batch.head.object
                {
                    return Err(StoreError::InvalidOutbound(
                        "prepared head exact readback differs from its signed bytes".to_string(),
                    ));
                }
                let registration = database
                    .activated_store_device_registration(head.author_registration.clone())
                    .await?;
                let nonactivations = observation.verified_nonactivations(
                    commit
                        .abandoned_candidates()
                        .iter()
                        .map(|manifest| manifest.candidate.clone()),
                    registration.value(),
                )?;
                database
                    .mark_store_head_uploaded(StoreDeviceHeadRef {
                        head_hash: head.head_hash(),
                        object: batch.head.object.clone(),
                    })
                    .await?;
                #[cfg(test)]
                db.reach_test_point(coven_database::DatabaseTestPoint::StoreWriteHeadReadBack {
                    write_id: write_id.clone(),
                })
                .await;
                match database
                    .complete_prepared_store_write(root, head.commit.clone(), nonactivations)
                    .await?
                {
                    coven_database::CompletePreparedStoreWriteOutcome::Published => {}
                    coven_database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                        device_id,
                    } => return Err(StoreError::AuthorExcluded { device_id }),
                }
                Ok::<bool, StoreError>(true)
            }
            .await;
            match attempt {
                Ok(false) => return Ok(published),
                Ok(true) => {}
                Err(error) => {
                    if let Some(block) = error.write_block() {
                        database.block_write_if_unresolved(&write_id, block).await?;
                    }
                    return Err(error);
                }
            }
            published = published
                .checked_add(1)
                .ok_or(StoreError::PublishCountExhausted)?;
            next = database.oldest_prepared_store_write().await?;
        }
        Ok(published)
    }

    pub(crate) async fn publish_pending_store_writes(&mut self) -> Result<u64, SyncCycleFailure> {
        let mut published = 0_u64;
        loop {
            if !self
                .prepare_store_write()
                .await
                .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))?
            {
                return Ok(published);
            }
            let drained = self
                .drain_prepared_store_writes()
                .await
                .map_err(|error| SyncCycleFailure::operation("publish Store write", error))?;
            published = published.checked_add(drained).ok_or_else(|| {
                SyncCycleFailure::operation(
                    "publish Store write",
                    StoreError::PublishCountExhausted,
                )
            })?;
        }
    }

    pub(crate) async fn publish_prepared_store_writes(&mut self) -> Result<u64, SyncCycleFailure> {
        self.drain_prepared_store_writes()
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store write", error))
    }

    pub(crate) async fn reclaim_packages(
        &mut self,
    ) -> Result<reclaim::StoreReclaimResult, reclaim::StoreReclaimError> {
        self.reclaim().run().await
    }

    pub(super) fn reclaim(&mut self) -> reclaim::AuthorizedReclaim<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        reclaim::AuthorizedReclaim::new(self, database, storage, root, membership)
    }

    pub(crate) async fn resume_operations(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<(), SyncCycleFailure> {
        self.device_exclusion()
            .resume()
            .await
            .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        let routing_key = routing_encryption
            .map(|encryption| {
                coven_protocol::circle::derive_row_routing_key(
                    encryption,
                    self.store_root().store_root_hash,
                )
            })
            .transpose()
            .map_err(|error| {
                SyncCycleFailure::operation("derive Circle operation routing key", error)
            })?;
        self.circles()
            .resume_circle_operations(routing_key.as_ref())
            .await
            .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    pub(super) async fn publish_prepared_remote_objects(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<(), StoreError> {
        use coven_protocol::objects::{
            BlobWriteAuthority, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain,
            StoreObjectError,
        };
        use coven_protocol::store_commit::{
            circle_package_semantic_prefix, package_semantic_prefix, ObjectHash,
        };

        let database = &self.database;
        let storage = self.storage.as_ref();
        let store_root_hash = self.store_root().store_root_hash;
        for prepared in database.prepared_remote_objects(write_id).await? {
            let remote = prepared.record;
            let prepared_state = match &remote {
                coven_protocol::remote_object::RemoteObjectRecord::CandidateCommit(record) => {
                    matches!(
                        record.state,
                        coven_protocol::remote_object::CandidateCommitState::Prepared
                    )
                }
                coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(record) => {
                    matches!(
                        record.state,
                        coven_protocol::remote_object::CandidateObjectState::Prepared { .. }
                    )
                }
                coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) => {
                    matches!(
                        record.state,
                        coven_protocol::remote_object::OwnedObjectState::Prepared { .. }
                    )
                }
                coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(_) => false,
            };
            match remote.bytes().stored() {
                coven_protocol::remote_object::RemoteStoredRepresentation::Inline {
                    bytes,
                    object,
                } => {
                    let package = coven_protocol::audience_package::AudiencePackage::parse(
                        remote.bytes().canonical_semantic_bytes(),
                    )
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                    let stream_id = package.commit_coord().stream_id.to_string();
                    let sequence = package.commit_coord().sequence;
                    let (context, prefix) = match package.audience() {
                        coven_protocol::audience_package::PackageAudience::Store => (
                            ProtocolObjectContext::store_encrypted(
                                store_root_hash,
                                ProtocolObjectDomain::StorePackage,
                            ),
                            package_semantic_prefix(
                                package.candidate_family(),
                                &stream_id,
                                sequence,
                                ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                            ),
                        ),
                        coven_protocol::audience_package::PackageAudience::Circle {
                            circle_id,
                            control,
                            ..
                        } => {
                            let access = database
                                .circle_publication_context(*circle_id, control.clone())
                                .await?;
                            (
                                access.protocol_context(
                                    store_root_hash,
                                    ProtocolObjectDomain::CirclePackage,
                                ),
                                circle_package_semantic_prefix(
                                    *circle_id,
                                    package.candidate_family(),
                                    &stream_id,
                                    sequence,
                                    ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                                ),
                            )
                        }
                    };
                    let prepared = PreparedExactObject::new(object.clone(), bytes.clone())
                        .map_err(StoreObjectError::from)?;
                    if prepared_state {
                        storage
                            .create_protocol_object(&prepared)
                            .await
                            .map_err(StoreObjectError::from)?;
                    }
                    storage
                        .verify_readback(
                            &context,
                            object,
                            &prefix,
                            remote.bytes().canonical_semantic_bytes(),
                        )
                        .await
                        .map_err(StoreError::readback)?;
                }
                coven_protocol::remote_object::RemoteStoredRepresentation::Blob { object } => {
                    let locator = coven_protocol::blob::locator::BlobLocator::parse(
                        remote.bytes().canonical_semantic_bytes(),
                    )
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                    let uploader = locator.uploader().clone();
                    let registration = database
                        .activated_store_device_registration(uploader.clone())
                        .await?;
                    let authority = BlobWriteAuthority::new(&registration);
                    let blob =
                        coven_protocol::blob::locator::StoredBlobRef::new(locator, object.clone())
                            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                    if prepared_state {
                        let path = prepared.spool_path.as_deref().ok_or_else(|| {
                            StoreError::InvalidOutbound(format!(
                                "prepared blob {} awaiting upload has no local spool",
                                remote.object_id()
                            ))
                        })?;
                        storage
                            .create_blob_object_from_file(
                                &blob,
                                &authority,
                                path,
                                &crate::storage::cloud::no_progress(),
                            )
                            .await
                            .map_err(|source| StoreError::BlobStorage {
                                namespace: blob.locator().namespace().to_string(),
                                id: blob.locator().blob_id().to_string(),
                                source,
                            })?;
                    }
                    storage.verify_blob_object(&blob).await.map_err(|source| {
                        StoreError::BlobStorage {
                            namespace: blob.locator().namespace().to_string(),
                            id: blob.locator().blob_id().to_string(),
                            source,
                        }
                    })?;
                }
                coven_protocol::remote_object::RemoteStoredRepresentation::ExternalExact {
                    ..
                } => {
                    return Err(StoreError::InvalidOutbound(format!(
                        "prepared outbound object {} has no locally stored representation",
                        remote.object_id()
                    )));
                }
            }
            if prepared_state {
                database.mark_remote_object_uploaded(remote).await?;
            }
        }
        Ok(())
    }
}
