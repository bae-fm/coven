use super::*;
use crate::sync::stage_timing::StageTimings;
use futures_util::stream::{FuturesUnordered, StreamExt};

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(crate) fn local_device_id(&self) -> &coven_protocol::store_commit::StoreDeviceId {
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

    pub(super) async fn drain_prepared_store_writes(
        &mut self,
        timings: &mut StageTimings,
    ) -> Result<u64, StoreError> {
        let operation = self;
        let database = &operation.database;
        // Each candidate here takes a position on this device's own stream by
        // publishing its head, so this waits its turn behind any operation composing
        // against that same position. Queued writes are the one composer that can
        // lose a position safely — they re-prepare against the winner — but nothing
        // else can, so they must not be the ones to take it out from under an
        // operation that is mid-activation.
        let _authorship = database.author_own_stream().await;
        timings
            .stage("retire blob spools", database.retire_uploaded_blob_spools())
            .await?;
        let Some(first) = database.oldest_prepared_store_write().await? else {
            return Ok(0);
        };
        let database = operation.database.clone();
        let storage = operation.storage.as_ref();
        #[cfg(any(test, feature = "test-utils"))]
        let db = &database;
        let mut published = 0_u64;
        let mut next = Some(first);
        while let Some(batch) = next {
            let root = operation.store_root().clone();
            let write_id = batch.commit.value.write_id.clone();
            database
                .set_write_status(&write_id, coven_protocol::write::WriteStatus::Publishing)
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
                    timings
                        .stage(
                            "publish packages",
                            operation.publish_prepared_remote_objects(&write_id),
                        )
                        .await?;
                    timings
                        .stage("retire blob spools", database.retire_uploaded_blob_spools())
                        .await?;
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
                timings
                    .stage(
                        "publish commit",
                        storage.create_verified_protocol_object(
                            &commit_context,
                            &batch.commit.prepared,
                            &commit_prefix,
                            &batch.commit.bytes,
                        ),
                    )
                    .await
                    .map_err(StoreError::prepared_object)?;
                database
                    .mark_candidate_commit_uploaded(head.commit.clone())
                    .await?;
                #[cfg(any(test, feature = "test-utils"))]
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
                let head_create = timings
                    .stage(
                        "publish head",
                        storage.create_protocol_object(&batch.head.prepared),
                    )
                    .await;
                if let Err(error) = head_create {
                    if !matches!(error, StorageError::SlotCollision(_)) {
                        return Err(StoreObjectError::from(error).into());
                    }
                    let observation = operation
                        .observe_occupied_merge_head(
                            head,
                            commit,
                            batch.head.prepared.reference().slot(),
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
                        #[cfg(any(test, feature = "test-utils"))]
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
                let observation = timings
                    .stage(
                        "read back head",
                        operation.observe_occupied_merge_head(
                            head,
                            commit,
                            batch.head.prepared.reference().slot(),
                            &head_prefix,
                        ),
                    )
                    .await?;
                if observation.winner() != head
                    || observation.winner_prepared().reference() != batch.head.prepared.reference()
                {
                    return Err(StoreError::InvalidOutbound(
                        "occupied Merge head body differs from the prepared signed bytes"
                            .to_string(),
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
                        object: batch.head.prepared.reference().clone(),
                    })
                    .await?;
                #[cfg(any(test, feature = "test-utils"))]
                db.reach_test_point(coven_database::DatabaseTestPoint::StoreWriteHeadReadBack {
                    write_id: write_id.clone(),
                })
                .await;
                match timings
                    .stage(
                        "complete write",
                        database.complete_prepared_store_write(
                            root,
                            head.commit.clone(),
                            nonactivations,
                        ),
                    )
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
        // Publishing one release's worth of host writes was the slowest stage of
        // a live cycle. Each commit it publishes costs several provider round
        // trips — two slot allocations while preparing, then the packages, the
        // commit, the head, and the read-back that confirms the head — on top of
        // sealing a package per audience. The stage totals say which of those
        // the time went to, and they accumulate across every commit the loop
        // publishes so a slow cycle is described by one line however many
        // commits it drained. Reported on every exit path, including failures.
        let mut timings = StageTimings::start("Store write publication");
        let outcome = Box::pin(self.publish_pending_store_writes_timed(&mut timings)).await;
        timings.report();
        outcome
    }

    async fn publish_pending_store_writes_timed(
        &mut self,
        timings: &mut StageTimings,
    ) -> Result<u64, SyncCycleFailure> {
        let mut published = 0_u64;
        loop {
            if !self
                .prepare_store_write(timings)
                .await
                .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))?
            {
                return Ok(published);
            }
            let drained = self
                .drain_prepared_store_writes(timings)
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
        self.drain_prepared_store_writes_timed("prepared Store write publication")
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store write", error))
    }

    /// Drain under a timing run of its own, for the callers that publish outside
    /// the cycle's own publication stage. `run` names which one in the line.
    pub(super) async fn drain_prepared_store_writes_timed(
        &mut self,
        run: &'static str,
    ) -> Result<u64, StoreError> {
        let mut timings = StageTimings::start(run);
        let outcome = Box::pin(self.drain_prepared_store_writes(&mut timings)).await;
        timings.report();
        outcome
    }

    pub(crate) async fn reclaim_packages(
        &mut self,
    ) -> Result<reclaim::StoreReclaimResult, reclaim::StoreReclaimError> {
        self.reclaim().run().await
    }

    pub(crate) fn reclaim(&mut self) -> reclaim::AuthorizedReclaim<'_, 'storage> {
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

    /// Create every prepared outbound object this write carries: its audience
    /// packages, and any blob whose upload this write is the one to perform.
    ///
    /// Runs up to `transfer_limits().uploads` at once. Each object is
    /// independent — its own bytes, its own create, its own durable mark — and a
    /// release publishes one package per audience plus a blob per file, so on a
    /// twenty-two file release this was forty serial provider round trips, nine
    /// of the eleven seconds a live publication took.
    ///
    /// The barrier is at the end, not between objects: the caller creates the
    /// commit and then the head only after this returns, because a commit names
    /// packages that must already exist. Concurrency here does not weaken that —
    /// it only stops the packages waiting on each other.
    ///
    /// A failure lets the objects already in flight finish before surfacing.
    /// Each object's create and durable mark stand on their own, so more of them
    /// landing is more progress carried into the retry, and the first error in
    /// queue order is the one returned.
    pub(super) async fn publish_prepared_remote_objects(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<(), StoreError> {
        let prepared = self.database.prepared_remote_objects(write_id).await?;
        let limit = self.database.transfer_limits().uploads.get();
        let mut pending = prepared.into_iter().enumerate();
        let mut inflight = FuturesUnordered::new();
        let mut failures: Vec<(usize, StoreError)> = Vec::new();
        loop {
            while inflight.len() < limit {
                let Some((position, prepared)) = pending.next() else {
                    break;
                };
                inflight.push(async move {
                    (
                        position,
                        self.publish_prepared_remote_object(prepared).await,
                    )
                });
            }
            match inflight.next().await {
                Some((position, Err(error))) => failures.push((position, error)),
                Some((_, Ok(()))) => {}
                None => break,
            }
        }
        match failures.into_iter().min_by_key(|(position, _)| *position) {
            Some((_, error)) => Err(error),
            None => Ok(()),
        }
    }

    async fn publish_prepared_remote_object(
        &self,
        prepared: coven_database::PreparedRemoteObject,
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
        let remote = prepared.closed;
        let prepared_state = match &*remote {
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
        match remote.payloads() {
            coven_protocol::remote_object::RemoteObjectPayloads::SpooledInline => {
                let object = remote.object();
                let semantic_bytes = remote.semantic_bytes().ok_or_else(|| {
                    StoreError::InvalidOutbound(format!(
                        "prepared outbound object {} names no plaintext",
                        remote.object_id()
                    ))
                })?;
                let stored_bytes = remote.stored_bytes().ok_or_else(|| {
                    StoreError::InvalidOutbound(format!(
                        "prepared outbound object {} names no ciphertext",
                        remote.object_id()
                    ))
                })?;
                let package =
                    coven_protocol::audience_package::AudiencePackage::parse(semantic_bytes)
                        .map_err(StoreError::from)?;
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
                            ObjectHash::digest(semantic_bytes),
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
                                ObjectHash::digest(semantic_bytes),
                            ),
                        )
                    }
                };
                let exact = PreparedExactObject::new(object.clone(), stored_bytes.to_vec())
                    .map_err(StoreObjectError::from)?;
                storage
                    .verify_prepared_protocol_object(&context, &exact, &prefix, semantic_bytes)
                    .await
                    .map_err(StoreError::prepared_object)?;
                if prepared_state {
                    storage
                        .create_protocol_object(&exact)
                        .await
                        .map_err(StoreObjectError::from)?;
                }
            }
            coven_protocol::remote_object::RemoteObjectPayloads::RowBlob { locator_bytes } => {
                let locator = coven_protocol::blob::locator::BlobLocator::parse(locator_bytes)
                    .map_err(StoreError::from)?;
                let uploader = locator.uploader().clone();
                let registration = database
                    .activated_store_device_registration(uploader.clone())
                    .await?;
                let authority = BlobWriteAuthority::new(&registration);
                let blob = coven_protocol::blob::locator::StoredBlobRef::new(
                    locator,
                    remote.object().clone(),
                )
                .map_err(StoreError::from)?;
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
                            &coven_storage::cloud::no_progress(),
                        )
                        .await
                        .map_err(|source| StoreError::BlobStorage {
                            namespace: blob.locator().namespace().to_string(),
                            id: blob.locator().blob_id().to_string(),
                            source,
                        })?;
                } else if !remote.records_verified_upload() {
                    // Nothing here uploads a blob it has no spool for, and a
                    // blob whose record does not say it was created is one this
                    // device never wrote. Publishing a commit that names it
                    // would name bytes nobody put at the provider.
                    return Err(StoreError::InvalidOutbound(format!(
                        "prepared blob {} has no durable record of its upload",
                        remote.object_id()
                    )));
                }
            }
            coven_protocol::remote_object::RemoteObjectPayloads::SpooledExternal => {
                return Err(StoreError::InvalidOutbound(format!(
                    "prepared outbound object {} has no locally stored representation",
                    remote.object_id()
                )));
            }
        }
        if prepared_state {
            database
                .mark_remote_object_uploaded(remote.into_record())
                .await?;
        }
        Ok(())
    }
}
