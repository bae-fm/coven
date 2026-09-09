use super::*;
use coven_foundation::stage_timing::StageTimings;
use futures_util::stream::{FuturesUnordered, StreamExt};

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(crate) fn local_device_id(&self) -> &coven_protocol::store_commit::StoreDeviceId {
        self.writer.device_id()
    }

    pub(crate) fn membership(&self) -> &coven_protocol::membership::MembershipChain {
        &self.membership
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
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<u64, StoreError> {
        let operation = self;
        let database = operation.database.clone();
        // Each candidate here takes a position on this device's own stream, so
        // this waits its turn behind any operation composing against that same
        // position.
        let _authorship = database.author_own_stream().await;
        timings
            .stage("retire blob spools", database.retire_uploaded_blob_spools())
            .await?;
        if database
            .active_store_publication()
            .await?
            .is_some_and(|active| active.is_awaiting_preparation())
            && !operation
                .prepare_store_write_with_authorship(timings, &_authorship)
                .await?
        {
            return Err(StoreError::InvalidOutbound(
                "reserved rebased write cannot enter preparation".to_string(),
            ));
        }
        let Some(first) = database.oldest_prepared_store_write().await? else {
            return Ok(0);
        };
        let routing_key = routing_encryption
            .map(|encryption| {
                coven_protocol::circle::derive_row_routing_key(
                    encryption,
                    operation.store_root().store_root_hash,
                )
            })
            .transpose()?;
        let database = operation.database.clone();
        let storage = operation.storage.clone();
        #[cfg(any(test, feature = "test-utils"))]
        let db = &database;
        let mut published = 0_u64;
        let mut next = Some(first);
        while let Some(batch) = next {
            operation.retire_replaced_store_write_candidates().await?;
            let root = operation.store_root().clone();
            let write_id = batch.commit.value.write_id.clone();
            database
                .set_write_status(&write_id, coven_protocol::write::WriteStatus::Publishing)
                .await?;
            let attempt = async {
                if let Some(covered) = database.covered_store_write(batch.commit.value.clone()).await? {
                    operation.complete_snapshot_covered_write(covered).await?;
                    return Ok(true);
                }
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
                let commit_ref = batch.commit.value.reference();
                let stream_id = commit_ref.coord.stream_id.to_string();
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
                    .mark_candidate_commit_uploaded(commit_ref.clone())
                    .await?;
                #[cfg(any(test, feature = "test-utils"))]
                db.reach_test_point(
                    coven_database::DatabaseTestPoint::StoreWriteCommitUploaded {
                        write_id: write_id.clone(),
                    },
                )
                .await;
                let accepted_publication = timings
                    .stage(
                        "publish shared position",
                        operation.publish_store_commit_publication(&batch.commit.value),
                    )
                    .await?;
                let accepted_publication = match accepted_publication {
                    crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome::Published(outcome) => outcome,
                    crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome::SnapshotCovered(covered) => {
                        operation.complete_snapshot_covered_write(covered).await?;
                        return Ok(true);
                    }
                    crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome::AwaitingPreparation(reserved) => {
                        if reserved != write_id {
                            return Err(StoreError::InvalidOutbound("publication rebase returned another write reservation".to_string()));
                        }
                        return Ok(false);
                    }
                    crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome::SnapshotRetired(_) => {
                        return Err(StoreError::InvalidOutbound("snapshot installation did not retire the reserved row-write candidate".into()));
                    }
                };
                #[cfg(any(test, feature = "test-utils"))]
                db.reach_test_point(
                    coven_database::DatabaseTestPoint::StoreWritePublicationAccepted {
                        write_id: write_id.clone(),
                    },
                )
                .await;
                let materialization = timings
                    .stage(
                        "complete write",
                        database.complete_prepared_store_write(
                            accepted_publication,
                            routing_key.clone(),
                        ),
                    )
                    .await?;
                if let Some(materialization) = materialization {
                    operation
                        .history
                        .admit_materialized_publication(&materialization)
                        .map_err(StoreError::from)?;
                }
                Ok::<bool, StoreError>(true)
            }
            .await;
            match attempt {
                Ok(false) => {
                    if !operation
                        .prepare_store_write_with_authorship(timings, &_authorship)
                        .await?
                    {
                        return Err(StoreError::InvalidOutbound(
                            "rebased write did not resume its reserved preparation".to_string(),
                        ));
                    }
                    next = Some(database.oldest_prepared_store_write().await?.ok_or_else(
                        || {
                            StoreError::InvalidOutbound(
                                "rebased write preparation produced no reserved candidate"
                                    .to_string(),
                            )
                        },
                    )?);
                    continue;
                }
                Ok(true) => {}
                Err(error) => {
                    if let Some((blocked_write, block)) = error.write_block(&write_id) {
                        if let Err(status) = database
                            .block_write_if_unresolved(&blocked_write, block)
                            .await
                        {
                            return Err(StoreError::WriteBlockNotRecorded {
                                write_id: blocked_write,
                                operation: Box::new(error),
                                status,
                            });
                        }
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

    async fn complete_snapshot_covered_write(
        &self,
        covered: coven_database::store::CoveredStoreWrite,
    ) -> Result<(), StoreError> {
        let _upload = self.database.blob_upload_drain_permit().await;
        let _snapshot = self.database.snapshot_publication_permit().await;
        let completion = self
            .database
            .begin_covered_write_completion(covered)
            .await?;
        #[cfg(any(test, feature = "test-utils"))]
        self.database
            .reach_test_point(coven_database::DatabaseTestPoint::CoveredWriteCleanupPrepared)
            .await;
        for object in completion.protocol_objects() {
            self.storage
                .delete_protocol_object(object)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
        }
        for blob in completion.blob_objects() {
            self.storage
                .delete_blob_object(blob)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
        }
        self.database.complete_covered_write(completion).await?;
        Ok(())
    }

    async fn retire_replaced_store_write_candidates(&self) -> Result<(), StoreError> {
        let Some(active) = self.database.active_store_publication().await? else {
            return Ok(());
        };
        crate::sync::store::authorization::retire_store_write_candidates(
            &self.database,
            self.storage.as_ref(),
            active,
        )
        .await
    }

    pub(crate) async fn publish_store_commit_publication(
        &mut self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome, StoreError>{
        self.writer
            .publish_store_commit(&mut self.history, &mut self.membership, commit)
            .await
    }

    pub(crate) async fn publish_pending_store_writes(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<u64, SyncCycleFailure> {
        // Publishing one release's worth of host writes was the slowest stage of
        // a live cycle. Each commit it publishes costs several provider round
        // trips for its packages, commit, publication entry, and conditional
        // current-record update, on top of sealing a package per audience.
        // Counting the run says how many of
        // those each stage actually made rather than leaving it to be read off
        // this comment, and both the times and the counts accumulate across
        // every commit the loop publishes, so a slow cycle is described by one
        // line however many commits it drained. Reported on every exit path,
        // including failures.
        let mut timings =
            StageTimings::counting("Store write publication", self.provider_requests());
        let outcome =
            Box::pin(self.publish_pending_store_writes_timed(&mut timings, routing_encryption))
                .await;
        timings.report();
        outcome
    }

    async fn publish_pending_store_writes_timed(
        &mut self,
        timings: &mut StageTimings,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
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
                .drain_prepared_store_writes(timings, routing_encryption)
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

    pub(crate) async fn publish_prepared_store_writes(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<u64, SyncCycleFailure> {
        self.drain_prepared_store_writes_timed(
            "prepared Store write publication",
            routing_encryption,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store write", error))
    }

    /// Drain under a timing run of its own, for the callers that publish outside
    /// the cycle's own publication stage. `run` names which one in the line.
    pub(super) async fn drain_prepared_store_writes_timed(
        &mut self,
        run: &'static str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<u64, StoreError> {
        let mut timings = StageTimings::counting(run, self.provider_requests());
        let outcome =
            Box::pin(self.drain_prepared_store_writes(&mut timings, routing_encryption)).await;
        timings.report();
        outcome
    }

    pub(crate) async fn reclaim_packages(
        &mut self,
        settled: &crate::sync::store::SettledCycle,
    ) -> Result<reclaim::StoreReclaimResult, reclaim::StoreReclaimError> {
        self.reclaim().run(settled).await
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
                    let control = coven_storage::cloud::UploadControl::running(
                        coven_storage::cloud::no_progress(),
                    );
                    let path = prepared.spool_path.as_deref().ok_or_else(|| {
                        StoreError::InvalidOutbound(format!(
                            "prepared blob {} awaiting upload has no local spool",
                            remote.object_id()
                        ))
                    })?;
                    storage
                        .create_blob_object_from_file(&blob, &authority, path, &control)
                        .await
                        .map_err(|source| StoreError::BlobStorage {
                            namespace: blob.locator().namespace().to_string(),
                            id: blob.locator().blob_id().to_string(),
                            source,
                        })?;
                } else if !remote.records_verified_upload() {
                    // Nothing here uploads a blob it has no spool for, so this
                    // is a blob whose record does not say it was created — and
                    // that is reachable while the write is still draining:
                    // these records are read live, and the nonactivation
                    // machinery retires a blob's ownership the moment its last
                    // pending candidate loses a merge race, is abandoned, or
                    // has its author excluded. Refuse the write. Skipping the
                    // blob would publish a commit naming bytes nobody put at
                    // the provider; reading the provider to find out would be
                    // the round trip this path exists to avoid.
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
