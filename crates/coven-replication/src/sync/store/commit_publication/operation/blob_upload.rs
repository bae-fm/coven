//! Store-writer upload execution. Each exact Local row-version journal moves
//! through Pending → Prepared → Created. Prepared durably binds its locator,
//! allocated object, and immutable spool; Created records that exact cloud bytes
//! were created and verified. The transition finalizer flips a root only when all
//! of its current row journals are Created.

use futures_util::stream::{FuturesUnordered, StreamExt};
use tracing::warn;

use coven_database::DbError;
use coven_database::{OutboxEntry, OutboxOperation, OutboxUploadState};
use coven_keys::encryption::EncryptionService;
use coven_protocol::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
use coven_protocol::blob::{BlobTransitionObserver, RowBlobRef};

use crate::blob::{DrainOutcome, UploadFailure, UploadFailureCause, UploadFailures};

use super::AuthorizedWriterOperation;

struct BlobUploadAttempt<'operation, 'storage, 'authority> {
    writer: &'operation AuthorizedWriterOperation<'storage>,
    authority: &'operation coven_protocol::objects::BlobWriteAuthority<'authority>,
    routing_encryption: Option<&'operation EncryptionService>,
    observer: Option<&'operation dyn BlobTransitionObserver>,
    now: chrono::DateTime<chrono::Utc>,
    entry: OutboxEntry,
}

impl AuthorizedWriterOperation<'_> {
    /// Drain pending blob uploads: read each local file, seal it under its scope,
    /// create its exact cloud object — and, for an entry marked `retain_pinned`, keep the
    /// plaintext in the protected locator-keyed local cache so the blob
    /// stays local without a later fetch.
    ///
    /// A failing entry is recorded and skipped rather than stopping the drain, with a
    /// per-entry backoff so a persistently-failing entry doesn't block the rest of
    /// the queue or get re-attempted every cycle. The `observer` (if any) is notified
    /// as each attempt starts, succeeds, or fails. Created journals remain until the
    /// pending Store write activates their normal locator bindings. A cancelled root
    /// exact-deletes each created object and its spool before removing the journal;
    /// restarting during cleanup resets that exact journal to Pending.
    ///
    /// This is the only place the queue is drained, and every caller reaches it —
    /// the sync cycle directly, the host's explicit drain and its retry-now
    /// through the loop handle. Whoever arrives second waits here and reads the
    /// queue afterwards, so it admits what the drain ahead of it left rather
    /// than a second view of entries already being uploaded.
    pub(crate) async fn drain_uploads(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
        routing_encryption: Option<&EncryptionService>,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<DrainOutcome, DbError> {
        // Taken before the queue is read, because reading it is what decides
        // which entries this pass will run: an entry is otherwise claimed only
        // by the compare-and-set that hands off its prepared object, which is
        // after the seal, the spool write, and the preparation progress an
        // observer has already been told about. Held to the end of the pass, so
        // it also covers the attempts still in flight when admission stops.
        let _drain = self.database.blob_upload_drain_permit().await;
        self.database
            .validate_store_write_routing(routing_encryption)?;
        let registration = self.database.local_blob_write_authority().await?;
        let authority = coven_protocol::objects::BlobWriteAuthority::new(&registration);
        let uploads = self.database.pending_blob_uploads().await?;
        if uploads.is_empty() {
            return Ok(DrainOutcome::QueueEmpty);
        }

        let now = clock.now();
        let mut count = 0;
        let mut yielded_for_publish = false;

        let eligible = uploads
            .into_iter()
            .map(|entry| {
                crate::blob::retry::entry_in_backoff(&entry, now)
                    .map(|in_backoff| (entry, in_backoff))
            })
            .collect::<Result<Vec<_>, DbError>>()?
            .into_iter()
            .filter_map(|(entry, in_backoff)| (!in_backoff).then_some(entry))
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return Ok(DrainOutcome::AllInBackoff);
        }

        // Run up to `max_concurrent_uploads` uploads at once, admitting in queue order
        // and refilling as each completes. At limit 1 this is the one-at-a-time drain: admit
        // one, await it fully, then the next — same order-observable effects and error
        // semantics. Prepared and Created handoffs are exact compare-and-set updates;
        // the finalizer reads every row-version journal in one transaction and flips
        // only when all are Created.
        let limit = self.database.transfer_limits().uploads.get();
        let mut pending = eligible.into_iter();
        let mut inflight = FuturesUnordered::new();
        // Set once a pause is seen or a make_remote completes: stop admitting new
        // uploads while letting those already in flight finish (never aborting them).
        let mut stop_admitting = false;
        // Entries this pass handed to an upload attempt. Zero means the pause was
        // seen on the very first admission check, since eligible entries exist and
        // nothing else stops admission before one is taken.
        let mut admitted = 0usize;
        let mut failures = Vec::new();

        loop {
            while !stop_admitting && inflight.len() < limit {
                // Host-driven pause: checked before admitting each entry so a freshly
                // paused queue stops admitting without aborting in-flight uploads, and a
                // resume mid-cycle picks back up.
                if let Some(obs) = observer {
                    if obs.should_skip_uploads() {
                        stop_admitting = true;
                        break;
                    }
                }
                let Some(entry) = pending.next() else {
                    break;
                };
                admitted += 1;
                inflight.push(
                    BlobUploadAttempt {
                        writer: self,
                        authority: &authority,
                        routing_encryption,
                        observer,
                        now,
                        entry,
                    }
                    .run(),
                );
            }

            match inflight.next().await {
                Some(EntryOutcome::Uploaded {
                    made_remote,
                    created_this_pass,
                }) => {
                    if created_this_pass {
                        count += 1;
                    }
                    if made_remote {
                        // This upload prepared the make_remote's Store write. Yield so this
                        // cycle publishes and activates it before another root advances; the
                        // uploads already in flight still finish here.
                        yielded_for_publish = true;
                        stop_admitting = true;
                    }
                }
                Some(EntryOutcome::NotUploaded(failure)) => failures.push(failure),
                // Nothing left in flight and none admitted this pass: the drain is done.
                None => break,
            }
        }

        if admitted == 0 {
            return Ok(DrainOutcome::Paused);
        }
        Ok(DrainOutcome::Drained {
            uploaded: count,
            yielded_for_publish,
            failures: UploadFailures::new(failures),
        })
    }

    async fn blob_upload_intent_state(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<Option<coven_database::MakeRemoteIntentState>, DbError> {
        self.database
            .make_remote_intent_state(root_table, root_id)
            .await
    }

    fn store_blob_key_fingerprint(
        &self,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, coven_protocol::objects::StorageError>
    {
        self.storage.store_blob_key_fingerprint()
    }

    async fn prepare_blob_upload(
        &self,
        locator: &BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        source_path: &std::path::Path,
        progress: coven_storage::cloud::PreparationProgress,
    ) -> Result<(StoredBlobRef, std::path::PathBuf), coven_protocol::objects::StorageError> {
        let spool_path = self
            .store_dir
            .outbound_blob_spool_path(locator.locator_hash());
        let spool = self
            .store_dir
            .stage_atomic_file(&spool_path)
            .await
            .map_err(coven_protocol::objects::StorageError::LocalFilesystem)?;
        self.storage
            .seal_store_blob_to_spool(locator, authority, source_path, spool, progress)
            .await?;
        let slot = self.storage.allocate_blob_slot(locator, authority).await?;
        let stored = self
            .storage
            .prepare_blob_object(locator, authority, slot, &spool_path)
            .await?;
        Ok((stored, spool_path))
    }

    async fn mark_blob_upload_prepared(
        &self,
        entry: &OutboxEntry,
        stored: StoredBlobRef,
        spool_path: std::path::PathBuf,
    ) -> Result<(), DbError> {
        self.database
            .mark_blob_upload_prepared(
                entry,
                coven_protocol::audience_package::PackageAudience::Store,
                stored,
                spool_path,
            )
            .await
    }

    async fn create_blob_upload_object(
        &self,
        blob: &StoredBlobRef,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        spool_path: &std::path::Path,
        progress: &coven_storage::cloud::UploadProgress,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage
            .create_blob_object_from_file(blob, authority, spool_path, progress)
            .await
    }

    async fn mark_blob_upload_created(&self, entry: &OutboxEntry) -> Result<(), DbError> {
        self.database.mark_blob_upload_created(entry).await
    }

    /// Drop the staged copy an upload was read from, now that the record says
    /// the provider has the bytes.
    ///
    /// The copy is the whole blob, sealed, sitting beside a library that already
    /// holds the plaintext, and it exists for exactly one reader: the create
    /// that uploads from it, and every resume of that create until one is
    /// durably recorded. Past that nothing reads it — the resume path skips the
    /// create outright, and a cancellation deletes the copy rather than reading
    /// it — so this is where its reason to exist ends.
    ///
    /// A removal that fails is reported and fails the attempt rather than being
    /// swallowed: the entry stays queued, so the next pass finds a record that
    /// already says created, skips straight back to here, and tries the removal
    /// again. Swallowing it is what left a real library holding gigabytes of
    /// copies of things the provider already had.
    async fn retire_blob_upload_spool(
        &self,
        stored: &StoredBlobRef,
    ) -> Result<(), coven_foundation::atomic_file::FileError> {
        self.store_dir
            .remove_outbound_blob_spool(stored.locator().locator_hash())
            .await
    }

    async fn pin_uploaded_blob(
        &self,
        stored: &StoredBlobRef,
        source_path: &std::path::Path,
    ) -> Result<(), coven_foundation::store_dir::StoreBlobFileError> {
        let locator = stored.locator();
        self.store_dir
            .populate_pinned_blob_from_file(
                locator.namespace(),
                locator.locator_hash(),
                locator.plaintext_size(),
                locator.plaintext_hash(),
                source_path,
            )
            .await
    }

    async fn finalize_blob_upload(
        &self,
        entry: &OutboxEntry,
        routing_encryption: Option<EncryptionService>,
    ) -> Result<coven_database::PostUpload, DbError> {
        self.database
            .finalize_created_blob_upload(entry, self.database.stamp(), routing_encryption)
            .await
    }

    async fn cancel_blob_upload(
        &self,
        entry: &OutboxEntry,
        state: &OutboxUploadState,
    ) -> Result<(), UploadFailureCause> {
        match state {
            OutboxUploadState::Pending => {}
            OutboxUploadState::Prepared { stored, .. }
            | OutboxUploadState::Created { stored, .. } => {
                self.storage
                    .delete_blob_object(stored)
                    .await
                    .map_err(UploadFailureCause::Storage)?;
                self.store_dir
                    .remove_outbound_blob_spool(stored.locator().locator_hash())
                    .await
                    .map_err(UploadFailureCause::File)?;
                let locator = stored.locator();
                self.store_dir
                    .remove_cached_locator(locator.namespace(), locator.locator_hash())
                    .await
                    .map_err(UploadFailureCause::CachedRemoval)?;
            }
        }
        self.database
            .finish_cancelled_blob_upload(entry)
            .await
            .map(|_| ())
            .map_err(UploadFailureCause::Database)
    }

    async fn record_outbox_failure(
        &self,
        entry: &OutboxEntry,
        error: &str,
        attempt_time: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DbError> {
        self.database
            .record_outbox_failure(entry, error, &attempt_time.to_rfc3339())
            .await
    }
}

/// What one entry's upload attempt did for the writer drain to aggregate.
enum EntryOutcome {
    /// The cloud write failed; the failure was recorded and the entry left queued.
    /// Not counted toward the drained pass's `uploaded`.
    NotUploaded(UploadFailure),
    /// The cloud write succeeded. It counts toward the drained pass's `uploaded`
    /// only when this pass is the one that created the object.
    /// `made_remote` is true iff the post-upload commit completed a make_remote, so
    /// the drain yields to publish and stops admitting new uploads.
    Uploaded {
        made_remote: bool,
        created_this_pass: bool,
    },
}

impl<'operation, 'storage, 'authority> BlobUploadAttempt<'operation, 'storage, 'authority> {
    async fn run(mut self) -> EntryOutcome {
        let OutboxOperation::Upload {
            root_table,
            root_id,
            row,
            source_path,
            retain_pinned,
            state,
            ..
        } = self.entry.operation.clone()
        else {
            unreachable!("pending_blob_uploads returns only Upload rows");
        };
        match self
            .writer
            .blob_upload_intent_state(&root_table, &root_id)
            .await
        {
            Ok(Some(coven_database::MakeRemoteIntentState::Cancelling)) => {
                return self.finish_cancelled(&state, &row).await;
            }
            Ok(_) => {}
            Err(error) => {
                return self
                    .local_failure(&row, self.label(), UploadFailureCause::Database(error))
                    .await;
            }
        }

        let mut created_this_pass = false;
        let (stored, spool_path) = match state {
            OutboxUploadState::Pending => {
                if let Some(observer) = self.observer {
                    observer.on_blob_preparation_started(&row).await;
                }
                let key_fingerprint = match self.writer.store_blob_key_fingerprint() {
                    Ok(fingerprint) => fingerprint,
                    Err(error) => return self.storage_failure(&row, error).await,
                };
                let locator = match key_fingerprint {
                    Some(key_fingerprint) => BlobLocator::opaque(
                        row.blob().namespace.clone(),
                        row.blob().id.clone(),
                        self.authority.reference.clone(),
                        RemoteAudience::Store,
                        row.blob().scope.clone(),
                        key_fingerprint,
                        row.plaintext_size(),
                        row.plaintext_hash(),
                    ),
                    None => {
                        let Some(cloud_path) = row.blob().cloud_path.clone() else {
                            let message = format!(
                                "Browsable blob {}/{} has no readable path",
                                row.blob().namespace,
                                row.blob().id
                            );
                            return self
                                .local_failure(
                                    &row,
                                    self.label(),
                                    UploadFailureCause::InvalidState(message),
                                )
                                .await;
                        };
                        BlobLocator::browsable(
                            row.blob().namespace.clone(),
                            row.blob().id.clone(),
                            self.authority.reference.clone(),
                            cloud_path,
                            row.plaintext_size(),
                            row.plaintext_hash(),
                        )
                    }
                };
                let locator = match locator {
                    Ok(locator) => locator,
                    Err(error) => {
                        return self
                            .local_failure(&row, self.label(), UploadFailureCause::Locator(error))
                            .await;
                    }
                };
                let (stored, spool_path) = match self
                    .prepare_with_progress(&locator, &source_path, &row)
                    .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => return self.storage_failure(&row, error).await,
                };
                if let Err(error) = self
                    .writer
                    .mark_blob_upload_prepared(&self.entry, stored.clone(), spool_path.clone())
                    .await
                {
                    let object_key = stored.object().slot().logical_key().to_string();
                    return self
                        .local_failure(&row, object_key, UploadFailureCause::Database(error))
                        .await;
                }
                self.set_state(OutboxUploadState::Prepared {
                    authority: coven_protocol::audience_package::PackageAudience::Store,
                    stored: stored.clone(),
                    spool_path: spool_path.clone(),
                });
                (stored, spool_path)
            }
            OutboxUploadState::Prepared {
                authority,
                stored,
                spool_path,
            }
            | OutboxUploadState::Created {
                authority,
                stored,
                spool_path,
            } => {
                if authority != coven_protocol::audience_package::PackageAudience::Store {
                    return self
                        .local_failure(
                            &row,
                            self.label(),
                            UploadFailureCause::InvalidState(
                                "make_remote upload has non-Store package authority".to_string(),
                            ),
                        )
                        .await;
                }
                (stored, spool_path)
            }
        };

        if matches!(
            &self.entry.operation,
            OutboxOperation::Upload {
                state: OutboxUploadState::Prepared { .. },
                ..
            }
        ) {
            if let Some(observer) = self.observer {
                observer.on_blob_upload_started(&row).await;
            }
            if let Err(error) = self.create_with_progress(&stored, &spool_path, &row).await {
                return self.storage_failure(&row, error).await;
            }
            if let Err(error) = self.writer.mark_blob_upload_created(&self.entry).await {
                let object_key = stored.object().slot().logical_key().to_string();
                return self
                    .local_failure(&row, object_key, UploadFailureCause::Database(error))
                    .await;
            }
            self.set_state(OutboxUploadState::Created {
                authority: coven_protocol::audience_package::PackageAudience::Store,
                stored: stored.clone(),
                spool_path: spool_path.clone(),
            });
            created_this_pass = true;
            if let Some(observer) = self.observer {
                observer.on_blob_uploaded(&row).await;
            }
        }

        // The record says the provider has these bytes, whether this pass put
        // them there or an earlier one did, so the staged copy has no reader
        // left.
        if let Err(error) = self.writer.retire_blob_upload_spool(&stored).await {
            let object_key = stored.object().slot().logical_key().to_string();
            return self
                .local_failure(&row, object_key, UploadFailureCause::File(error))
                .await;
        }

        if retain_pinned {
            if let Err(error) = self.writer.pin_uploaded_blob(&stored, &source_path).await {
                let object_key = stored.object().slot().logical_key().to_string();
                return self
                    .local_failure(&row, object_key, UploadFailureCause::Pin(error))
                    .await;
            }
        }

        match self
            .writer
            .finalize_blob_upload(&self.entry, self.routing_encryption.cloned())
            .await
        {
            Ok(coven_database::PostUpload::Waiting) => EntryOutcome::Uploaded {
                made_remote: false,
                created_this_pass,
            },
            Ok(coven_database::PostUpload::MadeRemote {
                root_table: _,
                root_id: _,
            }) => EntryOutcome::Uploaded {
                made_remote: true,
                created_this_pass,
            },
            Ok(coven_database::PostUpload::Cancelled) => {
                self.finish_cancelled(
                    &OutboxUploadState::Created {
                        authority: coven_protocol::audience_package::PackageAudience::Store,
                        stored,
                        spool_path,
                    },
                    &row,
                )
                .await
            }
            Err(error) => {
                let object_key = stored.object().slot().logical_key().to_string();
                self.local_failure(&row, object_key, UploadFailureCause::Database(error))
                    .await
            }
        }
    }

    async fn prepare_with_progress(
        &self,
        locator: &BlobLocator,
        source_path: &std::path::Path,
        upload: &RowBlobRef,
    ) -> Result<(StoredBlobRef, std::path::PathBuf), coven_protocol::objects::StorageError> {
        let total = upload.plaintext_size();
        let mut progress = crate::blob::progress::TransferProgress::new();
        let callback = progress.callback();
        let prepare =
            self.writer
                .prepare_blob_upload(locator, self.authority, source_path, callback);
        let Some(observer) = self.observer else {
            return prepare.await;
        };
        tokio::pin!(prepare);
        let result = loop {
            tokio::select! {
                result = &mut prepare => break result,
                current = progress.changed() => {
                    observer
                        .on_blob_preparation_progress(upload, current, total)
                        .await;
                }
            }
        };
        if result.is_ok() {
            if let Some(total) = progress.finish(total) {
                observer
                    .on_blob_preparation_progress(upload, total, total)
                    .await;
            }
        }
        result
    }

    async fn create_with_progress(
        &self,
        blob: &StoredBlobRef,
        spool_path: &std::path::Path,
        upload: &RowBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        let total = blob.object().stored_size();
        let mut progress = crate::blob::progress::TransferProgress::new();
        let callback = progress.callback();
        let create =
            self.writer
                .create_blob_upload_object(blob, self.authority, spool_path, &callback);
        let Some(observer) = self.observer else {
            return create.await;
        };
        tokio::pin!(create);
        let result = loop {
            tokio::select! {
                result = &mut create => break result,
                current = progress.changed() => {
                    observer
                        .on_blob_upload_progress(upload, current, total)
                        .await;
                }
            }
        };
        if result.is_ok() {
            if let Some(total) = progress.finish(total) {
                observer.on_blob_upload_progress(upload, total, total).await;
            }
        }
        result
    }

    async fn finish_cancelled(
        &self,
        state: &OutboxUploadState,
        upload: &RowBlobRef,
    ) -> EntryOutcome {
        let cleanup = self.writer.cancel_blob_upload(&self.entry, state).await;

        match cleanup {
            Ok(_) => EntryOutcome::Uploaded {
                made_remote: false,
                created_this_pass: false,
            },
            Err(cause) => {
                let message = cause.to_string();
                self.record_failure(upload, &message).await;
                EntryOutcome::NotUploaded(UploadFailure {
                    entry_id: self.entry.id,
                    object_key: self.label(),
                    cause,
                })
            }
        }
    }

    async fn local_failure(
        &self,
        upload: &RowBlobRef,
        object_key: String,
        cause: UploadFailureCause,
    ) -> EntryOutcome {
        let message = cause.to_string();
        self.record_failure(upload, &message).await;
        EntryOutcome::NotUploaded(UploadFailure {
            entry_id: self.entry.id,
            object_key,
            cause,
        })
    }

    async fn storage_failure(
        &self,
        upload: &RowBlobRef,
        error: coven_protocol::objects::StorageError,
    ) -> EntryOutcome {
        let message = error.to_string();
        warn!("Upload failed for {}: {message}", self.label());
        self.record_failure(upload, &message).await;
        EntryOutcome::NotUploaded(UploadFailure {
            entry_id: self.entry.id,
            object_key: self.label(),
            cause: UploadFailureCause::Storage(error),
        })
    }

    async fn record_failure(&self, upload: &RowBlobRef, error: &str) {
        if let Err(record_error) = self
            .writer
            .record_outbox_failure(&self.entry, error, &self.now)
            .await
        {
            warn!(
                "Failed to record upload failure for entry {}: {record_error}",
                self.entry.id
            );
        }
        if let Some(observer) = self.observer {
            observer.on_blob_upload_failed(upload, error).await;
        }
    }

    fn set_state(&mut self, next: OutboxUploadState) {
        let OutboxOperation::Upload { state, .. } = &mut self.entry.operation else {
            unreachable!("upload state can only be set on an upload entry");
        };
        *state = next;
    }

    fn label(&self) -> String {
        match &self.entry.operation {
            OutboxOperation::Upload { row, state, .. } => match state {
                OutboxUploadState::Pending => format!(
                    "{}/{}/{}@{}",
                    row.table(),
                    row.row_id(),
                    row.column(),
                    row.row_stamp()
                ),
                OutboxUploadState::Prepared { stored, .. }
                | OutboxUploadState::Created { stored, .. } => {
                    stored.object().slot().logical_key().to_string()
                }
            },
            OutboxOperation::Delete { stored } => stored.object().slot().logical_key().to_string(),
        }
    }
}
