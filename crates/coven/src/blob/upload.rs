//! The upload half of the blob engine. Each exact Local row-version journal moves
//! through Pending → Prepared → Created. Prepared durably binds its locator,
//! allocated object, and immutable spool; Created records that exact cloud bytes
//! were created and verified. The transition finalizer flips a root only when all
//! of its current row journals are Created.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tracing::warn;

use crate::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
use crate::blob::BlobTransitionObserver;
use crate::database::DbError;
use crate::database::{OutboxEntry, OutboxOperation, OutboxUploadState};
use crate::encryption::EncryptionService;
use crate::store_dir::StoreDir;

const PROGRESS_TICK: Duration = Duration::from_millis(300);

/// What one upload-queue drain pass did.
///
/// A pass that uploads nothing does so for one of four unlike reasons, and the
/// variant names which: the queue was empty, every entry was still inside its
/// retry backoff, the host had uploads paused, or entries were attempted and
/// none produced a new cloud object. Only [`Drained`](Self::Drained) carries a
/// count, so "nothing happened" can never be read as "the work is done".
#[derive(Debug)]
pub enum DrainOutcome {
    /// At least one queued entry was attempted.
    Drained {
        /// Cloud objects this pass created.
        ///
        /// Zero is a real answer here: an entry another pass had already created
        /// but not finished — a drain that died between the cloud write and its
        /// durable finalization — is finished by this pass and leaves the queue
        /// without a new object being written, so it is not counted. The count
        /// reports objects newly written to the cloud, not entries retired.
        uploaded: usize,
        /// The drain stopped early because an upload just *completed a make_remote*: the
        /// last of a gated root's user-provided blobs landed, so coven flipped the gate true
        /// and broke the drain so this cycle publishes the now-shareable subtree (and
        /// the loop runs the next cycle promptly to drain any other root's blobs).
        /// `false` when the queue drained in one pass, so the loop waits its
        /// normal interval.
        yielded_for_publish: bool,
        /// Exact failed queue entries. Provider failures remain typed so the sync
        /// loop can report Offline; local/semantic failures stay per-entry warnings.
        failures: UploadFailures,
    },
    /// The queue held no entries at all.
    QueueEmpty,
    /// The queue held entries and every one of them is still inside its retry
    /// backoff window, so none was attempted.
    AllInBackoff,
    /// The host's observer has uploads paused, so nothing was admitted. Entries
    /// eligible to run are still queued and the next pass after a resume takes
    /// them.
    Paused,
}

/// Readers for a test that planted work and expects the pass to have attempted
/// it. Each panics on any other disposition, so a drain that found an empty
/// queue — the shape a lost race produces — fails the test where it happened
/// instead of quietly reading as a zero count.
#[cfg(test)]
impl DrainOutcome {
    #[track_caller]
    fn drained(&self) -> (usize, bool, &UploadFailures) {
        match self {
            Self::Drained {
                uploaded,
                yielded_for_publish,
                failures,
            } => (*uploaded, *yielded_for_publish, failures),
            other => panic!("expected a drain that attempted queued entries, got {other:?}"),
        }
    }

    #[track_caller]
    pub(crate) fn uploaded(&self) -> usize {
        self.drained().0
    }

    #[track_caller]
    pub(crate) fn yielded_for_publish(&self) -> bool {
        self.drained().1
    }

    #[track_caller]
    pub(crate) fn failures(&self) -> &UploadFailures {
        self.drained().2
    }

    #[track_caller]
    pub(crate) fn into_failures(self) -> UploadFailures {
        match self {
            Self::Drained { failures, .. } => failures,
            other => panic!("expected a drain that attempted queued entries, got {other:?}"),
        }
    }
}

#[derive(Debug)]
pub enum UploadFailureCause {
    Local(String),
    Storage(crate::storage::StorageError),
}

impl std::fmt::Display for UploadFailureCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(reason) => write!(formatter, "local upload source: {reason}"),
            Self::Storage(error) => write!(formatter, "blob storage: {error}"),
        }
    }
}

#[derive(Debug)]
pub struct UploadFailure {
    pub entry_id: i64,
    pub object_key: String,
    pub cause: UploadFailureCause,
}

#[derive(Debug)]
pub struct UploadFailures(Vec<UploadFailure>);

impl UploadFailures {
    pub fn failures(&self) -> &[UploadFailure] {
        &self.0
    }

    pub fn has_transport_failure(&self) -> bool {
        self.0.iter().any(|failure| {
            matches!(&failure.cause, UploadFailureCause::Storage(error) if error.is_transport())
        })
    }
}

impl std::fmt::Display for UploadFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} blob upload(s) failed", self.0.len())?;
        for failure in &self.0 {
            write!(
                formatter,
                "; entry {} {}: {}",
                failure.entry_id, failure.object_key, failure.cause
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for UploadFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.iter().find_map(|failure| match &failure.cause {
            UploadFailureCause::Storage(error) if error.is_transport() => {
                Some(error as &(dyn std::error::Error + 'static))
            }
            _ => None,
        })
    }
}

struct BlobUploadAttempt<'queue, 'operation, 'authority> {
    queue: &'queue BlobUploadQueue<'operation, 'authority>,
    now: chrono::DateTime<chrono::Utc>,
    entry: OutboxEntry,
}

pub(crate) struct BlobUploadQueue<'operation, 'authority> {
    database: &'operation crate::database::StoreDatabase,
    storage: &'operation dyn crate::storage::SyncStorage,
    authority: crate::storage::BlobWriteAuthority<'authority>,
    store_dir: &'operation StoreDir,
    clock: &'operation dyn crate::clock::Clock,
    routing_encryption: Option<&'operation EncryptionService>,
    observer: Option<&'operation dyn BlobTransitionObserver>,
}

impl<'operation, 'authority> BlobUploadQueue<'operation, 'authority> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        database: &'operation crate::database::StoreDatabase,
        storage: &'operation dyn crate::storage::SyncStorage,
        authority: crate::storage::BlobWriteAuthority<'authority>,
        store_dir: &'operation StoreDir,
        clock: &'operation dyn crate::clock::Clock,
        routing_encryption: Option<&'operation EncryptionService>,
        observer: Option<&'operation dyn BlobTransitionObserver>,
    ) -> Self {
        Self {
            database,
            storage,
            authority,
            store_dir,
            clock,
            routing_encryption,
            observer,
        }
    }

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
    pub(crate) async fn drain(&self) -> Result<DrainOutcome, DbError> {
        let uploads = self.database.pending_blob_uploads().await?;
        if uploads.is_empty() {
            return Ok(DrainOutcome::QueueEmpty);
        }

        let now = self.clock.now();
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
                if let Some(obs) = self.observer {
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
                        queue: self,
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
                        // This upload completed a make_remote: the gate is flipped and the
                        // subtree is shareable. Yield so this cycle publishes it, and stop
                        // admitting new uploads — any other root's blobs drain on the
                        // promptly-run next cycle, and the in-flight uploads finish here.
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
            failures: UploadFailures(failures),
        })
    }
}

/// What one entry's upload attempt did, for [`BlobUploadQueue::drain`] to aggregate.
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

impl<'queue, 'operation, 'authority> BlobUploadAttempt<'queue, 'operation, 'authority> {
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
            unreachable!("get_pending_cloud_uploads returns only Upload rows");
        };
        let file_id = row.blob().id.clone();
        if let Some(observer) = self.queue.observer {
            observer.on_blob_upload_started(&file_id).await;
        }

        match self
            .queue
            .database
            .make_remote_intent_state(&root_table, &root_id)
            .await
        {
            Ok(Some(crate::database::MakeRemoteIntentState::Cancelling)) => {
                return self.finish_cancelled(&state, &file_id).await;
            }
            Ok(_) => {}
            Err(error) => {
                return self
                    .local_failure(&file_id, self.label(), error.to_string())
                    .await;
            }
        }

        let mut created_this_pass = false;
        let (stored, spool_path) = match state {
            OutboxUploadState::Pending => {
                let protection = match self.queue.storage.store_blob_protection() {
                    Ok(protection) => protection,
                    Err(error) => return self.storage_failure(&file_id, error).await,
                };
                let locator = match &protection {
                    crate::storage::BlobSpoolProtection::Opaque(encryption) => BlobLocator::opaque(
                        row.blob().namespace.clone(),
                        row.blob().id.clone(),
                        self.queue.authority.reference.clone(),
                        RemoteAudience::Store,
                        row.blob().scope.clone(),
                        encryption.seal_key_fingerprint(),
                        row.plaintext_size(),
                        row.plaintext_hash(),
                    ),
                    crate::storage::BlobSpoolProtection::Browsable => {
                        let Some(cloud_path) = row.blob().cloud_path.clone() else {
                            let message = format!(
                                "Browsable blob {}/{} has no readable path",
                                row.blob().namespace,
                                row.blob().id
                            );
                            return self.local_failure(&file_id, self.label(), message).await;
                        };
                        BlobLocator::browsable(
                            row.blob().namespace.clone(),
                            row.blob().id.clone(),
                            self.queue.authority.reference.clone(),
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
                            .local_failure(&file_id, self.label(), error.to_string())
                            .await;
                    }
                };
                let spool_path = self
                    .queue
                    .store_dir
                    .outbound_blob_spool_path(locator.locator_hash());
                if let Err(error) = self
                    .queue
                    .storage
                    .seal_blob_to_spool(
                        &locator,
                        &self.queue.authority,
                        protection,
                        &source_path,
                        &spool_path,
                    )
                    .await
                {
                    return self.storage_failure(&file_id, error).await;
                }
                let slot = match self
                    .queue
                    .storage
                    .allocate_blob_slot(&locator, &self.queue.authority)
                    .await
                {
                    Ok(slot) => slot,
                    Err(error) => return self.storage_failure(&file_id, error).await,
                };
                let stored = match self
                    .queue
                    .storage
                    .prepare_blob_object(&locator, &self.queue.authority, slot, &spool_path)
                    .await
                {
                    Ok(stored) => stored,
                    Err(error) => return self.storage_failure(&file_id, error).await,
                };
                if let Err(error) = self
                    .queue
                    .database
                    .mark_blob_upload_prepared(
                        &self.entry,
                        crate::protocol::audience_package::PackageAudience::Store,
                        stored.clone(),
                        spool_path.clone(),
                    )
                    .await
                {
                    let object_key = stored.object().slot().logical_key().to_string();
                    return self
                        .local_failure(&file_id, object_key, error.to_string())
                        .await;
                }
                self.set_state(OutboxUploadState::Prepared {
                    authority: crate::protocol::audience_package::PackageAudience::Store,
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
                if authority != crate::protocol::audience_package::PackageAudience::Store {
                    return self
                        .local_failure(
                            &file_id,
                            self.label(),
                            "make_remote upload has non-Store package authority".to_string(),
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
            if let Err(error) = self
                .create_with_progress(&stored, &spool_path, &file_id)
                .await
            {
                return self.storage_failure(&file_id, error).await;
            }
            if let Err(error) = self.queue.storage.verify_blob_object(&stored).await {
                return self.storage_failure(&file_id, error).await;
            }
            if let Err(error) = self
                .queue
                .database
                .mark_blob_upload_created(&self.entry)
                .await
            {
                let object_key = stored.object().slot().logical_key().to_string();
                return self
                    .local_failure(&file_id, object_key, error.to_string())
                    .await;
            }
            self.set_state(OutboxUploadState::Created {
                authority: crate::protocol::audience_package::PackageAudience::Store,
                stored: stored.clone(),
                spool_path: spool_path.clone(),
            });
            created_this_pass = true;
            if let Some(observer) = self.queue.observer {
                observer.on_blob_uploaded(&file_id).await;
            }
        }

        if retain_pinned {
            let locator = stored.locator();
            if let Err(error) = self
                .queue
                .store_dir
                .populate_pinned_blob_from_file(
                    locator.namespace(),
                    locator.locator_hash(),
                    locator.plaintext_size(),
                    locator.plaintext_hash(),
                    &source_path,
                )
                .await
            {
                let object_key = stored.object().slot().logical_key().to_string();
                return self
                    .local_failure(&file_id, object_key, format!("pin uploaded blob: {error}"))
                    .await;
            }
        }

        let stamp = self.queue.database.stamp();
        match self
            .queue
            .database
            .finalize_created_blob_upload(
                &self.entry,
                stamp,
                self.queue.routing_encryption.cloned(),
            )
            .await
        {
            Ok(crate::blob::transition::PostUpload::Waiting) => EntryOutcome::Uploaded {
                made_remote: false,
                created_this_pass,
            },
            Ok(crate::blob::transition::PostUpload::MadeRemote {
                root_table,
                root_id,
            }) => {
                if let Some(observer) = self.queue.observer {
                    observer.on_root_made_remote(&root_table, &root_id).await;
                }
                EntryOutcome::Uploaded {
                    made_remote: true,
                    created_this_pass,
                }
            }
            Ok(crate::blob::transition::PostUpload::Cancelled) => {
                self.finish_cancelled(
                    &OutboxUploadState::Created {
                        authority: crate::protocol::audience_package::PackageAudience::Store,
                        stored,
                        spool_path,
                    },
                    &file_id,
                )
                .await
            }
            Err(error) => {
                let object_key = stored.object().slot().logical_key().to_string();
                self.local_failure(&file_id, object_key, error.to_string())
                    .await
            }
        }
    }

    async fn create_with_progress(
        &self,
        blob: &StoredBlobRef,
        spool_path: &std::path::Path,
        file_id: &str,
    ) -> Result<(), crate::storage::StorageError> {
        let total = blob.object().stored_size();
        let sent = Arc::new(AtomicU64::new(0));
        let progress = {
            let sent = sent.clone();
            move |count: u64| sent.store(count, Ordering::Relaxed)
        };
        let create = self.queue.storage.create_blob_object_from_file(
            blob,
            &self.queue.authority,
            spool_path,
            &progress,
        );
        let Some(observer) = self.queue.observer else {
            return create.await;
        };
        tokio::pin!(create);
        let mut ticker = tokio::time::interval(PROGRESS_TICK);
        ticker.tick().await;
        let mut forwarded = 0;
        let result = loop {
            tokio::select! {
                result = &mut create => break result,
                _ = ticker.tick() => {
                    let current = sent.load(Ordering::Relaxed);
                    if current != forwarded {
                        forwarded = current;
                        observer.on_blob_upload_progress(file_id, current, total).await;
                    }
                }
            }
        };
        if result.is_ok() {
            observer
                .on_blob_upload_progress(file_id, total, total)
                .await;
        }
        result
    }

    async fn finish_cancelled(&self, state: &OutboxUploadState, file_id: &str) -> EntryOutcome {
        let cleanup = async {
            match state {
                OutboxUploadState::Pending => {}
                OutboxUploadState::Prepared { stored, .. }
                | OutboxUploadState::Created { stored, .. } => {
                    self.queue
                        .storage
                        .delete_blob_object(stored)
                        .await
                        .map_err(UploadFailureCause::Storage)?;
                    self.queue
                        .store_dir
                        .remove_outbound_blob_spool(stored.locator().locator_hash())
                        .await
                        .map_err(UploadFailureCause::Local)?;
                    let locator = stored.locator();
                    self.queue
                        .store_dir
                        .remove_cached_locator(locator.namespace(), locator.locator_hash())
                        .await
                        .map_err(|error| {
                            UploadFailureCause::Local(format!("drop cancelled cache copy: {error}"))
                        })?;
                }
            }
            self.queue
                .database
                .finish_cancelled_blob_upload(&self.entry)
                .await
                .map_err(|error| UploadFailureCause::Local(error.to_string()))
        }
        .await;

        match cleanup {
            Ok(_) => EntryOutcome::Uploaded {
                made_remote: false,
                created_this_pass: false,
            },
            Err(cause) => {
                let message = cause.to_string();
                self.record_failure(file_id, &message).await;
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
        file_id: &str,
        object_key: String,
        message: String,
    ) -> EntryOutcome {
        self.record_failure(file_id, &message).await;
        EntryOutcome::NotUploaded(UploadFailure {
            entry_id: self.entry.id,
            object_key,
            cause: UploadFailureCause::Local(message),
        })
    }

    async fn storage_failure(
        &self,
        file_id: &str,
        error: crate::storage::StorageError,
    ) -> EntryOutcome {
        let message = error.to_string();
        warn!("Upload failed for {}: {message}", self.label());
        self.record_failure(file_id, &message).await;
        EntryOutcome::NotUploaded(UploadFailure {
            entry_id: self.entry.id,
            object_key: self.label(),
            cause: UploadFailureCause::Storage(error),
        })
    }

    async fn record_failure(&self, file_id: &str, error: &str) {
        if let Err(record_error) = self
            .queue
            .database
            .record_blob_upload_failure(&self.entry, error, &self.now.to_rfc3339())
            .await
        {
            warn!(
                "Failed to record upload failure for entry {}: {record_error}",
                self.entry.id
            );
        }
        if let Some(observer) = self.queue.observer {
            observer.on_blob_upload_failed(file_id, error).await;
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
