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
use crate::db::{OutboxEntry, OutboxOperation, OutboxUploadState};
use crate::encryption::EncryptionService;
use crate::store_dir::StoreDir;
use crate::sync::hlc::Hlc;

const PROGRESS_TICK: Duration = Duration::from_millis(300);

async fn create_with_progress(
    storage: &dyn crate::storage::SyncStorage,
    blob: &StoredBlobRef,
    authority: &crate::storage::BlobWriteAuthority<'_>,
    spool_path: &std::path::Path,
    file_id: &str,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<(), crate::storage::StorageError> {
    let total = blob.object().stored_size();
    let sent = Arc::new(AtomicU64::new(0));
    let progress = {
        let sent = sent.clone();
        move |count: u64| sent.store(count, Ordering::Relaxed)
    };
    let create = storage.create_blob_object_from_file(blob, authority, spool_path, &progress);
    let Some(observer) = observer else {
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

/// Record a failed upload attempt and notify the observer. The entry is left
/// queued; it becomes eligible for retry again once its backoff window (see
/// `blob::retry`) has elapsed. Uploads
/// additionally notify the observer, so the caller passes the upload's `file_id`.
async fn record_failure(
    db: &crate::database::StoreDatabase,
    entry: &OutboxEntry,
    file_id: &str,
    error: &str,
    now: chrono::DateTime<chrono::Utc>,
    observer: Option<&dyn BlobTransitionObserver>,
) {
    if let Err(e) = db
        .record_blob_upload_failure(entry, error, &now.to_rfc3339())
        .await
    {
        warn!(
            "Failed to record upload failure for entry {}: {e}",
            entry.id
        );
    }
    if let Some(obs) = observer {
        obs.on_blob_upload_failed(file_id, error).await;
    }
}

/// The result of one upload-queue drain pass.
pub struct DrainOutcome {
    /// Number of successful uploads this pass.
    pub uploaded: usize,
    /// The drain stopped early because an upload just *completed a make_remote*: the
    /// last of a gated root's user-provided blobs landed, so coven flipped the gate true
    /// and broke the drain so this cycle publishes the now-shareable subtree (and
    /// the loop runs the next cycle promptly to drain any other root's blobs).
    /// `false` when the queue drained in one pass (or stopped on a pause / left
    /// only backed-off entries), so the loop waits its normal interval.
    pub yielded_for_publish: bool,
    /// Exact failed queue entries. Provider failures remain typed so the sync
    /// loop can report Offline; local/semantic failures stay per-entry warnings.
    pub failures: UploadFailures,
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

#[derive(Debug, Default)]
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
pub(crate) async fn drain_uploads(
    database: &crate::database::StoreDatabase,
    storage: &dyn crate::storage::SyncStorage,
    authority: crate::storage::BlobWriteAuthority<'_>,
    store_dir: &StoreDir,
    clock: &dyn crate::clock::Clock,
    hlc: &Hlc,
    routing_encryption: Option<&EncryptionService>,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<DrainOutcome, DbError> {
    let db = &database;
    let uploads = db.pending_blob_uploads().await?;

    let now = clock.now();
    let mut count = 0;
    let mut yielded_for_publish = false;

    if uploads.is_empty() {
        return Ok(DrainOutcome {
            uploaded: 0,
            yielded_for_publish: false,
            failures: UploadFailures::default(),
        });
    }
    // Run up to `max_concurrent_uploads` uploads at once, admitting in queue order
    // and refilling as each completes. At limit 1 this is the one-at-a-time drain: admit
    // one, await it fully, then the next — same order-observable effects and error
    // semantics. Prepared and Created handoffs are exact compare-and-set updates;
    // the finalizer reads every row-version journal in one transaction and flips
    // only when all are Created.
    let limit = db.transfer_limits().uploads.get();
    let mut pending = uploads
        .into_iter()
        .map(|entry| {
            crate::blob::retry::entry_in_backoff(&entry, now).map(|in_backoff| (entry, in_backoff))
        })
        .collect::<Result<Vec<_>, DbError>>()?
        .into_iter()
        .filter_map(|(entry, in_backoff)| (!in_backoff).then_some(entry));
    let mut inflight = FuturesUnordered::new();
    // Set once a pause is seen or a make_remote completes: stop admitting new
    // uploads while letting those already in flight finish (never aborting them).
    let mut stop_admitting = false;
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
            inflight.push(upload_entry(
                database,
                storage,
                authority,
                store_dir,
                now,
                hlc,
                routing_encryption.cloned(),
                observer,
                entry,
            ));
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

    Ok(DrainOutcome {
        uploaded: count,
        yielded_for_publish,
        failures: UploadFailures(failures),
    })
}

/// What one entry's upload attempt did, for [`drain_uploads`] to aggregate.
enum EntryOutcome {
    /// The cloud write failed; the failure was recorded and the entry left queued.
    /// Not counted toward [`DrainOutcome::uploaded`].
    NotUploaded(UploadFailure),
    /// The cloud write succeeded (counts toward [`DrainOutcome::uploaded`]).
    /// `made_remote` is true iff the post-upload commit completed a make_remote, so
    /// the drain yields to publish and stops admitting new uploads.
    Uploaded {
        made_remote: bool,
        created_this_pass: bool,
    },
}

#[allow(clippy::too_many_arguments)]
async fn upload_entry(
    database: &crate::database::StoreDatabase,
    storage: &dyn crate::storage::SyncStorage,
    authority: crate::storage::BlobWriteAuthority<'_>,
    store_dir: &StoreDir,
    now: chrono::DateTime<chrono::Utc>,
    hlc: &Hlc,
    routing_encryption: Option<EncryptionService>,
    observer: Option<&dyn BlobTransitionObserver>,
    mut entry: OutboxEntry,
) -> EntryOutcome {
    let db = &database;
    let OutboxOperation::Upload {
        root_table,
        root_id,
        row,
        source_path,
        retain_pinned,
        state,
        ..
    } = entry.operation.clone()
    else {
        unreachable!("get_pending_cloud_uploads returns only Upload rows");
    };
    let file_id = row.blob().id.clone();
    if let Some(observer) = observer {
        observer.on_blob_upload_started(&file_id).await;
    }

    let cancelling = db.make_remote_intent_state(&root_table, &root_id).await;
    match cancelling {
        Ok(Some(crate::database::MakeRemoteIntentState::Cancelling)) => {
            return finish_cancelled_upload(
                db, storage, store_dir, &entry, &state, &file_id, now, observer,
            )
            .await;
        }
        Ok(_) => {}
        Err(error) => {
            let label = upload_label(&entry);
            return local_upload_failed(
                db,
                &entry,
                &file_id,
                now,
                observer,
                label,
                error.to_string(),
            )
            .await;
        }
    }

    let mut created_this_pass = false;
    let (stored, spool_path) = match state {
        OutboxUploadState::Pending => {
            let protection = match storage.store_blob_protection() {
                Ok(protection) => protection,
                Err(error) => {
                    return upload_failed(db, &entry, &file_id, now, observer, error).await;
                }
            };
            let locator = match &protection {
                crate::storage::BlobSpoolProtection::Opaque(encryption) => BlobLocator::opaque(
                    row.blob().namespace.clone(),
                    row.blob().id.clone(),
                    authority.reference.clone(),
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
                        let label = upload_label(&entry);
                        return local_upload_failed(
                            db, &entry, &file_id, now, observer, label, message,
                        )
                        .await;
                    };
                    BlobLocator::browsable(
                        row.blob().namespace.clone(),
                        row.blob().id.clone(),
                        authority.reference.clone(),
                        cloud_path,
                        row.plaintext_size(),
                        row.plaintext_hash(),
                    )
                }
            };
            let locator = match locator {
                Ok(locator) => locator,
                Err(error) => {
                    let label = upload_label(&entry);
                    return local_upload_failed(
                        db,
                        &entry,
                        &file_id,
                        now,
                        observer,
                        label,
                        error.to_string(),
                    )
                    .await;
                }
            };
            let spool_path = store_dir.outbound_blob_spool_path(locator.locator_hash());
            if let Err(error) = storage
                .seal_blob_to_spool(&locator, &authority, protection, &source_path, &spool_path)
                .await
            {
                return upload_failed(db, &entry, &file_id, now, observer, error).await;
            }
            let slot = match storage.allocate_blob_slot(&locator, &authority).await {
                Ok(slot) => slot,
                Err(error) => {
                    return upload_failed(db, &entry, &file_id, now, observer, error).await;
                }
            };
            let stored = match storage
                .prepare_blob_object(&locator, &authority, slot, &spool_path)
                .await
            {
                Ok(stored) => stored,
                Err(error) => {
                    return upload_failed(db, &entry, &file_id, now, observer, error).await;
                }
            };
            if let Err(error) = db
                .mark_blob_upload_prepared(
                    &entry,
                    crate::protocol::audience_package::PackageAudience::Store,
                    stored.clone(),
                    spool_path.clone(),
                )
                .await
            {
                let object_key = stored.object().slot().logical_key().to_string();
                return local_upload_failed(
                    db,
                    &entry,
                    &file_id,
                    now,
                    observer,
                    object_key,
                    error.to_string(),
                )
                .await;
            }
            set_upload_state(
                &mut entry,
                OutboxUploadState::Prepared {
                    authority: crate::protocol::audience_package::PackageAudience::Store,
                    stored: stored.clone(),
                    spool_path: spool_path.clone(),
                },
            );
            (stored, spool_path)
        }
        OutboxUploadState::Prepared {
            authority: package_authority,
            stored,
            spool_path,
        }
        | OutboxUploadState::Created {
            authority: package_authority,
            stored,
            spool_path,
        } => {
            if package_authority != crate::protocol::audience_package::PackageAudience::Store {
                let label = upload_label(&entry);
                return local_upload_failed(
                    db,
                    &entry,
                    &file_id,
                    now,
                    observer,
                    label,
                    "make_remote upload has non-Store package authority".to_string(),
                )
                .await;
            }
            (stored, spool_path)
        }
    };

    if matches!(
        &entry.operation,
        OutboxOperation::Upload {
            state: OutboxUploadState::Prepared { .. },
            ..
        }
    ) {
        if let Err(error) = create_with_progress(
            storage,
            &stored,
            &authority,
            &spool_path,
            &file_id,
            observer,
        )
        .await
        {
            return upload_failed(db, &entry, &file_id, now, observer, error).await;
        }
        if let Err(error) = storage.verify_blob_object(&stored).await {
            return upload_failed(db, &entry, &file_id, now, observer, error).await;
        }
        if let Err(error) = db.mark_blob_upload_created(&entry).await {
            let object_key = stored.object().slot().logical_key().to_string();
            return local_upload_failed(
                db,
                &entry,
                &file_id,
                now,
                observer,
                object_key,
                error.to_string(),
            )
            .await;
        }
        set_upload_state(
            &mut entry,
            OutboxUploadState::Created {
                authority: crate::protocol::audience_package::PackageAudience::Store,
                stored: stored.clone(),
                spool_path: spool_path.clone(),
            },
        );
        created_this_pass = true;
        if let Some(observer) = observer {
            observer.on_blob_uploaded(&file_id).await;
        }
    }

    if retain_pinned {
        if let Err(error) =
            crate::blob::cache::populate_pinned(store_dir, &stored, &source_path).await
        {
            let object_key = stored.object().slot().logical_key().to_string();
            return local_upload_failed(
                db,
                &entry,
                &file_id,
                now,
                observer,
                object_key,
                format!("pin uploaded blob: {error}"),
            )
            .await;
        }
    }

    let stamp = hlc.now().to_string();
    match crate::blob::transition::finalize_created_upload(
        database,
        &entry,
        stamp,
        routing_encryption,
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
            if let Some(observer) = observer {
                observer.on_root_made_remote(&root_table, &root_id).await;
            }
            EntryOutcome::Uploaded {
                made_remote: true,
                created_this_pass,
            }
        }
        Ok(crate::blob::transition::PostUpload::Cancelled) => {
            finish_cancelled_upload(
                db,
                storage,
                store_dir,
                &entry,
                &OutboxUploadState::Created {
                    authority: crate::protocol::audience_package::PackageAudience::Store,
                    stored,
                    spool_path,
                },
                &file_id,
                now,
                observer,
            )
            .await
        }
        Err(error) => {
            let object_key = stored.object().slot().logical_key().to_string();
            local_upload_failed(
                db,
                &entry,
                &file_id,
                now,
                observer,
                object_key,
                error.to_string(),
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_cancelled_upload(
    db: &crate::database::StoreDatabase,
    storage: &dyn crate::storage::SyncStorage,
    store_dir: &StoreDir,
    entry: &OutboxEntry,
    state: &OutboxUploadState,
    file_id: &str,
    now: chrono::DateTime<chrono::Utc>,
    observer: Option<&dyn BlobTransitionObserver>,
) -> EntryOutcome {
    let cleanup = async {
        match state {
            OutboxUploadState::Pending => {}
            OutboxUploadState::Prepared {
                stored, spool_path, ..
            }
            | OutboxUploadState::Created {
                stored, spool_path, ..
            } => {
                storage
                    .delete_blob_object(stored)
                    .await
                    .map_err(UploadFailureCause::Storage)?;
                remove_spool(spool_path)
                    .await
                    .map_err(UploadFailureCause::Local)?;
                crate::blob::cache::drop_cached_stored_blob(store_dir, stored)
                    .await
                    .map_err(|error| {
                        UploadFailureCause::Local(format!("drop cancelled cache copy: {error}"))
                    })?;
            }
        }
        db.finish_cancelled_blob_upload(entry)
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
            record_failure(db, entry, file_id, &message, now, observer).await;
            EntryOutcome::NotUploaded(UploadFailure {
                entry_id: entry.id,
                object_key: upload_label(entry),
                cause,
            })
        }
    }
}

fn set_upload_state(entry: &mut OutboxEntry, next: OutboxUploadState) {
    let OutboxOperation::Upload { state, .. } = &mut entry.operation else {
        unreachable!("upload state can only be set on an upload entry");
    };
    *state = next;
}

fn upload_label(entry: &OutboxEntry) -> String {
    match &entry.operation {
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

/// Record a local (non-storage) failure against the entry and report it as
/// not-uploaded. The sibling of [`upload_failed`] for failures minted on this
/// device — a missing readable path, a database write, a pin — where there is
/// no [`StorageError`](crate::storage::StorageError) to carry.
async fn local_upload_failed(
    db: &crate::database::StoreDatabase,
    entry: &OutboxEntry,
    file_id: &str,
    now: chrono::DateTime<chrono::Utc>,
    observer: Option<&dyn BlobTransitionObserver>,
    object_key: String,
    message: String,
) -> EntryOutcome {
    record_failure(db, entry, file_id, &message, now, observer).await;
    EntryOutcome::NotUploaded(UploadFailure {
        entry_id: entry.id,
        object_key,
        cause: UploadFailureCause::Local(message),
    })
}

async fn upload_failed(
    db: &crate::database::StoreDatabase,
    entry: &OutboxEntry,
    file_id: &str,
    now: chrono::DateTime<chrono::Utc>,
    observer: Option<&dyn BlobTransitionObserver>,
    error: crate::storage::StorageError,
) -> EntryOutcome {
    let message = error.to_string();
    warn!("Upload failed for {}: {message}", upload_label(entry));
    record_failure(db, entry, file_id, &message, now, observer).await;
    EntryOutcome::NotUploaded(UploadFailure {
        entry_id: entry.id,
        object_key: upload_label(entry),
        cause: UploadFailureCause::Storage(error),
    })
}

async fn remove_spool(path: &std::path::Path) -> Result<(), String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => crate::local_blob::sync_parent_dir(path).await,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "remove exact blob spool {}: {error}",
            path.display()
        )),
    }
}
