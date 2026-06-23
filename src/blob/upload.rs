//! The upload half of the blob engine: drain the durable upload queue, sealing
//! each blob under its scope and writing it to the cloud with coalesced progress.
//!
//! The engine owns a blob's whole local-durability lifecycle. [`cache`](super::cache)
//! is the device-local half (bytes on disk, pin/unpin, eviction); this is the
//! cloud half (local-only → uploaded). The host stages a blob (writing it locally
//! and enqueuing an upload row on `(operation, cloud_key)`), and the sync cycle
//! calls [`drain_uploads`] each round before it pushes: the drain reads each
//! pending row, seals the local plaintext under the blob's scope, and writes it to
//! the cloud.
//!
//! The queue persists the **final cloud object key** the host built at enqueue
//! (the `cloud_key` column), not the `(namespace, id, cloud_path)` a [`BlobRef`]
//! carries, so the drain replays that key verbatim through
//! [`CloudHome::write`](crate::storage::cloud::CloudHome::write) rather than
//! re-deriving it through [`SyncStorage::put_blob`](crate::sync::storage::SyncStorage::put_blob).
//! That write is also the only seam that threads the chunked `progress` closure
//! the per-file progress bar needs — `put_blob` discards progress. So the upload
//! seals with the same [`CloudCipher::seal_scoped`] the storage layer uses (the
//! one sealing point both directions share, via
//! [`encryption_for_scope`](crate::sync::cloud_storage::encryption_for_scope)),
//! but writes through `CloudHome` directly. The read path
//! ([`cache::read_blob`](super::cache::read_blob)) goes through `SyncStorage`
//! because it has the `BlobRef` and resolves coordinates to a key per call; the
//! upload path replays a key already committed to the durable queue.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::blob::{BlobScope, BlobUploadObserver, DrainControl};
use crate::database::Database;
use crate::db::{OutboxEntry, OutboxOperation};
use crate::storage::cloud::{CloudHome, CloudHomeError};
use crate::sync::cloud_storage::CloudCipher;

/// How often the upload pipeline forwards a mid-file byte count to the observer.
/// coven's `write` reports per chunk (every few MiB), which on a fast link can be
/// many times a second; coalescing to this interval keeps the host from
/// rebuilding its outbox snapshot on every chunk while still moving the bar
/// smoothly. Modeled on the torrent download session's fixed progress tick.
const PROGRESS_TICK: Duration = Duration::from_millis(300);

/// Run one blob upload while forwarding coalesced byte progress to the observer.
///
/// coven's `CloudHome::write` reports cumulative bytes through a synchronous
/// `progress` closure as each chunk lands. That closure can't await, so it just
/// stores the latest count in `sent`; a concurrent ticker reads `sent` every
/// [`PROGRESS_TICK`] and makes the async `on_blob_upload_progress` call. The
/// ticker stops as soon as `write` returns, then a final forward emits the
/// terminal count so the bar reaches 100% even if the last chunk landed between
/// ticks. With no observer the ticker is skipped entirely.
async fn upload_with_progress(
    cloud_home: &dyn CloudHome,
    cloud_key: &str,
    file_id: &str,
    data: Vec<u8>,
    observer: Option<&dyn BlobUploadObserver>,
) -> Result<(), CloudHomeError> {
    let total = data.len() as u64;
    let sent = Arc::new(AtomicU64::new(0));
    let progress = {
        let sent = sent.clone();
        move |n: u64| sent.store(n, Ordering::Relaxed)
    };

    let write = cloud_home.write(cloud_key, data, &progress);

    let Some(obs) = observer else {
        return write.await;
    };

    // Forward `sent` to the observer on a fixed tick until the write completes,
    // skipping ticks where nothing advanced. Runs concurrently with the write
    // on the same task; both borrow `obs` and `sent`.
    tokio::pin!(write);
    let mut ticker = tokio::time::interval(PROGRESS_TICK);
    ticker.tick().await; // first tick fires immediately; consume it.
    let mut last_forwarded = 0u64;
    let result = loop {
        tokio::select! {
            r = &mut write => break r,
            _ = ticker.tick() => {
                let now = sent.load(Ordering::Relaxed);
                if now != last_forwarded {
                    last_forwarded = now;
                    obs.on_blob_upload_progress(file_id, now, total).await;
                }
            }
        }
    };

    // Terminal forward: on success the file is fully uploaded, so report the
    // full size even if the last chunk landed between ticks. On failure leave
    // the last observed count — the entry stays queued and will retry.
    if result.is_ok() {
        obs.on_blob_upload_progress(file_id, total, total).await;
    }
    result
}

/// Minimum delay before a failed upload entry is retried, keyed on its prior
/// `attempt_count`. Exponential (`30s · 2^(n-1)`) capped at one hour: the base
/// equals the sync-loop interval so the first retry rides the next natural
/// cycle, and the cap keeps a persistently-failing entry retrying hourly rather
/// than every cycle. A freshly-queued entry (`attempt_count == 0`) is eligible
/// immediately.
pub(crate) fn backoff_window(attempt_count: i64) -> chrono::Duration {
    if attempt_count <= 0 {
        return chrono::Duration::zero();
    }
    let n = (attempt_count - 1) as u32;
    chrono::Duration::seconds(crate::sync::backoff::backoff_secs(n, 3600) as i64)
}

/// Record a failed upload attempt and notify the observer. The entry is left
/// queued; it becomes eligible for retry again after [`backoff_window`]. Only
/// uploads fail this way (a delete failure just retries next cycle), so the
/// caller passes the upload's `file_id` for the observer notification.
async fn record_failure(
    db: &Database,
    entry: &OutboxEntry,
    file_id: &str,
    error: &str,
    now: chrono::DateTime<chrono::Utc>,
    observer: Option<&dyn BlobUploadObserver>,
) {
    if let Err(e) = db
        .record_cloud_upload_failure(entry.id, error, &now.to_rfc3339())
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

/// Read an upload's local plaintext, resolve its scope to a key, and seal it for
/// storage (encrypting under the scope's key for an encrypted home, or storing
/// it verbatim for a plaintext one).
///
/// The two failure modes — the local file can't be read, the scope can't be
/// resolved to a key (a missing `item_keys` row) — both surface as an `Err`
/// carrying a host-readable message, so the upload loop has one failure path
/// (warn + record + skip) instead of one per step. The scope is resolved at
/// drain (not enqueue) because the key may be minted/synced after the blob was
/// queued, and an `Item` scope reads `item_keys` here, holding `db`.
async fn resolve_and_seal(
    db: &Database,
    cipher: &std::sync::RwLock<CloudCipher>,
    file_path: &Path,
    scope: BlobScope,
) -> Result<Vec<u8>, String> {
    let data = crate::local_blob::read(file_path).await?;
    let resolved = db
        .resolve_blob_scope(scope)
        .await
        .map_err(|e| format!("cannot resolve blob scope: {e}"))?;
    Ok(cipher.read().unwrap().seal_scoped(resolved, &data))
}

/// The result of one upload-queue drain pass.
pub struct DrainOutcome {
    /// Number of successful uploads this pass.
    pub uploaded: usize,
    /// The drain stopped early because [`BlobUploadObserver::on_blob_uploaded`]
    /// returned [`DrainControl::Publish`] — the host made new rows shareable, so
    /// the cycle should publish them and the loop should run the next cycle
    /// promptly to drain the rest. `false` when the queue drained in one pass
    /// (or stopped on a pause / left only backed-off entries), so the loop waits
    /// its normal interval.
    pub yielded_for_publish: bool,
}

/// Drain pending blob uploads: read each local file, seal it under its scope,
/// write it to the cloud.
///
/// A failing entry is recorded and skipped rather than stopping the drain, with a
/// per-entry backoff so a persistently-failing entry doesn't block the rest of
/// the queue or get re-attempted every cycle. The `observer` (if any) is notified
/// as each attempt starts, succeeds, or fails.
///
/// The drain stops early when [`BlobUploadObserver::on_blob_uploaded`] returns
/// [`DrainControl::Publish`] — the host signals it just made new rows shareable,
/// so the cycle should publish them before draining the rest (the remaining
/// entries stay queued). The returned [`DrainOutcome`] carries that signal.
/// Without an observer, or while it returns [`DrainControl::Continue`], the queue
/// drains in one pass.
pub async fn drain_uploads(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    library_dir: &Path,
    clock: &dyn crate::clock::Clock,
    observer: Option<&dyn BlobUploadObserver>,
) -> Result<DrainOutcome, String> {
    let uploads = db
        .get_pending_cloud_uploads()
        .await
        .map_err(|e| format!("Failed to get pending uploads: {e}"))?;

    let now = clock.now();
    let mut count = 0;
    let mut yielded_for_publish = false;
    for entry in uploads {
        // Host-driven pause: short-circuit before pulling the next entry so a
        // freshly paused queue stops draining without aborting an in-flight
        // upload. Checked per entry so resume mid-cycle picks back up.
        if let Some(obs) = observer {
            if obs.should_skip_uploads() {
                break;
            }
        }
        // Per-entry backoff: skip an entry still inside its retry window so a
        // poisoned entry isn't re-attempted every cycle.
        if let Some(last) = entry.last_attempt_at.as_deref() {
            match chrono::DateTime::parse_from_rfc3339(last) {
                Ok(last_dt) => {
                    let elapsed = now.signed_duration_since(last_dt.with_timezone(&chrono::Utc));
                    if elapsed < backoff_window(entry.attempt_count) {
                        continue;
                    }
                }
                Err(e) => {
                    // Don't strand an entry on a corrupt timestamp — log and retry.
                    warn!(
                        "Outbox entry {} has unparseable last_attempt_at {last:?}: {e}; retrying",
                        entry.id
                    );
                }
            }
        }

        // Every row from `get_pending_cloud_uploads` is an `Upload` (the query
        // filters `operation = 'upload'`); destructure the upload-only fields. A
        // `Delete` here would be a broken query invariant, not a skippable row.
        let OutboxOperation::Upload {
            file_id,
            source_path,
            scope,
        } = &entry.operation
        else {
            unreachable!("get_pending_cloud_uploads returns only Upload rows");
        };

        if let Some(obs) = observer {
            obs.on_blob_upload_started(file_id).await;
        }

        let file_path = match source_path {
            Some(p) => std::path::PathBuf::from(p),
            None => match crate::storage::local::storage_path(file_id) {
                Ok(rel) => library_dir.join(rel),
                Err(e) => {
                    // A locally-enqueued upload id that can't form a storage path is
                    // a host bug, not attacker data; record it as this entry's
                    // failure and keep draining the rest rather than aborting.
                    let msg = format!("invalid upload file id: {e}");
                    warn!(
                        "Upload failed for {} (file_id {file_id}): {msg}",
                        entry.cloud_key
                    );
                    record_failure(db, &entry, file_id, &msg, now, observer).await;
                    continue;
                }
            },
        };

        // Read the local plaintext, resolve the scope to a key (an `Item` scope
        // reads `item_keys` here, holding `db`), and seal it for storage — all in
        // one step with a single failure path. A missing key is a host bug;
        // record it as a failure rather than silently sealing under the master
        // key (which no share recipient could read).
        let sealed = match resolve_and_seal(db, cipher, &file_path, scope.clone()).await {
            Ok(bytes) => bytes,
            Err(msg) => {
                warn!("Upload failed for {}: {msg}", entry.cloud_key);
                record_failure(db, &entry, file_id, &msg, now, observer).await;
                continue;
            }
        };

        match upload_with_progress(cloud_home, &entry.cloud_key, file_id, sealed, observer).await {
            Ok(()) => {
                if let Err(e) = db.remove_cloud_outbox_entry(entry.id).await {
                    warn!("Failed to remove outbox entry {}: {e}", entry.id);
                }
                count += 1;

                if let Some(obs) = observer {
                    if obs.on_blob_uploaded(file_id).await == DrainControl::Publish {
                        // The host just made new rows shareable (e.g. flipped a
                        // gate column on). Stop draining so this cycle publishes
                        // them now; the entries still queued drain on the next
                        // cycle, which the loop runs promptly.
                        yielded_for_publish = true;
                        break;
                    }
                }
            }
            Err(e) => {
                let msg = format!("cloud write failed: {e}");
                warn!("Upload failed for {}: {msg}", entry.cloud_key);
                record_failure(db, &entry, file_id, &msg, now, observer).await;
                continue;
            }
        }
    }

    Ok(DrainOutcome {
        uploaded: count,
        yielded_for_publish,
    })
}
