//! Cloud outbox: async upload/delete of encrypted blobs.
//!
//! The host enqueues upload entries (after writing a blob locally) and delete
//! entries (tagged with the current sync seq). The sync cycle processes the
//! outbox: uploads before push, deletes after pull.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::blob::BlobUploadObserver;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::storage::cloud::CloudHome;

/// How often the upload pipeline forwards a mid-file byte count to the
/// observer. coven's `write` reports per chunk (every few MiB), which on a fast
/// link can be many times a second; coalescing to this interval keeps the host
/// from rebuilding its outbox snapshot on every chunk while still moving the
/// bar smoothly. Modeled on the torrent download session's fixed progress tick.
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
) -> Result<(), crate::storage::cloud::CloudHomeError> {
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
pub(super) fn backoff_window(attempt_count: i64) -> chrono::Duration {
    if attempt_count <= 0 {
        return chrono::Duration::zero();
    }
    let n = (attempt_count - 1) as u32;
    chrono::Duration::seconds(super::backoff::backoff_secs(n, 3600) as i64)
}

/// Record a failed upload attempt and notify the observer. The entry is left
/// queued; it becomes eligible for retry again after [`backoff_window`].
async fn record_failure(
    db: &Database,
    entry: &crate::db::OutboxEntry,
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
        obs.on_blob_upload_failed(&entry.file_id, error).await;
    }
}

/// Process pending uploads: read local file, encrypt, write to cloud.
///
/// Returns the number of successful uploads. A failing entry is recorded and
/// skipped rather than stopping the drain, with a per-entry backoff so a
/// persistently-failing entry doesn't block the rest of the queue or get
/// re-attempted every cycle. The `observer` (if any) is notified as each
/// attempt starts, succeeds, or fails.
pub async fn process_uploads(
    db: &Database,
    cloud_home: &dyn CloudHome,
    encryption: &std::sync::RwLock<EncryptionService>,
    library_dir: &Path,
    clock: &dyn crate::clock::Clock,
    observer: Option<&dyn BlobUploadObserver>,
) -> Result<usize, String> {
    let uploads = db
        .get_pending_cloud_uploads()
        .await
        .map_err(|e| format!("Failed to get pending uploads: {e}"))?;

    let now = clock.now();
    let mut count = 0;
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

        if let Some(obs) = observer {
            obs.on_blob_upload_started(&entry.file_id).await;
        }

        let file_path = match &entry.source_path {
            Some(p) => std::path::PathBuf::from(p),
            None => library_dir.join(crate::storage::local::storage_path(&entry.file_id)),
        };

        let data = match tokio::fs::read(&file_path).await {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("cannot read local file {}: {e}", file_path.display());
                warn!("Upload failed: {msg}");
                record_failure(db, &entry, &msg, now, observer).await;
                continue;
            }
        };

        let encrypted = match &entry.content_key {
            Some(key) => EncryptionService::from_key(*key).encrypt(&data),
            None => encryption.read().unwrap().encrypt(&data),
        };

        match upload_with_progress(
            cloud_home,
            &entry.cloud_key,
            &entry.file_id,
            encrypted,
            observer,
        )
        .await
        {
            Ok(()) => {
                if let Err(e) = db.remove_cloud_outbox_entry(entry.id).await {
                    warn!("Failed to remove outbox entry {}: {e}", entry.id);
                }
                count += 1;

                if let Some(obs) = observer {
                    obs.on_blob_uploaded(&entry.file_id).await;
                }
            }
            Err(e) => {
                let msg = format!("cloud write failed: {e}");
                warn!("Upload failed for {}: {msg}", entry.cloud_key);
                record_failure(db, &entry, &msg, now, observer).await;
                continue;
            }
        }
    }

    Ok(count)
}

/// Process pending deletes: remove cloud files whose deletion has been synced.
///
/// A delete is only safe when all known device heads have advanced past the
/// entry's `min_seq`. Returns the number of successful deletes.
pub async fn process_deletes(
    db: &Database,
    cloud_home: &dyn CloudHome,
    device_head_seqs: &[u64],
) -> Result<usize, String> {
    let deletes = db
        .get_pending_cloud_deletes()
        .await
        .map_err(|e| format!("Failed to get pending deletes: {e}"))?;

    if deletes.is_empty() {
        return Ok(0);
    }

    let min_head = device_head_seqs.iter().copied().min();

    let mut count = 0;
    for entry in deletes {
        if let Some(min_seq) = entry.min_seq {
            if let Some(head) = min_head {
                if head <= min_seq {
                    continue;
                }
            }
        }

        match cloud_home.delete(&entry.cloud_key).await {
            Ok(()) => {
                if let Err(e) = db.remove_cloud_outbox_entry(entry.id).await {
                    warn!("Failed to remove outbox entry {}: {e}", entry.id);
                }
                count += 1;
            }
            Err(e) => {
                warn!("Delete failed for {}: {e}", entry.cloud_key);
                // Continue trying other deletes — unlike uploads, order doesn't matter
            }
        }
    }

    Ok(count)
}
