//! Cloud outbox: async upload/delete of encrypted blobs.
//!
//! The host enqueues upload entries (after writing a blob locally) and delete
//! entries (tagged with the current sync seq). The sync cycle processes the
//! outbox: uploads before push, deletes after pull.

use std::path::Path;

use tracing::warn;

use crate::blob::BlobUploadObserver;
use crate::db::SyncBookkeeping;
use crate::encryption::EncryptionService;
use crate::storage::cloud::CloudHome;

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
    db: &dyn SyncBookkeeping,
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
    db: &dyn SyncBookkeeping,
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

        let encrypted = {
            let enc = encryption.read().unwrap();
            enc.encrypt(&data)
        };

        match cloud_home.write(&entry.cloud_key, encrypted).await {
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
    db: &dyn SyncBookkeeping,
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
