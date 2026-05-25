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

/// Process pending uploads: read local file, encrypt, write to cloud.
///
/// Returns the number of successful uploads. Stops at the first failure so we
/// don't push out-of-order. After each successful upload, `observer` (if any)
/// is notified so the host can run its own bookkeeping.
pub async fn process_uploads(
    db: &dyn SyncBookkeeping,
    cloud_home: &dyn CloudHome,
    encryption: &std::sync::RwLock<EncryptionService>,
    library_dir: &Path,
    observer: Option<&dyn BlobUploadObserver>,
) -> Result<usize, String> {
    let uploads = db
        .get_pending_cloud_uploads()
        .await
        .map_err(|e| format!("Failed to get pending uploads: {e}"))?;

    let mut count = 0;
    for entry in uploads {
        let file_path = match &entry.source_path {
            Some(p) => std::path::PathBuf::from(p),
            None => library_dir.join(crate::storage::local::storage_path(&entry.file_id)),
        };

        let data = match tokio::fs::read(&file_path).await {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Upload failed: cannot read local file {}: {e}",
                    file_path.display()
                );
                break;
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
                warn!("Upload failed for {}: {e}", entry.cloud_key);
                break;
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
