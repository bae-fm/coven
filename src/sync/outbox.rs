//! Cloud outbox deletes: async delete of cloud blobs, processed by the sync cycle.
//!
//! The host enqueues delete entries (tagged with the current sync seq); the sync
//! cycle processes them after pull. Uploads — the other `cloud_outbox` operation —
//! are the blob engine's responsibility now: see [`crate::blob::upload`], which the
//! cycle drains before push. Both operations persist in the same `cloud_outbox`
//! table; this module owns only the delete drain.

use tracing::warn;

use crate::database::Database;
use crate::storage::cloud::CloudHome;

/// Process pending deletes: remove the queued cloud blobs.
///
/// A blob is deleted as soon as the deletion is queued and the cloud is
/// reachable — coven does not hold the delete until peers have synced past it. A
/// peer that still references the row pulls the row's removal on its own next
/// cycle, so the blob and the row that points at it converge. Gating the delete
/// on every peer having pulled bought only deferred cleanup while letting a
/// single departed device wedge deletion forever. Returns the number of
/// successful deletes.
pub async fn process_deletes(db: &Database, cloud_home: &dyn CloudHome) -> Result<usize, String> {
    let deletes = db
        .get_pending_cloud_deletes()
        .await
        .map_err(|e| format!("Failed to get pending deletes: {e}"))?;

    let mut count = 0;
    for entry in deletes {
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
