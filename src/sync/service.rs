//! Full sync orchestrator: gate + push local changes, pull remote changes.
//!
//! Protocol within a cycle:
//! 1. The caller captured the outgoing changeset (resetting the recorded batch)
//!    and passes the bytes in. Capture stays ENABLED throughout.
//! 2. Gate the captured changeset (cut gated-false rows, re-emit on flip).
//! 3. Push our changeset's blobs, then build the signed envelope to push.
//! 4. Pull incoming changesets and apply them — the pull disables capture around
//!    only each apply (so applied rows aren't echoed) and re-enables it at once,
//!    so a host write landing during the network phases is still captured.
//! 5. The caller runs snapshot policy.
//!
//! All connection access goes through the owned [`Database`]; capture is never
//! suspended across the network steps — only the apply briefly disables it.

use std::collections::HashMap;

use tracing::{error, info};

use crate::blob::BlobSource;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::sync::session::SyncedTable;

use super::envelope;
use super::gate;
use super::membership::{self, MembershipCoord};
use super::pull::{self, PullResult};
use super::push::{OutgoingChangeset, SCHEMA_VERSION};
use super::storage::SyncStorage;

/// Configuration for a sync service.
pub struct SyncService {
    pub device_id: String,
}

/// Everything the caller needs after the gate + push-prep + pull steps.
pub struct SyncResult {
    /// The outgoing changeset bytes (if any local changes survived the gate).
    /// The caller is responsible for pushing this to the storage.
    pub outgoing: Option<OutgoingChangeset>,
    /// Pull results (how many incoming changesets were applied).
    pub pull: PullResult,
    /// Updated cursor map (caller should persist to sync_cursors table).
    pub updated_cursors: HashMap<String, u64>,
}

impl SyncService {
    pub fn new(device_id: String) -> Self {
        SyncService { device_id }
    }

    /// Gate the captured `outgoing` changeset, prepare its push envelope, and
    /// pull remote changes.
    ///
    /// `outgoing` is the changeset the caller captured via
    /// `Database::take_changeset`; capture stays enabled, and the apply inside
    /// `pull` disables it around only the apply, so the applied rows are not
    /// re-recorded while host writes during the network steps are.
    #[allow(clippy::too_many_arguments)]
    pub async fn sync(
        &self,
        db: &Database,
        tables: &[SyncedTable],
        outgoing: Vec<u8>,
        local_seq: u64,
        cursors: &HashMap<String, u64>,
        storage: &dyn SyncStorage,
        timestamp: &str,
        message: &str,
        keypair: &UserKeypair,
        library_dir: &LibraryDir,
        blob_source: &dyn BlobSource,
    ) -> Result<SyncResult, SyncCycleError> {
        let _ = library_dir;

        // Step 2: apply row-level sync gating. Cut gated-false rows (and their
        // FK-descendants) so they stay local; re-emit a root's full subtree when
        // its gate flips false→true. Runs on the owned connection; capture stays
        // enabled (gating reads current row state from the live tables, and the
        // pull disables capture only around its apply). Done before the blob scan
        // so blob upload sees the gated set, not the cut rows.
        let outgoing_cs: Option<Vec<u8>> = if outgoing.is_empty() {
            None
        } else {
            let tables = tables.to_vec();
            let gated = db
                .call(move |conn| {
                    let gates = gate::Gates::from_tables(conn, &tables)
                        .map_err(|e| crate::database::DbError(format!("gate build: {e}")))?;
                    gate::gate_outbound(conn, &outgoing, &gates)
                        .map_err(|e| crate::database::DbError(format!("gate outbound: {e}")))
                })
                .await
                .map_err(|e| SyncCycleError::Gate(e.0))?;
            if gated.is_empty() {
                None
            } else {
                Some(gated)
            }
        };

        // Step 3: upload the blobs the outgoing changeset references, before the
        // envelope, so pullers can fetch them as soon as they see the change. Both
        // retention classes upload here — OnDemand differs from Mirrored only on
        // the pull side (it is not downloaded), not on push.
        if let Some(ref cs) = outgoing_cs {
            let changes = crate::changeset::walk(cs).map_err(SyncCycleError::AssetScan)?;
            for blob in changes.iter().flat_map(|c| blob_source.blobs_for_change(c)) {
                match crate::local_blob::exists(&blob.local_path).await {
                    Ok(true) => {}
                    // The file is absent but the outgoing changeset references it.
                    // The device that authored the row is the only one that holds
                    // the file, so the old "another device may push it" rationale
                    // does not apply — packing and publishing the changeset now would
                    // make the row visible to every device while its blob is missing
                    // from the cloud, and pullers would 404 on it permanently (the
                    // seq advances; the row is never a fresh INSERT again). A missing
                    // blob means the changeset is not ready to publish — it is not a
                    // skip. Abort the cycle (the changeset is neither packed nor
                    // pushed), exactly like the storage-error arm below; the next
                    // cycle retries once the file is back.
                    Ok(false) => {
                        error!(
                            id = %blob.id,
                            path = %blob.local_path.display(),
                            "blob file missing locally; aborting push so the changeset \
                             is not published without its blob"
                        );
                        return Err(SyncCycleError::BlobMissing(format!(
                            "blob {} file not found at {}",
                            blob.id,
                            blob.local_path.display()
                        )));
                    }
                    // A real storage failure checking existence is not "absent" —
                    // abort the cycle rather than silently dropping the upload.
                    Err(e) => {
                        return Err(SyncCycleError::AssetUpload(format!(
                            "checking local blob for {}: {e}",
                            blob.id
                        )));
                    }
                }
                // Resolve the host's public scope to the internal key scope before
                // storage encrypts. An `Item(id)` scope reads the key from
                // `item_keys`; a missing row is a host bug and aborts the cycle.
                let resolved = db
                    .resolve_blob_scope(blob.scope.clone())
                    .await
                    .map_err(|e| SyncCycleError::AssetUpload(e.0))?;
                let bytes = crate::local_blob::read(&blob.local_path)
                    .await
                    .map_err(SyncCycleError::AssetUpload)?;
                storage
                    .put_blob(
                        &blob.namespace,
                        &blob.id,
                        resolved,
                        blob.cloud_path.as_deref(),
                        bytes,
                    )
                    .await
                    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
                info!(id = %blob.id, namespace = %blob.namespace, "uploaded blob");
            }
        }

        // Bind the outgoing changeset to the membership entry that authorizes us
        // to write. A puller that has not yet seen that entry (membership entries
        // and changesets are separate, unordered object streams) fetches it by
        // this coordinate to resolve the gap, instead of judging us non-member and
        // skipping the changeset forever. Only needed when we actually publish.
        let membership_grant = match &outgoing_cs {
            Some(_) => self.resolve_write_grant(storage, keypair).await?,
            None => None,
        };

        let outgoing = outgoing_cs.map(|cs| {
            let next_seq = local_seq + 1;
            let packed = envelope::pack_signed(
                &self.device_id,
                next_seq,
                SCHEMA_VERSION,
                message,
                timestamp,
                keypair,
                membership_grant,
                &cs,
            );
            OutgoingChangeset {
                packed,
                seq: next_seq,
            }
        });

        // Step 4 + 5: pull incoming changesets and apply them (the pull disables
        // capture around only each apply, so applied rows are not echoed).
        let (updated_cursors, pull_result) = pull::pull_changes(
            db,
            tables,
            storage,
            &self.device_id,
            cursors,
            library_dir,
            blob_source,
        )
        .await
        .map_err(SyncCycleError::Pull)?;

        if pull_result.changesets_applied > 0 {
            info!(
                applied = pull_result.changesets_applied,
                devices = pull_result.devices_pulled,
                "pull complete"
            );
        }

        Ok(SyncResult {
            outgoing,
            pull: pull_result,
            updated_cursors,
        })
    }

    /// The storage coordinate of the membership entry that authorizes this device
    /// to write, or `None` for a solo library (no membership chain). Embedded in
    /// the outgoing changeset so a puller can resolve a membership-propagation gap.
    ///
    /// A storage failure aborts the cycle rather than publishing a changeset with
    /// no grant: a puller hitting the gap window would otherwise skip it as
    /// non-member — the very loss this binding exists to prevent.
    async fn resolve_write_grant(
        &self,
        storage: &dyn SyncStorage,
        keypair: &UserKeypair,
    ) -> Result<Option<MembershipCoord>, SyncCycleError> {
        let entry_keys = storage
            .list_membership_entries()
            .await
            .map_err(|e| SyncCycleError::Membership(format!("list membership entries: {e}")))?;
        if entry_keys.is_empty() {
            return Ok(None);
        }
        let entries = super::membership_ops::download_entries(storage, &entry_keys)
            .await
            .map_err(|e| SyncCycleError::Membership(e.0))?;
        let our_pubkey = hex::encode(keypair.public_key);
        Ok(membership::write_grant_coord(&entries, &our_pubkey))
    }
}

#[derive(Debug)]
pub enum SyncCycleError {
    Gate(String),
    Pull(pull::PullError),
    AssetScan(String),
    AssetUpload(String),
    /// An outgoing changeset references a blob whose local file is missing, so the
    /// changeset cannot be published without stranding pullers on a 404.
    BlobMissing(String),
    /// The membership chain could not be loaded to bind the outgoing changeset to
    /// the entry that authorizes this device — publishing without it risks pullers
    /// skipping the changeset as non-member.
    Membership(String),
}

impl std::fmt::Display for SyncCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCycleError::Gate(e) => write!(f, "gate error: {e}"),
            SyncCycleError::Pull(e) => write!(f, "pull error: {e}"),
            SyncCycleError::AssetScan(e) => write!(f, "asset scan error: {e}"),
            SyncCycleError::AssetUpload(e) => write!(f, "asset upload error: {e}"),
            SyncCycleError::BlobMissing(e) => write!(f, "blob missing: {e}"),
            SyncCycleError::Membership(e) => write!(f, "membership error: {e}"),
        }
    }
}

impl std::error::Error for SyncCycleError {}
