//! Full sync orchestrator: gate + push local changes, pull remote changes.
//!
//! Protocol within a cycle:
//! 1. The caller captured the outgoing changeset and suspended the capture
//!    session (so incoming applies are not re-recorded). The bytes are passed in.
//! 2. Gate the captured changeset (cut gated-false rows, re-emit on flip).
//! 3. Push our changeset's blobs, then build the signed envelope to push.
//! 4. Pull incoming changesets and apply them (session still suspended).
//! 5. The caller resumes the capture session and runs snapshot policy.
//!
//! All connection access goes through the owned [`Database`]; the session
//! lifecycle (suspend/resume) is the caller's, since it spans the network steps.

use std::collections::HashMap;

use tracing::{info, warn};

use crate::blob::BlobPlan;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::sync::session::SyncedTable;

use super::envelope::{self, sign_envelope, ChangesetEnvelope};
use super::gate;
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
    /// `Database::take_changeset_and_suspend`; the capture session is suspended
    /// for the duration, so the apply inside `pull` is not re-recorded.
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
        blob_plan: &dyn BlobPlan,
    ) -> Result<SyncResult, SyncCycleError> {
        let _ = library_dir;

        // Step 2: apply row-level sync gating. Cut gated-false rows (and their
        // FK-descendants) so they stay local; re-emit a root's full subtree when
        // its gate flips false→true. Runs on the owned connection with the capture
        // session already suspended. Done before the blob scan so blob upload sees
        // the gated set, not the cut rows.
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

        // Step 3: upload blobs the outgoing changeset references, before the
        // envelope, so pullers can fetch them as soon as they see the change.
        if let Some(ref cs) = outgoing_cs {
            let changes = crate::changeset::walk(cs).map_err(SyncCycleError::AssetScan)?;
            for blob in blob_plan.blobs_to_push(&changes) {
                if !blob.local_path.exists() {
                    warn!(id = %blob.id, "blob file not found locally, skipping upload");
                    continue;
                }
                // Resolve the host's public scope to the internal key scope before
                // storage encrypts. An `Item(id)` scope reads the key from
                // `item_keys`; a missing row is a host bug and aborts the cycle.
                let resolved = db
                    .resolve_blob_scope(blob.scope.clone())
                    .await
                    .map_err(|e| SyncCycleError::AssetUpload(e.0))?;
                let bytes = std::fs::read(&blob.local_path)
                    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
                storage
                    .put_blob(&blob.namespace, &blob.id, resolved, bytes)
                    .await
                    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
                info!(id = %blob.id, namespace = %blob.namespace, "uploaded blob");
            }
        }

        let outgoing = outgoing_cs.map(|cs| {
            let next_seq = local_seq + 1;
            let mut env = ChangesetEnvelope {
                device_id: self.device_id.clone(),
                seq: next_seq,
                schema_version: SCHEMA_VERSION,
                message: message.to_string(),
                timestamp: timestamp.to_string(),
                changeset_size: cs.len(),
                author_pubkey: None,
                signature: None,
            };
            sign_envelope(&mut env, keypair, &cs);
            let packed = envelope::pack(&env, &cs);
            OutgoingChangeset {
                packed,
                seq: next_seq,
            }
        });

        // Step 4 + 5: pull incoming changesets and apply them (session suspended).
        let (updated_cursors, pull_result) = pull::pull_changes(
            db,
            tables,
            storage,
            &self.device_id,
            cursors,
            library_dir,
            blob_plan,
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
}

#[derive(Debug)]
pub enum SyncCycleError {
    Gate(String),
    Pull(pull::PullError),
    AssetScan(String),
    AssetUpload(String),
}

impl std::fmt::Display for SyncCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCycleError::Gate(e) => write!(f, "gate error: {e}"),
            SyncCycleError::Pull(e) => write!(f, "pull error: {e}"),
            SyncCycleError::AssetScan(e) => write!(f, "asset scan error: {e}"),
            SyncCycleError::AssetUpload(e) => write!(f, "asset upload error: {e}"),
        }
    }
}

impl std::error::Error for SyncCycleError {}
