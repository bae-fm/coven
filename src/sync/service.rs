/// Full sync orchestrator: push local changes, pull remote changes.
///
/// Protocol:
/// 1. Grab changeset from the current session.
/// 2. End the session (so incoming applies don't contaminate outgoing).
/// 3. Push our changeset to S3 (handled by push module, stubbed here).
/// 4. Pull incoming changesets (NO session active -- critical).
/// 5. Apply incoming with conflict handler.
/// 6. Start a new session for the next round.
///
/// The SyncService holds the configuration for a sync cycle but does NOT own
/// the session or the raw sqlite3 handle. Those are passed in by the caller
/// because session lifetime is tied to the write connection lock.
use std::collections::HashMap;

use tracing::{info, warn};

use crate::blob::BlobPlan;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;

use super::envelope::{self, sign_envelope, ChangesetEnvelope};
use super::pull::{self, PullResult};
use super::push::{OutgoingChangeset, SCHEMA_VERSION};
use super::session::SyncSession;
use super::storage::SyncStorage;

/// Configuration for a sync service.
pub struct SyncService {
    pub device_id: String,
}

/// Everything the caller needs after a sync cycle.
pub struct SyncResult {
    /// The outgoing changeset bytes (if any local changes existed).
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

    /// Run a full sync cycle.
    ///
    /// This takes the current session, grabs its changeset, drops the session,
    /// pulls remote changes, and returns what the caller needs to push and
    /// to start a new session.
    ///
    /// The `message` parameter is a human-readable description of what changed
    /// (e.g., "Imported Album One"). Callers derive this from the app event
    /// that triggered the sync.
    ///
    /// The caller should:
    /// 1. Push `outgoing` to the storage (if Some).
    /// 2. Persist `updated_cursors` to the sync_cursors table.
    /// 3. Start a new SyncSession on the write connection.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection pointer.
    /// The session must have been created on this same connection.
    pub async unsafe fn sync(
        &self,
        db: *mut libsqlite3_sys::sqlite3,
        session: SyncSession,
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

        // Step 1: grab outgoing changeset from the session.
        let outgoing_cs = session.changeset().map_err(SyncCycleError::Session)?;

        // Step 2: end the session (drop it).
        drop(session);

        // Step 3: upload blobs the outgoing changeset references, before the
        // envelope, so pullers can fetch them as soon as they see the change.
        if let Some(ref cs) = outgoing_cs {
            let changes =
                crate::changeset::walk(cs.as_bytes()).map_err(SyncCycleError::AssetScan)?;
            for blob in blob_plan.blobs_to_push(&changes) {
                if !blob.local_path.exists() {
                    warn!(id = %blob.id, "blob file not found locally, skipping upload");
                    continue;
                }
                let bytes = std::fs::read(&blob.local_path)
                    .map_err(|e| SyncCycleError::AssetUpload(e.to_string()))?;
                storage
                    .put_blob(&blob.namespace, &blob.id, blob.scope.clone(), bytes)
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
            sign_envelope(&mut env, keypair, cs.as_bytes());
            let packed = envelope::pack(&env, cs.as_bytes());
            OutgoingChangeset {
                packed,
                seq: next_seq,
            }
        });

        // Step 4 + 5: pull incoming changesets (no session active).
        let (updated_cursors, pull_result) = pull::pull_changes(
            db,
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

        // Step 6: the caller starts a new session after this returns.

        Ok(SyncResult {
            outgoing,
            pull: pull_result,
            updated_cursors,
        })
    }
}

#[derive(Debug)]
pub enum SyncCycleError {
    Session(super::session::SyncError),
    Pull(pull::PullError),
    AssetScan(String),
    AssetUpload(String),
}

impl std::fmt::Display for SyncCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncCycleError::Session(e) => write!(f, "session error: {e}"),
            SyncCycleError::Pull(e) => write!(f, "pull error: {e}"),
            SyncCycleError::AssetScan(e) => write!(f, "asset scan error: {e}"),
            SyncCycleError::AssetUpload(e) => write!(f, "asset upload error: {e}"),
        }
    }
}

impl std::error::Error for SyncCycleError {}
