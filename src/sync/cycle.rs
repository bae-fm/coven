//! Sync cycle orchestration.
//!
//! Runs a single sync cycle (gate + push local changes, pull remote changes,
//! manage snapshots) and initializes sync infrastructure. All connection access
//! goes through the owned [`Database`]; the capture session is suspended for the
//! gate/pull span and resumed before the snapshot.

use std::path::PathBuf;

use tracing::{error, info, warn};

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::changeset::RowChange;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::{KeyService, UserKeypair};
use crate::library_dir::LibraryDir;
use crate::storage::cloud::CloudHome;

use super::encrypted_storage::EncryptedSyncStorage;
use super::hlc::Hlc;
use super::service::SyncService;
use super::storage::SyncStorage;

/// Result of a single sync cycle.
pub struct SyncCycleResult {
    /// Number of remote changesets that were applied.
    pub changesets_applied: u64,
    /// Changesets from a newer schema version that we couldn't apply. The
    /// cursor advanced past them, so the count is per-cycle (transient) — it
    /// surfaces once and clears once the user updates the client.
    pub skipped_schema: u64,
    /// Number of other devices seen in the sync storage.
    pub other_device_count: usize,
    /// RFC 3339 timestamp of when this cycle completed.
    pub sync_time: String,
    /// Asset downloads failed — cursor not advanced for those changesets.
    pub asset_downloads_failed: bool,
    /// Row changes from applied changesets, for the host to map to domain events.
    pub row_changes: Vec<RowChange>,
}

/// Path for staging outgoing changeset bytes that survived a push failure.
pub fn staging_path(library_dir: &LibraryDir) -> PathBuf {
    library_dir.join("sync_staging.bin")
}

/// Stage outgoing changeset bytes to disk before pushing.
pub fn stage_changeset(library_dir: &LibraryDir, packed: &[u8]) {
    if let Err(e) = std::fs::write(staging_path(library_dir), packed) {
        warn!("Failed to stage outgoing changeset: {e}");
    }
}

/// Clear the staged changeset after a successful push.
pub fn clear_staged_changeset(library_dir: &LibraryDir) {
    let _ = std::fs::remove_file(staging_path(library_dir));
}

/// Read a previously staged changeset (if any) for retry.
pub fn read_staged_changeset(library_dir: &LibraryDir) -> Option<Vec<u8>> {
    let path = staging_path(library_dir);
    if path.exists() {
        match std::fs::read(&path) {
            Ok(data) if !data.is_empty() => Some(data),
            Ok(_) => {
                clear_staged_changeset(library_dir);
                None
            }
            Err(e) => {
                warn!("Failed to read staged changeset: {e}");
                clear_staged_changeset(library_dir);
                None
            }
        }
    } else {
        None
    }
}

/// Push a changeset to the sync storage and update the device head.
pub async fn push_changeset(
    storage: &dyn SyncStorage,
    device_id: &str,
    seq: u64,
    packed: Vec<u8>,
    snapshot_seq: Option<u64>,
    timestamp: &str,
) -> Result<(), super::storage::StorageError> {
    storage.put_changeset(device_id, seq, packed).await?;
    storage
        .put_head(device_id, seq, snapshot_seq, timestamp)
        .await?;
    Ok(())
}

/// Run a single sync cycle: capture + gate + push, pull, bookkeeping, snapshot.
///
/// All connection access goes through `db`. The capture session is suspended at
/// the start of the cycle (so the apply during pull is not re-recorded) and
/// resumed before the snapshot. Loads/persists all cycle state (local_seq,
/// cursors, staging, snapshots) through `db`'s bookkeeping API rather than
/// keeping mutable state across calls.
#[allow(clippy::too_many_arguments)]
pub async fn run_single_sync_cycle(
    storage: &dyn SyncStorage,
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    db: &Database,
    encryption: &std::sync::RwLock<EncryptionService>,
    user_keypair: &UserKeypair,
    library_dir: &LibraryDir,
    cloud_home: Option<&dyn CloudHome>,
    blob_plan: &dyn BlobPlan,
    observer: Option<&dyn BlobUploadObserver>,
) -> Result<SyncCycleResult, String> {
    // The synced-table set is owned by the Database; read it once here.
    let tables = db.synced_tables();
    let sync_service = SyncService::new(device_id.to_string());

    // Load persisted sync state — DB errors abort the cycle (a transient SQLite
    // error must not make us treat the device as brand-new at seq 0). None (key
    // not set yet) legitimately defaults to 0 / None.
    let mut local_seq = match db.get_sync_state("local_seq").await {
        Ok(Some(v)) => v
            .parse::<u64>()
            .map_err(|e| format!("Corrupt local_seq value: {e}"))?,
        Ok(None) => 0,
        Err(e) => return Err(format!("Failed to read local_seq: {e}")),
    };

    let snapshot_seq: Option<u64> = match db.get_sync_state("snapshot_seq").await {
        Ok(Some(v)) => Some(
            v.parse::<u64>()
                .map_err(|e| format!("Corrupt snapshot_seq value: {e}"))?,
        ),
        Ok(None) => None,
        Err(e) => return Err(format!("Failed to read snapshot_seq: {e}")),
    };

    let last_snapshot_time: Option<chrono::DateTime<chrono::Utc>> =
        match db.get_sync_state("last_snapshot_time").await {
            Ok(Some(v)) => Some(
                chrono::DateTime::parse_from_rfc3339(&v)
                    .map_err(|e| format!("Corrupt last_snapshot_time value: {e}"))?
                    .with_timezone(&chrono::Utc),
            ),
            Ok(None) => None,
            Err(e) => return Err(format!("Failed to read last_snapshot_time: {e}")),
        };

    let staged_seq: Option<u64> = match db.get_sync_state("staged_seq").await {
        Ok(Some(v)) if v.is_empty() => None,
        Ok(Some(v)) => Some(
            v.parse::<u64>()
                .map_err(|e| format!("Corrupt staged_seq value: {e}"))?,
        ),
        Ok(None) => None,
        Err(e) => return Err(format!("Failed to read staged_seq: {e}")),
    };

    // Retry any staged changeset from a previous failed push.
    if let Some(seq) = staged_seq {
        if let Some(staged_data) = read_staged_changeset(library_dir) {
            let timestamp = hlc.now().to_string();
            info!(seq, "Retrying staged changeset push");

            match push_changeset(
                storage,
                device_id,
                seq,
                staged_data,
                snapshot_seq,
                &timestamp,
            )
            .await
            {
                Ok(()) => {
                    info!(seq, "Staged changeset push succeeded");
                    clear_staged_changeset(library_dir);
                    local_seq = seq;

                    db.set_sync_state("local_seq", &seq.to_string())
                        .await
                        .map_err(|e| {
                            format!("Failed to persist local_seq after staged push: {e}")
                        })?;
                    db.set_sync_state("staged_seq", "").await.map_err(|e| {
                        format!("Failed to clear staged_seq after staged push: {e}")
                    })?;
                }
                Err(e) => return Err(format!("Staged changeset push failed: {e}")),
            }
        } else {
            db.set_sync_state("staged_seq", "")
                .await
                .map_err(|e| format!("Failed to clear stale staged_seq: {e}"))?;
        }
    }

    // Process outbox uploads (files must be in cloud before changeset references them).
    if let Some(ch) = cloud_home {
        match super::outbox::process_uploads(
            db,
            ch,
            encryption,
            library_dir.as_ref(),
            clock,
            observer,
        )
        .await
        {
            Ok(n) if n > 0 => info!(count = n, "Processed outbox uploads"),
            Err(e) => warn!("Outbox upload processing error: {e}"),
            _ => {}
        }
    }

    // Whether to gate changeset push on pending uploads.
    let has_pending_uploads = db
        .has_pending_cloud_uploads()
        .await
        .map_err(|e| format!("Failed to check pending cloud uploads: {e}"))?;

    let cursors = db
        .get_all_sync_cursors()
        .await
        .map_err(|e| format!("Failed to load sync cursors: {e}"))?;

    let timestamp = hlc.now().to_string();

    // ---- Suspended span ----
    //
    // From the changeset capture (which drops the session) through the last
    // bookkeeping persist, the capture session is suspended so the apply during
    // pull is not re-recorded as a local change. The `Database` is reused every
    // cycle (the actor outlives the loop), so a span that exits without resuming
    // would leave capture permanently off — silent, total sync loss.
    //
    // To make that impossible the entire span runs in one inner block that yields
    // a `Result`; whatever it returns (Ok or Err, including a capture error or a
    // mid-span bookkeeping-persist failure), `resume_session()` runs once,
    // unconditionally, immediately after. Only then is the inner result
    // propagated. There is exactly one suspend and exactly one matching resume,
    // both on the same straight-line path — no early `return` can skip it.
    let span = async {
        // Capture the outgoing changeset and suspend the capture session. Inside
        // the guarded span so a capture error also resumes below.
        let outgoing = db
            .take_changeset_and_suspend()
            .await
            .map_err(|e| format!("Failed to capture outgoing changeset: {e}"))?;

        // Run the core gate + push-prep + pull.
        let sync_result = sync_service
            .sync(
                db,
                tables,
                outgoing,
                local_seq,
                &cursors,
                storage,
                &timestamp,
                "background sync",
                user_keypair,
                library_dir,
                blob_plan,
            )
            .await
            .map_err(|e| format!("Sync cycle error: {e}"))?;

        // Handle outgoing changeset (push). Skip push if there are still pending
        // cloud uploads — remote devices should not learn about releases whose
        // audio files aren't in cloud yet.
        if has_pending_uploads {
            if sync_result.outgoing.is_some() {
                info!("Deferring changeset push: pending cloud uploads remain");
            }
        } else if let Some(outgoing) = &sync_result.outgoing {
            let seq = outgoing.seq;

            // Stage before pushing so bytes survive a push failure.
            stage_changeset(library_dir, &outgoing.packed);

            db.set_sync_state("staged_seq", &seq.to_string())
                .await
                .map_err(|e| format!("Failed to persist staged_seq before push: {e}"))?;

            match push_changeset(
                storage,
                device_id,
                seq,
                outgoing.packed.clone(),
                snapshot_seq,
                &timestamp,
            )
            .await
            {
                Ok(()) => {
                    clear_staged_changeset(library_dir);
                    local_seq = seq;

                    db.set_sync_state("local_seq", &seq.to_string())
                        .await
                        .map_err(|e| format!("Failed to persist local_seq after push: {e}"))?;
                    db.set_sync_state("staged_seq", "")
                        .await
                        .map_err(|e| format!("Failed to clear staged_seq after push: {e}"))?;

                    info!(seq, "Pushed changeset");
                }
                Err(e) => {
                    warn!(seq, "Push failed, changeset staged for retry: {e}");
                }
            }
        }

        // Persist updated cursors.
        for (cursor_device_id, cursor_seq) in &sync_result.updated_cursors {
            if let Err(e) = db.set_sync_cursor(cursor_device_id, *cursor_seq).await {
                warn!(
                    device_id = cursor_device_id,
                    seq = cursor_seq,
                    "Failed to persist sync cursor: {e}"
                );
            }
        }

        // Advance the HLC past every applied row's `_updated_at`, so the next
        // local stamp sorts causally after anything just pulled. The source is the
        // max applied-row `_updated_at` (a real HLC stamp), not the envelope/head
        // timestamp — only the row register drives last-writer-wins. This is an
        // authoritative register value the LWW layer already wrote to disk, so the
        // advance is unconditional (no skew cap): capping it could mint the next
        // local stamp below an already-stored applied row and lose LWW to it.
        if let Some(max_applied) = &sync_result.pull.max_applied_updated_at {
            hlc.advance_past(max_applied);
        }

        // Flush the clock's high-water mark so a restart re-seeds past it. This
        // captures both the merge above and any host stamps minted this cycle
        // (e.g. the changeset envelope timestamp), since `high_water` reads the
        // clock's current state. A persist error aborts the cycle rather than
        // risking a backward jump after restart.
        db.set_sync_state(
            crate::sync::hlc::HIGHWATER_STATE_KEY,
            &hlc.high_water().to_string(),
        )
        .await
        .map_err(|e| format!("Failed to persist HLC high-water mark: {e}"))?;

        // Process outbox deletes (safe after all devices synced past the deletion).
        if let Some(ch) = cloud_home {
            let device_head_seqs: Vec<u64> = sync_result
                .pull
                .remote_heads
                .iter()
                .map(|h| h.seq)
                .collect();

            match super::outbox::process_deletes(db, ch, &device_head_seqs).await {
                Ok(n) if n > 0 => info!(count = n, "Processed outbox deletes"),
                Err(e) => warn!("Outbox delete processing error: {e}"),
                _ => {}
            }
        }

        Ok::<_, String>((sync_result, local_seq))
    }
    .await;

    // Resume the capture session now that the suspended span is done, before any
    // further host writes (and before the snapshot, which VACUUMs the live db).
    // UNCONDITIONAL: this runs whether the span succeeded or failed, so a failure
    // mid-span can never leave capture suspended for the life of the reused
    // `Database`. A resume failure is itself loud — log it at error and surface.
    if let Err(re) = db.resume_session().await {
        error!("Failed to resume capture session after sync span: {re}");
        // Propagate the span error if there was one; otherwise surface the resume
        // failure (capture is now off and must not be reported as a clean cycle).
        return Err(match span {
            Err(span_err) => {
                format!("{span_err} (also failed to resume capture session: {re})")
            }
            Ok(_) => format!("Failed to resume capture session: {re}"),
        });
    }

    let (sync_result, local_seq) = span?;

    // Check snapshot policy.
    let hours_since = last_snapshot_time.map(|t| {
        let elapsed = clock.now().signed_duration_since(t);
        elapsed.num_hours().max(0) as u64
    });

    // Initial sync: library has data but the session produced no changeset (data
    // was inserted before the cycle ran — e.g. user connected a provider to an
    // existing library). Push a snapshot so the existing data reaches the cloud.
    let is_initial_sync =
        local_seq == 0 && snapshot_seq.is_none() && sync_result.outgoing.is_none();

    if is_initial_sync
        || super::snapshot::should_create_snapshot(local_seq, snapshot_seq, hours_since)
    {
        if is_initial_sync {
            info!("Initial sync: pushing snapshot of existing library data");
        } else {
            info!("Snapshot policy triggered, creating snapshot");
        }

        let temp_dir = std::env::temp_dir();
        let snapshot_result = {
            let enc = encryption.read().unwrap().clone();
            let tables = tables.to_vec();
            db.call(move |conn| {
                super::snapshot::create_snapshot(conn, &temp_dir, &tables, &enc)
                    .map_err(|e| crate::database::DbError(e.to_string()))
            })
            .await
        };

        match snapshot_result {
            Ok(encrypted) => {
                match super::snapshot::push_snapshot(
                    storage,
                    encrypted,
                    device_id,
                    sync_result.updated_cursors.clone(),
                    local_seq,
                    clock,
                )
                .await
                {
                    Ok(()) => {
                        db.set_sync_state("snapshot_seq", &local_seq.to_string())
                            .await
                            .map_err(|e| format!("Failed to persist snapshot_seq: {e}"))?;
                        db.set_sync_state("last_snapshot_time", &clock.now().to_rfc3339())
                            .await
                            .map_err(|e| format!("Failed to persist last_snapshot_time: {e}"))?;

                        info!(local_seq, "Snapshot created and pushed");
                    }
                    Err(e) => warn!("Failed to push snapshot: {e}"),
                }
            }
            Err(e) => warn!("Failed to create snapshot: {e}"),
        }
    }

    // Build status from remote heads.
    let now = clock.now().to_rfc3339();
    let core_status =
        super::status::build_sync_status(&sync_result.pull.remote_heads, device_id, Some(&now));
    let other_device_count = core_status.other_devices.len();

    Ok(SyncCycleResult {
        changesets_applied: sync_result.pull.changesets_applied,
        skipped_schema: sync_result.pull.skipped_schema,
        other_device_count,
        sync_time: now,
        asset_downloads_failed: sync_result.pull.asset_downloads_failed,
        row_changes: sync_result.pull.row_changes,
    })
}

/// Initialize sync infrastructure from config and credentials.
///
/// Creates the encrypted storage, bootstraps auth keys, and returns the
/// components the sync loop needs. Returns None if any component isn't available
/// (missing config, credentials, etc.).
pub async fn init_sync(
    config: &Config,
    key_service: &KeyService,
    db: &Database,
    clock: ClockRef,
    encryption: &EncryptionService,
    hlc: std::sync::Arc<Hlc>,
) -> Option<SyncComponents> {
    // Integration guard. The host declared its synced tables to `Database::open`;
    // an empty set means a synced library would attach nothing, every changeset
    // would come out empty, and sync would silently become snapshot-only. Refuse
    // loudly instead of pretending to sync.
    if db.synced_tables().is_empty() {
        error!(
            "sync init aborted: no synced tables — the host must pass a non-empty \
             synced-table set to coven::database::Database::open before sync starts"
        );
        return None;
    }

    let storage = match crate::storage::cloud::setup::create_sync_storage(
        config,
        key_service,
        &Some(encryption.clone()),
        clock.clone(),
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to create sync storage: {e}");
            return None;
        }
    };
    let encryption_lock = storage.shared_encryption();

    let user_keypair = match key_service.get_or_create_user_keypair() {
        Ok(kp) => kp,
        Err(e) => {
            warn!("Failed to get/create user keypair for sync: {e}");
            return None;
        }
    };

    // Bootstrap auth keys if none exist yet.
    let cloud_home = storage.cloud_home();

    let existing_keys = match cloud_home.list("auth/keys/").await {
        Ok(keys) => keys,
        Err(e) => {
            warn!("Failed to list auth keys: {e}");
            return None;
        }
    };

    if existing_keys.is_empty() {
        // Check if membership entries exist (shared library upgrade path).
        let membership_entries = match storage.list_membership_entries().await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to list membership entries: {e}");
                return None;
            }
        };

        if membership_entries.is_empty() {
            // Solo library — just write our own key.
            let user_pk = hex::encode(user_keypair.public_key);

            if let Err(e) = cloud_home
                .write(
                    &format!("auth/keys/{user_pk}"),
                    vec![],
                    &crate::storage::cloud::no_progress(),
                )
                .await
            {
                warn!("Failed to write auth key: {e}");
                return None;
            }
        } else {
            // Shared library — download chain and write keys for all current members.
            match super::membership_ops::download_chain(&storage, &membership_entries).await {
                Ok(chain) => {
                    if let Err(e) =
                        super::membership_ops::sync_authorized_keys(cloud_home, &chain).await
                    {
                        warn!("Failed to bootstrap auth keys from membership chain: {e}");
                        return None;
                    }
                }
                Err(e) => {
                    warn!("Failed to download membership chain for auth key bootstrap: {e}");
                    return None;
                }
            }
        }
    }

    info!("Sync initialized (device: {})", config.device_id);

    Some(SyncComponents {
        storage: std::sync::Arc::new(storage),
        hlc,
        device_id: config.device_id.clone(),
        encryption: encryption_lock,
        user_keypair,
    })
}

/// Components needed to run sync cycles.
///
/// The connection itself is the shared [`Database`]; the sync loop holds a clone
/// of it, so these carry only the storage, register clock, device identity,
/// encryption, and keypair.
pub struct SyncComponents {
    pub storage: std::sync::Arc<EncryptedSyncStorage>,
    pub hlc: std::sync::Arc<Hlc>,
    pub device_id: String,
    pub encryption: std::sync::Arc<std::sync::RwLock<EncryptionService>>,
    pub user_keypair: UserKeypair,
}
