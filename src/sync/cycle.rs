//! Sync cycle orchestration.
//!
//! Contains the logic for running a single sync cycle (push local changes,
//! pull remote changes, manage snapshots) and for initializing sync
//! infrastructure.

use std::path::PathBuf;

use tracing::{info, warn};

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::changeset::RowChange;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::db::{RawDbHandle, SyncBookkeeping};
use crate::encryption::EncryptionService;
use crate::keys::{KeyService, UserKeypair};
use crate::library_dir::LibraryDir;
use crate::storage::cloud::CloudHome;

use super::encrypted_storage::EncryptedSyncStorage;
use super::hlc::{Hlc, Timestamp};
use super::service::SyncService;
use super::session::SyncSession;
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

/// Outcome of a sync cycle attempt.
pub enum SyncCycleOutcome {
    /// Cycle completed successfully. Contains the result and a new session.
    Ok(SyncCycleResult, SyncSession),
    /// Cycle failed but we recovered a session for next time.
    ErrWithSession(String, SyncSession),
    /// Cycle failed and we couldn't recover a session either.
    ErrNoSession(String),
}

/// Run a single sync cycle: grab changeset, push, pull, restart session.
///
/// This manages all the state (local_seq, cursors, staging, snapshots) by
/// loading/persisting from the database each cycle, rather than keeping
/// mutable state across calls.
///
/// Always tries to return a usable session, even on error.
///
/// # Safety
///
/// `raw_db` must be a valid sqlite3 write connection pointer that outlives
/// this call. `session` is consumed and a new one is started on `raw_db`.
pub async unsafe fn run_single_sync_cycle(
    storage: &dyn SyncStorage,
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    raw_db: *mut libsqlite3_sys::sqlite3,
    session: SyncSession,
    encryption: &std::sync::RwLock<EncryptionService>,
    user_keypair: &UserKeypair,
    db: &dyn SyncBookkeeping,
    library_dir: &LibraryDir,
    cloud_home: Option<&dyn CloudHome>,
    blob_plan: &dyn BlobPlan,
    observer: Option<&dyn BlobUploadObserver>,
) -> SyncCycleOutcome {
    let sync_service = SyncService::new(device_id.to_string());

    // Helper macro to recover a session on error
    macro_rules! recover_session_on_err {
        ($err:expr) => {
            match unsafe { SyncSession::start(raw_db) } {
                Ok(s) => return SyncCycleOutcome::ErrWithSession($err, s),
                Err(se) => {
                    return SyncCycleOutcome::ErrNoSession(format!(
                        "{} (also failed to restart session: {se})",
                        $err
                    ))
                }
            }
        };
    }

    // Load persisted sync state — DB errors abort the cycle (a transient
    // SQLite error must not make us treat the device as brand-new at seq 0).
    // None (key not set yet) legitimately defaults to 0 / None.
    let mut local_seq = match db.get_sync_state("local_seq").await {
        Ok(Some(v)) => match v.parse::<u64>() {
            Ok(n) => n,
            Err(e) => {
                recover_session_on_err!(format!("Corrupt local_seq value: {e}"));
            }
        },
        Ok(None) => 0,
        Err(e) => {
            recover_session_on_err!(format!("Failed to read local_seq: {e}"));
        }
    };

    let snapshot_seq: Option<u64> = match db.get_sync_state("snapshot_seq").await {
        Ok(Some(v)) => match v.parse::<u64>() {
            Ok(n) => Some(n),
            Err(e) => {
                recover_session_on_err!(format!("Corrupt snapshot_seq value: {e}"));
            }
        },
        Ok(None) => None,
        Err(e) => {
            recover_session_on_err!(format!("Failed to read snapshot_seq: {e}"));
        }
    };

    let last_snapshot_time: Option<chrono::DateTime<chrono::Utc>> =
        match db.get_sync_state("last_snapshot_time").await {
            Ok(Some(v)) => match chrono::DateTime::parse_from_rfc3339(&v) {
                Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
                Err(e) => {
                    recover_session_on_err!(format!("Corrupt last_snapshot_time value: {e}"));
                }
            },
            Ok(None) => None,
            Err(e) => {
                recover_session_on_err!(format!("Failed to read last_snapshot_time: {e}"));
            }
        };

    let staged_seq: Option<u64> = match db.get_sync_state("staged_seq").await {
        Ok(Some(v)) if v.is_empty() => None,
        Ok(Some(v)) => match v.parse::<u64>() {
            Ok(n) => Some(n),
            Err(e) => {
                recover_session_on_err!(format!("Corrupt staged_seq value: {e}"));
            }
        },
        Ok(None) => None,
        Err(e) => {
            recover_session_on_err!(format!("Failed to read staged_seq: {e}"));
        }
    };

    // Retry any staged changeset from a previous failed push
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

                    if let Err(e) = db.set_sync_state("local_seq", &seq.to_string()).await {
                        recover_session_on_err!(format!(
                            "Failed to persist local_seq after staged push: {e}"
                        ));
                    }
                    if let Err(e) = db.set_sync_state("staged_seq", "").await {
                        recover_session_on_err!(format!(
                            "Failed to clear staged_seq after staged push: {e}"
                        ));
                    }
                }
                Err(e) => {
                    recover_session_on_err!(format!("Staged changeset push failed: {e}"));
                }
            }
        } else if let Err(e) = db.set_sync_state("staged_seq", "").await {
            recover_session_on_err!(format!("Failed to clear stale staged_seq: {e}"));
        }
    }

    // Process outbox uploads (files must be in cloud before changeset references them)
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

    // Check whether we should gate changeset push
    let has_pending_uploads = match db.has_pending_cloud_uploads().await {
        Ok(v) => v,
        Err(e) => {
            recover_session_on_err!(format!("Failed to check pending cloud uploads: {e}"));
        }
    };

    // Load current cursors from DB
    let cursors = match db.get_all_sync_cursors().await {
        Ok(c) => c,
        Err(e) => {
            recover_session_on_err!(format!("Failed to load sync cursors: {e}"));
        }
    };

    let timestamp = hlc.now().to_string();

    // Run the core sync cycle
    let sync_result = unsafe {
        sync_service
            .sync(
                raw_db,
                session,
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
    };

    let sync_result = match sync_result {
        Ok(r) => r,
        Err(e) => {
            recover_session_on_err!(format!("Sync cycle error: {e}"));
        }
    };

    // Handle outgoing changeset (push)
    // Skip push if there are still pending cloud uploads — remote devices
    // should not learn about releases whose audio files aren't in cloud yet.
    if has_pending_uploads {
        if sync_result.outgoing.is_some() {
            info!("Deferring changeset push: pending cloud uploads remain");
        }
    } else if let Some(outgoing) = &sync_result.outgoing {
        let seq = outgoing.seq;

        // Stage before pushing so bytes survive a push failure
        stage_changeset(library_dir, &outgoing.packed);

        if let Err(e) = db.set_sync_state("staged_seq", &seq.to_string()).await {
            recover_session_on_err!(format!("Failed to persist staged_seq before push: {e}"));
        }

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

                if let Err(e) = db.set_sync_state("local_seq", &seq.to_string()).await {
                    recover_session_on_err!(format!("Failed to persist local_seq after push: {e}"));
                }
                if let Err(e) = db.set_sync_state("staged_seq", "").await {
                    recover_session_on_err!(format!("Failed to clear staged_seq after push: {e}"));
                }

                info!(seq, "Pushed changeset");
            }
            Err(e) => {
                warn!(seq, "Push failed, changeset staged for retry: {e}");
            }
        }
    }

    // Persist updated cursors
    for (cursor_device_id, cursor_seq) in &sync_result.updated_cursors {
        if let Err(e) = db.set_sync_cursor(cursor_device_id, *cursor_seq).await {
            warn!(
                device_id = cursor_device_id,
                seq = cursor_seq,
                "Failed to persist sync cursor: {e}"
            );
        }
    }

    // Update HLC with max remote timestamp
    let max_remote_ts = sync_result
        .pull
        .remote_heads
        .iter()
        .filter(|h| h.device_id != device_id)
        .filter_map(|h| h.last_sync.as_deref())
        .filter_map(
            |ts_str| match chrono::DateTime::parse_from_rfc3339(ts_str) {
                Ok(dt) => Some(dt.timestamp_millis().max(0) as u64),
                Err(e) => {
                    warn!(
                        timestamp = ts_str,
                        "Failed to parse peer HLC timestamp: {e}"
                    );
                    None
                }
            },
        )
        .max();

    if let Some(remote_millis) = max_remote_ts {
        let remote_ts = Timestamp::new(remote_millis, 0, "remote".to_string());
        hlc.update(&remote_ts);
    }

    // Process outbox deletes (safe to delete after all devices have synced past the deletion)
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

    // Start a new sync session
    let new_session = match unsafe { SyncSession::start(raw_db) } {
        Ok(s) => s,
        Err(e) => {
            return SyncCycleOutcome::ErrNoSession(format!("Failed to restart sync session: {e}"));
        }
    };

    // Check snapshot policy
    let hours_since = last_snapshot_time.map(|t| {
        let elapsed = clock.now().signed_duration_since(t);
        elapsed.num_hours().max(0) as u64
    });

    // Initial sync: library has data but the session produced no changeset
    // (data was inserted before the sync session started — e.g., user connected
    // a cloud provider to an existing library). Push a snapshot so the existing
    // data reaches the cloud.
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
            let enc = encryption.read().unwrap();
            unsafe { super::snapshot::create_snapshot(raw_db, &temp_dir, &enc) }
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
                        if let Err(e) = db
                            .set_sync_state("snapshot_seq", &local_seq.to_string())
                            .await
                        {
                            recover_session_on_err!(format!("Failed to persist snapshot_seq: {e}"));
                        }
                        if let Err(e) = db
                            .set_sync_state("last_snapshot_time", &clock.now().to_rfc3339())
                            .await
                        {
                            recover_session_on_err!(format!(
                                "Failed to persist last_snapshot_time: {e}"
                            ));
                        }

                        info!(local_seq, "Snapshot created and pushed");
                    }
                    Err(e) => {
                        warn!("Failed to push snapshot: {e}");
                    }
                }
            }
            Err(e) => {
                warn!("Failed to create snapshot: {e}");
            }
        }
    }

    // Build status from remote heads
    let now = clock.now().to_rfc3339();
    let core_status =
        super::status::build_sync_status(&sync_result.pull.remote_heads, device_id, Some(&now));

    let other_device_count = core_status.other_devices.len();

    SyncCycleOutcome::Ok(
        SyncCycleResult {
            changesets_applied: sync_result.pull.changesets_applied,
            skipped_schema: sync_result.pull.skipped_schema,
            other_device_count,
            sync_time: now,
            asset_downloads_failed: sync_result.pull.asset_downloads_failed,
            row_changes: sync_result.pull.row_changes,
        },
        new_session,
    )
}

/// Initialize sync infrastructure from config and credentials.
///
/// Initialize sync: create storage, extract raw sqlite3 handle, start session.
///
/// Returns None if any component isn't available (missing config, credentials, etc.).
pub async fn init_sync(
    config: &Config,
    key_service: &KeyService,
    raw_db_handle: &dyn RawDbHandle,
    clock: ClockRef,
    encryption: &EncryptionService,
) -> Option<SyncComponents> {
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

    // Bootstrap auth keys if none exist yet
    let cloud_home = storage.cloud_home();

    let existing_keys = match cloud_home.list("auth/keys/").await {
        Ok(keys) => keys,
        Err(e) => {
            warn!("Failed to list auth keys: {e}");
            return None;
        }
    };

    if existing_keys.is_empty() {
        // Check if membership entries exist (shared library upgrade path)
        let membership_entries = match storage.list_membership_entries().await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to list membership entries: {e}");
                return None;
            }
        };

        if membership_entries.is_empty() {
            // Solo library — just write our own key
            let user_pk = hex::encode(user_keypair.public_key);

            if let Err(e) = cloud_home
                .write(&format!("auth/keys/{user_pk}"), vec![])
                .await
            {
                warn!("Failed to write auth key: {e}");
                return None;
            }
        } else {
            // Shared library — download chain and write keys for all current members
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

    // Acquire the raw sqlite handle and start the session AFTER all awaits.
    // The free `*mut sqlite3` is !Send; moving it next to `SyncComponents`
    // (which is unsafe impl Send) keeps the future's await points Send-clean.
    //
    // The auth-key bootstrap above persists to cloud-home before raw_db is
    // acquired. If raw_write_handle fails, the bootstrapped key stays put;
    // the next init_sync call finds existing_keys non-empty and skips the
    // bootstrap branch entirely, so partial-success is idempotent.
    let raw_db = match raw_db_handle.raw_write_handle().await {
        Ok(ptr) => ptr,
        Err(e) => {
            warn!("Failed to extract raw write handle for sync: {e}");
            return None;
        }
    };

    let session = match unsafe { SyncSession::start(raw_db) } {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to start initial sync session: {e}");
            return None;
        }
    };

    let hlc = Hlc::new(config.device_id.clone());

    info!("Sync initialized (device: {})", config.device_id);

    Some(SyncComponents {
        storage: std::sync::Arc::new(storage),
        hlc: std::sync::Arc::new(hlc),
        device_id: config.device_id.clone(),
        encryption: encryption_lock,
        raw_db,
        session,
        user_keypair,
    })
}

/// Components needed to run sync cycles.
///
/// The caller is responsible for wrapping these in the appropriate
/// thread-safe containers (Arc, Mutex, etc.) for their runtime model.
pub struct SyncComponents {
    pub storage: std::sync::Arc<EncryptedSyncStorage>,
    pub hlc: std::sync::Arc<Hlc>,
    pub device_id: String,
    pub encryption: std::sync::Arc<std::sync::RwLock<EncryptionService>>,
    pub raw_db: *mut libsqlite3_sys::sqlite3,
    pub session: SyncSession,
    pub user_keypair: UserKeypair,
}

// SAFETY: The raw sqlite3 pointer is only used for session extension operations
// which are serialized through the sync loop. The pointer itself is stable
// (heap-allocated write connection inside Arc<DatabaseInner>).
unsafe impl Send for SyncComponents {}
