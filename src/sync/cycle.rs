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
use super::envelope;
use super::hlc::Hlc;
use super::push::SCHEMA_VERSION;
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

/// Merge a freshly-gated changeset into an already-staged one while uploads are
/// pending. Both are signed envelopes; unpack each to its raw changeset,
/// concatenate the raw changesets (via the gate's changegroup), and re-sign the
/// merged result at `seq` (the bytes changed, so the signature must too).
///
/// This merge exists because the cycle must suspend the capture session before
/// the pull (so the apply isn't re-recorded), and the only suspend primitive —
/// `take_changeset_and_suspend` — *consumes* the changeset. A capture-level
/// "peek, don't consume" would dissolve this merge, but it would also have to
/// keep the session enabled across the pull's apply (re-recording remote rows as
/// local) or break the span's single unconditional resume; both are worse than
/// staging the already-captured bytes and concatenating subsequent ones here.
async fn merge_deferred_changesets(
    db: &Database,
    staged: &[u8],
    incoming: &[u8],
    device_id: &str,
    seq: u64,
    keypair: &UserKeypair,
    timestamp: &str,
) -> Result<Vec<u8>, String> {
    let (_, staged_raw) =
        envelope::unpack(staged).map_err(|e| format!("Failed to unpack staged changeset: {e}"))?;
    let (_, incoming_raw) = envelope::unpack(incoming)
        .map_err(|e| format!("Failed to unpack incoming changeset: {e}"))?;
    let merged = db
        .call(move |conn| {
            super::gate::concat_changesets(conn, &staged_raw, &incoming_raw)
                .map_err(|e| crate::database::DbError(e.to_string()))
        })
        .await
        .map_err(|e| format!("Failed to accumulate deferred changeset: {e}"))?;
    Ok(envelope::pack_signed(
        device_id,
        seq,
        SCHEMA_VERSION,
        "background sync",
        timestamp,
        keypair,
        &merged,
    ))
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

/// Commit a successful changeset push: advance `local_seq`, then clear the
/// staging record. The order matters — `local_seq` is persisted BEFORE the
/// staged_seq marker and the staged file are cleared, so a crash between them
/// leaves the staged changeset for an idempotent re-push at the same seq while
/// `local_seq` is already advanced, so no later changeset can reuse it and
/// overwrite the pushed one on the remote. Shared by the staged-retry and
/// direct-push arms so the ordering can't drift between them.
async fn commit_push_success(
    db: &Database,
    library_dir: &LibraryDir,
    seq: u64,
    local_seq: &mut u64,
) -> Result<(), String> {
    *local_seq = seq;
    db.set_sync_state("local_seq", &seq.to_string())
        .await
        .map_err(|e| format!("Failed to persist local_seq after push: {e}"))?;
    db.set_sync_state("staged_seq", "")
        .await
        .map_err(|e| format!("Failed to clear staged_seq after push: {e}"))?;
    clear_staged_changeset(library_dir);
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

    // A snapshot bootstrap that could not land every blob it references records a
    // pending flag (an empty/absent value means caught up). While it is set, the
    // reconciliation below re-runs each cycle until every blob is local; a clear
    // flag skips the scan entirely, so a caught-up library pays nothing.
    let snapshot_blob_backfill_pending = match db
        .get_sync_state(super::snapshot::SNAPSHOT_BLOB_BACKFILL_PENDING)
        .await
    {
        Ok(Some(v)) => !v.is_empty(),
        Ok(None) => false,
        Err(e) => return Err(format!("Failed to read snapshot blob backfill flag: {e}")),
    };

    // Process outbox uploads (files must be in cloud before any changeset or
    // snapshot references them). Run BEFORE the staged-push decisions below, so a
    // changeset whose blobs just finished uploading this cycle can push now.
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

    // Whether to gate changeset/snapshot propagation on pending uploads: remote
    // devices must not learn about rows whose blobs (audio) aren't in cloud yet.
    let has_pending_uploads = db
        .has_pending_cloud_uploads()
        .await
        .map_err(|e| format!("Failed to check pending cloud uploads: {e}"))?;

    // This device's pull cursors: where the pull starts from, and what we publish
    // in our head so peers know how far we've consumed each of them.
    let cursors = db
        .get_all_sync_cursors()
        .await
        .map_err(|e| format!("Failed to load sync cursors: {e}"))?;

    // Push the staged changeset (deferred for pending uploads in an earlier
    // cycle, or surviving a failed push) — but only once its blobs are in the
    // cloud. While uploads remain pending it stays staged and this cycle's
    // capture accumulates into it in the span below.
    if let Some(seq) = staged_seq {
        if has_pending_uploads {
            info!(
                seq,
                "Holding staged changeset: pending cloud uploads remain"
            );
        } else if let Some(staged_data) = read_staged_changeset(library_dir) {
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
                    commit_push_success(db, library_dir, seq, &mut local_seq).await?;
                }
                Err(e) => return Err(format!("Staged changeset push failed: {e}")),
            }
        } else {
            // staged_seq set but the file is absent: the stage write failed and
            // the push never committed (the success path persists local_seq
            // before clearing the file, so it can't produce this state). The seq
            // was never consumed on the remote — drop the marker, keep local_seq.
            // Abnormal: the changeset those bytes held is gone, so surface it.
            warn!(
                seq,
                "Stale staged_seq with no staged file; dropping the marker"
            );
            db.set_sync_state("staged_seq", "")
                .await
                .map_err(|e| format!("Failed to clear stale staged_seq: {e}"))?;
        }
    }

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

        // Propagate the captured changeset. While cloud uploads are still
        // pending we must NOT make the rows visible to peers (their blobs aren't
        // in the cloud yet), but we must NOT drop the captured changeset either —
        // the capture already consumed it from the session, so dropping it loses
        // those rows forever. Stage it instead, accumulating into any
        // already-staged changeset, and let the gated retry above push it once
        // the uploads finish.
        if let Some(outgoing) = &sync_result.outgoing {
            let seq = outgoing.seq;

            if has_pending_uploads {
                let staged = match read_staged_changeset(library_dir) {
                    Some(existing) => {
                        merge_deferred_changesets(
                            db,
                            &existing,
                            &outgoing.packed,
                            device_id,
                            seq,
                            user_keypair,
                            &timestamp,
                        )
                        .await?
                    }
                    None => outgoing.packed.clone(),
                };
                stage_changeset(library_dir, &staged);
                db.set_sync_state("staged_seq", &seq.to_string())
                    .await
                    .map_err(|e| format!("Failed to persist staged_seq while deferring: {e}"))?;
                info!(
                    seq,
                    "Deferred changeset staged: pending cloud uploads remain"
                );
            } else {
                // Stage before pushing so the bytes survive a push failure.
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
                        commit_push_success(db, library_dir, seq, &mut local_seq).await?;
                        info!(seq, "Pushed changeset");
                    }
                    Err(e) => {
                        warn!(seq, "Push failed, changeset staged for retry: {e}");
                    }
                }
            }
        }

        // Persist updated cursors. A failure here aborts the cycle like the
        // sibling bookkeeping persists (local_seq, staged_seq, HLC high-water):
        // leaving a cursor behind the rows already applied this cycle would
        // silently desync this device and mask a real DB error.
        for (cursor_device_id, cursor_seq) in &sync_result.updated_cursors {
            db.set_sync_cursor(cursor_device_id, *cursor_seq)
                .await
                .map_err(|e| {
                    format!("Failed to persist sync cursor for {cursor_device_id}: {e}")
                })?;
        }

        // Republish our head every cycle, even when we pushed no changeset of our
        // own. push_changeset writes the head only when this device produces a
        // changeset — so a device that only pulls would otherwise never refresh
        // its head. The head's last-sync time is what the sync-status view reads
        // to show how recently each device synced; writing it here after the pull
        // keeps that current. Best-effort: a transient failure leaves last cycle's
        // head, and the next cycle republishes unconditionally, so we log rather
        // than abort.
        if let Err(e) = storage
            .put_head(device_id, local_seq, snapshot_seq, &timestamp)
            .await
        {
            warn!("Failed to republish head after pull: {e}");
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

        // Process outbox deletes: remove the queued cloud blobs now, without
        // waiting on peers. A peer still holding the referencing row pulls its
        // removal on its own next cycle.
        if let Some(ch) = cloud_home {
            match super::outbox::process_deletes(db, ch).await {
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

    // Reconcile the blob files a snapshot bootstrap could not land. Runs only
    // while the pending flag is set, and after the pull's span resumed capture so
    // any blob whose `item_keys` row arrived this cycle now resolves its key. The
    // reconciliation is read-only (it downloads files, writes no rows), so it does
    // not need the suspended span. On the first run that lands every referenced
    // blob the flag clears and no later cycle scans.
    if snapshot_blob_backfill_pending {
        match super::snapshot::reconcile_snapshot_blobs(
            db,
            &library_dir.db_path(),
            storage,
            blob_plan,
        )
        .await
        {
            Ok(true) => {
                db.set_sync_state(super::snapshot::SNAPSHOT_BLOB_BACKFILL_PENDING, "")
                    .await
                    .map_err(|e| format!("Failed to clear snapshot blob backfill flag: {e}"))?;
                info!("Snapshot blob backfill reconciled; flag cleared");
            }
            Ok(false) => {
                info!("Snapshot blob backfill still incomplete; will retry next cycle");
            }
            Err(e) => warn!("Snapshot blob reconciliation error: {e}"),
        }
    }

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

    // The snapshot is the second channel that propagates rows to peers, so it
    // honors the same blob-before-row gate as the changeset push: defer it while
    // uploads are pending, otherwise a bootstrapping peer would materialize a row
    // whose audio isn't in the cloud yet.
    if !has_pending_uploads
        && (is_initial_sync
            || super::snapshot::should_create_snapshot(local_seq, snapshot_seq, hours_since))
    {
        if is_initial_sync {
            info!("Initial sync: pushing snapshot of existing library data");
        } else {
            info!("Snapshot policy triggered, creating snapshot");
        }

        // Scratch the snapshot copy in the library dir, not the shared system
        // temp dir: create_snapshot writes a fixed `snapshot.db` filename, so two
        // libraries syncing concurrently (or parallel tests) would otherwise race
        // on one `/tmp/snapshot.db`. A library's own cycles run serially.
        let temp_dir = library_dir.as_ref().to_path_buf();
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
