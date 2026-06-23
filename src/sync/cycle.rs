//! Sync cycle orchestration.
//!
//! Runs a single sync cycle (gate + push local changes, pull remote changes,
//! manage snapshots) and initializes sync infrastructure. All connection access
//! goes through the owned [`Database`]; the capture session is suspended for the
//! gate/pull span and resumed before the snapshot.

use std::path::PathBuf;

use tracing::{debug, error, info, warn};

use crate::blob::{BlobSource, BlobUploadObserver};
use crate::changeset::RowChange;
// `Config`/`ClockRef`/`KeyService` are used only by the native-only `init_sync`.
#[cfg(not(target_arch = "wasm32"))]
use crate::clock::ClockRef;
#[cfg(not(target_arch = "wasm32"))]
use crate::config::Config;
use crate::database::Database;
#[cfg(not(target_arch = "wasm32"))]
use crate::keys::KeyService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;
use crate::storage::cloud::CloudHome;

use super::cloud_storage::{CloudCipher, CloudSyncStorage};
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
    /// Changesets skipped because their author is not a write-capable member,
    /// judged against the exact membership entry they are signed under (forged or
    /// revoked, not a propagation lag). The cursor advanced past them so the
    /// device isn't stuck; the count is per-cycle and surfaces as a warning.
    pub rejected_unauthorized: u64,
    /// Number of other devices seen in the sync storage.
    pub other_device_count: usize,
    /// RFC 3339 timestamp of when this cycle completed.
    pub sync_time: String,
    /// Asset downloads failed — cursor not advanced for those changesets.
    pub asset_downloads_failed: bool,
    /// Row changes from applied changesets, for the host to map to domain events.
    pub row_changes: Vec<RowChange>,
    /// The outbox drain broke this cycle to publish a just-completed unit
    /// (`DrainControl::Publish`), so the loop should run the next cycle promptly
    /// to drain + publish the rest instead of waiting the idle interval.
    pub resume_drain_promptly: bool,
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
    library_id: &str,
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    db: &Database,
    cipher: &std::sync::RwLock<CloudCipher>,
    user_keypair: &UserKeypair,
    library_dir: &LibraryDir,
    cloud_home: Option<&dyn CloudHome>,
    blob_source: &dyn BlobSource,
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

    // Drain the blob engine's upload queue. Blob-before-row ordering is the
    // host's responsibility: it keeps a blob-bearing row's gate column off until
    // its blobs land, then flips it on in `on_blob_uploaded` (which can also return
    // `DrainControl::Publish` to break this drain so a just-completed unit
    // publishes now instead of waiting for the whole batch). The changeset is
    // gated per row by the gate column, not by a global "any upload pending"
    // flag. The drain reports whether it broke to publish, which drives the
    // loop's cadence below.
    let mut resume_drain_promptly = false;
    if let Some(ch) = cloud_home {
        match crate::blob::upload::drain_uploads(
            db,
            ch,
            cipher,
            library_dir.as_ref(),
            clock,
            observer,
        )
        .await
        {
            Ok(outcome) => {
                resume_drain_promptly = outcome.yielded_for_publish;
                if outcome.uploaded > 0 {
                    info!(count = outcome.uploaded, "Drained blob uploads");
                }
            }
            Err(e) => warn!("Blob upload drain error: {e}"),
        }

        // Retry any tombstone-cancel an upload's inline cancel could not complete.
        // Runs right after the upload drain (and before the tombstone GC below), so
        // a blob re-uploaded this cycle has its tombstone removed before the GC
        // could reclaim it. A cancel that still fails stays queued for the next
        // cycle — the live re-uploaded blob must never lose its tombstone-cancel.
        match crate::blob::delete::drain_tombstone_cancels(db, ch, cipher).await {
            Ok(n) if n > 0 => info!(count = n, "Completed pending tombstone cancels"),
            Err(e) => warn!("Tombstone cancel drain error: {e}"),
            _ => {}
        }
    }

    // This device's pull cursors: where the pull starts from, and what we publish
    // in our head so peers know how far we've consumed each of them.
    let cursors = db
        .get_all_sync_cursors()
        .await
        .map_err(|e| format!("Failed to load sync cursors: {e}"))?;

    // Retry a staged changeset left behind by a failed push in an earlier cycle.
    // Staging exists only to let the bytes survive a push failure (the capture
    // already consumed them from the session, so a lost push must not lose the
    // rows); a fresh capture stages-then-pushes in the span below.
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
                blob_source,
            )
            .await
            .map_err(|e| format!("Sync cycle error: {e}"))?;

        // Propagate the captured changeset. The gate already cut any row whose
        // gate column is off (the host keeps a blob-bearing row gated until its
        // blobs upload), so whatever the gate emitted is safe to publish now —
        // there is no global upload deferral. Stage the bytes before pushing so a
        // push failure doesn't lose them (the capture already consumed them from
        // the session); the staged-retry above re-pushes on the next cycle.
        if let Some(outgoing) = &sync_result.outgoing {
            let seq = outgoing.seq;

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

        // Turn queued blob deletes into signed cloud tombstones (the deletion's
        // durable record), then GC tombstones whose convergence grace has passed
        // (the actual blob deletion). Holding the blob for the grace
        // keeps a peer that still references the row from being stranded; the
        // signature stops a non-member forging a deletion. (The snapshot
        // `garbage_collect` has no production caller, so the tombstone GC must run
        // here, not there.)
        if let Some(ch) = cloud_home {
            match crate::blob::delete::drain_tombstones(
                db,
                ch,
                cipher,
                library_id,
                user_keypair,
                clock,
            )
            .await
            {
                Ok(n) if n > 0 => info!(count = n, "Wrote blob tombstones"),
                Err(e) => warn!("Tombstone drain error: {e}"),
                _ => {}
            }
            // A still-pending tombstone-cancel means a blob was re-uploaded this
            // cycle (or earlier) but its cancel couldn't reach the cloud, so the
            // tombstone is still present though the blob is live. Reclaiming now would
            // delete that re-upload, so skip the GC entirely while any cancel is
            // pending — the next cycle retries the cancels (above) before reclaiming.
            let cancels_pending = match db.get_pending_cloud_cancels().await {
                Ok(cancels) => !cancels.is_empty(),
                Err(e) => {
                    // Can't confirm the cancel queue is clear — don't risk reclaiming.
                    warn!("Tombstone GC skipped: failed to read pending cancels: {e}");
                    true
                }
            };
            if cancels_pending {
                debug!("tombstone cancels still pending; skipping reclaim this cycle");
            } else {
                // Anchor the tombstone GC's authorization to the device's pinned owner
                // (set on join/restore/found), the same pin `ensure_owner_anchored_chain`
                // reads. A read failure aborts the GC for this cycle rather than falling
                // back to trust-on-first-use: deleting user blobs on an unverifiable
                // owner is the exact attack the pin closes. The next cycle retries.
                match db
                    .get_sync_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
                    .await
                {
                    Ok(pinned_owner) => {
                        match crate::blob::delete::gc_tombstones(
                            storage,
                            ch,
                            cipher,
                            library_id,
                            pinned_owner.as_deref(),
                            clock,
                        )
                        .await
                        {
                            Ok(n) if n > 0 => {
                                info!(count = n, "Reclaimed blobs past the tombstone grace")
                            }
                            Err(e) => warn!("Tombstone GC error: {e}"),
                            _ => {}
                        }
                    }
                    Err(e) => warn!("Tombstone GC skipped: failed to read pinned owner: {e}"),
                }
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
            library_dir,
            blob_source,
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

    // The snapshot is the second channel that propagates rows to peers. It
    // applies the same row-level gate as the changeset push (create_snapshot runs
    // the gate's delete_gated_false), so a row whose gate column is off — which
    // the host keeps off until its blobs upload — is already excluded. No global
    // upload deferral is needed: the snapshot can never carry a row whose blobs
    // aren't in the cloud.
    if is_initial_sync
        || super::snapshot::should_create_snapshot(local_seq, snapshot_seq, hours_since)
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
            let cipher = cipher.read().unwrap().clone();
            let tables = tables.to_vec();
            db.call(move |conn| {
                super::snapshot::create_snapshot(conn, &temp_dir, &tables, &cipher)
                    .map_err(|e| crate::database::DbError(e.to_string()))
            })
            .await
        };

        match snapshot_result {
            Ok(encrypted) => {
                match super::snapshot::push_snapshot(
                    storage,
                    library_id,
                    encrypted,
                    device_id,
                    sync_result.updated_cursors.clone(),
                    local_seq,
                    user_keypair,
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
        rejected_unauthorized: sync_result.pull.rejected_unauthorized.len() as u64,
        other_device_count,
        sync_time: now,
        asset_downloads_failed: sync_result.pull.asset_downloads_failed,
        row_changes: sync_result.pull.row_changes,
        resume_drain_promptly,
    })
}

/// Initialize sync infrastructure from config and credentials.
///
/// Creates the cloud storage with the caller's [`CloudCipher`] (so the sync loop
/// and snapshot creation share one instance — a member removal rotates the key
/// in place through it), bootstraps auth keys, and returns the components the
/// sync loop needs. Returns None if any component isn't available (missing
/// config, credentials, etc.).
///
/// Native-only: builds the storage through the native-only
/// [`crate::storage::cloud::setup::create_sync_storage`] (which constructs a
/// native-only concrete backend).
#[cfg(not(target_arch = "wasm32"))]
pub async fn init_sync(
    config: &Config,
    key_service: &KeyService,
    db: &Database,
    clock: ClockRef,
    cipher: &CloudCipher,
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
        Some(cipher.clone()),
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
    let cipher_lock = storage.shared_cipher();

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

    let our_pk = hex::encode(user_keypair.public_key);

    if cipher.is_plaintext() {
        // Browsable (plaintext) home: no membership chain — open by design. Keep
        // only the device's own auth-key marker; a plaintext home has no chain.
        if existing_keys.is_empty() {
            if let Err(e) = cloud_home
                .write(
                    &format!("auth/keys/{our_pk}"),
                    vec![],
                    &crate::storage::cloud::no_progress(),
                )
                .await
            {
                warn!("Failed to write auth key: {e}");
                return None;
            }
        }
    } else {
        // Opaque (encrypted) home: every library has an owner-anchored membership
        // chain from creation (issue #102). Establish it on first connect, and on
        // every connect verify the chain is still founded by the pinned owner —
        // refusing a missing or refounded chain as a takeover attempt (#95/#104).
        let chain = match ensure_owner_anchored_chain(&storage, db, &user_keypair, &hlc).await {
            Ok(chain) => chain,
            Err(e) => {
                error!("Membership chain bootstrap/anchor failed: {e}");
                return None;
            }
        };
        // First-time auth-key bootstrap from the chain. Refreshing the auth-key set
        // on every cycle (#85/#87) is the auth-key refresh path's job, separate from
        // this one-time bootstrap.
        if existing_keys.is_empty() {
            if let Err(e) = super::membership_ops::sync_authorized_keys(cloud_home, &chain).await {
                error!("Failed to bootstrap auth keys from membership chain: {e}");
                return None;
            }
        }
    }

    info!("Sync initialized (device: {})", config.device_id);

    Some(SyncComponents {
        storage: std::sync::Arc::new(storage),
        hlc,
        library_id: config.library_id.clone(),
        device_id: config.device_id.clone(),
        cipher: cipher_lock,
        user_keypair,
    })
}

/// Establish or verify the owner-anchored membership chain for an opaque library
/// (issue #102). Returns the validated chain for auth-key bootstrap, or an error
/// to abort sync.
///
/// Founding is two non-atomic writes — the founder entry to cloud storage and the
/// owner pin to the local DB — so this completes a half-done founding (in either
/// order) idempotently when the chain is founded by *our* key, and otherwise
/// refuses. It never adopts a chain founded by a *different* key with no owner
/// pinned: that is the first-connect takeover window (#95). Every legitimate
/// non-creator pins the owner before this runs — join from the invite's owner,
/// restore from the chain founder — so an absent pin against a foreign founder is
/// either an attacker who seeded the bucket or an unestablished library, both of
/// which we refuse rather than trust.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn ensure_owner_anchored_chain(
    storage: &dyn SyncStorage,
    db: &Database,
    owner_keypair: &UserKeypair,
    hlc: &Hlc,
) -> Result<crate::sync::membership::MembershipChain, String> {
    use super::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let our_pk = hex::encode(owner_keypair.public_key);
    let pinned = db
        .get_sync_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|e| format!("read pinned owner: {e}"))?;
    let entries = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("list membership entries: {e}"))?;

    if entries.is_empty() {
        match pinned.as_deref() {
            // Created library, first connect: we are the owner. Found + pin.
            None => found_and_pin(storage, db, owner_keypair, &our_pk, hlc).await,
            // An owner is pinned but the chain is gone. Founding writes the entry
            // before pinning, so a crash never leaves this state — it means an
            // established chain was wiped. Re-founding would silently drop every
            // member, so refuse and surface it (#104) rather than self-heal.
            Some(p) => Err(format!(
                "membership chain is missing but owner {p} is pinned for this library \
                 — refusing (wiped or tampered membership/*)"
            )),
        }
    } else {
        let chain = super::membership_ops::download_chain(storage, &entries)
            .await
            .map_err(|e| e.0)?;
        let founder = chain
            .founder_pubkey()
            .ok_or_else(|| "loaded membership chain has no founder".to_string())?
            .to_string();
        match pinned.as_deref() {
            // Anchored: the founder is the pinned owner.
            Some(p) if p == founder => Ok(chain),
            // Refounded under a different key (#95) — refuse.
            Some(p) => Err(format!(
                "membership chain founder {founder} does not match the pinned owner \
                 {p} — refusing (owner-takeover attempt)"
            )),
            // No pin, but the chain is founded by our own key. Founding is two
            // non-atomic writes — the cloud founder entry, then the local pin — and
            // `found_and_pin` already fails loud (sync does not start) if either
            // fails; this is that same founding operation's idempotent retry, not a
            // separate self-heal pass. Cross-store atomicity isn't available, so a
            // crash after the founder write but before the pin lands here; the next
            // connect completes the pin. Refusing instead would brick the library
            // forever on a mid-founding crash. Safe: an attacker cannot forge a
            // founder signed by our key.
            None if founder == our_pk => {
                db.set_sync_state(OWNER_PUBKEY_STATE_KEY, &our_pk)
                    .await
                    .map_err(|e| format!("pin owner: {e}"))?;
                Ok(chain)
            }
            // No pin and the chain is founded by someone else: we neither founded
            // this nor pinned an owner (join/restore pin before this runs), so this
            // is an attacker-seeded or unestablished chain. Refuse rather than adopt
            // a foreign founder on trust (closes the first-connect takeover window).
            None => Err(format!(
                "membership chain is founded by {founder}, not this device, and no \
                 owner is pinned — refusing (unestablished or foreign chain)"
            )),
        }
    }
}

/// Write the founder entry to cloud storage and pin the owner in the local DB,
/// then return the single-entry founder chain. Shared by the first-connect found
/// and the crash-recovery completion so the two writes can't drift.
#[cfg(not(target_arch = "wasm32"))]
async fn found_and_pin(
    storage: &dyn SyncStorage,
    db: &Database,
    owner_keypair: &UserKeypair,
    our_pk: &str,
    hlc: &Hlc,
) -> Result<crate::sync::membership::MembershipChain, String> {
    use super::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let ts = hlc.now().to_string();
    super::membership_ops::write_founder_entry(storage, owner_keypair, &ts)
        .await
        .map_err(|e| e.0)?;
    db.set_sync_state(OWNER_PUBKEY_STATE_KEY, our_pk)
        .await
        .map_err(|e| format!("pin owner: {e}"))?;
    info!(owner = %our_pk, "Founded library: wrote owner-anchored founder entry");
    crate::sync::membership::MembershipChain::from_entries(vec![
        crate::sync::membership::founder_entry(owner_keypair, &ts),
    ])
    .map_err(|e| format!("build founder chain: {e}"))
}

/// Components needed to run sync cycles.
///
/// The connection itself is the shared [`Database`]; the sync loop holds a clone
/// of it, so these carry only the storage, register clock, device identity, the
/// at-rest cipher, and keypair.
pub struct SyncComponents {
    pub storage: std::sync::Arc<CloudSyncStorage>,
    pub hlc: std::sync::Arc<Hlc>,
    /// The library this sync loop is for. Binds the snapshot meta/pointer it
    /// publishes so a member of two libraries can't replay one's catalog as the
    /// other's.
    pub library_id: String,
    pub device_id: String,
    pub cipher: std::sync::Arc<std::sync::RwLock<CloudCipher>>,
    pub user_keypair: UserKeypair,
}
