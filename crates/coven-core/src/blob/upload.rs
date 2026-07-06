//! The upload half of the blob engine: drain the durable upload queue, sealing
//! each blob under its scope and writing it to the cloud with coalesced progress.
//!
//! The engine owns a blob's whole local-durability lifecycle. [`cache`](super::cache)
//! is the device-local half for Remote blobs (bytes on disk, pin/unpin, eviction);
//! this is the cloud half (local-only → uploaded). A make_remote enqueues an upload
//! row per user-provided blob on `(operation, cloud_key)`, and the sync cycle calls
//! [`drain_uploads`] each round before it pushes: the drain reads each pending row,
//! seals the local plaintext under the blob's scope, and writes it to the cloud.
//!
//! The queue persists the **final cloud object key** the host built at enqueue
//! (the `cloud_key` column), not the `(namespace, id, cloud_path)` a [`BlobRef`]
//! carries, so the drain replays that key verbatim through
//! [`CloudHome::write`](crate::storage::cloud::CloudHome::write) rather than
//! re-deriving it through [`SyncStorage::put_blob`](crate::sync::storage::SyncStorage::put_blob).
//! That write is also the only seam that threads the chunked `progress` closure
//! the per-file progress bar needs — `put_blob` discards progress. So the upload
//! seals with the same [`CloudCipher::seal_scoped`] the storage layer uses (the
//! one sealing point both directions share, via
//! [`encryption_for_scope`](crate::sync::cloud_storage::encryption_for_scope)),
//! but writes through `CloudHome` directly. The read path
//! ([`cache::read_blob`](super::cache::read_blob)) goes through `SyncStorage`
//! because it has the `BlobRef` and resolves coordinates to a key per call; the
//! upload path replays a key already committed to the durable queue.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{params_from_iter, Connection, ToSql};
use tracing::warn;

use crate::blob::decl::BlobDecls;
use crate::blob::{BlobScope, BlobTransitionObserver, Provenance};
use crate::database::{Database, DbError};
use crate::db::{OutboxEntry, OutboxOperation};
use crate::library_dir::LibraryDir;
use crate::storage::cloud::{BlobBody, CloudHome, CloudHomeError};
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::gate::Gates;
use crate::sync::hlc::Hlc;

/// How often the upload pipeline forwards a mid-file byte count to the observer.
/// coven's `write` reports per chunk (every few MiB), which on a fast link can be
/// many times a second; coalescing to this interval keeps the host from
/// rebuilding its outbox snapshot on every chunk while still moving the bar
/// smoothly. Uses the same fixed progress tick as the download path.
const PROGRESS_TICK: Duration = Duration::from_millis(300);

/// Run one blob upload while forwarding coalesced byte progress to the observer.
///
/// coven's `CloudHome::write` reports cumulative bytes through a synchronous
/// `progress` closure as each chunk lands. That closure can't await, so it just
/// stores the latest count in `sent`; a concurrent ticker reads `sent` every
/// [`PROGRESS_TICK`] and makes the async `on_blob_upload_progress` call. The
/// ticker stops as soon as `write` returns, then a final forward emits the
/// terminal count so the bar reaches 100% even if the last chunk landed between
/// ticks. With no observer the ticker is skipped entirely.
async fn upload_with_progress(
    cloud_home: &dyn CloudHome,
    cloud_key: &str,
    file_id: &str,
    body: BlobBody,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<(), CloudHomeError> {
    let total = body.len();
    let sent = Arc::new(AtomicU64::new(0));
    let progress = {
        let sent = sent.clone();
        move |n: u64| sent.store(n, Ordering::Relaxed)
    };

    let write = cloud_home.write(cloud_key, body, &progress);

    let Some(obs) = observer else {
        return write.await;
    };

    // Forward `sent` to the observer on a fixed tick until the write completes,
    // skipping ticks where nothing advanced. Runs concurrently with the write
    // on the same task; both borrow `obs` and `sent`.
    tokio::pin!(write);
    let mut ticker = tokio::time::interval(PROGRESS_TICK);
    ticker.tick().await; // first tick fires immediately; consume it.
    let mut last_forwarded = 0u64;
    let result = loop {
        tokio::select! {
            r = &mut write => break r,
            _ = ticker.tick() => {
                let now = sent.load(Ordering::Relaxed);
                if now != last_forwarded {
                    last_forwarded = now;
                    obs.on_blob_upload_progress(file_id, now, total).await;
                }
            }
        }
    };

    // Terminal forward: on success the file is fully uploaded, so report the
    // full size even if the last chunk landed between ticks. On failure leave
    // the last observed count — the entry stays queued and will retry.
    if result.is_ok() {
        obs.on_blob_upload_progress(file_id, total, total).await;
    }
    result
}

/// Minimum delay before a failed upload entry is retried, keyed on its prior
/// `attempt_count`. Exponential (`30s · 2^(n-1)`) capped at one hour: the base
/// equals the sync-loop interval so the first retry rides the next natural
/// cycle, and the cap keeps a persistently-failing entry retrying hourly rather
/// than every cycle. A freshly-queued entry (`attempt_count == 0`) is eligible
/// immediately.
pub(crate) fn backoff_window(attempt_count: i64) -> chrono::Duration {
    if attempt_count <= 0 {
        return chrono::Duration::zero();
    }
    let n = (attempt_count - 1) as u32;
    chrono::Duration::seconds(crate::sync::backoff::backoff_secs(n, 3600) as i64)
}

/// Record a failed upload attempt and notify the observer. The entry is left
/// queued; it becomes eligible for retry again after [`backoff_window`]. Uploads
/// additionally notify the observer, so the caller passes the upload's `file_id`.
async fn record_failure(
    db: &Database,
    entry: &OutboxEntry,
    file_id: &str,
    error: &str,
    now: chrono::DateTime<chrono::Utc>,
    observer: Option<&dyn BlobTransitionObserver>,
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
        obs.on_blob_upload_failed(file_id, error).await;
    }
}

/// Resolve an upload's scope to a key and open a streaming [`BlobBody`] over its
/// local plaintext file — sealing each chunk under the scope's key for an
/// encrypted home, or passing the plaintext through for a browsable one — without
/// ever reading or sealing the whole blob into memory. The cloud write consumes
/// the body chunk by chunk; a `retain_pinned` upload pins by stream-copying the
/// same source file afterward (it never holds the plaintext in RAM).
///
/// The two failure modes — the scope can't be resolved to a key (a missing
/// `item_keys` row), or the local file can't be opened — both surface as an `Err`
/// carrying a host-readable message, so the upload loop has one failure path
/// (warn + record + skip) instead of one per step. The scope is resolved at drain
/// (not enqueue) because the key may be minted/synced after the blob was queued,
/// and an `Item` scope reads `item_keys` here, holding `db`.
async fn resolve_and_open(
    db: &Database,
    cipher: &std::sync::RwLock<CloudCipher>,
    file_path: &Path,
    library_id: &str,
    cloud_key: &str,
    scope: BlobScope,
) -> Result<BlobBody, String> {
    let resolved = db
        .resolve_blob_scope(scope)
        .await
        .map_err(|e| format!("cannot resolve blob scope: {e}"))?;
    // Snapshot the cipher out of the lock so the (streaming, await-holding) body
    // doesn't keep the guard across the upload.
    let cipher = cipher.read().unwrap().clone();
    let aad_context = crate::sync::cloud_storage::cloud_aad_context(library_id, cloud_key);
    cipher.open_body(resolved, file_path, &aad_context).await
}

/// The result of one upload-queue drain pass.
pub struct DrainOutcome {
    /// Number of successful uploads this pass.
    pub uploaded: usize,
    /// The drain stopped early because an upload just *completed a make_remote*: the
    /// last of a gated root's user-provided blobs landed, so coven flipped the gate true
    /// and broke the drain so this cycle publishes the now-shareable subtree (and
    /// the loop runs the next cycle promptly to drain any other root's blobs).
    /// `false` when the queue drained in one pass (or stopped on a pause / left
    /// only backed-off entries), so the loop waits its normal interval.
    pub yielded_for_publish: bool,
}

/// Drain pending blob uploads: read each local file, seal it under its scope,
/// write it to the cloud — and, for an entry marked `retain_pinned`, keep the
/// plaintext in the protected local cache (`storage/pinned/<id>`) so the blob
/// stays local without a later fetch.
///
/// A failing entry is recorded and skipped rather than stopping the drain, with a
/// per-entry backoff so a persistently-failing entry doesn't block the rest of
/// the queue or get re-attempted every cycle. The `observer` (if any) is notified
/// as each attempt starts, succeeds, or fails.
///
/// coven owns the make_remote *completion*: after a successful upload it walks the
/// blob's row up to its gated root, and if that root has a `blob_make_remote_intents`
/// row with no other pending upload, takes the single atomic commit
/// `{remove the final outbox row + flip the gate true + drop the root's external
/// refs + delete the intent}` and breaks the drain so this cycle publishes the
/// re-emitted subtree (the returned [`DrainOutcome::yielded_for_publish`]).
/// Removing the final outbox row *inside* that commit is the crash-safety
/// invariant: until it commits the row is present, so a crash re-runs the
/// (idempotent overwrite) upload and retries the flip — there is no window where
/// the blobs are up but the gate isn't, with nothing queued to drive it. An upload
/// whose root has NO intent is an orphan from a cancelled make_remote: it is
/// tombstoned (and its cache dropped) rather than flipped.
pub async fn drain_uploads(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    library_id: &str,
    library_dir: &LibraryDir,
    clock: &dyn crate::clock::Clock,
    hlc: &Hlc,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<DrainOutcome, String> {
    let uploads = db
        .get_pending_cloud_uploads()
        .await
        .map_err(|e| format!("Failed to get pending uploads: {e}"))?;

    let now = clock.now();
    let now_rfc = now.to_rfc3339();
    let mut count = 0;
    let mut yielded_for_publish = false;

    // The blob/gate model the make_remote completion check reads, resolved once from
    // the declared set + live schema (the schema can't change mid-drain). An upload's
    // row → gated root → its `blob_make_remote_intents` lookup all run off these. The
    // gate-column map lets the flip name the root's gate column. Built only when
    // there is something to drain; a build failure aborts the drain (nothing was
    // uploaded, the rows stay queued, the next cycle retries).
    let (gates, blob_decls, gate_columns) = if uploads.is_empty() {
        return Ok(DrainOutcome {
            uploaded: 0,
            yielded_for_publish: false,
        });
    } else {
        build_transition_models(db).await?
    };
    for entry in uploads {
        // Host-driven pause: short-circuit before pulling the next entry so a
        // freshly paused queue stops draining without aborting an in-flight
        // upload. Checked per entry so resume mid-cycle picks back up.
        if let Some(obs) = observer {
            if obs.should_skip_uploads() {
                break;
            }
        }
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

        // Every row from `get_pending_cloud_uploads` is an `Upload` (the query
        // filters `operation = 'upload'`); destructure the upload-only fields. A
        // `Delete` here would be a broken query invariant, not a skippable row.
        let OutboxOperation::Upload {
            file_id,
            source_path,
            scope,
            retain_pinned,
        } = &entry.operation
        else {
            unreachable!("get_pending_cloud_uploads returns only Upload rows");
        };

        // The cache namespace this blob's copy lives under is the first component of
        // its durable cloud key (see [`namespace_from_cloud_key`]); the outbox stores
        // the key, not a second copy of the namespace. Used by the pinned-cache
        // populate and the cancelled-make_remote orphan drop below.
        let namespace = crate::sync::cloud_storage::namespace_from_cloud_key(&entry.cloud_key);

        if let Some(obs) = observer {
            obs.on_blob_upload_started(file_id).await;
        }

        let file_path = match source_path {
            Some(p) => std::path::PathBuf::from(p),
            None => match crate::storage::local::storage_path(file_id) {
                Ok(rel) => library_dir.join(rel),
                Err(e) => {
                    // A locally-enqueued upload id that can't form a storage path is
                    // a host bug, not attacker data; record it as this entry's
                    // failure and keep draining the rest rather than aborting.
                    let msg = format!("invalid upload file id: {e}");
                    warn!(
                        "Upload failed for {} (file_id {file_id}): {msg}",
                        entry.cloud_key
                    );
                    record_failure(db, &entry, file_id, &msg, now, observer).await;
                    continue;
                }
            },
        };

        // Resolve the scope to a key (an `Item` scope reads `item_keys` here,
        // holding `db`) and open a streaming body over the local plaintext — the
        // body seals each chunk as it uploads, never holding the whole blob. A
        // missing key is a host bug; record it as a failure rather than silently
        // sealing under the master key (which no share recipient could read).
        let body = match resolve_and_open(
            db,
            cipher,
            &file_path,
            library_id,
            &entry.cloud_key,
            scope.clone(),
        )
        .await
        {
            Ok(body) => body,
            Err(msg) => {
                warn!("Upload failed for {}: {msg}", entry.cloud_key);
                record_failure(db, &entry, file_id, &msg, now, observer).await;
                continue;
            }
        };

        match upload_with_progress(cloud_home, &entry.cloud_key, file_id, body, observer).await {
            Ok(()) => {
                count += 1;

                // A pinned upload keeps its blob local: stream-copy the source
                // plaintext file into the protected cache folder
                // (`storage/pinned/<id>`), so the blob is budget-exempt and serves
                // from disk with no later cloud round-trip — and a large pinned blob
                // is never materialized in RAM. Best-effort, like the cache's own
                // post-populate eviction: the upload has already succeeded and the
                // bytes are in the cloud, so a populate failure is logged and
                // swallowed (a later read re-fetches into the cache) rather than
                // failing a completed upload.
                if *retain_pinned {
                    if let Err(e) = crate::blob::cache::populate_pinned(
                        library_dir,
                        namespace,
                        file_id,
                        &file_path,
                    )
                    .await
                    {
                        warn!(
                            "Upload of {} succeeded but pinning it into the local cache (namespace {namespace}) failed (a later read will re-fetch): {e}",
                            entry.cloud_key
                        );
                    }
                }

                if let Some(obs) = observer {
                    obs.on_blob_uploaded(file_id).await;
                }

                // A successful write wins over any pending deletion: remove the
                // tombstone a prior cycle (possibly another device) wrote for this
                // key, so the GC won't reclaim the blob we just re-uploaded — the
                // round-trip re-make_remote case (a prior make_local tombstoned this key).
                // The enqueue layer already drops a same-device pending delete row;
                // this covers a tombstone already committed to the cloud. The cancel
                // must not be lost if it fails (a surviving tombstone past its grace
                // deletes this live blob), so on failure the post-upload commit below
                // persists a durable `cancel` row atomically with removing the upload
                // row; the tombstone-cancel drain retries until the tombstone is gone.
                let suffix = cipher.read().unwrap().suffix();
                let cancel_failed = if let Err(e) =
                    crate::blob::delete::cancel_tombstone(cloud_home, suffix, &entry.cloud_key)
                        .await
                {
                    warn!(
                        "tombstone cancel for {} failed ({e}); queuing a durable cancel for retry",
                        entry.cloud_key
                    );
                    true
                } else {
                    false
                };

                // The post-upload commit: mint the gate-flip stamp off coven's HLC
                // (the same register the host stamps rows from, so the flip sorts
                // causally and is captured into this cycle's changeset), then in one
                // DB call complete a make_remote (flip), continue, or tombstone an orphan.
                let stamp = hlc.now().to_string();
                let outcome = commit_after_upload(
                    db,
                    gates.clone(),
                    blob_decls.clone(),
                    gate_columns.clone(),
                    file_id.to_string(),
                    entry.cloud_key.clone(),
                    entry.id,
                    cancel_failed,
                    stamp,
                    now_rfc.clone(),
                )
                .await;

                match outcome {
                    Ok(PostUpload::Continued) => {}
                    Ok(PostUpload::Orphan) => {
                        // A cancelled make_remote left this blob uploaded with no intent;
                        // it is tombstoned in the commit above. Drop its local cache
                        // copy too (a `retain_pinned` upload populated `pinned/`,
                        // which is budget-exempt and would otherwise leak).
                        if let Err(e) =
                            crate::blob::cache::drop_cached_blob(library_dir, namespace, file_id)
                                .await
                        {
                            warn!("failed to drop the cache copy of orphaned blob {namespace}/{file_id}: {e}");
                        }
                    }
                    Ok(PostUpload::MadeRemote {
                        root_table,
                        root_id,
                    }) => {
                        // The gate is flipped, the bytes are in the cloud (and pinned
                        // cache), and the external ref is dropped in the commit above.
                        // A user-provided blob is the user's own file, referenced in
                        // place — coven never deletes the user's original on disk.
                        if let Some(obs) = observer {
                            obs.on_root_made_remote(&root_table, &root_id).await;
                        }
                        // Break so this cycle publishes the now-shareable subtree;
                        // any other root's blobs drain on the promptly-run next cycle.
                        yielded_for_publish = true;
                        break;
                    }
                    Err(e) => {
                        // The upload landed but the bookkeeping commit failed. The
                        // outbox row is still present (the commit is all-or-nothing),
                        // so the next cycle re-runs the idempotent upload and retries
                        // the commit — no half-state. Log and keep draining.
                        warn!(
                            "post-upload commit for {} failed; will retry next cycle: {e}",
                            entry.cloud_key
                        );
                    }
                }
            }
            Err(e) => {
                let msg = format!("cloud write failed: {e}");
                warn!("Upload failed for {}: {msg}", entry.cloud_key);
                record_failure(db, &entry, file_id, &msg, now, observer).await;
                continue;
            }
        }
    }

    Ok(DrainOutcome {
        uploaded: count,
        yielded_for_publish,
    })
}

/// What the post-upload commit did, so the drain can run the cloud-/filesystem-side
/// effects (which can't run inside the DB transaction) and decide whether to break.
enum PostUpload {
    /// A plain upload (no gated root) or a non-final make_remote upload: the outbox
    /// row was removed. Nothing more for the drain to do.
    Continued,
    /// The blob was uploaded but its gated root has no make_remote intent — an orphan
    /// from a cancelled make_remote. Its cloud delete was enqueued (tombstone) and
    /// its outbox row removed in the commit; the drain drops its local cache copy.
    Orphan,
    /// This upload completed a make_remote: the single atomic commit flipped the gate
    /// true, dropped the root's external refs, removed the final outbox row, and
    /// deleted the intent. The drain notifies `on_root_made_remote`, then breaks to
    /// publish the subtree. The user's own source files are referenced in place and
    /// left untouched — only the external ref is dropped, never the file.
    MadeRemote { root_table: String, root_id: String },
}

/// Build the gate model, blob declarations, and the root→gate-column map the
/// make_remote completion check reads, once per drain pass. All three resolve off
/// the declared synced-table set + the live schema, so they're built together in
/// one DB call.
async fn build_transition_models(
    db: &Database,
) -> Result<(Arc<Gates>, Arc<BlobDecls>, Arc<HashMap<String, String>>), String> {
    let tables = db.synced_tables().to_vec();
    let gate_columns: HashMap<String, String> = tables
        .iter()
        .filter_map(|t| {
            t.gate_column()
                .map(|c| (t.name().to_string(), c.to_string()))
        })
        .collect();
    db.call(move |conn| {
        let gates = Gates::from_tables(conn, &tables).map_err(|e| DbError(e.to_string()))?;
        let decls = BlobDecls::from_tables(conn, &tables).map_err(|e| DbError(e.to_string()))?;
        Ok((Arc::new(gates), Arc::new(decls), Arc::new(gate_columns)))
    })
    .await
    .map_err(|e| format!("build blob-transition models: {e}"))
}

/// The post-upload bookkeeping for one successful upload, as one DB call so the
/// completion check and the flip it gates are atomic: map the blob to its row and
/// gated root, then either complete a make_remote (the single atomic flip commit),
/// continue (remove the row), or tombstone an orphan.
#[allow(clippy::too_many_arguments)]
async fn commit_after_upload(
    db: &Database,
    gates: Arc<Gates>,
    decls: Arc<BlobDecls>,
    gate_columns: Arc<HashMap<String, String>>,
    blob_id: String,
    cloud_key: String,
    final_outbox_id: i64,
    cancel_failed: bool,
    stamp: String,
    now_rfc: String,
) -> Result<PostUpload, DbError> {
    let tables = db.synced_tables().to_vec();
    db.call_with_capture_reset(move |conn| {
        // Map the uploaded blob to its row, then up to its gated root. The blob's
        // namespace is the first component of its durable cloud key, so the row is
        // looked up in exactly that namespace's table (never a scan-all that could
        // first-match a colliding id in another table). A blob with no backing row, or
        // whose row resolves to no gated root, is a plain upload (e.g. a drain test):
        // just remove the row.
        let namespace = crate::sync::cloud_storage::namespace_from_cloud_key(&cloud_key);
        let root = match decls
            .row_for_blob_in_namespace(conn, namespace, &blob_id)
            .map_err(|e| DbError(e.to_string()))?
        {
            Some((table, pk)) => gates
                .resolve_root_of(conn, &table, &pk)
                .map_err(|e| DbError(e.to_string()))?,
            None => None,
        };
        let Some((root_table, root_id)) = root else {
            commit_finish(conn, final_outbox_id, &cloud_key, cancel_failed, &now_rfc)?;
            return Ok(PostUpload::Continued);
        };

        // No intent ⇒ a cancelled make_remote left this blob orphaned in the cloud.
        // Tombstone it (enqueue_delete drops the upload row and adds the delete), in
        // one commit so a crash can't drop the row while leaving no tombstone.
        if !Database::make_remote_intent_exists(conn, &root_table, &root_id)? {
            let tx = conn.unchecked_transaction()?;
            Database::enqueue_delete_on(&tx, &cloud_key, &now_rfc)?;
            tx.commit().map_err(DbError::from)?;
            return Ok(PostUpload::Orphan);
        }

        // A make_remote is in flight for this root. It completes iff no OTHER
        // user-provided blob of its subtree still has a pending upload (this entry's
        // row is still present — it's removed inside the flip commit below). Scoped to
        // user-provided: those are the blobs make_remote enqueues, so a host-provided
        // blob's inline-push upload neither blocks completion nor gets an external ref
        // cleared here.
        let refs = decls
            .refs_for_root(conn, &gates, &root_table, &root_id)
            .map_err(|e| DbError(e.to_string()))?;
        let blob_ids: Vec<String> = refs
            .iter()
            .filter(|b| b.provenance == Provenance::UserProvided)
            .map(|b| b.id.clone())
            .collect();
        let has_host_provided = refs
            .iter()
            .any(|b| b.provenance == Provenance::HostProvided);
        if count_other_pending_uploads(conn, &blob_ids, final_outbox_id)? > 0 {
            // Not the last blob: remove this row, leave the gate off until the rest land.
            commit_finish(conn, final_outbox_id, &cloud_key, cancel_failed, &now_rfc)?;
            return Ok(PostUpload::Continued);
        }

        // Host-provided blobs still need to land before the root may publish. The
        // pre-capture host-provided completion sees the intent with no pending
        // user-provided uploads, uploads those local-store blobs, then flips the
        // gate and clears the external refs. Removing this final outbox row is safe:
        // the intent remains as the durable driver for the remaining completion.
        if has_host_provided {
            commit_finish(conn, final_outbox_id, &cloud_key, cancel_failed, &now_rfc)?;
            return Ok(PostUpload::Continued);
        }

        // The last blob and no host-provided blobs: the SINGLE atomic commit.
        // Removing the final outbox row here (not before) is the crash-safety
        // invariant — until commit the row is present, so a crash re-runs the
        // idempotent upload and retries this flip.
        let gate_col = gate_columns.get(&root_table).ok_or_else(|| {
            DbError(format!(
                "make_remote completion: gated root {root_table} has no gate column"
            ))
        })?;
        Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
            finish_outbox_row(tx, final_outbox_id, &cloud_key, cancel_failed, &now_rfc)?;
            crate::sync::gate::write_gate(tx, &root_table, gate_col, true, &stamp, &root_id)
                .map_err(DbError::from)?;
            for id in &blob_ids {
                Database::clear_external_blob_on(tx, id)?;
            }
            Database::delete_make_remote_intent_on(tx, &root_table, &root_id)?;
            Ok(PostUpload::MadeRemote {
                root_table,
                root_id,
            })
        })
    })
    .await
}

/// The non-completing post-upload outcome: remove the upload's outbox row (and, on a
/// failed tombstone-cancel, its durable `cancel` row) in one transaction, so the two
/// statements commit together. The `Continued` branches — a plain upload with no
/// gated root, and a non-final make_remote upload — share this single commit shape.
fn commit_finish(
    conn: &Connection,
    id: i64,
    cloud_key: &str,
    cancel_failed: bool,
    now_rfc: &str,
) -> Result<(), DbError> {
    let tx = conn.unchecked_transaction()?;
    finish_outbox_row(&tx, id, cloud_key, cancel_failed, now_rfc)?;
    tx.commit().map_err(DbError::from)
}

/// Remove a completed upload's outbox row. If the inline tombstone-cancel failed,
/// also enqueue a durable `cancel` row so the tombstone-cancel drain retries. Every
/// caller runs this inside its own transaction, so the row removal and the cancel
/// row commit together — a crash can't drop the upload while leaving the tombstone.
fn finish_outbox_row(
    conn: &Connection,
    id: i64,
    cloud_key: &str,
    cancel_failed: bool,
    now_rfc: &str,
) -> Result<(), DbError> {
    conn.execute("DELETE FROM cloud_outbox WHERE id = ?1", [id])
        .map_err(DbError::from)?;
    if cancel_failed {
        conn.execute(
            "INSERT OR IGNORE INTO cloud_outbox (operation, cloud_key, scope, created_at) \
             VALUES ('cancel', ?1, NULL, ?2)",
            (cloud_key, now_rfc),
        )
        .map_err(DbError::from)?;
    }
    Ok(())
}

/// How many pending uploads remain for the given blob ids, excluding the row
/// `exclude_id` (the just-uploaded entry, whose row is removed inside the flip
/// commit). Zero means this upload was the last of the make_remote's subtree.
fn count_other_pending_uploads(
    conn: &Connection,
    blob_ids: &[String],
    exclude_id: i64,
) -> Result<i64, DbError> {
    if blob_ids.is_empty() {
        return Ok(0);
    }
    let placeholders = vec!["?"; blob_ids.len()].join(", ");
    let sql = format!(
        "SELECT COUNT(*) FROM cloud_outbox \
         WHERE operation = 'upload' AND id != ?1 AND file_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql).map_err(DbError::from)?;
    let params = count_other_pending_upload_params(&exclude_id, blob_ids);
    stmt.query_row(params_from_iter(params), |r| r.get::<_, i64>(0))
        .map_err(DbError::from)
}

fn count_other_pending_upload_params<'a>(
    exclude_id: &'a i64,
    blob_ids: &'a [String],
) -> Vec<&'a dyn ToSql> {
    let mut params: Vec<&dyn ToSql> = Vec::with_capacity(blob_ids.len() + 1);
    params.push(exclude_id);
    params.extend(blob_ids.iter().map(|id| id as &dyn ToSql));
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_other_pending_uploads_excludes_current_row_and_counts_matching_blob_ids() {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(crate::db::MIGRATION_SQL)
            .expect("create bookkeeping schema");
        for (id, operation, file_id, cloud_key) in [
            (1, "upload", "blob-a", "key-a-current"),
            (2, "upload", "blob-a", "key-a-other"),
            (3, "upload", "blob-b", "key-b"),
            (4, "delete", "blob-a", "key-a-delete"),
        ] {
            conn.execute(
                "INSERT INTO cloud_outbox (id, operation, file_id, cloud_key, scope, created_at) \
                 VALUES (?1, ?2, ?3, ?4, 'master', '2024-01-01T00:00:00Z')",
                (id, operation, file_id, cloud_key),
            )
            .expect("insert outbox row");
        }

        let blob_ids = vec!["blob-a".to_string()];
        let count = count_other_pending_uploads(&conn, &blob_ids, 1).expect("count uploads");

        assert_eq!(count, 1);
    }

    #[test]
    fn count_other_pending_upload_params_keep_database_types() {
        let conn = Connection::open_in_memory().expect("open sqlite");
        let exclude_id = 1_i64;
        let blob_ids = vec!["blob-a".to_string()];
        let params = count_other_pending_upload_params(&exclude_id, &blob_ids);

        let (id_type, blob_type): (String, String) = conn
            .query_row(
                "SELECT typeof(?1), typeof(?2)",
                params_from_iter(params),
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("inspect parameter types");

        assert_eq!(id_type, "integer");
        assert_eq!(blob_type, "text");
    }
}
