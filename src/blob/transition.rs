//! coven-owned locality transitions: `make_remote` (Local → Remote) and
//! `make_local` (Remote → Local), plus `cancel_make_remote`.
//!
//! A blob-bearing root is **Local** (its blobs live on-device — a user-provided
//! blob at the user's path, a host-provided blob in coven's local store — and the
//! root's gate is off) or **Remote** (its blobs live in the cloud fronted by
//! coven's cache, and the gate is on). coven owns moving between the two, with the
//! durable-copy-before-delete ordering and a single atomic commit point each way:
//!
//! - [`make_remote`] enqueues an upload per **user-provided** blob from its external
//!   file and records a [`blob_make_remote_intents`](crate::db) marker, then returns.
//!   The upload drain ([`crate::blob::upload::drain_uploads`]) uploads each and, on
//!   the last, takes the single commit `{remove the outbox row + flip the gate true +
//!   drop the external refs + delete the intent}`. The gate flip re-emits the
//!   subtree, and the cycle's inline push then uploads the root's **host-provided**
//!   blobs (which coven owns, in its local store) and moves each copy into the cache.
//!   [`cancel_make_remote`] undoes an in-flight make_remote (clears the marker +
//!   pending uploads, tombstones any blob that already landed); the gate never flips,
//!   so the root stays Local.
//! - [`make_local`] brings each blob back to a local file durability-first
//!   (read from cache/cloud → write the local copy → verify): a **user-provided**
//!   blob to its `dest` path (path required) registered as an external ref, a
//!   **host-provided** blob to coven's local store (no path). Then it takes the
//!   single commit `{flip the gate false + register the external refs + enqueue the
//!   cloud deletes}`. The gate retract removes the subtree from peers; the tombstone
//!   drain reclaims the cloud blobs after the grace.
//!
//! make_remote's outbox operates on the root's **user-provided** blobs — the user's
//! own files it promotes to the cloud, and the exact set its cancel (and the drain's
//! completion) act on. A host-provided blob is uploaded by the cycle's inline push
//! once the gate flips, not via this outbox. make_local, by contrast, brings back
//! **every** blob of the root, branching per provenance.
//!
//! Every destructive step is enqueued durably *inside* the one commit, and nothing
//! destructive happens before it, so there is no representable half-state and retry
//! after a crash is idempotent.
//!
//! See the [blob concept tree](crate::blob) for how Local/Remote, provenance, and
//! the cache fit together.

use rusqlite::OptionalExtension;

use crate::blob::{cache, BlobRef, Provenance};
use crate::database::{Database, DbError};
use crate::library_dir::LibraryDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use crate::sync::gate::Gates;
use crate::sync::hlc::Hlc;
use crate::sync::session::SyncedTable;

// `make_local` (the foreground op with a cancel signal) is native-only; its types
// are too, so they don't warn unused on the wasm build that omits it.
#[cfg(not(target_arch = "wasm32"))]
use crate::blob::BlobTransitionObserver;
#[cfg(not(target_arch = "wasm32"))]
use crate::sync::storage::SyncStorage;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::watch;

/// Why a make_remote (or its cancel) could not be started.
#[derive(Debug, thiserror::Error)]
pub enum MakeRemoteError {
    #[error("sync is not running, so a transition cannot start")]
    SyncNotReady,
    #[error("table {0:?} is not a gated root, so it has no Local/Remote state")]
    NotGated(String),
    #[error("nothing to make Remote: root {0:?}/{1:?} has no user-provided blobs")]
    NothingToMakeRemote(String, String),
    #[error("blob {0:?} is not a user-provided (external) file, so it cannot be made Remote")]
    NotExternal(String),
    #[error("external source for blob {blob_id:?} at {path}: {detail}")]
    Source {
        blob_id: String,
        path: String,
        detail: String,
    },
    #[error("derive cloud key for blob {0:?}: {1}")]
    CloudKey(String, String),
    #[error("database error: {0}")]
    Db(#[from] DbError),
}

/// Why a make_local could not complete.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
pub enum MakeLocalError {
    #[error("sync is not running, so a transition cannot start")]
    SyncNotReady,
    #[error("table {0:?} is not a gated root, so it has no Local/Remote state")]
    NotGated(String),
    #[error("no destination path supplied for user-provided blob {0:?}")]
    MissingDest(String),
    #[error("destination path for user-provided blob {blob_id:?} is not valid UTF-8: {path}")]
    NonUtf8Dest { blob_id: String, path: String },
    #[error("read blob {0:?} to materialize: {1}")]
    Read(String, String),
    #[error("write materialized blob {blob_id:?} to {path}: {detail}")]
    Write {
        blob_id: String,
        path: String,
        detail: String,
    },
    #[error("derive cloud key for blob {0:?}: {1}")]
    CloudKey(String, String),
    #[error("make_local cancelled before the commit; the release stays Remote")]
    Cancelled,
    /// Rolling back a partially-materialized make_local could not remove a
    /// host-provided blob's local-store copy. Unlike a stray user-folder file, this
    /// leftover is presence-read by [`cache::read_blob`] AND budget-exempt, so it
    /// would read as a Local home for a still-Remote blob — surfaced loud so the
    /// caller retries (a retry re-materializes over it).
    #[error("could not roll back the local-store copy at {path}: {detail}")]
    CleanupLocalStore { path: String, detail: String },
    #[error("database error: {0}")]
    Db(#[from] DbError),
}

/// The gate column of `root_table`, or `None` if it is not a gated root.
fn gate_column<'a>(tables: &'a [SyncedTable], root_table: &str) -> Option<&'a str> {
    tables
        .iter()
        .find(|t| t.name() == root_table)
        .and_then(|t| t.gate_column())
}

/// Resolve the blobs of the gated root's subtree, building the gate + blob-decl
/// models from the declared set + live schema in one DB call.
async fn refs_for_root(
    db: &Database,
    tables: Vec<SyncedTable>,
    root_table: String,
    root_id: String,
) -> Result<Vec<BlobRef>, DbError> {
    db.call(move |conn| {
        let gates = Gates::from_tables(conn, &tables).map_err(|e| DbError(e.to_string()))?;
        let decls = crate::blob::decl::BlobDecls::from_tables(conn, &tables)
            .map_err(|e| DbError(e.to_string()))?;
        decls
            .refs_for_root(conn, &gates, &root_table, &root_id)
            .map_err(|e| DbError(e.to_string()))
    })
    .await
}

/// The **user-provided** blobs of the gated root's subtree — the user files a
/// make_remote promotes to the cloud via the durable outbox, and the exact set its
/// cancel (and the drain's completion) act on. A host-provided blob is not part of
/// this outbox set (the inline push uploads it once the gate flips). Rejects a
/// non-gated root. Shared by [`make_remote`] and [`cancel_make_remote`] (both
/// returning a [`MakeRemoteError`]).
async fn user_provided_root_refs(
    db: &Database,
    root_table: &str,
    root_id: &str,
) -> Result<Vec<BlobRef>, MakeRemoteError> {
    let tables = db.synced_tables().to_vec();
    if gate_column(&tables, root_table).is_none() {
        return Err(MakeRemoteError::NotGated(root_table.to_string()));
    }
    Ok(
        refs_for_root(db, tables, root_table.to_string(), root_id.to_string())
            .await?
            .into_iter()
            .filter(|b| b.provenance == Provenance::UserProvided)
            .collect(),
    )
}

/// The final cloud object key for `blob` under `scheme`. Shared by the make_remote,
/// cancel, and make_local paths, each wrapping the error in its own enum.
fn cloud_key_for(scheme: BlobPathScheme, blob: &BlobRef) -> Result<String, String> {
    CloudSyncStorage::blob_key(
        scheme,
        &blob.namespace,
        &blob.id,
        blob.cloud_path.as_deref(),
    )
    .map_err(|e| e.to_string())
}

/// Start making `(root_table, root_id)` Remote: verify each user-provided blob's
/// external source file, then enqueue an upload per blob and record the make_remote
/// intent in one transaction. Returns once enqueued — the upload drain uploads each
/// blob and, on the last, flips the gate true (see [`crate::blob::upload`]); the
/// gate flip then re-emits the subtree so the cycle's inline push uploads the root's
/// host-provided blobs. The caller triggers a sync cycle to start the drain.
///
/// Verifying every source up front (exists + length matches the registered size)
/// means a missing file aborts with nothing enqueued, rather than leaving a
/// half-queued make_remote. `pin` becomes each upload's `retain_pinned`, so the
/// blob is kept in coven's cache as a pinned (offline) copy.
pub async fn make_remote(
    db: &Database,
    scheme: BlobPathScheme,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
    pin: bool,
) -> Result<(), MakeRemoteError> {
    let user_provided = user_provided_root_refs(db, root_table, root_id).await?;
    if user_provided.is_empty() {
        return Err(MakeRemoteError::NothingToMakeRemote(
            root_table.to_string(),
            root_id.to_string(),
        ));
    }

    // Verify each external source and derive its cloud key up front: any miss aborts
    // before a single upload is enqueued, so a make_remote either queues whole or not
    // at all.
    let mut uploads: Vec<(String, String, String, crate::blob::BlobScope)> = Vec::new();
    for blob in &user_provided {
        let ext = db
            .external_blob(&blob.id)
            .await?
            .ok_or_else(|| MakeRemoteError::NotExternal(blob.id.clone()))?;
        let len = file_len(&ext.path)
            .await
            .map_err(|detail| MakeRemoteError::Source {
                blob_id: blob.id.clone(),
                path: ext.path.display().to_string(),
                detail,
            })?;
        if len != ext.size {
            return Err(MakeRemoteError::Source {
                blob_id: blob.id.clone(),
                path: ext.path.display().to_string(),
                detail: format!(
                    "length {len} no longer matches the registered size {}",
                    ext.size
                ),
            });
        }
        let cloud_key = cloud_key_for(scheme, blob)
            .map_err(|e| MakeRemoteError::CloudKey(blob.id.clone(), e))?;
        let source = ext.path.to_str().ok_or_else(|| MakeRemoteError::Source {
            blob_id: blob.id.clone(),
            path: ext.path.display().to_string(),
            detail: "external path is not valid UTF-8".to_string(),
        })?;
        uploads.push((
            blob.id.clone(),
            cloud_key,
            source.to_string(),
            blob.scope.clone(),
        ));
    }

    let created_at = hlc.now().to_string();
    let (root_table, root_id) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| {
        let tx = conn.unchecked_transaction()?;
        Database::insert_make_remote_intent_on(&tx, &root_table, &root_id)?;
        for (id, cloud_key, source, scope) in &uploads {
            Database::enqueue_upload_on(
                &tx,
                id,
                cloud_key,
                Some(source),
                scope.clone(),
                pin,
                &created_at,
            )?;
        }
        tx.commit().map_err(DbError::from)
    })
    .await?;
    Ok(())
}

/// Cancel an in-flight make_remote of `(root_table, root_id)`: delete the intent and
/// the root's pending uploads, and tombstone any user-provided blob that already
/// reached the cloud, all in one transaction. The gate never flips, so the root
/// stays Local.
///
/// Scoped to the user-provided blobs a make_remote enqueues — never a host-provided
/// blob, whose cloud copy (if any) this transition did not create via the outbox.
///
/// A blob still in flight when this runs keeps its outbox row (the drain removes it
/// only on success); the drain's completion check then finds an upload for a root
/// with no intent and tombstones that orphan. So this handles the
/// already-uploaded-by-cancel-time blobs, and the drain handles the in-flight one —
/// every uploaded blob ends up tombstoned.
///
/// The one residual window: a crash between an in-flight upload landing and the
/// drain's orphan tombstone leaves that cloud blob un-tombstoned — the same
/// network→DB-commit boundary every upload has, and the orphan is overwritten by any
/// later re-make_remote of the same key. Tombstoning every blob here unconditionally
/// would close it but write spurious tombstones for blobs that were never uploaded
/// (a large release's worth on an early cancel), so the pending/uploaded split is the
/// deliberate, cheaper trade.
pub async fn cancel_make_remote(
    db: &Database,
    library_dir: &LibraryDir,
    scheme: BlobPathScheme,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
) -> Result<(), MakeRemoteError> {
    let user_provided = user_provided_root_refs(db, root_table, root_id).await?;
    // The cloud key + cache namespace per blob (derived outside the closure, which
    // can't reach the home's path scheme; the namespace places the post-commit cache
    // drop under the segmented `storage/cache/<namespace>/<id>`).
    let mut keyed: Vec<(String, String, String)> = Vec::new();
    for blob in &user_provided {
        let cloud_key = cloud_key_for(scheme, blob)
            .map_err(|e| MakeRemoteError::CloudKey(blob.id.clone(), e))?;
        keyed.push((blob.id.clone(), blob.namespace.clone(), cloud_key));
    }

    let now = hlc.now().to_string();
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    // Returns the (id, namespace) of blobs that were already uploaded (so their cache
    // copies are dropped post-commit).
    let dropped: Vec<(String, String)> = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let mut dropped = Vec::new();
            for (id, namespace, cloud_key) in &keyed {
                let still_pending: bool = tx
                    .query_row(
                        "SELECT 1 FROM cloud_outbox WHERE operation = 'upload' AND file_id = ?1",
                        [id],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(DbError::from)?
                    .is_some();
                if still_pending {
                    // Not yet uploaded: drop its queued upload, nothing in the cloud.
                    tx.execute(
                        "DELETE FROM cloud_outbox WHERE operation = 'upload' AND file_id = ?1",
                        [id],
                    )
                    .map_err(DbError::from)?;
                } else {
                    // Already uploaded: tombstone the cloud blob and drop its cache.
                    Database::enqueue_delete_on(&tx, cloud_key, &now)?;
                    dropped.push((id.clone(), namespace.clone()));
                }
            }
            Database::delete_make_remote_intent_on(&tx, &root_table_owned, &root_id_owned)?;
            tx.commit().map_err(DbError::from)?;
            Ok(dropped)
        })
        .await?;

    for (id, namespace) in dropped {
        if let Err(e) = cache::drop_cached_blob(library_dir, &namespace, &id).await {
            tracing::warn!(
                "cancel_make_remote: failed to drop cache copy of {namespace}/{id}: {e}"
            );
        }
    }
    Ok(())
}

/// The length of the file at `path`, or an error describing why it can't be read.
async fn file_len(path: &std::path::Path) -> Result<u64, String> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::fs::metadata(path)
            .await
            .map(|m| m.len())
            .map_err(|e| e.to_string())
    }
    #[cfg(target_arch = "wasm32")]
    {
        // wasm has no std::fs metadata; read the file's length through the OPFS layer.
        crate::local_blob::read(path).await.map(|b| b.len() as u64)
    }
}

// ===========================================================================
// make_local — native-only (foreground op with a cancel signal)
// ===========================================================================

/// One blob materialized back to a local file by [`make_local`], carrying what the
/// single commit needs, split by provenance — a type distinction rather than an
/// `Option`, because provenance decides the whole materialization shape. Both
/// variants carry the [`BlobRef`] (its id + namespace) and the cloud key to
/// tombstone; only a user-provided blob carries a `dest` to register as an external
/// ref (a host-provided blob's bytes live in the local store, tracked by file
/// presence, not a DB row).
#[cfg(not(target_arch = "wasm32"))]
enum Materialized {
    UserProvided {
        blob: BlobRef,
        dest: PathBuf,
        size: u64,
        cloud_key: String,
    },
    HostProvided {
        blob: BlobRef,
        size: u64,
        cloud_key: String,
    },
}

#[cfg(not(target_arch = "wasm32"))]
impl Materialized {
    /// The materialized blob, carried by both variants. The single match the other
    /// accessors read through.
    fn blob(&self) -> &BlobRef {
        match self {
            Materialized::UserProvided { blob, .. } | Materialized::HostProvided { blob, .. } => {
                blob
            }
        }
    }

    /// The blob id, for the post-commit cache drop.
    fn blob_id(&self) -> &str {
        &self.blob().id
    }

    /// The blob's cache namespace, for the post-commit cache drop: the cache copy
    /// lives under the segmented `storage/cache/<namespace>/<id>`.
    fn namespace(&self) -> &str {
        &self.blob().namespace
    }
}

/// A local copy a make_local has written, tracked so an abort can roll it back. The
/// two kinds differ in how a *failed* removal is treated: a user-folder leftover is a
/// harmless stray (a warning), but a local-store leftover is presence-read by
/// [`cache::read_blob`] AND budget-exempt, so a failed removal of one is surfaced loud
/// (see [`cleanup_partial`]).
#[cfg(not(target_arch = "wasm32"))]
enum WrittenFile {
    /// A user-provided blob's materialized file at the user's chosen path.
    UserPath(PathBuf),
    /// A host-provided blob's copy in coven's local store.
    LocalStore(PathBuf),
}

/// Bring a Remote root's blobs back to local files, then flip it Local in one atomic
/// commit. Foreground and awaitable, with per-blob materialize progress and
/// cooperative cancellation.
///
/// Durability-first, exactly the ordering a Remote→Local transition needs: for each
/// blob, read it (cache or cloud) and write its local copy — a **user-provided**
/// blob to `dest[blob_id]` (path required), a **host-provided** blob to coven's
/// local store (no path) — each via temp + rename + fsync (file and directory) +
/// length verify, emitting progress. Only after ALL blobs are durable does the
/// single commit run: flip the gate false, register each user-provided file as an
/// external ref, and enqueue each cloud blob's delete — together, so a crash can't
/// flip the root Local while leaving the cloud blobs un-tombstoned. The gate retract
/// removes the subtree from peers next push.
///
/// `dest` carries user-provided ids only; a missing host-provided dest is not an
/// error. Cancellation, or any failure (a missing user-provided dest, a read error,
/// a write error) before the commit, deletes the partial local copies already
/// written and aborts: the gate is still on and the cloud is intact, so the root
/// stays Remote and a retry re-materializes cleanly.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments)]
pub async fn make_local(
    db: &Database,
    storage: &dyn SyncStorage,
    library_dir: &LibraryDir,
    scheme: BlobPathScheme,
    hlc: &Hlc,
    observer: Option<&dyn BlobTransitionObserver>,
    root_table: &str,
    root_id: &str,
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
) -> Result<(), MakeLocalError> {
    let tables = db.synced_tables().to_vec();
    let gate_col = gate_column(&tables, root_table)
        .ok_or_else(|| MakeLocalError::NotGated(root_table.to_string()))?
        .to_string();

    let refs = refs_for_root(db, tables, root_table.to_string(), root_id.to_string()).await?;

    // Validate every provided dest is UTF-8 up front — before any materialization — so
    // a non-UTF-8 path aborts with nothing written and the cloud intact, rather than
    // being caught only at commit-building after the destructive materialize (and on a
    // filesystem that rejects non-UTF-8 names, the write would fail first and that
    // check would never be reached). An external ref persists the path as a string, so
    // a non-UTF-8 path cannot be registered; fail loud rather than lossily rewrite it
    // and tombstone the cloud copy.
    for (blob_id, path) in dest {
        if path.to_str().is_none() {
            return Err(MakeLocalError::NonUtf8Dest {
                blob_id: blob_id.clone(),
                path: path.display().to_string(),
            });
        }
    }

    // Any error after the first local copy is written must roll those files back, so
    // an aborted make_local leaves no partial materialization behind. `written` tracks
    // what to remove (typed by kind so the rollback treats a local-store leftover
    // loud); the loop's result drives the cleanup-or-commit decision.
    let mut written: Vec<WrittenFile> = Vec::new();
    let materialized = match materialize_blobs(
        db,
        storage,
        library_dir,
        scheme,
        observer,
        root_table,
        root_id,
        &refs,
        dest,
        cancel,
        &mut written,
    )
    .await
    {
        Ok(m) => m,
        Err(e) => return Err(roll_back(&written, e).await),
    };

    // Build the per-blob commit data, converting each user-provided dest to a UTF-8
    // string FALLIBLY here — before the commit — so a non-UTF-8 path aborts cleanly
    // (the cloud is still intact, the partials rolled back) instead of being silently
    // rewritten by a lossy conversion, registered as a wrong external ref, and its
    // cloud copy tombstoned (data loss). Tuple: (id, namespace, external-ref path or
    // None, size, cloud key).
    let stamp = hlc.now().to_string();
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    let commit: Vec<(String, String, Option<String>, u64, String)> = match materialized
        .iter()
        .map(|m| -> Result<_, MakeLocalError> {
            Ok(match m {
                Materialized::UserProvided {
                    blob,
                    dest,
                    size,
                    cloud_key,
                } => {
                    let external_path =
                        dest.to_str().ok_or_else(|| MakeLocalError::NonUtf8Dest {
                            blob_id: blob.id.clone(),
                            path: dest.display().to_string(),
                        })?;
                    (
                        blob.id.clone(),
                        blob.namespace.clone(),
                        Some(external_path.to_string()),
                        *size,
                        cloud_key.clone(),
                    )
                }
                Materialized::HostProvided {
                    blob,
                    size,
                    cloud_key,
                } => (
                    blob.id.clone(),
                    blob.namespace.clone(),
                    None,
                    *size,
                    cloud_key.clone(),
                ),
            })
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(c) => c,
        Err(e) => return Err(roll_back(&written, e).await),
    };

    // The single atomic commit: flip false + register external refs (user-provided
    // only) + enqueue the cloud deletes, together. The destructive cloud delete is
    // durable inside this commit, so a crash right after can never leave the root
    // Local with the cloud blobs un-tombstoned.
    db.call(move |conn| {
        let tx = conn.unchecked_transaction()?;
        crate::sync::gate::write_gate(
            &tx,
            &root_table_owned,
            &gate_col,
            false,
            &stamp,
            &root_id_owned,
        )
        .map_err(DbError::from)?;
        for (id, namespace, external_path, size, cloud_key) in &commit {
            // A user-provided blob now lives at the user's path — register the
            // external ref. A host-provided blob lives in the local store, tracked by
            // file presence, so it registers no ref.
            if let Some(path) = external_path {
                Database::register_external_blob_on(
                    &tx,
                    id,
                    namespace,
                    std::path::Path::new(path),
                    *size,
                )?;
            }
            Database::enqueue_delete_on(&tx, cloud_key, &stamp)?;
        }
        tx.commit().map_err(DbError::from)
    })
    .await?;

    // Post-commit, best-effort: the bytes now live at their local file (a user path
    // or the local store) and the cloud blob is tombstoned, so the cache copies are
    // pure redundancy — drop them. A failure leaves only stray cache space; a read
    // serves the local file. Log and go on.
    for m in &materialized {
        if let Err(e) = cache::drop_cached_blob(library_dir, m.namespace(), m.blob_id()).await {
            tracing::warn!(
                "make_local: failed to drop cache copy of {}/{}: {e}",
                m.namespace(),
                m.blob_id()
            );
        }
    }
    if let Some(obs) = observer {
        obs.on_root_made_local(root_table, root_id).await;
    }
    Ok(())
}

/// Read each of `refs`'s blobs and write it durably to its local file, pushing each
/// written path into `written` as it lands and returning the per-blob [`Materialized`]
/// records the commit needs. A user-provided blob goes to its `dest` path (required,
/// else [`MakeLocalError::MissingDest`]); a host-provided blob goes to coven's local
/// store (no dest). Any error (cancel, a missing user-provided dest, a read or write
/// failure, a key-derivation failure) returns early; the caller rolls back `written`.
/// Separated from the commit so every error path runs that one rollback.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments)]
async fn materialize_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    library_dir: &LibraryDir,
    scheme: BlobPathScheme,
    observer: Option<&dyn BlobTransitionObserver>,
    root_table: &str,
    root_id: &str,
    refs: &[BlobRef],
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
    written: &mut Vec<WrittenFile>,
) -> Result<Vec<Materialized>, MakeLocalError> {
    let total = refs.len() as u64;
    let mut materialized: Vec<Materialized> = Vec::new();

    for (i, blob) in refs.iter().enumerate() {
        if *cancel.borrow() {
            return Err(MakeLocalError::Cancelled);
        }

        let bytes = cache::read_blob(db, library_dir, Some(storage), blob)
            .await
            .map_err(|e| MakeLocalError::Read(blob.id.clone(), e.to_string()))?;
        let cloud_key = cloud_key_for(scheme, blob)
            .map_err(|e| MakeLocalError::CloudKey(blob.id.clone(), e))?;

        // Where the blob's bytes go is its provenance's Local home: a user-provided
        // blob to the user's chosen `dest` path (registered as an external ref); a
        // host-provided blob to coven's local store (no path, no ref). The kind is
        // recorded in `written` so an abort's rollback treats a local-store leftover
        // loud.
        let record = match blob.provenance {
            Provenance::UserProvided => {
                let dest_path = dest
                    .get(&blob.id)
                    .ok_or_else(|| MakeLocalError::MissingDest(blob.id.clone()))?
                    .clone();
                write_durable(&dest_path, &bytes).await.map_err(|detail| {
                    MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: dest_path.display().to_string(),
                        detail,
                    }
                })?;
                written.push(WrittenFile::UserPath(dest_path.clone()));
                Materialized::UserProvided {
                    blob: blob.clone(),
                    dest: dest_path,
                    size: bytes.len() as u64,
                    cloud_key,
                }
            }
            Provenance::HostProvided => {
                let store_path = library_dir
                    .local_blob_path(&blob.namespace, &blob.id)
                    .map_err(|e| MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: format!("local/{}/{}", blob.namespace, blob.id),
                        detail: e.to_string(),
                    })?;
                write_durable(&store_path, &bytes).await.map_err(|detail| {
                    MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: store_path.display().to_string(),
                        detail,
                    }
                })?;
                written.push(WrittenFile::LocalStore(store_path));
                Materialized::HostProvided {
                    blob: blob.clone(),
                    size: bytes.len() as u64,
                    cloud_key,
                }
            }
        };
        materialized.push(record);

        if let Some(obs) = observer {
            obs.on_blob_materialize_progress(root_table, root_id, &blob.id, (i + 1) as u64, total)
                .await;
        }
    }

    if *cancel.borrow() {
        return Err(MakeLocalError::Cancelled);
    }
    Ok(materialized)
}

/// Write `bytes` to `dest` durably and atomically, then prove the new file survives
/// a crash. Composes [`crate::local_blob::write_atomic`] (temp sibling, fsynced,
/// renamed into place) for the atomic write, then verifies the destination length
/// and fsyncs the parent directory. Unlike `write_atomic` — which serves the
/// re-fetchable cache and so skips the directory fsync — a materialized local file is
/// the ONLY copy once the cloud blob is tombstoned, so a directory-fsync failure is a
/// hard error here: it aborts the make_local (the cloud copy is still intact) rather
/// than commit a tombstone over a destination whose entry might not survive a crash.
#[cfg(not(target_arch = "wasm32"))]
async fn write_durable(dest: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    crate::local_blob::write_atomic(dest, bytes).await?;

    // Verify the destination holds exactly the bytes we wrote — defense-in-depth
    // before the commit tombstones the cloud copy.
    let len = tokio::fs::metadata(dest)
        .await
        .map(|m| m.len())
        .map_err(|e| format!("stat dest {}: {e}", dest.display()))?;
    if len != bytes.len() as u64 {
        return Err(format!(
            "dest {} is {len} bytes after write, expected {}",
            dest.display(),
            bytes.len()
        ));
    }

    // fsync the parent dir so the rename's new entry is durable, not just the data.
    // Hard error (see fn doc): the materialized file is the only copy after the commit.
    let parent = dest
        .parent()
        .ok_or_else(|| format!("destination has no parent dir: {}", dest.display()))?;
    let dir = tokio::fs::File::open(parent)
        .await
        .map_err(|e| format!("open dest dir {} to fsync: {e}", parent.display()))?;
    dir.sync_all()
        .await
        .map_err(|e| format!("fsync dest dir {}: {e}", parent.display()))?;
    Ok(())
}

/// Roll back the partial local copies an aborted make_local wrote, then return the
/// error to surface. Returns the original `abort_err` when the rollback succeeds; if
/// the rollback itself fails to remove a local-store leftover it returns THAT
/// instead — the more urgent signal, since that leftover is a readable, budget-exempt
/// copy of a still-Remote blob (a retry re-materializes over it).
#[cfg(not(target_arch = "wasm32"))]
async fn roll_back(written: &[WrittenFile], abort_err: MakeLocalError) -> MakeLocalError {
    match cleanup_partial(written).await {
        Ok(()) => abort_err,
        Err(cleanup_err) => cleanup_err,
    }
}

/// Delete the partial local copies an aborted make_local wrote. A user-folder
/// leftover is a harmless stray — it needs an external-ref row to ever be read and
/// the aborted commit registered none — so a failed removal of one is logged and
/// swallowed. A local-store leftover is NOT harmless: [`cache::read_blob`]
/// presence-reads the local store and the budget sweep never walks it, so a stray
/// host-provided copy would read as a Local home for a still-Remote blob and never be
/// evicted. So a failed local-store removal is surfaced loud
/// ([`MakeLocalError::CleanupLocalStore`]) rather than swallowed — the caller retries
/// (the retry re-materializes over it). An already-absent file is not an error
/// ([`crate::local_blob::remove_file`] reports `Ok(false)`).
#[cfg(not(target_arch = "wasm32"))]
async fn cleanup_partial(written: &[WrittenFile]) -> Result<(), MakeLocalError> {
    for file in written {
        match file {
            WrittenFile::UserPath(path) => {
                if let Err(e) = crate::local_blob::remove_file(path).await {
                    tracing::warn!(
                        "make_local cleanup: could not remove stray user file {}: {e}",
                        path.display()
                    );
                }
            }
            WrittenFile::LocalStore(path) => {
                crate::local_blob::remove_file(path)
                    .await
                    .map_err(|detail| MakeLocalError::CleanupLocalStore {
                        path: path.display().to_string(),
                        detail,
                    })?;
            }
        }
    }
    Ok(())
}
