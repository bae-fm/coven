//! coven-owned locality transitions: `manage` (Unmanaged → Managed) and
//! `unmanage` (Managed → Unmanaged), plus `cancel_manage`.
//!
//! A blob-bearing root is Unmanaged (its blobs are the user's own files, tracked
//! as external refs, and the root's gate is off) or Managed (its blobs live in the
//! cloud fronted by coven's cache, and the gate is on). coven owns moving between
//! the two, with the durable-copy-before-delete ordering and a single atomic commit
//! point each way:
//!
//! - [`manage_blobs`] enqueues an upload per CacheLazy blob from its external file
//!   and records a [`blob_manage_intents`](crate::db) marker, then returns. The
//!   upload drain ([`crate::blob::upload::drain_uploads`]) uploads each and, on the
//!   last, takes the single commit `{remove the outbox row + flip the gate true +
//!   drop the external refs + delete the intent}`. [`cancel_manage_blobs`] undoes an
//!   in-flight manage (clears the marker + pending uploads, tombstones any blob that
//!   already landed); the gate never flips, so the root stays Unmanaged.
//! - [`unmanage_blobs`] materializes each blob back to a user file durability-first
//!   (read from cache/cloud → temp + rename + fsync → verify), then takes the single
//!   commit `{flip the gate false + register the external refs + enqueue the cloud
//!   deletes}`. The gate retract removes the subtree from peers; the tombstone drain
//!   reclaims the cloud blobs after the grace.
//!
//! The manage lifecycle (manage / cancel / the drain's completion) operates on the
//! root's **CacheLazy** blobs — the user's own files manage promotes to the cloud. A
//! CacheEager blob (e.g. cover art coven already syncs eagerly) is never touched by a
//! transition; its cloud copy belongs to the normal push/delete path.
//!
//! Every destructive step is enqueued durably *inside* the one commit, and nothing
//! destructive happens before it, so there is no representable half-state and retry
//! after a crash is idempotent.

use rusqlite::OptionalExtension;

use crate::blob::{cache, BlobRef, CacheFill};
use crate::database::{Database, DbError};
use crate::library_dir::LibraryDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use crate::sync::gate::Gates;
use crate::sync::hlc::Hlc;
use crate::sync::session::SyncedTable;

// `unmanage` (the foreground op with a cancel signal) is native-only; its types
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

/// Why a manage (or its cancel) could not be started.
#[derive(Debug, thiserror::Error)]
pub enum ManageError {
    #[error("sync is not running, so a transition cannot start")]
    SyncNotReady,
    #[error("table {0:?} is not a gated root, so it has no managed/unmanaged state")]
    NotGated(String),
    #[error("nothing to manage: root {0:?}/{1:?} has no CacheLazy blobs")]
    NothingToManage(String, String),
    #[error("blob {0:?} is not an external (unmanaged) file, so it cannot be managed")]
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

/// Why an unmanage could not complete.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
pub enum UnmanageError {
    #[error("sync is not running, so a transition cannot start")]
    SyncNotReady,
    #[error("table {0:?} is not a gated root, so it has no managed/unmanaged state")]
    NotGated(String),
    #[error("no destination path supplied for blob {0:?}")]
    MissingDest(String),
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
    #[error("unmanage cancelled before the commit; the release stays Managed")]
    Cancelled,
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

/// The CacheLazy blobs of the gated root's subtree — the user files a manage promotes
/// to the cloud, and the exact set its cancel (and the drain's completion) act on; a
/// CacheEager blob is never part of a transition. Rejects a non-gated root. Shared by
/// [`manage_blobs`] and [`cancel_manage_blobs`] (both returning a [`ManageError`]).
async fn on_demand_root_refs(
    db: &Database,
    root_table: &str,
    root_id: &str,
) -> Result<Vec<BlobRef>, ManageError> {
    let tables = db.synced_tables().to_vec();
    if gate_column(&tables, root_table).is_none() {
        return Err(ManageError::NotGated(root_table.to_string()));
    }
    Ok(
        refs_for_root(db, tables, root_table.to_string(), root_id.to_string())
            .await?
            .into_iter()
            .filter(|b| b.sync == CacheFill::CacheLazy)
            .collect(),
    )
}

/// The final cloud object key for `blob` under `scheme`. Shared by the manage,
/// cancel, and unmanage paths, each wrapping the error in its own enum.
fn cloud_key_for(scheme: BlobPathScheme, blob: &BlobRef) -> Result<String, String> {
    CloudSyncStorage::blob_key(
        scheme,
        &blob.namespace,
        &blob.id,
        blob.cloud_path.as_deref(),
    )
    .map_err(|e| e.to_string())
}

/// Start managing `(root_table, root_id)`: verify each CacheLazy blob's external
/// source file, then enqueue an upload per blob and record the manage intent in one
/// transaction. Returns once enqueued — the upload drain uploads each blob and, on
/// the last, flips the gate true (see [`crate::blob::upload`]). The caller triggers
/// a sync cycle to start the drain.
///
/// Verifying every source up front (exists + length matches the registered size)
/// means a missing file aborts with nothing enqueued, rather than leaving a
/// half-queued manage. `pin` becomes each upload's `retain_pinned`, so a managed
/// blob is kept in coven's protected cache.
pub async fn manage_blobs(
    db: &Database,
    scheme: BlobPathScheme,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
    pin: bool,
) -> Result<(), ManageError> {
    let on_demand = on_demand_root_refs(db, root_table, root_id).await?;
    if on_demand.is_empty() {
        return Err(ManageError::NothingToManage(
            root_table.to_string(),
            root_id.to_string(),
        ));
    }

    // Verify each external source and derive its cloud key up front: any miss aborts
    // before a single upload is enqueued, so a manage either queues whole or not at
    // all.
    let mut uploads: Vec<(String, String, String, crate::blob::BlobScope)> = Vec::new();
    for blob in &on_demand {
        let ext = db
            .external_blob(&blob.id)
            .await?
            .ok_or_else(|| ManageError::NotExternal(blob.id.clone()))?;
        let len = file_len(&ext.path)
            .await
            .map_err(|detail| ManageError::Source {
                blob_id: blob.id.clone(),
                path: ext.path.display().to_string(),
                detail,
            })?;
        if len != ext.size {
            return Err(ManageError::Source {
                blob_id: blob.id.clone(),
                path: ext.path.display().to_string(),
                detail: format!(
                    "length {len} no longer matches the registered size {}",
                    ext.size
                ),
            });
        }
        let cloud_key =
            cloud_key_for(scheme, blob).map_err(|e| ManageError::CloudKey(blob.id.clone(), e))?;
        let source = ext.path.to_str().ok_or_else(|| ManageError::Source {
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
        Database::insert_manage_intent_on(&tx, &root_table, &root_id)?;
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

/// Cancel an in-flight manage of `(root_table, root_id)`: delete the intent and the
/// root's pending uploads, and tombstone any CacheLazy blob that already reached the
/// cloud, all in one transaction. The gate never flips, so the root stays Unmanaged.
///
/// Scoped to the CacheLazy blobs a manage enqueues — never a CacheEager blob, whose
/// cloud copy this transition did not create and must not tombstone.
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
/// later re-manage of the same key. Tombstoning every blob here unconditionally would
/// close it but write spurious tombstones for blobs that were never uploaded (a
/// large release's worth on an early cancel), so the pending/uploaded split is the
/// deliberate, cheaper trade.
pub async fn cancel_manage_blobs(
    db: &Database,
    library_dir: &LibraryDir,
    scheme: BlobPathScheme,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
) -> Result<(), ManageError> {
    let on_demand = on_demand_root_refs(db, root_table, root_id).await?;
    // The cloud key per blob (derived outside the closure, which can't reach the
    // home's path scheme).
    let mut keyed: Vec<(String, String)> = Vec::new();
    for blob in &on_demand {
        let cloud_key =
            cloud_key_for(scheme, blob).map_err(|e| ManageError::CloudKey(blob.id.clone(), e))?;
        keyed.push((blob.id.clone(), cloud_key));
    }

    let now = hlc.now().to_string();
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    // Returns the ids of blobs that were already uploaded (so their cache copies are
    // dropped post-commit).
    let dropped: Vec<String> = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let mut dropped = Vec::new();
            for (id, cloud_key) in &keyed {
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
                    dropped.push(id.clone());
                }
            }
            Database::delete_manage_intent_on(&tx, &root_table_owned, &root_id_owned)?;
            tx.commit().map_err(DbError::from)?;
            Ok(dropped)
        })
        .await?;

    for id in dropped {
        if let Err(e) = cache::drop_cached_blob(library_dir, &id).await {
            tracing::warn!("cancel_manage: failed to drop cache copy of {id}: {e}");
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
// unmanage — native-only (foreground op with a cancel signal)
// ===========================================================================

/// Materialize a managed root's blobs back to user files, then flip it Unmanaged in
/// one atomic commit. Foreground and awaitable, with per-blob materialize progress
/// and cooperative cancellation.
///
/// Durability-first, exactly the ordering a Managed→Unmanaged transition needs: for
/// each blob, read it (cache or cloud) and write it to `dest[blob_id]` via temp +
/// rename + fsync (file and directory) + length verify, emitting progress. Only
/// after ALL blobs are durable does the single commit run: flip the gate false,
/// register each file as an external ref, and enqueue each cloud blob's delete —
/// together, so a crash can't flip the root Unmanaged while leaving the cloud blobs
/// un-tombstoned. The gate retract removes the subtree from peers next push.
///
/// Cancellation, or any failure (a missing dest, a read error, a write error)
/// before the commit, deletes the partial dest copies already written and aborts:
/// the gate is still on and the cloud is intact, so the root stays Managed and a
/// retry re-materializes cleanly.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments)]
pub async fn unmanage_blobs(
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
) -> Result<(), UnmanageError> {
    let tables = db.synced_tables().to_vec();
    let gate_col = gate_column(&tables, root_table)
        .ok_or_else(|| UnmanageError::NotGated(root_table.to_string()))?
        .to_string();

    let refs = refs_for_root(db, tables, root_table.to_string(), root_id.to_string()).await?;

    // Any error after the first dest file is written must roll those files back, so
    // an aborted unmanage leaves no partial materialization behind. `written` tracks
    // what to remove; the loop's result drives the cleanup-or-commit decision.
    let mut written: Vec<PathBuf> = Vec::new();
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
        Err(e) => {
            cleanup_partial(&written).await;
            return Err(e);
        }
    };

    // The single atomic commit: flip false + register external refs + enqueue the
    // cloud deletes, together. The destructive cloud delete is durable inside this
    // commit, so a crash right after can never leave the root Unmanaged with the
    // cloud blobs un-tombstoned.
    let stamp = hlc.now().to_string();
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    let commit = materialized.clone();
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
        for (id, namespace, path, size, cloud_key) in &commit {
            Database::register_external_blob_on(&tx, id, namespace, path, *size)?;
            Database::enqueue_delete_on(&tx, cloud_key, &stamp)?;
        }
        tx.commit().map_err(DbError::from)
    })
    .await?;

    // Post-commit, best-effort: the bytes now live at dest and the cloud blob is
    // tombstoned, so the cache copies are pure redundancy — drop them. A failure
    // leaves only stray cache space; a read serves the external file. Log and go on.
    for (id, _, _, _, _) in &materialized {
        if let Err(e) = cache::drop_cached_blob(library_dir, id).await {
            tracing::warn!("unmanage: failed to drop cache copy of {id}: {e}");
        }
    }
    if let Some(obs) = observer {
        obs.on_root_unmanaged(root_table, root_id).await;
    }
    Ok(())
}

/// Read each of `refs`'s blobs and write it durably to its `dest` path, pushing each
/// written path into `written` as it lands and returning the per-blob commit tuples
/// `(blob_id, namespace, dest, plaintext size, cloud key)`. Any error (cancel, a
/// missing dest, a read or write failure, a key-derivation failure) returns early;
/// the caller rolls back `written`. Separated from the commit so every error path
/// runs that one rollback.
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
    written: &mut Vec<PathBuf>,
) -> Result<Vec<(String, String, PathBuf, u64, String)>, UnmanageError> {
    let total = refs.len() as u64;
    let mut materialized: Vec<(String, String, PathBuf, u64, String)> = Vec::new();

    for (i, blob) in refs.iter().enumerate() {
        if *cancel.borrow() {
            return Err(UnmanageError::Cancelled);
        }
        let dest_path = dest
            .get(&blob.id)
            .ok_or_else(|| UnmanageError::MissingDest(blob.id.clone()))?;

        let bytes = cache::read_blob(db, library_dir, storage, blob)
            .await
            .map_err(|e| UnmanageError::Read(blob.id.clone(), e.to_string()))?;

        write_durable(dest_path, &bytes)
            .await
            .map_err(|detail| UnmanageError::Write {
                blob_id: blob.id.clone(),
                path: dest_path.display().to_string(),
                detail,
            })?;
        written.push(dest_path.clone());

        let cloud_key =
            cloud_key_for(scheme, blob).map_err(|e| UnmanageError::CloudKey(blob.id.clone(), e))?;
        materialized.push((
            blob.id.clone(),
            blob.namespace.clone(),
            dest_path.clone(),
            bytes.len() as u64,
            cloud_key,
        ));

        if let Some(obs) = observer {
            obs.on_blob_materialize_progress(root_table, root_id, &blob.id, (i + 1) as u64, total)
                .await;
        }
    }

    if *cancel.borrow() {
        return Err(UnmanageError::Cancelled);
    }
    Ok(materialized)
}

/// Write `bytes` to `dest` durably and atomically, then prove the new file survives
/// a crash. Composes [`crate::local_blob::write_atomic`] (temp sibling, fsynced,
/// renamed into place) for the atomic write, then verifies the destination length
/// and fsyncs the parent directory. Unlike `write_atomic` — which serves the
/// re-fetchable cache and so skips the directory fsync — an unmanaged file is the
/// ONLY copy once the cloud blob is tombstoned, so a directory-fsync failure is a
/// hard error here: it aborts the unmanage (the cloud copy is still intact) rather
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
    // Hard error (see fn doc): the unmanaged file is the only copy after the commit.
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

/// Delete the partial dest copies an aborted unmanage wrote, best-effort. An
/// already-absent file is not an error ([`crate::local_blob::remove_file`] reports
/// it as `Ok(false)`); a real removal failure is logged — the system state is
/// already correct (the root stays Managed, the cloud is intact), so a stray file in
/// the user's chosen folder is the most this can leave behind.
#[cfg(not(target_arch = "wasm32"))]
async fn cleanup_partial(written: &[PathBuf]) {
    for path in written {
        if let Err(e) = crate::local_blob::remove_file(path).await {
            tracing::warn!("unmanage cleanup: could not remove {}: {e}", path.display());
        }
    }
}
