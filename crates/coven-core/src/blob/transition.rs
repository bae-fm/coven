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
//!   The upload drain ([`crate::blob::upload::drain_uploads`]) uploads each. Once no
//!   user-provided uploads remain, the sync cycle uploads the root's
//!   **host-provided** blobs (which coven owns, in its local store), then takes the
//!   single commit `{flip the gate true + drop external refs + delete the intent}`.
//!   The gate flip re-emits the subtree, and local host-provided copies move into
//!   the cache.
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
//! own files it promotes to the cloud, and the exact set its cancel acts on. A
//! host-provided blob is uploaded by the pre-capture cycle completion, not via this
//! outbox. make_local, by contrast, brings back **every** blob of the root,
//! branching per provenance.
//!
//! A [`SyncedTable::remote_root`](crate::sync::session::SyncedTable::remote_root)
//! has no Local state in this model: its rows sync normally and its blobs are
//! Remote by construction, so these transition APIs reject it.
//!
//! Every destructive step is enqueued durably *inside* the one commit, and nothing
//! destructive happens before it, so there is no representable half-state and retry
//! after a crash is idempotent.
//!
//! See the [blob concept tree](crate::blob) for how Local/Remote, provenance, and
//! the cache fit together.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use crate::blob::{cache, BlobRef, Provenance};
use crate::database::{Database, DbError};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use crate::sync::gate::Gates;
use crate::sync::hlc::Hlc;
use crate::sync::service::DeferredLocalBlobDrop;
use crate::sync::session::SyncedTable;

// `make_local` (the foreground op with a cancel signal) is native-only; its types
// are too, so they don't warn unused on the wasm build that omits it.
#[cfg(not(target_arch = "wasm32"))]
use crate::blob::BlobTransitionObserver;
#[cfg(not(target_arch = "wasm32"))]
use crate::sync::storage::SyncStorage;
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
    #[error("table {0:?} is a remote root, so its blobs are already Remote")]
    RemoteRoot(String),
    #[error("root {0:?}/{1:?} is already Remote, so make_remote has nothing to do")]
    AlreadyRemote(String, String),
    #[error("root {0:?}/{1:?} has no resolvable Local/Remote state (row absent or gate NULL)")]
    UnresolvedLocality(String, String),
    #[error("nothing to make Remote: root {0:?}/{1:?} has no blobs")]
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
    #[error("table {0:?} is a remote root, so its blobs have no Local state")]
    RemoteRoot(String),
    #[error("root {0:?}/{1:?} is already Local, so make_local has nothing to do")]
    AlreadyLocal(String, String),
    #[error("root {0:?}/{1:?} has no resolvable Local/Remote state (row absent or gate NULL)")]
    UnresolvedLocality(String, String),
    #[error("no destination path supplied for user-provided blob {0:?}")]
    MissingDest(String),
    #[error("destination path for user-provided blob {blob_id:?} is not valid UTF-8: {path}")]
    NonUtf8Dest { blob_id: String, path: String },
    #[error("destination already exists for blob {blob_id:?}: {path}")]
    DestinationExists { blob_id: String, path: String },
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
    #[error("could not roll back the created destination at {path}: {detail}")]
    CleanupUserPath { path: String, detail: String },
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

fn is_remote_root(tables: &[SyncedTable], root_table: &str) -> bool {
    tables
        .iter()
        .any(|t| t.name() == root_table && t.is_remote_root())
}

/// The `root_table → gate_column` map for every gated root in `tables` — the lookup a
/// make_remote completion uses to name the root's gate column. Built off the declared
/// synced-table set + live schema, so it is stable across a cycle. Shared by both
/// completion paths (the upload drain and the sync cycle's host-provided completion).
pub(crate) fn gate_columns(tables: &[SyncedTable]) -> HashMap<String, String> {
    tables
        .iter()
        .filter_map(|t| {
            t.gate_column()
                .map(|c| (t.name().to_string(), c.to_string()))
        })
        .collect()
}

/// Whether any of `blob_ids` still has a pending `upload` outbox row, optionally
/// excluding one row by id. The upload drain excludes the just-finished upload's own
/// row — still present until the flip commit removes it — to ask whether any OTHER
/// blob of the subtree is still uploading; the sync cycle's host-provided completion
/// passes `None` to ask whether any user-provided blob is still uploading at all. Zero
/// pending means every user-provided upload of the make_remote's subtree has landed.
pub(crate) fn pending_upload_exists(
    conn: &Connection,
    blob_ids: &[String],
    exclude_id: Option<i64>,
) -> Result<bool, DbError> {
    for id in blob_ids {
        // `exclude_id` binds as NULL when `None`, so `?2 IS NULL` disables the
        // exclusion and every matching upload row counts.
        let found = conn
            .query_row(
                "SELECT 1 FROM cloud_outbox \
                 WHERE operation = 'upload' AND file_id = ?1 AND (?2 IS NULL OR id != ?2) LIMIT 1",
                rusqlite::params![id, exclude_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(DbError::from)?;
        if found.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
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

/// Validate that `root_table` is a coven-owned gated root (rejecting a remote root
/// and a non-gated table) and return its gate column. The gate column names the row
/// whose truth is the root's Local/Remote state — [`make_remote`] reads it to refuse
/// a root already Remote.
fn gated_root_gate_col(
    tables: &[SyncedTable],
    root_table: &str,
) -> Result<String, MakeRemoteError> {
    if is_remote_root(tables, root_table) {
        return Err(MakeRemoteError::RemoteRoot(root_table.to_string()));
    }
    gate_column(tables, root_table)
        .map(str::to_string)
        .ok_or_else(|| MakeRemoteError::NotGated(root_table.to_string()))
}

/// The blobs of the gated root's subtree. Rejects a non-gated root. Shared by
/// [`make_remote`] and [`cancel_make_remote`].
async fn root_refs(
    db: &Database,
    root_table: &str,
    root_id: &str,
) -> Result<Vec<BlobRef>, MakeRemoteError> {
    let tables = db.synced_tables().to_vec();
    gated_root_gate_col(&tables, root_table)?;
    refs_for_root(db, tables, root_table.to_string(), root_id.to_string())
        .await
        .map_err(MakeRemoteError::from)
}

/// The final cloud object key for `blob` under `scheme` and its recorded location.
/// Shared by the make_remote, cancel, and make_local paths.
fn cloud_key_for_location(
    scheme: BlobPathScheme,
    location: Option<&crate::blob::CloudBlobLocation>,
    blob: &BlobRef,
) -> Result<String, String> {
    CloudSyncStorage::blob_key_at(
        scheme,
        &blob.namespace,
        location,
        &blob.id,
        blob.cloud_path.as_deref(),
    )
    .map_err(|e| e.to_string())
}

/// Start making `(root_table, root_id)` Remote: refuse a root already Remote, then
/// verify each user-provided blob's external source file, enqueue an upload per blob,
/// and record the make_remote intent in one transaction. Returns once enqueued — the
/// sync cycle uploads all needed blobs and flips the gate true only after they land.
/// The caller triggers a sync cycle to start that completion.
///
/// Verifying every source up front (exists + length matches the registered size)
/// means a missing file aborts with nothing enqueued, rather than leaving a
/// half-queued make_remote. `pin` becomes each upload's `retain_pinned`, so the
/// blob is kept in coven's cache as a pinned (offline) copy.
pub async fn make_remote(
    db: &Database,
    scheme: BlobPathScheme,
    self_uploader: &str,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
    pin: bool,
) -> Result<(), MakeRemoteError> {
    let tables = db.synced_tables().to_vec();
    let gate_col = gated_root_gate_col(&tables, root_table)?;
    let refs = refs_for_root(db, tables, root_table.to_string(), root_id.to_string())
        .await
        .map_err(MakeRemoteError::from)?;
    if refs.is_empty() {
        return Err(MakeRemoteError::NothingToMakeRemote(
            root_table.to_string(),
            root_id.to_string(),
        ));
    }
    // A host-provided-only root has no user-provided uploads; `uploads` stays empty and
    // the commit below records just the intent (the sync cycle uploads its host-provided
    // blobs and flips the gate). Verify each external source and derive its cloud key up
    // front: any miss aborts before a single upload is enqueued, so a make_remote queues
    // whole or not at all.
    let user_provided: Vec<BlobRef> = refs
        .iter()
        .filter(|b| b.provenance == Provenance::UserProvided)
        .cloned()
        .collect();
    let created_at = hlc.now().to_string();
    let generated_location = crate::blob::CloudBlobLocation::generated(self_uploader, &created_at);
    let mut locations = Vec::new();
    for blob in &refs {
        let location = match db.blob_location(&blob.namespace, &blob.id).await? {
            Some(existing) if existing.version.is_some() => existing,
            _ => generated_location.clone(),
        };
        locations.push((blob.namespace.clone(), blob.id.clone(), location));
    }
    let mut uploads: Vec<(
        String,
        String,
        String,
        String,
        crate::blob::BlobScope,
        crate::blob::CloudBlobLocation,
        String,
    )> = Vec::new();
    for blob in &user_provided {
        let ext = db
            .external_blob(&blob.id)
            .await?
            .ok_or_else(|| MakeRemoteError::NotExternal(blob.id.clone()))?;
        let len = crate::local_blob::file_len(&ext.path)
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
        let expected_hash =
            cache::expected_blob_hash(db, blob)
                .await
                .map_err(|error| MakeRemoteError::Source {
                    blob_id: blob.id.clone(),
                    path: ext.path.display().to_string(),
                    detail: error.to_string(),
                })?;
        let actual_hash = crate::blob::content_hash_file(&ext.path)
            .await
            .map_err(|detail| MakeRemoteError::Source {
                blob_id: blob.id.clone(),
                path: ext.path.display().to_string(),
                detail,
            })?;
        if actual_hash != expected_hash {
            return Err(MakeRemoteError::Source {
                blob_id: blob.id.clone(),
                path: ext.path.display().to_string(),
                detail: format!(
                    "content hash {actual_hash} does not match the row's signed hash {expected_hash}"
                ),
            });
        }
        let location = locations
            .iter()
            .find(|(namespace, id, _)| namespace == &blob.namespace && id == &blob.id)
            .map(|(_, _, location)| location.clone())
            .ok_or_else(|| {
                MakeRemoteError::CloudKey(
                    blob.id.clone(),
                    "make_remote did not assign a blob location".to_string(),
                )
            })?;
        let cloud_key = cloud_key_for_location(scheme, Some(&location), blob)
            .map_err(|e| MakeRemoteError::CloudKey(blob.id.clone(), e))?;
        let source = ext.path.to_str().ok_or_else(|| MakeRemoteError::Source {
            blob_id: blob.id.clone(),
            path: ext.path.display().to_string(),
            detail: "external path is not valid UTF-8".to_string(),
        })?;
        uploads.push((
            blob.id.clone(),
            blob.namespace.clone(),
            cloud_key,
            source.to_string(),
            blob.scope.clone(),
            location,
            expected_hash,
        ));
    }

    let (rt, gc, ri) = (
        root_table.to_string(),
        gate_col.clone(),
        root_id.to_string(),
    );
    // Read the root's locality and record the intent + uploads only when it is Local,
    // in one transaction. Reading and inserting atomically means an already-Remote root
    // never receives an intent whose completion would re-flip the on gate with a fresh
    // stamp and re-publish the whole subtree. `query_truth` is the same locality reader
    // the read/delete paths use, so there is one definition of a root's Local/Remote
    // state.
    let locality = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let locality = crate::sync::gate::query_truth(&tx, &rt, &gc, &ri)
                .map_err(|e| DbError(e.to_string()))?;
            if locality == Some(false) {
                Database::insert_make_remote_intent_on(&tx, &rt, &ri, pin)?;
                for (namespace, id, location) in &locations {
                    Database::record_blob_location_on(&tx, namespace, id, location)?;
                }
                for (id, _namespace, cloud_key, source, scope, _location, expected_hash) in &uploads
                {
                    Database::enqueue_upload_on(
                        &tx,
                        id,
                        cloud_key,
                        Some(source),
                        scope.clone(),
                        pin,
                        Some(expected_hash),
                        &created_at,
                    )?;
                }
                tx.commit().map_err(DbError::from)?;
            }
            Ok(locality)
        })
        .await?;
    match locality {
        Some(false) => Ok(()),
        Some(true) => Err(MakeRemoteError::AlreadyRemote(
            root_table.to_string(),
            root_id.to_string(),
        )),
        None => Err(MakeRemoteError::UnresolvedLocality(
            root_table.to_string(),
            root_id.to_string(),
        )),
    }
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
    store_dir: &StoreDir,
    scheme: BlobPathScheme,
    _self_uploader: &str,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
) -> Result<(), MakeRemoteError> {
    let user_provided: Vec<BlobRef> = root_refs(db, root_table, root_id)
        .await?
        .into_iter()
        .filter(|b| b.provenance == Provenance::UserProvided)
        .collect();
    // The exact immutable cloud key + cache namespace per blob. The location was
    // recorded atomically with the upload enqueue, so cancellation never rebuilds
    // a key from the current device identity.
    let mut keyed: Vec<(String, String, String)> = Vec::new();
    for blob in &user_provided {
        let location = db
            .blob_location(&blob.namespace, &blob.id)
            .await?
            .ok_or_else(|| {
                MakeRemoteError::CloudKey(
                    blob.id.clone(),
                    "make_remote intent has no recorded blob location".to_string(),
                )
            })?;
        let cloud_key = cloud_key_for_location(scheme, Some(&location), blob)
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
            if !Database::make_remote_intent_exists(&tx, &root_table_owned, &root_id_owned)? {
                return Ok(Vec::new());
            }
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
                Database::clear_blob_location_on(&tx, namespace, id, &now)?;
            }
            Database::delete_make_remote_intent_on(&tx, &root_table_owned, &root_id_owned)?;
            tx.commit().map_err(DbError::from)?;
            Ok(dropped)
        })
        .await?;

    for (id, namespace) in dropped {
        if let Err(e) = cache::drop_cached_blob(store_dir, &namespace, &id).await {
            tracing::warn!(
                "cancel_make_remote: failed to drop cache copy of {namespace}/{id}: {e}"
            );
        }
    }
    Ok(())
}

/// What a make_remote completion commits inside the flip transaction *besides* the
/// shared `{flip the gate true, clear the root's user-provided external refs, delete
/// the intent}`. The two completion paths differ only in this.
pub(crate) enum MakeRemoteCompletion {
    /// The upload drain's inline path: the just-finished upload was the last
    /// user-provided blob of the subtree, so its outbox row is removed inside the
    /// flip. Removing it here (not before) is the crash-safety invariant — until the
    /// commit the row is present, so a crash re-runs the idempotent upload and retries
    /// the flip.
    FinalOutboxRow {
        id: i64,
        cloud_key: String,
        created_at: String,
    },
    /// The sync cycle's host-provided path: the local-store dispositions for the
    /// host-provided blobs this cycle uploaded are committed inside the flip, keyed by
    /// the sequence the flip's re-emitted changeset publishes at. The insert's `ON
    /// CONFLICT DO NOTHING` keeps this authoritative record — written first, carrying
    /// the intent's `retain_pinned` — from being overwritten by the inline push's later
    /// default-disposition re-scan of the same re-emitted blob (the intent is gone by
    /// then, so that re-scan cannot recover `retain_pinned`).
    Dispositions {
        intent_seq: u64,
        drops: Vec<DeferredLocalBlobDrop>,
    },
}

/// The single atomic make_remote completion commit, shared by the upload drain's
/// inline path and the sync cycle's host-provided path: flip the root's gate true,
/// clear its user-provided blobs' external refs, and delete the intent — plus the
/// path-specific `completion` write — in one journaled transaction. The gate flip
/// re-emits the now-shareable subtree into the captured changeset. Runs synchronously
/// on a connection the caller already holds, so each path's preamble (mapping the
/// upload to its root, or scanning the ready intents) and this flip stay in one DB
/// call — the completion check and the flip it gates commit together.
#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_make_remote_flip(
    conn: &Connection,
    tables: &[SyncedTable],
    root_table: &str,
    gate_column: &str,
    root_id: &str,
    stamp: &str,
    user_blob_ids: &[String],
    completion: MakeRemoteCompletion,
) -> Result<(), DbError> {
    Database::run_pending_journaled_transaction_on(conn, tables, |tx| {
        crate::sync::gate::write_gate(tx, root_table, gate_column, true, stamp, root_id)
            .map_err(DbError::from)?;
        for id in user_blob_ids {
            Database::clear_external_blob_on(tx, id)?;
        }
        match &completion {
            MakeRemoteCompletion::FinalOutboxRow {
                id,
                cloud_key,
                created_at,
            } => {
                crate::blob::upload::finish_outbox_row(tx, *id, cloud_key, created_at)?;
            }
            MakeRemoteCompletion::Dispositions { intent_seq, drops } => {
                for drop in drops {
                    crate::sync::cycle::insert_published_blob_drop_intent(tx, *intent_seq, drop)?;
                }
            }
        }
        Database::delete_make_remote_intent_on(tx, root_table, root_id)
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn pending_upload_exists_excludes_the_named_row() {
        let conn = Connection::open_in_memory().expect("open sqlite");
        crate::db::apply_coven_schema(&conn).expect("create bookkeeping schema");
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
        // Row 2 is another pending upload for blob-a, so excluding row 1 still finds it;
        // the `delete` row (4) never counts.
        assert!(pending_upload_exists(&conn, &blob_ids, Some(1)).expect("query"));
        // Drop row 2: excluding row 1 now leaves only the delete row, so nothing pending.
        conn.execute("DELETE FROM cloud_outbox WHERE id = 2", [])
            .expect("delete row");
        assert!(!pending_upload_exists(&conn, &blob_ids, Some(1)).expect("query"));
        // With no exclusion, the remaining row 1 upload for blob-a counts.
        assert!(pending_upload_exists(&conn, &blob_ids, None).expect("query"));
    }
}

// ===========================================================================
// make_local — native-only (foreground op with a cancel signal)
// ===========================================================================

/// One blob materialized back to a local file by [`make_local`], carrying what the
/// single commit needs. `dest` is present for a user-provided blob whose local
/// home is the user's path; absent for a host-provided blob whose local home is
/// coven's local store.
#[cfg(not(target_arch = "wasm32"))]
struct Materialized {
    blob: BlobRef,
    dest: Option<PathBuf>,
    size: u64,
    cloud_key: String,
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
    store_dir: &StoreDir,
    scheme: BlobPathScheme,
    hlc: &Hlc,
    observer: Option<&dyn BlobTransitionObserver>,
    root_table: &str,
    root_id: &str,
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
) -> Result<(), MakeLocalError> {
    let tables = db.synced_tables().to_vec();
    if is_remote_root(&tables, root_table) {
        return Err(MakeLocalError::RemoteRoot(root_table.to_string()));
    }
    let gate_col = gate_column(&tables, root_table)
        .ok_or_else(|| MakeLocalError::NotGated(root_table.to_string()))?
        .to_string();

    // Refuse a root already Local before any materialization. Otherwise the
    // materializer would try to read each blob back from the cloud — a Local blob has
    // no cloud copy — and fail deep inside with a misleading cloud-read error.
    // `query_truth` is the same locality reader the read/delete paths use, so there is
    // one definition of a root's Local/Remote state.
    let (rt, gc, ri) = (
        root_table.to_string(),
        gate_col.clone(),
        root_id.to_string(),
    );
    let locality = db
        .call(move |conn| {
            crate::sync::gate::query_truth(conn, &rt, &gc, &ri).map_err(|e| DbError(e.to_string()))
        })
        .await?;
    match locality {
        Some(true) => {}
        Some(false) => {
            return Err(MakeLocalError::AlreadyLocal(
                root_table.to_string(),
                root_id.to_string(),
            ))
        }
        None => {
            return Err(MakeLocalError::UnresolvedLocality(
                root_table.to_string(),
                root_id.to_string(),
            ))
        }
    }

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
    let stamp = hlc.now().to_string();
    let mut written: Vec<WrittenFile> = Vec::new();
    let materialized = match materialize_blobs(
        db,
        storage,
        store_dir,
        scheme,
        observer,
        root_table,
        root_id,
        &refs,
        dest,
        cancel,
        &stamp,
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
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    let commit: Vec<(String, String, Option<String>, u64, String)> = match materialized
        .iter()
        .map(|m| -> Result<_, MakeLocalError> {
            Ok({
                if let Some(dest) = &m.dest {
                    let external_path =
                        dest.to_str().ok_or_else(|| MakeLocalError::NonUtf8Dest {
                            blob_id: m.blob.id.clone(),
                            path: dest.display().to_string(),
                        })?;
                    (
                        m.blob.id.clone(),
                        m.blob.namespace.clone(),
                        Some(external_path.to_string()),
                        m.size,
                        m.cloud_key.clone(),
                    )
                } else {
                    (
                        m.blob.id.clone(),
                        m.blob.namespace.clone(),
                        None,
                        m.size,
                        m.cloud_key.clone(),
                    )
                }
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
    let tables = db.synced_tables().to_vec();
    let commit_result = db
        .call(move |conn| {
            Database::run_pending_journaled_transaction_on(conn, &tables, |tx| {
                crate::sync::gate::write_gate(
                    tx,
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
                            tx,
                            id,
                            namespace,
                            std::path::Path::new(path),
                            *size,
                        )?;
                    }
                    Database::enqueue_delete_on(tx, cloud_key, &stamp)?;
                    Database::clear_blob_location_on(tx, namespace, id, &stamp)?;
                }
                Ok(())
            })
        })
        .await;
    if let Err(error) = commit_result {
        return Err(roll_back(&written, MakeLocalError::Db(error)).await);
    }

    // Post-commit, best-effort: the bytes now live at their local file (a user path
    // or the local store) and the cloud blob is tombstoned, so the cache copies are
    // pure redundancy — drop them. A failure leaves only stray cache space; a read
    // serves the local file. Log and go on.
    for m in &materialized {
        if let Err(e) = cache::drop_cached_blob(store_dir, &m.blob.namespace, &m.blob.id).await {
            tracing::warn!(
                "make_local: failed to drop cache copy of {}/{}: {e}",
                m.blob.namespace,
                m.blob.id
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
    store_dir: &StoreDir,
    scheme: BlobPathScheme,
    observer: Option<&dyn BlobTransitionObserver>,
    root_table: &str,
    root_id: &str,
    refs: &[BlobRef],
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
    operation_id: &str,
    written: &mut Vec<WrittenFile>,
) -> Result<Vec<Materialized>, MakeLocalError> {
    let total = refs.len() as u64;
    let mut materialized: Vec<Materialized> = Vec::new();

    for (i, blob) in refs.iter().enumerate() {
        if *cancel.borrow() {
            return Err(MakeLocalError::Cancelled);
        }

        // The cloud object this make_local will tombstone may sit under a peer's
        // prefix (a peer made this root remote), so resolve its uploader rather than
        // assuming ourselves.
        let location = crate::blob::cache::resolve_blob_location(db, storage, blob)
            .await
            .map_err(|e| MakeLocalError::CloudKey(blob.id.clone(), e.to_string()))?;
        let cloud_key = cloud_key_for_location(scheme, location.as_ref(), blob)
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
                prepare_parent_dir(&dest_path)
                    .await
                    .map_err(|detail| MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: dest_path.display().to_string(),
                        detail,
                    })?;
                let temp_path =
                    operation_temp_path(&dest_path, operation_id, i).map_err(|detail| {
                        MakeLocalError::Write {
                            blob_id: blob.id.clone(),
                            path: dest_path.display().to_string(),
                            detail,
                        }
                    })?;
                let size = cache::materialize_remote_blob_to_file(
                    db,
                    store_dir,
                    Some(storage),
                    blob,
                    &temp_path,
                )
                .await
                .map_err(|e| MakeLocalError::Read(blob.id.clone(), e.to_string()));
                let size = match size {
                    Ok(size) => size,
                    Err(error) => {
                        return Err(cleanup_operation_temp(&temp_path, error).await);
                    }
                };
                if let Err(detail) = verify_durable(&temp_path, size).await {
                    let error = MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: temp_path.display().to_string(),
                        detail,
                    };
                    return Err(cleanup_operation_temp(&temp_path, error).await);
                }
                let commit =
                    crate::local_blob::commit_temp_no_replace(&temp_path, &dest_path).await;
                if let Err(error) = commit {
                    let error = match error {
                        crate::local_blob::CommitNoReplaceError::DestinationExists(_) => {
                            MakeLocalError::DestinationExists {
                                blob_id: blob.id.clone(),
                                path: dest_path.display().to_string(),
                            }
                        }
                        crate::local_blob::CommitNoReplaceError::Other(detail) => {
                            MakeLocalError::Write {
                                blob_id: blob.id.clone(),
                                path: dest_path.display().to_string(),
                                detail,
                            }
                        }
                        crate::local_blob::CommitNoReplaceError::DestinationCreated(detail) => {
                            written.push(WrittenFile::UserPath(dest_path.clone()));
                            MakeLocalError::Write {
                                blob_id: blob.id.clone(),
                                path: dest_path.display().to_string(),
                                detail,
                            }
                        }
                    };
                    return Err(cleanup_operation_temp(&temp_path, error).await);
                }
                written.push(WrittenFile::UserPath(dest_path.clone()));
                Materialized {
                    blob: blob.clone(),
                    dest: Some(dest_path),
                    size,
                    cloud_key,
                }
            }
            Provenance::HostProvided => {
                let store_path = store_dir
                    .local_blob_path(&blob.namespace, &blob.id)
                    .map_err(|e| MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: format!("local/{}/{}", blob.namespace, blob.id),
                        detail: e.to_string(),
                    })?;
                let size = cache::materialize_remote_blob_to_file(
                    db,
                    store_dir,
                    Some(storage),
                    blob,
                    &store_path,
                )
                .await
                .map_err(|e| MakeLocalError::Read(blob.id.clone(), e.to_string()))?;
                verify_durable(&store_path, size).await.map_err(|detail| {
                    MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: store_path.display().to_string(),
                        detail,
                    }
                })?;
                written.push(WrittenFile::LocalStore(store_path));
                Materialized {
                    blob: blob.clone(),
                    dest: None,
                    size,
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

/// Prove a materialized local file has the expected length and fsync its parent
/// directory before make_local can tombstone the cloud copy. The materializer has
/// already written through a temp file / rename path; this check is the
/// Local-specific durability gate. A materialized local file is the ONLY copy once
/// the cloud blob is tombstoned, so a directory-fsync failure is a hard error here:
/// it aborts the make_local (the cloud copy is still intact) rather than commit a
/// tombstone over a destination whose entry might not survive a crash.
#[cfg(not(target_arch = "wasm32"))]
async fn verify_durable(dest: &std::path::Path, expected_size: u64) -> Result<(), String> {
    let len = crate::local_blob::file_len(dest).await?;
    if len != expected_size {
        return Err(format!(
            "dest {} is {len} bytes after materialize, expected {expected_size}",
            dest.display()
        ));
    }

    // fsync the parent dir so the rename's new entry is durable, not just the data.
    // Hard error (see fn doc): the materialized file is the only copy after the commit.
    crate::local_blob::sync_parent_dir(dest).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn prepare_parent_dir(dest: &std::path::Path) -> Result<(), String> {
    let parent = dest
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", dest.display()))?;
    crate::local_blob::create_dir_all(parent).await
}

#[cfg(not(target_arch = "wasm32"))]
fn operation_temp_path(
    dest: &std::path::Path,
    operation_id: &str,
    blob_index: usize,
) -> Result<PathBuf, String> {
    let parent = dest
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", dest.display()))?;
    let identity = format!("{operation_id}:{blob_index}:{}", dest.display());
    Ok(parent.join(format!(
        "{}{}",
        crate::local_blob::TEMP_BLOB_PREFIX,
        crate::blob::content_hash(identity.as_bytes())
    )))
}

#[cfg(not(target_arch = "wasm32"))]
async fn cleanup_operation_temp(path: &std::path::Path, error: MakeLocalError) -> MakeLocalError {
    match crate::local_blob::remove_file(path).await {
        Ok(_) => error,
        Err(detail) => MakeLocalError::CleanupUserPath {
            path: path.display().to_string(),
            detail,
        },
    }
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

/// Delete the partial local copies an aborted make_local wrote. Failure to remove
/// either kind is surfaced: a user path was created exclusively by this operation,
/// while a local-store leftover would make a still-Remote blob read as Local.
/// An already-absent file is not an error
/// ([`crate::local_blob::remove_file`] reports `Ok(false)`).
#[cfg(not(target_arch = "wasm32"))]
async fn cleanup_partial(written: &[WrittenFile]) -> Result<(), MakeLocalError> {
    for file in written {
        match file {
            WrittenFile::UserPath(path) => {
                crate::local_blob::remove_file(path)
                    .await
                    .map_err(|detail| MakeLocalError::CleanupUserPath {
                        path: path.display().to_string(),
                        detail,
                    })?;
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
