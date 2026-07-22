//! coven-owned locality transitions: `make_remote` (Local → Remote) and
//! `make_local` (Remote → Local), plus `cancel_make_remote`.
//!
//! A blob-bearing root is **Local** (its blobs live on-device — a user-provided
//! blob at the user's path, a host-provided blob in coven's local store — and the
//! root's gate is off) or **Remote** (its blobs live in the cloud fronted by
//! coven's cache, and the gate is on). coven owns moving between the two, with the
//! durable-copy-before-delete ordering and a single atomic commit point each way:
//!
//! - [`make_remote`] verifies and journals every blob-bearing row beneath the root,
//!   then returns. The upload drain ([`crate::blob::upload::drain_uploads`]) creates
//!   each exact cloud object. Once every row has a created object, one Store write
//!   flips the gate true and drops external-file ownership. The intent and exact
//!   upload journals remain authoritative until that Store write activates, when
//!   activation consumes them atomically. Before publication starts,
//!   [`cancel_make_remote`] marks the intent Cancelling; the upload drain
//!   exact-deletes every object and spool before atomically removing the last
//!   journal and intent, so the root remains Local.
//! - [`make_local`] brings each blob back to a local file durability-first
//!   (read from cache/cloud → write the local copy → verify): a **user-provided**
//!   blob to its `dest` path (path required) registered as an external ref, a
//!   **host-provided** blob to coven's local store (no path). Then it takes the
//!   single commit `{flip the gate false + register the external refs + enqueue the
//!   cloud deletes}`. The gate retract removes the subtree from peers; the tombstone
//!   drain reclaims the cloud blobs after the grace.
//!
//! Both transitions operate on every exact blob-bearing row under the root and
//! branch on provenance only to choose its Local filesystem home.
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

use crate::blob::{cache, Provenance, RowBlobRef};
use crate::database::{Database, DbError, MakeRemoteIntentState};
use crate::db::{OutboxEntry, OutboxOperation, OutboxUploadState};
use crate::store_dir::StoreDir;
use crate::sync::hlc::Hlc;
use crate::sync::session::SyncedTable;

use crate::blob::BlobTransitionObserver;
use crate::sync::storage::SyncStorage;
use std::path::PathBuf;
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
    #[error("database error: {0}")]
    Db(#[from] DbError),
}

/// Why a make_local could not complete.
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
    #[error("read blob {0:?} to materialize: {1}")]
    Read(String, String),
    #[error("write materialized blob {blob_id:?} to {path}: {detail}")]
    Write {
        blob_id: String,
        path: String,
        detail: String,
    },
    #[error("make_local cancelled before the commit; the release stays Remote")]
    Cancelled,
    #[error("could not roll back materialized file at {path}: {detail}")]
    Cleanup { path: String, detail: String },
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
    store_dir: &StoreDir,
    hlc: &Hlc,
    root_table: &str,
    root_id: &str,
    pin: bool,
) -> Result<(), MakeRemoteError> {
    let tables = db.synced_tables().to_vec();
    let gate_col = gated_root_gate_col(&tables, root_table)?;
    let (locality_table, locality_column, locality_id) = (
        root_table.to_string(),
        gate_col.clone(),
        root_id.to_string(),
    );
    let locality = db
        .call(move |conn| {
            crate::sync::gate::query_truth(conn, &locality_table, &locality_column, &locality_id)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await?;
    match locality {
        Some(false) => {}
        Some(true) => {
            return Err(MakeRemoteError::AlreadyRemote(
                root_table.to_string(),
                root_id.to_string(),
            ));
        }
        None => {
            return Err(MakeRemoteError::UnresolvedLocality(
                root_table.to_string(),
                root_id.to_string(),
            ));
        }
    }

    let refs = db.row_blob_refs_for_root(root_table, root_id).await?;
    if refs.is_empty() {
        return Err(MakeRemoteError::NothingToMakeRemote(
            root_table.to_string(),
            root_id.to_string(),
        ));
    }

    let mut uploads = Vec::with_capacity(refs.len());
    for reference in refs {
        if reference.authority() != &crate::blob::RowBlobAuthority::Local
            || reference.stored().is_some()
        {
            return Err(MakeRemoteError::AlreadyRemote(
                root_table.to_string(),
                root_id.to_string(),
            ));
        }
        let blob = reference.blob();
        let source_path = match blob.provenance {
            Provenance::UserProvided => {
                db.external_blob_for_row(&reference)
                    .await?
                    .ok_or_else(|| MakeRemoteError::NotExternal(blob.id.clone()))?
                    .path
            }
            Provenance::HostProvided => store_dir
                .local_blob_path(&blob.namespace, &blob.id)
                .map_err(|error| MakeRemoteError::Source {
                    blob_id: blob.id.clone(),
                    path: format!("local/{}/{}", blob.namespace, blob.id),
                    detail: error.to_string(),
                })?,
        };
        let (actual_size, actual_hash) = crate::local_blob::exact_file_facts(&source_path)
            .await
            .map_err(|detail| MakeRemoteError::Source {
                blob_id: blob.id.clone(),
                path: source_path.display().to_string(),
                detail,
            })?;
        if actual_size != reference.plaintext_size() || actual_hash != reference.plaintext_hash() {
            return Err(MakeRemoteError::Source {
                blob_id: blob.id.clone(),
                path: source_path.display().to_string(),
                detail: format!(
                    "plaintext facts {actual_size}/{actual_hash} differ from row facts {}/{}",
                    reference.plaintext_size(),
                    reference.plaintext_hash(),
                ),
            });
        }
        uploads.push((reference, source_path));
    }

    let created_at = hlc.now().to_string();
    let (rt, gc, ri) = (
        root_table.to_string(),
        gate_col.clone(),
        root_id.to_string(),
    );
    let gates = db.gates();
    let locality = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let locality = crate::sync::gate::query_truth(&tx, &rt, &gc, &ri)
                .map_err(|e| DbError::Message(e.to_string()))?;
            if locality == Some(false) {
                let current = Database::row_blob_refs_for_root_on(
                    &tx, &gates, &tables, &rt, &ri,
                )?;
                if current.len() != uploads.len()
                    || current
                        .iter()
                        .zip(&uploads)
                        .any(|(current, (verified, _))| current != verified)
                {
                    return Err(DbError::Message(format!(
                        "blob rows below {rt:?}/{ri:?} changed while make_remote verified their sources"
                    )));
                }
                Database::insert_make_remote_intent_on(&tx, &rt, &ri, pin)?;
                for (reference, source_path) in &uploads {
                    Database::enqueue_upload_on(
                        &tx,
                        &rt,
                        &ri,
                        reference,
                        source_path,
                        pin,
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

/// Cancel an uploading make_remote by durably marking its intent Cancelling. The gate
/// remains Local. The upload drain exact-deletes each prepared/created cloud object
/// and spool, then removes the last journal and intent together. A new transition
/// cannot start over cleanup in progress. Once the publication Store write exists,
/// cancellation is rejected because that write owns the transition's atomic outcome.
pub async fn cancel_make_remote(
    db: &Database,
    root_table: &str,
    root_id: &str,
) -> Result<(), MakeRemoteError> {
    let tables = db.synced_tables();
    gated_root_gate_col(tables, root_table)?;
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    db.call(move |conn| {
        let tx = conn.unchecked_transaction()?;
        match Database::make_remote_intent_state(&tx, &root_table_owned, &root_id_owned)? {
            Some(MakeRemoteIntentState::Uploading) => {
                Database::mark_make_remote_cancelling_on(
                    &tx,
                    &root_table_owned,
                    &root_id_owned,
                )?;
            }
            Some(MakeRemoteIntentState::Cancelling) => {}
            Some(MakeRemoteIntentState::Publishing(write_id)) => {
                return Err(DbError::Message(format!(
                    "make_remote for {root_table_owned:?}/{root_id_owned:?} is already publishing as {write_id}"
                )));
            }
            None => {
                return Err(DbError::Message(format!(
                    "make_remote for {root_table_owned:?}/{root_id_owned:?} does not exist"
                )));
            }
        }
        tx.commit().map_err(DbError::from)
    })
    .await
    .map_err(MakeRemoteError::from)
}

pub(crate) enum PostUpload {
    Waiting,
    Cancelled,
    MadeRemote { root_table: String, root_id: String },
}

/// Complete a Created upload journal entry without exposing a Remote row that lacks
/// exact object authority. Every blob-bearing row below the same gated root must
/// still match its queued row version and have reached Created. The final transaction
/// then flips the gate, clears external-file ownership, records the pending Store
/// write, and binds the transition intent to that write together. The intent and
/// Created handoffs remain until that Store write activates, so a crash cannot make
/// the upload drain mistake a published object for an orphan.
pub(crate) async fn finalize_created_upload(
    db: &Database,
    entry: &OutboxEntry,
    stamp: String,
    routing_encryption: Option<crate::encryption::EncryptionService>,
) -> Result<PostUpload, DbError> {
    let OutboxOperation::Upload {
        root_table,
        root_id,
        row,
        state,
        ..
    } = &entry.operation
    else {
        return Err(DbError::Message(
            "make_remote finalizer received a non-upload outbox entry".to_string(),
        ));
    };
    if !matches!(state, OutboxUploadState::Created { .. }) {
        return Err(DbError::Message(
            "make_remote finalizer requires a Created exact upload".to_string(),
        ));
    }

    let entry = entry.clone();
    let root_table = root_table.clone();
    let root_id = root_id.clone();
    let row = row.clone();
    let tables = db.synced_tables().to_vec();
    let gates = db.gates();
    let write_id = db.new_write_id();
    db.call(move |conn| {
        let resolved_root = gates
            .resolve_root_of(conn, row.table(), row.row_id())
            .map_err(|error| DbError::Message(error.to_string()))?
            .ok_or_else(|| {
                DbError::Message(format!(
                    "upload row {:?}/{:?} has no gated transition root",
                    row.table(),
                    row.row_id()
                ))
            })?;
        if resolved_root != (root_table.clone(), root_id.clone()) {
            return Err(DbError::Message(format!(
                "upload row {:?}/{:?} moved from make_remote root {:?}/{:?} to {:?}/{:?}",
                row.table(),
                row.row_id(),
                root_table,
                root_id,
                resolved_root.0,
                resolved_root.1
            )));
        }
        match Database::make_remote_intent_state(conn, &root_table, &root_id)? {
            Some(MakeRemoteIntentState::Uploading) => {}
            Some(MakeRemoteIntentState::Publishing(_)) => return Ok(PostUpload::Waiting),
            Some(MakeRemoteIntentState::Cancelling) => {
                return Ok(PostUpload::Cancelled);
            }
            None => {
                return Err(DbError::Message(format!(
                    "upload for {root_table:?}/{root_id:?} has no make_remote intent"
                )));
            }
        }

        let rows = Database::row_blob_refs_for_root_on(
            conn,
            &gates,
            &tables,
            &root_table,
            &root_id,
        )?;
        let entries = Database::upload_entries_for_root_on(
            conn,
            &gates,
            &tables,
            &root_table,
            &root_id,
        )?;
        if rows.len() != entries.len() {
            return Err(DbError::Message(format!(
                "make_remote root {root_table:?}/{root_id:?} has {} blob rows but {} exact upload journals",
                rows.len(),
                entries.len()
            )));
        }
        if !entries.iter().all(|candidate| {
            matches!(
                candidate.operation,
                OutboxOperation::Upload {
                    state: OutboxUploadState::Created { .. },
                    ..
                }
            )
        }) {
            return Ok(PostUpload::Waiting);
        }
        if !entries.iter().any(|candidate| candidate == &entry) {
            return Err(DbError::Message(
                "Created upload changed before make_remote finalization".to_string(),
            ));
        }

        let gate_column = gate_column(&tables, &root_table).ok_or_else(|| {
            DbError::Message(format!(
                "make_remote root {root_table:?} has no boolean gate column"
            ))
        })?;
        let publication_write_id = write_id.clone();
        Database::run_internal_store_write_transaction_on(
            conn,
            &tables,
            routing_encryption.as_ref(),
            write_id,
            |tx| {
                crate::sync::gate::write_gate(
                    tx,
                    &root_table,
                    gate_column,
                    true,
                    &stamp,
                    &root_id,
                )
                .map_err(DbError::from)?;
                for reference in &rows {
                    if reference.blob().provenance == Provenance::UserProvided {
                        Database::clear_external_blob_on(tx, reference)?;
                    }
                }
                Database::mark_make_remote_publishing_on(
                    tx,
                    &root_table,
                    &root_id,
                    &publication_write_id,
                )
            },
        )?;
        Ok(PostUpload::MadeRemote {
            root_table,
            root_id,
        })
    })
    .await
}

// ===========================================================================
// make_local — foreground operation with a cancel signal
// ===========================================================================

/// One blob materialized back to a local file by [`make_local`], carrying what the
/// single commit needs. `dest` is present for a user-provided blob whose local
/// home is the user's path; absent for a host-provided blob whose local home is
/// coven's local store.
#[derive(Clone)]
struct Materialized {
    remote: RowBlobRef,
    stored: crate::blob::locator::StoredBlobRef,
    dest: Option<PathBuf>,
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
#[allow(clippy::too_many_arguments)]
pub async fn make_local(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    hlc: &Hlc,
    routing_encryption: Option<crate::encryption::EncryptionService>,
    observer: Option<&dyn BlobTransitionObserver>,
    root_table: &str,
    root_id: &str,
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
) -> Result<(), MakeLocalError> {
    let tables = db.synced_tables().to_vec();
    Database::validate_store_write_routing(db.gates().as_ref(), routing_encryption.as_ref())?;
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
            crate::sync::gate::query_truth(conn, &rt, &gc, &ri)
                .map_err(|e| DbError::Message(e.to_string()))
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

    let refs = db.row_blob_refs_for_root(root_table, root_id).await?;
    for reference in &refs {
        if !matches!(
            reference.authority(),
            crate::blob::RowBlobAuthority::Remote(_)
        ) || reference.stored().is_none()
        {
            return Err(MakeLocalError::UnresolvedLocality(
                root_table.to_string(),
                root_id.to_string(),
            ));
        }
    }

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
    let mut written: Vec<PathBuf> = Vec::new();
    let materialized = match materialize_blobs(
        db,
        storage,
        store_dir,
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

    let stamp = hlc.now().to_string();
    let (root_table_owned, root_id_owned) = (root_table.to_string(), root_id.to_string());
    let committed = materialized.clone();

    // The single atomic commit: flip false + register external refs (user-provided
    // only) + enqueue the cloud deletes, together. The destructive cloud delete is
    // durable inside this commit, so a crash right after can never leave the root
    // Local with the cloud blobs un-tombstoned.
    let tables = db.synced_tables().to_vec();
    let write_id = db.new_write_id();
    let gates = db.gates();
    db.call(move |conn| {
        Database::run_internal_store_write_transaction_on(
            conn,
            &tables,
            routing_encryption.as_ref(),
            write_id,
            |tx| {
                let remote = Database::row_blob_refs_for_root_on(
                    tx,
                    &gates,
                    &tables,
                    &root_table_owned,
                    &root_id_owned,
                )?;
                if remote.len() != committed.len()
                    || remote
                        .iter()
                        .zip(&committed)
                        .any(|(current, materialized)| {
                            !same_row_blob_version(current, &materialized.remote)
                                || current.authority() != materialized.remote.authority()
                                || current.stored() != Some(&materialized.stored)
                        })
                {
                    return Err(DbError::Message(format!(
                        "make_local root {:?}/{:?} changed while its blobs were materialized",
                        root_table_owned, root_id_owned
                    )));
                }
                crate::sync::gate::write_gate(
                    tx,
                    &root_table_owned,
                    &gate_col,
                    false,
                    &stamp,
                    &root_id_owned,
                )
                .map_err(DbError::from)?;

                // A new exact cloud object cannot bind to an existing blob row
                // version. Restamp every blob-bearing child as part of the Local
                // transition; the gated root itself was restamped by write_gate.
                for materialized in &committed {
                    let reference = &materialized.remote;
                    if reference.table() == root_table_owned && reference.row_id() == root_id_owned
                    {
                        continue;
                    }
                    let sql = format!(
                        "UPDATE {} SET _updated_at = ?1 WHERE id = ?2 AND _updated_at = ?3",
                        crate::sync::session::quote_ident(reference.table())
                    );
                    let updated = tx
                        .execute(
                            &sql,
                            rusqlite::params![&stamp, reference.row_id(), reference.row_stamp()],
                        )
                        .map_err(DbError::from)?;
                    if updated != 1 {
                        return Err(DbError::Message(format!(
                            "make_local row {:?}/{:?} changed before restamping",
                            reference.table(),
                            reference.row_id()
                        )));
                    }
                }
                let local = Database::row_blob_refs_for_root_on(
                    tx,
                    &gates,
                    &tables,
                    &root_table_owned,
                    &root_id_owned,
                )?;
                if local.len() != committed.len() {
                    return Err(DbError::Message(format!(
                        "make_local root {:?}/{:?} changed while its blobs were materialized",
                        root_table_owned, root_id_owned
                    )));
                }
                for (local, materialized) in local.iter().zip(&committed) {
                    if local.table() != materialized.remote.table()
                        || local.row_id() != materialized.remote.row_id()
                        || local.column() != materialized.remote.column()
                        || local.row_stamp() != stamp
                        || local.blob() != materialized.remote.blob()
                        || local.plaintext_size() != materialized.remote.plaintext_size()
                        || local.plaintext_hash() != materialized.remote.plaintext_hash()
                        || local.authority() != &crate::blob::RowBlobAuthority::Local
                        || local.stored().is_some()
                    {
                        return Err(DbError::Message(format!(
                            "make_local row {:?}/{:?}/{:?} changed while its blob was materialized",
                            materialized.remote.table(),
                            materialized.remote.row_id(),
                            materialized.remote.column()
                        )));
                    }
                    if let Some(path) = &materialized.dest {
                        Database::register_external_blob_on(tx, local, path)?;
                    }
                    Database::enqueue_delete_on(tx, &materialized.stored, &stamp)?;
                }
                Ok(())
            },
        )
    })
    .await?;

    if let Some(obs) = observer {
        obs.on_root_made_local(root_table, root_id).await;
    }
    Ok(())
}

fn same_row_blob_version(left: &RowBlobRef, right: &RowBlobRef) -> bool {
    left.table() == right.table()
        && left.row_id() == right.row_id()
        && left.row_stamp() == right.row_stamp()
        && left.column() == right.column()
        && left.blob() == right.blob()
        && left.plaintext_size() == right.plaintext_size()
        && left.plaintext_hash() == right.plaintext_hash()
}

/// Read each of `refs`'s blobs and write it durably to its local file, pushing each
/// written path into `written` as it lands and returning the per-blob [`Materialized`]
/// records the commit needs. A user-provided blob goes to its `dest` path (required,
/// else [`MakeLocalError::MissingDest`]); a host-provided blob goes to coven's local
/// store (no dest). Any error (cancel, a missing user-provided dest, a read or write
/// failure, a key-derivation failure) returns early; the caller rolls back `written`.
/// Separated from the commit so every error path runs that one rollback.
#[allow(clippy::too_many_arguments)]
async fn materialize_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    observer: Option<&dyn BlobTransitionObserver>,
    root_table: &str,
    root_id: &str,
    refs: &[RowBlobRef],
    dest: &HashMap<String, PathBuf>,
    cancel: &watch::Receiver<bool>,
    written: &mut Vec<PathBuf>,
) -> Result<Vec<Materialized>, MakeLocalError> {
    let total = refs.len() as u64;
    let mut materialized: Vec<Materialized> = Vec::new();

    for (i, reference) in refs.iter().enumerate() {
        if *cancel.borrow() {
            return Err(MakeLocalError::Cancelled);
        }
        let blob = reference.blob();
        let stored = reference.stored().cloned().ok_or_else(|| {
            MakeLocalError::Read(
                blob.id.clone(),
                "remote row has no exact stored blob reference".to_string(),
            )
        })?;

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
                let staged = cache::stage_remote_blob_plaintext(
                    db,
                    store_dir,
                    Some(storage),
                    reference,
                    &dest_path,
                )
                .await
                .map_err(|e| MakeLocalError::Read(blob.id.clone(), e.to_string()))?;
                staged
                    .commit_new()
                    .await
                    .map_err(|detail| MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: dest_path.display().to_string(),
                        detail: detail.to_string(),
                    })?;
                verify_durable(&dest_path, reference.plaintext_size())
                    .await
                    .map_err(|detail| MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: dest_path.display().to_string(),
                        detail,
                    })?;
                written.push(dest_path.clone());
                Materialized {
                    remote: reference.clone(),
                    stored,
                    dest: Some(dest_path),
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
                let staged = cache::stage_remote_blob_plaintext(
                    db,
                    store_dir,
                    Some(storage),
                    reference,
                    &store_path,
                )
                .await
                .map_err(|e| MakeLocalError::Read(blob.id.clone(), e.to_string()))?;
                match staged.commit_new().await {
                    Ok(()) => written.push(store_path.clone()),
                    Err(crate::local_blob::CommitNewFileError::DestinationExists(_)) => {
                        let (size, hash) = crate::local_blob::exact_file_facts(&store_path)
                            .await
                            .map_err(|detail| MakeLocalError::Write {
                            blob_id: blob.id.clone(),
                            path: store_path.display().to_string(),
                            detail,
                        })?;
                        if size != reference.plaintext_size() || hash != reference.plaintext_hash()
                        {
                            return Err(MakeLocalError::Write {
                                blob_id: blob.id.clone(),
                                path: store_path.display().to_string(),
                                detail:
                                    "existing local-store file differs from the exact remote blob"
                                        .to_string(),
                            });
                        }
                    }
                    Err(error) => {
                        return Err(MakeLocalError::Write {
                            blob_id: blob.id.clone(),
                            path: store_path.display().to_string(),
                            detail: error.to_string(),
                        });
                    }
                }
                verify_durable(&store_path, reference.plaintext_size())
                    .await
                    .map_err(|detail| MakeLocalError::Write {
                        blob_id: blob.id.clone(),
                        path: store_path.display().to_string(),
                        detail,
                    })?;
                Materialized {
                    remote: reference.clone(),
                    stored,
                    dest: None,
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

async fn prepare_parent_dir(dest: &std::path::Path) -> Result<(), String> {
    let parent = dest
        .parent()
        .ok_or_else(|| format!("blob path has no parent dir: {}", dest.display()))?;
    crate::local_blob::create_dir_all(parent).await
}

/// Roll back the partial local copies an aborted make_local wrote, then return the
/// error to surface. Returns the original `abort_err` when the rollback succeeds; if
/// the rollback itself fails to remove a local-store leftover it returns THAT
/// instead — the more urgent signal, since that leftover is a readable, budget-exempt
/// copy of a still-Remote blob (a retry re-materializes over it).
async fn roll_back(written: &[PathBuf], abort_err: MakeLocalError) -> MakeLocalError {
    match cleanup_partial(written).await {
        Ok(()) => abort_err,
        Err(cleanup_err) => cleanup_err,
    }
}

/// Delete every file this operation published before an aborted make_local.
/// An already-absent path is idempotent; any filesystem failure is returned to
/// the initiator because the operation cannot claim rollback while a destination
/// remains visible.
async fn cleanup_partial(written: &[PathBuf]) -> Result<(), MakeLocalError> {
    for path in written {
        crate::local_blob::remove_file(path)
            .await
            .map_err(|detail| MakeLocalError::Cleanup {
                path: path.display().to_string(),
                detail,
            })?;
    }
    Ok(())
}
