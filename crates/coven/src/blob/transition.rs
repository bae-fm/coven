//! coven-owned locality transitions: `make_remote` (Local → Remote) and
//! `make_local` (Remote → Local), plus `cancel_make_remote`.
//!
//! A blob-bearing root is **Local** (its blobs live on-device — a user-provided
//! blob at the user's path, a host-provided blob in coven's local store — and the
//! root's gate is off) or **Remote** (its blobs live in the cloud fronted by
//! coven's cache, and the gate is on). coven owns moving between the two, with the
//! durable-copy-before-delete ordering and a single atomic commit point each way:
//!
//! - [`LocalBlobTransitions::make_remote`] verifies and journals every
//!   blob-bearing row beneath the root,
//!   then returns. The upload drain creates
//!   each exact cloud object. Once every row has a created object, one Store write
//!   flips the gate true and drops external-file ownership. The intent and exact
//!   upload journals remain authoritative until that Store write activates, when
//!   activation consumes them atomically. Before publication starts,
//!   [`LocalBlobTransitions::cancel_make_remote`] marks the intent Cancelling;
//!   the upload drain
//!   exact-deletes every object and spool before atomically removing the last
//!   journal and intent, so the root remains Local.
//! - [`ConnectedBlobTransitions::make_local`] brings each blob back to a local
//!   file durability-first
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
//! A [`SyncedTable::remote_root`](crate::protocol::synced_schema::SyncedTable::remote_root)
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

use crate::database::DbError;
use crate::database::StoreDatabase;
use crate::protocol::blob::{Provenance, RowBlobRef};
use crate::protocol::synced_schema::SyncedTable;
use crate::store_dir::StoreDir;

use crate::protocol::blob::BlobTransitionObserver;
use std::path::PathBuf;
use std::sync::Arc;
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

struct ExactPlaintextFile {
    path: PathBuf,
    expected_size: u64,
    expected_hash: crate::protocol::store_commit::ObjectHash,
}

impl ExactPlaintextFile {
    fn new(
        path: PathBuf,
        expected_size: u64,
        expected_hash: crate::protocol::store_commit::ObjectHash,
    ) -> Self {
        Self {
            path,
            expected_size,
            expected_hash,
        }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    async fn verify(&self) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncReadExt;

        let mut file = tokio::fs::File::open(&self.path)
            .await
            .map_err(|error| format!("open {}: {error}", self.path.display()))?;
        let mut size = 0_u64;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1 << 20];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|error| format!("read {}: {error}", self.path.display()))?;
            if read == 0 {
                break;
            }
            size = size
                .checked_add(read as u64)
                .ok_or_else(|| format!("file size overflow: {}", self.path.display()))?;
            hasher.update(&buffer[..read]);
        }
        let hash = crate::protocol::store_commit::ObjectHash::from_digest(hasher.finalize().into());
        if size != self.expected_size || hash != self.expected_hash {
            return Err(format!(
                "plaintext facts {size}/{hash} differ from expected facts {}/{}",
                self.expected_size, self.expected_hash
            ));
        }
        Ok(())
    }

    async fn ensure_parent(&self) -> Result<(), String> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| format!("blob path has no parent: {}", self.path.display()))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("create blob parent {}: {error}", parent.display()))
    }

    async fn verify_durable(&self) -> Result<(), String> {
        let length = tokio::fs::metadata(&self.path)
            .await
            .map_err(|error| format!("stat {}: {error}", self.path.display()))?
            .len();
        if length != self.expected_size {
            return Err(format!(
                "destination {} has {length} bytes, expected {}",
                self.path.display(),
                self.expected_size
            ));
        }
        crate::atomic_file::sync_parent_dir(&self.path).await
    }

    async fn remove(&self) -> Result<(), String> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("remove {}: {error}", self.path.display())),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LocalBlobTransitions {
    database: StoreDatabase,
    store_dir: StoreDir,
}

impl LocalBlobTransitions {
    pub(crate) fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        Self {
            database,
            store_dir,
        }
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
    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        let db = &self.database;
        let tables = db.synced_tables().to_vec();
        let gate_col = gated_root_gate_col(&tables, root_table)?;
        let locality = db
            .gated_root_locality(root_table, &gate_col, root_id)
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
            if reference.authority() != &crate::protocol::blob::RowBlobAuthority::Local
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
                Provenance::HostProvided => self
                    .store_dir
                    .local_blob_path(&blob.namespace, &blob.id)
                    .map_err(|error| MakeRemoteError::Source {
                        blob_id: blob.id.clone(),
                        path: format!("local/{}/{}", blob.namespace, blob.id),
                        detail: error.to_string(),
                    })?,
            };
            let source = ExactPlaintextFile::new(
                source_path.clone(),
                reference.plaintext_size(),
                reference.plaintext_hash(),
            );
            source
                .verify()
                .await
                .map_err(|detail| MakeRemoteError::Source {
                    blob_id: blob.id.clone(),
                    path: source_path.display().to_string(),
                    detail,
                })?;
            uploads.push((reference, source_path));
        }

        let created_at = db.stamp();
        let locality = db
            .begin_make_remote(root_table, &gate_col, root_id, pin, created_at, uploads)
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

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        gated_root_gate_col(self.database.synced_tables(), root_table)?;
        self.database
            .cancel_make_remote(root_table, root_id)
            .await
            .map_err(MakeRemoteError::from)
    }

    async fn prepare_make_local(
        &self,
        root_table: &str,
        root_id: &str,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<PreparedMakeLocal, MakeLocalError> {
        let tables = self.database.synced_tables().to_vec();
        self.database
            .validate_store_write_routing(routing_encryption)?;
        if is_remote_root(&tables, root_table) {
            return Err(MakeLocalError::RemoteRoot(root_table.to_string()));
        }
        let gate_column = gate_column(&tables, root_table)
            .ok_or_else(|| MakeLocalError::NotGated(root_table.to_string()))?
            .to_string();
        match self
            .database
            .gated_root_locality(root_table, &gate_column, root_id)
            .await?
        {
            Some(true) => {}
            Some(false) => {
                return Err(MakeLocalError::AlreadyLocal(
                    root_table.to_string(),
                    root_id.to_string(),
                ));
            }
            None => {
                return Err(MakeLocalError::UnresolvedLocality(
                    root_table.to_string(),
                    root_id.to_string(),
                ));
            }
        }

        let references = self
            .database
            .row_blob_refs_for_root(root_table, root_id)
            .await?;
        for reference in &references {
            if !matches!(
                reference.authority(),
                crate::protocol::blob::RowBlobAuthority::Remote(_)
            ) || reference.stored().is_none()
            {
                return Err(MakeLocalError::UnresolvedLocality(
                    root_table.to_string(),
                    root_id.to_string(),
                ));
            }
        }
        Ok(PreparedMakeLocal {
            gate_column,
            references,
        })
    }

    async fn commit_make_local(
        &self,
        root_table: &str,
        root_id: &str,
        gate_column: &str,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        records: Vec<crate::database::MaterializedLocalBlob>,
    ) -> Result<(), DbError> {
        self.database
            .commit_make_local(
                root_table,
                root_id,
                gate_column,
                self.database.stamp(),
                routing_encryption,
                records,
            )
            .await
    }
}

struct PreparedMakeLocal {
    gate_column: String,
    references: Vec<RowBlobRef>,
}

pub(crate) struct ConnectedBlobTransitions {
    local: LocalBlobTransitions,
    blob_access: crate::store_blobs::StoreBlobAccess,
    routing_encryption: Option<crate::encryption::EncryptionService>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
}

impl ConnectedBlobTransitions {
    pub(crate) fn new(
        local: LocalBlobTransitions,
        blob_access: crate::store_blobs::StoreBlobAccess,
        routing_encryption: Option<crate::encryption::EncryptionService>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
    ) -> Self {
        Self {
            local,
            blob_access,
            routing_encryption,
            observer,
        }
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        self.local.make_remote(root_table, root_id, pin).await
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        self.local.cancel_make_remote(root_table, root_id).await
    }
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
/// whose truth is the root's Local/Remote state —
/// [`LocalBlobTransitions::make_remote`] reads it to refuse a root already Remote.
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

pub(crate) enum PostUpload {
    Waiting,
    Cancelled,
    MadeRemote { root_table: String, root_id: String },
}

// ===========================================================================
// make_local — foreground operation with a cancel signal
// ===========================================================================

/// Local files and database records produced by one make-local attempt before
/// its atomic database commit. Until that commit succeeds, every file created by
/// the attempt remains a rollback obligation owned by this value.
struct MakeLocalMaterialization<'operation> {
    transitions: &'operation ConnectedBlobTransitions,
    root_table: &'operation str,
    root_id: &'operation str,
    gate_column: String,
    records: Vec<crate::database::MaterializedLocalBlob>,
    created_files: Vec<ExactPlaintextFile>,
}

impl<'operation> MakeLocalMaterialization<'operation> {
    fn new(
        transitions: &'operation ConnectedBlobTransitions,
        root_table: &'operation str,
        root_id: &'operation str,
        gate_column: String,
    ) -> Self {
        Self {
            transitions,
            root_table,
            root_id,
            gate_column,
            records: Vec::new(),
            created_files: Vec::new(),
        }
    }

    fn record_created_file(&mut self, file: ExactPlaintextFile) {
        self.created_files.push(file);
    }

    fn record_blob(&mut self, record: crate::database::MaterializedLocalBlob) {
        self.records.push(record);
    }

    async fn abort(self, cause: MakeLocalError) -> MakeLocalError {
        for file in self.created_files {
            if let Err(detail) = file.remove().await {
                return MakeLocalError::Cleanup {
                    path: file.path().display().to_string(),
                    detail,
                };
            }
        }
        cause
    }

    async fn commit(self) -> Result<(), MakeLocalError> {
        let records = self.records.clone();
        match self
            .transitions
            .local
            .commit_make_local(
                self.root_table,
                self.root_id,
                &self.gate_column,
                self.transitions.routing_encryption.clone(),
                records,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => Err(self.abort(MakeLocalError::Db(error)).await),
        }
    }
}

/// One blob materialized back to a local file by
/// [`ConnectedBlobTransitions::make_local`], carrying what the single commit needs.
/// `dest` is present for a user-provided blob whose local home is the user's path;
/// absent for a host-provided blob whose local home is coven's local store.
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
impl ConnectedBlobTransitions {
    pub(crate) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        let prepared = self
            .local
            .prepare_make_local(root_table, root_id, self.routing_encryption.as_ref())
            .await?;

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
        // an aborted make_local leaves no partial materialization behind. The retained
        // materialization owns that cleanup obligation until the database commit succeeds.
        let mut materialization =
            MakeLocalMaterialization::new(self, root_table, root_id, prepared.gate_column);
        if let Err(error) = self
            .materialize_blobs(
                root_table,
                root_id,
                &prepared.references,
                dest,
                cancel,
                &mut materialization,
            )
            .await
        {
            return Err(materialization.abort(error).await);
        }

        // The single atomic commit: flip false + register external refs (user-provided
        // only) + enqueue the cloud deletes, together. The destructive cloud delete is
        // durable inside this commit, so a crash right after can never leave the root
        // Local with the cloud blobs un-tombstoned.
        materialization.commit().await?;

        if let Some(obs) = self.observer.as_deref() {
            obs.on_root_made_local(root_table, root_id).await;
        }
        Ok(())
    }

    /// Materialize every remote blob through this operation's retained access,
    /// local store, and observer, recording published paths for rollback.
    async fn materialize_blobs(
        &self,
        root_table: &str,
        root_id: &str,
        refs: &[RowBlobRef],
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
        materialization: &mut MakeLocalMaterialization<'_>,
    ) -> Result<(), MakeLocalError> {
        let total = refs.len() as u64;

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
            let record =
                match blob.provenance {
                    Provenance::UserProvided => {
                        let dest_path = dest
                            .get(&blob.id)
                            .ok_or_else(|| MakeLocalError::MissingDest(blob.id.clone()))?
                            .clone();
                        let destination = ExactPlaintextFile::new(
                            dest_path.clone(),
                            reference.plaintext_size(),
                            reference.plaintext_hash(),
                        );
                        destination.ensure_parent().await.map_err(|detail| {
                            MakeLocalError::Write {
                                blob_id: blob.id.clone(),
                                path: dest_path.display().to_string(),
                                detail,
                            }
                        })?;
                        let staged = self
                            .blob_access
                            .stage_verified_local_copy(reference, &dest_path)
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
                        destination.verify_durable().await.map_err(|detail| {
                            MakeLocalError::Write {
                                blob_id: blob.id.clone(),
                                path: dest_path.display().to_string(),
                                detail,
                            }
                        })?;
                        materialization.record_created_file(destination);
                        crate::database::MaterializedLocalBlob {
                            remote: reference.clone(),
                            stored,
                            destination: Some(dest_path),
                        }
                    }
                    Provenance::HostProvided => {
                        let store_path = self
                            .local
                            .store_dir
                            .local_blob_path(&blob.namespace, &blob.id)
                            .map_err(|e| MakeLocalError::Write {
                                blob_id: blob.id.clone(),
                                path: format!("local/{}/{}", blob.namespace, blob.id),
                                detail: e.to_string(),
                            })?;
                        let destination = ExactPlaintextFile::new(
                            store_path.clone(),
                            reference.plaintext_size(),
                            reference.plaintext_hash(),
                        );
                        let staged = self
                            .blob_access
                            .stage_verified_local_copy(reference, &store_path)
                            .await
                            .map_err(|e| MakeLocalError::Read(blob.id.clone(), e.to_string()))?;
                        match staged.commit_new().await {
                            Ok(()) => materialization.record_created_file(destination),
                            Err(crate::local_file::CommitNewFileError::DestinationExists(_)) => {
                                destination.verify().await.map_err(|detail| {
                                    MakeLocalError::Write {
                                        blob_id: blob.id.clone(),
                                        path: store_path.display().to_string(),
                                        detail,
                                    }
                                })?;
                            }
                            Err(error) => {
                                return Err(MakeLocalError::Write {
                                    blob_id: blob.id.clone(),
                                    path: store_path.display().to_string(),
                                    detail: error.to_string(),
                                });
                            }
                        }
                        ExactPlaintextFile::new(
                            store_path.clone(),
                            reference.plaintext_size(),
                            reference.plaintext_hash(),
                        )
                        .verify_durable()
                        .await
                        .map_err(|detail| MakeLocalError::Write {
                            blob_id: blob.id.clone(),
                            path: store_path.display().to_string(),
                            detail,
                        })?;
                        crate::database::MaterializedLocalBlob {
                            remote: reference.clone(),
                            stored,
                            destination: None,
                        }
                    }
                };
            materialization.record_blob(record);

            if let Some(obs) = self.observer.as_deref() {
                obs.on_blob_materialize_progress(
                    root_table,
                    root_id,
                    &blob.id,
                    (i + 1) as u64,
                    total,
                )
                .await;
            }
        }

        if *cancel.borrow() {
            return Err(MakeLocalError::Cancelled);
        }
        Ok(())
    }
}
