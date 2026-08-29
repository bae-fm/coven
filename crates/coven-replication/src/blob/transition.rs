//! coven-owned locality transitions: `make_remote` (Local → Remote) and
//! `make_local` (Remote → Local), plus `cancel_make_remote`.
//!
//! A blob-bearing root is **Local** (its blobs live on-device — a user-provided
//! blob at the user's path, a host-provided blob in coven's local store — and the
//! root's gate is off) or **Remote** (its blobs live in the cloud fronted by
//! coven's cache, and the gate is on). coven owns moving between the two, with the
//! durable-copy-before-delete ordering and a single atomic commit point each way:
//!
//! - `LocalBlobTransitions::make_remote` verifies and journals every
//!   blob-bearing row beneath the root,
//!   then returns. The upload drain creates
//!   each exact cloud object. Once every row has a created object, one Store write
//!   flips the gate true and drops external-file ownership. The intent and exact
//!   upload journals remain authoritative until that Store write activates, when
//!   activation consumes them atomically. Before publication starts,
//!   `LocalBlobTransitions::cancel_make_remote` marks the intent Cancelling;
//!   the upload drain
//!   exact-deletes every object and spool before atomically removing the last
//!   journal and intent, so the root remains Local.
//! - `ConnectedBlobTransitions::make_local` brings each blob back to a local
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
//! A [`SyncedTable::remote_root`](coven_protocol::synced_schema::SyncedTable::remote_root)
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

use coven_database::DbError;
use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
use coven_protocol::blob::{Provenance, RowBlobRef};

use coven_protocol::blob::BlobTransitionObserver;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;

/// Why a make_remote (or its cancel) could not be started.
#[derive(Debug, thiserror::Error)]
pub enum MakeRemoteError {
    #[error("a make_remote batch must contain at least one root")]
    EmptyBatch,
    #[error("root {0:?} appears more than once in the make_remote batch")]
    DuplicateRoot(String),
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
    #[error("external source path for blob {blob_id:?} at {path}: {source}")]
    SourcePath {
        blob_id: String,
        path: String,
        #[source]
        source: coven_foundation::store_dir::PathTokenError,
    },
    #[error("external source for blob {blob_id:?} at {path}: {source}")]
    SourceFile {
        blob_id: String,
        path: String,
        #[source]
        source: ExactPlaintextFileError,
    },
    #[error("database error: {0}")]
    Db(#[from] DbError),
}

#[derive(Clone)]
pub struct MakeRemoteRoot {
    pub id: String,
    pub label: String,
    pub refs: Vec<RowBlobRef>,
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
    #[error("remote row for blob {0:?} has no exact stored reference")]
    MissingStoredReference(String),
    #[error("read blob {blob_id:?} to materialize: {source}")]
    Read {
        blob_id: String,
        #[source]
        source: crate::sync::BlobCacheError,
    },
    #[error("materialized blob path for {blob_id:?} at {path}: {source}")]
    WritePath {
        blob_id: String,
        path: String,
        #[source]
        source: coven_foundation::store_dir::PathTokenError,
    },
    #[error("write materialized blob {blob_id:?} to {path}: {source}")]
    WriteFile {
        blob_id: String,
        path: String,
        #[source]
        source: ExactPlaintextFileError,
    },
    #[error("publish materialized blob {blob_id:?} to {path}: {source}")]
    CommitFile {
        blob_id: String,
        path: String,
        #[source]
        source: coven_foundation::local_file::CommitNewFileError,
    },
    #[error("make_local cancelled before the commit; the release stays Remote")]
    Cancelled,
    #[error("{operation}; materialized-file rollback failed: {failures}")]
    Cleanup {
        operation: Box<MakeLocalError>,
        failures: MaterializedFileCleanupFailures,
    },
    #[error("database error: {0}")]
    Db(#[from] DbError),
}

struct ExactPlaintextFile {
    path: PathBuf,
    expected_size: u64,
    expected_hash: coven_protocol::store_commit::ObjectHash,
}

#[derive(Debug, thiserror::Error)]
pub enum ExactPlaintextFileError {
    #[error("{operation} {}: {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("blob path has no parent: {}", path.display())]
    NoParent { path: PathBuf },
    #[error("file size overflow: {}", path.display())]
    SizeOverflow { path: PathBuf },
    #[error("plaintext facts {actual_size}/{actual_hash} differ from expected facts {expected_size}/{expected_hash}")]
    FactsMismatch {
        actual_size: u64,
        actual_hash: coven_protocol::store_commit::ObjectHash,
        expected_size: u64,
        expected_hash: coven_protocol::store_commit::ObjectHash,
    },
    #[error("destination {} has {actual} bytes, expected {expected}", path.display())]
    SizeMismatch {
        path: PathBuf,
        actual: u64,
        expected: u64,
    },
}

#[derive(Debug)]
pub struct MaterializedFileCleanupFailure {
    path: PathBuf,
    source: ExactPlaintextFileError,
}

#[derive(Debug)]
pub struct MaterializedFileCleanupFailures(Vec<MaterializedFileCleanupFailure>);

impl std::fmt::Display for MaterializedFileCleanupFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, failure) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{}: {}", failure.path.display(), failure.source)?;
        }
        Ok(())
    }
}

impl std::error::Error for MaterializedFileCleanupFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|failure| &failure.source as _)
    }
}

impl ExactPlaintextFile {
    fn new(
        path: PathBuf,
        expected_size: u64,
        expected_hash: coven_protocol::store_commit::ObjectHash,
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

    async fn verify(&self) -> Result<(), ExactPlaintextFileError> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncReadExt;

        let mut file = tokio::fs::File::open(&self.path).await.map_err(|source| {
            ExactPlaintextFileError::Io {
                operation: "open",
                path: self.path.clone(),
                source,
            }
        })?;
        let mut size = 0_u64;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1 << 20];
        loop {
            let read =
                file.read(&mut buffer)
                    .await
                    .map_err(|source| ExactPlaintextFileError::Io {
                        operation: "read",
                        path: self.path.clone(),
                        source,
                    })?;
            if read == 0 {
                break;
            }
            size = size.checked_add(read as u64).ok_or_else(|| {
                ExactPlaintextFileError::SizeOverflow {
                    path: self.path.clone(),
                }
            })?;
            hasher.update(&buffer[..read]);
        }
        let hash = coven_protocol::store_commit::ObjectHash::from_digest(hasher.finalize().into());
        if size != self.expected_size || hash != self.expected_hash {
            return Err(ExactPlaintextFileError::FactsMismatch {
                actual_size: size,
                actual_hash: hash,
                expected_size: self.expected_size,
                expected_hash: self.expected_hash,
            });
        }
        Ok(())
    }

    async fn ensure_parent(&self) -> Result<(), ExactPlaintextFileError> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| ExactPlaintextFileError::NoParent {
                path: self.path.clone(),
            })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| ExactPlaintextFileError::Io {
                operation: "create parent",
                path: parent.to_path_buf(),
                source,
            })
    }

    async fn verify_installed_size(&self) -> Result<(), ExactPlaintextFileError> {
        let length = tokio::fs::metadata(&self.path)
            .await
            .map_err(|source| ExactPlaintextFileError::Io {
                operation: "stat",
                path: self.path.clone(),
                source,
            })?
            .len();
        if length != self.expected_size {
            return Err(ExactPlaintextFileError::SizeMismatch {
                path: self.path.clone(),
                actual: length,
                expected: self.expected_size,
            });
        }
        Ok(())
    }

    async fn remove(&self) -> Result<(), ExactPlaintextFileError> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ExactPlaintextFileError::Io {
                operation: "remove",
                path: self.path.clone(),
                source,
            }),
        }
    }
}

#[derive(Clone)]
pub struct LocalBlobTransitions {
    database: StoreDatabase,
    store_dir: StoreDir,
}

impl LocalBlobTransitions {
    pub fn new(database: StoreDatabase, store_dir: StoreDir) -> Self {
        Self {
            database,
            store_dir,
        }
    }

    /// Start making `(root_table, root_id)` Remote: refuse a root already Remote, then
    /// verify each supplied user-provided blob's external source file, enqueue an upload per blob
    /// in the supplied order,
    /// and record the make_remote intent in one transaction. Returns once enqueued — the
    /// sync cycle uploads all needed blobs, prepares the gate change only after they
    /// land, and publishes that change before the durable intent completes. The caller
    /// triggers a sync cycle to start that work.
    ///
    /// The supplied rows must be exactly the root's current blob set. Verifying every source up front
    /// (exists + length matches the registered size)
    /// means a missing file aborts with nothing enqueued, rather than leaving a
    /// half-queued make_remote. `pin` becomes each upload's `retain_pinned`, so the
    /// blob is kept in coven's cache as a pinned (offline) copy.
    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        pin: bool,
        refs: Vec<coven_protocol::blob::RowBlobRef>,
    ) -> Result<(), MakeRemoteError> {
        require_make_remote_root(&self.database, root_table)?;
        let prepared = self
            .prepare_make_remote(root_table, root_id, root_label, refs)
            .await?;
        let locality = self
            .database
            .begin_make_remote(
                root_table,
                &prepared.root_id,
                &prepared.root_label,
                pin,
                self.database.stamp(),
                prepared.uploads,
            )
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

    async fn prepare_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        refs: Vec<RowBlobRef>,
    ) -> Result<coven_database::MakeRemoteAdmission, MakeRemoteError> {
        let db = &self.database;
        let locality = db.gated_root_locality(root_table, root_id).await?;
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
        if refs.is_empty() {
            return Err(MakeRemoteError::NothingToMakeRemote(
                root_table.to_string(),
                root_id.to_string(),
            ));
        }

        let mut uploads = Vec::with_capacity(refs.len());
        for reference in refs {
            if reference.authority() != &coven_protocol::blob::RowBlobAuthority::Local
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
                    .map_err(|source| MakeRemoteError::SourcePath {
                        blob_id: blob.id.clone(),
                        path: format!("local/{}/{}", blob.namespace, blob.id),
                        source,
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
                .map_err(|source| MakeRemoteError::SourceFile {
                    blob_id: blob.id.clone(),
                    path: source_path.display().to_string(),
                    source,
                })?;
            uploads.push((reference, source_path));
        }
        Ok(coven_database::MakeRemoteAdmission {
            root_id: root_id.to_string(),
            root_label: root_label.to_string(),
            uploads,
        })
    }

    pub(crate) async fn make_remote_batch(
        &self,
        root_table: &str,
        roots: Vec<MakeRemoteRoot>,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        require_make_remote_root(&self.database, root_table)?;
        if roots.is_empty() {
            return Err(MakeRemoteError::EmptyBatch);
        }
        let mut root_ids = std::collections::HashSet::with_capacity(roots.len());
        for root in &roots {
            if !root_ids.insert(root.id.clone()) {
                return Err(MakeRemoteError::DuplicateRoot(root.id.clone()));
            }
        }
        let mut prepared = Vec::with_capacity(roots.len());
        for root in roots {
            prepared.push(
                self.prepare_make_remote(root_table, &root.id, &root.label, root.refs)
                    .await?,
            );
        }
        self.database
            .begin_make_remote_batch(root_table, pin, self.database.stamp(), prepared)
            .await
            .map_err(MakeRemoteError::from)
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        require_make_remote_root(&self.database, root_table)?;
        self.database
            .cancel_make_remote(root_table, root_id)
            .await
            .map_err(MakeRemoteError::from)
    }

    async fn prepare_make_local(
        &self,
        root_table: &str,
        root_id: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<PreparedMakeLocal, MakeLocalError> {
        self.database
            .validate_store_write_routing(routing_encryption)?;
        require_make_local_root(&self.database, root_table)?;
        match self
            .database
            .gated_root_locality(root_table, root_id)
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
                coven_protocol::blob::RowBlobAuthority::Remote(_)
            ) || reference.stored().is_none()
            {
                return Err(MakeLocalError::UnresolvedLocality(
                    root_table.to_string(),
                    root_id.to_string(),
                ));
            }
        }
        Ok(PreparedMakeLocal { references })
    }

    async fn commit_make_local(
        &self,
        root_table: &str,
        root_id: &str,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        records: Vec<coven_database::MaterializedLocalBlob>,
    ) -> Result<(), DbError> {
        self.database
            .commit_make_local(
                root_table,
                root_id,
                self.database.stamp(),
                routing_encryption,
                records,
            )
            .await
    }
}

struct PreparedMakeLocal {
    references: Vec<RowBlobRef>,
}

/// Materializing a blob back to a local file needs one capability from the
/// connected blob access the host composes: staging a verified local copy of
/// an exact reference. Transitions name only this port.
#[async_trait::async_trait]
pub trait VerifiedLocalCopyStaging: Send + Sync {
    async fn stage_verified_local_copy(
        &self,
        reference: &RowBlobRef,
        destination: &std::path::Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, crate::sync::BlobCacheError>;
}

pub struct ConnectedBlobTransitions {
    local: LocalBlobTransitions,
    blob_access: Arc<dyn VerifiedLocalCopyStaging>,
    routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
}

impl ConnectedBlobTransitions {
    pub fn new(
        local: LocalBlobTransitions,
        blob_access: Arc<dyn VerifiedLocalCopyStaging>,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
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
        root_label: &str,
        pin: bool,
        refs: Vec<coven_protocol::blob::RowBlobRef>,
    ) -> Result<(), MakeRemoteError> {
        self.local
            .make_remote(root_table, root_id, root_label, pin, refs)
            .await
    }

    pub(crate) async fn make_remote_batch(
        &self,
        root_table: &str,
        roots: Vec<MakeRemoteRoot>,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        self.local.make_remote_batch(root_table, roots, pin).await
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        self.local.cancel_make_remote(root_table, root_id).await
    }

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
        let mut materialization = MakeLocalMaterialization::new(self, root_table, root_id);
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
            let stored = reference
                .stored()
                .cloned()
                .ok_or_else(|| MakeLocalError::MissingStoredReference(blob.id.clone()))?;

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
                    let destination = ExactPlaintextFile::new(
                        dest_path.clone(),
                        reference.plaintext_size(),
                        reference.plaintext_hash(),
                    );
                    destination.ensure_parent().await.map_err(|source| {
                        MakeLocalError::WriteFile {
                            blob_id: blob.id.clone(),
                            path: dest_path.display().to_string(),
                            source,
                        }
                    })?;
                    let staged = self
                        .blob_access
                        .stage_verified_local_copy(reference, &dest_path)
                        .await
                        .map_err(|source| MakeLocalError::Read {
                            blob_id: blob.id.clone(),
                            source,
                        })?;
                    staged
                        .commit_new()
                        .await
                        .map_err(|source| MakeLocalError::CommitFile {
                            blob_id: blob.id.clone(),
                            path: dest_path.display().to_string(),
                            source,
                        })?;
                    destination
                        .verify_installed_size()
                        .await
                        .map_err(|source| MakeLocalError::WriteFile {
                            blob_id: blob.id.clone(),
                            path: dest_path.display().to_string(),
                            source,
                        })?;
                    materialization.record_created_file(destination);
                    coven_database::MaterializedLocalBlob {
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
                        .map_err(|source| MakeLocalError::WritePath {
                            blob_id: blob.id.clone(),
                            path: format!("local/{}/{}", blob.namespace, blob.id),
                            source,
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
                        .map_err(|source| MakeLocalError::Read {
                            blob_id: blob.id.clone(),
                            source,
                        })?;
                    match staged.commit_new().await {
                        Ok(()) => materialization.record_created_file(destination),
                        Err(
                            coven_foundation::local_file::CommitNewFileError::DestinationExists(_),
                        ) => {
                            destination.verify().await.map_err(|source| {
                                MakeLocalError::WriteFile {
                                    blob_id: blob.id.clone(),
                                    path: store_path.display().to_string(),
                                    source,
                                }
                            })?;
                        }
                        Err(error) => {
                            return Err(MakeLocalError::CommitFile {
                                blob_id: blob.id.clone(),
                                path: store_path.display().to_string(),
                                source: error,
                            });
                        }
                    }
                    ExactPlaintextFile::new(
                        store_path.clone(),
                        reference.plaintext_size(),
                        reference.plaintext_hash(),
                    )
                    .verify_installed_size()
                    .await
                    .map_err(|source| MakeLocalError::WriteFile {
                        blob_id: blob.id.clone(),
                        path: store_path.display().to_string(),
                        source,
                    })?;
                    coven_database::MaterializedLocalBlob {
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

fn require_make_remote_root(
    database: &StoreDatabase,
    root_table: &str,
) -> Result<(), MakeRemoteError> {
    match database.blob_transition_root(root_table) {
        coven_database::BlobTransitionRoot::Gated => Ok(()),
        coven_database::BlobTransitionRoot::RemoteRoot => {
            Err(MakeRemoteError::RemoteRoot(root_table.to_string()))
        }
        coven_database::BlobTransitionRoot::NotGated => {
            Err(MakeRemoteError::NotGated(root_table.to_string()))
        }
    }
}

fn require_make_local_root(
    database: &StoreDatabase,
    root_table: &str,
) -> Result<(), MakeLocalError> {
    match database.blob_transition_root(root_table) {
        coven_database::BlobTransitionRoot::Gated => Ok(()),
        coven_database::BlobTransitionRoot::RemoteRoot => {
            Err(MakeLocalError::RemoteRoot(root_table.to_string()))
        }
        coven_database::BlobTransitionRoot::NotGated => {
            Err(MakeLocalError::NotGated(root_table.to_string()))
        }
    }
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
    records: Vec<coven_database::MaterializedLocalBlob>,
    created_files: Vec<ExactPlaintextFile>,
}

impl<'operation> MakeLocalMaterialization<'operation> {
    fn new(
        transitions: &'operation ConnectedBlobTransitions,
        root_table: &'operation str,
        root_id: &'operation str,
    ) -> Self {
        Self {
            transitions,
            root_table,
            root_id,
            records: Vec::new(),
            created_files: Vec::new(),
        }
    }

    fn record_created_file(&mut self, file: ExactPlaintextFile) {
        self.created_files.push(file);
    }

    fn record_blob(&mut self, record: coven_database::MaterializedLocalBlob) {
        self.records.push(record);
    }

    async fn abort(self, cause: MakeLocalError) -> MakeLocalError {
        let mut failures = Vec::new();
        for file in self.created_files {
            if let Err(source) = file.remove().await {
                failures.push(MaterializedFileCleanupFailure {
                    path: file.path().to_path_buf(),
                    source,
                });
            }
        }
        if failures.is_empty() {
            cause
        } else {
            MakeLocalError::Cleanup {
                operation: Box::new(cause),
                failures: MaterializedFileCleanupFailures(failures),
            }
        }
    }

    async fn commit(self) -> Result<(), MakeLocalError> {
        let records = self.records.clone();
        match self
            .transitions
            .local
            .commit_make_local(
                self.root_table,
                self.root_id,
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
