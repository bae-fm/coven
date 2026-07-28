/// Snapshot image creation, Store snapshot bootstrap, and blob installation.
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::database::Database;
use crate::migration::Migration;
use crate::sync::session::SyncedTable;
use crate::sync::storage::{StorageError, SyncStorage};

/// Default: create a snapshot after this many changesets since the last one.
const SNAPSHOT_CHANGESET_THRESHOLD: u64 = 100;

/// Default: create a snapshot after this many hours since the last one.
const SNAPSHOT_HOURS_THRESHOLD: u64 = 24;

pub(crate) struct CreatedSnapshot {
    pub db_image: Vec<u8>,
    pub blobs: Vec<SnapshotBlobFact>,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotBlobFact {
    pub fact: crate::database::StoreWriteBlobFact,
    pub audience: SnapshotBlobAudience,
    pub store_dir: crate::store_dir::StoreDir,
}

#[derive(Debug, Clone)]
pub(crate) enum SnapshotBlobAudience {
    Store,
    Circle {
        circle_id: crate::sync::circle::CircleId,
        control: crate::sync::gate::CirclePartitionControl,
    },
}

/// Error type for snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("VACUUM INTO failed: {0}")]
    VacuumFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot control JSON parse failed: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Bucket(#[from] StorageError),
    #[error("Store protocol object error: {0}")]
    StoreObject(#[source] crate::sync::store_objects::StoreObjectError),
    /// No synced tables were registered, so we cannot determine which tables
    /// are safe to share. Emitting a snapshot here would either leak every
    /// local-only table or clear the whole DB — both wrong, so we refuse.
    #[error("no synced tables registered; refusing to emit an all-cleared snapshot")]
    NoSyncedTables,
    /// Scoping the snapshot copy down to shareable data failed (sqlite FFI):
    /// either clearing local-only tables or applying the row-level gate that
    /// excludes gated-false subtrees (the changeset gate cuts them too).
    #[error("failed to scope snapshot down to shareable data: {0}")]
    ClearFailed(String),
    /// The snapshot's author is not authorized to publish a catalog image: not a
    /// current Owner of the store's membership chain, or the
    /// chain itself is not anchored to the store's owner (a wiped/refounded
    /// chain). The snapshot is refused rather than adopted.
    #[error("snapshot author is not an authorized owner: {0}")]
    UnauthorizedAuthor(String),
    /// The snapshot's synced-schema version is newer than this binary's top
    /// migration, so its DB image carries columns this binary's tables lack. The
    /// generation is refused before its image is downloaded; the same refusal is
    /// the at-open backstop in [`crate::migration::run_migrations`].
    #[error(
        "snapshot schema version {snapshot_version} is newer than this binary supports \
         ({supported}); update the app"
    )]
    SchemaTooNew {
        snapshot_version: u32,
        supported: u32,
    },
    #[error("snapshot blob preflight failed: {0}")]
    PublishBlobs(String),
    #[error("snapshot bootstrap belongs to store {bound:?}, not {requested:?}")]
    BootstrapStoreMismatch { bound: String, requested: String },
    #[error(
        "snapshot bootstrap belongs to database path {bound}, not {requested}",
        bound = .bound.display(),
        requested = .requested.display()
    )]
    BootstrapDestinationMismatch { bound: PathBuf, requested: PathBuf },
    #[error("snapshot bootstrap database changed after verification")]
    BootstrapDatabaseChanged,
    #[error("snapshot bootstrap database: {0}")]
    BootstrapDatabase(String),
    #[error("snapshot bootstrap state: {0}")]
    BootstrapState(String),
    #[error("snapshot publication state: {0}")]
    PublicationState(String),
    #[error("snapshot bootstrap failed and its incomplete database could not be removed: {cleanup} (bootstrap error: {cause})")]
    BootstrapCleanup {
        cleanup: String,
        cause: Box<SnapshotError>,
    },
}

fn prepare_snapshot_path(temp_dir: &Path) -> Result<std::path::PathBuf, SnapshotError> {
    let snapshot_path = temp_dir.join("snapshot.db");
    match std::fs::remove_file(&snapshot_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(SnapshotError::Io(e)),
    }
    Ok(snapshot_path)
}

fn cleanup_snapshot_path(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(error = %e, path = %path.display(), "failed to remove temp snapshot");
        }
    }
}

fn read_and_remove_snapshot(path: &Path) -> Result<Vec<u8>, SnapshotError> {
    let bytes = std::fs::read(path)?;
    cleanup_snapshot_path(path);
    Ok(bytes)
}

fn write_snapshot_db(target_path: &Path, plaintext: &[u8]) -> Result<(), SnapshotError> {
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(target_path, plaintext)?;
    Ok(())
}

/// SHA-256 of a snapshot DB image, hex-encoded for durable bootstrap state.
fn snapshot_db_hash(db_image: &[u8]) -> String {
    hex::encode(Sha256::digest(db_image))
}

/// Verified authority to open one downloaded snapshot as one store database and
/// install exactly its signed commit coverage. Its fields are private so callers
/// cannot transplant coverage into an unrelated database.
///
/// The authority is consumed by installation and cannot be duplicated:
///
/// ```compile_fail
/// fn duplicate(result: coven_core::sync::store::snapshot::BootstrapResult) {
///     let _copy = result.clone();
/// }
/// ```
#[derive(Debug)]
pub struct BootstrapResult {
    store_id: String,
    target_path: PathBuf,
    db_hash: String,
    store_root:
        crate::sync::store_objects::VerifiedObject<crate::sync::store_commit::StoreProtocolRoot>,
    founder_registration: crate::sync::store_objects::VerifiedObject<
        crate::sync::store_commit::StoreDeviceRegistration,
    >,
    snapshot: crate::database::PublishedStoreSnapshot,
    coverage: crate::sync::store_commit::CommitFrontier,
    stability: crate::sync::store::pull::VerifiedStoreSnapshotStability,
    #[cfg(any(test, feature = "test-utils"))]
    fail_circle_install: bool,
}

impl BootstrapResult {
    /// Arm the Circle-install failure injection carried into `open_database`'s
    /// install transaction — a test's stand-in for a crash between the Store and
    /// Circle installs.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_circle_install_for_test(mut self) -> Self {
        self.fail_circle_install = true;
        self
    }

    pub fn coverage_count(&self) -> usize {
        self.coverage.position_count()
    }

    /// Consume the verified bootstrap authority by opening its bound database file
    /// and atomically installing the Store image together with the staged Circle
    /// images the restoring identity selects.
    ///
    /// Circle staging runs between the raw image landing on disk and the final
    /// install: a throwaway copy of the raw image is opened through the same
    /// verified install authority so the identity's own access can be re-resolved
    /// from the verified control chain (never the snapshot author's preserved
    /// caches), producing per-Circle install/clear decisions. The real install
    /// then applies the Store image and every decision inside one transaction, so
    /// a partially installed union is never exposed.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_database(
        self,
        store_id: &str,
        target_path: &Path,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        device_id: String,
        migrations: &[Migration],
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        storage: &dyn SyncStorage,
        restorer_identity: &crate::keys::UserKeypair,
    ) -> Result<Database, SnapshotError> {
        let bound_path = self.target_path.clone();
        let result = async {
            if store_id != self.store_id {
                return Err(SnapshotError::BootstrapStoreMismatch {
                    bound: self.store_id,
                    requested: store_id.to_string(),
                });
            }
            let requested = std::fs::canonicalize(target_path)?;
            if requested != self.target_path {
                return Err(SnapshotError::BootstrapDestinationMismatch {
                    bound: self.target_path,
                    requested,
                });
            }
            let database_bytes = std::fs::read(&requested)?;
            if snapshot_db_hash(&database_bytes) != self.db_hash {
                return Err(SnapshotError::BootstrapDatabaseChanged);
            }
            // Capture the selection inputs before the authority is consumed into
            // the install.
            let root = crate::sync::store_commit::StoreRootRef {
                store_root_id: self.store_root.value.descriptor.store_root_id(),
                store_root_hash: self.store_root.semantic_hash,
                object: self.store_root.object.clone(),
            };
            let history_root = self.store_root.clone();
            let history_founder = self.founder_registration.clone();
            let store_frontier = self.coverage.clone();
            let install = crate::database::VerifiedSnapshotBootstrapInstall::new(
                self.snapshot,
                self.store_root,
                self.founder_registration,
                self.stability,
                routing_encryption,
                Vec::new(),
            )
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;

            let decisions = match routing_encryption {
                // Circles exist only in a scoped (Circle-routing) Store; without
                // routing encryption there are no Circle images to stage.
                Some(encryption) => {
                    let mut history_verifier =
                        crate::sync::store::pull::MergeHistoryVerifier::from_verified_root_and_founder(
                            storage,
                            &root,
                            history_root,
                            &history_founder,
                        )
                        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
                    let routing_key = crate::sync::circle::derive_row_routing_key(
                        encryption,
                        root.store_root_hash,
                    )
                    .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
                    stage_restore_circle_decisions(
                        &requested,
                        &install,
                        &synced_tables,
                        blob_tombstone_grace,
                        transfer_limits,
                        &device_id,
                        migrations,
                        storage,
                        &root,
                        &mut history_verifier,
                        &store_frontier,
                        restorer_identity,
                        Some(&routing_key),
                    )
                    .await?
                }
                None => Vec::new(),
            };
            let install = install.with_circle_decisions(decisions);
            #[cfg(any(test, feature = "test-utils"))]
            let install = if self.fail_circle_install {
                install.fail_circle_install_for_test()
            } else {
                install
            };
            let (db, _stamper) = Database::open_initialized_store(
                &requested,
                &install,
                synced_tables,
                blob_tombstone_grace,
                transfer_limits,
                device_id,
                migrations,
            )
            .map_err(|error| SnapshotError::BootstrapDatabase(error.to_string()))?;
            Ok(db)
        }
        .await;
        match result {
            Ok(db) => Ok(db),
            Err(cause) => match remove_incomplete_database(&bound_path) {
                Ok(()) => Err(cause),
                Err(cleanup) => Err(SnapshotError::BootstrapCleanup {
                    cleanup: cleanup.to_string(),
                    cause: Box::new(cause),
                }),
            },
        }
    }
}

/// Stage the Circle install/clear decisions for a restore by re-resolving the
/// restoring identity's access against a throwaway copy of the raw Store image.
/// The copy is installed through the same verified authority the real install
/// uses, queried, then deleted; only the decisions cross back to the real
/// install, which applies them in its single transaction.
#[allow(clippy::too_many_arguments)]
async fn stage_restore_circle_decisions(
    raw_image_path: &Path,
    install: &crate::database::VerifiedSnapshotBootstrapInstall,
    synced_tables: &[SyncedTable],
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: crate::blob::TransferLimits,
    device_id: &str,
    migrations: &[Migration],
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    store_frontier: &crate::sync::store_commit::CommitFrontier,
    restorer_identity: &crate::keys::UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<Vec<crate::database::StagedCircleDecision>, SnapshotError> {
    let query_path = raw_image_path.with_extension("restore-select.db");
    remove_incomplete_database(&query_path)?;
    std::fs::copy(raw_image_path, &query_path)?;
    let staged = async {
        let (query_db, _stamper) = Database::open_initialized_store(
            &query_path,
            install,
            synced_tables.to_vec(),
            blob_tombstone_grace,
            transfer_limits,
            device_id.to_string(),
            migrations,
        )
        .map_err(|error| SnapshotError::BootstrapDatabase(error.to_string()))?;
        let store_database = crate::sync::store::StoreDatabase::from_database(query_db);
        super::select_staged_circle_decisions(
            history_verifier,
            &store_database,
            storage,
            root,
            store_frontier,
            restorer_identity,
            routing_key,
        )
        .await
    }
    .await;
    remove_incomplete_database(&query_path)?;
    staged
}

fn remove_incomplete_database(path: &Path) -> std::io::Result<()> {
    for candidate in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        match std::fs::remove_file(candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Create a snapshot of the database as bytes ready for storage.
///
/// Uses `VACUUM INTO` to create a clean copy of the database at a temp path,
/// then clears every non-synced table's data from that copy, reads the bytes,
/// returns the DB image. Store publication hashes the image and appends it under
/// `store-v1/snapshot-images/{author}/{image_hash}.db`, binding the
/// semantic prefix as authenticated encryption context.
///
/// A snapshot is restored byte-for-byte as the joining device's `store.db`
/// (no migration rebuild), so it must carry only data that is eligible to
/// cross devices — the host's declared synced tables. Local-only tables
/// (per-device paths, caches) and per-device sync bookkeeping must not ride
/// along; their schemas are kept, but their rows are deleted from the copy.
///
/// `conn` is the owned live connection; `tables` is the host's synced set.
pub fn create_snapshot(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
    routing_encryption: Option<&crate::encryption::EncryptionService>,
) -> Result<Vec<u8>, SnapshotError> {
    create_snapshot_with_host_blobs(conn, temp_dir, tables, routing_encryption)
        .map(|snapshot| snapshot.db_image)
}

pub(crate) fn create_circle_snapshot_with_host_blobs(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
    routing_encryption: &crate::encryption::EncryptionService,
    circle_id: crate::sync::circle::CircleId,
) -> Result<CreatedSnapshot, SnapshotError> {
    create_snapshot_for_audience_with_host_blobs(
        conn,
        temp_dir,
        tables,
        Some(routing_encryption),
        &crate::sync::circle::Audience::Circle(circle_id),
    )
}

#[cfg(test)]
fn create_circle_snapshot(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
    routing_encryption: &crate::encryption::EncryptionService,
    circle_id: crate::sync::circle::CircleId,
) -> Result<Vec<u8>, SnapshotError> {
    create_circle_snapshot_with_host_blobs(conn, temp_dir, tables, routing_encryption, circle_id)
        .map(|snapshot| snapshot.db_image)
}

pub(crate) fn create_snapshot_with_host_blobs(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
    routing_encryption: Option<&crate::encryption::EncryptionService>,
) -> Result<CreatedSnapshot, SnapshotError> {
    create_snapshot_for_audience_with_host_blobs(
        conn,
        temp_dir,
        tables,
        routing_encryption,
        &crate::sync::circle::Audience::Store,
    )
}

fn create_snapshot_for_audience_with_host_blobs(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    audience: &crate::sync::circle::Audience,
) -> Result<CreatedSnapshot, SnapshotError> {
    // A snapshot with no synced set would either leak every local-only table or
    // clear the whole DB — both wrong. Refuse before doing any work.
    if tables.is_empty() {
        return Err(SnapshotError::NoSyncedTables);
    }

    let gates = crate::sync::gate::Gates::from_tables(conn, tables)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let routing_key = if gates.has_scoped_graph() {
        let encryption = routing_encryption.ok_or_else(|| {
            SnapshotError::ClearFailed(
                "scoped snapshot creation requires Store routing encryption".to_string(),
            )
        })?;
        let store_root =
            crate::database::required_store_root_authority_on(conn).map_err(|error| {
                SnapshotError::ClearFailed(format!("read snapshot Store root: {error}"))
            })?;
        let key =
            crate::sync::circle::derive_row_routing_key(encryption, store_root.store_root_hash)
                .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        Some(key)
    } else {
        None
    };

    let snapshot_path = prepare_snapshot_path(temp_dir)?;
    let path_str = snapshot_path
        .to_str()
        .expect("temp path should be valid UTF-8");

    // VACUUM INTO creates a clean, defragmented copy of the live database.
    if let Err(e) = conn.execute("VACUUM INTO ?1", [path_str]) {
        cleanup_snapshot_path(&snapshot_path);
        return Err(SnapshotError::VacuumFailed(e.to_string()));
    }

    // The copy is a whole-DB byte image, so it still holds every local-only
    // table's data. Strip those before reading: open the copy as its own
    // connection and DELETE from every table outside the synced set.
    if let Err(e) = clear_local_only_tables(&snapshot_path, tables, routing_key.as_ref(), audience)
    {
        cleanup_snapshot_path(&snapshot_path);
        return Err(e);
    }

    let blobs = match snapshot_blob_facts(conn, &snapshot_path, temp_dir, tables) {
        Ok(blobs) => blobs,
        Err(error) => {
            cleanup_snapshot_path(&snapshot_path);
            return Err(error);
        }
    };
    if matches!(audience, crate::sync::circle::Audience::Circle(_)) {
        if let Err(error) = strip_circle_snapshot_transport_state(&snapshot_path) {
            cleanup_snapshot_path(&snapshot_path);
            return Err(error);
        }
    }

    // Read the cleared snapshot file. The storage implementation seals it at the
    // final cloud key so the AEAD context can bind that key.
    let plaintext = read_and_remove_snapshot(&snapshot_path)?;
    let plaintext_size = plaintext.len();

    info!(plaintext_size, "created snapshot");

    Ok(CreatedSnapshot {
        db_image: plaintext,
        blobs,
    })
}

fn snapshot_blob_facts(
    live: &Connection,
    snapshot_path: &Path,
    store_path: &Path,
    tables: &[SyncedTable],
) -> Result<Vec<SnapshotBlobFact>, SnapshotError> {
    let snapshot = Connection::open(snapshot_path)
        .map_err(|error| SnapshotError::ClearFailed(format!("open scoped snapshot: {error}")))?;
    let declarations = crate::blob::decl::BlobDecls::from_tables(&snapshot, tables)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let publications = declarations
        .publication_blobs_in_db(&snapshot)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let gates = crate::sync::gate::Gates::from_tables(live, tables)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let mut facts = Vec::with_capacity(publications.len());
    for publication in publications {
        let plaintext_hash = publication.plaintext_hash.parse().map_err(|error| {
            SnapshotError::ClearFailed(format!(
                "snapshot blob {}/{} plaintext hash: {error}",
                publication.blob.namespace, publication.blob.id
            ))
        })?;
        let external_path = if publication.blob.provenance == crate::blob::Provenance::UserProvided
        {
            live.query_row(
                "SELECT path FROM local_blob_refs
                 WHERE table_name = ?1 AND row_id = ?2 AND column_name = ?3
                   AND row_stamp = ?4 AND namespace = ?5 AND blob_id = ?6",
                rusqlite::params![
                    publication.table,
                    publication.row_id,
                    publication.column,
                    publication.row_stamp,
                    publication.blob.namespace,
                    publication.blob.id,
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?
            .map(PathBuf::from)
        } else {
            None
        };
        let previous = crate::database::previous_row_blob_for_write_on(
            &snapshot,
            &publication.table,
            &publication.row_id,
            &publication.row_stamp,
            &publication.column,
            &publication.blob,
            publication.plaintext_size,
            plaintext_hash,
        )
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        let audience = match crate::sync::gate::live_row_audience(
            live,
            &gates,
            &publication.table,
            &publication.row_id,
        )
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?
        {
            crate::sync::circle::Audience::Store => SnapshotBlobAudience::Store,
            crate::sync::circle::Audience::Circle(circle_id) => SnapshotBlobAudience::Circle {
                circle_id,
                control: crate::sync::gate::active_circle_control(live, circle_id)
                    .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?,
            },
            crate::sync::circle::Audience::Local => {
                return Err(SnapshotError::ClearFailed(format!(
                    "scoped snapshot retains local blob row {:?}/{:?}",
                    publication.table, publication.row_id
                )))
            }
        };
        facts.push(SnapshotBlobFact {
            fact: crate::database::StoreWriteBlobFact {
                table: publication.table,
                row_id: publication.row_id,
                row_stamp: publication.row_stamp,
                column: publication.column,
                blob: publication.blob,
                plaintext_size: publication.plaintext_size,
                plaintext_hash,
                external_path,
                previous,
                audience_move: None,
            },
            audience,
            store_dir: crate::store_dir::StoreDir::new(store_path),
        });
    }
    Ok(facts)
}

pub(crate) fn install_snapshot_blob_graph(
    image: Vec<u8>,
    blobs: &[crate::database::PreparedSnapshotBlob],
    store_dir: &crate::store_dir::StoreDir,
) -> Result<Vec<u8>, SnapshotError> {
    if blobs.is_empty() {
        return Ok(image);
    }
    let path = store_dir.as_ref().join("snapshot-closure.db");
    cleanup_snapshot_path(&path);
    write_snapshot_db(&path, &image)?;
    let result = (|| {
        let mut conn = Connection::open(&path).map_err(|error| {
            SnapshotError::ClearFailed(format!("open snapshot closure image: {error}"))
        })?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        let tx = conn
            .transaction()
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        for blob in blobs {
            blob.remote
                .validate()
                .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
            if blob.bindings.is_empty()
                || blob
                    .bindings
                    .iter()
                    .any(|binding| binding.blob().object() != blob.remote.object())
            {
                return Err(SnapshotError::ClearFailed(
                    "snapshot blob binding differs from its remote object".to_string(),
                ));
            }
            crate::database::install_snapshot_blob_plan_on(&tx, blob).map_err(|error| {
                SnapshotError::ClearFailed(format!("install snapshot blob: {error}"))
            })?;
        }
        tx.commit()
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        conn.execute_batch("VACUUM").map_err(|error| {
            SnapshotError::ClearFailed(format!("vacuum snapshot closure: {error}"))
        })?;
        conn.close().map_err(|(_, error)| {
            SnapshotError::ClearFailed(format!("close snapshot closure image: {error}"))
        })?;
        std::fs::read(&path).map_err(SnapshotError::Io)
    })();
    cleanup_snapshot_path(&path);
    result
}

pub(crate) fn verify_circle_bootstrap_image(
    image: &[u8],
    reference: &crate::sync::circle::CircleBootstrapRef,
    circle_id: crate::sync::circle::CircleId,
    tables: &[SyncedTable],
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<(), SnapshotError> {
    if crate::sync::store_commit::ObjectHash::digest(image) != reference.image.image_hash {
        return Err(SnapshotError::ClearFailed(
            "Circle bootstrap image differs from its signed hash".to_string(),
        ));
    }
    let connection = open_database_image(image)?;
    let schema_version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    if schema_version != reference.schema_version {
        return Err(SnapshotError::ClearFailed(format!(
            "Circle bootstrap schema is {schema_version}, expected {}",
            reference.schema_version
        )));
    }
    let routing_contract =
        crate::sync::routing_contract::SyncRoutingContract::from_connection(&connection, tables)
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    if routing_contract.hash() != reference.sync_routing_hash {
        return Err(SnapshotError::ClearFailed(
            "Circle bootstrap routing contract differs from its signed hash".to_string(),
        ));
    }
    let gates = crate::sync::gate::Gates::from_tables(&connection, tables)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    if gates.has_scoped_graph() {
        let routing_key = routing_key.ok_or_else(|| {
            SnapshotError::ClearFailed(
                "scoped Circle bootstrap verification requires Store routing authentication"
                    .to_string(),
            )
        })?;
        crate::sync::gate::validate_snapshot_routing_state(
            &connection,
            &gates,
            routing_key,
            &crate::sync::circle::Audience::Circle(circle_id),
        )
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    }
    let declarations = crate::blob::decl::BlobDecls::from_tables(&connection, tables)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let rows = declarations
        .publication_blobs_in_db(&connection)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    if rows.len() != reference.blobs.len() {
        return Err(SnapshotError::ClearFailed(
            "Circle bootstrap blob closure does not exactly cover its image rows".to_string(),
        ));
    }
    for row in &rows {
        let mut matching = reference.blobs.iter().filter(|binding| {
            row.table == binding.table()
                && row.row_id == binding.row_id()
                && row.row_stamp == binding.row_stamp()
                && row.column == binding.column()
        });
        let binding = matching.next().ok_or_else(|| {
            SnapshotError::ClearFailed(
                "Circle bootstrap image row has no exact signed blob binding".to_string(),
            )
        })?;
        if matching.next().is_some()
            || &row.blob != binding.blob()
            || row.plaintext_size != binding.plaintext_size()
            || row.plaintext_hash != binding.plaintext_hash().to_string()
            || !matches!(
                binding.authority(),
                crate::blob::RowBlobAuthority::Remote(
                    crate::sync::audience_package::PackageAudience::Circle {
                        circle_id: binding_circle,
                        ..
                    }
                ) if *binding_circle == circle_id
            )
            || binding.stored().is_none()
        {
            return Err(SnapshotError::ClearFailed(
                "Circle bootstrap blob closure differs from an exact image row".to_string(),
            ));
        }
    }
    for table in crate::db::user_table_names(&connection)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?
    {
        if tables.iter().any(|synced| synced.name() == table)
            || matches!(table.as_str(), "_coven_audience" | "_coven_row_routes")
        {
            continue;
        }
        let count: i64 = connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    crate::sync::session::quote_ident(&table)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        if count != 0 {
            return Err(SnapshotError::ClearFailed(format!(
                "Circle bootstrap retains non-projection table {table:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn open_database_image(image: &[u8]) -> Result<Connection, SnapshotError> {
    let connection = crate::database::open_database_image(image)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    Ok(connection)
}

/// Delete every non-synced table's rows from the snapshot copy at `path`,
/// keeping all table schemas intact.
///
/// Opens `path` as its own connection (the copy must be edited in isolation from
/// the live DB). Errors if any step fails — a snapshot that silently dropped
/// synced data, or silently kept local-only data, is worse than no snapshot.
fn clear_local_only_tables(
    path: &Path,
    synced: &[SyncedTable],
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    audience: &crate::sync::circle::Audience,
) -> Result<(), SnapshotError> {
    let conn = Connection::open(path)
        .map_err(|e| SnapshotError::ClearFailed(format!("failed to open snapshot copy: {e}")))?;
    clear_non_synced(&conn, synced, routing_key, audience)?;
    conn.close()
        .map_err(|(_, e)| SnapshotError::ClearFailed(format!("failed to close snapshot copy: {e}")))
}

/// Non-synced authenticated indexes whose source Store commits are covered by the
/// snapshot and therefore will not replay after bootstrap. The three blob tables
/// are one foreign-key-closed ownership graph and are scoped to the surviving app
/// rows together below. Device-state snapshots retain the exact predecessor state
/// needed to extend any stream at the signed snapshot coverage frontier.
const SNAPSHOT_PRESERVED_NON_SYNCED_TABLES: &[&str] = &[
    "_coven_audience",
    "_coven_row_routes",
    "remote_objects",
    "blob_locators",
    "row_blob_locators",
    "store_device_registration_activations",
    "store_device_state_snapshots",
    "store_author_exclusion_activations",
    "circle_control_activations",
    "circle_access_cache",
    "circle_bootstrap_coverage",
    "circle_current_state",
    "retained_merge_materializations",
    "retained_replay_objects",
];

// Circle projection keeps this closed graph only until `snapshot_blob_facts`
// extracts the exact signed references. The final image strips every transport
// and ownership row; recipient installation rebuilds them from the bootstrap.
const CIRCLE_IMAGE_PRESERVED_NON_SYNCED_TABLES: &[&str] = &[
    "_coven_audience",
    "_coven_row_routes",
    "remote_objects",
    "blob_locators",
    "row_blob_locators",
    "retained_merge_materializations",
    "retained_replay_objects",
];

/// On the snapshot-copy connection, scope it down to exactly what is eligible to
/// cross devices, then VACUUM to reclaim the freed pages:
///
/// 1. Table-level: DELETE every user table not in `synced` (except the
///    [`SNAPSHOT_PRESERVED_NON_SYNCED_TABLES`]) — local-only tables keep their
///    schema, lose their rows.
/// 2. Row-level: within the synced tables, DELETE the rows the gate excludes
///    (gated-false roots and their FK-descendants), so a private subtree does
///    not ride the snapshot to a restoring peer. This is the same exclusion the
///    outbound changeset gate applies; both reuse [`crate::sync::gate::Gates`].
fn clear_non_synced(
    conn: &Connection,
    synced: &[SyncedTable],
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    audience: &crate::sync::circle::Audience,
) -> Result<(), SnapshotError> {
    let gates = crate::sync::gate::Gates::from_tables(conn, synced)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    if gates.has_scoped_graph() && routing_key.is_none() {
        return Err(SnapshotError::ClearFailed(
            "scoped snapshot projection requires a row-routing key".to_string(),
        ));
    }
    let tx = conn
        .unchecked_transaction()
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    tx.pragma_update(None, "defer_foreign_keys", "ON")
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let coverage = crate::sync::store::StoreDatabase::materialized_frontier_on(&tx, None)
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let cleared_materialization_tables = ["materialized_commits"];
    for table in cleared_materialization_tables {
        tx.execute_batch(&format!(
            "DELETE FROM {}",
            crate::sync::session::quote_ident(table)
        ))
        .map_err(|error| SnapshotError::ClearFailed(format!("clear {table}: {error}")))?;
    }
    if matches!(audience, crate::sync::circle::Audience::Store) {
        crate::sync::store::StoreDatabase::retain_snapshot_replay_inputs_on(&tx)
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        crate::sync::store::StoreDatabase::retain_snapshot_device_states_on(&tx, coverage)
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    }
    let preserved_non_synced_tables = match audience {
        crate::sync::circle::Audience::Store => SNAPSHOT_PRESERVED_NON_SYNCED_TABLES,
        crate::sync::circle::Audience::Circle(_) => CIRCLE_IMAGE_PRESERVED_NON_SYNCED_TABLES,
        crate::sync::circle::Audience::Local => {
            return Err(SnapshotError::ClearFailed(
                "Local rows cannot enter a snapshot".to_string(),
            ));
        }
    };
    for table in crate::db::user_table_names(conn)
        .map_err(|error| SnapshotError::ClearFailed(format!("list user tables: {error}")))?
    {
        if synced.iter().any(|t| t.name() == table) {
            continue;
        }
        if preserved_non_synced_tables.contains(&table.as_str()) {
            continue;
        }
        if cleared_materialization_tables.contains(&table.as_str()) {
            continue;
        }
        tx.execute_batch(&format!(
            "DELETE FROM {}",
            crate::sync::session::quote_ident(&table)
        ))
        .map_err(|e| SnapshotError::ClearFailed(format!("clear {table}: {e}")))?;
    }

    // The snapshot is a second propagation channel: the changeset gate cuts
    // gated-false rows on the wire, so the snapshot must drop them too or a
    // private subtree leaks to a restoring device. Reuse the changeset gate's
    // model rather than re-deriving the FK walk.
    match audience {
        crate::sync::circle::Audience::Store => gates
            .delete_gated_false(&tx)
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?,
        crate::sync::circle::Audience::Circle(_) => {
            crate::sync::gate::retain_snapshot_audience_rows(&tx, &gates, audience)
                .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        }
        crate::sync::circle::Audience::Local => {
            return Err(SnapshotError::ClearFailed(
                "Local rows cannot enter a snapshot".to_string(),
            ));
        }
    }
    if let Some(routing_key) = routing_key {
        crate::sync::gate::prune_private_routes_without_rows(&tx, &gates)
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
        crate::sync::gate::validate_snapshot_routing_state(&tx, &gates, routing_key, audience)
            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    }

    scope_authenticated_blob_graph(&tx, synced)?;
    tx.commit()
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;

    // Circle projection removes its temporary transport graph after blob facts
    // are extracted and vacuums once after that final deletion.
    if matches!(audience, crate::sync::circle::Audience::Store) {
        conn.execute_batch("VACUUM")
            .map_err(|e| SnapshotError::ClearFailed(format!("vacuum: {e}")))?;
    }
    Ok(())
}

fn strip_circle_snapshot_transport_state(path: &Path) -> Result<(), SnapshotError> {
    let mut conn = Connection::open(path).map_err(|error| {
        SnapshotError::ClearFailed(format!(
            "open Circle snapshot transport projection: {error}"
        ))
    })?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let tx = conn
        .transaction()
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    tx.execute_batch(
        "DELETE FROM row_blob_locators;
         DELETE FROM blob_locators;
         DELETE FROM retained_replay_objects;
         DELETE FROM remote_objects;
         DELETE FROM retained_merge_materializations;",
    )
    .map_err(|error| {
        SnapshotError::ClearFailed(format!("strip Circle snapshot transport state: {error}"))
    })?;
    tx.commit()
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    conn.execute_batch("VACUUM").map_err(|error| {
        SnapshotError::ClearFailed(format!(
            "vacuum Circle snapshot transport projection: {error}"
        ))
    })?;
    conn.close().map_err(|(_, error)| {
        SnapshotError::ClearFailed(format!(
            "close Circle snapshot transport projection: {error}"
        ))
    })
}

fn scope_authenticated_blob_graph(
    conn: &Connection,
    synced: &[SyncedTable],
) -> Result<(), SnapshotError> {
    conn.execute_batch(
        "CREATE TEMP TABLE snapshot_live_blob_bindings (
             table_name TEXT NOT NULL,
             row_id TEXT NOT NULL,
             column_name TEXT NOT NULL,
             row_stamp TEXT NOT NULL,
             PRIMARY KEY (table_name, row_id, column_name, row_stamp)
         ) STRICT;",
    )
    .map_err(|error| SnapshotError::ClearFailed(format!("create blob scope: {error}")))?;
    for table in synced {
        let Some(declaration) = table.blob() else {
            continue;
        };
        conn.execute(
            &format!(
                "INSERT INTO snapshot_live_blob_bindings
                 (table_name, row_id, column_name, row_stamp)
                 SELECT ?1, id, ?2, _updated_at FROM {}
                 WHERE {} IS NOT NULL",
                crate::sync::session::quote_ident(table.name()),
                crate::sync::session::quote_ident(&declaration.id_column),
            ),
            rusqlite::params![table.name(), &declaration.id_column],
        )
        .map_err(|error| {
            SnapshotError::ClearFailed(format!(
                "collect live blob bindings for {:?}: {error}",
                table.name()
            ))
        })?;
    }
    conn.execute_batch(
        "DELETE FROM row_blob_locators
         WHERE NOT EXISTS (
             SELECT 1 FROM snapshot_live_blob_bindings AS live
             WHERE live.table_name = row_blob_locators.table_name
               AND live.row_id = row_blob_locators.row_id
               AND live.column_name = row_blob_locators.column_name
               AND live.row_stamp = row_blob_locators.row_stamp
         );
         DELETE FROM blob_locators
         WHERE NOT EXISTS (
             SELECT 1 FROM row_blob_locators AS binding
             WHERE binding.remote_object_id = blob_locators.remote_object_id
         );
         DELETE FROM remote_objects
         WHERE NOT EXISTS (
             SELECT 1 FROM blob_locators AS locator
             WHERE locator.remote_object_id = remote_objects.object_id
         ) AND NOT EXISTS (
             SELECT 1 FROM retained_replay_objects AS retained
             WHERE retained.object_id = remote_objects.object_id
         );
         DROP TABLE snapshot_live_blob_bindings;",
    )
    .map_err(|error| SnapshotError::ClearFailed(format!("scope blob ownership graph: {error}")))?;
    Ok(())
}

/// Check whether it's time to create a new snapshot.
///
/// Returns true if:
/// - `changesets_since_snapshot` >= the changeset threshold (100), OR
/// - `hours_since_snapshot` >= the time threshold (24h), OR
/// - No snapshot has ever been created (`last_snapshot_seq` is None)
///   AND at least one changeset has been pushed.
pub(crate) fn should_create_snapshot(
    local_seq: u64,
    last_snapshot_seq: Option<u64>,
    hours_since_snapshot: Option<u64>,
) -> bool {
    // Never created a snapshot, and we have at least one changeset.
    let Some(snap_seq) = last_snapshot_seq else {
        return local_seq > 0;
    };

    let changesets_since = local_seq.saturating_sub(snap_seq);
    if changesets_since >= SNAPSHOT_CHANGESET_THRESHOLD {
        return true;
    }

    if let Some(hours) = hours_since_snapshot {
        if hours >= SNAPSHOT_HOURS_THRESHOLD && changesets_since > 0 {
            return true;
        }
    }

    false
}

/// Bootstrap a new device from an immutable Store snapshot.
///
/// The reader verifies the expected Store protocol root, loads the owner-anchored membership
/// chain, and considers only snapshot metadata signed by a current owner. It
/// selects a maximal signed coverage vector deterministically, refuses a schema
/// newer than this binary, loads the exact content-addressed image, and writes it
/// to `target_path`. Any failure returns a typed [`SnapshotError`] without
/// granting authority to install coverage.
///
/// The returned [`BootstrapResult`] binds the verified image, store, destination,
/// Store protocol root, snapshot hash, and coverage. Consuming it rechecks the image and
/// installs all bootstrap state in one database transaction.
pub async fn bootstrap_from_snapshot(
    storage: &dyn SyncStorage,
    store_id: &str,
    expected_store_root: crate::sync::store_commit::StoreRootRef,
    membership_floor: &crate::join_code::MembershipFloor,
    binary_schema_version: u32,
    target_path: &Path,
) -> Result<BootstrapResult, SnapshotError> {
    // Authenticate Store protocol root, membership, snapshot metadata, and the exact image
    // before returning installation authority.
    let (store_root, snapshot, plaintext, stability) = Box::pin(super::select_store_snapshot(
        storage,
        &expected_store_root,
        membership_floor,
        binary_schema_version,
    ))
    .await?;
    let coverage = snapshot.meta.coverage.clone();
    write_snapshot_db(target_path, &plaintext)?;
    let founder_registration =
        crate::sync::store_objects::load_founder_registration(storage, &expected_store_root)
            .await
            .map_err(|error| SnapshotError::Parse(error.to_string()))?;
    let target_path = std::fs::canonicalize(target_path)?;
    info!(
        num_positions = coverage.position_count(),
        db_size = plaintext.len(),
        path = %target_path.display(),
        "bootstrapped from snapshot"
    );

    Ok(BootstrapResult {
        store_id: store_id.to_string(),
        target_path,
        db_hash: snapshot_db_hash(&plaintext),
        store_root,
        founder_registration,
        snapshot,
        coverage,
        stability,
        #[cfg(any(test, feature = "test-utils"))]
        fail_circle_install: false,
    })
}

/// Download the blob files the DB at `db_path` references but whose local file is
/// absent, returning true once every referenced blob is on local disk.
///
/// `bootstrap_from_snapshot` writes only the catalog DB; the incremental pull
/// that follows starts past the snapshot's covered commit positions, so commits
/// already represented by the image are not replayed for their eager blobs.
/// Without this reconciliation a bootstrapped device has the rows but none of the
/// files they point at (a synced album shows a placeholder cover). Only the
/// `CacheEager` blobs are reconciled: a `CacheLazy` blob (e.g. audio) is fetched on
/// first read, so a bootstrapped device need not download it up front — this scan
/// filters [`BlobDecls::refs_in_db`](crate::blob::decl::BlobDecls::refs_in_db) to
/// the `CacheEager` class, the same class the incremental pull downloads.
///
/// coven derives the blobs the DB at `db_path` references from the blob
/// declarations in `tables`, then downloads the `CacheEager` ones via the same
/// `download_blobs` path the incremental pull uses — into the
/// locator-keyed evictable cache under `storage/cache/<namespace>`, skipping
/// any already present in either cache folder. A failed download is logged there
/// and reflected in the returned flag; the bootstrap that calls this refuses to
/// save the store unless the flag is true.
///
/// `refs_in_db` is a read-only enumeration run against a short-lived connection to
/// the same on-disk DB the `db` actor owns; `db` is still needed because
/// `download_blobs` resolves each blob's uploader through it. At bootstrap the
/// pull has not started; in a cycle this runs after the pull. It is read-only
/// either way (a SELECT, which journals nothing), so it does not re-record rows
/// or race the connection thread.
pub async fn reconcile_snapshot_blobs(
    database: &crate::sync::store::StoreDatabase,
    db_path: &Path,
    storage: &dyn SyncStorage,
    store_dir: &crate::store_dir::StoreDir,
    tables: &[SyncedTable],
    cancel: &watch::Receiver<bool>,
) -> Result<SnapshotBlobReconcile, crate::database::DbError> {
    let db = database.sqlite();
    let row_ids: Vec<(String, String)> = {
        let conn = Connection::open(db_path).map_err(crate::database::DbError::from)?;
        let mut row_ids = Vec::new();
        for table in tables {
            let Some(declaration) = table.blob() else {
                continue;
            };
            if declaration.fill != crate::blob::CacheFill::CacheEager {
                continue;
            }
            let sql = format!(
                "SELECT id FROM {} WHERE {} IS NOT NULL ORDER BY id",
                crate::sync::session::quote_ident(table.name()),
                crate::sync::session::quote_ident(&declaration.id_column),
            );
            let mut statement = conn.prepare(&sql).map_err(crate::database::DbError::from)?;
            let ids = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(crate::database::DbError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(crate::database::DbError::from)?;
            row_ids.extend(ids.into_iter().map(|id| (table.name().to_string(), id)));
        }
        row_ids
    };

    let mut blobs = Vec::with_capacity(row_ids.len());
    for (table, row_id) in row_ids {
        let reference = db.row_blob_ref(&table, &row_id).await?;
        blobs.push(
            crate::sync::store::pull::BlobDownload::from_row(reference)
                .map_err(crate::database::DbError::Message)?,
        );
    }

    if blobs.is_empty() {
        return Ok(SnapshotBlobReconcile::Complete);
    }

    let total = blobs.len();
    // The blobs are `CacheEager`, so `download_blobs` writes each into the
    // locator-keyed evictable cache under `storage/cache/<namespace>`.
    // A snapshot carries the authenticated uploader index, so each blob read
    // dispatches directly to its recorded member prefix.
    //
    // The loop lives here (rather than handing the whole batch to `download_blobs`)
    // so cancellation is checked between blobs — never mid-download — the same
    // phase-boundary discipline `make_local` uses. `download_blobs` stays a single
    // batch primitive the sync cycle can call without a cancel concern.
    let mut all_ok = true;
    for blob in blobs {
        if *cancel.borrow() {
            info!(total, "snapshot blob reconciliation cancelled");
            return Ok(SnapshotBlobReconcile::Cancelled);
        }
        if crate::sync::store::pull::download_blobs(database, vec![blob], storage, store_dir)
            .await
            .is_err()
        {
            all_ok = false;
        }
    }
    if all_ok {
        info!(total, "snapshot blob reconciliation complete");
        Ok(SnapshotBlobReconcile::Complete)
    } else {
        warn!(total, "some snapshot blob files are not local");
        Ok(SnapshotBlobReconcile::Incomplete)
    }
}

/// The outcome of [`reconcile_snapshot_blobs`]: every required eager blob is
/// local, at least one could not be downloaded, or the caller's cancel signal
/// fired between blobs and the reconcile stopped early. A three-way result, not
/// a `bool` plus an out-param, so the bootstrap can map each outcome to its own
/// error (or none) at one match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBlobReconcile {
    Complete,
    Incomplete,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::keys::UserKeypair;
    use crate::sync::store::StoreDatabase;
    use crate::sync::store_commit::CommitFrontier;

    fn open_scoped_snapshot_test_db() -> Database {
        crate::sync::test_helpers::open_test_db_schema(
            vec![
                SyncedTable::new(
                    "documents",
                    crate::sync::session::RowIdentity::IndependentUuid,
                )
                .scoped_by("audience"),
                SyncedTable::new(
                    "paragraphs",
                    crate::sync::session::RowIdentity::IndependentUuid,
                )
                .inherits_audience_through("document_id"),
            ],
            vec![Migration::sql(
                1,
                "scoped snapshot schema",
                "CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE paragraphs (
                     id TEXT PRIMARY KEY,
                     document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )],
        )
    }

    async fn seed_scoped_snapshot_rows(source: &Database) -> crate::sync::circle::CircleId {
        let tables = source.synced_tables().to_vec();
        let gates = source.gates();
        let blob_decls = source.blob_decls();
        let write_id = source.new_write_id();
        source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                let (circle, _) = crate::sync::test_helpers::install_test_active_circle(
                    connection,
                    "snapshot-route-circle",
                );
                crate::sync::store::StoreDatabase::run_store_write_transaction_on(
                    connection,
                    &tables,
                    &gates,
                    &blob_decls,
                    Some(&routing),
                    None,
                    write_id,
                    |transaction| {
                        transaction.execute(
                            "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                            (
                                "01890a5d-ac96-774b-bcce-b302099c3f74",
                                "Store document",
                                "0000000001000-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO paragraphs VALUES (?1, ?2, ?3, ?4)",
                            (
                                "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                                "01890a5d-ac96-774b-bcce-b302099c3f74",
                                "Store paragraph",
                                "0000000001001-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO documents VALUES (?1, ?2, ?3, ?4)",
                            (
                                "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                                circle.to_string(),
                                "Circle document",
                                "0000000001002-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO paragraphs VALUES (?1, ?2, ?3, ?4)",
                            (
                                "82df8bb7-52f0-44db-a8e7-3ec0e44cd609",
                                "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                                "Circle paragraph",
                                "0000000001003-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO documents VALUES (?1, 'local', ?2, ?3)",
                            (
                                "4a1b99f1-9d07-40d3-b6ac-b746e8d59983",
                                "Local document",
                                "0000000001004-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO paragraphs VALUES (?1, ?2, ?3, ?4)",
                            (
                                "5fe26b58-ecf7-48b1-bb20-13469b5b9be9",
                                "4a1b99f1-9d07-40d3-b6ac-b746e8d59983",
                                "Local paragraph",
                                "0000000001005-0000-owner",
                            ),
                        )?;
                        Ok(circle)
                    },
                )
            })
            .await
            .expect("commit scoped snapshot rows")
            .value
    }

    fn circle_bootstrap_reference(
        source: &Database,
        image: &[u8],
    ) -> crate::sync::circle::CircleBootstrapRef {
        let image_hash = crate::sync::store_commit::ObjectHash::digest(image);
        crate::sync::circle::CircleBootstrapRef {
            coverage: CommitFrontier(BTreeMap::new()),
            schema_version: source.schema_version(),
            sync_routing_hash: source.sync_routing_hash(),
            image: crate::sync::store_commit::SnapshotImageRef {
                image_hash,
                object: crate::sync::storage::ExactObjectRef::new(
                    crate::storage::cloud::ObjectSlot::logical(
                        "circle-bootstrap-routing.db".to_string(),
                    )
                    .expect("construct Circle bootstrap routing slot"),
                    image.len() as u64,
                    image_hash,
                ),
            },
            blobs: Vec::new(),
        }
    }

    #[tokio::test]
    async fn circle_bootstrap_verification_requires_authenticated_routing() {
        let source = open_scoped_snapshot_test_db();
        crate::sync::test_helpers::TestStore::create(
            &source,
            "circle-bootstrap-routing-key",
            UserKeypair::generate(),
        )
        .await
        .expect("create Circle bootstrap routing Store");
        let circle_id = seed_scoped_snapshot_rows(&source).await;
        let image_dir = tempfile::tempdir().expect("Circle bootstrap routing image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                create_circle_snapshot(
                    connection,
                    &image_path,
                    &tables,
                    &crate::encryption::EncryptionService::from_key([42; 32]),
                    circle_id,
                )
                .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create Circle bootstrap routing image");
        let reference = circle_bootstrap_reference(&source, &image);

        let error = verify_circle_bootstrap_image(
            &image,
            &reference,
            circle_id,
            source.synced_tables(),
            None,
        )
        .expect_err("scoped Circle bootstrap verification must require its routing key");
        assert!(
            error
                .to_string()
                .contains("requires Store routing authentication"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn circle_bootstrap_verification_rejects_scoped_store_rows() {
        let source = open_scoped_snapshot_test_db();
        crate::sync::test_helpers::TestStore::create(
            &source,
            "circle-bootstrap-store-row",
            UserKeypair::generate(),
        )
        .await
        .expect("create Circle bootstrap Store-row Store");
        let circle_id = seed_scoped_snapshot_rows(&source).await;
        let image_dir = tempfile::tempdir().expect("Circle bootstrap Store-row image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                create_circle_snapshot(
                    connection,
                    &image_path,
                    &tables,
                    &crate::encryption::EncryptionService::from_key([42; 32]),
                    circle_id,
                )
                .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create Circle projection for Store-row tampering");
        let routing_key = crate::sync::circle::derive_row_routing_key(
            &crate::encryption::EncryptionService::from_key([42; 32]),
            StoreDatabase::new(&source)
                .local_store_root_ref()
                .await
                .expect("read Store-row Store root")
                .expect("Store-row Store root is installed")
                .store_root_hash,
        )
        .expect("derive Store-row routing key");
        let store_row_id = "00000000-0000-4000-8000-000000000008";
        let store_row_stamp = "0000000001008-0000-owner";
        let store_routing_id =
            crate::sync::circle::row_routing_id(&routing_key, "documents", store_row_id)
                .to_string();
        let image = edit_snapshot_image(image_dir.path(), image, |connection| {
            connection
                .execute(
                    "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                    (store_row_id, "Store row in Circle image", store_row_stamp),
                )
                .expect("insert scoped Store row into Circle bootstrap");
            connection
                .execute(
                    "INSERT INTO _coven_row_routes VALUES (?1, 'documents', ?2, ?3)",
                    (&store_routing_id, store_row_id, store_row_stamp),
                )
                .expect("insert scoped Store row route into Circle bootstrap");
            connection
                .execute(
                    "INSERT INTO _coven_audience VALUES (?1, NULL, ?2)",
                    (&store_routing_id, store_row_stamp),
                )
                .expect("insert scoped Store audience mirror into Circle bootstrap");
        });
        let reference = circle_bootstrap_reference(&source, &image);

        let error = verify_circle_bootstrap_image(
            &image,
            &reference,
            circle_id,
            source.synced_tables(),
            Some(&routing_key),
        )
        .expect_err("Circle bootstrap must reject a scoped Store row");
        assert!(
            error
                .to_string()
                .contains("outside its exact audience closure"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn circle_bootstrap_verification_rejects_unscoped_rows() {
        let source = crate::sync::test_helpers::open_test_db_schema(
            vec![
                SyncedTable::new(
                    "documents",
                    crate::sync::session::RowIdentity::IndependentUuid,
                )
                .scoped_by("audience"),
                SyncedTable::new(
                    "settings",
                    crate::sync::session::RowIdentity::IndependentUuid,
                ),
            ],
            vec![Migration::sql(
                1,
                "Circle bootstrap unscoped schema",
                "CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE settings (
                     id TEXT PRIMARY KEY,
                     value TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )],
        );
        crate::sync::test_helpers::TestStore::create(
            &source,
            "circle-bootstrap-unscoped-row",
            UserKeypair::generate(),
        )
        .await
        .expect("create Circle bootstrap unscoped-row Store");
        let circle_id = source
            .call(|connection| {
                Ok::<_, crate::database::DbError>(
                    crate::sync::test_helpers::install_test_active_circle(
                        connection,
                        "circle-bootstrap-unscoped",
                    )
                    .0,
                )
            })
            .await
            .expect("install Circle bootstrap unscoped Circle");
        let image_dir = tempfile::tempdir().expect("Circle bootstrap unscoped image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                create_circle_snapshot(
                    connection,
                    &image_path,
                    &tables,
                    &crate::encryption::EncryptionService::from_key([42; 32]),
                    circle_id,
                )
                .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create Circle bootstrap unscoped image");
        let image = edit_snapshot_image(image_dir.path(), image, |connection| {
            connection
                .execute(
                    "INSERT INTO settings VALUES (?1, ?2, ?3)",
                    (
                        "00000000-0000-4000-8000-000000000009",
                        "not Circle-scoped",
                        "0000000001000-0000-owner",
                    ),
                )
                .expect("insert unscoped row into Circle bootstrap");
        });
        let reference = circle_bootstrap_reference(&source, &image);
        let routing_key = crate::sync::circle::derive_row_routing_key(
            &crate::encryption::EncryptionService::from_key([42; 32]),
            StoreDatabase::new(&source)
                .local_store_root_ref()
                .await
                .expect("read unscoped-row Store root")
                .expect("unscoped-row Store root is installed")
                .store_root_hash,
        )
        .expect("derive unscoped-row routing key");

        let error = verify_circle_bootstrap_image(
            &image,
            &reference,
            circle_id,
            source.synced_tables(),
            Some(&routing_key),
        )
        .expect_err("Circle bootstrap must reject an unscoped synced row");
        assert!(
            error
                .to_string()
                .contains("outside its exact audience closure"),
            "{error}"
        );
    }

    #[derive(Clone, Copy)]
    enum ScopedSnapshotImage {
        Valid,
        UnauthenticatedRoute,
        CircleRow,
        InvalidCircleMirror,
        OrphanStoreMirror,
    }

    struct PublishedScopedSnapshot {
        source: Database,
        store: crate::sync::test_helpers::TestStore,
        membership: crate::sync::membership::MembershipChain,
    }

    fn edit_snapshot_image(
        image_dir: &Path,
        image: Vec<u8>,
        edit: impl FnOnce(&Connection),
    ) -> Vec<u8> {
        let edited_path = image_dir.join("edited.db");
        std::fs::write(&edited_path, image).expect("write edited snapshot image");
        let connection = Connection::open(&edited_path).expect("open edited snapshot image");
        edit(&connection);
        connection
            .close()
            .map_err(|(_, error)| error)
            .expect("close edited snapshot image");
        std::fs::read(&edited_path).expect("read edited snapshot image")
    }

    async fn publish_scoped_snapshot(
        store_id: &str,
        image_kind: ScopedSnapshotImage,
    ) -> PublishedScopedSnapshot {
        let source = open_scoped_snapshot_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(&source, store_id, signer.clone())
            .await
            .expect("create published scoped snapshot Store");
        let membership = store
            .open_into(&source)
            .await
            .expect("load published scoped snapshot membership");
        seed_scoped_snapshot_rows(&source).await;

        let image_dir = tempfile::tempdir().expect("published scoped snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let image_tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                create_snapshot(connection, &image_path, &image_tables, Some(&routing))
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create published scoped snapshot image");
        let image = match image_kind {
            ScopedSnapshotImage::Valid => image,
            ScopedSnapshotImage::UnauthenticatedRoute => {
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .execute(
                            "UPDATE _coven_row_routes
                         SET routing_id =
                             '0000000000000000000000000000000000000000000000000000000000000000'
                         WHERE table_name = 'documents'",
                            [],
                        )
                        .expect("tamper private route id");
                })
            }
            ScopedSnapshotImage::CircleRow => {
                let route = source
                    .call(|connection| {
                        connection
                            .query_row(
                                "SELECT document.audience, route.routing_id, route._updated_at
                                 FROM documents AS document
                                 JOIN _coven_row_routes AS route
                                   ON route.table_name = 'documents'
                                  AND route.row_id = document.id
                                 WHERE document.id =
                                     '2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7'",
                                [],
                                |row| {
                                    Ok((
                                        row.get::<_, String>(0)?,
                                        row.get::<_, String>(1)?,
                                        row.get::<_, String>(2)?,
                                    ))
                                },
                            )
                            .map_err(crate::database::DbError::from)
                    })
                    .await
                    .expect("load Circle row route");
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .execute(
                            "INSERT INTO documents VALUES (?1, ?2, ?3, ?4)",
                            (
                                "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                                &route.0,
                                "Circle document",
                                &route.2,
                            ),
                        )
                        .expect("insert Circle row into Store snapshot");
                    connection
                        .execute(
                            "INSERT INTO _coven_row_routes VALUES (?1, 'documents', ?2, ?3)",
                            (&route.1, "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7", &route.2),
                        )
                        .expect("insert Circle private route into Store snapshot");
                })
            }
            ScopedSnapshotImage::InvalidCircleMirror => {
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .execute(
                            "UPDATE _coven_audience
                             SET circle_id = 'local'
                             WHERE routing_id = (
                                 SELECT routing_id
                                 FROM _coven_audience
                                 WHERE circle_id IS NOT NULL
                                 ORDER BY routing_id
                                 LIMIT 1
                             )",
                            [],
                        )
                        .expect("replace Circle mirror with Local audience");
                })
            }
            ScopedSnapshotImage::OrphanStoreMirror => {
                edit_snapshot_image(image_dir.path(), image, |connection| {
                    connection
                        .execute(
                            "UPDATE _coven_audience
                             SET circle_id = NULL
                             WHERE routing_id = (
                                 SELECT routing_id
                                 FROM _coven_audience
                                 WHERE circle_id IS NOT NULL
                                 ORDER BY routing_id
                                 LIMIT 1
                             )",
                            [],
                        )
                        .expect("replace Circle mirror with orphan Store audience");
                })
            }
        };
        let coverage = CommitFrontier(BTreeMap::new());
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            image,
            coverage.clone(),
            &signer,
            &membership,
            &source,
        )
        .await
        .expect("publish scoped snapshot");
        crate::sync::test_helpers::publish_store_ack_fixture(
            &source,
            &store.storage,
            coverage,
            &signer,
        )
        .await
        .expect("publish scoped snapshot acknowledgement");

        PublishedScopedSnapshot {
            source,
            store,
            membership,
        }
    }

    async fn open_published_scoped_snapshot(
        fixture: &PublishedScopedSnapshot,
        store_id: &str,
        database_path: &Path,
    ) -> Result<Database, SnapshotError> {
        let bootstrap = bootstrap_from_snapshot(
            &fixture.store.storage,
            store_id,
            fixture.store.root.clone(),
            &crate::join_code::MembershipFloor(fixture.membership.head_refs().to_vec()),
            1,
            database_path,
        )
        .await?;
        let routing = crate::encryption::EncryptionService::from_key([42; 32]);
        bootstrap
            .open_database(
                store_id,
                database_path,
                fixture.source.synced_tables().to_vec(),
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "joining-device".to_string(),
                &crate::sync::test_helpers::test_migrations(),
                Some(&routing),
                &fixture.store.storage,
                &crate::keys::UserKeypair::generate(),
            )
            .await
    }

    #[tokio::test]
    async fn snapshot_preserves_authenticated_routes_for_every_scoped_row() {
        let source = open_scoped_snapshot_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-authenticated-routes",
            UserKeypair::generate(),
        )
        .await
        .expect("create scoped snapshot Store");
        seed_scoped_snapshot_rows(&source).await;

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let image_tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                create_snapshot(connection, &image_path, &image_tables, Some(&routing))
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create scoped snapshot image");
        let inspected_path = image_dir.path().join("inspected.db");
        std::fs::write(&inspected_path, image).expect("write inspected scoped snapshot");
        let inspected = Connection::open(inspected_path).expect("open inspected scoped snapshot");
        let routes = inspected
            .query_row("SELECT COUNT(*) FROM _coven_row_routes", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count snapshot private routes");
        let mirrors = inspected
            .query_row("SELECT COUNT(*) FROM _coven_audience", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count snapshot audience mirrors");
        let materialized: (i64, i64) = inspected
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM documents),
                     (SELECT COUNT(*) FROM paragraphs)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("count scoped snapshot rows");
        assert_eq!(materialized, (1, 1));
        assert_eq!((routes, mirrors), (2, 4), "Store root {:?}", store.root);
    }

    #[tokio::test]
    async fn circle_snapshot_contains_only_its_rows_routes_and_mirrors() {
        let source = open_scoped_snapshot_test_db();
        crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-circle-projection",
            UserKeypair::generate(),
        )
        .await
        .expect("create Circle snapshot Store");
        let circle_id = seed_scoped_snapshot_rows(&source).await;

        let image_dir = tempfile::tempdir().expect("Circle snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let image_tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                create_circle_snapshot(connection, &image_path, &image_tables, &routing, circle_id)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create Circle snapshot image");
        let inspected_path = image_dir.path().join("circle.db");
        std::fs::write(&inspected_path, image).expect("write inspected Circle snapshot");
        let inspected = Connection::open(inspected_path).expect("open inspected Circle snapshot");
        let rows = inspected
            .query_row(
                "SELECT
                     (SELECT group_concat(body, ',') FROM documents),
                     (SELECT group_concat(body, ',') FROM paragraphs),
                     (SELECT COUNT(*) FROM _coven_row_routes),
                     (SELECT COUNT(*) FROM _coven_audience),
                     (SELECT COUNT(*) FROM circle_current_state),
                     (SELECT COUNT(*) FROM protocol_state),
                     (SELECT COUNT(*) FROM remote_objects),
                     (SELECT COUNT(*) FROM blob_locators),
                     (SELECT COUNT(*) FROM row_blob_locators),
                     (SELECT COUNT(*) FROM retained_merge_materializations),
                     (SELECT COUNT(*) FROM retained_replay_objects)",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                    ))
                },
            )
            .expect("inspect Circle snapshot rows");
        assert_eq!(
            rows,
            (
                "Circle document".to_string(),
                "Circle paragraph".to_string(),
                2,
                2,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            )
        );
    }

    #[tokio::test]
    async fn circle_snapshot_keeps_only_referenced_store_parent_rows() {
        let tables = vec![
            SyncedTable::new(
                "folders",
                crate::sync::session::RowIdentity::IndependentUuid,
            ),
            SyncedTable::new(
                "documents",
                crate::sync::session::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
        ];
        let source = crate::sync::test_helpers::open_test_db_schema(
            tables.clone(),
            vec![Migration::sql(
                1,
                "Circle snapshot Store parent schema",
                "CREATE TABLE folders (
                     id TEXT PRIMARY KEY,
                     name TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     folder_id TEXT NOT NULL REFERENCES folders(id),
                     body TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )],
        );
        crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-circle-store-parent",
            UserKeypair::generate(),
        )
        .await
        .expect("create Circle parent snapshot Store");
        let gates = source.gates();
        let blob_decls = source.blob_decls();
        let write_id = source.new_write_id();
        let write_tables = tables.clone();
        let circle_id = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                let (circle_id, _) = crate::sync::test_helpers::install_test_active_circle(
                    connection,
                    "snapshot-parent-circle",
                );
                StoreDatabase::run_store_write_transaction_on(
                    connection,
                    &write_tables,
                    &gates,
                    &blob_decls,
                    Some(&routing),
                    None,
                    write_id,
                    |transaction| {
                        transaction.execute(
                            "INSERT INTO folders VALUES (?1, 'kept', ?2)",
                            (
                                "93c8343e-6a43-4d66-9aba-f275825047ac",
                                "0000000001000-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO folders VALUES (?1, 'omitted', ?2)",
                            (
                                "7d748d61-0a3b-4c79-9651-75be31988680",
                                "0000000001001-0000-owner",
                            ),
                        )?;
                        transaction.execute(
                            "INSERT INTO documents VALUES (?1, ?2, ?3, 'Circle document', ?4)",
                            (
                                "17052cff-e9ce-469a-8987-bf4e02c2ce0d",
                                circle_id.to_string(),
                                "93c8343e-6a43-4d66-9aba-f275825047ac",
                                "0000000001002-0000-owner",
                            ),
                        )?;
                        Ok(circle_id)
                    },
                )
            })
            .await
            .expect("commit Circle row with Store parent")
            .value;

        let image_dir = tempfile::tempdir().expect("Circle parent snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let image = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                create_circle_snapshot(connection, &image_path, &tables, &routing, circle_id)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create Circle snapshot with Store parent");
        let reference = circle_bootstrap_reference(&source, &image);
        let routing_key = crate::sync::circle::derive_row_routing_key(
            &crate::encryption::EncryptionService::from_key([42; 32]),
            StoreDatabase::new(&source)
                .local_store_root_ref()
                .await
                .expect("read Circle parent Store root")
                .expect("Circle parent Store root is installed")
                .store_root_hash,
        )
        .expect("derive Circle parent routing key");
        verify_circle_bootstrap_image(
            &image,
            &reference,
            circle_id,
            source.synced_tables(),
            Some(&routing_key),
        )
        .expect("verify Circle bootstrap with its required Store parent");
        let inspected_path = image_dir.path().join("circle-parent.db");
        std::fs::write(&inspected_path, image).expect("write inspected Circle parent snapshot");
        let inspected =
            Connection::open(inspected_path).expect("open inspected Circle parent snapshot");
        let rows = inspected
            .query_row(
                "SELECT
                     (SELECT group_concat(name, ',') FROM folders),
                     (SELECT group_concat(body, ',') FROM documents),
                     (SELECT COUNT(*) FROM _coven_row_routes),
                     (SELECT COUNT(*) FROM _coven_audience),
                     (SELECT COUNT(*) FROM pragma_foreign_key_check)",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .expect("inspect Circle parent snapshot rows");
        assert_eq!(
            rows,
            ("kept".to_string(), "Circle document".to_string(), 1, 1, 0,)
        );
    }

    #[tokio::test]
    async fn snapshot_refuses_an_unauthenticated_live_private_route() {
        let source = open_scoped_snapshot_test_db();
        crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-invalid-live-route",
            UserKeypair::generate(),
        )
        .await
        .expect("create invalid-live-route Store");
        seed_scoped_snapshot_rows(&source).await;
        source
            .call(|connection| {
                connection
                    .execute(
                        "UPDATE _coven_row_routes
                         SET routing_id =
                             '0000000000000000000000000000000000000000000000000000000000000000'
                         WHERE table_name = 'documents'
                           AND row_id = '01890a5d-ac96-774b-bcce-b302099c3f74'",
                        [],
                    )
                    .map(|_| ())
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("corrupt live private route");

        let image_dir = tempfile::tempdir().expect("invalid-live-route snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let image_tables = source.synced_tables().to_vec();
        let result = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                create_snapshot(connection, &image_path, &image_tables, Some(&routing))
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await;
        let error = match result {
            Ok(_) => panic!("unauthenticated live private route must block snapshot creation"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("private route id does not authenticate"),
            "{error}"
        );
        assert_eq!(
            std::fs::read_dir(image_dir.path())
                .expect("read invalid-live-route snapshot directory")
                .count(),
            0,
            "route validation fails before creating snapshot files"
        );
    }

    #[tokio::test]
    async fn bootstrap_installs_a_valid_scoped_snapshot_with_authenticated_routes() {
        let store_id = "snapshot-valid-private-routes";
        let fixture = publish_scoped_snapshot(store_id, ScopedSnapshotImage::Valid).await;
        let destination = tempfile::tempdir().expect("valid-route bootstrap destination");
        let database_path = destination.path().join("store.db");
        let database = open_published_scoped_snapshot(&fixture, store_id, &database_path)
            .await
            .expect("open valid scoped snapshot");
        let counts = database
            .call(|connection| {
                connection
                    .query_row(
                        "SELECT
                             (SELECT COUNT(*) FROM documents),
                             (SELECT COUNT(*) FROM paragraphs),
                             (SELECT COUNT(*) FROM _coven_row_routes)",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        },
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("inspect valid scoped bootstrap");
        assert_eq!(counts, (1, 1, 2));
    }

    #[tokio::test]
    async fn bootstrap_migrates_before_validating_scoped_snapshot_routes() {
        const DOCUMENT_SCHEMA: &str = "CREATE TABLE documents (
             id TEXT PRIMARY KEY,
             audience TEXT,
             body TEXT NOT NULL,
             _updated_at TEXT NOT NULL
         ) STRICT;";
        let source_tables = vec![SyncedTable::new(
            "documents",
            crate::sync::session::RowIdentity::IndependentUuid,
        )
        .scoped_by("audience")];
        let source = crate::sync::test_helpers::open_test_db_schema(
            source_tables.clone(),
            vec![Migration::sql(1, "document schema", DOCUMENT_SCHEMA)],
        );
        let signer = UserKeypair::generate();
        let store_id = "snapshot-scoped-migration";
        let store = crate::sync::test_helpers::TestStore::create(&source, store_id, signer.clone())
            .await
            .expect("create scoped migration Store");
        let membership = store
            .open_into(&source)
            .await
            .expect("load scoped migration membership");
        let gates = source.gates();
        let blob_decls = source.blob_decls();
        let write_id = source.new_write_id();
        let write_tables = source_tables.clone();
        source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                StoreDatabase::run_store_write_transaction_on(
                    connection,
                    &write_tables,
                    &gates,
                    &blob_decls,
                    Some(&routing),
                    None,
                    write_id,
                    |transaction| {
                        transaction
                            .execute(
                                "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                                (
                                    "6b432d70-7440-4ba8-b824-f17d6733f252",
                                    "Migrated document",
                                    "0000000002000-0000-owner",
                                ),
                            )
                            .map(|_| ())
                            .map_err(crate::database::DbError::from)
                    },
                )
            })
            .await
            .expect("commit pre-migration scoped row");

        let image_dir = tempfile::tempdir().expect("scoped migration snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let snapshot_tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                let routing = crate::encryption::EncryptionService::from_key([42; 32]);
                create_snapshot(connection, &image_path, &snapshot_tables, Some(&routing))
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create pre-migration scoped snapshot");
        let coverage = CommitFrontier(BTreeMap::new());
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            image,
            coverage.clone(),
            &signer,
            &membership,
            &source,
        )
        .await
        .expect("publish pre-migration scoped snapshot");
        crate::sync::test_helpers::publish_store_ack_fixture(
            &source,
            &store.storage,
            coverage,
            &signer,
        )
        .await
        .expect("publish pre-migration snapshot acknowledgement");

        let target_tables = source_tables;
        let target_migrations = vec![
            Migration::sql(1, "document schema", DOCUMENT_SCHEMA),
            Migration::sql(
                2,
                "ordinary document column",
                "ALTER TABLE documents
                     ADD COLUMN ordinary TEXT NOT NULL DEFAULT 'ordinary';
                 CREATE INDEX documents_ordinary ON documents(ordinary);",
            ),
        ];
        let destination = tempfile::tempdir().expect("scoped migration bootstrap destination");
        let database_path = destination.path().join("store.db");
        let bootstrap = bootstrap_from_snapshot(
            &store.storage,
            store_id,
            store.root.clone(),
            &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
            2,
            &database_path,
        )
        .await
        .expect("verify pre-migration scoped snapshot");
        let routing = crate::encryption::EncryptionService::from_key([42; 32]);
        let database = bootstrap
            .open_database(
                store_id,
                &database_path,
                target_tables,
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "joining-device".to_string(),
                &target_migrations,
                Some(&routing),
                &store.storage,
                &crate::keys::UserKeypair::generate(),
            )
            .await
            .expect("migrate and validate scoped snapshot");
        assert_eq!(database.schema_version(), 2);
        let migrated = database
            .call(|connection| {
                connection
                    .query_row(
                        "SELECT
                             (SELECT COUNT(*) FROM documents),
                             (SELECT COUNT(*) FROM _coven_row_routes),
                             (SELECT ordinary FROM documents)",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("inspect migrated scoped snapshot");
        assert_eq!(migrated, (1, 1, "ordinary".to_string()));
    }

    #[tokio::test]
    async fn bootstrap_rejects_a_signed_snapshot_with_an_unauthenticated_private_route() {
        let store_id = "snapshot-invalid-private-route";
        let fixture =
            publish_scoped_snapshot(store_id, ScopedSnapshotImage::UnauthenticatedRoute).await;
        let destination = tempfile::tempdir().expect("route-tamper bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = open_published_scoped_snapshot(&fixture, store_id, &database_path).await;
        let error = match result {
            Ok(_) => panic!("unauthenticated private route must block bootstrap"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("private route id does not authenticate"),
            "{error}"
        );
        assert!(
            !database_path.exists(),
            "failed bootstrap removes the unauthenticated database image"
        );
    }

    #[tokio::test]
    async fn bootstrap_rejects_a_store_snapshot_containing_a_circle_row() {
        let store_id = "snapshot-store-image-circle-row";
        let fixture = publish_scoped_snapshot(store_id, ScopedSnapshotImage::CircleRow).await;
        let destination = tempfile::tempdir().expect("Circle-row bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = open_published_scoped_snapshot(&fixture, store_id, &database_path).await;
        let error = match result {
            Ok(_) => panic!("Store snapshot containing a Circle row must block bootstrap"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Store snapshot contains Circle row"),
            "{error}"
        );
        assert!(
            !database_path.exists(),
            "failed bootstrap removes the Circle-bearing Store image"
        );
    }

    #[tokio::test]
    async fn bootstrap_rejects_an_invalid_opaque_circle_mirror() {
        let store_id = "snapshot-invalid-opaque-circle-mirror";
        let fixture =
            publish_scoped_snapshot(store_id, ScopedSnapshotImage::InvalidCircleMirror).await;
        let destination = tempfile::tempdir().expect("invalid-mirror bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = open_published_scoped_snapshot(&fixture, store_id, &database_path).await;
        let error = match result {
            Ok(_) => panic!("invalid opaque Circle mirror must block bootstrap"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Store audience mirror has invalid audience"),
            "{error}"
        );
        assert!(
            !database_path.exists(),
            "failed bootstrap removes the invalid-mirror Store image"
        );
    }

    #[tokio::test]
    async fn bootstrap_rejects_an_orphan_store_mirror() {
        let store_id = "snapshot-orphan-store-mirror";
        let fixture =
            publish_scoped_snapshot(store_id, ScopedSnapshotImage::OrphanStoreMirror).await;
        let destination = tempfile::tempdir().expect("orphan-mirror bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = open_published_scoped_snapshot(&fixture, store_id, &database_path).await;
        let error = match result {
            Ok(_) => panic!("orphan Store mirror must block bootstrap"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("Store audience mirror has no materialized row"),
            "{error}"
        );
        assert!(
            !database_path.exists(),
            "failed bootstrap removes the orphan-mirror Store image"
        );
    }

    #[tokio::test]
    async fn snapshot_retains_only_frontier_device_states_without_exclusion_authority() {
        let source = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-device-state-frontier",
            signer.clone(),
        )
        .await
        .expect("create device-state snapshot Store");
        let membership = store
            .open_into(&source)
            .await
            .expect("open device-state snapshot Store membership");
        let device_id = source
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load snapshot device id")
            .expect("snapshot Store has a local device id");
        let (_store_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        for sequence in 1..=3 {
            crate::sync::test_helpers::host_exec(
                &source,
                &format!(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('snapshot-state-{sequence}', 'state', NULL, 1, \
                             '000000000100{sequence}-0000-state', '2026-07-21')"
                ),
            )
            .await;
            assert!(crate::sync::store::preparation::prepare_store_write(
                &StoreDatabase::new(&source),
                &store.storage,
                &device_id,
                "2026-07-21T00:00:00Z",
                &signer,
                &store_dir,
                &membership,
            )
            .await
            .expect("prepare snapshot history write"));
            assert_eq!(
                crate::sync::store::publication::drain_store_writes(
                    &StoreDatabase::new(&source),
                    &store.storage,
                )
                .await
                .expect("publish snapshot history write"),
                1,
            );
        }
        let expected = StoreDatabase::new(&source)
            .materialized_frontier()
            .await
            .expect("load snapshot frontier")
            .into_values()
            .map(|reference| serde_json::to_string(&reference).expect("encode frontier reference"))
            .collect::<BTreeSet<_>>();
        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = crate::sync::test_helpers::test_synced_tables();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &image_path, &tables, None)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create scoped snapshot image");
        let scoped_path = image_dir.path().join("scoped.db");
        std::fs::write(&scoped_path, image).expect("write scoped snapshot image");
        let scoped = Connection::open(&scoped_path).expect("open scoped snapshot image");
        let mut statement = scoped
            .prepare("SELECT commit_ref FROM store_device_state_snapshots ORDER BY commit_ref")
            .expect("query scoped device states");
        let actual = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("read scoped device states")
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .expect("collect scoped device states");
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn bootstrap_installs_the_verified_exact_store_root() {
        Box::pin(run_bootstrap_installs_the_verified_exact_store_root()).await;
    }

    async fn run_bootstrap_installs_the_verified_exact_store_root() {
        let source = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-bootstrap-exact-root",
            signer.clone(),
        )
        .await
        .expect("create exact bootstrap Store");
        let membership = store
            .open_into(&source)
            .await
            .expect("open bootstrap Store membership");
        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = crate::sync::test_helpers::test_synced_tables();
        let image_tables = tables.clone();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &image_path, &image_tables, None)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create bootstrap database image");
        let published_snapshot = crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            image,
            CommitFrontier(BTreeMap::new()),
            &signer,
            &membership,
            &source,
        )
        .await
        .expect("publish bootstrap database image");
        crate::sync::store::stage_store_acknowledgement_for_test(
            &source,
            &store.storage,
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:01Z".to_string(),
            &signer,
        )
        .await
        .expect("stage snapshot stability acknowledgement");
        crate::sync::store::drain_store_acknowledgements_for_test(&source, &store.storage, &signer)
            .await
            .expect("activate snapshot stability acknowledgement");

        let destination = tempfile::tempdir().expect("bootstrap destination");
        let database_path = destination.path().join("store.db");
        let bootstrap = bootstrap_from_snapshot(
            &store.storage,
            "snapshot-bootstrap-exact-root",
            store.root.clone(),
            &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
            1,
            &database_path,
        )
        .await
        .expect("verify bootstrap authority");
        let installed = bootstrap
            .open_database(
                "snapshot-bootstrap-exact-root",
                &database_path,
                tables,
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "joining-device".to_string(),
                &crate::sync::test_helpers::test_migrations(),
                None,
                &store.storage,
                &crate::keys::UserKeypair::generate(),
            )
            .await
            .expect("install bootstrap authority");

        assert_eq!(
            crate::sync::store::database::StoreDatabase::new(&installed)
                .local_store_root_ref()
                .await
                .expect("read installed Store root"),
            Some(store.root.clone()),
        );
        let baseline = installed
            .call(StoreDatabase::generation_zero_replay_baseline_on)
            .await
            .expect("load installed snapshot replay baseline");
        assert_eq!(baseline.exact_cut, published_snapshot.coverage);
        match &baseline.authority {
            crate::sync::store::retained_replay::RetainedReplayAuthority::StableSnapshot(
                authority,
            ) => {
                assert_eq!(authority.store_root, store.root);
                assert_eq!(authority.metadata, published_snapshot);
            }
            crate::sync::store::retained_replay::RetainedReplayAuthority::Genesis(_) => {
                panic!("snapshot bootstrap installed a genesis replay baseline")
            }
        }
        baseline
            .validate_image()
            .expect("validate snapshot replay baseline");
        let mut tampered = baseline.authority.clone();
        let crate::sync::store::retained_replay::RetainedReplayAuthority::StableSnapshot(authority) =
            &mut tampered
        else {
            panic!("snapshot bootstrap installed a genesis replay baseline")
        };
        authority.metadata.signature = "00".repeat(64);
        authority
            .validate()
            .expect_err("retained snapshot authority must re-open its signed metadata");
        let authority_bytes = serde_json::to_vec(&tampered).expect("serialize tampered authority");
        installed
            .call(move |connection| {
                connection
                    .execute(
                        "UPDATE retained_replay_baselines SET authority_bytes = ?1 \
                         WHERE singleton = 1",
                        [authority_bytes],
                    )
                    .map_err(crate::database::DbError::from)?;
                Ok(())
            })
            .await
            .expect("tamper retained snapshot metadata");
        installed
            .call(StoreDatabase::generation_zero_replay_baseline_on)
            .await
            .expect_err("restart must reject retained snapshot metadata with another signature");
    }

    #[tokio::test]
    async fn bootstrap_refuses_an_owner_snapshot_without_stability_acknowledgements() {
        let source = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-bootstrap-requires-stability",
            signer.clone(),
        )
        .await
        .expect("create unstable bootstrap Store");
        let membership = store
            .open_into(&source)
            .await
            .expect("open unstable bootstrap Store membership");
        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = crate::sync::test_helpers::test_synced_tables();
        let image_tables = tables.clone();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &image_path, &image_tables, None)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create unstable bootstrap database image");
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            image,
            CommitFrontier(BTreeMap::new()),
            &signer,
            &membership,
            &source,
        )
        .await
        .expect("publish unstable bootstrap database image");

        let destination = tempfile::tempdir().expect("bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = bootstrap_from_snapshot(
            &store.storage,
            "snapshot-bootstrap-requires-stability",
            store.root,
            &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
            1,
            &database_path,
        )
        .await;

        assert!(result.is_err());
        assert!(!database_path.exists());
    }

    #[tokio::test]
    async fn snapshot_removes_the_closed_merge_materialization_graph() {
        let source = crate::sync::test_helpers::open_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-merge-materialization-graph",
            UserKeypair::generate(),
        )
        .await
        .expect("create snapshot materialization Store");
        let changeset = crate::sync::test_helpers::capture_bytes(
            &source,
            &[
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('snapshot-row', 'Snapshot', 1, \
                         '0000000001000-0000-snapshot', '2026-01-01')",
            ],
        )
        .await;
        store
            .publish_changeset("snapshot", 1, &changeset, 1)
            .await
            .expect("publish snapshot materialization fixture");
        let live_counts = source
            .call(|connection| {
                let materialized = connection
                    .query_row("SELECT COUNT(*) FROM materialized_commits", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(crate::database::DbError::from)?;
                let retained = connection
                    .query_row(
                        "SELECT COUNT(*) FROM retained_merge_materializations",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(crate::database::DbError::from)?;
                let replay_objects = connection
                    .query_row("SELECT COUNT(*) FROM retained_replay_objects", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(crate::database::DbError::from)?;
                Ok::<_, crate::database::DbError>((materialized, retained, replay_objects))
            })
            .await
            .expect("count live materialization graph");
        assert!(live_counts.0 > 0);
        assert!(live_counts.1 > 0);
        assert!(live_counts.2 > 0);

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = crate::sync::test_helpers::test_synced_tables();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &image_path, &tables, None)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create materialization snapshot");
        let inspection_dir = tempfile::tempdir().expect("snapshot inspection directory");
        let inspection_path = inspection_dir.path().join("snapshot.db");
        std::fs::write(&inspection_path, image).expect("write inspected snapshot");
        let snapshot = Connection::open(inspection_path).expect("open inspected snapshot");
        for table in [
            "materialized_commits",
            "retained_replay_objects",
            "retained_merge_materializations",
        ] {
            let count = snapshot
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("count snapshot materialization table");
            assert_eq!(count, 0, "snapshot removes {table}");
        }
        let foreign_key_violations = snapshot
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("check snapshot materialization foreign keys");
        assert_eq!(foreign_key_violations, 0);
    }

    #[tokio::test]
    async fn snapshot_keeps_the_authenticated_blob_graph_closed() {
        Box::pin(run_snapshot_keeps_the_authenticated_blob_graph_closed()).await;
    }

    async fn run_snapshot_keeps_the_authenticated_blob_graph_closed() {
        let declaration = crate::sync::session::BlobDecl::new(
            "photos",
            crate::blob::Provenance::HostProvided,
            crate::blob::CacheFill::CacheEager,
        );
        let tables = crate::sync::test_helpers::test_synced_tables_with_blob(declaration.clone());
        let source = crate::sync::test_helpers::open_test_db_with_blob(declaration);
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-blob-ownership-graph",
            signer.clone(),
        )
        .await
        .expect("create exact blob Store");
        crate::sync::test_helpers::host_exec(
            &source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
        )
        .await;
        crate::sync::test_helpers::host_exec(
            &source,
            &format!(
                "INSERT INTO note_photos
                 (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo1', 'n1', 'cover', 11, '{}',
                         '0000000001000-0000-owner', '2026-01-01')",
                crate::blob::content_hash(b"cover-bytes"),
            ),
        )
        .await;
        let (_source_temp, source_dir) = crate::sync::test_helpers::temp_store_dir();
        crate::blob::local_files::store(&source_dir, "photos", "photo1", b"cover-bytes")
            .await
            .expect("stage source blob");
        let writer = crate::sync::cloud_storage::CloudSyncStorage::new(
            store.home.clone(),
            crate::sync::cloud_storage::CloudCipher::Encrypted(
                crate::encryption::EncryptionService::from_key([42; 32]),
            ),
            crate::sync::cloud_storage::BlobPathScheme::Hashed,
            "snapshot-blob-ownership-graph",
            signer,
        )
        .expect("construct blob writer");
        crate::sync::test_helpers::run_cycle_fixture(&source, writer, &source_dir)
            .await
            .expect("publish source blob");

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let image_tables = tables.clone();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &image_path, &image_tables, None)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create blob snapshot");
        let snapshot_dir = tempfile::tempdir().expect("snapshot inspection directory");
        let snapshot_path = snapshot_dir.path().join("snapshot.db");
        std::fs::write(&snapshot_path, image).expect("write inspected snapshot");
        let snapshot = Connection::open(snapshot_path).expect("open inspected snapshot");
        let graph: (String, String, String, String, String, String) = snapshot
            .query_row(
                "SELECT binding.table_name, binding.row_id, binding.column_name,
                        binding.row_stamp, locator.locator_hash, remote.object_id
                 FROM row_blob_locators AS binding
                 JOIN blob_locators AS locator
                   ON locator.remote_object_id = binding.remote_object_id
                 JOIN remote_objects AS remote
                   ON remote.object_id = locator.remote_object_id",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .expect("read closed snapshot blob graph");
        assert_eq!(graph.0, "note_photos");
        assert_eq!(graph.1, "photo1");
        assert_eq!(graph.2, "id");
        assert_eq!(graph.3, "0000000001000-0000-owner");
        assert_eq!(graph.4.len(), 64);
        assert_eq!(graph.5.len(), 64);
        let remote_state: String = snapshot
            .query_row(
                "SELECT state FROM remote_objects WHERE object_id = ?1",
                [&graph.5],
                |row| row.get(0),
            )
            .expect("read snapshot remote blob state");
        assert!(
            !remote_state.contains(source_dir.storage_dir().to_string_lossy().as_ref()),
            "snapshot remote blob state must not carry its source StoreDir",
        );
        let remote: crate::sync::remote_object::RemoteObjectRecord =
            serde_json::from_str(&remote_state).expect("parse snapshot remote blob state");
        assert!(matches!(
            remote.bytes().stored(),
            crate::sync::remote_object::RemoteStoredRepresentation::Blob { .. }
        ));
        for table in ["row_blob_locators", "blob_locators", "remote_objects"] {
            let count: i64 = snapshot
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count snapshot blob ownership table");
            assert_eq!(count, 1, "snapshot carries one {table} row");
        }
        let foreign_key_violations: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .expect("check snapshot blob foreign keys");
        assert_eq!(foreign_key_violations, 0);
    }

    fn blob_graph_activation(label: &str) -> crate::sync::store_commit::StreamActivationId {
        let registration_bytes = format!("{label} snapshot registration");
        let registration = crate::sync::store_commit::StoreDeviceRegistrationRef {
            device_id: format!("{:0>64}", label.len())
                .parse()
                .expect("valid blob graph test device id"),
            registration_hash: crate::sync::store_commit::ObjectHash::digest(
                registration_bytes.as_bytes(),
            ),
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/snapshot-registration.json"
                ))
                .expect("valid blob graph registration slot"),
                registration_bytes.len() as u64,
                crate::sync::store_commit::ObjectHash::digest(registration_bytes.as_bytes()),
            ),
        };
        crate::sync::store_commit::StreamActivation::device_authorized(
            crate::sync::store_commit::ObjectHash::digest(format!("{label} Store root").as_bytes()),
            registration,
            crate::sync::store_commit::DeviceStreamAnchor::StoreSnapshots {
                first_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/snapshots/1.json"
                ))
                .expect("valid blob graph activation slot"),
            },
        )
        .activation_id()
    }

    fn blob_graph_binding(
        row_id: &str,
        stamp: &str,
        bytes: &[u8],
    ) -> crate::sync::audience_package::RowBlobLocatorBinding {
        let plaintext_hash = crate::sync::store_commit::ObjectHash::digest(bytes);
        let uploader_bytes = b"blob graph test uploader registration";
        let uploader = crate::sync::store_commit::StoreDeviceRegistrationRef {
            device_id: "aa".repeat(32).parse().expect("valid blob graph device id"),
            registration_hash: crate::sync::store_commit::ObjectHash::digest(uploader_bytes),
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/devices/blob-graph-test-uploader.json".to_string(),
                )
                .expect("valid blob graph uploader slot"),
                uploader_bytes.len() as u64,
                crate::sync::store_commit::ObjectHash::digest(uploader_bytes),
            ),
        };
        let locator = crate::blob::locator::BlobLocator::browsable(
            "images",
            row_id,
            uploader,
            format!("photos/{row_id}.bin"),
            bytes.len() as u64,
            plaintext_hash,
        )
        .expect("valid blob graph locator");
        let slot = crate::storage::cloud::ObjectSlot::logical(locator.semantic_key())
            .expect("valid blob graph object slot");
        let object = crate::sync::storage::ExactObjectRef::new(
            slot,
            bytes.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(bytes),
        );
        crate::sync::audience_package::RowBlobLocatorBinding::new(
            "photos",
            row_id,
            stamp,
            "id",
            crate::blob::locator::StoredBlobRef::new(locator, object)
                .expect("valid blob graph stored blob"),
        )
        .expect("valid blob graph row binding")
    }

    /// The image installer's `ON CONFLICT ... DO NOTHING` on `row_blob_locators`
    /// keeps whatever binding the image already carries. When that pre-existing
    /// binding at the same row stamp points at different exact content, the
    /// install must fail loudly instead of shipping an image whose row binding
    /// contradicts the prepared blob.
    #[test]
    fn blob_graph_install_rejects_a_conflicting_existing_row_binding() {
        let dir = tempfile::tempdir().expect("blob graph conflict directory");
        let image_path = dir.path().join("image.db");
        let owner = crate::sync::remote_object::SnapshotObjectOwner {
            activation: blob_graph_activation("conflict"),
            generation: 0,
        };
        let existing = blob_graph_binding(
            "photo-conflict",
            "0000000001000-0000-owner",
            b"existing blob bytes",
        );
        let existing_remote =
            crate::sync::remote_object::RemoteObjectRecord::snapshot_activated_blob(
                existing.blob(),
                owner.clone(),
            )
            .expect("activate existing blob graph object");
        {
            let connection = Connection::open(&image_path).expect("open blob graph image");
            crate::db::apply_coven_schema(&connection).expect("apply blob graph schema");
            connection
                .execute(
                    "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
                    rusqlite::params![
                        existing_remote.object_id().to_string(),
                        serde_json::to_string(&existing_remote)
                            .expect("serialize existing blob graph object"),
                    ],
                )
                .expect("install existing blob graph object");
            connection
                .execute(
                    "INSERT INTO blob_locators (remote_object_id, locator_hash) VALUES (?1, ?2)",
                    rusqlite::params![
                        existing_remote.object_id().to_string(),
                        existing.blob().locator().locator_hash().to_string(),
                    ],
                )
                .expect("install existing blob graph locator");
            connection
                .execute(
                    "INSERT INTO row_blob_locators
                     (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        existing.table(),
                        existing.row_id(),
                        existing.column(),
                        existing.row_stamp(),
                        serde_json::to_string(&crate::sync::audience_package::PackageAudience::Store)
                            .expect("serialize existing blob graph audience"),
                        existing_remote.object_id().to_string(),
                    ],
                )
                .expect("install existing blob graph row binding");
            connection
                .close()
                .map_err(|(_, error)| error)
                .expect("close blob graph image");
        }
        let image = std::fs::read(&image_path).expect("read blob graph image");

        // Same row, column, and stamp; different content, so a different
        // locator and object.
        let replacement = blob_graph_binding(
            "photo-conflict",
            "0000000001000-0000-owner",
            b"replacement blob bytes",
        );
        let replacement_remote =
            crate::sync::remote_object::RemoteObjectRecord::snapshot_activated_blob(
                replacement.blob(),
                owner,
            )
            .expect("activate replacement blob graph object");
        let prepared = crate::database::PreparedSnapshotBlob {
            bindings: vec![replacement],
            authority: crate::sync::audience_package::PackageAudience::Store,
            remote: replacement_remote,
            spool_path: None,
        };
        let store_dir = crate::store_dir::StoreDir::new(dir.path());
        let error = install_snapshot_blob_graph(image, &[prepared], &store_dir)
            .expect_err("a conflicting existing row binding must fail the image install");
        assert!(
            error
                .to_string()
                .contains("already bound to different exact content"),
            "{error}"
        );
    }
}
