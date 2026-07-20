/// Snapshot image creation, Store snapshot bootstrap, and blob installation.
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{info, warn};

use super::session::SyncedTable;
use super::storage::{StorageError, SyncStorage};
use crate::database::Database;
use crate::migration::Migration;

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
        circle_id: super::circle::CircleId,
        control: super::gate::CirclePartitionControl,
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
    StoreObject(#[source] super::store_objects::StoreObjectError),
    #[error("decryption failed: {0}")]
    Decryption(String),
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
    #[error("failed to check remote blob {namespace}/{id}: {source}")]
    PublishBlobRemoteCheck {
        namespace: String,
        id: String,
        #[source]
        source: StorageError,
    },
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
/// fn duplicate(result: coven_core::sync::snapshot::BootstrapResult) {
///     let _copy = result.clone();
/// }
/// ```
#[derive(Debug)]
pub struct BootstrapResult {
    store_id: String,
    target_path: PathBuf,
    db_hash: String,
    store_root: super::store_objects::VerifiedObject<super::store_commit::StoreProtocolRoot>,
    founder_registration:
        super::store_objects::VerifiedObject<super::store_commit::StoreDeviceRegistration>,
    snapshot: crate::database::PublishedStoreSnapshot,
    coverage: super::store_commit::CommitFrontier,
}

impl BootstrapResult {
    pub fn coverage_count(&self) -> usize {
        self.coverage.position_count()
    }

    pub fn write_policy(&self) -> crate::WritePolicy {
        self.coverage.policy()
    }

    /// Consume the verified bootstrap authority by opening its bound database
    /// file and atomically installing its Store protocol root and exact signed coverage.
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
            let write_policy = self.coverage.policy();
            let (db, _stamper) = Database::open_initialized_store(
                &requested,
                synced_tables,
                blob_tombstone_grace,
                transfer_limits,
                write_policy,
                device_id,
                migrations,
            )
            .map_err(|error| SnapshotError::BootstrapDatabase(error.to_string()))?;
            db.install_bootstrap_state(
                &self.coverage,
                self.snapshot,
                self.store_root,
                self.founder_registration,
            )
            .await
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
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
) -> Result<Vec<u8>, SnapshotError> {
    create_snapshot_with_host_blobs(conn, temp_dir, tables).map(|snapshot| snapshot.db_image)
}

pub(crate) fn create_snapshot_with_host_blobs(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
) -> Result<CreatedSnapshot, SnapshotError> {
    // A snapshot with no synced set would either leak every local-only table or
    // clear the whole DB — both wrong. Refuse before doing any work.
    if tables.is_empty() {
        return Err(SnapshotError::NoSyncedTables);
    }

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
    if let Err(e) = clear_local_only_tables(&snapshot_path, tables) {
        cleanup_snapshot_path(&snapshot_path);
        return Err(e);
    }

    let blobs = snapshot_blob_facts(conn, &snapshot_path, temp_dir, tables)?;

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
    let write_policy: crate::WritePolicy = live
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [crate::database::WRITE_POLICY_STATE_KEY],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))
        .and_then(|encoded| {
            serde_json::from_str(&encoded)
                .map_err(|error| SnapshotError::ClearFailed(error.to_string()))
        })?;
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
            super::circle::Audience::Store => SnapshotBlobAudience::Store,
            super::circle::Audience::Circle(circle_id) => SnapshotBlobAudience::Circle {
                circle_id,
                control: super::gate::active_circle_control(live, circle_id, write_policy)
                    .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?,
            },
            super::circle::Audience::Local => {
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
            let object_id = blob.remote.object_id();
            let existing = tx
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
            let remote = if let Some(existing) = existing {
                let mut existing: crate::sync::remote_object::RemoteObjectRecord =
                    serde_json::from_str(&existing)
                        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
                for owner in blob.remote.snapshot_owners() {
                    existing
                        .merge_snapshot_owner(blob.bindings[0].blob(), owner.clone())
                        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
                }
                existing
            } else {
                blob.remote.clone()
            };
            tx.execute(
                "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
                 ON CONFLICT(object_id) DO UPDATE SET state = excluded.state",
                rusqlite::params![
                    object_id.to_string(),
                    serde_json::to_string(&remote)
                        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?,
                ],
            )
            .map_err(|error| {
                SnapshotError::ClearFailed(format!("install snapshot blob: {error}"))
            })?;
            tx.execute(
                "INSERT INTO blob_locators (remote_object_id, locator_hash) VALUES (?1, ?2)
                 ON CONFLICT(remote_object_id) DO NOTHING",
                rusqlite::params![
                    object_id.to_string(),
                    blob.bindings[0].blob().locator().locator_hash().to_string(),
                ],
            )
            .map_err(|error| {
                SnapshotError::ClearFailed(format!("install snapshot locator: {error}"))
            })?;
            for binding in &blob.bindings {
                tx.execute(
                    "INSERT INTO row_blob_locators
                 (table_name, row_id, column_name, row_stamp, audience_authority, remote_object_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(table_name, row_id, column_name, row_stamp) DO NOTHING",
                    rusqlite::params![
                        binding.table(),
                        binding.row_id(),
                        binding.column(),
                        binding.row_stamp(),
                        serde_json::to_string(&blob.authority)
                            .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?,
                        object_id.to_string(),
                    ],
                )
                .map_err(|error| {
                    SnapshotError::ClearFailed(format!("install snapshot row blob: {error}"))
                })?;
            }
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

/// Delete every non-synced table's rows from the snapshot copy at `path`,
/// keeping all table schemas intact.
///
/// Opens `path` as its own connection (the copy must be edited in isolation from
/// the live DB). Errors if any step fails — a snapshot that silently dropped
/// synced data, or silently kept local-only data, is worse than no snapshot.
fn clear_local_only_tables(path: &Path, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    let conn = Connection::open(path)
        .map_err(|e| SnapshotError::ClearFailed(format!("failed to open snapshot copy: {e}")))?;
    clear_non_synced(&conn, synced)?;
    conn.close()
        .map_err(|(_, e)| SnapshotError::ClearFailed(format!("failed to close snapshot copy: {e}")))
}

/// Non-synced authenticated indexes whose source Store commits are covered by the
/// snapshot and therefore will not replay after bootstrap. The three blob tables
/// are one foreign-key-closed ownership graph and are scoped to the surviving app
/// rows together below. Device-state snapshots retain the exact predecessor state
/// needed to extend any stream at the signed snapshot coverage frontier.
const SNAPSHOT_PRESERVED_NON_SYNCED_TABLES: &[&str] = &[
    "remote_objects",
    "blob_locators",
    "row_blob_locators",
    "store_device_registration_activations",
    "store_device_state_snapshots",
    "store_author_exclusion_activations",
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
fn clear_non_synced(conn: &Connection, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    let gates = crate::sync::gate::Gates::from_tables(conn, synced)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;
    let cleared_materialization_tables =
        ["materialized_commits", "retained_merge_materializations"];
    for table in cleared_materialization_tables {
        tx.execute_batch(&format!(
            "DELETE FROM {}",
            crate::sync::session::quote_ident(table)
        ))
        .map_err(|error| SnapshotError::ClearFailed(format!("clear {table}: {error}")))?;
    }
    for table in list_user_tables(conn)? {
        if synced.iter().any(|t| t.name() == table) {
            continue;
        }
        if SNAPSHOT_PRESERVED_NON_SYNCED_TABLES.contains(&table.as_str()) {
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
    gates
        .delete_gated_false(&tx)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;

    scope_authenticated_blob_graph(&tx, synced)?;
    tx.commit()
        .map_err(|error| SnapshotError::ClearFailed(error.to_string()))?;

    // Reclaim the pages freed by the DELETEs so the blob shrinks.
    conn.execute_batch("VACUUM")
        .map_err(|e| SnapshotError::ClearFailed(format!("vacuum: {e}")))?;
    Ok(())
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
         );
         DROP TABLE snapshot_live_blob_bindings;",
    )
    .map_err(|error| SnapshotError::ClearFailed(format!("scope blob ownership graph: {error}")))?;
    Ok(())
}

/// List user table names (excluding sqlite internal `sqlite_%` tables).
fn list_user_tables(conn: &Connection) -> Result<Vec<String>, SnapshotError> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .map_err(|e| SnapshotError::ClearFailed(format!("prepare table list: {e}")))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| SnapshotError::ClearFailed(format!("query table list: {e}")))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| SnapshotError::ClearFailed(format!("step table list: {e}")))
}

/// Check whether it's time to create a new snapshot.
///
/// Returns true if:
/// - `changesets_since_snapshot` >= the changeset threshold (100), OR
/// - `hours_since_snapshot` >= the time threshold (24h), OR
/// - No snapshot has ever been created (`last_snapshot_seq` is None)
///   AND at least one changeset has been pushed.
pub fn should_create_snapshot(
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
    expected_store_root: super::store_commit::StoreRootRef,
    membership_floor: &crate::join_code::MembershipFloor,
    binary_schema_version: u32,
    target_path: &Path,
) -> Result<BootstrapResult, SnapshotError> {
    // Authenticate Store protocol root, membership, snapshot metadata, and the exact image
    // before returning installation authority.
    let (store_root, write_policy, snapshot, plaintext) =
        super::store_snapshot::select_store_snapshot(
            storage,
            &expected_store_root,
            membership_floor,
            binary_schema_version,
        )
        .await?;
    let coverage = snapshot.meta.coverage.clone();
    if coverage.policy() != write_policy {
        return Err(SnapshotError::Parse(format!(
            "snapshot coverage uses {:?}, Store protocol root uses {write_policy:?}",
            coverage.policy()
        )));
    }
    write_snapshot_db(target_path, &plaintext)?;
    let founder_registration =
        super::store_objects::load_founder_registration(storage, &expected_store_root)
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
/// [`crate::sync::pull::download_blobs`] path the incremental pull uses — into the
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
    db: &crate::database::Database,
    db_path: &Path,
    storage: &dyn SyncStorage,
    store_dir: &crate::store_dir::StoreDir,
    tables: &[SyncedTable],
    cancel: &watch::Receiver<bool>,
) -> Result<SnapshotBlobReconcile, crate::database::DbError> {
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
            crate::sync::pull::BlobDownload::from_row(reference)
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
        if crate::sync::pull::download_blobs(db, vec![blob], storage, store_dir)
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::keys::UserKeypair;
    use crate::sync::store_commit::CommitFrontier;

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
                create_snapshot(connection, &image_path, &image_tables)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create bootstrap database image");
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            image,
            CommitFrontier::MergeConcurrent(BTreeMap::new()),
            &signer,
            Some(&membership),
            &source,
        )
        .await
        .expect("publish bootstrap database image");

        let destination = tempfile::tempdir().expect("bootstrap destination");
        let database_path = destination.path().join("store.db");
        let bootstrap = bootstrap_from_snapshot(
            &store.storage,
            "snapshot-bootstrap-exact-root",
            store.root.clone(),
            &crate::join_code::MembershipFloor::MergeConcurrent(membership.head_refs().to_vec()),
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
                crate::blob::TransferLimits::serial(),
                "joining-device".to_string(),
                &crate::sync::test_helpers::test_migrations(),
            )
            .await
            .expect("install bootstrap authority");

        assert_eq!(
            installed
                .local_store_root_ref()
                .await
                .expect("read installed Store root"),
            Some(store.root),
        );
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
                Ok::<_, crate::database::DbError>((materialized, retained))
            })
            .await
            .expect("count live materialization graph");
        assert!(live_counts.0 > 0);
        assert!(live_counts.1 > 0);

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let tables = crate::sync::test_helpers::test_synced_tables();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &image_path, &tables)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))
            })
            .await
            .expect("create materialization snapshot");
        let inspection_dir = tempfile::tempdir().expect("snapshot inspection directory");
        let inspection_path = inspection_dir.path().join("snapshot.db");
        std::fs::write(&inspection_path, image).expect("write inspected snapshot");
        let snapshot = Connection::open(inspection_path).expect("open inspected snapshot");
        for table in ["materialized_commits", "retained_merge_materializations"] {
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
                create_snapshot(connection, &image_path, &image_tables)
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
}
