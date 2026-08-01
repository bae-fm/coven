use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use tracing::info;

use crate::database::*;
use crate::sync::session::SyncedTable;

use super::*;

pub(crate) struct CreatedSnapshot {
    pub(crate) db_image: Vec<u8>,
    pub(crate) blobs: Vec<SnapshotBlobFact>,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotBlobFact {
    pub(crate) fact: crate::database::StoreWriteBlobFact,
    pub(crate) audience: SnapshotBlobAudience,
    pub(crate) store_dir: crate::store_dir::StoreDir,
}

#[derive(Debug, Clone)]
pub(crate) enum SnapshotBlobAudience {
    Store,
    Circle {
        circle_id: crate::protocol::circle::CircleId,
        control: crate::database::CirclePartitionControl,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotImageError {
    #[error("VACUUM INTO failed: {0}")]
    VacuumFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no synced tables registered; refusing to emit an all-cleared snapshot")]
    NoSyncedTables,
    #[error("failed to scope snapshot down to shareable data: {0}")]
    Projection(String),
    #[error(
        "could not remove staged snapshot database {path}: {cleanup}",
        path = .path.display()
    )]
    Cleanup { path: PathBuf, cleanup: String },
    #[error(
        "snapshot operation failed and staged database {path} could not be removed: {cleanup} \
         (operation error: {cause})",
        path = .path.display()
    )]
    CleanupAfterFailure {
        path: PathBuf,
        cleanup: String,
        cause: Box<SnapshotImageError>,
    },
}

/// One uncommitted SQLite image and its sidecar files.
///
/// The path remains armed until the operation commits or consumes the image.
/// Failures report cleanup failure instead of leaving a plaintext image behind.
#[derive(Debug)]
pub(crate) struct SnapshotDatabaseImage {
    path: PathBuf,
    armed: bool,
}

impl SnapshotDatabaseImage {
    pub(crate) fn prepare(path: PathBuf) -> Result<Self, SnapshotImageError> {
        let mut staged = Self { path, armed: true };
        if let Err(cleanup) = staged.remove_files() {
            staged.armed = false;
            return Err(SnapshotImageError::Cleanup {
                path: staged.path.clone(),
                cleanup: cleanup.to_string(),
            });
        }
        Ok(staged)
    }

    pub(crate) fn create(path: PathBuf, plaintext: &[u8]) -> Result<Self, SnapshotImageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self { path, armed: false }.write_new(plaintext)
    }

    pub(crate) fn replace(path: PathBuf, plaintext: &[u8]) -> Result<Self, SnapshotImageError> {
        Self::prepare(path)?.write_new(plaintext)
    }

    fn write_new(mut self, plaintext: &[u8]) -> Result<Self, SnapshotImageError> {
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
        {
            Ok(file) => file,
            Err(error) => {
                self.armed = false;
                return Err(SnapshotImageError::Io(error));
            }
        };
        self.armed = true;
        if let Err(error) = std::io::Write::write_all(&mut file, plaintext) {
            drop(file);
            return self.finish(Err(SnapshotImageError::Io(error)));
        }
        drop(file);
        Ok(self)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn read_and_discard(self) -> Result<Vec<u8>, SnapshotImageError> {
        let outcome = std::fs::read(&self.path).map_err(SnapshotImageError::Io);
        self.finish(outcome)
    }

    pub(crate) fn canonicalize(mut self) -> Result<Self, SnapshotImageError> {
        match std::fs::canonicalize(&self.path) {
            Ok(path) => {
                self.path = path;
                Ok(self)
            }
            Err(error) => self.finish(Err(SnapshotImageError::Io(error))),
        }
    }

    pub(crate) fn finish<T>(
        mut self,
        outcome: Result<T, SnapshotImageError>,
    ) -> Result<T, SnapshotImageError> {
        let cleanup = self.remove_files();
        self.armed = false;
        match (outcome, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(cause), Ok(())) => Err(cause),
            (Ok(_), Err(cleanup)) => Err(SnapshotImageError::Cleanup {
                path: self.path.clone(),
                cleanup: cleanup.to_string(),
            }),
            (Err(cause), Err(cleanup)) => Err(SnapshotImageError::CleanupAfterFailure {
                path: self.path.clone(),
                cleanup: cleanup.to_string(),
                cause: Box::new(cause),
            }),
        }
    }

    pub(crate) fn discard(mut self) -> Result<(), SnapshotImageError> {
        let cleanup = self.remove_files();
        self.armed = false;
        cleanup.map_err(|cleanup| SnapshotImageError::Cleanup {
            path: self.path.clone(),
            cleanup: cleanup.to_string(),
        })
    }

    pub(crate) fn commit(mut self) -> PathBuf {
        self.armed = false;
        std::mem::take(&mut self.path)
    }

    fn strip_circle_transport_state(&self) -> Result<(), SnapshotImageError> {
        let mut connection = Connection::open(&self.path).map_err(|error| {
            SnapshotImageError::Projection(format!(
                "open Circle snapshot transport projection: {error}"
            ))
        })?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        transaction
            .execute_batch(
                "DELETE FROM row_blob_locators;
                 DELETE FROM blob_locators;
                 DELETE FROM retained_replay_objects;
                 DELETE FROM remote_objects;
                 DELETE FROM retained_merge_materializations;",
            )
            .map_err(|error| {
                SnapshotImageError::Projection(format!(
                    "strip Circle snapshot transport state: {error}"
                ))
            })?;
        transaction
            .commit()
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        connection.execute_batch("VACUUM").map_err(|error| {
            SnapshotImageError::Projection(format!(
                "vacuum Circle snapshot transport projection: {error}"
            ))
        })?;
        connection.close().map_err(|(_, error)| {
            SnapshotImageError::Projection(format!(
                "close Circle snapshot transport projection: {error}"
            ))
        })
    }

    pub(crate) fn install_blob_graph(
        self,
        blobs: &[crate::database::PreparedSnapshotBlob],
    ) -> Result<Vec<u8>, SnapshotImageError> {
        let result = (|| {
            let mut connection = Connection::open(self.path()).map_err(|error| {
                SnapshotImageError::Projection(format!("open snapshot closure image: {error}"))
            })?;
            connection
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            let transaction = connection
                .transaction()
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            for blob in blobs {
                blob.remote
                    .validate()
                    .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
                if blob.bindings.is_empty()
                    || blob
                        .bindings
                        .iter()
                        .any(|binding| binding.blob().object() != blob.remote.object())
                {
                    return Err(SnapshotImageError::Projection(
                        "snapshot blob binding differs from its remote object".to_string(),
                    ));
                }
                crate::database::install_snapshot_blob_plan_on(&transaction, blob).map_err(
                    |error| {
                        SnapshotImageError::Projection(format!("install snapshot blob: {error}"))
                    },
                )?;
            }
            transaction
                .commit()
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            connection.execute_batch("VACUUM").map_err(|error| {
                SnapshotImageError::Projection(format!("vacuum snapshot closure: {error}"))
            })?;
            connection.close().map_err(|(_, error)| {
                SnapshotImageError::Projection(format!("close snapshot closure image: {error}"))
            })?;
            std::fs::read(self.path()).map_err(SnapshotImageError::Io)
        })();
        self.finish(result)
    }

    fn remove_files(&self) -> std::io::Result<()> {
        for candidate in [
            self.path.clone(),
            PathBuf::from(format!("{}-wal", self.path.display())),
            PathBuf::from(format!("{}-shm", self.path.display())),
        ] {
            match std::fs::remove_file(candidate) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl Drop for SnapshotDatabaseImage {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Err(error) = self.remove_files() {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "could not remove abandoned staged snapshot database"
            );
        }
    }
}

impl StoreDatabase {
    #[cfg(test)]
    pub(crate) async fn capture_snapshot_image_for_test(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: Option<crate::encryption::EncryptionService>,
    ) -> Result<Vec<u8>, DbError> {
        let tables = self.synced_tables().to_vec();
        self.connection
            .call(move |connection| {
                create_snapshot_for_audience(
                    connection,
                    &root,
                    &temp_dir,
                    &tables,
                    routing_encryption.as_ref(),
                    &crate::protocol::circle::Audience::Store,
                )
                .map(|snapshot| snapshot.db_image)
                .map_err(snapshot_image_db_error)
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn capture_circle_snapshot_image_for_test(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: crate::encryption::EncryptionService,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Vec<u8>, DbError> {
        let tables = self.synced_tables().to_vec();
        self.connection
            .call(move |connection| {
                create_snapshot_for_audience(
                    connection,
                    &root,
                    &temp_dir,
                    &tables,
                    Some(&routing_encryption),
                    &crate::protocol::circle::Audience::Circle(circle_id),
                )
                .map(|snapshot| snapshot.db_image)
                .map_err(snapshot_image_db_error)
            })
            .await
    }

    pub(crate) async fn capture_store_snapshot_cut(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        tables: Vec<SyncedTable>,
        routing_encryption: Option<crate::encryption::EncryptionService>,
    ) -> Result<
        (
            CreatedSnapshot,
            crate::protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.connection
            .call(move |connection| {
                require_no_unpublished_store_writes(connection)?;
                let snapshot = create_snapshot_for_audience(
                    connection,
                    &root,
                    &temp_dir,
                    &tables,
                    routing_encryption.as_ref(),
                    &crate::protocol::circle::Audience::Store,
                )
                .map_err(snapshot_image_db_error)?;
                let coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
                    Self::materialized_frontier_on(connection, None)?,
                )
                .map_err(|error| DbError::Message(format!("snapshot coverage: {error}")))?;
                Ok((snapshot, coverage))
            })
            .await
    }

    pub(crate) async fn capture_circle_snapshot_cut(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        tables: Vec<SyncedTable>,
        routing_encryption: crate::encryption::EncryptionService,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<
        (
            CreatedSnapshot,
            crate::protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.connection
            .call(move |connection| {
                require_no_unpublished_store_writes(connection)?;
                let snapshot = create_snapshot_for_audience(
                    connection,
                    &root,
                    &temp_dir,
                    &tables,
                    Some(&routing_encryption),
                    &crate::protocol::circle::Audience::Circle(circle_id),
                )
                .map_err(snapshot_image_db_error)?;
                let coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
                    Self::materialized_frontier_on(connection, None)?,
                )
                .map_err(|error| DbError::Message(format!("snapshot coverage: {error}")))?;
                Ok((snapshot, coverage))
            })
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn capture_circle_snapshot_at_cutoff(
        &self,
        root: crate::protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        tables: Vec<SyncedTable>,
        routing_encryption: crate::encryption::EncryptionService,
        routing_key: crate::protocol::circle::RowRoutingKey,
        circle_id: crate::protocol::circle::CircleId,
        cutoff: crate::protocol::store_commit::CommitFrontier,
    ) -> Result<CreatedSnapshot, DbError> {
        let blob_decls = self.blob_decls();
        let gates = self.gates();
        let retained = self.retained_merge_materialization_cache();
        self.connection
            .call(move |connection| {
                let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
                let mut retained = retained.lock().map_err(|_| {
                    DbError::Message(
                        "retained Merge materialization cache lock is poisoned".to_string(),
                    )
                })?;
                let replay = crate::database::replay_retained_merge_projection_on(
                    &transaction,
                    &root,
                    &mut retained,
                    &blob_decls,
                    &gates,
                    &tables,
                    Some(&routing_key),
                    &std::collections::BTreeSet::new(),
                    Some(&cutoff),
                    false,
                    crate::sync::LocalStoreMembership::Current,
                )?;
                transaction.rollback().map_err(DbError::from)?;
                let replay_frontier = crate::protocol::store_commit::CommitFrontier::from_refs(
                    Self::materialized_frontier_on(&replay, None)?,
                )
                .map_err(|error| DbError::Message(error.to_string()))?;
                if replay_frontier != cutoff {
                    return Err(DbError::Message(
                        "Circle close cutoff is not an exact retained Store frontier".to_string(),
                    ));
                }
                create_snapshot_for_audience(
                    &replay,
                    &root,
                    &temp_dir,
                    &tables,
                    Some(&routing_encryption),
                    &crate::protocol::circle::Audience::Circle(circle_id),
                )
                .map_err(snapshot_image_db_error)
            })
            .await
    }
}

fn snapshot_image_db_error(error: SnapshotImageError) -> DbError {
    DbError::Message(error.to_string())
}

fn require_no_unpublished_store_writes(connection: &Connection) -> Result<(), DbError> {
    let pending: i64 = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM store_writes
                WHERE status != '\"local_only\"'
                  AND json_extract(status, '$.published') IS NULL
            )",
            [],
            |row| row.get(0),
        )
        .map_err(DbError::from)?;
    if pending != 0 {
        return Err(DbError::Message(
            "snapshot cut refused while unpublished Store writes exist".to_string(),
        ));
    }
    Ok(())
}

fn create_snapshot_for_audience(
    connection: &Connection,
    root: &crate::protocol::store_commit::StoreRootRef,
    temp_dir: &Path,
    tables: &[SyncedTable],
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    audience: &crate::protocol::circle::Audience,
) -> Result<CreatedSnapshot, SnapshotImageError> {
    if tables.is_empty() {
        return Err(SnapshotImageError::NoSyncedTables);
    }

    let gates = crate::database::Gates::from_tables(connection, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let routing_key = if gates.has_scoped_graph() {
        let encryption = routing_encryption.ok_or_else(|| {
            SnapshotImageError::Projection(
                "scoped snapshot creation requires Store routing encryption".to_string(),
            )
        })?;
        Some(
            crate::protocol::circle::derive_row_routing_key(encryption, root.store_root_hash)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?,
        )
    } else {
        None
    };

    let snapshot_image = SnapshotDatabaseImage::prepare(temp_dir.join("snapshot.db"))?;
    let snapshot_path = snapshot_image.path();
    let path_str = snapshot_path
        .to_str()
        .expect("temp path should be valid UTF-8");
    if let Err(error) = connection.execute("VACUUM INTO ?1", [path_str]) {
        return snapshot_image.finish(Err(SnapshotImageError::VacuumFailed(error.to_string())));
    }
    if let Err(error) =
        project_snapshot_image(snapshot_path, root, tables, routing_key.as_ref(), audience)
    {
        return snapshot_image.finish(Err(error));
    }
    let blobs = match snapshot_blob_facts(connection, snapshot_path, temp_dir, tables) {
        Ok(blobs) => blobs,
        Err(error) => return snapshot_image.finish(Err(error)),
    };
    if matches!(audience, crate::protocol::circle::Audience::Circle(_)) {
        if let Err(error) = snapshot_image.strip_circle_transport_state() {
            return snapshot_image.finish(Err(error));
        }
    }

    let plaintext = snapshot_image.read_and_discard()?;
    info!(plaintext_size = plaintext.len(), "created snapshot");
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
) -> Result<Vec<SnapshotBlobFact>, SnapshotImageError> {
    let snapshot = Connection::open(snapshot_path).map_err(|error| {
        SnapshotImageError::Projection(format!("open scoped snapshot: {error}"))
    })?;
    let declarations = crate::database::BlobDecls::from_tables(&snapshot, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let publications = declarations
        .publication_blobs_in_db(&snapshot)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let gates = crate::database::Gates::from_tables(live, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let mut facts = Vec::with_capacity(publications.len());
    for publication in publications {
        let plaintext_hash = publication.plaintext_hash.parse().map_err(|error| {
            SnapshotImageError::Projection(format!(
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
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?
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
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let audience = match crate::database::live_row_audience(
            live,
            &gates,
            &publication.table,
            &publication.row_id,
        )
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?
        {
            crate::protocol::circle::Audience::Store => SnapshotBlobAudience::Store,
            crate::protocol::circle::Audience::Circle(circle_id) => SnapshotBlobAudience::Circle {
                circle_id,
                control: crate::database::active_circle_control(live, circle_id)
                    .map_err(|error| SnapshotImageError::Projection(error.to_string()))?,
            },
            crate::protocol::circle::Audience::Local => {
                return Err(SnapshotImageError::Projection(format!(
                    "scoped snapshot retains local blob row {:?}/{:?}",
                    publication.table, publication.row_id
                )));
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

const CIRCLE_IMAGE_PRESERVED_NON_SYNCED_TABLES: &[&str] = &[
    "_coven_audience",
    "_coven_row_routes",
    "remote_objects",
    "blob_locators",
    "row_blob_locators",
    "retained_merge_materializations",
    "retained_replay_objects",
];

fn project_snapshot_image(
    path: &Path,
    root: &crate::protocol::store_commit::StoreRootRef,
    synced: &[SyncedTable],
    routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    audience: &crate::protocol::circle::Audience,
) -> Result<(), SnapshotImageError> {
    let connection = Connection::open(path).map_err(|error| {
        SnapshotImageError::Projection(format!("failed to open snapshot copy: {error}"))
    })?;
    let result = project_snapshot_connection(&connection, root, synced, routing_key, audience);
    match (result, connection.close()) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err((_, error))) => Err(SnapshotImageError::Projection(format!(
            "failed to close snapshot copy: {error}"
        ))),
    }
}

fn project_snapshot_connection(
    connection: &Connection,
    root: &crate::protocol::store_commit::StoreRootRef,
    synced: &[SyncedTable],
    routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    audience: &crate::protocol::circle::Audience,
) -> Result<(), SnapshotImageError> {
    let gates = crate::database::Gates::from_tables(connection, synced)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if gates.has_scoped_graph() && routing_key.is_none() {
        return Err(SnapshotImageError::Projection(
            "scoped snapshot projection requires a row-routing key".to_string(),
        ));
    }
    let transaction = connection
        .unchecked_transaction()
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    transaction
        .pragma_update(None, "defer_foreign_keys", "ON")
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let coverage = StoreDatabase::materialized_frontier_on(&transaction, None)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let cleared_materialization_tables = ["materialized_commits"];
    for table in cleared_materialization_tables {
        transaction
            .execute_batch(&format!(
                "DELETE FROM {}",
                crate::database::quote_ident(table)
            ))
            .map_err(|error| SnapshotImageError::Projection(format!("clear {table}: {error}")))?;
    }
    if matches!(audience, crate::protocol::circle::Audience::Store) {
        StoreDatabase::retain_snapshot_replay_inputs_on(&transaction, root)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        StoreDatabase::retain_snapshot_device_states_on(&transaction, root, coverage)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    }
    let preserved_non_synced_tables = match audience {
        crate::protocol::circle::Audience::Store => SNAPSHOT_PRESERVED_NON_SYNCED_TABLES,
        crate::protocol::circle::Audience::Circle(_) => CIRCLE_IMAGE_PRESERVED_NON_SYNCED_TABLES,
        crate::protocol::circle::Audience::Local => {
            return Err(SnapshotImageError::Projection(
                "Local rows cannot enter a snapshot".to_string(),
            ));
        }
    };
    for table in crate::database::user_table_names(connection)
        .map_err(|error| SnapshotImageError::Projection(format!("list user tables: {error}")))?
    {
        if synced.iter().any(|synced| synced.name() == table)
            || preserved_non_synced_tables.contains(&table.as_str())
            || cleared_materialization_tables.contains(&table.as_str())
        {
            continue;
        }
        transaction
            .execute_batch(&format!(
                "DELETE FROM {}",
                crate::database::quote_ident(&table)
            ))
            .map_err(|error| SnapshotImageError::Projection(format!("clear {table}: {error}")))?;
    }

    match audience {
        crate::protocol::circle::Audience::Store => gates
            .delete_gated_false(&transaction)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?,
        crate::protocol::circle::Audience::Circle(_) => {
            crate::database::retain_snapshot_audience_rows(&transaction, &gates, audience)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        }
        crate::protocol::circle::Audience::Local => {
            return Err(SnapshotImageError::Projection(
                "Local rows cannot enter a snapshot".to_string(),
            ));
        }
    }
    if let Some(routing_key) = routing_key {
        crate::database::prune_private_routes_without_rows(&transaction, &gates)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        crate::database::validate_snapshot_routing_state(
            &transaction,
            &gates,
            routing_key,
            audience,
        )
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    }

    scope_authenticated_blob_graph(&transaction, synced)?;
    transaction
        .commit()
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if matches!(audience, crate::protocol::circle::Audience::Store) {
        connection
            .execute_batch("VACUUM")
            .map_err(|error| SnapshotImageError::Projection(format!("vacuum: {error}")))?;
    }
    Ok(())
}

fn scope_authenticated_blob_graph(
    connection: &Connection,
    synced: &[SyncedTable],
) -> Result<(), SnapshotImageError> {
    connection
        .execute_batch(
            "CREATE TEMP TABLE snapshot_live_blob_bindings (
                 table_name TEXT NOT NULL,
                 row_id TEXT NOT NULL,
                 column_name TEXT NOT NULL,
                 row_stamp TEXT NOT NULL,
                 PRIMARY KEY (table_name, row_id, column_name, row_stamp)
             ) STRICT;",
        )
        .map_err(|error| SnapshotImageError::Projection(format!("create blob scope: {error}")))?;
    for table in synced {
        let Some(declaration) = table.blob() else {
            continue;
        };
        connection
            .execute(
                &format!(
                    "INSERT INTO snapshot_live_blob_bindings
                     (table_name, row_id, column_name, row_stamp)
                     SELECT ?1, id, ?2, _updated_at FROM {}
                     WHERE {} IS NOT NULL",
                    crate::database::quote_ident(table.name()),
                    crate::database::quote_ident(&declaration.id_column),
                ),
                rusqlite::params![table.name(), &declaration.id_column],
            )
            .map_err(|error| {
                SnapshotImageError::Projection(format!(
                    "collect live blob bindings for {:?}: {error}",
                    table.name()
                ))
            })?;
    }
    connection
        .execute_batch(
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
        .map_err(|error| {
            SnapshotImageError::Projection(format!("scope blob ownership graph: {error}"))
        })?;
    Ok(())
}

pub(crate) fn verify_circle_bootstrap_image(
    image: &[u8],
    reference: &crate::protocol::circle::CircleBootstrapRef,
    circle_id: crate::protocol::circle::CircleId,
    tables: &[SyncedTable],
    routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
) -> Result<(), SnapshotImageError> {
    if crate::protocol::store_commit::ObjectHash::digest(image) != reference.image.image_hash {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap image differs from its signed hash".to_string(),
        ));
    }
    let connection = crate::database::open_database_image(image)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let schema_version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if schema_version != reference.schema_version {
        return Err(SnapshotImageError::Projection(format!(
            "Circle bootstrap schema is {schema_version}, expected {}",
            reference.schema_version
        )));
    }
    let routing_contract =
        crate::database::SyncRoutingContract::from_connection(&connection, tables)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if routing_contract.hash() != reference.sync_routing_hash {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap routing contract differs from its signed hash".to_string(),
        ));
    }
    let gates = crate::database::Gates::from_tables(&connection, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if gates.has_scoped_graph() {
        let routing_key = routing_key.ok_or_else(|| {
            SnapshotImageError::Projection(
                "scoped Circle bootstrap verification requires Store routing authentication"
                    .to_string(),
            )
        })?;
        crate::database::validate_snapshot_routing_state(
            &connection,
            &gates,
            routing_key,
            &crate::protocol::circle::Audience::Circle(circle_id),
        )
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    }
    let declarations = crate::database::BlobDecls::from_tables(&connection, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let rows = declarations
        .publication_blobs_in_db(&connection)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if rows.len() != reference.blobs.len() {
        return Err(SnapshotImageError::Projection(
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
            SnapshotImageError::Projection(
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
                    crate::protocol::audience_package::PackageAudience::Circle {
                        circle_id: binding_circle,
                        ..
                    }
                ) if *binding_circle == circle_id
            )
            || binding.stored().is_none()
        {
            return Err(SnapshotImageError::Projection(
                "Circle bootstrap blob closure differs from an exact image row".to_string(),
            ));
        }
    }
    for table in crate::database::user_table_names(&connection)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?
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
                    crate::database::quote_ident(&table)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        if count != 0 {
            return Err(SnapshotImageError::Projection(format!(
                "Circle bootstrap retains non-projection table {table:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_database_cleanup_reports_a_target_that_remains() {
        let directory = tempfile::tempdir().expect("snapshot cleanup directory");
        let path = directory.path().join("snapshot.db");
        std::fs::create_dir(&path).expect("create unremovable snapshot target");

        let error = SnapshotDatabaseImage::prepare(path.clone())
            .expect_err("an unremovable staged database must fail");

        assert!(
            matches!(
                error,
                SnapshotImageError::Cleanup {
                    path: ref failed_path,
                    ..
                } if *failed_path == path
            ),
            "{error}"
        );
        std::fs::remove_dir(path).expect("remove cleanup obstruction");
    }

    #[test]
    fn staged_database_cleanup_preserves_the_operation_failure() {
        let directory = tempfile::tempdir().expect("snapshot cleanup directory");
        let path = directory.path().join("snapshot.db");
        let staged =
            SnapshotDatabaseImage::prepare(path.clone()).expect("prepare staged database image");
        std::fs::create_dir(&path).expect("create cleanup obstruction");

        let error = staged
            .finish::<()>(Err(SnapshotImageError::VacuumFailed(
                "injected operation failure".to_string(),
            )))
            .expect_err("operation and cleanup failures must both surface");

        assert!(
            matches!(
                error,
                SnapshotImageError::CleanupAfterFailure {
                    path: ref failed_path,
                    ref cause,
                    ..
                } if *failed_path == path
                    && matches!(
                        cause.as_ref(),
                        SnapshotImageError::VacuumFailed(message)
                            if message == "injected operation failure"
                    )
            ),
            "{error}"
        );
        std::fs::remove_dir(path).expect("remove cleanup obstruction");
    }

    #[test]
    fn staged_database_creation_refuses_an_existing_target() {
        let directory = tempfile::tempdir().expect("snapshot creation directory");
        let path = directory.path().join("snapshot.db");
        std::fs::write(&path, b"existing database").expect("write existing database");

        let result = SnapshotDatabaseImage::create(path.clone(), b"replacement database");

        assert!(result.is_err(), "creation must refuse an existing database");
        assert_eq!(
            std::fs::read(path).expect("read preserved database"),
            b"existing database"
        );
    }
}
