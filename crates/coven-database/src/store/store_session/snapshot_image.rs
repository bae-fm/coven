use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use tracing::info;

use crate::*;
use coven_protocol::synced_schema::SyncedTable;

use super::*;

pub struct CreatedSnapshot {
    pub db_image: SnapshotDatabaseImage,
    pub blobs: Vec<SnapshotBlobFact>,
}

#[derive(Debug, Clone)]
pub struct SnapshotBlobFact {
    pub fact: crate::StoreWriteBlobFact,
    pub audience: SnapshotBlobAudience,
}

#[derive(Debug, Clone)]
pub enum SnapshotBlobAudience {
    Store,
    Circle {
        circle_id: coven_protocol::circle::CircleId,
        control: crate::CirclePartitionControl,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotImageError {
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

#[derive(Debug)]
pub enum SnapshotImageOperationError<E> {
    Operation(E),
    Cleanup {
        path: PathBuf,
        cleanup: String,
    },
    CleanupAfterFailure {
        path: PathBuf,
        cleanup: String,
        cause: E,
    },
}

/// One uncommitted SQLite image and its sidecar files.
///
/// The path remains armed until the operation commits or consumes the image.
/// Failures report cleanup failure instead of leaving a plaintext image behind.
#[derive(Debug)]
pub struct SnapshotDatabaseImage {
    path: PathBuf,
    armed: bool,
}

impl SnapshotDatabaseImage {
    pub fn prepare(path: PathBuf) -> Result<Self, SnapshotImageError> {
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

    pub fn create(path: PathBuf, plaintext: &[u8]) -> Result<Self, SnapshotImageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self { path, armed: false }.write_new(plaintext)
    }

    pub fn replace(path: PathBuf, plaintext: &[u8]) -> Result<Self, SnapshotImageError> {
        Self::prepare(path)?.write_new(plaintext)
    }

    fn prepare_snapshot(temp_dir: &Path) -> Result<Self, SnapshotImageError> {
        Self::prepare(temp_dir.join("snapshot.db"))
    }

    pub(super) fn capture_on(
        self,
        connection: &rusqlite::Connection,
        store_dir: &coven_foundation::store_dir::StoreDir,
        root: &coven_protocol::store_commit::StoreRootRef,
        tables: &[SyncedTable],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: &coven_protocol::circle::Audience,
    ) -> Result<CreatedSnapshot, SnapshotImageError> {
        if tables.is_empty() {
            return self.finish(Err(SnapshotImageError::NoSyncedTables));
        }
        let gates = match crate::Gates::from_tables(connection, tables) {
            Ok(gates) => gates,
            Err(error) => {
                return self.finish(Err(SnapshotImageError::Projection(error.to_string())));
            }
        };
        let routing_key = if gates.has_scoped_graph() {
            let encryption = match routing_encryption {
                Some(encryption) => encryption,
                None => {
                    return self.finish(Err(SnapshotImageError::Projection(
                        "scoped snapshot creation requires Store routing encryption".to_string(),
                    )));
                }
            };
            match coven_protocol::circle::derive_row_routing_key(encryption, root.store_root_hash) {
                Ok(routing_key) => Some(routing_key),
                Err(error) => {
                    return self.finish(Err(SnapshotImageError::Projection(error.to_string())));
                }
            }
        } else {
            None
        };

        let source_image = match crate::connection_io::serialize_database_image(connection) {
            Ok(image) => image,
            Err(error) => {
                return self.finish(Err(SnapshotImageError::Projection(error.to_string())));
            }
        };
        let mut snapshot = match Connection::open_in_memory().map_err(DbError::from) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.finish(Err(SnapshotImageError::Projection(error.to_string())));
            }
        };
        if let Err(error) =
            crate::connection_io::deserialize_database_image_into(&mut snapshot, &source_image)
        {
            return self.finish(Err(SnapshotImageError::Projection(error.to_string())));
        }
        if let Err(error) = Self::project(
            &mut snapshot,
            store_dir,
            root,
            tables,
            routing_key.as_ref(),
            audience,
        ) {
            return self.finish(Err(error));
        }
        let blobs = match Self::blob_facts(connection, &snapshot, tables) {
            Ok(blobs) => blobs,
            Err(error) => return self.finish(Err(error)),
        };
        if matches!(audience, coven_protocol::circle::Audience::Circle(_)) {
            if let Err(error) = Self::strip_circle_transport_state(&mut snapshot) {
                return self.finish(Err(error));
            }
        }

        let image = match crate::connection_io::serialize_database_image(&snapshot) {
            Ok(image) => image,
            Err(error) => {
                return self.finish(Err(SnapshotImageError::Projection(error.to_string())));
            }
        };
        drop(snapshot);
        let snapshot = self.write_new(&image)?;

        let plaintext_size = match std::fs::metadata(snapshot.path()) {
            Ok(metadata) => metadata.len(),
            Err(error) => return snapshot.finish(Err(SnapshotImageError::Io(error))),
        };
        info!(plaintext_size, "created snapshot");
        Ok(CreatedSnapshot {
            db_image: snapshot,
            blobs,
        })
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn read(&self) -> Result<Vec<u8>, SnapshotImageError> {
        tokio::fs::read(&self.path).await.map_err(|error| {
            SnapshotImageError::Projection(format!(
                "read staged snapshot database {}: {error}",
                self.path.display()
            ))
        })
    }

    pub fn read_and_discard(self) -> Result<Vec<u8>, SnapshotImageError> {
        let outcome = std::fs::read(&self.path).map_err(SnapshotImageError::Io);
        self.finish(outcome)
    }

    pub fn canonicalize(mut self) -> Result<Self, SnapshotImageError> {
        match std::fs::canonicalize(&self.path) {
            Ok(path) => {
                self.path = path;
                Ok(self)
            }
            Err(error) => self.finish(Err(SnapshotImageError::Io(error))),
        }
    }

    pub fn finish<T>(
        self,
        outcome: Result<T, SnapshotImageError>,
    ) -> Result<T, SnapshotImageError> {
        match self.finish_operation(outcome) {
            Ok(value) => Ok(value),
            Err(SnapshotImageOperationError::Operation(cause)) => Err(cause),
            Err(SnapshotImageOperationError::Cleanup { path, cleanup }) => {
                Err(SnapshotImageError::Cleanup { path, cleanup })
            }
            Err(SnapshotImageOperationError::CleanupAfterFailure {
                path,
                cleanup,
                cause,
            }) => Err(SnapshotImageError::CleanupAfterFailure {
                path,
                cleanup,
                cause: Box::new(cause),
            }),
        }
    }

    pub fn finish_operation<T, E>(
        mut self,
        outcome: Result<T, E>,
    ) -> Result<T, SnapshotImageOperationError<E>> {
        let cleanup = self.remove_files();
        self.armed = false;
        match (outcome, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(cause), Ok(())) => Err(SnapshotImageOperationError::Operation(cause)),
            (Ok(_), Err(cleanup)) => Err(SnapshotImageOperationError::Cleanup {
                path: self.path.clone(),
                cleanup: cleanup.to_string(),
            }),
            (Err(cause), Err(cleanup)) => Err(SnapshotImageOperationError::CleanupAfterFailure {
                path: self.path.clone(),
                cleanup: cleanup.to_string(),
                cause,
            }),
        }
    }

    pub fn commit(mut self) -> PathBuf {
        self.armed = false;
        std::mem::take(&mut self.path)
    }

    fn project(
        connection: &mut Connection,
        store_dir: &coven_foundation::store_dir::StoreDir,
        root: &coven_protocol::store_commit::StoreRootRef,
        synced: &[SyncedTable],
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        audience: &coven_protocol::circle::Audience,
    ) -> Result<(), SnapshotImageError> {
        let gates = crate::Gates::from_tables(connection, synced)
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
        let coverage =
            crate::store::materialized_commit_index::materialized_frontier_on(&transaction, None)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let cleared_materialization_tables = ["materialized_commits"];
        for table in cleared_materialization_tables {
            transaction
                .execute_batch(&format!("DELETE FROM {}", crate::quote_ident(table)))
                .map_err(|error| {
                    SnapshotImageError::Projection(format!("clear {table}: {error}"))
                })?;
        }
        if matches!(audience, coven_protocol::circle::Audience::Store) {
            let records =
                crate::store::store_session::StoreTransaction::new(&transaction, store_dir);
            let mut authority = super::VerifiedStoreAuthority::default();
            records
                .retain_snapshot_replay_inputs(&mut authority, root)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            records
                .retain_snapshot_device_states(&mut authority, root, coverage)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        }
        let preserved_non_synced_tables = match audience {
            coven_protocol::circle::Audience::Store => SNAPSHOT_PRESERVED_NON_SYNCED_TABLES,
            coven_protocol::circle::Audience::Circle(_) => CIRCLE_IMAGE_PRESERVED_NON_SYNCED_TABLES,
            coven_protocol::circle::Audience::Local => {
                return Err(SnapshotImageError::Projection(
                    "Local rows cannot enter a snapshot".to_string(),
                ));
            }
        };
        for table in crate::user_table_names(connection)
            .map_err(|error| SnapshotImageError::Projection(format!("list user tables: {error}")))?
        {
            if synced.iter().any(|synced| synced.name() == table)
                || preserved_non_synced_tables.contains(&table.as_str())
                || cleared_materialization_tables.contains(&table.as_str())
            {
                continue;
            }
            transaction
                .execute_batch(&format!("DELETE FROM {}", crate::quote_ident(&table)))
                .map_err(|error| {
                    SnapshotImageError::Projection(format!("clear {table}: {error}"))
                })?;
        }

        match audience {
            coven_protocol::circle::Audience::Store => gates
                .delete_gated_false(&transaction)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?,
            coven_protocol::circle::Audience::Circle(_) => {
                crate::retain_snapshot_audience_rows(&transaction, &gates, audience)
                    .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            }
            coven_protocol::circle::Audience::Local => {
                return Err(SnapshotImageError::Projection(
                    "Local rows cannot enter a snapshot".to_string(),
                ));
            }
        }
        if let Some(routing_key) = routing_key {
            crate::prune_private_routes_without_rows(&transaction, &gates)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            crate::validate_snapshot_routing_state(&transaction, &gates, routing_key, audience)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        }

        scope_authenticated_blob_graph(&transaction, synced)?;
        transaction
            .commit()
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        if matches!(audience, coven_protocol::circle::Audience::Store) {
            connection
                .execute_batch("VACUUM")
                .map_err(|error| SnapshotImageError::Projection(format!("vacuum: {error}")))?;
        }
        Ok(())
    }

    fn strip_circle_transport_state(connection: &mut Connection) -> Result<(), SnapshotImageError> {
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let transaction = connection
            .transaction()
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        // These rows are the copy's, describing the spool of the device that
        // built it, so they are deleted without releasing any payload claim.
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
        Ok(())
    }

    pub fn install_blob_graph(
        self,
        blobs: &[crate::PreparedSnapshotBlob],
    ) -> Result<Self, SnapshotImageError> {
        let result = (|| {
            let source = std::fs::read(self.path()).map_err(SnapshotImageError::Io)?;
            let mut connection = Connection::open_in_memory()
                .map_err(DbError::from)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            crate::connection_io::deserialize_database_image_into(&mut connection, &source)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
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
                crate::install_snapshot_blob_plan_on(&transaction, blob).map_err(|error| {
                    SnapshotImageError::Projection(format!("install snapshot blob: {error}"))
                })?;
            }
            transaction
                .commit()
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            connection.execute_batch("VACUUM").map_err(|error| {
                SnapshotImageError::Projection(format!("vacuum snapshot closure: {error}"))
            })?;
            let image = crate::connection_io::serialize_database_image(&connection)
                .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            connection.close().map_err(|(_, error)| {
                SnapshotImageError::Projection(format!("close snapshot closure image: {error}"))
            })?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(self.path())
                .map_err(SnapshotImageError::Io)?;
            std::io::Write::write_all(&mut file, &image).map_err(SnapshotImageError::Io)?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(self),
            Err(error) => self.finish(Err(error)),
        }
    }

    fn blob_facts(
        live: &Connection,
        snapshot: &Connection,
        tables: &[SyncedTable],
    ) -> Result<Vec<SnapshotBlobFact>, SnapshotImageError> {
        let declarations = crate::BlobDecls::from_tables(snapshot, tables)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let publications = declarations
            .publication_blobs_in_db(snapshot)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let gates = crate::Gates::from_tables(live, tables)
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
        let mut facts = Vec::with_capacity(publications.len());
        for publication in publications {
            let plaintext_hash = publication.plaintext_hash.parse().map_err(|error| {
                SnapshotImageError::Projection(format!(
                    "snapshot blob {}/{} plaintext hash: {error}",
                    publication.blob.namespace, publication.blob.id
                ))
            })?;
            let external_path =
                if publication.blob.provenance == coven_protocol::blob::Provenance::UserProvided {
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
            let previous = crate::previous_row_blob_for_write_on(
                snapshot,
                &publication.table,
                &publication.row_id,
                &publication.row_stamp,
                &publication.column,
                &publication.blob,
                publication.plaintext_size,
                plaintext_hash,
            )
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
            let audience = match crate::live_row_audience(
                live,
                &gates,
                &publication.table,
                &publication.row_id,
            )
            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?
            {
                coven_protocol::circle::Audience::Store => SnapshotBlobAudience::Store,
                coven_protocol::circle::Audience::Circle(circle_id) => {
                    SnapshotBlobAudience::Circle {
                        circle_id,
                        control: crate::active_circle_control(live, circle_id)
                            .map_err(|error| SnapshotImageError::Projection(error.to_string()))?,
                    }
                }
                coven_protocol::circle::Audience::Local => {
                    return Err(SnapshotImageError::Projection(format!(
                        "scoped snapshot retains local blob row {:?}/{:?}",
                        publication.table, publication.row_id
                    )));
                }
            };
            facts.push(SnapshotBlobFact {
                fact: crate::StoreWriteBlobFact {
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
            });
        }
        Ok(facts)
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

impl StoreSession<'_> {
    fn capture_snapshot_cut(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        temp_dir: &Path,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: coven_protocol::circle::Audience,
    ) -> Result<
        (
            CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        let records = self.records;
        require_no_unpublished_store_writes(self.records.conn)?;
        let snapshot = SnapshotDatabaseImage::prepare_snapshot(temp_dir)
            .and_then(|image| {
                records.capture_snapshot(
                    image,
                    root,
                    self.synced_tables,
                    routing_encryption,
                    &audience,
                )
            })
            .map_err(snapshot_image_db_error)?;
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            crate::store::materialized_commit_index::materialized_frontier_on(
                self.records.conn,
                None,
            )?,
        )
        .map_err(|error| DbError::context("snapshot coverage", error))?;
        Ok((snapshot, coverage))
    }

    #[allow(clippy::too_many_arguments)]
    fn capture_circle_snapshot_at_cutoff(
        &mut self,
        root: &coven_protocol::store_commit::StoreRootRef,
        temp_dir: &Path,
        routing_encryption: &coven_keys::encryption::EncryptionService,
        routing_key: &coven_protocol::circle::RowRoutingKey,
        circle_id: coven_protocol::circle::CircleId,
        cutoff: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<CreatedSnapshot, DbError> {
        let transaction = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
        let replay = crate::store::store_session::StoreTransaction::new(
            &transaction,
            self.records.store_dir,
        )
        .replay_projection_with_authority(
            self.verified_store_authority,
            root,
            self.blob_decls,
            self.gates,
            self.synced_tables,
            Some(routing_key),
            &std::collections::BTreeSet::new(),
            Some(cutoff),
            false,
            coven_protocol::membership::LocalStoreMembership::Current,
        )?;
        transaction.rollback().map_err(DbError::from)?;
        let replay_frontier = replay.materialized_frontier()?;
        if replay_frontier != *cutoff {
            return Err(DbError::Message(
                "Circle close cutoff is not an exact retained Store frontier".to_string(),
            ));
        }
        SnapshotDatabaseImage::prepare_snapshot(temp_dir)
            .and_then(|image| {
                replay.capture_snapshot(
                    image,
                    root,
                    self.synced_tables,
                    Some(routing_encryption),
                    &coven_protocol::circle::Audience::Circle(circle_id),
                )
            })
            .map_err(snapshot_image_db_error)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn capture_snapshot_image_for_test(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        temp_dir: &Path,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: coven_protocol::circle::Audience,
    ) -> Result<Vec<u8>, DbError> {
        SnapshotDatabaseImage::prepare_snapshot(temp_dir)
            .and_then(|image| {
                self.records.capture_snapshot(
                    image,
                    root,
                    self.synced_tables,
                    routing_encryption,
                    &audience,
                )
            })
            .and_then(|snapshot| snapshot.db_image.read_and_discard())
            .map_err(snapshot_image_db_error)
    }
}

impl StoreDatabase {
    pub async fn capture_store_snapshot_cut(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    ) -> Result<
        (
            CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.call_store(move |session| {
            session.capture_snapshot_cut(
                &root,
                &temp_dir,
                routing_encryption.as_ref(),
                coven_protocol::circle::Audience::Store,
            )
        })
        .await
    }

    pub async fn capture_circle_snapshot_cut(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: coven_keys::encryption::EncryptionService,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<
        (
            CreatedSnapshot,
            coven_protocol::store_commit::CommitFrontier,
        ),
        DbError,
    > {
        self.call_store(move |session| {
            session.capture_snapshot_cut(
                &root,
                &temp_dir,
                Some(&routing_encryption),
                coven_protocol::circle::Audience::Circle(circle_id),
            )
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn capture_circle_snapshot_at_cutoff(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: coven_keys::encryption::EncryptionService,
        routing_key: coven_protocol::circle::RowRoutingKey,
        circle_id: coven_protocol::circle::CircleId,
        cutoff: coven_protocol::store_commit::CommitFrontier,
    ) -> Result<CreatedSnapshot, DbError> {
        self.call_store(move |session| {
            session.capture_circle_snapshot_at_cutoff(
                &root,
                &temp_dir,
                &routing_encryption,
                &routing_key,
                circle_id,
                &cutoff,
            )
        })
        .await
    }

    pub async fn verify_circle_bootstrap_image(
        &self,
        image: Vec<u8>,
        reference: coven_protocol::circle::CircleBootstrapRef,
        circle_id: coven_protocol::circle::CircleId,
        routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    ) -> Result<Vec<u8>, SnapshotImageError> {
        self.call_store(move |session| {
            let verification = verify_circle_bootstrap_image(
                &image,
                &reference,
                circle_id,
                session.synced_tables,
                routing_key.as_ref(),
            );
            Ok(verification.map(|()| image))
        })
        .await
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn capture_snapshot_image_for_test(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    ) -> Result<Vec<u8>, DbError> {
        self.call_store(move |session| {
            session.capture_snapshot_image_for_test(
                &root,
                &temp_dir,
                routing_encryption.as_ref(),
                coven_protocol::circle::Audience::Store,
            )
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn capture_circle_snapshot_image_for_test(
        &self,
        root: coven_protocol::store_commit::StoreRootRef,
        temp_dir: PathBuf,
        routing_encryption: coven_keys::encryption::EncryptionService,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Vec<u8>, DbError> {
        self.call_store(move |session| {
            session.capture_snapshot_image_for_test(
                &root,
                &temp_dir,
                Some(&routing_encryption),
                coven_protocol::circle::Audience::Circle(circle_id),
            )
        })
        .await
    }
}

pub(super) fn snapshot_image_db_error(error: SnapshotImageError) -> DbError {
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
                    crate::quote_ident(table.name()),
                    crate::quote_ident(&declaration.id_column),
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
    // As above: the projection prunes the copy's rows, never this device's
    // payload claims.
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

pub(super) fn verify_circle_bootstrap_image(
    image: &[u8],
    reference: &coven_protocol::circle::CircleBootstrapRef,
    circle_id: coven_protocol::circle::CircleId,
    tables: &[SyncedTable],
    routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
) -> Result<(), SnapshotImageError> {
    if coven_protocol::store_commit::ObjectHash::digest(image) != reference.image.image_hash {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap image differs from its signed hash".to_string(),
        ));
    }
    let mut connection = Connection::open_in_memory()
        .map_err(DbError::from)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    crate::connection_io::deserialize_database_image_into(&mut connection, image)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    verify_circle_bootstrap_connection(&connection, reference, circle_id, tables, routing_key)
}

pub(crate) fn verify_circle_bootstrap_connection(
    connection: &Connection,
    reference: &coven_protocol::circle::CircleBootstrapRef,
    circle_id: coven_protocol::circle::CircleId,
    tables: &[SyncedTable],
    routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
) -> Result<(), SnapshotImageError> {
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
    let routing_contract = crate::SyncRoutingContract::from_connection(connection, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if routing_contract.hash() != reference.sync_routing_hash {
        return Err(SnapshotImageError::Projection(
            "Circle bootstrap routing contract differs from its signed hash".to_string(),
        ));
    }
    let gates = crate::Gates::from_tables(connection, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    if gates.has_scoped_graph() {
        let routing_key = routing_key.ok_or_else(|| {
            SnapshotImageError::Projection(
                "scoped Circle bootstrap verification requires Store routing authentication"
                    .to_string(),
            )
        })?;
        crate::validate_snapshot_routing_state(
            connection,
            &gates,
            routing_key,
            &coven_protocol::circle::Audience::Circle(circle_id),
        )
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    }
    let declarations = crate::BlobDecls::from_tables(connection, tables)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?;
    let rows = declarations
        .publication_blobs_in_db(connection)
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
                coven_protocol::blob::RowBlobAuthority::Remote(
                    coven_protocol::audience_package::PackageAudience::Circle {
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
    for table in crate::user_table_names(connection)
        .map_err(|error| SnapshotImageError::Projection(error.to_string()))?
    {
        if tables.iter().any(|synced| synced.name() == table)
            || matches!(table.as_str(), "_coven_audience" | "_coven_row_routes")
        {
            continue;
        }
        let count: i64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM {}", crate::quote_ident(&table)),
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
#[path = "snapshot_image_tests.rs"]
mod tests;
