use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tracing::info;

use crate::*;
use coven_protocol::synced_schema::SyncedTable;

use super::*;

pub struct CreatedSnapshot {
    db_image: SnapshotDatabaseImage,
    blobs: Vec<RowBlobRef>,
}

impl CreatedSnapshot {
    pub fn new(db_image: SnapshotDatabaseImage, blobs: Vec<RowBlobRef>) -> Self {
        Self { db_image, blobs }
    }

    pub fn blobs(&self) -> &[RowBlobRef] {
        &self.blobs
    }

    pub async fn read_image(&self) -> Result<Vec<u8>, SnapshotImageError> {
        self.db_image.read().await
    }

    pub fn into_parts(self) -> (SnapshotDatabaseImage, Vec<RowBlobRef>) {
        (self.db_image, self.blobs)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn image_path_for_test(&self) -> &Path {
        self.db_image.path()
    }
}

/// One captured Circle bootstrap: the Circle's whole projection as a row
/// changeset, and the blob closure those rows bind.
pub struct CreatedCircleSnapshot {
    rows: Vec<u8>,
    blobs: Vec<RowBlobRef>,
}

impl CreatedCircleSnapshot {
    pub fn new(rows: Vec<u8>, blobs: Vec<RowBlobRef>) -> Self {
        Self { rows, blobs }
    }

    pub fn blobs(&self) -> &[RowBlobRef] {
        &self.blobs
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn into_parts(self) -> (Vec<u8>, Vec<RowBlobRef>) {
        (self.rows, self.blobs)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotImageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("no synced tables registered; refusing to emit an all-cleared snapshot")]
    NoSyncedTables,
    #[error("failed to scope snapshot down to shareable data: {0}")]
    Projection(String),
    #[error("snapshot database: {0}")]
    Database(#[source] Box<DbError>),
    #[error("snapshot gate: {0}")]
    Gate(#[from] crate::GateError),
    #[error("snapshot row routing key: {0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    #[error("snapshot SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("snapshot remote object: {0}")]
    RemoteObject(#[from] coven_protocol::remote_object::RemoteObjectRecordError),
    #[error("snapshot blob declarations: {0}")]
    BlobDecl(#[from] crate::BlobDeclError),
    #[error("snapshot routing contract: {0}")]
    RoutingContract(#[from] crate::SyncRoutingContractError),
    #[error("snapshot projection {operation}: {source}")]
    ProjectionSqlite {
        operation: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("snapshot projection {operation}: {source}")]
    ProjectionDatabase {
        operation: String,
        #[source]
        source: Box<DbError>,
    },
    #[error("snapshot projection {operation}: {source}")]
    ProjectionIo {
        operation: String,
        #[source]
        source: std::io::Error,
    },
    #[error("snapshot projection {operation}: {source}")]
    ProjectionPayloadStore {
        operation: String,
        #[source]
        source: crate::PayloadStoreError,
    },
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

impl From<DbError> for SnapshotImageError {
    fn from(error: DbError) -> Self {
        Self::Database(Box::new(error))
    }
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

    pub(super) fn prepare_snapshot(temp_dir: &Path) -> Result<Self, SnapshotImageError> {
        Self::prepare(temp_dir.join("snapshot.db"))
    }

    pub(super) fn capture_on(
        self,
        mut snapshot: Connection,
        store_dir: &coven_foundation::store_dir::StoreDir,
        mut authority: VerifiedStoreAuthority,
        root: &coven_protocol::store_commit::StoreRootRef,
        tables: &[SyncedTable],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<CreatedSnapshot, SnapshotImageError> {
        let blobs = match project_shared_snapshot(
            &mut snapshot,
            store_dir,
            &mut authority,
            root,
            tables,
            routing_encryption,
            &coven_protocol::circle::Audience::Store,
        ) {
            Ok(blobs) => blobs,
            Err(error) => return self.finish(Err(error)),
        };

        let image = match crate::connection_io::serialize_database_image(&snapshot) {
            Ok(image) => image,
            Err(error) => {
                return self.finish(Err(SnapshotImageError::from(error)));
            }
        };
        drop(snapshot);
        let snapshot = self.write_new(&image)?;

        let plaintext_size = match std::fs::metadata(snapshot.path()) {
            Ok(metadata) => metadata.len(),
            Err(error) => return snapshot.finish(Err(SnapshotImageError::Io(error))),
        };
        info!(plaintext_size, "created snapshot");
        Ok(CreatedSnapshot::new(snapshot, blobs))
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

    /// Read the exact image's replay state without installing it in a live database.
    /// The replication owner must authenticate acceptance and membership before admission.
    pub fn read_replay_baseline(
        plaintext: &[u8],
        snapshot: PublishedStoreSnapshot,
        genesis: &coven_protocol::store_commit::ResolvedStoreDeviceState,
    ) -> Result<InstalledReplayBaseline, SnapshotImageError> {
        if coven_protocol::store_commit::ObjectHash::digest(plaintext)
            != snapshot.meta.image.image_hash
        {
            return Err(SnapshotImageError::Projection(
                "snapshot image differs from its signed hash".into(),
            ));
        }
        snapshot
            .meta
            .history_summary
            .validate_snapshot_baseline()
            .map_err(DbError::from)?;
        let mut connection = Connection::open_in_memory()?;
        crate::connection_io::deserialize_database_image_into(&mut connection, plaintext)?;
        let coverage = snapshot.meta.coverage.clone();
        let (reference, state) = if coverage.commits().is_empty() {
            genesis.validate_canonical().map_err(DbError::from)?;
            (
                coven_protocol::store_commit::StoreDeviceStateRef::from_resolved(
                    coverage.clone(),
                    genesis,
                )
                .map_err(DbError::from)?,
                genesis.clone(),
            )
        } else {
            crate::store::store_device_state::store_device_state_for_history_cut_on(
                &connection,
                &coven_protocol::store_commit::StoreHistoryCut(coverage.commits().clone()),
            )?
        };
        if state != snapshot.meta.state.devices
            || reference != snapshot.meta.history_summary.post_state
        {
            return Err(SnapshotImageError::Projection(
                "snapshot image device state differs from its signed cut".into(),
            ));
        }
        let states = crate::store::store_device_state::load_covered_store_device_snapshots_on(
            &connection,
            &coverage,
        )?;
        if states.keys().any(|reference| {
            snapshot
                .meta
                .history_summary
                .causal_cut
                .get(&reference.coord)
                != Some(reference)
        }) {
            return Err(SnapshotImageError::Projection(
                "snapshot image carries a device state outside its exact accepted history".into(),
            ));
        }
        Ok(InstalledReplayBaseline::from_snapshot(snapshot, states))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The caller authenticates this snapshot's accepted publication. Its image
    /// carries exact Store blob provenance after source packages are retired.
    pub fn contains_reclaimable_store_blob(
        plaintext: &[u8],
        snapshot: &coven_protocol::store_commit::SnapshotMeta,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, SnapshotImageError> {
        if ObjectHash::digest(plaintext) != snapshot.image.image_hash {
            return Err(SnapshotImageError::Projection(
                "snapshot blob inventory differs from its signed image hash".into(),
            ));
        }
        if stored.locator().audience() != RemoteAudience::Store {
            return Err(SnapshotImageError::Projection(
                "Store snapshot inventory cannot authorize a Circle blob".into(),
            ));
        }
        let mut connection = Connection::open_in_memory()?;
        crate::connection_io::deserialize_database_image_into(&mut connection, plaintext)?;
        let id = coven_protocol::remote_object::remote_object_id(stored.object());
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM blob_locators WHERE remote_object_id = ?1)",
            [id.to_string()],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(false);
        }
        crate::blob_records::validate_stored_locator_on(&connection, stored)?;
        let remote = crate::remote_object_records::load_remote_object_on(&connection, id)?;
        remote.validate_reclaimable_stored_blob(stored)?;
        let owners = remote.stored_blob_commit_owners();
        if owners.is_empty()
            || owners
                .iter()
                .any(|owner| snapshot.history_summary.causal_cut.get(&owner.coord) != Some(owner))
        {
            return Err(SnapshotImageError::Projection(
                "snapshot blob inventory has no exact accepted publication owner".into(),
            ));
        }
        let live: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM row_blob_locators WHERE remote_object_id = ?1)",
            [id.to_string()],
            |row| row.get(0),
        )?;
        let pinned_for_replay: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM retained_replay_objects WHERE object_id = ?1)",
            [id.to_string()],
            |row| row.get(0),
        )?;
        Ok(!live && !pinned_for_replay && remote.snapshot_owners().next().is_none())
    }

    pub async fn read(&self) -> Result<Vec<u8>, SnapshotImageError> {
        tokio::fs::read(&self.path)
            .await
            .map_err(|error| SnapshotImageError::ProjectionIo {
                operation: format!("read staged snapshot database {}", self.path.display()),
                source: error,
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

    pub fn install_blob_graph(
        self,
        owner: &coven_protocol::remote_object::SnapshotObjectOwner,
        blobs: &[crate::PreparedSnapshotBlob],
        pending_store_snapshots: &BTreeSet<coven_protocol::objects::ObjectSlot>,
    ) -> Result<Self, SnapshotImageError> {
        let result = (|| {
            let source = std::fs::read(self.path()).map_err(SnapshotImageError::Io)?;
            let mut connection = Connection::open_in_memory()
                .map_err(DbError::from)
                .map_err(SnapshotImageError::from)?;
            crate::connection_io::deserialize_database_image_into(&mut connection, &source)
                .map_err(SnapshotImageError::from)?;
            connection
                .pragma_update(None, "foreign_keys", "ON")
                .map_err(SnapshotImageError::from)?;
            let transaction = connection.transaction().map_err(SnapshotImageError::from)?;
            for blob in blobs {
                blob.remote.validate().map_err(SnapshotImageError::from)?;
                if blob.remote.snapshot_owners().collect::<Vec<_>>() != [owner]
                    || blob.bindings.is_empty()
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
                    SnapshotImageError::ProjectionDatabase {
                        operation: "install snapshot blob".to_string(),
                        source: Box::new(error),
                    }
                })?;
            }
            crate::snapshot_objects::replace_snapshot_object_owners_on(
                &transaction,
                owner,
                blobs,
                pending_store_snapshots,
            )?;
            transaction.commit().map_err(SnapshotImageError::from)?;
            connection.execute_batch("VACUUM").map_err(|error| {
                SnapshotImageError::ProjectionSqlite {
                    operation: "vacuum snapshot closure".to_string(),
                    source: error,
                }
            })?;
            let image = crate::connection_io::serialize_database_image(&connection)
                .map_err(SnapshotImageError::from)?;
            connection
                .close()
                .map_err(|(_, error)| SnapshotImageError::ProjectionSqlite {
                    operation: "close snapshot closure image".to_string(),
                    source: error,
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

fn project(
    connection: &mut Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    authority: &mut VerifiedStoreAuthority,
    root: &coven_protocol::store_commit::StoreRootRef,
    synced: &[SyncedTable],
    routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    audience: &coven_protocol::circle::Audience,
) -> Result<(), SnapshotImageError> {
    let gates = crate::Gates::from_tables(connection, synced).map_err(SnapshotImageError::from)?;
    if gates.has_scoped_graph() && routing_key.is_none() {
        return Err(SnapshotImageError::Projection(
            "scoped snapshot projection requires a row-routing key".to_string(),
        ));
    }
    let transaction = connection
        .unchecked_transaction()
        .map_err(SnapshotImageError::from)?;
    transaction
        .pragma_update(None, "defer_foreign_keys", "ON")
        .map_err(SnapshotImageError::from)?;
    let coverage =
        crate::store::materialized_commit_index::materialized_frontier_on(&transaction, None)
            .map_err(SnapshotImageError::from)?;
    let cleared_materialization_tables = ["materialized_commits"];
    for table in cleared_materialization_tables {
        transaction
            .execute_batch(&format!("DELETE FROM {}", crate::quote_ident(table)))
            .map_err(|error| SnapshotImageError::ProjectionSqlite {
                operation: format!("clear {table}"),
                source: error,
            })?;
    }
    if matches!(audience, coven_protocol::circle::Audience::Store) {
        let records = crate::store::store_session::StoreTransaction::new(&transaction, store_dir);
        records
            .project_shared_snapshot_replay_inputs(authority, root)
            .map_err(SnapshotImageError::from)?;
        records
            .retain_snapshot_replay_inputs(
                authority,
                root,
                &coven_protocol::store_commit::CommitFrontier::from_refs(coverage.clone())
                    .map_err(DbError::from)?,
            )
            .map_err(SnapshotImageError::from)?;
        records
            .retain_snapshot_device_states(authority, root, coverage)
            .map_err(SnapshotImageError::from)?;
    }
    let preserved_non_synced_tables = match audience {
        coven_protocol::circle::Audience::Store | coven_protocol::circle::Audience::Circle(_) => {
            SNAPSHOT_PRESERVED_NON_SYNCED_TABLES
        }
        coven_protocol::circle::Audience::Local => {
            return Err(SnapshotImageError::Projection(
                "Local rows cannot enter a snapshot".to_string(),
            ));
        }
    };
    for table in crate::user_table_names(connection).map_err(|error| {
        SnapshotImageError::ProjectionSqlite {
            operation: "list user tables".to_string(),
            source: error,
        }
    })? {
        if synced.iter().any(|synced| synced.name() == table)
            || preserved_non_synced_tables.contains(&table.as_str())
            || cleared_materialization_tables.contains(&table.as_str())
        {
            continue;
        }
        transaction
            .execute_batch(&format!("DELETE FROM {}", crate::quote_ident(&table)))
            .map_err(|error| SnapshotImageError::ProjectionSqlite {
                operation: format!("clear {table}"),
                source: error,
            })?;
    }

    match audience {
        coven_protocol::circle::Audience::Store => gates
            .delete_gated_false(&transaction)
            .map_err(SnapshotImageError::from)?,
        coven_protocol::circle::Audience::Circle(_) => {
            crate::retain_snapshot_audience_rows(&transaction, &gates, audience, routing_key)
                .map_err(SnapshotImageError::from)?;
        }
        coven_protocol::circle::Audience::Local => {
            return Err(SnapshotImageError::Projection(
                "Local rows cannot enter a snapshot".to_string(),
            ));
        }
    }
    if let Some(routing_key) = routing_key {
        crate::validate_snapshot_routing_state(&transaction, &gates, routing_key, audience)
            .map_err(SnapshotImageError::from)?;
    }

    scope_authenticated_blob_graph(&transaction, synced, audience)?;
    transaction.commit().map_err(SnapshotImageError::from)?;
    // The Store image is the serialized file, so its free pages ship with it. A
    // Circle capture reads its rows out of this throwaway connection instead.
    if matches!(audience, coven_protocol::circle::Audience::Store) {
        connection.execute_batch("VACUUM").map_err(|error| {
            SnapshotImageError::ProjectionSqlite {
                operation: "vacuum".to_string(),
                source: error,
            }
        })?;
    }
    Ok(())
}

fn blob_refs(
    snapshot: &Connection,
    tables: &[SyncedTable],
) -> Result<Vec<RowBlobRef>, SnapshotImageError> {
    let gates = crate::Gates::from_tables(snapshot, tables)?;
    let mut references = Vec::new();
    for table in tables {
        let Some(declaration) = table.blob() else {
            continue;
        };
        let sql = format!(
            "SELECT id FROM {} WHERE {} IS NOT NULL ORDER BY id",
            crate::quote_ident(table.name()),
            crate::quote_ident(&declaration.id_column),
        );
        for row_id in crate::query_mapped_rows(snapshot, &sql, [], |row| row.get::<_, String>(0))? {
            let reference = Database::row_blob_ref_on(snapshot, &gates, table, &row_id)?;
            if reference.audience() == coven_protocol::circle::Audience::Local {
                return Err(SnapshotImageError::Projection(format!(
                    "scoped snapshot retains local blob row {:?}/{row_id:?}",
                    table.name(),
                )));
            }
            references.push(reference);
        }
    }
    Ok(references)
}

/// Scope a captured projection down to what `audience` may read, and collect
/// the blob closure its rows bind. Both published shapes run this before they
/// encode: the Store image serializes the connection, a Circle bootstrap reads
/// its projection tables out as a row changeset.
fn project_shared_snapshot(
    snapshot: &mut Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    authority: &mut VerifiedStoreAuthority,
    root: &coven_protocol::store_commit::StoreRootRef,
    tables: &[SyncedTable],
    routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    audience: &coven_protocol::circle::Audience,
) -> Result<Vec<RowBlobRef>, SnapshotImageError> {
    if tables.is_empty() {
        return Err(SnapshotImageError::NoSyncedTables);
    }
    let gates = crate::Gates::from_tables(snapshot, tables)?;
    let routing_key = if gates.has_scoped_graph() {
        let encryption = routing_encryption.ok_or_else(|| {
            SnapshotImageError::Projection(
                "scoped snapshot creation requires Store routing encryption".to_string(),
            )
        })?;
        Some(coven_protocol::circle::derive_row_routing_key(
            encryption,
            root.store_root_hash,
        )?)
    } else {
        None
    };
    project(
        snapshot,
        store_dir,
        authority,
        root,
        tables,
        routing_key.as_ref(),
        audience,
    )?;
    blob_refs(snapshot, tables)
}

/// Capture one Circle's bootstrap: its whole projection as a row changeset,
/// carrying every declared synced table plus the two routing tables, and the
/// blob closure those rows bind.
#[allow(clippy::too_many_arguments)]
pub(super) fn capture_circle_bootstrap_rows(
    mut snapshot: Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    mut authority: VerifiedStoreAuthority,
    root: &coven_protocol::store_commit::StoreRootRef,
    tables: &[SyncedTable],
    routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    circle_id: coven_protocol::circle::CircleId,
) -> Result<CreatedCircleSnapshot, SnapshotImageError> {
    let blobs = project_shared_snapshot(
        &mut snapshot,
        store_dir,
        &mut authority,
        root,
        tables,
        routing_encryption,
        &coven_protocol::circle::Audience::Circle(circle_id),
    )?;
    let projection_tables = circle_projection_tables(&snapshot, tables)?;
    let rows = crate::gate::full_state_rows(&snapshot, &projection_tables)?;
    Ok(CreatedCircleSnapshot::new(rows, blobs))
}

/// The tables a Circle bootstrap states: every declared synced table, plus the
/// audience mirror when the schema carries it (a database whose host declares
/// no scoped table has none). One order the capture, the staging and the
/// install all use.
pub(super) fn circle_projection_tables(
    connection: &Connection,
    tables: &[SyncedTable],
) -> Result<Vec<String>, SnapshotImageError> {
    let present = crate::user_table_names(connection).map_err(|error| {
        SnapshotImageError::ProjectionSqlite {
            operation: "list user tables".to_string(),
            source: error,
        }
    })?;
    let mut projection = tables
        .iter()
        .map(|table| table.name().to_string())
        .collect::<Vec<_>>();
    projection.extend(
        ["_coven_audience"]
            .into_iter()
            .filter(|routing| present.iter().any(|table| table == routing))
            .map(str::to_string),
    );
    projection.sort();
    projection.dedup();
    Ok(projection)
}

pub(super) fn snapshot_image_db_error(error: SnapshotImageError) -> DbError {
    DbError::from(error)
}

const SNAPSHOT_PRESERVED_NON_SYNCED_TABLES: &[&str] = &[
    "_coven_audience",
    "remote_objects",
    "blob_locators",
    "row_blob_locators",
    "store_device_registration_activations",
    "store_device_state_snapshots",
    "store_device_states",
    "store_author_exclusion_activations",
    "store_publication_current",
    "store_publication_entries",
    // Successor Circle heads locate their signed activation through this index.
    // Keep it in both the shared image and the recipient's replay baseline.
    "stream_activations",
    "circle_control_activations",
    "circle_access_cache",
    "circle_bootstrap_coverage",
    "circle_current_state",
    "retained_merge_materializations",
    "retained_replay_objects",
];

fn scope_authenticated_blob_graph(
    connection: &Connection,
    synced: &[SyncedTable],
    audience: &coven_protocol::circle::Audience,
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
        .map_err(|error| SnapshotImageError::ProjectionSqlite {
            operation: "create blob scope".to_string(),
            source: error,
        })?;
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
            .map_err(|error| SnapshotImageError::ProjectionSqlite {
                operation: format!("collect live blob bindings for {:?}", table.name()),
                source: error,
            })?;
    }
    // As above: the projection prunes the copy's rows, never this device's
    // payload claims.
    connection.execute_batch(
        "DELETE FROM row_blob_locators
             WHERE NOT EXISTS (
                 SELECT 1 FROM snapshot_live_blob_bindings AS live
                 WHERE live.table_name = row_blob_locators.table_name
                   AND live.row_id = row_blob_locators.row_id
                   AND live.column_name = row_blob_locators.column_name
                   AND live.row_stamp = row_blob_locators.row_stamp
             );",
    )?;
    // Accepted Store blobs stay in the encrypted inventory until their exact
    // deletion receipt. Their source packages may already have been retired.
    // A Circle bootstrap keeps only its live row bindings: the inventory is
    // scoped here to collect the closure the reference carries, and the payload
    // states rows alone.
    let mut statement = connection.prepare(
        "SELECT remote_object_id FROM blob_locators
         WHERE NOT EXISTS (SELECT 1 FROM row_blob_locators AS binding
                           WHERE binding.remote_object_id = blob_locators.remote_object_id)",
    )?;
    let orphan_ids = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for object_id in orphan_ids {
        let id = object_id
            .parse()
            .map_err(|error| DbError::context("snapshot inventory object id", error))?;
        let remote = crate::remote_object_records::load_remote_object_on(connection, id)?;
        let keep = if matches!(audience, coven_protocol::circle::Audience::Store)
            && remote.is_activated_stored_blob()
        {
            let locator =
                crate::blob_records::carried_blob_locator(&remote, "snapshot blob inventory")?;
            locator.audience() == RemoteAudience::Store
                && !remote.stored_blob_commit_owners().is_empty()
        } else {
            false
        };
        if !keep {
            connection.execute(
                "DELETE FROM blob_locators WHERE remote_object_id = ?1",
                [&object_id],
            )?;
        }
    }
    connection
        .execute_batch(
            "
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
        .map_err(|error| SnapshotImageError::ProjectionSqlite {
            operation: "scope blob ownership graph".to_string(),
            source: error,
        })?;
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_image_tests.rs"]
mod tests;
