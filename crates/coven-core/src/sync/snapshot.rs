/// Snapshot image creation, Store snapshot bootstrap, and blob installation.
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{info, warn};

use super::session::SyncedTable;
use super::storage::{StorageError, SyncStorage};
use crate::blob::{BlobRef, Provenance};
use crate::database::Database;
use crate::migration::Migration;

/// Default: create a snapshot after this many changesets since the last one.
const SNAPSHOT_CHANGESET_THRESHOLD: u64 = 100;

/// Default: create a snapshot after this many hours since the last one.
const SNAPSHOT_HOURS_THRESHOLD: u64 = 24;

pub(crate) struct CreatedSnapshot {
    pub db_image: Vec<u8>,
    pub host_blobs: Vec<BlobRef>,
    pub publish_blobs: Vec<BlobRef>,
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
    store_root_hash: super::store_commit::ObjectHash,
    snapshot_hash: super::store_commit::ObjectHash,
    coverage: super::store_commit::CommitFrontier,
}

#[cfg(test)]
mod bootstrap_capability_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::{CommitPosition, ObjectHash};
    use crate::sync::test_helpers::{
        open_test_db, publish_test_founder_membership, publish_test_store_protocol_root,
        test_migrations, test_synced_tables,
    };

    const STORE_ID: &str = "bootstrap-capability-store";

    struct PublishedSnapshot {
        _temp: tempfile::TempDir,
        storage: CloudSyncStorage,
        owner: UserKeypair,
        store_root_hash: ObjectHash,
        membership_floor: crate::join_code::MembershipFloor,
        coverage: BTreeMap<String, CommitPosition>,
    }

    async fn published_snapshot() -> PublishedSnapshot {
        let temp = tempfile::tempdir().expect("snapshot fixture directory");
        let owner = UserKeypair::generate();
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            STORE_ID,
            owner.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(
            "bootstrap-capability",
        )));
        let source = open_test_db();
        let store_root_hash =
            publish_test_store_protocol_root(&source, &storage, STORE_ID, "source", &owner).await;
        let membership = publish_test_founder_membership(&storage, STORE_ID, &owner).await;
        let snapshot_dir = temp.path().to_path_buf();
        let tables = source.synced_tables().to_vec();
        let image = source
            .call(move |connection| {
                create_snapshot(connection, &snapshot_dir, &tables)
                    .map_err(|error| crate::database::DbError(error.to_string()))
            })
            .await
            .expect("create snapshot image");
        let coverage = BTreeMap::from([(
            "source".to_string(),
            CommitPosition {
                seq: 3,
                commit_hash: ObjectHash::digest(b"source-commit-three"),
            },
        )]);
        super::super::store_snapshot::push_store_snapshot(
            &storage,
            store_root_hash,
            CreatedSnapshot {
                db_image: image,
                host_blobs: Vec::new(),
                publish_blobs: Vec::new(),
            },
            crate::CommitFrontier::MergeConcurrent(coverage.clone()),
            source.schema_version(),
            &owner,
            "2026-07-14T00:00:00Z".to_string(),
            Some(&membership),
            &source,
        )
        .await
        .expect("publish Store snapshot");
        PublishedSnapshot {
            _temp: temp,
            storage,
            owner,
            store_root_hash,
            membership_floor: crate::join_code::MembershipFloor::MergeConcurrent(
                membership.author_heads(),
            ),
            coverage,
        }
    }

    async fn capability(fixture: &PublishedSnapshot, target: &Path) -> BootstrapResult {
        bootstrap_from_snapshot(
            &fixture.storage,
            STORE_ID,
            fixture.store_root_hash,
            &crate::keys::public_key_hex(&fixture.owner),
            &fixture.membership_floor,
            1,
            target,
        )
        .await
        .expect("verify snapshot into destination")
    }

    async fn consume(
        result: BootstrapResult,
        store_id: &str,
        target: &Path,
    ) -> Result<Database, SnapshotError> {
        result
            .open_database(
                store_id,
                target,
                test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                "reader".to_string(),
                &test_migrations(),
            )
            .await
    }

    #[tokio::test]
    async fn verified_coverage_cannot_cross_store_or_destination() {
        let fixture = published_snapshot().await;

        let wrong_store_path = fixture._temp.path().join("wrong-store.db");
        let wrong_store = capability(&fixture, &wrong_store_path).await;
        assert!(matches!(
            consume(wrong_store, "another-store", &wrong_store_path).await,
            Err(SnapshotError::BootstrapStoreMismatch { .. })
        ));
        assert!(!wrong_store_path.exists());

        let bound_path = fixture._temp.path().join("bound.db");
        let wrong_destination = capability(&fixture, &bound_path).await;
        let other_path = fixture._temp.path().join("other.db");
        std::fs::copy(&bound_path, &other_path).expect("copy verified image");
        assert!(matches!(
            consume(wrong_destination, STORE_ID, &other_path).await,
            Err(SnapshotError::BootstrapDestinationMismatch { .. })
        ));
        assert!(!bound_path.exists());
    }

    #[tokio::test]
    async fn verified_database_hash_is_rechecked_when_capability_is_consumed() {
        let fixture = published_snapshot().await;
        let target = fixture._temp.path().join("changed.db");
        let result = capability(&fixture, &target).await;
        std::fs::write(&target, b"substituted database").expect("replace verified image");

        assert!(matches!(
            consume(result, STORE_ID, &target).await,
            Err(SnapshotError::BootstrapDatabaseChanged)
        ));
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn consuming_capability_installs_exact_store_protocol_root_snapshot_and_coverage() {
        let fixture = published_snapshot().await;
        let target = fixture._temp.path().join("installed.db");
        let result = capability(&fixture, &target).await;
        let db = consume(result, STORE_ID, &target)
            .await
            .expect("consume verified snapshot capability");

        assert_eq!(
            db.get_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
                .await
                .expect("read installed Store protocol root"),
            Some(fixture.store_root_hash.to_string())
        );
        assert_eq!(
            db.snapshot_coverage_frontier()
                .await
                .expect("read installed coverage"),
            fixture.coverage
        );
        assert!(db
            .get_protocol_state(crate::database::LAST_SNAPSHOT_HASH_STATE_KEY)
            .await
            .expect("read installed snapshot hash")
            .is_some());
    }
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
            db.install_bootstrap_state(&self.coverage, self.snapshot_hash, self.store_root_hash)
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
/// `store-v1/snapshot-images/{author}/{image_hash}/copies/...`, binding the
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

    let publish_blobs = match snapshot_publish_blobs(&snapshot_path, tables) {
        Ok(blobs) => blobs,
        Err(e) => {
            cleanup_snapshot_path(&snapshot_path);
            return Err(e);
        }
    };
    let host_blobs = publish_blobs
        .iter()
        .filter(|blob| blob.provenance == Provenance::HostProvided)
        .cloned()
        .collect();

    // Read the cleared snapshot file. The storage implementation seals it at the
    // final cloud key so the AEAD context can bind that key.
    let plaintext = read_and_remove_snapshot(&snapshot_path)?;
    let plaintext_size = plaintext.len();

    info!(plaintext_size, "created snapshot");

    Ok(CreatedSnapshot {
        db_image: plaintext,
        host_blobs,
        publish_blobs,
    })
}

fn snapshot_publish_blobs(
    path: &Path,
    tables: &[SyncedTable],
) -> Result<Vec<BlobRef>, SnapshotError> {
    let conn = Connection::open(path)
        .map_err(|e| SnapshotError::ClearFailed(format!("failed to open snapshot copy: {e}")))?;
    let decls = crate::blob::decl::BlobDecls::from_tables(&conn, tables)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    let mut seen = HashSet::new();
    decls
        .refs_in_db(&conn)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))
        .map(|refs| {
            refs.into_iter()
                .filter(|blob| seen.insert((blob.namespace.clone(), blob.id.clone())))
                .collect()
        })
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

/// The one non-synced table whose rows ride a snapshot. A blob's uploader — which
/// member's cloud prefix holds it — is a member-global fact identical on every
/// device (unlike per-device materialized positions, outboxes, and cache budgets),
/// and it is recorded only from authenticated sources (a signed Store commit's
/// author or our own upload). A device that bootstraps from a snapshot does not
/// replay commits covered by that snapshot, so the owner-signed image must carry
/// the authoritative uploader index used to dispatch blob reads.
const SNAPSHOT_PRESERVED_NON_SYNCED_TABLE: &str = "blob_uploaders";

/// On the snapshot-copy connection, scope it down to exactly what is eligible to
/// cross devices, then VACUUM to reclaim the freed pages:
///
/// 1. Table-level: DELETE every user table not in `synced` (except the
///    [`SNAPSHOT_PRESERVED_NON_SYNCED_TABLE`]) — local-only tables keep their
///    schema, lose their rows.
/// 2. Row-level: within the synced tables, DELETE the rows the gate excludes
///    (gated-false roots and their FK-descendants), so a private subtree does
///    not ride the snapshot to a restoring peer. This is the same exclusion the
///    outbound changeset gate applies; both reuse [`crate::sync::gate::Gates`].
fn clear_non_synced(conn: &Connection, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    for table in list_user_tables(conn)? {
        if synced.iter().any(|t| t.name() == table) {
            continue;
        }
        if table == SNAPSHOT_PRESERVED_NON_SYNCED_TABLE {
            continue;
        }
        conn.execute_batch(&format!(
            "DELETE FROM {}",
            crate::sync::session::quote_ident(&table)
        ))
        .map_err(|e| SnapshotError::ClearFailed(format!("clear {table}: {e}")))?;
    }

    // The snapshot is a second propagation channel: the changeset gate cuts
    // gated-false rows on the wire, so the snapshot must drop them too or a
    // private subtree leaks to a restoring device. Reuse the changeset gate's
    // model rather than re-deriving the FK walk.
    let gates = crate::sync::gate::Gates::from_tables(conn, synced)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    gates
        .delete_gated_false(conn)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;

    // Reclaim the pages freed by the DELETEs so the blob shrinks.
    conn.execute_batch("VACUUM")
        .map_err(|e| SnapshotError::ClearFailed(format!("vacuum: {e}")))?;
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
    expected_store_root_hash: super::store_commit::ObjectHash,
    owner_pubkey: &str,
    membership_floor: &crate::join_code::MembershipFloor,
    binary_schema_version: u32,
    target_path: &Path,
) -> Result<BootstrapResult, SnapshotError> {
    // Authenticate Store protocol root, membership, snapshot metadata, and the exact image
    // before returning installation authority.
    let (store_root_hash, write_policy, meta, plaintext) =
        super::store_snapshot::select_store_snapshot(
            storage,
            store_id,
            expected_store_root_hash,
            owner_pubkey,
            membership_floor,
            binary_schema_version,
        )
        .await?;
    let snapshot_hash = meta.snapshot_hash();
    let coverage = meta.coverage;
    if coverage.policy() != write_policy {
        return Err(SnapshotError::Parse(format!(
            "snapshot coverage uses {:?}, Store protocol root uses {write_policy:?}",
            coverage.policy()
        )));
    }
    write_snapshot_db(target_path, &plaintext)?;
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
        store_root_hash,
        snapshot_hash,
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
/// evictable cache `storage/cache/<namespace>/<id>` under `store_dir`, skipping
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
    let blobs: Vec<crate::sync::pull::BlobDownload> = {
        let conn = Connection::open(db_path).map_err(crate::database::DbError::from)?;
        let decls = crate::blob::decl::BlobDecls::from_tables(&conn, tables)
            .map_err(|e| crate::database::DbError(format!("blob decls: {e}")))?;
        decls
            .refs_in_db(&conn)
            .map_err(|e| crate::database::DbError(format!("blob decls: {e}")))?
            .into_iter()
            .filter(|blob| blob.fill == crate::blob::CacheFill::CacheEager)
            .map(crate::sync::pull::BlobDownload::from_installed_db)
            .collect()
    };

    if blobs.is_empty() {
        return Ok(SnapshotBlobReconcile::Complete);
    }

    let total = blobs.len();
    // The blobs are `CacheEager`, so `download_blobs` writes each into the
    // evictable cache `storage/cache/<namespace>/<id>` under `store_dir`.
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
        if crate::sync::pull::download_blobs(db, vec![blob], storage, store_dir, None)
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
