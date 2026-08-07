/// Snapshot image creation, Store snapshot bootstrap, and blob installation.
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tracing::info;

use coven_database::Migration;
use coven_database::{Database, SnapshotDatabaseImage};
use coven_protocol::objects::StorageError;
use coven_protocol::synced_schema::SyncedTable;
use coven_storage::SyncStorage;

/// Default: create a snapshot after this many changesets since the last one.
const SNAPSHOT_CHANGESET_THRESHOLD: u64 = 100;

/// Default: create a snapshot after this many hours since the last one.
const SNAPSHOT_HOURS_THRESHOLD: u64 = 24;

/// Error type for snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Image(#[from] coven_database::SnapshotImageError),
    #[error("snapshot control JSON parse failed: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Bucket(#[from] StorageError),
    #[error("Store protocol object error: {0}")]
    StoreObject(#[source] coven_protocol::objects::StoreObjectError),
    #[error("Store history: {0}")]
    StoreHistory(#[from] crate::sync::store::owner::pull::StorePullError),
    /// The snapshot's author is not authorized to publish a catalog image: not a
    /// current Owner of the store's membership chain, or the
    /// chain itself is not anchored to the store's owner (a wiped/refounded
    /// chain). The snapshot is refused rather than adopted.
    #[error("snapshot author is not an authorized owner: {0}")]
    UnauthorizedAuthor(String),
    /// The snapshot's synced-schema version is newer than this binary's top
    /// migration, so its DB image carries columns this binary's tables lack. The
    /// generation is refused before its image is downloaded; the same refusal is
    /// the at-open backstop in [`coven_database::run_migrations_in_transaction`].
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
    #[error("snapshot bootstrap database changed after verification")]
    BootstrapDatabaseChanged,
    #[error("snapshot bootstrap database: {0}")]
    BootstrapDatabase(String),
    #[error("snapshot bootstrap state: {0}")]
    BootstrapState(String),
    #[error("snapshot publication state: {0}")]
    PublicationState(String),
    #[error(
        "could not remove staged snapshot database {path}: {cleanup}",
        path = .path.display()
    )]
    StagedDatabaseCleanup { path: PathBuf, cleanup: String },
    #[error(
        "snapshot operation failed and staged database {path} could not be removed: {cleanup} \
         (operation error: {cause})",
        path = .path.display()
    )]
    StagedDatabaseCleanupAfterFailure {
        path: PathBuf,
        cleanup: String,
        cause: Box<SnapshotError>,
    },
}

impl From<coven_database::SnapshotImageOperationError<SnapshotError>> for SnapshotError {
    fn from(error: coven_database::SnapshotImageOperationError<SnapshotError>) -> Self {
        match error {
            coven_database::SnapshotImageOperationError::Operation(cause) => cause,
            coven_database::SnapshotImageOperationError::Cleanup { path, cleanup } => {
                Self::StagedDatabaseCleanup { path, cleanup }
            }
            coven_database::SnapshotImageOperationError::CleanupAfterFailure {
                path,
                cleanup,
                cause,
            } => Self::StagedDatabaseCleanupAfterFailure {
                path,
                cleanup,
                cause: Box::new(cause),
            },
        }
    }
}

impl From<coven_database::DbError> for SnapshotError {
    fn from(error: coven_database::DbError) -> Self {
        Self::PublicationState(error.to_string())
    }
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
/// fn duplicate(result: crate::sync::store::snapshot::PreparedSnapshotBootstrap) {
///     let _copy = result.clone();
/// }
/// ```
pub struct PreparedSnapshotBootstrap<'storage> {
    database_image: SnapshotDatabaseImage,
    db_hash: String,
    history_verifier:
        crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<'storage>,
    storage: &'storage std::sync::Arc<dyn SyncStorage>,
    founder_registration: coven_protocol::objects::VerifiedObject<
        coven_protocol::store_commit::StoreDeviceRegistration,
    >,
    restorer_identity: coven_keys::keys::UserKeypair,
    snapshot: coven_database::PublishedStoreSnapshot,
    coverage: coven_protocol::store_commit::CommitFrontier,
    stability: coven_database::VerifiedStoreSnapshotStability,
    membership: coven_protocol::membership::MembershipChain,
    #[cfg(any(test, feature = "test-utils"))]
    fail_circle_install: bool,
}

impl std::fmt::Debug for PreparedSnapshotBootstrap<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSnapshotBootstrap")
            .field("target_path", &self.database_image.path())
            .field("db_hash", &self.db_hash)
            .field("snapshot", &self.snapshot.reference)
            .field("coverage", &self.coverage)
            .finish_non_exhaustive()
    }
}

impl<'storage> PreparedSnapshotBootstrap<'storage> {
    /// Authenticate and stage one snapshot image as installation authority.
    pub async fn prepare(
        storage: &'storage std::sync::Arc<dyn SyncStorage>,
        mut history_verifier: crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<
            'storage,
        >,
        membership_floor: &coven_protocol::membership::MembershipFloor,
        binary_schema_version: u32,
        target_path: &Path,
        restorer_identity: &coven_keys::keys::UserKeypair,
    ) -> Result<Self, SnapshotError> {
        let root = history_verifier.verified_root();
        if root.protocol().descriptor.store_root_id() != root.reference().store_root_id {
            return Err(SnapshotError::UnauthorizedAuthor(
                "Store root differs from bootstrap authority".to_string(),
            ));
        }
        let heads = &membership_floor.0;
        let mut registrations = std::collections::BTreeMap::new();
        let mut resolutions = std::collections::BTreeSet::new();
        for reference in heads {
            let head = history_verifier
                .load_exact_membership_head(reference)
                .await
                .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
            resolutions.extend(head.body.resolutions.iter().cloned());
            let registration = history_verifier
                .load_registration(&head.body.author_registration)
                .await
                .map_err(|error| SnapshotError::Parse(error.to_string()))?
                .value;
            registrations.insert(head.body.author_registration.clone(), registration);
        }
        let resolutions = resolutions.into_iter().collect::<Vec<_>>();
        let membership = history_verifier
            .load_membership_at_exact_heads(heads, &resolutions)
            .await
            .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
        let registrations = registrations
            .into_iter()
            .filter(|(_, registration)| membership.is_owner_now(&registration.author_pubkey))
            .collect::<Vec<_>>();
        let mut authorized = Vec::new();
        for (registration_ref, registration) in registrations {
            authorized.extend(
                history_verifier
                    .load_store_snapshot_stream(&registration_ref, &registration)
                    .await?,
            );
        }
        let selected = Box::pin(history_verifier.select_maximal_stable_store_snapshot(authorized))
            .await
            .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?
            .ok_or_else(|| {
                SnapshotError::Bucket(coven_protocol::objects::StorageError::NotFound(
                    "Store snapshot stream".to_string(),
                ))
            })?;
        let snapshot = selected.snapshot;
        if snapshot.meta.schema_version > binary_schema_version {
            return Err(SnapshotError::SchemaTooNew {
                snapshot_version: snapshot.meta.schema_version,
                supported: binary_schema_version,
            });
        }
        let plaintext = history_verifier
            .load_snapshot_image(&snapshot)
            .await
            .map_err(SnapshotError::StoreObject)?;
        if coven_protocol::store_commit::ObjectHash::digest(&plaintext)
            != snapshot.meta.image.image_hash
        {
            return Err(SnapshotError::Parse(
                "Store snapshot image differs from its exact reference".to_string(),
            ));
        }
        let founder_registration = history_verifier
            .load_founder_registration()
            .await
            .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        let stability = selected.stability;
        let coverage = snapshot.meta.coverage.clone();
        let database_image =
            SnapshotDatabaseImage::create(target_path.to_path_buf(), &plaintext)?.canonicalize()?;
        info!(
            num_positions = coverage.position_count(),
            db_size = plaintext.len(),
            path = %database_image.path().display(),
            "bootstrapped from snapshot"
        );

        Ok(Self {
            database_image,
            db_hash: snapshot_db_hash(&plaintext),
            history_verifier,
            storage,
            founder_registration,
            restorer_identity: restorer_identity.clone(),
            snapshot,
            coverage,
            stability,
            membership,
            #[cfg(any(test, feature = "test-utils"))]
            fail_circle_install: false,
        })
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
    pub async fn install(
        self,
        store_dir: &'storage coven_foundation::store_dir::StoreDir,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::RestoringStore<'storage>, SnapshotError> {
        let PreparedSnapshotBootstrap {
            database_image,
            db_hash,
            mut history_verifier,
            storage,
            founder_registration,
            restorer_identity,
            snapshot,
            coverage,
            stability,
            membership,
            #[cfg(any(test, feature = "test-utils"))]
            fail_circle_install,
        } = self;
        let root = history_verifier.verified_root().clone();
        let bound_path = database_image.path().to_path_buf();
        let result = async {
            let database_bytes = std::fs::read(&bound_path)?;
            if snapshot_db_hash(&database_bytes) != db_hash {
                return Err(SnapshotError::BootstrapDatabaseChanged);
            }
            let root_ref = root.reference().clone();
            let store_frontier = coverage.clone();
            let install = coven_database::VerifiedSnapshotBootstrapInstall::new(
                snapshot,
                root.object().clone(),
                founder_registration,
                stability,
                coven_database::InitialStoreMembershipAuthority {
                    head_refs: membership.head_refs().to_vec(),
                },
                routing_encryption,
                Vec::new(),
            )
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;

            let decisions = match routing_encryption {
                // Circles exist only in a scoped (Circle-routing) Store; without
                // routing encryption there are no Circle images to stage.
                Some(encryption) => {
                    let routing_key = coven_protocol::circle::derive_row_routing_key(
                        encryption,
                        root_ref.store_root_hash,
                    )
                    .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
                    let query_path = bound_path.with_extension("restore-select.db");
                    let query_image = SnapshotDatabaseImage::prepare(query_path)?;
                    if let Err(error) = std::fs::copy(&bound_path, query_image.path()) {
                        return query_image
                            .finish_operation(Err(SnapshotError::Io(error)))
                            .map_err(SnapshotError::from);
                    }
                    let query_path = query_image.path().to_path_buf();
                    let staged = async {
                        let query_db = Database::open_initialized_store(
                            &query_path,
                            &install,
                            synced_tables.clone(),
                            blob_tombstone_grace,
                            transfer_limits,
                            device_id.clone(),
                            clock.clone(),
                            migrations,
                        )
                        .map_err(|error| SnapshotError::BootstrapDatabase(error.to_string()))?;
                        let store_database = coven_database::StoreDatabase::from_database(query_db);
                        crate::sync::store::snapshots::CircleSnapshotReader::new(
                            &store_database,
                            storage.as_ref(),
                            &mut history_verifier,
                        )
                        .select_staged_decisions(
                            &store_frontier,
                            &restorer_identity,
                            Some(&routing_key),
                        )
                        .await
                    }
                    .await;
                    query_image
                        .finish_operation(staged)
                        .map_err(SnapshotError::from)?
                }
                None => Vec::new(),
            };
            let install = install.with_circle_decisions(decisions);
            #[cfg(any(test, feature = "test-utils"))]
            let install = if fail_circle_install {
                install.fail_circle_install_for_test()
            } else {
                install
            };
            let db = Database::open_initialized_store(
                &bound_path,
                &install,
                synced_tables,
                blob_tombstone_grace,
                transfer_limits,
                device_id,
                clock,
                migrations,
            )
            .map_err(|error| SnapshotError::BootstrapDatabase(error.to_string()))?;
            let database = coven_database::StoreDatabase::from_database(db);
            let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
                database.clone(),
                storage.as_ref(),
                root_ref.clone(),
            );
            let keyrings =
                crate::sync::store::owner::keyring::StoreKeyrings::new(storage.as_ref(), root_ref);
            let blob_cache =
                crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
            Ok(
                crate::sync::store::owner::history::AuthorizedStoreHistory::from_snapshot(
                    super::SnapshotHistoryConstruction,
                    database,
                    storage,
                    store_dir,
                    blob_cache,
                    history_verifier,
                    blob_source,
                    keyrings,
                )
                .bind_restore(membership, restorer_identity),
            )
        }
        .await;
        match result {
            Ok(database) => {
                let committed_path = database_image.commit();
                debug_assert_eq!(committed_path, bound_path);
                Ok(database)
            }
            Err(cause) => database_image
                .finish_operation(Err(cause))
                .map_err(SnapshotError::from),
        }
    }

    /// Arm the Circle-install failure injection carried into `install`'s
    /// install transaction — a test's stand-in for a crash between the Store and
    /// Circle installs.
    #[cfg(test)]
    pub(crate) fn fail_circle_install_for_test(mut self) -> Self {
        self.fail_circle_install = true;
        self
    }

    #[cfg(test)]
    pub(crate) fn selected_snapshot_hash_for_test(
        &self,
    ) -> coven_protocol::store_commit::ObjectHash {
        self.snapshot.reference.snapshot_hash
    }

    #[cfg(test)]
    pub(crate) fn selected_snapshot_object_hash_for_test(
        &self,
    ) -> coven_protocol::store_commit::ObjectHash {
        self.snapshot.reference.object.stored_hash()
    }

    #[cfg(test)]
    pub(crate) fn staged_database_bytes_for_test(&self) -> Result<Vec<u8>, SnapshotError> {
        Ok(std::fs::read(self.database_image.path())?)
    }
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

/// The outcome of snapshot blob reconciliation: every required eager blob is
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
#[path = "image_tests.rs"]
mod tests;
