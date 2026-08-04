/// Snapshot image creation, Store snapshot bootstrap, and blob installation.
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tracing::info;

use crate::database::{Database, SnapshotDatabaseImage};
use crate::protocol::objects::StorageError;
use crate::storage::SyncStorage;
use crate::sync::session::SyncedTable;
use crate::Migration;

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
    Image(#[from] crate::database::SnapshotImageError),
    #[error("snapshot control JSON parse failed: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Bucket(#[from] StorageError),
    #[error("Store protocol object error: {0}")]
    StoreObject(#[source] crate::protocol::objects::StoreObjectError),
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
    /// the at-open backstop in [`crate::database::run_migrations_in_transaction`].
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

impl From<crate::database::SnapshotImageOperationError<SnapshotError>> for SnapshotError {
    fn from(error: crate::database::SnapshotImageOperationError<SnapshotError>) -> Self {
        match error {
            crate::database::SnapshotImageOperationError::Operation(cause) => cause,
            crate::database::SnapshotImageOperationError::Cleanup { path, cleanup } => {
                Self::StagedDatabaseCleanup { path, cleanup }
            }
            crate::database::SnapshotImageOperationError::CleanupAfterFailure {
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

impl From<crate::database::DbError> for SnapshotError {
    fn from(error: crate::database::DbError) -> Self {
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
pub(crate) struct PreparedSnapshotBootstrap<'storage> {
    database_image: SnapshotDatabaseImage,
    db_hash: String,
    history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
    storage: &'storage std::sync::Arc<dyn SyncStorage>,
    founder_registration: crate::protocol::objects::VerifiedObject<
        crate::protocol::store_commit::StoreDeviceRegistration,
    >,
    restorer_identity: crate::keys::UserKeypair,
    snapshot: crate::database::PublishedStoreSnapshot,
    coverage: crate::protocol::store_commit::CommitFrontier,
    stability: crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
    membership: crate::protocol::membership::MembershipChain,
    #[cfg(test)]
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
    pub(crate) async fn prepare(
        storage: &'storage std::sync::Arc<dyn SyncStorage>,
        mut history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<
            'storage,
        >,
        membership_floor: &crate::joining::MembershipFloor,
        binary_schema_version: u32,
        target_path: &Path,
        restorer_identity: &crate::keys::UserKeypair,
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
                SnapshotError::Bucket(crate::protocol::objects::StorageError::NotFound(
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
        if crate::protocol::store_commit::ObjectHash::digest(&plaintext)
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
            #[cfg(test)]
            fail_circle_install: false,
        })
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
    ) -> crate::protocol::store_commit::ObjectHash {
        self.snapshot.reference.snapshot_hash
    }

    #[cfg(test)]
    pub(crate) fn selected_snapshot_object_hash_for_test(
        &self,
    ) -> crate::protocol::store_commit::ObjectHash {
        self.snapshot.reference.object.stored_hash()
    }

    #[cfg(test)]
    pub(crate) fn staged_database_bytes_for_test(&self) -> Result<Vec<u8>, SnapshotError> {
        Ok(std::fs::read(self.database_image.path())?)
    }

    pub(crate) fn coverage_count(&self) -> usize {
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
    pub(crate) async fn install(
        self,
        store_dir: &'storage crate::store_dir::StoreDir,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: crate::blob::TransferLimits,
        device_id: String,
        clock: crate::clock::ClockRef,
        migrations: &[Migration],
        routing_encryption: Option<&crate::encryption::EncryptionService>,
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
            #[cfg(test)]
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
            let install = crate::database::VerifiedSnapshotBootstrapInstall::new(
                snapshot,
                root.object().clone(),
                founder_registration,
                stability,
                crate::database::InitialStoreMembershipAuthority {
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
                    let routing_key = crate::protocol::circle::derive_row_routing_key(
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
                        let store_database =
                            crate::database::StoreDatabase::from_database(query_db);
                        crate::sync::store::owner::circles::snapshots::CircleSnapshotReader::new(
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
            #[cfg(test)]
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
            let database = crate::database::StoreDatabase::from_database(db);
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
pub(crate) enum SnapshotBlobReconcile {
    Complete,
    Incomplete,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::database::{verify_circle_bootstrap_image, StoreDatabase};
    use crate::keys::UserKeypair;
    use crate::protocol::store_commit::CommitFrontier;

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

    async fn seed_scoped_snapshot_rows(source: &Database) -> crate::protocol::circle::CircleId {
        let database = StoreDatabase::new(source);
        let circle = database
            .install_test_active_circle("snapshot-route-circle".to_string())
            .await
            .expect("install snapshot route Circle");
        let write_circle = circle;
        database
            .run_host_store_write_for_test(
                Some(crate::encryption::EncryptionService::from_key([42; 32])),
                None,
                move |transaction| {
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
                            write_circle.to_string(),
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
                    Ok(())
                },
            )
            .await
            .expect("commit scoped snapshot rows");
        circle
    }

    fn circle_bootstrap_reference(
        source: &Database,
        image: &[u8],
    ) -> crate::protocol::circle::CircleBootstrapRef {
        let image_hash = crate::protocol::store_commit::ObjectHash::digest(image);
        crate::protocol::circle::CircleBootstrapRef {
            coverage: CommitFrontier(BTreeMap::new()),
            schema_version: source.schema_version(),
            sync_routing_hash: source.sync_routing_hash(),
            image: crate::protocol::store_commit::SnapshotImageRef {
                image_hash,
                object: crate::protocol::objects::ExactObjectRef::new(
                    crate::protocol::objects::ObjectSlot::logical(
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
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "circle-bootstrap-routing-key",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create Circle bootstrap routing Store");
        let circle_id = seed_scoped_snapshot_rows(&source).await;
        let image_dir = tempfile::tempdir().expect("Circle bootstrap routing image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_circle_snapshot_image_for_test(
                root,
                image_path,
                crate::encryption::EncryptionService::from_key([42; 32]),
                circle_id,
            )
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
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "circle-bootstrap-store-row",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create Circle bootstrap Store-row Store");
        let circle_id = seed_scoped_snapshot_rows(&source).await;
        let image_dir = tempfile::tempdir().expect("Circle bootstrap Store-row image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_circle_snapshot_image_for_test(
                root,
                image_path,
                crate::encryption::EncryptionService::from_key([42; 32]),
                circle_id,
            )
            .await
            .expect("create Circle projection for Store-row tampering");
        let routing_key = crate::protocol::circle::derive_row_routing_key(
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
            crate::protocol::circle::row_routing_id(&routing_key, "documents", store_row_id)
                .to_string();
        let image = edit_snapshot_image(image_dir.path(), image, |connection| {
            connection
                .execute(
                    "INSERT INTO documents VALUES (?1, NULL, ?2, ?3)",
                    (store_row_id, "Store row in Circle image", store_row_stamp),
                )
                .expect("insert scoped Store row into Circle bootstrap");
            connection
                .install_row_route(
                    &store_routing_id,
                    "documents",
                    store_row_id,
                    store_row_stamp,
                )
                .expect("insert scoped Store row route into Circle bootstrap");
            connection
                .install_audience_mirror(&store_routing_id, None, store_row_stamp)
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
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "circle-bootstrap-unscoped-row",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create Circle bootstrap unscoped-row Store");
        let circle_id = source
            .test_sql(|connection| {
                Ok::<_, crate::database::DbError>(
                    connection
                        .install_test_active_circle("circle-bootstrap-unscoped")
                        .0,
                )
            })
            .await
            .expect("install Circle bootstrap unscoped Circle");
        let image_dir = tempfile::tempdir().expect("Circle bootstrap unscoped image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_circle_snapshot_image_for_test(
                root,
                image_path,
                crate::encryption::EncryptionService::from_key([42; 32]),
                circle_id,
            )
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
        let routing_key = crate::protocol::circle::derive_row_routing_key(
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
        store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
        membership: crate::protocol::membership::MembershipChain,
        _store_dir_temp: tempfile::TempDir,
        store_dir: crate::store_dir::StoreDir,
    }

    impl PublishedScopedSnapshot {
        async fn publish(store_id: &str, image_kind: ScopedSnapshotImage) -> Self {
            let source = open_scoped_snapshot_test_db();
            let signer = UserKeypair::generate();
            let store = crate::sync::test_helpers::TestStore::create(
                &source,
                store_id,
                signer.clone(),
                crate::sync::test_helpers::test_cloud_home(),
            )
            .await
            .expect("create published scoped snapshot Store");
            let device = store
                .open_into(&source)
                .await
                .expect("load published scoped snapshot membership");
            let membership = device
                .membership_for_test()
                .await
                .expect("project published scoped snapshot membership");
            seed_scoped_snapshot_rows(&source).await;

            let image_dir = tempfile::tempdir().expect("published scoped snapshot image directory");
            let image_path = image_dir.path().to_path_buf();
            let root = store.root.clone();
            let image = StoreDatabase::new(&source)
                .capture_snapshot_image_for_test(
                    root,
                    image_path,
                    Some(crate::encryption::EncryptionService::from_key([42; 32])),
                )
                .await
                .expect("create published scoped snapshot image");
            let image = match image_kind {
                ScopedSnapshotImage::Valid => image,
                ScopedSnapshotImage::UnauthenticatedRoute => {
                    edit_snapshot_image(image_dir.path(), image, |connection| {
                        connection
                            .corrupt_document_route_id()
                            .expect("tamper private route id");
                    })
                }
                ScopedSnapshotImage::CircleRow => {
                    let route = source
                        .test_sql(|database| {
                            database.document_circle_route("2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7")
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
                            .install_row_route(
                                &route.1,
                                "documents",
                                "2f1a7bc0-5d31-4ce6-9f4b-e37de58b11b7",
                                &route.2,
                            )
                            .expect("insert Circle private route into Store snapshot");
                    })
                }
                ScopedSnapshotImage::InvalidCircleMirror => {
                    edit_snapshot_image(image_dir.path(), image, |connection| {
                        connection
                            .replace_first_circle_audience(Some("local"))
                            .expect("replace Circle mirror with Local audience");
                    })
                }
                ScopedSnapshotImage::OrphanStoreMirror => {
                    edit_snapshot_image(image_dir.path(), image, |connection| {
                        connection
                            .replace_first_circle_audience(None)
                            .expect("replace Circle mirror with orphan Store audience");
                    })
                }
            };
            let coverage = CommitFrontier(BTreeMap::new());
            device
                .publish_snapshot(image, coverage.clone())
                .await
                .expect("publish scoped snapshot");
            device
                .publish_acknowledgement(coverage)
                .await
                .expect("publish scoped snapshot acknowledgement");

            let (store_dir_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
            Self {
                source,
                store,
                membership,
                _store_dir_temp: store_dir_temp,
                store_dir,
            }
        }

        async fn open<'storage>(
            &'storage self,
            database_path: &Path,
        ) -> Result<crate::sync::store::RestoringStore<'storage>, SnapshotError> {
            let restorer_identity = crate::keys::UserKeypair::generate();
            let bootstrap = self
                .store
                .prepare_snapshot_bootstrap(
                    &crate::joining::MembershipFloor(self.membership.head_refs().to_vec()),
                    1,
                    database_path,
                    &restorer_identity,
                )
                .await?;
            let routing = crate::encryption::EncryptionService::from_key([42; 32]);
            bootstrap
                .install(
                    &self.store_dir,
                    self.source.synced_tables().to_vec(),
                    crate::blob::BLOB_TOMBSTONE_GRACE,
                    crate::blob::TransferLimits::one_at_a_time(),
                    "joining-device".to_string(),
                    std::sync::Arc::new(crate::clock::SystemClock),
                    &crate::sync::test_helpers::test_migrations(),
                    Some(&routing),
                )
                .await
        }
    }

    fn edit_snapshot_image(
        _image_dir: &Path,
        image: Vec<u8>,
        edit: impl FnOnce(&crate::database::DatabaseImageTest),
    ) -> Vec<u8> {
        let connection = crate::database::DatabaseImageTest::from_bytes(&image)
            .expect("open edited snapshot image");
        edit(&connection);
        connection
            .into_bytes()
            .expect("serialize edited snapshot image")
    }

    #[tokio::test]
    async fn snapshot_preserves_authenticated_routes_for_every_scoped_row() {
        let source = open_scoped_snapshot_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-authenticated-routes",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create scoped snapshot Store");
        seed_scoped_snapshot_rows(&source).await;

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(
                root,
                image_path,
                Some(crate::encryption::EncryptionService::from_key([42; 32])),
            )
            .await
            .expect("create scoped snapshot image");
        let inspected = crate::database::DatabaseImageTest::from_bytes(&image)
            .expect("open inspected scoped snapshot");
        let routes = inspected
            .coven_table_row_count(crate::database::DatabaseTestTable::named(
                "_coven_row_routes",
            ))
            .expect("count snapshot private routes");
        let mirrors = inspected
            .coven_table_row_count(crate::database::DatabaseTestTable::named("_coven_audience"))
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
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-circle-projection",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create Circle snapshot Store");
        let circle_id = seed_scoped_snapshot_rows(&source).await;

        let image_dir = tempfile::tempdir().expect("Circle snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_circle_snapshot_image_for_test(
                root,
                image_path,
                crate::encryption::EncryptionService::from_key([42; 32]),
                circle_id,
            )
            .await
            .expect("create Circle snapshot image");
        let inspected = crate::database::DatabaseImageTest::from_bytes(&image)
            .expect("open inspected Circle snapshot");
        let materialized = inspected
            .query_row(
                "SELECT
                     (SELECT group_concat(body, ',') FROM documents),
                     (SELECT group_concat(body, ',') FROM paragraphs)",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .expect("inspect Circle snapshot rows");
        assert_eq!(
            materialized,
            (
                "Circle document".to_string(),
                "Circle paragraph".to_string()
            )
        );
        for (table, expected) in [
            ("_coven_row_routes", 2),
            ("_coven_audience", 2),
            ("circle_current_state", 0),
            ("protocol_state", 0),
            ("remote_objects", 0),
            ("blob_locators", 0),
            ("row_blob_locators", 0),
            ("retained_merge_materializations", 0),
            ("retained_replay_objects", 0),
        ] {
            assert_eq!(
                inspected
                    .coven_table_row_count(crate::database::DatabaseTestTable::named(table))
                    .expect("count Circle snapshot Coven rows"),
                expected,
                "unexpected {table} row count"
            );
        }
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
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-circle-store-parent",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create Circle parent snapshot Store");
        let database = StoreDatabase::new(&source);
        let circle_id = database
            .install_test_active_circle("snapshot-parent-circle".to_string())
            .await
            .expect("install snapshot parent Circle");
        let write_circle_id = circle_id;
        database
            .run_host_store_write_for_test(
                Some(crate::encryption::EncryptionService::from_key([42; 32])),
                None,
                move |transaction| {
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
                            write_circle_id.to_string(),
                            "93c8343e-6a43-4d66-9aba-f275825047ac",
                            "0000000001002-0000-owner",
                        ),
                    )?;
                    Ok(())
                },
            )
            .await
            .expect("commit Circle row with Store parent");

        let image_dir = tempfile::tempdir().expect("Circle parent snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_circle_snapshot_image_for_test(
                root,
                image_path,
                crate::encryption::EncryptionService::from_key([42; 32]),
                circle_id,
            )
            .await
            .expect("create Circle snapshot with Store parent");
        let reference = circle_bootstrap_reference(&source, &image);
        let routing_key = crate::protocol::circle::derive_row_routing_key(
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
        let inspected = crate::database::DatabaseImageTest::from_bytes(&image)
            .expect("open inspected Circle parent snapshot");
        let rows = inspected
            .query_row(
                "SELECT
                     (SELECT group_concat(name, ',') FROM folders),
                     (SELECT group_concat(body, ',') FROM documents),
                     (SELECT COUNT(*) FROM pragma_foreign_key_check)",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .expect("inspect Circle parent snapshot rows");
        assert_eq!(
            rows,
            ("kept".to_string(), "Circle document".to_string(), 0,)
        );
        assert_eq!(
            inspected
                .coven_table_row_count(crate::database::DatabaseTestTable::named(
                    "_coven_row_routes",
                ))
                .expect("count Circle parent routes"),
            1
        );
        assert_eq!(
            inspected
                .coven_table_row_count(
                    crate::database::DatabaseTestTable::named("_coven_audience",)
                )
                .expect("count Circle parent audiences"),
            1
        );
    }

    #[tokio::test]
    async fn snapshot_refuses_an_unauthenticated_live_private_route() {
        let source = open_scoped_snapshot_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-invalid-live-route",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create invalid-live-route Store");
        seed_scoped_snapshot_rows(&source).await;
        source
            .test_sql(|connection| {
                connection.corrupt_live_document_route_id("01890a5d-ac96-774b-bcce-b302099c3f74")
            })
            .await
            .expect("corrupt live private route");

        let image_dir = tempfile::tempdir().expect("invalid-live-route snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let result = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(
                root,
                image_path,
                Some(crate::encryption::EncryptionService::from_key([42; 32])),
            )
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
        let fixture = PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::Valid).await;
        let destination = tempfile::tempdir().expect("valid-route bootstrap destination");
        let database_path = destination.path().join("store.db");
        let database = fixture
            .open(&database_path)
            .await
            .expect("open valid scoped snapshot");
        let counts = database
            .scoped_snapshot_counts_for_test()
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
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            store_id,
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create scoped migration Store");
        let device = store
            .open_into(&source)
            .await
            .expect("load scoped migration membership");
        let membership = device
            .membership_for_test()
            .await
            .expect("project scoped migration membership");
        StoreDatabase::new(&source)
            .run_host_store_write_for_test(
                Some(crate::encryption::EncryptionService::from_key([42; 32])),
                None,
                move |transaction| {
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
            .await
            .expect("commit pre-migration scoped row");

        let image_dir = tempfile::tempdir().expect("scoped migration snapshot directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(
                root,
                image_path,
                Some(crate::encryption::EncryptionService::from_key([42; 32])),
            )
            .await
            .expect("create pre-migration scoped snapshot");
        let coverage = CommitFrontier(BTreeMap::new());
        device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish pre-migration scoped snapshot");
        device
            .publish_acknowledgement(coverage)
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
        let bootstrap = store
            .prepare_snapshot_bootstrap(
                &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
                2,
                &database_path,
                &signer,
            )
            .await
            .expect("verify pre-migration scoped snapshot");
        let routing = crate::encryption::EncryptionService::from_key([42; 32]);
        let store_dir = crate::store_dir::StoreDir::new(destination.path());
        let database = bootstrap
            .install(
                &store_dir,
                target_tables,
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "joining-device".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
                &target_migrations,
                Some(&routing),
            )
            .await
            .expect("migrate and validate scoped snapshot");
        assert_eq!(database.schema_version_for_test(), 2);
        let migrated = database
            .migrated_scoped_snapshot_facts_for_test()
            .await
            .expect("inspect migrated scoped snapshot");
        assert_eq!(migrated, (1, 1, "ordinary".to_string()));
    }

    #[tokio::test]
    async fn bootstrap_rejects_a_signed_snapshot_with_an_unauthenticated_private_route() {
        let store_id = "snapshot-invalid-private-route";
        let fixture =
            PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::UnauthenticatedRoute)
                .await;
        let destination = tempfile::tempdir().expect("route-tamper bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = fixture.open(&database_path).await;
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
        let fixture =
            PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::CircleRow).await;
        let destination = tempfile::tempdir().expect("Circle-row bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = fixture.open(&database_path).await;
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
            PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::InvalidCircleMirror)
                .await;
        let destination = tempfile::tempdir().expect("invalid-mirror bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = fixture.open(&database_path).await;
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
            PublishedScopedSnapshot::publish(store_id, ScopedSnapshotImage::OrphanStoreMirror)
                .await;
        let destination = tempfile::tempdir().expect("orphan-mirror bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = fixture.open(&database_path).await;
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
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create device-state snapshot Store");
        let loaded_store = store
            .bind_device(&source, &signer)
            .await
            .expect("load device-state snapshot Store");
        let mut writer = loaded_store
            .authorize_writer()
            .await
            .expect("authorize device-state snapshot writer");
        for sequence in 1..=3 {
            source
                .execute_test_host_write(&format!(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('snapshot-state-{sequence}', 'state', NULL, 1, \
                             '000000000100{sequence}-0000-state', '2026-07-21')"
                ))
                .await;
            assert!(writer
                .prepare_pending_store_write()
                .await
                .expect("prepare snapshot history write"));
            assert_eq!(
                writer
                    .drain_store_writes()
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
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(root, image_path, None)
            .await
            .expect("create scoped snapshot image");
        let scoped = crate::database::DatabaseImageTest::from_bytes(&image)
            .expect("open scoped snapshot image");
        let actual = scoped
            .store_device_state_snapshot_refs()
            .expect("read scoped device states")
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn bootstrap_installs_the_verified_exact_store_root() {
        Box::pin(async {
            let source = crate::sync::test_helpers::open_test_db();
            let signer = UserKeypair::generate();
            let store = crate::sync::test_helpers::TestStore::create(
                &source,
                "snapshot-bootstrap-exact-root",
                signer.clone(),
                crate::sync::test_helpers::test_cloud_home(),
            )
            .await
            .expect("create exact bootstrap Store");
            let device = store
                .open_into(&source)
                .await
                .expect("open bootstrap Store membership");
            let membership = device
                .membership_for_test()
                .await
                .expect("project bootstrap Store membership");
            let image_dir = tempfile::tempdir().expect("snapshot image directory");
            let image_path = image_dir.path().to_path_buf();
            let tables = crate::sync::test_helpers::test_synced_tables();
            let root = store.root.clone();
            let image = StoreDatabase::new(&source)
                .capture_snapshot_image_for_test(root, image_path, None)
                .await
                .expect("create bootstrap database image");
            let published_snapshot = device
                .publish_snapshot(image, CommitFrontier(BTreeMap::new()))
                .await
                .expect("publish bootstrap database image");
            device
                .stage_acknowledgement(
                    CommitFrontier(BTreeMap::new()),
                    "2026-07-16T00:00:01Z".to_string(),
                )
                .await
                .expect("stage snapshot stability acknowledgement");
            device
                .drain_acknowledgements()
                .await
                .expect("activate snapshot stability acknowledgement");

            let destination = tempfile::tempdir().expect("bootstrap destination");
            let database_path = destination.path().join("store.db");
            let bootstrap = store
                .prepare_snapshot_bootstrap(
                    &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
                    1,
                    &database_path,
                    &signer,
                )
                .await
                .expect("verify bootstrap authority");
            let store_dir = crate::store_dir::StoreDir::new(destination.path());
            let installed = bootstrap
                .install(
                    &store_dir,
                    tables,
                    crate::blob::BLOB_TOMBSTONE_GRACE,
                    crate::blob::TransferLimits::one_at_a_time(),
                    "joining-device".to_string(),
                    std::sync::Arc::new(crate::clock::SystemClock),
                    &crate::sync::test_helpers::test_migrations(),
                    None,
                )
                .await
                .expect("install bootstrap authority");

            assert_eq!(
                installed
                    .installed_store_root_for_test()
                    .await
                    .expect("read installed Store root"),
                Some(store.root.clone()),
            );
            let baseline = installed
                .generation_zero_replay_baseline_for_test()
                .await
                .expect("load installed snapshot replay baseline");
            assert_eq!(baseline.exact_cut, published_snapshot.coverage);
            match &baseline.authority {
                crate::database::RetainedReplayAuthority::StableSnapshot(authority) => {
                    assert_eq!(authority.store_root, store.root);
                    assert_eq!(authority.metadata, published_snapshot);
                }
                crate::database::RetainedReplayAuthority::Genesis(_) => {
                    panic!("snapshot bootstrap installed a genesis replay baseline")
                }
            }
            baseline
                .validate_image()
                .expect("validate snapshot replay baseline");
            let mut tampered = baseline.authority.clone();
            let crate::database::RetainedReplayAuthority::StableSnapshot(authority) = &mut tampered
            else {
                panic!("snapshot bootstrap installed a genesis replay baseline")
            };
            authority.metadata.signature = "00".repeat(64);
            authority
                .validate()
                .expect_err("retained snapshot authority must re-open its signed metadata");
            let authority_bytes =
                serde_json::to_vec(&tampered).expect("serialize tampered authority");
            installed
                .replace_generation_zero_replay_authority_for_test(authority_bytes)
                .await
                .expect("tamper retained snapshot metadata");
            installed
                .generation_zero_replay_baseline_for_test()
                .await
                .expect_err(
                    "restart must reject retained snapshot metadata with another signature",
                );
        })
        .await;
    }

    #[tokio::test]
    async fn bootstrap_refuses_an_owner_snapshot_without_stability_acknowledgements() {
        let source = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "snapshot-bootstrap-requires-stability",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create unstable bootstrap Store");
        let device = store
            .open_into(&source)
            .await
            .expect("open unstable bootstrap Store membership");
        let membership = device
            .membership_for_test()
            .await
            .expect("project unstable bootstrap Store membership");
        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(root, image_path, None)
            .await
            .expect("create unstable bootstrap database image");
        device
            .publish_snapshot(image, CommitFrontier(BTreeMap::new()))
            .await
            .expect("publish unstable bootstrap database image");

        let destination = tempfile::tempdir().expect("bootstrap destination");
        let database_path = destination.path().join("store.db");
        let result = store
            .prepare_snapshot_bootstrap(
                &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
                1,
                &database_path,
                &signer,
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
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create snapshot materialization Store");
        let changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('snapshot-row', 'Snapshot', 1, \
                         '0000000001000-0000-snapshot', '2026-01-01')",
            ])
            .await;
        store
            .publish_changeset("snapshot", 1, &changeset, 1)
            .await
            .expect("publish snapshot materialization fixture");
        let live_counts = source
            .test_sql(|database| {
                Ok((
                    database.table_row_count(crate::database::DatabaseTestTable::named(
                        "materialized_commits",
                    ))?,
                    database.table_row_count(crate::database::DatabaseTestTable::named(
                        "retained_merge_materializations",
                    ))?,
                    database.table_row_count(crate::database::DatabaseTestTable::named(
                        "retained_replay_objects",
                    ))?,
                ))
            })
            .await
            .expect("count live materialization graph");
        assert!(live_counts.0 > 0);
        assert!(live_counts.1 > 0);
        assert!(live_counts.2 > 0);

        let image_dir = tempfile::tempdir().expect("snapshot image directory");
        let image_path = image_dir.path().to_path_buf();
        let root = store.root.clone();
        let image = StoreDatabase::new(&source)
            .capture_snapshot_image_for_test(root, image_path, None)
            .await
            .expect("create materialization snapshot");
        let snapshot = crate::database::DatabaseImageTest::from_bytes(&image)
            .expect("open inspected snapshot");
        assert_eq!(
            snapshot
                .materialization_graph_counts()
                .expect("count snapshot materialization graph"),
            (0, 0, 0)
        );
        let foreign_key_violations = snapshot
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("check snapshot materialization foreign keys");
        assert_eq!(foreign_key_violations, 0);
    }

    #[tokio::test]
    async fn snapshot_keeps_the_authenticated_blob_graph_closed() {
        Box::pin(async {
            let declaration = crate::sync::session::BlobDecl::new(
                "photos",
                crate::blob::Provenance::HostProvided,
                crate::blob::CacheFill::CacheEager,
            );
            let source = crate::sync::test_helpers::open_test_db_with_blob(declaration);
            let signer = UserKeypair::generate();
            let home = crate::sync::test_helpers::test_cloud_home();
            let store = crate::sync::test_helpers::TestStore::create(
                &source,
                "snapshot-blob-ownership-graph",
                signer.clone(),
                home.clone(),
            )
            .await
            .expect("create exact blob Store");
            source
                .execute_test_host_write(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at)
             VALUES ('n1', 'Album', 1, '0000000001000-0000-owner', '2026-01-01')",
                )
                .await;
            source
                .execute_test_host_write(&format!(
                    "INSERT INTO note_photos
                 (id, note_id, kind, size, hash, _updated_at, created_at)
                 VALUES ('photo1', 'n1', 'cover', 11, '{}',
                         '0000000001000-0000-owner', '2026-01-01')",
                    crate::blob::content_hash(b"cover-bytes"),
                ))
                .await;
            let (_source_temp, source_dir) = crate::sync::test_helpers::temp_store_dir();
            crate::store_dir::StoreDir::store_local_blob(
                &source_dir,
                "photos",
                "photo1",
                b"cover-bytes",
            )
            .await
            .expect("stage source blob");
            let writer = crate::storage::CloudSyncStorage::new(
                home,
                crate::storage::CloudCipher::Encrypted(
                    crate::encryption::EncryptionService::from_key([42; 32]),
                ),
                crate::storage::BlobPathScheme::Hashed,
                "snapshot-blob-ownership-graph",
                signer.clone(),
            )
            .expect("construct blob writer");
            let components = crate::sync::test_owner_graph::TestOwnerGraph::new(
                StoreDatabase::new(&source),
                source_dir.clone(),
            )
            .prepare_sync(writer, signer)
            .await
            .expect("prepare source blob publication");
            components
                .run_cycle(&crate::clock::SystemClock, None, None)
                .await
                .expect("publish source blob");

            let image_dir = tempfile::tempdir().expect("snapshot image directory");
            let image_path = image_dir.path().to_path_buf();
            let root = store.root.clone();
            let image = StoreDatabase::new(&source)
                .capture_snapshot_image_for_test(root, image_path, None)
                .await
                .expect("create blob snapshot");
            let snapshot = crate::database::DatabaseImageTest::from_bytes(&image)
                .expect("open inspected snapshot");
            let graph = snapshot
                .snapshot_blob_graph()
                .expect("read closed snapshot blob graph");
            assert_eq!(graph.0, "note_photos");
            assert_eq!(graph.1, "photo1");
            assert_eq!(graph.2, "id");
            assert_eq!(graph.3, "0000000001000-0000-owner");
            assert_eq!(graph.4.len(), 64);
            assert_eq!(graph.5.object_id().to_string().len(), 64);
            assert!(
                !serde_json::to_string(&graph.5)
                    .expect("serialize snapshot remote blob")
                    .contains(source_dir.storage_dir().to_string_lossy().as_ref()),
                "snapshot remote blob state must not carry its source StoreDir",
            );
            assert!(matches!(
                graph.5.bytes().stored(),
                crate::protocol::remote_object::RemoteStoredRepresentation::Blob { .. }
            ));
            for table in ["row_blob_locators", "blob_locators", "remote_objects"] {
                let count = snapshot
                    .coven_table_row_count(crate::database::DatabaseTestTable::named(table))
                    .expect("count snapshot blob ownership table");
                assert_eq!(count, 1, "snapshot carries one {table} row");
            }
            let foreign_key_violations: i64 = snapshot
                .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                    row.get(0)
                })
                .expect("check snapshot blob foreign keys");
            assert_eq!(foreign_key_violations, 0);
        })
        .await;
    }

    fn blob_graph_activation(label: &str) -> crate::protocol::store_commit::StreamActivationId {
        let registration_bytes = format!("{label} snapshot registration");
        let registration = crate::protocol::store_commit::StoreDeviceRegistrationRef {
            device_id: format!("{:0>64}", label.len())
                .parse()
                .expect("valid blob graph test device id"),
            registration_hash: crate::protocol::store_commit::ObjectHash::digest(
                registration_bytes.as_bytes(),
            ),
            object: crate::protocol::objects::ExactObjectRef::new(
                crate::protocol::objects::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/snapshot-registration.json"
                ))
                .expect("valid blob graph registration slot"),
                registration_bytes.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(registration_bytes.as_bytes()),
            ),
        };
        crate::protocol::store_commit::StreamActivation::device_authorized(
            crate::protocol::store_commit::ObjectHash::digest(
                format!("{label} Store root").as_bytes(),
            ),
            registration,
            crate::protocol::store_commit::DeviceStreamAnchor::StoreSnapshots {
                first_slot: crate::protocol::objects::ObjectSlot::logical(format!(
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
    ) -> crate::protocol::audience_package::RowBlobLocatorBinding {
        let plaintext_hash = crate::protocol::store_commit::ObjectHash::digest(bytes);
        let uploader_bytes = b"blob graph test uploader registration";
        let uploader = crate::protocol::store_commit::StoreDeviceRegistrationRef {
            device_id: "aa".repeat(32).parse().expect("valid blob graph device id"),
            registration_hash: crate::protocol::store_commit::ObjectHash::digest(uploader_bytes),
            object: crate::protocol::objects::ExactObjectRef::new(
                crate::protocol::objects::ObjectSlot::logical(
                    "store-v1/devices/blob-graph-test-uploader.json".to_string(),
                )
                .expect("valid blob graph uploader slot"),
                uploader_bytes.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(uploader_bytes),
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
        let slot = crate::protocol::objects::ObjectSlot::logical(locator.semantic_key())
            .expect("valid blob graph object slot");
        let object = crate::protocol::objects::ExactObjectRef::new(
            slot,
            bytes.len() as u64,
            crate::protocol::store_commit::ObjectHash::digest(bytes),
        );
        crate::protocol::audience_package::RowBlobLocatorBinding::new(
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
        let owner = crate::protocol::remote_object::SnapshotObjectOwner {
            activation: blob_graph_activation("conflict"),
            generation: 0,
        };
        let existing = blob_graph_binding(
            "photo-conflict",
            "0000000001000-0000-owner",
            b"existing blob bytes",
        );
        let existing_remote =
            crate::protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
                existing.blob(),
                owner.clone(),
            )
            .expect("activate existing blob graph object");
        {
            let connection = crate::database::DatabaseImageTest::open(&image_path)
                .expect("open blob graph image");
            connection
                .apply_coven_schema()
                .expect("apply blob graph schema");
            connection
                .install_snapshot_blob_binding(&existing, &existing_remote)
                .expect("install existing blob graph binding");
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
            crate::protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
                replacement.blob(),
                owner,
            )
            .expect("activate replacement blob graph object");
        let prepared = crate::database::PreparedSnapshotBlob {
            bindings: vec![replacement],
            authority: crate::protocol::audience_package::PackageAudience::Store,
            remote: replacement_remote,
            spool_path: None,
        };
        let store_dir = crate::store_dir::StoreDir::new(dir.path());
        let error =
            SnapshotDatabaseImage::replace(store_dir.as_ref().join("snapshot-closure.db"), &image)
                .and_then(|image| image.install_blob_graph(&[prepared]))
                .expect_err("a conflicting existing row binding must fail the image install");
        assert!(
            error
                .to_string()
                .contains("already bound to different exact content"),
            "{error}"
        );
    }
}
