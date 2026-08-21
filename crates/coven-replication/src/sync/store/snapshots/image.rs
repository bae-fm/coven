/// Snapshot image creation, Store snapshot bootstrap, and blob installation.
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tracing::info;

use coven_database::Migration;
use coven_database::{Database, SnapshotDatabaseImage};
use coven_protocol::objects::StorageError;
use coven_protocol::synced_schema::SyncedTable;
use coven_storage::CloudSyncObjectStorage;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotSpoolCleanupError {
    #[error("snapshot spool is absent: {}", path.display())]
    Missing { path: PathBuf },
    #[error("snapshot spool file: {0}")]
    File(#[from] coven_foundation::atomic_file::FileError),
}

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
    #[error("snapshot database: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("snapshot control JSON parse failed: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Bucket(#[from] StorageError),
    #[error("Store protocol object error: {0}")]
    StoreObject(#[from] coven_protocol::objects::StoreObjectError),
    #[error("snapshot Store protocol: {0}")]
    StoreProtocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("snapshot JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("snapshot audience package: {0}")]
    AudiencePackage(#[from] coven_protocol::audience_package::AudiencePackageError),
    #[error("snapshot remote object: {0}")]
    RemoteObject(#[from] coven_protocol::remote_object::RemoteObjectRecordError),
    #[error("snapshot row routing key: {0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    #[error("snapshot Circle state: {0}")]
    CircleState(#[from] coven_protocol::circle_activation::CircleStateError),
    #[error("snapshot database open: {0}")]
    DatabaseOpen(#[from] coven_database::OpenError),
    #[error("Store history: {0}")]
    StoreHistory(#[from] crate::sync::store::pull::StorePullError),
    /// The snapshot's author is not authorized to publish a catalog image: not a
    /// current Owner of the store's membership chain, or the
    /// chain itself is not anchored to the store's owner (a wiped/refounded
    /// chain). The snapshot is refused rather than adopted.
    #[error("snapshot author is not an authorized owner: {0}")]
    UnauthorizedAuthor(String),
    /// The snapshot's synced-schema version is newer than this binary's top
    /// migration, so its DB image carries columns this binary's tables lack. The
    /// generation is refused before its image is downloaded; the same refusal is
    /// the at-open backstop reported as
    /// [`coven_database::MigrationError::SchemaTooNew`].
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
    #[error("snapshot timestamp: {0}")]
    Timestamp(#[from] chrono::ParseError),
    #[error("snapshot publication Store: {0}")]
    PublicationStore(#[source] Box<crate::sync::store::StoreError>),
    #[error("snapshot writer authorization: {0}")]
    WriterAuthorization(#[source] Box<crate::sync::store::StoreWriterAuthorizationError>),
    #[error("snapshot Circle operation: {0}")]
    CircleOperation(#[source] Box<crate::sync::store::circles::CircleOperationError>),
    #[error("snapshot membership chain: {0}")]
    AnchoredChain(#[source] Box<crate::sync::store::AnchoredChainError>),
    #[error("snapshot acknowledgement: {0}")]
    Acknowledgement(#[source] Box<crate::sync::store::StoreAckError>),
    #[error("snapshot spool cleanup: {0}")]
    SpoolCleanup(#[from] SnapshotSpoolCleanupError),
    #[error("snapshot download was cancelled")]
    Cancelled,
    #[error("snapshot operation failed: {cause}; spool cleanup also failed: {cleanup}")]
    SpoolCleanupAfterFailure {
        cause: Box<SnapshotError>,
        cleanup: SnapshotSpoolCleanupError,
    },
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

impl From<crate::sync::store::StoreError> for SnapshotError {
    fn from(error: crate::sync::store::StoreError) -> Self {
        Self::PublicationStore(Box::new(error))
    }
}

impl From<crate::sync::store::StoreWriterAuthorizationError> for SnapshotError {
    fn from(error: crate::sync::store::StoreWriterAuthorizationError) -> Self {
        Self::WriterAuthorization(Box::new(error))
    }
}

impl From<crate::sync::store::circles::CircleOperationError> for SnapshotError {
    fn from(error: crate::sync::store::circles::CircleOperationError) -> Self {
        Self::CircleOperation(Box::new(error))
    }
}

impl From<crate::sync::store::AnchoredChainError> for SnapshotError {
    fn from(error: crate::sync::store::AnchoredChainError) -> Self {
        Self::AnchoredChain(Box::new(error))
    }
}

impl From<crate::sync::store::StoreAckError> for SnapshotError {
    fn from(error: crate::sync::store::StoreAckError) -> Self {
        Self::Acknowledgement(Box::new(error))
    }
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

/// SHA-256 of a snapshot DB image, hex-encoded for durable bootstrap state.
fn snapshot_db_hash(db_image: &[u8]) -> String {
    hex::encode(Sha256::digest(db_image))
}

async fn download_snapshot_image(
    storage: &dyn CloudSyncObjectStorage,
    root: &coven_protocol::store_commit::StoreRootRef,
    snapshot: &coven_database::PublishedStoreSnapshot,
    on_progress: &crate::sync::JoiningDeviceJoinProgressObserver,
    cancel: &tokio::sync::watch::Receiver<bool>,
) -> Result<Vec<u8>, SnapshotError> {
    if *cancel.borrow() {
        return Err(SnapshotError::Cancelled);
    }
    let bytes_total = snapshot.meta.image.object.stored_size();
    on_progress(
        crate::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
            bytes_done: 0,
            bytes_total,
        },
    );
    let context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        coven_protocol::objects::ProtocolObjectDomain::StoreSnapshotImage,
    );
    let semantic_prefix = coven_protocol::store_commit::snapshot_image_semantic_prefix(
        &snapshot.meta.author_registration.device_id.to_string(),
        snapshot.meta.image.image_hash,
    );
    let mut progress = crate::blob::progress::TransferProgress::new();
    let download = storage.read_protocol_object_with_progress(
        &context,
        &snapshot.meta.image.object,
        &semantic_prefix,
        progress.callback(),
    );
    tokio::pin!(download);
    let mut cancellation = cancel.clone();
    let mut cancellation_open = true;
    let plaintext = loop {
        tokio::select! {
            result = &mut download => break result.map_err(coven_protocol::objects::StoreObjectError::from)?,
            bytes_done = progress.changed() => {
                on_progress(crate::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                    bytes_done,
                    bytes_total,
                });
            }
            changed = cancellation.changed(), if cancellation_open => {
                match changed {
                    Ok(()) if *cancellation.borrow() => return Err(SnapshotError::Cancelled),
                    Ok(()) => {}
                    Err(_) => cancellation_open = false,
                }
            }
        }
    };
    if let Some(bytes_done) = progress.finish(bytes_total) {
        on_progress(
            crate::sync::JoiningDeviceJoinProgress::DownloadingSnapshot {
                bytes_done,
                bytes_total,
            },
        );
    }
    if coven_protocol::store_commit::ObjectHash::digest(&plaintext)
        != snapshot.meta.image.image_hash
    {
        return Err(SnapshotError::Parse(
            "Store snapshot image differs from its exact reference".to_string(),
        ));
    }
    Ok(plaintext)
}

/// A same-provider join's already-verified installation authority together
/// with its downloaded database image. Installing it performs no Store-history
/// discovery and never downloads Circle images.
pub struct PreparedDeviceJoinSnapshot {
    database_image: SnapshotDatabaseImage,
    db_hash: String,
    root: coven_protocol::objects::VerifiedObject<coven_protocol::store_commit::StoreProtocolRoot>,
    founder: coven_protocol::objects::VerifiedObject<
        coven_protocol::store_commit::StoreDeviceRegistration,
    >,
    snapshot: coven_database::PublishedStoreSnapshot,
    authority: coven_database::VerifiedStoreSnapshotAuthority,
    membership: coven_database::InitialStoreMembershipAuthority,
    attempt: coven_protocol::store_commit::DeviceJoinAttempt,
    outcome: coven_protocol::store_commit::DeviceJoinOutcome,
    bootstrap: coven_database::DeviceJoinBootstrapPlan,
}

impl PreparedDeviceJoinSnapshot {
    pub async fn prepare(
        storage: &std::sync::Arc<dyn CloudSyncObjectStorage>,
        installation: coven_protocol::store_commit::device_join_exchange::SamePrincipalStoreInstallation,
        binary_schema_version: u32,
        target_path: &Path,
        on_progress: &crate::sync::JoiningDeviceJoinProgressObserver,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<Self, SnapshotError> {
        let mut timings = coven_foundation::stage_timing::StageTimings::counting(
            "Device join snapshot preparation",
            storage.provider_requests(),
        );
        timings.mark("verify the snapshot authority", || {
            installation.authority.validate()
        })?;
        if installation.metadata.schema_version > binary_schema_version {
            return Err(SnapshotError::SchemaTooNew {
                snapshot_version: installation.metadata.schema_version,
                supported: binary_schema_version,
            });
        }
        let root_ref = installation.authority.store_root.clone();
        let root_bytes = installation.store_root.to_bytes();
        root_ref.object.verify(&root_bytes)?;
        if installation.store_root.object_hash() != root_ref.store_root_hash
            || installation.store_root.descriptor.store_root_id() != root_ref.store_root_id
        {
            return Err(SnapshotError::BootstrapState(
                "device join Store root differs from its exact reference".to_string(),
            ));
        }
        let root_value =
            coven_protocol::store_commit::StoreProtocolRoot::parse_pinned(&root_bytes, &root_ref)?;
        let root = coven_protocol::objects::VerifiedObject {
            value: root_value,
            bytes: root_bytes,
            semantic_hash: root_ref.store_root_hash,
            object: root_ref.object.clone(),
        };
        let founder_reference = installation.bootstrap.founder.reference().clone();
        let carried_founder = installation.bootstrap.founder.value().clone();
        let founder_bytes = carried_founder.to_bytes();
        founder_reference.object.verify(&founder_bytes)?;
        let founder_value = coven_protocol::store_commit::StoreDeviceRegistration::parse_at(
            &founder_bytes,
            &root_ref,
            founder_reference.device_id,
        )?;
        if founder_value != carried_founder {
            return Err(SnapshotError::BootstrapState(
                "device join founder differs from its verified registration".to_string(),
            ));
        }
        let founder = coven_protocol::objects::VerifiedObject {
            value: founder_value,
            bytes: founder_bytes,
            semantic_hash: founder_reference.registration_hash,
            object: founder_reference.object,
        };
        let membership = coven_database::InitialStoreMembershipAuthority {
            head_refs: installation.bootstrap.membership.0.clone(),
        };
        // Every commit in the carried closure is parsed and signature-checked
        // here. The closure holds only the history this snapshot does not
        // cover, so that is the work of the tail rather than of the Store's
        // whole life — the rest arrives inside the image, under the owner's
        // signature over the snapshot metadata validated above.
        let bootstrap = timings.mark("verify the carried history", || {
            coven_database::DeviceJoinBootstrapPlan::from_closure(&root_ref, installation.bootstrap)
        })?;
        let snapshot = coven_database::PublishedStoreSnapshot {
            reference: installation.snapshot,
            successor_slot: installation.metadata.successor.next_slot.clone(),
            meta: installation.metadata,
        };
        let authority =
            coven_database::VerifiedStoreSnapshotAuthority::from_authority(installation.authority)?;
        let plaintext = timings
            .stage(
                "download the snapshot image",
                download_snapshot_image(
                    storage.as_ref(),
                    &root_ref,
                    &snapshot,
                    on_progress,
                    cancel,
                ),
            )
            .await?;
        let database_image = timings.mark("stage the image on disk", || {
            SnapshotDatabaseImage::create(target_path.to_path_buf(), &plaintext)?.canonicalize()
        })?;
        timings.report();
        Ok(Self {
            database_image,
            db_hash: snapshot_db_hash(&plaintext),
            root,
            founder,
            snapshot,
            authority,
            membership,
            attempt: installation.attempt,
            outcome: installation.outcome,
            bootstrap,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn install(
        self,
        synced_tables: Vec<SyncedTable>,
        blob_tombstone_grace: chrono::Duration,
        transfer_limits: coven_protocol::blob::TransferLimits,
        device_id: String,
        clock: coven_foundation::clock::ClockRef,
        migrations: &[Migration],
        routing_encryption: &coven_keys::encryption::EncryptionService,
    ) -> Result<crate::sync::store::InstalledDeviceJoinSnapshot, SnapshotError> {
        let Self {
            database_image,
            db_hash,
            root,
            founder,
            snapshot,
            authority,
            membership,
            attempt,
            outcome,
            bootstrap,
        } = self;
        let bound_path = database_image.path().to_path_buf();
        let result = (|| {
            let database_bytes = std::fs::read(&bound_path)?;
            if snapshot_db_hash(&database_bytes) != db_hash {
                return Err(SnapshotError::BootstrapDatabaseChanged);
            }
            let root_ref = coven_protocol::store_commit::StoreRootRef {
                store_root_id: root.value.descriptor.store_root_id(),
                store_root_hash: root.semantic_hash,
                object: root.object.clone(),
            };
            let install = coven_database::VerifiedSnapshotBootstrapInstall::new(
                snapshot,
                root.clone(),
                founder,
                authority,
                membership,
                Some(routing_encryption),
            )?
            .with_circle_installs(Vec::new());
            let db = Database::open_initialized_store(
                &bound_path,
                &install,
                synced_tables,
                blob_tombstone_grace,
                transfer_limits,
                device_id,
                clock,
                migrations,
            )?;
            Ok(crate::sync::store::InstalledDeviceJoinSnapshot {
                database: coven_database::StoreDatabase::from_database(db),
                root: root_ref,
                verified_root: root,
                attempt,
                outcome,
                bootstrap,
            })
        })();
        match result {
            Ok(installed) => {
                let committed_path = database_image.commit();
                debug_assert_eq!(committed_path, bound_path);
                Ok(installed)
            }
            Err(cause) => database_image
                .finish_operation(Err(cause))
                .map_err(SnapshotError::from),
        }
    }
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
    storage: &'storage std::sync::Arc<dyn CloudSyncObjectStorage>,
    founder_registration: coven_protocol::objects::VerifiedObject<
        coven_protocol::store_commit::StoreDeviceRegistration,
    >,
    restorer_identity: coven_keys::keys::UserKeypair,
    snapshot: coven_database::PublishedStoreSnapshot,
    coverage: coven_protocol::store_commit::CommitFrontier,
    authority: coven_database::VerifiedStoreSnapshotAuthority,
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
        storage: &'storage std::sync::Arc<dyn CloudSyncObjectStorage>,
        mut history_verifier: crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<
            'storage,
        >,
        membership_floor: &coven_protocol::membership::MembershipFloor,
        binary_schema_version: u32,
        target_path: &Path,
        restorer_identity: &coven_keys::keys::UserKeypair,
        on_progress: crate::sync::JoiningDeviceJoinProgressObserver,
        cancel: &tokio::sync::watch::Receiver<bool>,
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
                .map_err(SnapshotError::from)?;
            resolutions.extend(head.body.resolutions.iter().cloned());
            let registration = history_verifier
                .load_registration(&head.body.author_registration)
                .await
                .map_err(SnapshotError::from)?
                .value;
            registrations.insert(head.body.author_registration.clone(), registration);
        }
        let resolutions = resolutions.into_iter().collect::<Vec<_>>();
        let membership = history_verifier
            .load_membership_at_exact_heads(heads, &resolutions)
            .await
            .map_err(SnapshotError::from)?;
        let registrations = registrations
            .into_iter()
            .filter(|(_, registration)| membership.is_owner_now(&registration.author_pubkey))
            .collect::<Vec<_>>();
        let selected =
            Box::pin(history_verifier.select_listed_installable_store_snapshot(&registrations))
                .await
                .map_err(SnapshotError::from)?
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
        let plaintext = download_snapshot_image(
            storage.as_ref(),
            history_verifier.verified_root().reference(),
            &snapshot,
            &on_progress,
            cancel,
        )
        .await?;
        let founder_registration = history_verifier
            .load_founder_registration()
            .await
            .map_err(SnapshotError::from)?;
        let authority = selected.verified;
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
            authority,
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
    /// caches), selecting the Circle images the restored database can use. The
    /// real install then applies the Store image and every selection inside one
    /// transaction, so a partially installed union is never exposed.
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
            authority,
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
                authority,
                coven_database::InitialStoreMembershipAuthority {
                    head_refs: membership.head_refs().to_vec(),
                },
                routing_encryption,
            )
            .map_err(SnapshotError::from)?;

            let circle_installs = match routing_encryption {
                // Circles exist only in a scoped (Circle-routing) Store; without
                // routing encryption there are no Circle images to stage.
                Some(encryption) => {
                    let routing_key = coven_protocol::circle::derive_row_routing_key(
                        encryption,
                        root_ref.store_root_hash,
                    )
                    .map_err(SnapshotError::from)?;
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
                        .map_err(SnapshotError::from)?;
                        let store_database = coven_database::StoreDatabase::from_database(query_db);
                        crate::sync::store::snapshots::CircleSnapshotReader::new(
                            &store_database,
                            storage.as_ref(),
                            &mut history_verifier,
                        )
                        .select_staged_installs(
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
            let install = install.with_circle_installs(circle_installs);
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
            .map_err(SnapshotError::from)?;
            let database = coven_database::StoreDatabase::from_database(db);
            let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
                database.clone(),
                storage.as_ref(),
                root_ref.clone(),
            );
            let keyrings = crate::sync::store::authorization::keyring::StoreKeyrings::new(
                storage.as_ref(),
                root_ref,
            );
            let blob_cache =
                crate::sync::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone());
            Ok(
                crate::sync::store::authorization::history::AuthorizedStoreHistory::from_snapshot(
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

#[cfg(test)]
#[path = "image_tests.rs"]
mod tests;
