//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::apply::{
    apply_changeset_strict_on, resolve_and_apply_changeset_with_schema_on, ValidatedChangeset,
};
use super::conflict::TableSchema;
use super::membership::{MembershipChain, SerialAuthorizationState};
use super::pull::{
    advance_max_updated_at, cache_eager_blobs, download_blobs, introduced_blob_uploads,
    local_blob_cleanup_intents,
};
use super::session::SyncedTable;
use super::storage::{CoordinationError, CoordinationStorage, SyncStorage};
use super::store_commit::{
    serial_head_key, CommitPosition, ObjectHash, StoreBatchCommit, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolError, StoreSerialHead,
    SERIAL_STREAM_ID,
};
use super::store_objects::{
    list_visible_heads, load_commit_slot, load_package, load_registration_ref,
    load_serial_commit_at_position, load_store_protocol_root_at_hash, StoreObjectError,
};
use crate::blob::local_cleanup::{self, LocalBlobCleanupIntent};
use crate::changeset::RowChange;
use crate::database::{Database, DbError};
use crate::store_dir::StoreDir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStorePositionReason {
    MissingCommit,
    MissingPackage,
    MissingDeviceRegistration {
        device_id: String,
        revision: u64,
        registration_hash: ObjectHash,
    },
    MissingPredecessor(CommitPosition),
    MissingDependency {
        device_id: String,
        position: CommitPosition,
    },
    NewerSchema {
        local: u32,
        required: u32,
    },
    Unauthorized,
    InvalidChangeset(String),
    InvalidRowIdentity {
        table: String,
        reason: String,
    },
    BlobDownloadFailed,
    ForeignKeyDependency,
    ConstraintConflict(Vec<String>),
    HashMismatch {
        referenced_device_id: String,
        referenced_position: CommitPosition,
        materialized_hash: ObjectHash,
    },
    InvalidSignature,
    WrongSlot(String),
    ObjectCollision(String),
    ObjectUnreadable {
        key: String,
        detail: String,
    },
    InvalidObject(String),
    HeadAuthorMismatch {
        head_author: String,
        commit_author: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStoreCoordinate {
    Head {
        device_id: String,
        seq: u64,
        head_hash: ObjectHash,
    },
    Commit {
        device_id: String,
        position: CommitPosition,
    },
    Package {
        device_id: String,
        seq: u64,
        package_hash: ObjectHash,
    },
    Dependency {
        dependent_device_id: String,
        dependent_position: CommitPosition,
        required_device_id: String,
        required_position: CommitPosition,
    },
}

impl HeldStoreCoordinate {
    pub fn device_id(&self) -> &str {
        match self {
            Self::Head { device_id, .. }
            | Self::Commit { device_id, .. }
            | Self::Package { device_id, .. } => device_id,
            Self::Dependency {
                dependent_device_id,
                ..
            } => dependent_device_id,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::Head { seq, .. } | Self::Package { seq, .. } => *seq,
            Self::Commit { position, .. } => position.seq,
            Self::Dependency {
                dependent_position, ..
            } => dependent_position.seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldStorePosition {
    pub coordinate: HeldStoreCoordinate,
    pub reason: HeldStorePositionReason,
}

#[derive(Debug)]
pub struct StorePullResult {
    pub changesets_applied: u64,
    pub devices_pulled: u64,
    pub held_positions: Vec<HeldStorePosition>,
    pub visible_heads: Vec<StoreDeviceHead>,
    pub serial_head: Option<StoreSerialHead>,
    pub row_changes: Vec<RowChange>,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
    pub frontier: BTreeMap<String, CommitPosition>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullError {
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("database: {0}")]
    Database(String),
    #[error("membership: {0}")]
    Membership(#[source] StorePullMembershipError),
    #[error("Serial Store: {0}")]
    Serial(String),
    #[error("Serial coordination: {0}")]
    Coordination(#[source] CoordinationError),
    #[error("{0}")]
    BlobDownloads(#[source] super::pull::BlobDownloadFailures),
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullMembershipError {
    #[error("{0}")]
    Object(#[source] StoreObjectError),
    #[error("{0}")]
    Chain(#[source] super::membership_ops::AnchoredChainError),
    #[error("{0}")]
    Message(String),
}

impl From<DbError> for StorePullError {
    fn from(error: DbError) -> Self {
        Self::Database(error.0)
    }
}

#[derive(Clone)]
struct Candidate {
    commit: StoreBatchCommit,
    package: Option<Vec<u8>>,
    registrations: Vec<StoreDeviceRegistration>,
}

struct AuthorizedSerialCommit {
    commit: StoreBatchCommit,
    registrations: Vec<StoreDeviceRegistration>,
    authorization_after: SerialAuthorizationState,
}

enum RegistrationLoadError {
    Missing(StoreDeviceRegistrationRef),
    Object(StoreObjectError),
}

async fn load_commit_registrations(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
) -> Result<Vec<StoreDeviceRegistration>, RegistrationLoadError> {
    let mut registrations = Vec::with_capacity(commit.device_registrations.len());
    for reference in &commit.device_registrations {
        match load_registration_ref(storage, commit.store_root_hash, reference).await {
            Ok(Some(registration)) => registrations.push(registration.value),
            Ok(None) => return Err(RegistrationLoadError::Missing(reference.clone())),
            Err(error) => return Err(RegistrationLoadError::Object(error)),
        }
    }
    Ok(registrations)
}

#[doc(hidden)]
pub struct SerialResolutionCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) package: Option<Vec<u8>>,
    pub(crate) uploads: Vec<(String, String, String)>,
    pub(crate) cleanup: Vec<LocalBlobCleanupIntent>,
    pub(crate) registrations: Vec<StoreDeviceRegistration>,
    pub(crate) authorization_after: SerialAuthorizationState,
}

#[doc(hidden)]
pub struct SerialResolutionPlan {
    pub(crate) head: StoreSerialHead,
    pub(crate) commits: Vec<SerialResolutionCommit>,
}

enum ApplyOutcome {
    Applied(Vec<RowChange>),
    Held(HeldStorePositionReason),
}

/// Discover every visible immutable head, then repeatedly materialize any commit
/// whose exact predecessor and dependency positions are already durable.
#[allow(clippy::too_many_arguments)]
pub async fn pull_store_commits(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    our_device_id: &str,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    pull_store_commits_with_coordination(
        db,
        tables,
        storage,
        None,
        store_root_hash,
        our_device_id,
        store_dir,
        membership,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub async fn pull_store_commits_with_coordination(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    store_root_hash: ObjectHash,
    our_device_id: &str,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        return pull_serial_store_commits(
            db,
            tables,
            storage,
            serial_coordination.ok_or_else(|| {
                StorePullError::Serial("coordination capability is absent".to_string())
            })?,
            store_root_hash,
            store_dir,
        )
        .await;
    }
    let head_listing = list_visible_heads(storage, store_root_hash).await?;
    let mut first_failed_head_by_device = BTreeMap::new();
    for failure in head_listing.failures {
        let replace = first_failed_head_by_device
            .get(&failure.device_id)
            .is_none_or(|current: &super::store_objects::StoreHeadFailure| {
                failure.seq < current.seq
            });
        if replace {
            first_failed_head_by_device.insert(failure.device_id.clone(), failure);
        }
    }
    let mut latest_heads = BTreeMap::new();
    for verified in head_listing.heads {
        let device_id = verified.value.device_id.clone();
        if first_failed_head_by_device
            .get(&device_id)
            .is_some_and(|failure| verified.value.slot_sequence() >= failure.seq)
        {
            continue;
        }
        let replace = latest_heads.get(&device_id).is_none_or(
            |current: &super::store_objects::VerifiedCopies<StoreDeviceHead>| {
                verified.value.slot_sequence() > current.value.slot_sequence()
            },
        );
        if replace {
            latest_heads.insert(device_id, verified);
        }
    }
    let visible_heads: Vec<_> = latest_heads
        .values()
        .map(|verified| verified.value.clone())
        .collect();
    let mut held = Vec::new();
    for failure in first_failed_head_by_device.into_values() {
        let reason = held_object_error(failure.error);
        for head_hash in failure.semantic_hashes {
            held.push(HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head {
                    device_id: failure.device_id.clone(),
                    seq: failure.seq,
                    head_hash,
                },
                reason: reason.clone(),
            });
        }
    }
    let mut candidates = BTreeMap::new();
    let coverage = db.snapshot_coverage_frontier().await?;

    for verified_head in latest_heads.into_values() {
        let head_hash = verified_head.semantic_hash;
        let head = verified_head.value;
        let Some(mut expected_position) = head.position.clone() else {
            continue;
        };
        if head.device_id == our_device_id {
            continue;
        }
        loop {
            match position_is_materialized(
                db,
                storage,
                store_root_hash,
                &coverage,
                &head.device_id,
                &expected_position,
            )
            .await?
            {
                MaterializedCheck::Yes => break,
                MaterializedCheck::Missing => {}
                MaterializedCheck::Held(reason) => {
                    held.push(held_commit(&head.device_id, expected_position, reason));
                    break;
                }
            }
            let verified_commit = match load_commit_slot(
                storage,
                store_root_hash,
                &head.device_id,
                expected_position.seq,
            )
            .await
            {
                Ok(Some(commit)) => commit,
                Ok(None) => {
                    held.push(held_commit(
                        &head.device_id,
                        expected_position,
                        HeldStorePositionReason::MissingCommit,
                    ));
                    break;
                }
                Err(error) => {
                    held.push(held_commit(
                        &head.device_id,
                        expected_position,
                        held_object_error(error),
                    ));
                    break;
                }
            };
            let commit = verified_commit.value;
            let commit_hash = commit.commit_hash();
            if commit_hash != expected_position.commit_hash {
                held.push(held_commit(
                    &head.device_id,
                    expected_position.clone(),
                    HeldStorePositionReason::HashMismatch {
                        referenced_device_id: head.device_id.clone(),
                        referenced_position: expected_position,
                        materialized_hash: commit_hash,
                    },
                ));
                break;
            }
            if head.author_pubkey != commit.author_pubkey {
                held.push(HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: head.device_id.clone(),
                        seq: head.slot_sequence(),
                        head_hash,
                    },
                    reason: HeldStorePositionReason::HeadAuthorMismatch {
                        head_author: head.author_pubkey,
                        commit_author: commit.author_pubkey,
                    },
                });
                break;
            }
            let predecessor = commit
                .previous_commit_hash()
                .map(|commit_hash| CommitPosition {
                    seq: commit.seq() - 1,
                    commit_hash,
                });
            if !membership_authorizes(db, storage, membership, &commit).await? {
                held.push(held_commit(
                    &commit.device_id,
                    commit.position(),
                    HeldStorePositionReason::Unauthorized,
                ));
                let Some(predecessor) = predecessor else {
                    break;
                };
                expected_position = predecessor;
                continue;
            }
            if commit
                .store_package
                .as_ref()
                .is_some_and(|package| package.schema_version > db.schema_version())
            {
                let required = commit
                    .store_package
                    .as_ref()
                    .expect("checked Store package")
                    .schema_version;
                held.push(held_commit(
                    &commit.device_id,
                    commit.position(),
                    HeldStorePositionReason::NewerSchema {
                        local: db.schema_version(),
                        required,
                    },
                ));
                let Some(predecessor) = predecessor else {
                    break;
                };
                expected_position = predecessor;
                continue;
            }
            let registrations = match load_commit_registrations(storage, &commit).await {
                Ok(registrations) => registrations,
                Err(RegistrationLoadError::Missing(reference)) => {
                    held.push(held_commit(
                        &commit.device_id,
                        commit.position(),
                        HeldStorePositionReason::MissingDeviceRegistration {
                            device_id: reference.device_id,
                            revision: reference.revision,
                            registration_hash: reference.registration_hash,
                        },
                    ));
                    let Some(predecessor) = predecessor else {
                        break;
                    };
                    expected_position = predecessor;
                    continue;
                }
                Err(RegistrationLoadError::Object(error)) => {
                    held.push(held_commit(
                        &commit.device_id,
                        commit.position(),
                        held_object_error(error),
                    ));
                    let Some(predecessor) = predecessor else {
                        break;
                    };
                    expected_position = predecessor;
                    continue;
                }
            };
            let package = match load_package(storage, &commit).await {
                Ok(Some(package)) => Some(package.value),
                Ok(None) if commit.store_package.is_none() => None,
                Ok(None) => {
                    held.push(held_package(
                        &commit,
                        HeldStorePositionReason::MissingPackage,
                    ));
                    let Some(predecessor) = predecessor else {
                        break;
                    };
                    expected_position = predecessor;
                    continue;
                }
                Err(error) => {
                    held.push(held_package(&commit, held_object_error(error)));
                    let Some(predecessor) = predecessor else {
                        break;
                    };
                    expected_position = predecessor;
                    continue;
                }
            };
            candidates.insert(
                (commit.device_id.clone(), commit.seq()),
                Candidate {
                    commit,
                    package,
                    registrations,
                },
            );
            let Some(predecessor) = predecessor else {
                break;
            };
            expected_position = predecessor;
        }
    }

    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut frontier = db.materialized_frontier().await?;
    let mut applied_devices = BTreeSet::new();
    let mut row_changes = Vec::new();
    let mut changesets_applied = 0_u64;
    let mut asset_downloads_failed = false;
    let mut blocked = BTreeMap::new();

    loop {
        let mut progressed = false;
        let keys: Vec<_> = candidates.keys().cloned().collect();
        for key in keys {
            let candidate = candidates
                .get(&key)
                .expect("candidate key came from the same map");
            match readiness(
                db,
                storage,
                store_root_hash,
                &coverage,
                &frontier,
                &candidate.commit,
            )
            .await?
            {
                Readiness::AlreadyMaterialized => {
                    candidates.remove(&key);
                    blocked.remove(&key);
                    progressed = true;
                }
                Readiness::Held(held_position) => {
                    blocked.insert(key, held_position);
                }
                Readiness::Ready => {
                    let candidate = candidates
                        .remove(&key)
                        .expect("ready candidate remains present");
                    match apply_candidate(db, storage, store_dir, schema.clone(), &candidate)
                        .await?
                    {
                        ApplyOutcome::Applied(changes) => {
                            frontier.insert(
                                candidate.commit.device_id.clone(),
                                candidate.commit.position(),
                            );
                            applied_devices.insert(candidate.commit.device_id);
                            row_changes.extend(changes);
                            changesets_applied =
                                changesets_applied.checked_add(1).ok_or_else(|| {
                                    StorePullError::Database(
                                        "Store apply count exceeded u64".to_string(),
                                    )
                                })?;
                            blocked.remove(&key);
                            progressed = true;
                        }
                        ApplyOutcome::Held(reason) => {
                            if matches!(reason, HeldStorePositionReason::BlobDownloadFailed) {
                                asset_downloads_failed = true;
                            }
                            candidates.insert(key.clone(), candidate);
                            let candidate = candidates
                                .get(&key)
                                .expect("held candidate was restored to the map");
                            blocked.insert(
                                key,
                                held_commit(
                                    &candidate.commit.device_id,
                                    candidate.commit.position(),
                                    reason,
                                ),
                            );
                        }
                    }
                }
            }
        }
        if !progressed {
            break;
        }
    }

    for (_, blocked_position) in blocked {
        held.push(blocked_position);
    }
    held.sort_by(|left, right| {
        (left.coordinate.device_id(), left.coordinate.seq())
            .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
    });
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;

    Ok(StorePullResult {
        changesets_applied,
        devices_pulled: u64::try_from(applied_devices.len()).expect("device count fits in u64"),
        held_positions: held,
        visible_heads,
        serial_head: None,
        row_changes,
        asset_downloads_failed,
        local_blob_cleanup_pending,
        frontier,
    })
}

async fn load_authorized_serial_prefix(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    tip: Option<CommitPosition>,
) -> Result<(Vec<AuthorizedSerialCommit>, SerialAuthorizationState), StorePullError> {
    let root = load_store_protocol_root_at_hash(storage, store_root_hash)
        .await?
        .ok_or_else(|| StorePullError::Serial("Store protocol root is absent".to_string()))?
        .value;
    if root.write_policy != crate::WritePolicy::Serial {
        return Err(StorePullError::Serial(format!(
            "Store protocol root uses {:?}, not Serial",
            root.write_policy
        )));
    }
    let mut expected = tip;
    let mut reverse = Vec::new();
    while let Some(position) = expected {
        let commit = load_serial_commit_at_position(storage, store_root_hash, &position)
            .await?
            .ok_or_else(|| {
                StorePullError::Serial(format!(
                    "commit {} named by the signed head is absent",
                    position.seq
                ))
            })?
            .value;
        expected = commit
            .previous_commit_hash()
            .map(|commit_hash| CommitPosition {
                seq: commit.seq() - 1,
                commit_hash,
            });
        reverse.push(commit);
    }
    reverse.reverse();

    let mut authorization = SerialAuthorizationState::from_founder(store_root_hash, &root.founder)
        .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let mut authorized = Vec::with_capacity(reverse.len());
    for commit in reverse {
        let registrations = load_commit_registrations(storage, &commit)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Missing(reference) => StorePullError::Serial(format!(
                    "commit {} registration {:?}/{} ({}) is absent",
                    commit.seq(),
                    reference.device_id,
                    reference.revision,
                    reference.registration_hash,
                )),
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
            })?;
        authorization = authorization
            .authorize_and_apply_with_registrations(&commit, &registrations)
            .map_err(|error| {
                StorePullError::Serial(format!("commit {} authorization: {error}", commit.seq()))
            })?;
        authorized.push(AuthorizedSerialCommit {
            commit,
            registrations,
            authorization_after: authorization.clone(),
        });
    }
    Ok((authorized, authorization))
}

async fn load_authorized_serial_chain(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    head: &StoreSerialHead,
) -> Result<Vec<AuthorizedSerialCommit>, StorePullError> {
    let (authorized, _) =
        load_authorized_serial_prefix(storage, store_root_hash, head.commit.clone()).await?;
    match (head.commit.as_ref(), authorized.last()) {
        (None, None) => {}
        (Some(position), Some(tip))
            if position == &tip.commit.position()
                && head.tip_write_id.as_ref() == Some(&tip.commit.write_id)
                && head.author_pubkey == tip.commit.author_pubkey => {}
        _ => {
            return Err(StorePullError::Serial(
                "signed head is not bound to its exact tip commit".to_string(),
            ))
        }
    }
    Ok(authorized)
}

pub(crate) async fn load_serial_authorization_at_head(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    head: &StoreSerialHead,
) -> Result<SerialAuthorizationState, StorePullError> {
    let authorized = load_authorized_serial_chain(storage, store_root_hash, head).await?;
    match authorized.last() {
        Some(tip) => Ok(tip.authorization_after.clone()),
        None => load_serial_authorization_at_position(storage, store_root_hash, None).await,
    }
}

pub(crate) struct SerialCycleAuthorization {
    pub authorization: SerialAuthorizationState,
    pub head: Option<CommitPosition>,
    pub visible_activations: Vec<super::wrapped_store_key::WrappedKeyActivation>,
}

pub(crate) async fn load_serial_cycle_authorization(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    store_root_hash: ObjectHash,
) -> Result<SerialCycleAuthorization, StorePullError> {
    let head = match coordination.read_head(serial_head_key()).await {
        Ok(object) => StoreSerialHead::parse(&object.bytes, store_root_hash)
            .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?,
        Err(CoordinationError::NotFound(_)) => {
            return Ok(SerialCycleAuthorization {
                authorization: load_serial_authorization_at_position(
                    storage,
                    store_root_hash,
                    None,
                )
                .await?,
                head: None,
                visible_activations: Vec::new(),
            });
        }
        Err(error) => return Err(StorePullError::Coordination(error)),
    };
    let authorized = load_authorized_serial_chain(storage, store_root_hash, &head).await?;
    let authorization = match authorized.last() {
        Some(tip) => tip.authorization_after.clone(),
        None => load_serial_authorization_at_position(storage, store_root_hash, None).await?,
    };
    let visible_activations = authorized
        .into_iter()
        .map(|commit| {
            super::wrapped_store_key::WrappedKeyActivation::Serial(commit.commit.position())
        })
        .collect();
    Ok(SerialCycleAuthorization {
        authorization,
        head: head.commit,
        visible_activations,
    })
}

pub async fn load_serial_authorization_at_position(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    position: Option<CommitPosition>,
) -> Result<SerialAuthorizationState, StorePullError> {
    let (_, authorization) =
        load_authorized_serial_prefix(storage, store_root_hash, position).await?;
    Ok(authorization)
}

async fn pull_serial_store_commits(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
) -> Result<StorePullResult, StorePullError> {
    let local = db.materialized_frontier().await?.remove(SERIAL_STREAM_ID);
    let head = match coordination.read_head(serial_head_key()).await {
        Ok(object) => Some(
            StoreSerialHead::parse(&object.bytes, store_root_hash)
                .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?,
        ),
        Err(CoordinationError::NotFound(_)) => None,
        Err(error) => return Err(StorePullError::Coordination(error)),
    };
    let Some(head_value) = head.as_ref() else {
        load_serial_authorization_at_position(storage, store_root_hash, None).await?;
        if local.is_some() {
            return Err(StorePullError::Serial(format!(
                "signed head is absent but the durable Serial frontier is {local:?}"
            )));
        }
        return empty_serial_pull_result(db, store_dir, head).await;
    };
    let authorized_chain =
        load_authorized_serial_chain(storage, store_root_hash, head_value).await?;
    let Some(tip) = head_value.commit.clone() else {
        if local.is_some() {
            return Err(StorePullError::Serial(format!(
                "signed head is empty but the durable Serial frontier is {local:?}"
            )));
        }
        return empty_serial_pull_result(db, store_dir, head).await;
    };
    if local
        .as_ref()
        .is_some_and(|position| position.seq > tip.seq)
    {
        return Err(StorePullError::Serial(format!(
            "local Serial position is ahead of the signed head: local={local:?}, head={tip:?}"
        )));
    }
    if local
        .as_ref()
        .is_some_and(|position| position.seq == tip.seq && position.commit_hash != tip.commit_hash)
    {
        return Err(StorePullError::Serial(
            "local Serial position forks the signed head".to_string(),
        ));
    }
    let first_unmaterialized = match local.as_ref() {
        None => 0,
        Some(local) => authorized_chain
            .iter()
            .position(|authorized| authorized.commit.position() == *local)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(format!(
                    "Serial predecessor chain does not reach local position {local:?}"
                ))
            })?,
    };
    if let Some(local) = local.as_ref() {
        let authorization = authorized_chain
            .get(first_unmaterialized - 1)
            .expect("materialized Serial position was found in the authorized chain")
            .authorization_after
            .clone();
        db.install_serial_authorization_at_position(local.clone(), authorization)
            .await?;
    }
    let mut candidates = Vec::with_capacity(authorized_chain.len() - first_unmaterialized);
    for authorized in authorized_chain.into_iter().skip(first_unmaterialized) {
        let commit = authorized.commit;
        if commit
            .store_package
            .as_ref()
            .is_some_and(|package| package.schema_version > db.schema_version())
        {
            let package = commit
                .store_package
                .as_ref()
                .expect("checked Store package");
            return Err(StorePullError::Serial(format!(
                "commit {} requires schema {}, local schema is {}",
                commit.seq(),
                package.schema_version,
                db.schema_version()
            )));
        }
        let package = match load_package(storage, &commit).await? {
            Some(package) => Some(package.value),
            None if commit.store_package.is_none() => None,
            None => {
                return Err(StorePullError::Serial(format!(
                    "commit {} Store package is absent",
                    commit.seq()
                )))
            }
        };
        candidates.push((
            Candidate {
                commit,
                package,
                registrations: authorized.registrations,
            },
            authorized.authorization_after,
        ));
    }
    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut row_changes = Vec::new();
    let mut authors = BTreeSet::new();
    let mut applied_candidates = 0_u64;
    for (candidate, authorization_after) in &candidates {
        let changes = match apply_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            candidate,
            authorization_after,
        )
        .await
        {
            Ok(changes) => changes,
            Err(StorePullError::BlobDownloads(failures)) if !failures.has_transport_failure() => {
                tracing::warn!(
                    device_id = %candidate.commit.device_id,
                    seq = candidate.commit.seq(),
                    %failures,
                    "holding Serial commit on blob download failure"
                );
                let frontier = db.materialized_frontier().await?;
                let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
                return Ok(StorePullResult {
                    changesets_applied: applied_candidates,
                    devices_pulled: u64::try_from(authors.len()).map_err(|_| {
                        StorePullError::Serial("author count exceeds u64".to_string())
                    })?,
                    held_positions: vec![held_commit(
                        &candidate.commit.device_id,
                        candidate.commit.position(),
                        HeldStorePositionReason::BlobDownloadFailed,
                    )],
                    visible_heads: Vec::new(),
                    serial_head: head,
                    row_changes,
                    asset_downloads_failed: true,
                    local_blob_cleanup_pending,
                    frontier,
                });
            }
            Err(error) => return Err(error),
        };
        authors.insert(candidate.commit.device_id.clone());
        row_changes.extend(changes);
        applied_candidates = applied_candidates
            .checked_add(1)
            .ok_or_else(|| StorePullError::Serial("apply count exceeds u64".to_string()))?;
    }
    let changesets_applied = applied_candidates;
    let devices_pulled = u64::try_from(authors.len())
        .map_err(|_| StorePullError::Serial("author count exceeds u64".to_string()))?;
    let frontier = db.materialized_frontier().await?;
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(StorePullResult {
        changesets_applied,
        devices_pulled,
        held_positions: Vec::new(),
        visible_heads: Vec::new(),
        serial_head: head,
        row_changes,
        asset_downloads_failed: false,
        local_blob_cleanup_pending,
        frontier,
    })
}

async fn empty_serial_pull_result(
    db: &Database,
    store_dir: &StoreDir,
    serial_head: Option<StoreSerialHead>,
) -> Result<StorePullResult, StorePullError> {
    let frontier = db.materialized_frontier().await?;
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(StorePullResult {
        changesets_applied: 0,
        devices_pulled: 0,
        held_positions: Vec::new(),
        visible_heads: Vec::new(),
        serial_head,
        row_changes: Vec::new(),
        asset_downloads_failed: false,
        local_blob_cleanup_pending,
        frontier,
    })
}

#[doc(hidden)]
pub async fn prepare_serial_resolution(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    branch_base: Option<CommitPosition>,
) -> Result<SerialResolutionPlan, StorePullError> {
    let object = coordination
        .read_head(serial_head_key())
        .await
        .map_err(StorePullError::Coordination)?;
    let head = StoreSerialHead::parse(&object.bytes, store_root_hash)
        .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?;
    let authorized_chain = load_authorized_serial_chain(storage, store_root_hash, &head).await?;
    let mut expected = head.commit.clone();
    let mut reverse = Vec::new();
    while expected != branch_base {
        let position = expected.clone().ok_or_else(|| {
            StorePullError::Serial(
                "global chain ended before the conflicting branch base".to_string(),
            )
        })?;
        if branch_base
            .as_ref()
            .is_some_and(|base| position.seq <= base.seq)
        {
            return Err(StorePullError::Serial(
                "global chain does not descend from the conflicting branch base".to_string(),
            ));
        }
        let authorized = authorized_chain
            .iter()
            .find(|authorized| authorized.commit.position() == position)
            .ok_or_else(|| {
                StorePullError::Serial(format!(
                    "resolution commit {} is outside the authorized global chain",
                    position.seq
                ))
            })?;
        let commit = authorized.commit.clone();
        let authorization_after = authorized.authorization_after.clone();
        if commit
            .store_package
            .as_ref()
            .is_some_and(|package| package.schema_version > db.schema_version())
        {
            let package = commit
                .store_package
                .as_ref()
                .expect("checked Store package");
            return Err(StorePullError::Serial(format!(
                "resolution commit {} requires schema {}, local schema is {}",
                commit.seq(),
                package.schema_version,
                db.schema_version()
            )));
        }
        let package = match load_package(storage, &commit).await? {
            Some(package) => Some(package.value),
            None if commit.store_package.is_none() => None,
            None => {
                return Err(StorePullError::Serial(format!(
                    "resolution commit {} Store package is absent",
                    commit.seq()
                )))
            }
        };
        expected = commit
            .previous_commit_hash()
            .map(|commit_hash| CommitPosition {
                seq: commit.seq() - 1,
                commit_hash,
            });
        reverse.push((
            Candidate {
                commit,
                package,
                registrations: authorized.registrations.clone(),
            },
            authorization_after,
        ));
    }
    reverse.reverse();
    let schema: Arc<TableSchema> = {
        let tables = db.synced_tables().to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut commits = Vec::with_capacity(reverse.len());
    for (candidate, authorization_after) in reverse {
        let prepared =
            prepare_serial_candidate(db, storage, store_dir, schema.clone(), &candidate).await?;
        commits.push(SerialResolutionCommit {
            commit: candidate.commit,
            package: candidate.package,
            uploads: prepared.uploads,
            cleanup: prepared.cleanup,
            registrations: candidate.registrations,
            authorization_after,
        });
    }
    Ok(SerialResolutionPlan { head, commits })
}

async fn apply_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
    authorization_after: &SerialAuthorizationState,
) -> Result<Vec<RowChange>, StorePullError> {
    if candidate.package.is_none() {
        let commit = candidate.commit.clone();
        let registrations = candidate.registrations.clone();
        let authorization_after = authorization_after.clone();
        db.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            Database::record_activated_store_device_registrations_on(&tx, &commit, &registrations)?;
            Database::record_materialized_serial_commit_on(
                &tx,
                &commit,
                &authorization_after.membership,
                authorization_after.key_generation,
            )?;
            tx.commit().map_err(DbError::from)
        })
        .await?;
        return Ok(Vec::new());
    }
    let prepared = prepare_serial_candidate(db, storage, store_dir, schema, candidate).await?;
    let PreparedSerialCandidate {
        changeset,
        changes,
        uploads,
        cleanup,
    } = prepared;
    let commit = candidate.commit.clone();
    let registrations = candidate.registrations.clone();
    let authorization_after = authorization_after.clone();
    let returned_changes = changes.clone();
    let blob_decls = db.blob_decls();
    let receiver_wall_ms = db.receive_wall_ms();
    let mut changeset_max = None;
    advance_max_updated_at(
        &mut changeset_max,
        &changes,
        changeset.schema(),
        receiver_wall_ms,
    );
    let hlc = db.hlc();
    db.call(move |conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        apply_changeset_strict_on(&tx, changeset, &uploads).map_err(|error| {
            DbError(format!(
                "Serial commit {} did not apply exactly: {error}",
                commit.seq()
            ))
        })?;
        for intent in cleanup {
            local_cleanup::record_if_unreferenced_on(&tx, &blob_decls, &intent)?;
        }
        Database::record_activated_store_device_registrations_on(&tx, &commit, &registrations)?;
        Database::record_materialized_serial_commit_on(
            &tx,
            &commit,
            &authorization_after.membership,
            authorization_after.key_generation,
        )?;
        tx.commit().map_err(DbError::from)?;
        if let Some(max_applied) = changeset_max.as_ref() {
            hlc.advance_past(max_applied);
        }
        Ok(())
    })
    .await?;
    Ok(returned_changes)
}

struct PreparedSerialCandidate {
    changeset: ValidatedChangeset<Vec<u8>>,
    changes: Vec<RowChange>,
    uploads: Vec<(String, String, String)>,
    cleanup: Vec<LocalBlobCleanupIntent>,
}

async fn prepare_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
) -> Result<PreparedSerialCandidate, StorePullError> {
    let package = candidate.package.clone().ok_or_else(|| {
        StorePullError::Serial("row preparation requires a Store package".to_string())
    })?;
    let changeset = ValidatedChangeset::new(package, schema)
        .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
    let changes = crate::changeset::walk(changeset.bytes())
        .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
    let old_changes = crate::changeset::walk_old(changeset.bytes())
        .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
    let blob_decls = db.blob_decls();
    let eager = cache_eager_blobs(&blob_decls, &old_changes, &changes)
        .map_err(|error| StorePullError::Serial(format!("invalid blob changes: {error}")))?;
    if let Err(failures) = download_blobs(
        db,
        eager,
        storage,
        store_dir,
        Some(&candidate.commit.author_pubkey),
    )
    .await
    {
        return Err(StorePullError::BlobDownloads(failures));
    }
    let uploads = introduced_blob_uploads(
        &blob_decls,
        &old_changes,
        &changes,
        Some(&candidate.commit.author_pubkey),
    )
    .map_err(|error| StorePullError::Serial(format!("invalid blob changes: {error}")))?;
    let cleanup = local_blob_cleanup_intents(&blob_decls, &old_changes, &changes)
        .map_err(|error| StorePullError::Serial(format!("invalid blob changes: {error}")))?;
    Ok(PreparedSerialCandidate {
        changeset,
        changes,
        uploads,
        cleanup,
    })
}

async fn membership_authorizes(
    db: &Database,
    storage: &dyn SyncStorage,
    membership: Option<&MembershipChain>,
    commit: &StoreBatchCommit,
) -> Result<bool, StorePullError> {
    let Some(chain) = membership else {
        return Ok(true);
    };
    let Some(grant) = commit.membership_grant.as_ref() else {
        return Ok(false);
    };
    if chain.authorizes_write_at(grant, &commit.author_pubkey) {
        return Ok(true);
    }
    let owner = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?;
    let entries = super::membership_ops::list_membership_entries(storage)
        .await
        .map_err(|error| StorePullError::Membership(StorePullMembershipError::Object(error)))?;
    let refreshed = super::membership_ops::load_anchored_chain_with_candidates(
        storage,
        &entries,
        std::slice::from_ref(grant),
        owner.as_deref(),
        Some(db),
    )
    .await
    .map_err(|error| StorePullError::Membership(StorePullMembershipError::Chain(error)))?;
    Ok(refreshed
        .is_some_and(|refreshed| refreshed.authorizes_write_at(grant, &commit.author_pubkey)))
}

enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
}

enum MaterializedCheck {
    Yes,
    Missing,
    Held(HeldStorePositionReason),
}

fn held_object_error(error: StoreObjectError) -> HeldStorePositionReason {
    match error {
        StoreObjectError::CandidateUnreadable { key, source } => {
            HeldStorePositionReason::ObjectUnreadable {
                key,
                detail: source.to_string(),
            }
        }
        StoreObjectError::SemanticFork { slot, hashes } => {
            HeldStorePositionReason::ObjectCollision(format!(
                "semantic slot {slot} contains hashes {hashes:?}"
            ))
        }
        StoreObjectError::Collision {
            semantic_prefix,
            key,
            reason,
        } => HeldStorePositionReason::ObjectCollision(format!(
            "candidate {key} under {semantic_prefix}: {reason}"
        )),
        StoreObjectError::InvalidCandidate { source, .. } => match *source {
            StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
            StoreProtocolError::RelocatedSlot { .. }
            | StoreProtocolError::RelocatedPackage { .. }
            | StoreProtocolError::StoreRootMismatch { .. }
            | StoreProtocolError::StoreMismatch { .. }
            | StoreProtocolError::FounderMismatch { .. } => {
                HeldStorePositionReason::WrongSlot(source.to_string())
            }
            StoreProtocolError::ObjectHashMismatch { .. }
            | StoreProtocolError::PackageHashMismatch { .. } => {
                HeldStorePositionReason::InvalidObject(source.to_string())
            }
            source => HeldStorePositionReason::InvalidObject(source.to_string()),
        },
        error => HeldStorePositionReason::ObjectUnreadable {
            key: "Store object slot".to_string(),
            detail: error.to_string(),
        },
    }
}

async fn readiness(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    coverage: &BTreeMap<String, CommitPosition>,
    frontier: &BTreeMap<String, CommitPosition>,
    commit: &StoreBatchCommit,
) -> Result<Readiness, StorePullError> {
    if let Some(current) = frontier.get(&commit.device_id) {
        if commit.seq() <= current.seq {
            match position_is_materialized(
                db,
                storage,
                store_root_hash,
                coverage,
                &commit.device_id,
                &commit.position(),
            )
            .await?
            {
                MaterializedCheck::Yes => return Ok(Readiness::AlreadyMaterialized),
                MaterializedCheck::Missing => {}
                MaterializedCheck::Held(reason) => {
                    return Ok(Readiness::Held(held_commit(
                        &commit.device_id,
                        commit.position(),
                        reason,
                    )))
                }
            }
            return Ok(Readiness::Held(held_commit(
                &commit.device_id,
                commit.position(),
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: commit.device_id.clone(),
                    referenced_position: commit.position(),
                    materialized_hash: current.commit_hash,
                },
            )));
        }
        if commit.seq() != current.seq + 1
            || commit.previous_commit_hash() != Some(current.commit_hash)
        {
            return Ok(Readiness::Held(held_commit(
                &commit.device_id,
                commit.position(),
                HeldStorePositionReason::MissingPredecessor(CommitPosition {
                    seq: commit.seq() - 1,
                    commit_hash: commit
                        .previous_commit_hash()
                        .expect("verified non-initial commit has a predecessor"),
                }),
            )));
        }
    } else if commit.seq() != 1 || commit.previous_commit_hash().is_some() {
        return Ok(Readiness::Held(held_commit(
            &commit.device_id,
            commit.position(),
            HeldStorePositionReason::MissingPredecessor(CommitPosition {
                seq: commit.seq() - 1,
                commit_hash: commit
                    .previous_commit_hash()
                    .expect("verified non-initial commit has a predecessor"),
            }),
        )));
    }

    for (device_id, position) in commit.merge_dependencies().map_err(|error| {
        StorePullError::Database(format!("MergeConcurrent commit order: {error}"))
    })? {
        match position_is_materialized(db, storage, store_root_hash, coverage, device_id, position)
            .await?
        {
            MaterializedCheck::Yes => {}
            MaterializedCheck::Missing => {
                return Ok(Readiness::Held(held_dependency(
                    commit,
                    device_id,
                    position,
                    HeldStorePositionReason::MissingDependency {
                        device_id: device_id.clone(),
                        position: position.clone(),
                    },
                )))
            }
            MaterializedCheck::Held(reason) => {
                return Ok(Readiness::Held(held_dependency(
                    commit, device_id, position, reason,
                )))
            }
        }
    }
    Ok(Readiness::Ready)
}

async fn position_is_materialized(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    coverage: &BTreeMap<String, CommitPosition>,
    device_id: &str,
    position: &CommitPosition,
) -> Result<MaterializedCheck, DbError> {
    if let Some(actual) = db.exact_materialized_hash(device_id, position.seq).await? {
        if actual != position.commit_hash {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: device_id.to_string(),
                    referenced_position: position.clone(),
                    materialized_hash: actual,
                },
            ));
        }
        return Ok(MaterializedCheck::Yes);
    }
    let Some(covered) = coverage.get(device_id) else {
        return Ok(MaterializedCheck::Missing);
    };
    if position.seq > covered.seq {
        return Ok(MaterializedCheck::Missing);
    }
    let mut seq = covered.seq;
    let mut expected = covered.commit_hash;
    while seq > position.seq {
        let commit = match load_commit_slot(storage, store_root_hash, device_id, seq).await {
            Ok(Some(commit)) => commit,
            Ok(None) => return Ok(MaterializedCheck::Missing),
            Err(error) => return Ok(MaterializedCheck::Held(held_object_error(error))),
        };
        if commit.value.commit_hash() != expected {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: device_id.to_string(),
                    referenced_position: CommitPosition {
                        seq,
                        commit_hash: expected,
                    },
                    materialized_hash: commit.value.commit_hash(),
                },
            ));
        }
        expected = commit
            .value
            .previous_commit_hash()
            .expect("verified retained commit above sequence one has a predecessor");
        seq -= 1;
    }
    if expected != position.commit_hash {
        return Ok(MaterializedCheck::Held(
            HeldStorePositionReason::HashMismatch {
                referenced_device_id: device_id.to_string(),
                referenced_position: position.clone(),
                materialized_hash: expected,
            },
        ));
    }
    Ok(MaterializedCheck::Yes)
}

async fn apply_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
) -> Result<ApplyOutcome, StorePullError> {
    let Some(package) = candidate.package.clone() else {
        let commit = candidate.commit.clone();
        let registrations = candidate.registrations.clone();
        db.call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            Database::record_activated_store_device_registrations_on(&tx, &commit, &registrations)?;
            Database::record_materialized_commit_on(&tx, &commit)?;
            tx.commit().map_err(DbError::from)
        })
        .await?;
        return Ok(ApplyOutcome::Applied(Vec::new()));
    };
    let changeset = match ValidatedChangeset::new(package, schema) {
        Ok(changeset) => changeset,
        Err(error) => {
            return Ok(ApplyOutcome::Held(match error {
                super::session::ChangesetIdentityError::Row(error) => {
                    HeldStorePositionReason::InvalidRowIdentity {
                        table: error.table().to_string(),
                        reason: error.to_string(),
                    }
                }
                error => HeldStorePositionReason::InvalidChangeset(error.to_string()),
            }))
        }
    };
    let changes = match crate::changeset::walk(changeset.bytes()) {
        Ok(changes) => changes,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let old_changes = match crate::changeset::walk_old(changeset.bytes()) {
        Ok(changes) => changes,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let blob_decls = db.blob_decls();
    let eager = match cache_eager_blobs(&blob_decls, &old_changes, &changes) {
        Ok(eager) => eager,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    if let Err(failures) = download_blobs(
        db,
        eager,
        storage,
        store_dir,
        Some(&candidate.commit.author_pubkey),
    )
    .await
    {
        if failures.has_transport_failure() {
            return Err(StorePullError::BlobDownloads(failures));
        }
        return Ok(ApplyOutcome::Held(
            HeldStorePositionReason::BlobDownloadFailed,
        ));
    }
    let uploads = match introduced_blob_uploads(
        &blob_decls,
        &old_changes,
        &changes,
        Some(&candidate.commit.author_pubkey),
    ) {
        Ok(uploads) => uploads,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let cleanup = match local_blob_cleanup_intents(&blob_decls, &old_changes, &changes) {
        Ok(cleanup) => cleanup,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let outcome = commit_candidate(db, candidate, changes, changeset, uploads, cleanup).await?;
    #[cfg(any(test, feature = "test-utils"))]
    if matches!(outcome, ApplyOutcome::Applied(_)) {
        db.reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
            device_id: candidate.commit.device_id.clone(),
            seq: candidate.commit.seq(),
        })
        .await;
    }
    Ok(outcome)
}

async fn commit_candidate(
    db: &Database,
    candidate: &Candidate,
    changes: Vec<RowChange>,
    changeset: ValidatedChangeset<Vec<u8>>,
    uploads: Vec<(String, String, String)>,
    cleanup: Vec<LocalBlobCleanupIntent>,
) -> Result<ApplyOutcome, StorePullError> {
    let commit = candidate.commit.clone();
    let registrations = candidate.registrations.clone();
    let returned_changes = changes.clone();
    let receiver_wall_ms = db.receive_wall_ms();
    let blob_decls = db.blob_decls();
    let mut changeset_max = None;
    advance_max_updated_at(
        &mut changeset_max,
        &changes,
        changeset.schema(),
        receiver_wall_ms,
    );
    let hlc = db.hlc();
    let outcome = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let apply = resolve_and_apply_changeset_with_schema_on(
                &tx,
                changeset,
                receiver_wall_ms,
                &uploads,
            )?;
            if !apply.constraint_conflict_tables.is_empty() {
                tx.rollback().map_err(DbError::from)?;
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::ConstraintConflict(apply.constraint_conflict_tables),
                ));
            }
            if apply.had_fk_violations {
                tx.rollback().map_err(DbError::from)?;
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::ForeignKeyDependency,
                ));
            }
            for intent in cleanup {
                local_cleanup::record_if_unreferenced_on(&tx, &blob_decls, &intent)?;
            }
            Database::record_activated_store_device_registrations_on(&tx, &commit, &registrations)?;
            Database::record_materialized_commit_on(&tx, &commit)?;
            tx.commit().map_err(DbError::from)?;
            if let Some(max_applied) = changeset_max.as_ref() {
                hlc.advance_past(max_applied);
            }
            Ok(ApplyOutcome::Applied(returned_changes))
        })
        .await?;
    Ok(outcome)
}

fn held_commit(
    device_id: &str,
    position: CommitPosition,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Commit {
            device_id: device_id.to_string(),
            position,
        },
        reason,
    }
}

fn held_package(commit: &StoreBatchCommit, reason: HeldStorePositionReason) -> HeldStorePosition {
    let package = commit
        .store_package
        .as_ref()
        .expect("held Store package is named by the commit");
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Package {
            device_id: commit.device_id.clone(),
            seq: commit.seq(),
            package_hash: package.content_hash,
        },
        reason,
    }
}

fn held_dependency(
    dependent: &StoreBatchCommit,
    required_device_id: &str,
    required_position: &CommitPosition,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Dependency {
            dependent_device_id: dependent.device_id.clone(),
            dependent_position: dependent.position(),
            required_device_id: required_device_id.to_string(),
            required_position: required_position.clone(),
        },
        reason,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::database::StoreWriteBase;
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{CloudHome, SequentialCopyIdGenerator};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::{founder_entry, MemberRole, SerialAuthorizationState};
    use crate::sync::store_commit::{
        store_protocol_root_semantic_prefix, StoreControl, StoreProtocolRoot,
    };
    use crate::sync::store_objects::append_and_verify;
    use crate::sync::store_outbound::{
        drain_store_writes, drain_store_writes_with_coordination, prepare_pending_store_write,
        prepare_pending_store_write_with_coordination,
    };
    use crate::sync::test_helpers::{
        exec, host_exec, open_serial_test_db, open_test_db, open_test_db_schema,
        publish_test_serial_store_protocol_root, query_text, row_exists, temp_store_dir,
        test_migrations, test_synced_tables,
    };

    fn storage(
        home: &InMemoryCloudHome,
        keypair: &UserKeypair,
        copy_source: &str,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "causal-ordering-test",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)))
    }

    fn serial_storage(
        home: &InMemoryCloudHome,
        keypair: &UserKeypair,
        store_id: &str,
        copy_source: &str,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            store_id,
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)))
        .with_test_serial_coordination(Arc::new(home.clone()))
    }

    async fn publish_serial_pending(
        db: &Database,
        storage: &CloudSyncStorage,
        device_id: &str,
        keypair: &UserKeypair,
        store_dir: &StoreDir,
    ) -> u64 {
        assert!(prepare_pending_store_write_with_coordination(
            db,
            storage,
            Some(storage.serial_coordination().unwrap()),
            device_id,
            "2026-01-01T00:00:00Z",
            keypair,
            store_dir,
            None,
            None,
        )
        .await
        .expect("prepare Serial branch"));
        drain_store_writes_with_coordination(
            db,
            storage,
            Some(storage.serial_coordination().unwrap()),
        )
        .await
        .expect("publish Serial branch")
    }

    async fn pull_serial(
        db: &Database,
        storage: &CloudSyncStorage,
        store_root_hash: ObjectHash,
        store_dir: &StoreDir,
    ) -> Result<StorePullResult, StorePullError> {
        pull_store_commits_with_coordination(
            db,
            db.synced_tables(),
            storage,
            Some(storage.serial_coordination().unwrap()),
            store_root_hash,
            "peer",
            store_dir,
            None,
        )
        .await
    }

    struct SerialConflictFixture {
        _local_temp: tempfile::TempDir,
        local_dir: StoreDir,
        storage: CloudSyncStorage,
        local: Database,
        keypair: UserKeypair,
        root: ObjectHash,
        branch: crate::PendingBranch,
        remote_position: CommitPosition,
    }

    async fn serial_conflict_fixture(name: &str) -> SerialConflictFixture {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, name, name);
        let local = open_serial_test_db();
        let root =
            publish_test_serial_store_protocol_root(&local, &storage, name, "local", &keypair)
                .await;
        let remote = open_serial_test_db();
        bind_database(&remote, "remote", root).await;
        host_exec(
            &local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('local-provisional-a', 'local-a', NULL, 1, '0000000001000-0000-local', '2026-01-01')",
        )
        .await;
        host_exec(
            &local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('local-provisional-b', 'local-b', NULL, 1, '0000000001001-0000-local', '2026-01-01')",
        )
        .await;
        host_exec(
            &remote,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('remote-committed', 'remote', NULL, 1, '0000000001002-0000-remote', '2026-01-01')",
        )
        .await;
        let (_remote_temp, remote_dir) = temp_store_dir();
        assert_eq!(
            publish_serial_pending(&remote, &storage, "remote", &keypair, &remote_dir).await,
            1
        );
        let remote_position = remote
            .latest_outbound_store_position()
            .await
            .unwrap()
            .unwrap();
        let loser = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated(format!("{name}-orphan-loser")),
            "orphan-loser".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            1,
            &[],
            &keypair,
        )
        .unwrap();
        let loser_package = loser.store_package.as_ref().expect("Store package");
        append_and_verify(&storage, &loser_package.object_key, ".pkg", &[])
            .await
            .unwrap();
        append_and_verify(
            &storage,
            &crate::sync::store_commit::commit_semantic_prefix(
                SERIAL_STREAM_ID,
                1,
                loser.commit_hash(),
            ),
            ".json",
            &loser.to_bytes(),
        )
        .await
        .unwrap();
        let (local_temp, local_dir) = temp_store_dir();
        assert!(!prepare_pending_store_write_with_coordination(
            &local,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "local",
            "2026-01-01T00:00:00Z",
            &keypair,
            &local_dir,
            None,
            None,
        )
        .await
        .expect("record Serial branch conflict"));
        let branch = local.pending_branches().await.unwrap().unwrap();
        SerialConflictFixture {
            _local_temp: local_temp,
            local_dir,
            storage,
            local,
            keypair,
            root,
            branch,
            remote_position,
        }
    }

    async fn serial_resolution_plan(fixture: &SerialConflictFixture) -> SerialResolutionPlan {
        prepare_serial_resolution(
            &fixture.local,
            &fixture.storage,
            fixture.storage.serial_coordination().unwrap(),
            fixture.root,
            &fixture.local_dir,
            fixture.branch.base.clone(),
        )
        .await
        .expect("prepare verified Serial resolution")
    }

    async fn bind_database(db: &Database, device_id: &str, store_root_hash: ObjectHash) {
        db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
            .await
            .expect("bind local device id");
        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &store_root_hash.to_string(),
        )
        .await
        .expect("bind store protocol root");
    }

    async fn publish_pending(
        db: &Database,
        storage: &CloudSyncStorage,
        device_id: &str,
        keypair: &UserKeypair,
        store_dir: &StoreDir,
    ) -> CommitPosition {
        assert!(prepare_pending_store_write(
            db,
            storage,
            device_id,
            "2026-01-01T00:00:00Z",
            keypair,
            store_dir,
            None,
            None,
        )
        .await
        .expect("stage causal Store commit"));
        assert_eq!(
            drain_store_writes(db, storage)
                .await
                .expect("publish causal Store commit"),
            1
        );
        db.latest_local_store_position()
            .await
            .expect("read published Store position")
            .expect("published Store position exists")
    }

    async fn append_serial_commit(storage: &CloudSyncStorage, commit: &StoreBatchCommit) {
        if let Some(package) = commit.store_package.as_ref() {
            append_and_verify(storage, &package.object_key, ".pkg", &[])
                .await
                .unwrap();
        }
        append_and_verify(
            storage,
            &crate::sync::store_commit::commit_semantic_prefix(
                SERIAL_STREAM_ID,
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            &commit.to_bytes(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn serial_pull_applies_the_exact_global_chain_in_sequence() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-pull-chain", "serial-chain");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-pull-chain",
            "source",
            &keypair,
        )
        .await;
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-chain-a', 'first', NULL, 1, '0000000001000-0000-source', '2026-01-01')",
        )
        .await;
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-chain-b', 'second', NULL, 1, '0000000001001-0000-source', '2026-01-01')",
        )
        .await;
        let (_source_temp, source_dir) = temp_store_dir();
        assert_eq!(
            publish_serial_pending(&source, &storage, "source", &keypair, &source_dir).await,
            2
        );
        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        let (_peer_temp, peer_dir) = temp_store_dir();

        let result = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect("pull exact Serial chain");

        assert_eq!(result.changesets_applied, 2);
        assert!(row_exists(&peer, "SELECT 1 FROM notes WHERE id = 'serial-chain-a'").await);
        assert!(row_exists(&peer, "SELECT 1 FROM notes WHERE id = 'serial-chain-b'").await);
        assert_eq!(
            result
                .frontier
                .get(SERIAL_STREAM_ID)
                .map(|position| position.seq),
            Some(2)
        );
    }

    #[tokio::test]
    async fn serial_membership_and_rotation_activate_at_their_exact_global_commits() {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let member_pubkey = crate::keys::public_key_hex(&member);
        let storage = serial_storage(&home, &owner, "serial-control", "serial-control");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-control",
            "owner-device",
            &owner,
        )
        .await;
        let root_object = load_store_protocol_root_at_hash(&storage, root)
            .await
            .unwrap()
            .unwrap()
            .value;
        let authorization =
            SerialAuthorizationState::from_founder(root, &root_object.founder).unwrap();
        let add = authorization
            .membership
            .signed_set_member(
                &owner,
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                "2026-01-01T00:00:00Z".to_string(),
            )
            .unwrap();
        let add_commit = StoreBatchCommit::signed_with_control(
            root,
            crate::WriteId::from_generated("serial-add-member".to_string()),
            "owner-device".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            Some(StoreControl::SerialMembership { entry: add }),
            1,
            &[],
            &owner,
        )
        .unwrap();
        let after_add = authorization.authorize_and_apply(&add_commit).unwrap();
        let member_commit = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated("serial-member-write".to_string()),
            "member-device".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 2,
                previous_commit_hash: Some(add_commit.commit_hash()),
            },
            None,
            1,
            &[],
            &member,
        )
        .unwrap();
        append_serial_commit(&storage, &add_commit).await;
        append_serial_commit(&storage, &member_commit).await;
        let member_head = StoreSerialHead::signed(
            root,
            Some(member_commit.position()),
            Some(member_commit.write_id.clone()),
            &member,
        )
        .unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .create_head(serial_head_key(), &member_head.to_bytes())
            .await
            .unwrap();

        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        let (_temp, store_dir) = temp_store_dir();
        let pulled = pull_serial(&peer, &storage, root, &store_dir)
            .await
            .unwrap();
        assert_eq!(pulled.changesets_applied, 2);
        assert!(peer
            .serial_membership_state()
            .await
            .unwrap()
            .unwrap()
            .can_write(&member_pubkey));

        let removal = after_add
            .membership
            .signed_remove_member(
                &owner,
                member_pubkey.clone(),
                "2026-01-01T00:00:01Z".to_string(),
            )
            .unwrap();
        let removal_commit = StoreBatchCommit::signed_with_control(
            root,
            crate::WriteId::from_generated("serial-remove-member".to_string()),
            "owner-device".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 3,
                previous_commit_hash: Some(member_commit.commit_hash()),
            },
            None,
            Some(StoreControl::SerialMembershipAndKeyRotation {
                entry: removal,
                generation: 2,
            }),
            1,
            &[],
            &owner,
        )
        .unwrap();
        append_serial_commit(&storage, &removal_commit).await;
        let removal_head = StoreSerialHead::signed(
            root,
            Some(removal_commit.position()),
            Some(removal_commit.write_id.clone()),
            &owner,
        )
        .unwrap();
        let previous_head = storage
            .serial_coordination()
            .unwrap()
            .read_head(serial_head_key())
            .await
            .unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .replace_head(
                serial_head_key(),
                &previous_head.version,
                &removal_head.to_bytes(),
            )
            .await
            .unwrap();

        pull_serial(&peer, &storage, root, &store_dir)
            .await
            .unwrap();
        assert!(!peer
            .serial_membership_state()
            .await
            .unwrap()
            .unwrap()
            .can_write(&member_pubkey));
        assert_eq!(peer.serial_key_generation().await.unwrap(), Some(2));
        assert_eq!(
            peer.materialized_frontier()
                .await
                .unwrap()
                .get(SERIAL_STREAM_ID)
                .map(|position| position.seq),
            Some(3)
        );

        let rejected = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated("serial-removed-member-write".to_string()),
            "member-device".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 4,
                previous_commit_hash: Some(removal_commit.commit_hash()),
            },
            None,
            1,
            &[],
            &member,
        )
        .unwrap();
        append_serial_commit(&storage, &rejected).await;
        let rejected_head = StoreSerialHead::signed(
            root,
            Some(rejected.position()),
            Some(rejected.write_id.clone()),
            &member,
        )
        .unwrap();
        let previous_head = storage
            .serial_coordination()
            .unwrap()
            .read_head(serial_head_key())
            .await
            .unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .replace_head(
                serial_head_key(),
                &previous_head.version,
                &rejected_head.to_bytes(),
            )
            .await
            .unwrap();

        let error = pull_serial(&peer, &storage, root, &store_dir)
            .await
            .expect_err("removed member cannot advance the global chain");
        assert!(error.to_string().contains("not a current writer"));
        assert_eq!(
            peer.materialized_frontier()
                .await
                .unwrap()
                .get(SERIAL_STREAM_ID)
                .map(|position| position.seq),
            Some(3)
        );
        assert_eq!(peer.serial_key_generation().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn serial_notfound_aborts_rows_and_materialized_position_together() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-strict", "serial-strict");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-strict",
            "source",
            &keypair,
        )
        .await;
        exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) VALUES
             ('serial-present', 'source-old', NULL, 1, '0000000001000-0000-source', '2026-01-01'),
             ('serial-missing', 'delete-me', NULL, 1, '0000000001000-0000-source', '2026-01-01')",
        )
        .await;
        host_exec(
            &source,
            "UPDATE notes SET title = 'source-new', _updated_at = '0000000001001-0000-source'
             WHERE id = 'serial-present';
             DELETE FROM notes WHERE id = 'serial-missing';",
        )
        .await;
        let (_source_temp, source_dir) = temp_store_dir();
        assert_eq!(
            publish_serial_pending(&source, &storage, "source", &keypair, &source_dir).await,
            1
        );
        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        exec(
            &peer,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('serial-present', 'peer-old', NULL, 1, '0000000000900-0000-peer', '2026-01-01')",
        )
        .await;
        let (_peer_temp, peer_dir) = temp_store_dir();

        let error = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect_err("unexpected NOTFOUND rejects the Serial commit");

        assert!(error.to_string().contains("did not apply exactly"));
        assert_eq!(
            query_text(&peer, "SELECT title FROM notes WHERE id = 'serial-present'").await,
            "peer-old"
        );
        assert_eq!(
            peer.exact_materialized_hash(SERIAL_STREAM_ID, 1)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn idle_serial_pull_rejects_missing_and_empty_heads_after_materialization() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-idle-head", "serial-idle-head");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-idle-head",
            "source",
            &keypair,
        )
        .await;
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('serial-idle', 'idle', NULL, 1,
                     '0000000001000-0000-source', '2026-01-01')",
        )
        .await;
        let (_source_temp, source_dir) = temp_store_dir();
        assert_eq!(
            publish_serial_pending(&source, &storage, "source", &keypair, &source_dir).await,
            1
        );
        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        let (_peer_temp, peer_dir) = temp_store_dir();
        pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect("materialize the signed Serial head");

        storage
            .serial_coordination()
            .unwrap()
            .delete_probe_head(serial_head_key())
            .await
            .unwrap();
        let missing = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect_err("a missing head cannot authorize an idle durable frontier");
        assert!(missing.to_string().contains("head is absent"));

        let empty = StoreSerialHead::signed(root, None, None, &keypair).unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .create_head(serial_head_key(), &empty.to_bytes())
            .await
            .unwrap();
        let regressed = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect_err("an empty head cannot regress an idle durable frontier");
        assert!(regressed.to_string().contains("head is empty"));
    }

    #[tokio::test]
    async fn serial_snapshot_coverage_rejects_missing_and_empty_heads() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(
            &home,
            &keypair,
            "serial-bootstrap-head",
            "serial-bootstrap-head",
        );
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-bootstrap-head",
            "source",
            &keypair,
        )
        .await;
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('serial-covered', 'covered', NULL, 1,
                     '0000000001000-0000-source', '2026-01-01')",
        )
        .await;
        let (_source_temp, source_dir) = temp_store_dir();
        assert_eq!(
            publish_serial_pending(&source, &storage, "source", &keypair, &source_dir).await,
            1
        );
        let position = source
            .latest_outbound_store_position()
            .await
            .unwrap()
            .unwrap();
        let bootstrap = open_serial_test_db();
        bind_database(&bootstrap, "bootstrap", root).await;
        bootstrap
            .install_bootstrap_state(
                &crate::sync::store_commit::CommitFrontier::Serial(Some(position)),
                ObjectHash::digest(b"bootstrap-snapshot"),
                root,
            )
            .await
            .unwrap();
        let (_bootstrap_temp, bootstrap_dir) = temp_store_dir();

        storage
            .serial_coordination()
            .unwrap()
            .delete_probe_head(serial_head_key())
            .await
            .unwrap();
        let missing = pull_serial(&bootstrap, &storage, root, &bootstrap_dir)
            .await
            .expect_err("bootstrap coverage requires its signed head");
        assert!(missing.to_string().contains("head is absent"));

        let empty = StoreSerialHead::signed(root, None, None, &keypair).unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .create_head(serial_head_key(), &empty.to_bytes())
            .await
            .unwrap();
        let regressed = pull_serial(&bootstrap, &storage, root, &bootstrap_dir)
            .await
            .expect_err("bootstrap coverage cannot be retained under an empty head");
        assert!(regressed.to_string().contains("head is empty"));
    }

    #[tokio::test]
    async fn serial_missing_predecessor_fails_without_advancing() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-missing-prev", "serial-prev");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-missing-prev",
            "source",
            &keypair,
        )
        .await;
        for (id, stamp) in [("serial-prev-a", "1000"), ("serial-prev-b", "1001")] {
            host_exec(
                &source,
                &format!(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('{id}', '{id}', NULL, 1, '000000000{stamp}-0000-source', '2026-01-01')"
                ),
            )
            .await;
        }
        let (_source_temp, source_dir) = temp_store_dir();
        assert_eq!(
            publish_serial_pending(&source, &storage, "source", &keypair, &source_dir).await,
            2
        );
        let listed = storage
            .list_protocol_objects("store-v1/commits/serial/1/")
            .await
            .unwrap();
        for object in listed.objects {
            home.remove_appended_candidate(object.physical());
        }
        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        let (_peer_temp, peer_dir) = temp_store_dir();

        let error = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect_err("missing exact predecessor rejects the chain");
        assert!(error.to_string().contains("commit 1"));
        assert_eq!(
            peer.exact_materialized_hash(SERIAL_STREAM_ID, 1)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn serial_wrong_predecessor_fails_without_replacing_the_local_tip() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-wrong-prev", "serial-wrong");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-wrong-prev",
            "source",
            &keypair,
        )
        .await;
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('serial-base', 'base', NULL, 1, '0000000001000-0000-source', '2026-01-01')",
        )
        .await;
        let (_source_temp, source_dir) = temp_store_dir();
        publish_serial_pending(&source, &storage, "source", &keypair, &source_dir).await;
        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        let (_peer_temp, peer_dir) = temp_store_dir();
        pull_serial(&peer, &storage, root, &peer_dir).await.unwrap();
        let local_tip = peer
            .exact_materialized_hash(SERIAL_STREAM_ID, 1)
            .await
            .unwrap()
            .unwrap();
        let rogue = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated("rogue-serial-2".to_string()),
            "rogue-device".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 2,
                previous_commit_hash: Some(ObjectHash::digest(b"wrong predecessor")),
            },
            None,
            1,
            &[],
            &keypair,
        )
        .unwrap();
        let rogue_package = rogue.store_package.as_ref().expect("Store package");
        append_and_verify(&storage, &rogue_package.object_key, ".pkg", &[])
            .await
            .unwrap();
        append_and_verify(
            &storage,
            &crate::sync::store_commit::commit_semantic_prefix(
                SERIAL_STREAM_ID,
                2,
                rogue.commit_hash(),
            ),
            ".json",
            &rogue.to_bytes(),
        )
        .await
        .unwrap();
        let rogue_head = StoreSerialHead::signed(
            root,
            Some(rogue.position()),
            Some(rogue.write_id.clone()),
            &keypair,
        )
        .unwrap();
        let current = storage
            .serial_coordination()
            .unwrap()
            .read_head(serial_head_key())
            .await
            .unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .replace_head(serial_head_key(), &current.version, &rogue_head.to_bytes())
            .await
            .unwrap();

        let error = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect_err("wrong predecessor rejects the successor");
        assert!(
            error
                .to_string()
                .contains("commit 1 named by the signed head is absent"),
            "unexpected wrong-predecessor error: {error}"
        );
        assert_eq!(
            peer.exact_materialized_hash(SERIAL_STREAM_ID, 1)
                .await
                .unwrap(),
            Some(local_tip)
        );
        assert_eq!(
            peer.exact_materialized_hash(SERIAL_STREAM_ID, 2)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn serial_slot_rejects_merge_policy_commit_before_accepting_state() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-policy", "serial-policy");
        let db = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "serial-policy",
            "peer",
            &keypair,
        )
        .await;
        let wrong = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated("merge-in-serial-slot".to_string()),
            "merge-device".to_string(),
            crate::StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies: BTreeMap::new(),
            },
            None,
            1,
            &[],
            &keypair,
        )
        .unwrap();
        append_and_verify(
            &storage,
            &crate::sync::store_commit::commit_semantic_prefix(
                SERIAL_STREAM_ID,
                1,
                wrong.commit_hash(),
            ),
            ".json",
            &wrong.to_bytes(),
        )
        .await
        .unwrap();
        let head = StoreSerialHead::signed(
            root,
            Some(wrong.position()),
            Some(wrong.write_id.clone()),
            &keypair,
        )
        .unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .create_head(serial_head_key(), &head.to_bytes())
            .await
            .unwrap();
        let (_temp, store_dir) = temp_store_dir();

        let error = pull_serial(&db, &storage, root, &store_dir)
            .await
            .expect_err("MergeConcurrent commit cannot occupy a Serial slot");
        assert!(error.to_string().contains("write policy"));
        assert_eq!(
            db.exact_materialized_hash(SERIAL_STREAM_ID, 1)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn serial_pull_follows_the_signed_winner_when_a_same_sequence_loser_remains() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = serial_storage(&home, &keypair, "serial-cas-winner", "serial-cas-winner");
        let source = open_serial_test_db();
        let root = publish_test_serial_store_protocol_root(
            &source,
            &storage,
            "serial-cas-winner",
            "winner",
            &keypair,
        )
        .await;
        let winner_write = crate::WriteId::from_generated("serial-winner".to_string());
        let winner = StoreBatchCommit::signed(
            root,
            winner_write.clone(),
            "winner".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            1,
            &[],
            &keypair,
        )
        .unwrap();
        let loser = StoreBatchCommit::signed(
            root,
            crate::WriteId::from_generated("serial-loser".to_string()),
            "loser".to_string(),
            crate::StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            1,
            &[],
            &keypair,
        )
        .unwrap();
        for commit in [&winner, &loser] {
            let package = commit.store_package.as_ref().expect("Store package");
            append_and_verify(&storage, &package.object_key, ".pkg", &[])
                .await
                .unwrap();
            append_and_verify(
                &storage,
                &crate::sync::store_commit::commit_semantic_prefix(
                    SERIAL_STREAM_ID,
                    1,
                    commit.commit_hash(),
                ),
                ".json",
                &commit.to_bytes(),
            )
            .await
            .unwrap();
        }
        let head =
            StoreSerialHead::signed(root, Some(winner.position()), Some(winner_write), &keypair)
                .unwrap();
        storage
            .serial_coordination()
            .unwrap()
            .create_head(serial_head_key(), &head.to_bytes())
            .await
            .unwrap();
        let peer = open_serial_test_db();
        bind_database(&peer, "peer", root).await;
        let (_temp, peer_dir) = temp_store_dir();

        let result = pull_serial(&peer, &storage, root, &peer_dir)
            .await
            .expect("signed winner remains authoritative");

        assert_eq!(result.changesets_applied, 1);
        assert_eq!(
            peer.exact_materialized_hash(SERIAL_STREAM_ID, 1)
                .await
                .unwrap(),
            Some(winner.commit_hash())
        );
    }

    #[tokio::test]
    async fn discarding_serial_conflict_reverses_branch_and_applies_remote_chain_atomically() {
        let fixture = serial_conflict_fixture("serial-discard").await;
        host_exec(
            &fixture.local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('local-after-conflict', 'after-conflict', NULL, 1,
                     '0000000001003-0000-local', '2026-01-01')",
        )
        .await;
        let inspected = fixture.local.pending_branches().await.unwrap().unwrap();
        assert_eq!(inspected.writes.len(), 3);
        let plan = serial_resolution_plan(&fixture).await;
        let old_write_ids: Vec<_> = inspected
            .writes
            .iter()
            .map(|write| write.write_id.clone())
            .collect();

        fixture
            .local
            .discard_pending_serial_branch(fixture.branch.branch_id.clone(), plan)
            .await
            .expect("discard conflicted Serial branch");

        assert!(
            !row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id IN (
                    'local-provisional-a', 'local-provisional-b', 'local-after-conflict'
                )"
            )
            .await
        );
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'remote-committed'"
            )
            .await
        );
        assert_eq!(
            fixture
                .local
                .latest_outbound_store_position()
                .await
                .unwrap(),
            Some(fixture.remote_position)
        );
        for write_id in old_write_ids {
            assert_eq!(
                fixture.local.write_status(&write_id).await.unwrap(),
                crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
            );
        }
        assert!(fixture.local.pending_branches().await.unwrap().is_none());
        host_exec(
            &fixture.local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('after-conflict-discard', 'after-discard', NULL, 1,
                     '0000000001004-0000-local', '2026-01-01')",
        )
        .await;
        assert_eq!(
            publish_serial_pending(
                &fixture.local,
                &fixture.storage,
                "local",
                &fixture.keypair,
                &fixture.local_dir,
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn discard_failure_rolls_back_working_view_remote_chain_and_statuses() {
        let fixture = serial_conflict_fixture("serial-discard-rollback").await;
        let plan = serial_resolution_plan(&fixture).await;
        let old_write_ids: Vec<_> = fixture
            .branch
            .writes
            .iter()
            .map(|write| write.write_id.clone())
            .collect();
        fixture
            .local
            .call(|conn| {
                conn.execute_batch(
                    "CREATE TEMP TRIGGER fail_serial_resolution_status
                     BEFORE UPDATE OF status ON store_writes
                     WHEN json_extract(NEW.status, '$.resolved') IS NOT NULL
                     BEGIN SELECT RAISE(ABORT, 'injected resolution failure'); END;",
                )
                .map_err(DbError::from)
            })
            .await
            .unwrap();

        let error = fixture
            .local
            .discard_pending_serial_branch(fixture.branch.branch_id.clone(), plan)
            .await
            .expect_err("injected terminal-status write aborts discard");

        assert!(error.to_string().contains("injected resolution failure"));
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'local-provisional-a'"
            )
            .await
        );
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'local-provisional-b'"
            )
            .await
        );
        assert!(
            !row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'remote-committed'"
            )
            .await
        );
        assert_eq!(
            fixture
                .local
                .latest_outbound_store_position()
                .await
                .unwrap(),
            None
        );
        for write_id in old_write_ids {
            assert!(matches!(
                fixture.local.write_status(&write_id).await.unwrap(),
                crate::WriteStatus::Conflict(_)
            ));
        }
    }

    #[tokio::test]
    async fn replacing_serial_conflict_reruns_intent_on_remote_tip_with_exact_new_base() {
        let fixture = serial_conflict_fixture("serial-replace").await;
        host_exec(
            &fixture.local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('local-after-conflict', 'after-conflict', NULL, 1,
                     '0000000001003-0000-local', '2026-01-01')",
        )
        .await;
        let inspected = fixture.local.pending_branches().await.unwrap().unwrap();
        assert_eq!(inspected.writes.len(), 3);
        let plan = serial_resolution_plan(&fixture).await;
        let old_write_ids: Vec<_> = inspected
            .writes
            .iter()
            .map(|write| write.write_id.clone())
            .collect();
        let replacement_write_id = fixture.local.new_write_id();
        let expected_replacement = replacement_write_id.clone();

        let receipt = fixture
            .local
            .replace_pending_serial_branch(
                fixture.branch.branch_id.clone(),
                plan,
                replacement_write_id,
                |tx| {
                    let remote_title: String = tx
                        .query_row(
                            "SELECT title FROM notes WHERE id = 'remote-committed'",
                            [],
                            |row| row.get(0),
                        )
                        .map_err(DbError::from)?;
                    if remote_title != "remote" {
                        return Err(DbError(
                            "replacement did not observe remote tip".to_string(),
                        ));
                    }
                    tx.execute(
                        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
                         VALUES ('replacement-row', 'replacement', NULL, 1,
                                 '0000000001003-0000-local', '2026-01-01')",
                        [],
                    )
                    .map_err(DbError::from)?;
                    Ok(())
                },
            )
            .await
            .expect("replace conflicted Serial branch");

        assert_eq!(receipt.write_id, expected_replacement);
        assert_eq!(receipt.status, crate::WriteStatus::Pending);
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'remote-committed'"
            )
            .await
        );
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'replacement-row'"
            )
            .await
        );
        assert!(
            !row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id IN (
                    'local-provisional-a', 'local-provisional-b', 'local-after-conflict'
                )"
            )
            .await
        );
        let stored_base: String = fixture
            .local
            .call({
                let replacement = expected_replacement.clone();
                move |conn| {
                    conn.query_row(
                        "SELECT base FROM store_writes WHERE write_id = ?1",
                        [replacement.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
                }
            })
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<StoreWriteBase>(&stored_base).unwrap(),
            StoreWriteBase::Serial {
                branch_id: crate::PendingBranchId::from_first_write(expected_replacement.clone()),
                base: Some(fixture.remote_position),
            }
        );
        for write_id in old_write_ids {
            assert_eq!(
                fixture.local.write_status(&write_id).await.unwrap(),
                crate::WriteStatus::Resolved(crate::WriteResolution::Replaced {
                    replacement: expected_replacement.clone(),
                })
            );
        }
        host_exec(
            &fixture.local,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('after-conflict-replace', 'after-replace', NULL, 1,
                     '0000000001004-0000-local', '2026-01-01')",
        )
        .await;
        assert_eq!(
            publish_serial_pending(
                &fixture.local,
                &fixture.storage,
                "local",
                &fixture.keypair,
                &fixture.local_dir,
            )
            .await,
            2
        );
    }

    #[tokio::test]
    async fn replacement_failure_preserves_original_working_view_and_conflicted_branch() {
        let fixture = serial_conflict_fixture("serial-replace-rollback").await;
        let plan = serial_resolution_plan(&fixture).await;
        let replacement_write_id = fixture.local.new_write_id();

        let error = fixture
            .local
            .replace_pending_serial_branch(
                fixture.branch.branch_id.clone(),
                plan,
                replacement_write_id.clone(),
                |tx| {
                    tx.execute(
                        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
                         VALUES ('failed-replacement', 'failed', NULL, 1,
                                 '0000000001003-0000-local', '2026-01-01')",
                        [],
                    )
                    .map_err(DbError::from)?;
                    Err::<(), _>(DbError("injected replacement failure".to_string()))
                },
            )
            .await
            .expect_err("host replacement failure rolls back everything");

        assert!(error.to_string().contains("injected replacement failure"));
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'local-provisional-a'"
            )
            .await
        );
        assert!(
            row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id = 'local-provisional-b'"
            )
            .await
        );
        assert!(
            !row_exists(
                &fixture.local,
                "SELECT 1 FROM notes WHERE id IN ('remote-committed', 'failed-replacement')"
            )
            .await
        );
        assert_eq!(
            fixture
                .local
                .write_status(&fixture.branch.writes[0].write_id)
                .await
                .unwrap(),
            fixture.branch.writes[0].status
        );
        assert!(fixture
            .local
            .write_status(&replacement_write_id)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn resolved_write_statuses_and_subscriptions_reconstruct_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("resolved-status.sqlite");
        let (db, _) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "status-device".to_string(),
            &test_migrations(),
        )
        .unwrap();
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('resolved-discard', 'discard', NULL, 1,
                     '0000000001000-0000-status', '2026-01-01')",
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at)
             VALUES ('resolved-replace', 'replace', NULL, 1,
                     '0000000001001-0000-status', '2026-01-01')",
        )
        .await;
        let writes = db.pending_writes().await.unwrap();
        let replacement = db.new_write_id();
        let discarded = crate::WriteStatus::Resolved(crate::WriteResolution::Discarded);
        let replaced = crate::WriteStatus::Resolved(crate::WriteResolution::Replaced {
            replacement: replacement.clone(),
        });
        db.set_write_status(&writes[0].write_id, discarded.clone())
            .await
            .unwrap();
        db.set_write_status(&writes[1].write_id, replaced.clone())
            .await
            .unwrap();
        let discarded_id = writes[0].write_id.clone();
        let replaced_id = writes[1].write_id.clone();
        drop(db);

        let (reopened, _) = Database::open(
            &path,
            test_synced_tables(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "status-device".to_string(),
            &test_migrations(),
        )
        .unwrap();
        assert_eq!(
            reopened.write_status(&discarded_id).await.unwrap(),
            discarded
        );
        assert_eq!(reopened.write_status(&replaced_id).await.unwrap(), replaced);
        assert_eq!(
            *reopened
                .subscribe_write_status(&discarded_id)
                .await
                .unwrap()
                .borrow(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
        );
        assert_eq!(
            *reopened
                .subscribe_write_status(&replaced_id)
                .await
                .unwrap()
                .borrow(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Replaced { replacement })
        );
        assert!(reopened.pending_writes().await.unwrap().is_empty());
    }

    async fn publish_package(
        storage: &CloudSyncStorage,
        store_root_hash: ObjectHash,
        device_id: &str,
        package: &[u8],
        keypair: &UserKeypair,
    ) -> CommitPosition {
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            crate::WriteId::from_generated(format!("test-{device_id}-1")),
            device_id.to_string(),
            crate::StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies: BTreeMap::new(),
            },
            None,
            1,
            package,
            keypair,
        )
        .expect("sign Store commit");
        let head = StoreDeviceHead::signed(
            store_root_hash,
            device_id.to_string(),
            Some(commit.position()),
            "2026-01-01T00:00:00Z".to_string(),
            keypair,
        )
        .expect("sign Store head");
        let store_package = commit.store_package.as_ref().expect("Store package");
        append_and_verify(storage, &store_package.object_key, ".pkg", package)
            .await
            .expect("publish package");
        append_and_verify(
            storage,
            &crate::sync::store_commit::commit_semantic_prefix(device_id, 1, commit.commit_hash()),
            ".json",
            &commit.to_bytes(),
        )
        .await
        .expect("publish commit");
        append_and_verify(
            storage,
            &crate::sync::store_commit::head_semantic_prefix(device_id, 1, head.head_hash()),
            ".json",
            &head.to_bytes(),
        )
        .await
        .expect("publish head");
        commit.position()
    }

    fn independent_test_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::IndependentUuid)
                .gated_by("shared"),
            SyncedTable::new(
                "note_tags",
                crate::sync::session::RowIdentity::IndependentUuid,
            ),
            SyncedTable::new(
                "note_photos",
                crate::sync::session::RowIdentity::IndependentUuid,
            ),
        ]
    }

    async fn setup_store() -> (InMemoryCloudHome, UserKeypair, ObjectHash) {
        let home = InMemoryCloudHome::new();
        home.sort_listings();
        let identity = UserKeypair::generate();
        let founder = founder_entry(
            "causal-ordering-test",
            &identity,
            "0000000000001-0000-founder",
        );
        let store_protocol_root = StoreProtocolRoot::signed(
            "causal-ordering-test".to_string(),
            founder,
            1,
            crate::sync::test_helpers::test_sync_routing_hash(),
            crate::WritePolicy::MergeConcurrent,
            &identity,
        )
        .expect("sign store protocol root");
        let store_root_hash = store_protocol_root.object_hash();
        append_and_verify(
            &storage(&home, &identity, "store-protocol-root"),
            &store_protocol_root_semantic_prefix(store_root_hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .expect("publish store protocol root");
        (home, identity, store_root_hash)
    }

    #[tokio::test]
    async fn invalid_remote_uuid_holds_exact_commit_while_another_device_applies() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        let invalid_package = crate::sync::test_helpers::capture_bytes(
            &source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('not-a-uuid', 'invalid', NULL, 1, '0000000001000-0000-bad', '2026-01-01')",
            ],
        )
        .await;
        let valid_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        let valid_package = crate::sync::test_helpers::capture_bytes(
            &source,
            &[&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('{valid_id}', 'valid', NULL, 1, '0000000001001-0000-good', '2026-01-01')"
            )],
        )
        .await;
        let shared_storage = storage(&home, &identity, "identity-receipt");
        let invalid_position = publish_package(
            &shared_storage,
            store_root_hash,
            "dev-a-invalid",
            &invalid_package,
            &identity,
        )
        .await;
        let valid_position = publish_package(
            &shared_storage,
            store_root_hash,
            "dev-z-valid",
            &valid_package,
            &identity,
        )
        .await;

        let receiver = open_test_db_schema(
            independent_test_tables(),
            crate::sync::test_helpers::test_migrations(),
        );
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let pulled = pull_store_commits(
            &receiver,
            &independent_test_tables(),
            &shared_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("invalid identity is a held commit");

        assert_eq!(pulled.changesets_applied, 1);
        assert_eq!(pulled.held_positions.len(), 1);
        assert!(matches!(
            &pulled.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Commit { device_id, position },
                reason: HeldStorePositionReason::InvalidRowIdentity { table, reason },
            } if device_id == "dev-a-invalid"
                && position == &invalid_position
                && table == "notes"
                && reason.contains("not-a-uuid")
        ));
        assert_eq!(pulled.frontier.get("dev-z-valid"), Some(&valid_position));
        assert!(!pulled.frontier.contains_key("dev-a-invalid"));
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-a-invalid", 1)
                .await
                .expect("read invalid position"),
            None
        );
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-z-valid", 1)
                .await
                .expect("read valid position"),
            Some(valid_position.commit_hash)
        );
        assert!(!row_exists(&receiver, "SELECT 1 FROM notes WHERE id = 'not-a-uuid'").await);
        assert!(
            row_exists(
                &receiver,
                &format!("SELECT 1 FROM notes WHERE id = '{valid_id}'")
            )
            .await
        );
    }

    async fn remove_appended_prefix(
        home: &InMemoryCloudHome,
        prefix: &str,
    ) -> Vec<(String, Vec<u8>)> {
        let removed: Vec<_> = home
            .appended_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .map(|key| {
                let bytes = home.get_appended(&key).expect("listed appended bytes");
                (key, bytes)
            })
            .collect();
        let listing = home
            .list_appended(prefix)
            .await
            .expect("list appended candidates");
        for locator in listing.objects {
            home.remove_appended_candidate(&locator);
        }
        removed
    }

    fn restore_appended(home: &InMemoryCloudHome, removed: Vec<(String, Vec<u8>)>) {
        for (key, bytes) in removed {
            home.insert_appended_candidate(&key, bytes);
        }
    }

    fn unique_note_db() -> Database {
        open_test_db_schema(
            vec![SyncedTable::new(
                "unique_notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            vec![crate::migration::Migration::run(
                1,
                "unique-note-schema",
                |conn| {
                    conn.execute_batch(
                        "CREATE TABLE unique_notes (\
                           id TEXT PRIMARY KEY,\
                           slug TEXT NOT NULL UNIQUE,\
                           title TEXT NOT NULL,\
                           _updated_at TEXT NOT NULL,\
                           created_at TEXT NOT NULL\
                         ) STRICT;",
                    )
                    .map_err(DbError::from)
                },
            )],
        )
    }

    async fn assert_update_before_insert_converges(inserter: &str, updater: &str) {
        let home = InMemoryCloudHome::new();
        home.sort_listings();
        let identity = UserKeypair::generate();
        let founder = founder_entry(
            "causal-ordering-test",
            &identity,
            "0000000000001-0000-founder",
        );
        let store_protocol_root = StoreProtocolRoot::signed(
            "causal-ordering-test".to_string(),
            founder,
            1,
            crate::sync::test_helpers::test_sync_routing_hash(),
            crate::WritePolicy::MergeConcurrent,
            &identity,
        )
        .expect("sign store protocol root");
        let store_root_hash = store_protocol_root.object_hash();
        let setup_storage = storage(&home, &identity, "setup");
        append_and_verify(
            &setup_storage,
            &store_protocol_root_semantic_prefix(store_root_hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .expect("publish store protocol root");

        let insert_db = open_test_db();
        bind_database(&insert_db, inserter, store_root_hash).await;
        host_exec(
            &insert_db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'orig', NULL, 1, '0000000001000-0000-ins', '2026-01-01')",
        )
        .await;
        let (_insert_tmp, insert_dir) = temp_store_dir();
        let insert_storage = storage(&home, &identity, "insert");
        publish_pending(
            &insert_db,
            &insert_storage,
            inserter,
            &identity,
            &insert_dir,
        )
        .await;

        let update_db = open_test_db();
        bind_database(&update_db, updater, store_root_hash).await;
        let (_update_tmp, update_dir) = temp_store_dir();
        let update_storage = storage(&home, &identity, "update");
        let first_pull = pull_store_commits(
            &update_db,
            &test_synced_tables(),
            &update_storage,
            store_root_hash,
            updater,
            &update_dir,
            None,
        )
        .await
        .expect("updater observes the insert");
        assert_eq!(first_pull.changesets_applied, 1);
        host_exec(
            &update_db,
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-upd' \
             WHERE id = 'n1'",
        )
        .await;
        publish_pending(&update_db, &update_storage, updater, &identity, &update_dir).await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let receiver_storage = storage(&home, &identity, "receiver");
        let pull = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("receiver applies the causal ready queue");

        assert_eq!(pull.changesets_applied, 2);
        assert!(pull.held_positions.is_empty());
        assert_eq!(
            query_text(&receiver, "SELECT title FROM notes WHERE id = 'n1'").await,
            "updated",
            "the UPDATE must wait for and then apply after its causal INSERT",
        );
    }

    #[tokio::test]
    async fn update_applied_before_its_insert_diverges_notfound_omit() {
        assert_update_before_insert_converges("dev-z", "dev-a").await;
    }

    #[tokio::test]
    async fn update_and_insert_converge_in_the_opposite_discovery_order() {
        assert_update_before_insert_converges("dev-a", "dev-z").await;
    }

    #[tokio::test]
    async fn latest_head_walks_and_applies_every_unseen_predecessor() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = open_test_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        let publisher_storage = storage(&home, &identity, "publisher");

        host_exec(
            &publisher,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'first', NULL, 1, '0000000001000-0000-pub', '2026-01-01')",
        )
        .await;
        publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        host_exec(
            &publisher,
            "UPDATE notes SET title = 'second', _updated_at = '0000000002000-0000-pub' \
             WHERE id = 'n1'",
        )
        .await;
        let second = publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        assert_eq!(second.seq, 2);

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let result = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("walk latest head ancestry");

        assert_eq!(result.changesets_applied, 2);
        assert!(result.held_positions.is_empty());
        assert_eq!(
            query_text(&receiver, "SELECT title FROM notes WHERE id = 'n1'").await,
            "second"
        );
    }

    #[tokio::test]
    async fn row_and_materialized_position_roll_back_together_on_frontier_failure() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = open_test_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        host_exec(
            &publisher,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'atomic', NULL, 1, '0000000001000-0000-pub', '2026-01-01')",
        )
        .await;
        let position = publish_pending(
            &publisher,
            &storage(&home, &identity, "publisher"),
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        receiver
            .call(|conn| {
                conn.execute_batch(
                    "CREATE TEMP TRIGGER fail_materialized_frontier \
                     BEFORE INSERT ON materialized_commits \
                     BEGIN SELECT RAISE(ABORT, 'injected frontier failure'); END;",
                )
                .map_err(DbError::from)
            })
            .await
            .expect("install frontier fault");
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let first = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-first"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await;
        assert!(matches!(first, Err(StorePullError::Database(_))));
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            receiver
                .call(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM notes WHERE id = 'n1'", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(DbError::from)
                })
                .await
                .unwrap(),
            0,
            "the row must roll back when its exact position cannot commit",
        );

        receiver
            .call(|conn| {
                conn.execute_batch("DROP TRIGGER fail_materialized_frontier")
                    .map_err(DbError::from)
            })
            .await
            .expect("remove frontier fault");
        let retry = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-retry"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("retry atomic apply");
        assert_eq!(retry.changesets_applied, 1);
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            Some(position.commit_hash)
        );
    }

    #[tokio::test]
    async fn host_write_captures_the_exact_materialized_dependency_frontier() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        bind_database(&source, "dev-source", store_root_hash).await;
        let (_source_tmp, source_dir) = temp_store_dir();
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('source', 'source', NULL, 1, '0000000001000-0000-src', '2026-01-01')",
        )
        .await;
        let source_position = publish_pending(
            &source,
            &storage(&home, &identity, "source"),
            "dev-source",
            &identity,
            &source_dir,
        )
        .await;

        let writer = open_test_db();
        bind_database(&writer, "dev-writer", store_root_hash).await;
        let (_writer_tmp, writer_dir) = temp_store_dir();
        let writer_storage = storage(&home, &identity, "writer");
        pull_store_commits(
            &writer,
            &test_synced_tables(),
            &writer_storage,
            store_root_hash,
            "dev-writer",
            &writer_dir,
            None,
        )
        .await
        .expect("materialize dependency");
        host_exec(
            &writer,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('writer', 'writer', NULL, 1, '0000000002000-0000-wrt', '2026-01-01')",
        )
        .await;
        let base: crate::database::StoreWriteBase = serde_json::from_str(
            &query_text(
                &writer,
                "SELECT base FROM store_writes ORDER BY ordinal DESC LIMIT 1",
            )
            .await,
        )
        .expect("parse captured Store write base");
        let crate::database::StoreWriteBase::MergeConcurrent { dependencies } = base else {
            panic!("MergeConcurrent host write recorded a Serial base");
        };
        assert_eq!(
            dependencies.get("dev-source"),
            Some(&source_position),
            "the host transaction records the exact frontier it observed",
        );

        let writer_position = publish_pending(
            &writer,
            &writer_storage,
            "dev-writer",
            &identity,
            &writer_dir,
        )
        .await;
        let commit = load_commit_slot(
            &writer_storage,
            store_root_hash,
            "dev-writer",
            writer_position.seq,
        )
        .await
        .expect("load writer commit")
        .expect("writer commit exists");
        assert_eq!(commit.value.merge_dependencies().unwrap(), &dependencies);
    }

    #[tokio::test]
    async fn missing_dependency_is_held_across_restart_then_applied_when_visible() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        bind_database(&source, "dev-source", store_root_hash).await;
        let (_source_tmp, source_dir) = temp_store_dir();
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('source', 'source', NULL, 1, '0000000001000-0000-src', '2026-01-01')",
        )
        .await;
        let source_position = publish_pending(
            &source,
            &storage(&home, &identity, "source"),
            "dev-source",
            &identity,
            &source_dir,
        )
        .await;

        let dependent = open_test_db();
        bind_database(&dependent, "dev-dependent", store_root_hash).await;
        let (_dependent_tmp, dependent_dir) = temp_store_dir();
        let dependent_storage = storage(&home, &identity, "dependent");
        pull_store_commits(
            &dependent,
            &test_synced_tables(),
            &dependent_storage,
            store_root_hash,
            "dev-dependent",
            &dependent_dir,
            None,
        )
        .await
        .expect("dependent observes source");
        host_exec(
            &dependent,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('dependent', 'dependent', NULL, 1, '0000000002000-0000-dep', '2026-01-01')",
        )
        .await;
        publish_pending(
            &dependent,
            &dependent_storage,
            "dev-dependent",
            &identity,
            &dependent_dir,
        )
        .await;

        let removed_heads = remove_appended_prefix(&home, "store-v1/heads/dev-source/").await;
        assert!(!removed_heads.is_empty());
        for attempt in ["receiver-before-restart", "receiver-after-restart"] {
            let receiver = open_test_db();
            bind_database(&receiver, attempt, store_root_hash).await;
            let (_receiver_tmp, receiver_dir) = temp_store_dir();
            let held = pull_store_commits(
                &receiver,
                &test_synced_tables(),
                &storage(&home, &identity, attempt),
                store_root_hash,
                attempt,
                &receiver_dir,
                None,
            )
            .await
            .expect("missing dependency is a held state");
            assert!(held.held_positions.iter().any(|position| {
                matches!(
                    &position.reason,
                    HeldStorePositionReason::MissingDependency {
                        device_id,
                        position,
                    } if device_id == "dev-source" && position == &source_position
                )
            }));
            assert_eq!(held.changesets_applied, 0);
        }

        restore_appended(&home, removed_heads);
        let receiver = open_test_db();
        bind_database(&receiver, "dev-final", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let applied = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-final"),
            store_root_hash,
            "dev-final",
            &receiver_dir,
            None,
        )
        .await
        .expect("dependency becomes visible");
        assert_eq!(applied.changesets_applied, 2);
        assert!(applied.held_positions.is_empty());
    }

    #[tokio::test]
    async fn missing_predecessor_is_held_until_the_exact_commit_reappears() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = open_test_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        let publisher_storage = storage(&home, &identity, "publisher");
        host_exec(
            &publisher,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'one', NULL, 1, '0000000001000-0000-pub', '2026-01-01')",
        )
        .await;
        publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        host_exec(
            &publisher,
            "UPDATE notes SET title = 'two', _updated_at = '0000000002000-0000-pub' \
             WHERE id = 'n1'",
        )
        .await;
        publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        let removed = remove_appended_prefix(&home, "store-v1/commits/dev-publisher/1/").await;
        assert!(!removed.is_empty());

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let held = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("missing predecessor is held");
        assert!(held.held_positions.iter().any(|position| matches!(
            position.reason,
            HeldStorePositionReason::MissingPredecessor(_)
        )));
        assert_eq!(held.changesets_applied, 0);

        restore_appended(&home, removed);
        let retry = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-retry"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("predecessor reappears");
        assert_eq!(retry.changesets_applied, 2);
        assert!(retry.held_positions.is_empty());
    }

    #[tokio::test]
    async fn dependency_hash_mismatch_fails_against_the_durable_exact_position() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        bind_database(&source, "dev-source", store_root_hash).await;
        let (_source_tmp, source_dir) = temp_store_dir();
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('source', 'source', NULL, 1, '0000000001000-0000-src', '2026-01-01')",
        )
        .await;
        publish_pending(
            &source,
            &storage(&home, &identity, "source"),
            "dev-source",
            &identity,
            &source_dir,
        )
        .await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let receiver_storage = storage(&home, &identity, "receiver");
        pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("materialize exact source position");

        let package_source = open_test_db();
        let package = crate::sync::test_helpers::capture_bytes(
            &package_source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('dependent', 'dependent', NULL, 1, '0000000002000-0000-dep', '2026-01-01')",
            ],
        )
        .await;
        let fake_dependency = CommitPosition {
            seq: 1,
            commit_hash: ObjectHash::digest(b"not-the-source-commit"),
        };
        let mut dependencies = BTreeMap::new();
        dependencies.insert("dev-source".to_string(), fake_dependency.clone());
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            crate::WriteId::from_generated("test-dependent-1".to_string()),
            "dev-dependent".to_string(),
            crate::StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies,
            },
            None,
            receiver.schema_version(),
            &package,
            &identity,
        )
        .expect("sign mismatched dependency commit");
        let head = StoreDeviceHead::signed(
            store_root_hash,
            "dev-dependent".to_string(),
            Some(commit.position()),
            "2026-01-01T00:00:00Z".to_string(),
            &identity,
        )
        .expect("sign dependent head");
        append_and_verify(
            &receiver_storage,
            &commit
                .store_package
                .as_ref()
                .expect("Store package")
                .object_key,
            ".pkg",
            &package,
        )
        .await
        .unwrap();
        append_and_verify(
            &receiver_storage,
            &crate::sync::store_commit::commit_semantic_prefix(
                "dev-dependent",
                1,
                commit.commit_hash(),
            ),
            ".json",
            &commit.to_bytes(),
        )
        .await
        .unwrap();
        append_and_verify(
            &receiver_storage,
            &crate::sync::store_commit::head_semantic_prefix("dev-dependent", 1, head.head_hash()),
            ".json",
            &head.to_bytes(),
        )
        .await
        .unwrap();

        let result = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("dependency hash mismatch holds the dependent commit");
        assert_eq!(result.held_positions.len(), 1);
        assert!(matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Dependency {
                    dependent_device_id,
                    dependent_position,
                    required_device_id,
                    required_position,
                },
                reason: HeldStorePositionReason::HashMismatch {
                    materialized_hash,
                    ..
                },
            } if dependent_device_id == "dev-dependent"
                && dependent_position == &commit.position()
                && required_device_id == "dev-source"
                && required_position == &fake_dependency
                && materialized_hash != &fake_dependency.commit_hash
        ));
    }

    #[tokio::test]
    async fn non_fk_constraint_rolls_back_rows_and_exact_position_then_retries() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = unique_note_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        host_exec(
            &publisher,
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('partial', 'free-slug', 'Partial', '0000000001000-0000-pub', '2026-01-01'); \
             INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('remote', 'same-slug', 'Remote', '0000000001001-0000-pub', '2026-01-01')",
        )
        .await;
        let position = publish_pending(
            &publisher,
            &storage(&home, &identity, "publisher-unique"),
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;

        let receiver = unique_note_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        host_exec(
            &receiver,
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('local', 'same-slug', 'Local', '0000000002000-0000-rec', '2026-01-01')",
        )
        .await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let receiver_storage = storage(&home, &identity, "receiver-unique");
        let held = pull_store_commits(
            &receiver,
            &[SyncedTable::new(
                "unique_notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("uniqueness conflict is a held position");
        assert!(held.held_positions.iter().any(|held| matches!(
            &held.reason,
            HeldStorePositionReason::ConstraintConflict(tables)
                if tables == &["unique_notes".to_string()]
        )));
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            None
        );
        assert!(!row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'partial'").await);
        assert!(!row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'remote'").await);
        assert!(row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'local'").await);

        host_exec(&receiver, "DELETE FROM unique_notes WHERE id = 'local'").await;
        let retry = pull_store_commits(
            &receiver,
            &[SyncedTable::new(
                "unique_notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("constraint removed");
        assert_eq!(retry.changesets_applied, 1);
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            Some(position.commit_hash)
        );
        assert!(row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'partial'").await);
        assert!(row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'remote'").await);
    }

    async fn assert_concurrent_delete_update_converges(deleter: &str, updater: &str) {
        let (home, identity, store_root_hash) = setup_store().await;
        let base = open_test_db();
        bind_database(&base, "dev-base", store_root_hash).await;
        let (_base_tmp, base_dir) = temp_store_dir();
        host_exec(
            &base,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'base', NULL, 1, '0000000001000-0000-base', '2026-01-01')",
        )
        .await;
        publish_pending(
            &base,
            &storage(&home, &identity, "base"),
            "dev-base",
            &identity,
            &base_dir,
        )
        .await;

        let delete_db = open_test_db();
        bind_database(&delete_db, deleter, store_root_hash).await;
        let (_delete_tmp, delete_dir) = temp_store_dir();
        let delete_storage = storage(&home, &identity, "delete");
        pull_store_commits(
            &delete_db,
            &test_synced_tables(),
            &delete_storage,
            store_root_hash,
            deleter,
            &delete_dir,
            None,
        )
        .await
        .unwrap();

        let update_db = open_test_db();
        bind_database(&update_db, updater, store_root_hash).await;
        let (_update_tmp, update_dir) = temp_store_dir();
        let update_storage = storage(&home, &identity, "update");
        pull_store_commits(
            &update_db,
            &test_synced_tables(),
            &update_storage,
            store_root_hash,
            updater,
            &update_dir,
            None,
        )
        .await
        .unwrap();

        host_exec(&delete_db, "DELETE FROM notes WHERE id = 'n1'").await;
        publish_pending(&delete_db, &delete_storage, deleter, &identity, &delete_dir).await;
        host_exec(
            &update_db,
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-update' \
             WHERE id = 'n1'",
        )
        .await;
        publish_pending(&update_db, &update_storage, updater, &identity, &update_dir).await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let result = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-delete-update"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("apply both causal-ready branches");
        assert_eq!(result.changesets_applied, 3);
        assert!(result.held_positions.is_empty());
        assert!(
            !row_exists(&receiver, "SELECT 1 FROM notes WHERE id = 'n1'").await,
            "concurrent delete wins regardless of ready-queue order",
        );
    }

    #[tokio::test]
    async fn delete_then_update_when_both_are_causal_ready_converges() {
        assert_concurrent_delete_update_converges("dev-a-delete", "dev-z-update").await;
    }

    #[tokio::test]
    async fn update_then_delete_when_both_are_causal_ready_converges() {
        assert_concurrent_delete_update_converges("dev-z-delete", "dev-a-update").await;
    }

    #[tokio::test]
    async fn merge_and_serial_candidate_download_outages_reach_offline_classification() {
        for policy in [
            crate::WritePolicy::MergeConcurrent,
            crate::WritePolicy::Serial,
        ] {
            let tables = crate::sync::test_helpers::test_synced_tables_with_blob(
                crate::sync::session::BlobDecl::new(
                    "photos",
                    crate::blob::Provenance::HostProvided,
                    crate::blob::CacheFill::CacheEager,
                ),
            );
            let (receiver, _) = Database::open(
                std::path::Path::new(":memory:"),
                tables.clone(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                policy,
                format!("candidate-download-{policy:?}"),
                &test_migrations(),
            )
            .unwrap();
            let source = crate::sync::test_helpers::open_test_db_with_blob(
                crate::sync::session::BlobDecl::new(
                    "photos",
                    crate::blob::Provenance::HostProvided,
                    crate::blob::CacheFill::CacheEager,
                ),
            );
            let bytes = b"candidate-download";
            let hash = crate::blob::content_hash(bytes);
            let photo_sql = format!(
                "INSERT INTO note_photos \
                 (id, note_id, kind, size, hash, _updated_at, created_at) \
                 VALUES ('candidate-blob', 'candidate-root', 'cover', {}, '{}', \
                         '0000000001000-0000-source', '2026-01-01')",
                bytes.len(),
                hash,
            );
            let package = crate::sync::test_helpers::capture_bytes(
                &source,
                &[
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('candidate-root', 'candidate', NULL, 1, \
                             '0000000001000-0000-source', '2026-01-01')",
                    &photo_sql,
                ],
            )
            .await;
            let keypair = UserKeypair::generate();
            let order = match policy {
                crate::WritePolicy::MergeConcurrent => crate::StoreCommitOrder::MergeConcurrent {
                    seq: 1,
                    previous_commit_hash: None,
                    dependencies: BTreeMap::new(),
                },
                crate::WritePolicy::Serial => crate::StoreCommitOrder::Serial {
                    seq: 1,
                    previous_commit_hash: None,
                },
            };
            let commit = StoreBatchCommit::signed(
                ObjectHash::digest(b"candidate-download-root"),
                receiver.new_write_id(),
                "source".to_string(),
                order,
                None,
                1,
                &package,
                &keypair,
            )
            .unwrap();
            let candidate = Candidate {
                commit,
                package: Some(package),
                registrations: Vec::new(),
            };
            let storage = crate::sync::test_helpers::MockSyncStorage::new();
            storage
                .put_blob(
                    "photos",
                    "candidate-blob",
                    crate::blob::BlobScope::Master,
                    None,
                    bytes.to_vec(),
                )
                .await
                .unwrap();
            storage.fail_next_blob_reads(1);
            let (_temp, store_dir) = temp_store_dir();
            let schema_tables = tables.clone();
            let schema = Arc::new(
                receiver
                    .call(move |conn| TableSchema::from_db(conn, &schema_tables))
                    .await
                    .unwrap(),
            );

            let error = match policy {
                crate::WritePolicy::MergeConcurrent => {
                    match apply_candidate(&receiver, &storage, &store_dir, schema, &candidate).await
                    {
                        Ok(_) => panic!("MergeConcurrent candidate download outage must fail"),
                        Err(error) => error,
                    }
                }
                crate::WritePolicy::Serial => {
                    match prepare_serial_candidate(
                        &receiver, &storage, &store_dir, schema, &candidate,
                    )
                    .await
                    {
                        Ok(_) => panic!("Serial candidate download outage must fail"),
                        Err(error) => error,
                    }
                }
            };
            assert!(matches!(error, StorePullError::BlobDownloads(_)));
            assert!(
                crate::sync::cycle::SyncCycleFailure::operation("pull Store commits", error,)
                    .is_offline()
            );
        }
    }
}
