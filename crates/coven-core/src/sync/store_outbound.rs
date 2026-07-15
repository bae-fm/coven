//! Durable construction and ordered publication of local Store commits.

use super::membership::{MembershipChain, SerialAuthorizationState};
use super::storage::{
    CoordinationError, CoordinationStorage, CreateHeadError, ReplaceHeadError, SyncStorage,
    VersionToken,
};
use super::store_commit::{
    commit_semantic_prefix, head_semantic_prefix, serial_head_key, ObjectHash, StoreBatchCommit,
    StoreCommitOrder, StoreControl, StoreDeviceHead, StoreSerialHead, SERIAL_STREAM_ID,
};
use super::store_objects::{append_and_verify, StoreObjectError};
use crate::database::{
    Database, PreparedSerialStoreBranch, PreparedStoreWrite, PreparedStoreWriteCommit,
    SerialStoreWritePreparation, SerialStoreWritePreparationEntry, StoreBlobManifest,
    StoreWriteBase, StoreWritePreparation,
};
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

#[derive(Debug, thiserror::Error)]
pub enum StoreOutboundError {
    #[error("database: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store protocol state {key:?} is absent")]
    MissingState { key: &'static str },
    #[error("Store protocol state {key:?} is invalid: {reason}")]
    InvalidState { key: &'static str, reason: String },
    #[error("outbound Store row is invalid: {0}")]
    InvalidOutbound(String),
    #[error("outbound Store preparation failed: {0}")]
    Preparation(#[source] super::service::SyncCycleError),
    #[error("outbound blob {namespace}/{id} is local and cannot be published")]
    LocalUserBlob { namespace: String, id: String },
    #[error("outbound blob {namespace}/{id} is absent from storage")]
    MissingBlob { namespace: String, id: String },
    #[error("checking outbound blob {namespace}/{id}: {source}")]
    BlobStorage {
        namespace: String,
        id: String,
        source: super::storage::StorageError,
    },
    #[error("Serial coordination capability is required")]
    MissingSerialCoordination,
    #[error("Serial coordination: {0}")]
    Coordination(#[source] CoordinationError),
    #[error("Serial control branch is stale: expected {expected:?}, current {current:?}")]
    SerialControlConflict {
        expected: Option<crate::sync::store_commit::CommitPosition>,
        current: Option<crate::sync::store_commit::CommitPosition>,
    },
}

impl From<crate::database::DbError> for StoreOutboundError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

impl StoreOutboundError {
    pub(crate) fn definitely_uncommitted(&self) -> bool {
        match self {
            Self::Database(_) | Self::Coordination(_) => false,
            Self::BlobStorage { source, .. } => source.definitely_uncommitted(),
            Self::Object(_) => true,
            Self::MissingState { .. }
            | Self::InvalidState { .. }
            | Self::InvalidOutbound(_)
            | Self::Preparation(_)
            | Self::LocalUserBlob { .. }
            | Self::MissingBlob { .. }
            | Self::MissingSerialCoordination
            | Self::SerialControlConflict { .. } => true,
        }
    }
}

/// Prepare the oldest pending write as exact signed bytes. A blocked or already
/// prepared oldest write holds later writes behind it.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_pending_store_write(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
    cancel: Option<&super::service::HostUploadCloud<'_>>,
) -> Result<bool, StoreOutboundError> {
    prepare_pending_store_write_with_coordination(
        db, storage, None, device_id, timestamp, keypair, store_dir, membership, cancel,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_pending_store_write_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
    cancel: Option<&super::service::HostUploadCloud<'_>>,
) -> Result<bool, StoreOutboundError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        return prepare_serial_store_branch(
            db,
            storage,
            coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?,
            device_id,
            keypair,
            store_dir,
            cancel,
        )
        .await;
    }
    let Some(PreparedStoreWrite {
        write_id,
        changeset,
        inverse_changeset,
        base,
        blob_facts,
        partitions: _partitions,
    }) = db.prepare_store_write().await?
    else {
        return Ok(false);
    };
    if !changeset.is_empty() && inverse_changeset.is_empty() {
        return Err(StoreOutboundError::InvalidOutbound(
            "shared Store write has no inverse changeset".to_string(),
        ));
    }
    let dependencies = match base {
        StoreWriteBase::MergeConcurrent { dependencies } => dependencies,
        StoreWriteBase::Serial { .. } => {
            return Err(StoreOutboundError::InvalidOutbound(
                "serial Store write reached MergeConcurrent preparation".to_string(),
            ));
        }
    };
    let preparation = async {
        let payload = super::service::prepare_store_payload(
            db,
            storage,
            &blob_facts,
            keypair,
            store_dir,
            membership,
            cancel,
        )
        .await
        .map_err(StoreOutboundError::Preparation)?;
        let store_root_hash = required_store_root_hash(db).await?;
        let previous = db.latest_local_store_position().await?;
        let seq = previous
            .as_ref()
            .map_or(1, |position| position.seq.saturating_add(1));
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            write_id.clone(),
            device_id.to_string(),
            StoreCommitOrder::MergeConcurrent {
                seq,
                previous_commit_hash: previous.map(|position| position.commit_hash),
                dependencies,
            },
            payload.membership_grant,
            db.schema_version(),
            &changeset,
            keypair,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let head = StoreDeviceHead::signed(
            store_root_hash,
            device_id.to_string(),
            Some(commit.position()),
            timestamp.to_string(),
            keypair,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        Ok::<_, StoreOutboundError>(StoreWritePreparation {
            write_id: write_id.clone(),
            package_bytes: changeset,
            commit,
            head,
            blob_manifest: payload.blob_manifest,
            local_cleanup: payload.local_cleanup,
            completion: payload.completion,
        })
    }
    .await;
    let preparation = match preparation {
        Ok(preparation) => preparation,
        Err(error) => {
            record_preparation_failure(db, &write_id, &error).await?;
            return Err(error);
        }
    };
    db.prepare_store_write_commit(preparation).await?;
    Ok(true)
}

/// Publish prepared writes in sequence order. Each attempt appends fresh physical
/// copies of the package, commit, and head; only a verified head allows the local
/// write's published position and completion bookkeeping to commit.
pub async fn drain_store_writes(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreOutboundError> {
    drain_store_writes_with_coordination(db, storage, None).await
}

pub(crate) async fn drain_store_writes_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
) -> Result<u64, StoreOutboundError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        return drain_serial_store_branch(
            db,
            storage,
            coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?,
        )
        .await;
    }
    let mut published = 0_u64;
    while let Some(batch) = db.oldest_prepared_store_write().await? {
        let write_id = batch.commit.value.write_id.clone();
        db.set_write_status(&write_id, crate::WriteStatus::Publishing)
            .await?;
        let attempt = async {
            let store_root_hash = required_store_root_hash(db).await?;
            let device_id = db
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?
                .ok_or(StoreOutboundError::MissingState {
                    key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
                })?;
            validate_manifest(storage, &batch.blob_manifest).await?;
            validate_outbound(&batch, store_root_hash, &device_id)?;
            let commit = &batch.commit.value;
            let head = &batch.head.value;
            let package = required_store_package(commit)?;
            append_and_verify(
                storage,
                &super::storage::ProtocolObjectContext::store(
                    store_root_hash,
                    super::storage::ProtocolObjectDomain::StorePackage,
                ),
                &package.object_key,
                ".pkg",
                &batch.package_bytes,
            )
            .await?;
            append_and_verify(
                storage,
                &super::storage::ProtocolObjectContext::store(
                    store_root_hash,
                    super::storage::ProtocolObjectDomain::StoreCommit,
                ),
                &commit_semantic_prefix(&device_id, commit.seq(), commit.commit_hash()),
                ".json",
                &batch.commit.bytes,
            )
            .await?;
            append_and_verify(
                storage,
                &super::storage::ProtocolObjectContext::store(
                    store_root_hash,
                    super::storage::ProtocolObjectDomain::StoreHead,
                ),
                &head_semantic_prefix(&device_id, commit.seq(), head.head_hash()),
                ".json",
                &batch.head.bytes,
            )
            .await?;
            db.complete_prepared_store_write(commit.position()).await?;
            Ok::<(), StoreOutboundError>(())
        }
        .await;
        if let Err(error) = attempt {
            let status = match blocked_status(&error) {
                Some(block) => crate::WriteStatus::Blocked(block),
                None => crate::WriteStatus::Pending,
            };
            db.set_write_status(&write_id, status).await?;
            return Err(error);
        }
        published = published
            .checked_add(1)
            .ok_or_else(|| StoreOutboundError::Database("publish count exceeded u64".into()))?;
    }
    Ok(published)
}

enum SerialHeadObservation {
    Absent,
    Present {
        head: StoreSerialHead,
        version: VersionToken,
    },
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedSerialControl {
    pub base: Option<crate::sync::store_commit::CommitPosition>,
    pub commit: StoreBatchCommit,
    pub head: StoreSerialHead,
    pub authorization_after: SerialAuthorizationState,
}

pub(crate) struct SerialAuthorizationSnapshot {
    pub base: Option<crate::sync::store_commit::CommitPosition>,
    pub authorization: SerialAuthorizationState,
}

pub(crate) async fn current_serial_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
) -> Result<SerialAuthorizationState, StoreOutboundError> {
    Ok(
        current_serial_authorization_snapshot(db, storage, coordination)
            .await?
            .authorization,
    )
}

pub(crate) async fn current_serial_authorization_snapshot(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
) -> Result<SerialAuthorizationSnapshot, StoreOutboundError> {
    let store_root_hash = required_store_root_hash(db).await?;
    let observed = observe_serial_head(db, coordination).await?;
    let authorization = match observed.head() {
        Some(head) => {
            super::store_pull::load_serial_authorization_at_head(storage, store_root_hash, head)
                .await
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
        }
        None => {
            if db.latest_outbound_store_position().await?.is_some() {
                return Err(StoreOutboundError::InvalidState {
                    key: crate::database::STORE_ROOT_HASH_STATE_KEY,
                    reason: "Serial head is absent after a Serial commit was materialized"
                        .to_string(),
                });
            }
            let root =
                super::store_objects::load_store_protocol_root_at_hash(storage, store_root_hash)
                    .await?
                    .ok_or_else(|| StoreOutboundError::InvalidState {
                        key: crate::database::STORE_ROOT_HASH_STATE_KEY,
                        reason: "Store protocol root is absent".to_string(),
                    })?
                    .value;
            SerialAuthorizationState::from_founder(store_root_hash, &root.founder)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
        }
    }?;
    Ok(SerialAuthorizationSnapshot {
        base: observed.position(),
        authorization,
    })
}

pub(crate) async fn prepare_serial_control(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    control: StoreControl,
    keypair: &UserKeypair,
) -> Result<PreparedSerialControl, StoreOutboundError> {
    let store_root_hash = required_store_root_hash(db).await?;
    let snapshot = current_serial_authorization_snapshot(db, storage, coordination).await?;
    let base = snapshot.base;
    let seq = base.as_ref().map_or(1, |position| position.seq + 1);
    let commit = StoreBatchCommit::signed_with_control(
        store_root_hash,
        db.new_write_id(),
        device_id.to_string(),
        StoreCommitOrder::Serial {
            seq,
            previous_commit_hash: base.as_ref().map(|position| position.commit_hash),
        },
        None,
        Some(control),
        db.schema_version(),
        &[],
        keypair,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let authorization_after = snapshot
        .authorization
        .authorize_and_apply(&commit)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreSerialHead::signed(
        store_root_hash,
        Some(commit.position()),
        Some(commit.write_id.clone()),
        keypair,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok(PreparedSerialControl {
        base,
        commit,
        head,
        authorization_after,
    })
}

pub(crate) async fn activate_serial_control(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    prepared: &PreparedSerialControl,
) -> Result<(), StoreOutboundError> {
    activate_serial_commit_head(
        db,
        storage,
        coordination,
        prepared.base.clone(),
        &prepared.commit,
        &prepared.head,
    )
    .await?;
    db.materialize_serial_control_commit(
        prepared.commit.clone(),
        prepared.authorization_after.clone(),
    )
    .await?;
    Ok(())
}

pub(crate) async fn activate_serial_commit_head(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    base: Option<crate::sync::store_commit::CommitPosition>,
    commit: &StoreBatchCommit,
    head: &StoreSerialHead,
) -> Result<(), StoreOutboundError> {
    append_and_verify(
        storage,
        &super::storage::ProtocolObjectContext::store(
            commit.store_root_hash,
            super::storage::ProtocolObjectDomain::StoreCommit,
        ),
        &commit_semantic_prefix(SERIAL_STREAM_ID, commit.seq(), commit.commit_hash()),
        ".json",
        &commit.to_bytes(),
    )
    .await?;
    let observed = observe_serial_head(db, coordination).await?;
    if observed.head() == Some(head) {
        return Ok(());
    }
    if observed.position() != base {
        return Err(StoreOutboundError::SerialControlConflict {
            expected: base.clone(),
            current: observed.position(),
        });
    }
    let activation = match observed.version() {
        None => coordination
            .create_head(serial_head_key(), &head.to_bytes())
            .await
            .map_err(|error| match error {
                CreateHeadError::AlreadyExists => None,
                CreateHeadError::Coordination(error) => {
                    Some(StoreOutboundError::Coordination(error))
                }
            }),
        Some(version) => coordination
            .replace_head(serial_head_key(), version, &head.to_bytes())
            .await
            .map_err(|error| match error {
                ReplaceHeadError::VersionMismatch => None,
                ReplaceHeadError::Coordination(error) => {
                    Some(StoreOutboundError::Coordination(error))
                }
            }),
    };
    if activation
        .as_ref()
        .is_ok_and(|activated| activated.bytes == head.to_bytes())
    {
        return Ok(());
    }
    let after = observe_serial_head(db, coordination).await?;
    if after.head() == Some(head) {
        return Ok(());
    }
    if let Err(Some(error)) = activation {
        return Err(error);
    }
    Err(StoreOutboundError::SerialControlConflict {
        expected: base,
        current: after.position(),
    })
}

impl SerialHeadObservation {
    fn head(&self) -> Option<&StoreSerialHead> {
        match self {
            Self::Absent => None,
            Self::Present { head, .. } => Some(head),
        }
    }

    fn version(&self) -> Option<&VersionToken> {
        match self {
            Self::Absent => None,
            Self::Present { version, .. } => Some(version),
        }
    }

    fn position(&self) -> Option<crate::sync::store_commit::CommitPosition> {
        self.head().and_then(|head| head.commit.clone())
    }
}

#[doc(hidden)]
pub async fn current_serial_head_position(
    db: &Database,
    coordination: &dyn CoordinationStorage,
) -> Result<Option<crate::sync::store_commit::CommitPosition>, StoreOutboundError> {
    Ok(observe_serial_head(db, coordination).await?.position())
}

async fn observe_serial_head(
    db: &Database,
    coordination: &dyn CoordinationStorage,
) -> Result<SerialHeadObservation, StoreOutboundError> {
    let store_root_hash = required_store_root_hash(db).await?;
    match coordination.read_head(serial_head_key()).await {
        Ok(object) => {
            let head = StoreSerialHead::parse(&object.bytes, store_root_hash).map_err(|error| {
                StoreOutboundError::InvalidState {
                    key: crate::database::STORE_ROOT_HASH_STATE_KEY,
                    reason: format!("Serial head: {error}"),
                }
            })?;
            Ok(SerialHeadObservation::Present {
                head,
                version: object.version,
            })
        }
        Err(CoordinationError::NotFound(_)) => Ok(SerialHeadObservation::Absent),
        Err(error) => Err(StoreOutboundError::Coordination(error)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare_serial_store_branch(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    cancel: Option<&super::service::HostUploadCloud<'_>>,
) -> Result<bool, StoreOutboundError> {
    let Some(branch) = db.reserve_serial_store_branch().await? else {
        return Ok(false);
    };
    let branch_id = branch.branch_id.clone();
    let branch_base = branch.base.clone();
    let snapshot = match current_serial_authorization_snapshot(db, storage, coordination).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            release_serial_preparation_after_error(db, branch_id, branch_base, &error).await?;
            return Err(error);
        }
    };
    if snapshot.base != branch.base {
        db.mark_serial_branch_conflict(branch.branch_id, branch.base, snapshot.base)
            .await?;
        return Ok(false);
    }
    let preparation = async {
        if !snapshot
            .authorization
            .membership
            .can_write(&crate::keys::public_key_hex(keypair))
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "local Serial identity is not a current writer".to_string(),
            ));
        }
        let store_root_hash = required_store_root_hash(db).await?;
        let mut predecessor = branch.base.clone();
        let mut prepared = Vec::with_capacity(branch.writes.len());
        for write in branch.writes {
            if !write.changeset.is_empty() && write.inverse_changeset.is_empty() {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "Serial write {} has no inverse changeset",
                    write.write_id
                )));
            }
            let payload = super::service::prepare_store_payload(
                db,
                storage,
                &write.blob_facts,
                keypair,
                store_dir,
                None,
                cancel,
            )
            .await
            .map_err(StoreOutboundError::Preparation)?;
            let seq = predecessor
                .as_ref()
                .map_or(1, |position| position.seq.saturating_add(1));
            let commit = StoreBatchCommit::signed(
                store_root_hash,
                write.write_id.clone(),
                device_id.to_string(),
                StoreCommitOrder::Serial {
                    seq,
                    previous_commit_hash: predecessor.as_ref().map(|position| position.commit_hash),
                },
                payload.membership_grant,
                db.schema_version(),
                &write.changeset,
                keypair,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            predecessor = Some(commit.position());
            prepared.push(SerialStoreWritePreparationEntry {
                write_id: write.write_id,
                package_bytes: write.changeset,
                commit,
                blob_manifest: payload.blob_manifest,
                local_cleanup: payload.local_cleanup,
                completion: payload.completion,
            });
        }
        let tip = prepared
            .last()
            .ok_or_else(|| StoreOutboundError::InvalidOutbound("Serial branch is empty".into()))?;
        let head = StoreSerialHead::signed(
            store_root_hash,
            Some(tip.commit.position()),
            Some(tip.write_id.clone()),
            keypair,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        db.prepare_serial_store_branch_commit(SerialStoreWritePreparation {
            branch_id: branch.branch_id,
            base: branch.base,
            writes: prepared,
            head,
        })
        .await?;
        Ok::<(), StoreOutboundError>(())
    }
    .await;
    match preparation {
        Ok(()) => Ok(true),
        Err(error) => {
            release_serial_preparation_after_error(db, branch_id, branch_base, &error).await?;
            Err(error)
        }
    }
}

async fn release_serial_preparation_after_error(
    db: &Database,
    branch_id: crate::PendingBranchId,
    base: Option<crate::sync::store_commit::CommitPosition>,
    error: &StoreOutboundError,
) -> Result<(), StoreOutboundError> {
    let status = blocked_status(error)
        .map(crate::WriteStatus::Blocked)
        .unwrap_or(crate::WriteStatus::Pending);
    db.release_serial_store_branch_reservation(branch_id, base, status)
        .await
        .map_err(Into::into)
}

fn serial_head_activates_branch(
    observed: &SerialHeadObservation,
    branch: &PreparedSerialStoreBranch,
) -> bool {
    observed.head().is_some_and(|head| {
        head.commit == branch.head.value.commit
            && head.tip_write_id == branch.head.value.tip_write_id
    })
}

async fn conflict_serial_branch(
    db: &Database,
    branch: PreparedSerialStoreBranch,
    current: Option<crate::sync::store_commit::CommitPosition>,
) -> Result<u64, StoreOutboundError> {
    db.mark_serial_branch_conflict(branch.branch_id, branch.base, current)
        .await?;
    Ok(0)
}

async fn drain_serial_store_branch(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
) -> Result<u64, StoreOutboundError> {
    let Some(branch) = db.prepared_serial_store_branch().await? else {
        return Ok(0);
    };
    let observed = observe_serial_head(db, coordination).await?;
    if serial_head_activates_branch(&observed, &branch) {
        let tip =
            branch.head.value.commit.clone().ok_or_else(|| {
                StoreOutboundError::InvalidOutbound("Serial tip is absent".into())
            })?;
        let tip_write_id = branch.head.value.tip_write_id.clone().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound("Serial tip write is absent".into())
        })?;
        return db
            .complete_prepared_serial_branch(tip, tip_write_id)
            .await
            .map_err(Into::into);
    }
    if observed.position() != branch.base {
        let current = observed.position();
        return conflict_serial_branch(db, branch, current).await;
    }
    let store_root_hash = required_store_root_hash(db).await?;
    for write in &branch.writes {
        validate_manifest(storage, &write.blob_manifest).await?;
        let commit = StoreBatchCommit::parse_at(
            &write.commit.bytes,
            store_root_hash,
            crate::WritePolicy::Serial,
            SERIAL_STREAM_ID,
            write.commit.value.seq(),
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        if commit != write.commit.value {
            return Err(StoreOutboundError::InvalidOutbound(
                "stored Serial commit differs from its exact signed bytes".to_string(),
            ));
        }
        commit
            .verify_store_package(&write.package_bytes)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let package = required_store_package(&commit)?;
        append_and_verify(
            storage,
            &super::storage::ProtocolObjectContext::store(
                store_root_hash,
                super::storage::ProtocolObjectDomain::StorePackage,
            ),
            &package.object_key,
            ".pkg",
            &write.package_bytes,
        )
        .await?;
        append_and_verify(
            storage,
            &super::storage::ProtocolObjectContext::store(
                store_root_hash,
                super::storage::ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(SERIAL_STREAM_ID, commit.seq(), commit.commit_hash()),
            ".json",
            &write.commit.bytes,
        )
        .await?;
    }
    let activation = match observed.version() {
        None => coordination
            .create_head(serial_head_key(), &branch.head.bytes)
            .await
            .map_err(|error| match error {
                CreateHeadError::AlreadyExists => None,
                CreateHeadError::Coordination(error) => {
                    Some(StoreOutboundError::Coordination(error))
                }
            }),
        Some(version) => coordination
            .replace_head(serial_head_key(), version, &branch.head.bytes)
            .await
            .map_err(|error| match error {
                ReplaceHeadError::VersionMismatch => None,
                ReplaceHeadError::Coordination(error) => {
                    Some(StoreOutboundError::Coordination(error))
                }
            }),
    };
    match activation {
        Ok(activated) if activated.bytes == branch.head.bytes => {}
        Ok(_) => {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial head readback differs from exact prepared bytes".to_string(),
            ));
        }
        Err(error) => {
            let after = observe_serial_head(db, coordination).await?;
            if !serial_head_activates_branch(&after, &branch) {
                if after.position() != branch.base {
                    let current = after.position();
                    return conflict_serial_branch(db, branch, current).await;
                }
                if let Some(error) = error {
                    return Err(error);
                }
                return Err(StoreOutboundError::InvalidOutbound(
                    "Serial head compare-and-swap lost without an activated successor".to_string(),
                ));
            }
        }
    }
    let tip = branch
        .head
        .value
        .commit
        .clone()
        .ok_or_else(|| StoreOutboundError::InvalidOutbound("Serial tip is absent".into()))?;
    let tip_write_id =
        branch.head.value.tip_write_id.clone().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound("Serial tip write is absent".into())
        })?;
    db.complete_prepared_serial_branch(tip, tip_write_id)
        .await
        .map_err(Into::into)
}

fn blocked_status(error: &StoreOutboundError) -> Option<crate::WriteBlock> {
    match error {
        StoreOutboundError::Database(_)
        | StoreOutboundError::BlobStorage { .. }
        | StoreOutboundError::Coordination(_) => None,
        StoreOutboundError::MissingSerialCoordination => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::SerialControlConflict { .. } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Object(StoreObjectError::Storage(_))
        | StoreOutboundError::Object(StoreObjectError::CandidateUnreadable { .. })
        | StoreOutboundError::Object(StoreObjectError::AppendReadbackMismatch { .. }) => None,
        StoreOutboundError::MissingBlob { namespace, id } => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::LocalUserBlob { namespace, id } => {
            Some(crate::WriteBlock::LocalUserBlob {
                namespace: namespace.clone(),
                id: id.clone(),
            })
        }
        StoreOutboundError::MissingState { key } => Some(crate::WriteBlock::InvalidProtocolState {
            reason: format!("Store protocol state {key:?} is absent"),
        }),
        StoreOutboundError::InvalidState { key, reason } => {
            Some(crate::WriteBlock::InvalidProtocolState {
                reason: format!("Store protocol state {key:?} is invalid: {reason}"),
            })
        }
        StoreOutboundError::InvalidOutbound(_) | StoreOutboundError::Object(_) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Preparation(super::service::SyncCycleError::LocalUserBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::LocalUserBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::MissingBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::Gate(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::AssetScan(_)) => {
            Some(crate::WriteBlock::InvalidPackage {
                reason: error.to_string(),
            })
        }
        StoreOutboundError::Preparation(super::service::SyncCycleError::AssetUpload(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::Storage { .. }) => None,
    }
}

async fn record_preparation_failure(
    db: &Database,
    write_id: &crate::WriteId,
    error: &StoreOutboundError,
) -> Result<(), StoreOutboundError> {
    let Some(block) = blocked_status(error) else {
        return Ok(());
    };
    db.set_write_status(write_id, crate::WriteStatus::Blocked(block))
        .await
        .map_err(|status_error| {
            StoreOutboundError::Database(format!(
                "record blocked status for write {write_id} after {error}: {status_error}"
            ))
        })
}

fn validate_outbound(
    batch: &PreparedStoreWriteCommit,
    store_root_hash: ObjectHash,
    device_id: &str,
) -> Result<(), StoreOutboundError> {
    let commit = StoreBatchCommit::parse_at(
        &batch.commit.bytes,
        store_root_hash,
        crate::WritePolicy::MergeConcurrent,
        device_id,
        batch.commit.value.seq(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if commit != batch.commit.value {
        return Err(StoreOutboundError::InvalidOutbound(
            "stored commit differs from its exact signed bytes".to_string(),
        ));
    }
    commit
        .verify_store_package(&batch.package_bytes)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head =
        StoreDeviceHead::parse_at(&batch.head.bytes, store_root_hash, device_id, commit.seq())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if head != batch.head.value || head.position.as_ref() != Some(&commit.position()) {
        return Err(StoreOutboundError::InvalidOutbound(
            "stored head differs from its exact signed bytes".to_string(),
        ));
    }
    Ok(())
}

fn required_store_package(
    commit: &StoreBatchCommit,
) -> Result<&super::store_commit::StorePackageRef, StoreOutboundError> {
    commit.store_package.as_ref().ok_or_else(|| {
        StoreOutboundError::InvalidOutbound("prepared row write has no Store package".to_string())
    })
}

async fn validate_manifest(
    storage: &dyn SyncStorage,
    manifest: &StoreBlobManifest,
) -> Result<(), StoreOutboundError> {
    for blob in &manifest.blobs {
        let exists = storage
            .blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref())
            .await
            .map_err(|source| StoreOutboundError::BlobStorage {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
                source,
            })?;
        if !exists {
            return Err(StoreOutboundError::MissingBlob {
                namespace: blob.namespace.clone(),
                id: blob.id.clone(),
            });
        }
    }
    Ok(())
}

async fn required_store_root_hash(db: &Database) -> Result<ObjectHash, StoreOutboundError> {
    db.required_store_root_hash_mapped(
        || StoreOutboundError::MissingState {
            key: crate::database::STORE_ROOT_HASH_STATE_KEY,
        },
        |reason| StoreOutboundError::InvalidState {
            key: crate::database::STORE_ROOT_HASH_STATE_KEY,
            reason,
        },
        StoreOutboundError::from,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::storage::VersionedObject;
    use crate::sync::test_helpers::{
        host_exec, open_serial_test_db, open_test_db, publish_test_serial_store_protocol_root,
        publish_test_store_protocol_root, temp_store_dir,
    };

    struct FailFirstCoordinationRead<'a> {
        inner: &'a dyn CoordinationStorage,
        failed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CoordinationStorage for FailFirstCoordinationRead<'_> {
        async fn read_head(&self, key: &str) -> Result<VersionedObject, CoordinationError> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                return Err(CoordinationError::Storage(
                    "injected coordination read failure".to_string(),
                ));
            }
            self.inner.read_head(key).await
        }

        async fn create_head(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<VersionedObject, CreateHeadError> {
            self.inner.create_head(key, bytes).await
        }

        async fn replace_head(
            &self,
            key: &str,
            expected: &VersionToken,
            bytes: &[u8],
        ) -> Result<VersionedObject, ReplaceHeadError> {
            self.inner.replace_head(key, expected, bytes).await
        }

        async fn delete_probe_head(&self, key: &str) -> Result<(), CoordinationError> {
            self.inner.delete_probe_head(key).await
        }
    }

    #[tokio::test]
    async fn two_serial_writes_publish_as_one_branch_with_one_head_cas() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "serial-outbound",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("serial-outbound")))
        .with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_serial_test_db();
        let store_root_hash = publish_test_serial_store_protocol_root(
            &db,
            &storage,
            "serial-outbound",
            "dev-writer",
            &keypair,
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-a', 'first', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-b', 'second', NULL, 1, '0000000001001-0000-writer', '2026-01-01')",
        )
        .await;
        let pending = db.pending_writes().await.expect("pending Serial writes");
        assert_eq!(pending.len(), 2);
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("prepare one Serial branch"));

        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .expect("activate one Serial branch"),
            2,
        );
        assert_eq!(home.head_mutation_count(), 1);
        let first = db
            .exact_materialized_hash(SERIAL_STREAM_ID, 1)
            .await
            .unwrap()
            .expect("first Serial commit");
        let second = db
            .exact_materialized_hash(SERIAL_STREAM_ID, 2)
            .await
            .unwrap()
            .expect("second Serial commit");
        assert!(matches!(
            db.write_status(&pending[0].write_id).await.unwrap(),
            crate::WriteStatus::Published(crate::PublishedPosition::Serial { position })
                if position.seq == 1 && position.commit_hash == first
        ));
        assert!(matches!(
            db.write_status(&pending[1].write_id).await.unwrap(),
            crate::WriteStatus::Published(crate::PublishedPosition::Serial { position })
                if position.seq == 2 && position.commit_hash == second
        ));
        let head = storage
            .serial_coordination()
            .unwrap()
            .read_head(serial_head_key())
            .await
            .expect("read activated Serial head");
        let head = StoreSerialHead::parse(&head.bytes, store_root_hash).unwrap();
        assert_eq!(head.commit.unwrap().commit_hash, second);
        assert_eq!(head.tip_write_id, Some(pending[1].write_id.clone()));
    }

    async fn serial_fixture(
        name: &str,
    ) -> (
        InMemoryCloudHome,
        CloudSyncStorage,
        Database,
        UserKeypair,
        ObjectHash,
        Vec<crate::PendingWrite>,
    ) {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            name,
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(name)))
        .with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_serial_test_db();
        let root =
            publish_test_serial_store_protocol_root(&db, &storage, name, "dev-writer", &keypair)
                .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-race-a', 'first', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-race-b', 'second', NULL, 1, '0000000001001-0000-writer', '2026-01-01')",
        )
        .await;
        let pending = db.pending_writes().await.unwrap();
        (home, storage, db, keypair, root, pending)
    }

    async fn competing_head(
        storage: &CloudSyncStorage,
        root: ObjectHash,
        signer: &UserKeypair,
        marker: &str,
    ) -> StoreSerialHead {
        let write_id = crate::WriteId::from_generated(format!("competitor-{marker}"));
        let package_bytes = marker.as_bytes();
        let commit = StoreBatchCommit::signed(
            root,
            write_id.clone(),
            format!("competitor-{marker}"),
            crate::StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            1,
            package_bytes,
            signer,
        )
        .expect("sign competing Serial commit");
        let package = commit.store_package.as_ref().expect("Store package");
        crate::sync::store_objects::append_and_verify(
            storage,
            &crate::sync::storage::ProtocolObjectContext::store(
                root,
                crate::sync::storage::ProtocolObjectDomain::StorePackage,
            ),
            &package.object_key,
            ".pkg",
            package_bytes,
        )
        .await
        .expect("publish competing Serial package");
        crate::sync::store_objects::append_and_verify(
            storage,
            &crate::sync::storage::ProtocolObjectContext::store(
                root,
                crate::sync::storage::ProtocolObjectDomain::StoreCommit,
            ),
            &crate::sync::store_commit::commit_semantic_prefix(
                SERIAL_STREAM_ID,
                1,
                commit.commit_hash(),
            ),
            ".json",
            &commit.to_bytes(),
        )
        .await
        .expect("publish competing Serial commit");
        StoreSerialHead::signed(root, Some(commit.position()), Some(write_id), signer)
            .expect("sign competing Serial head")
    }

    #[tokio::test]
    async fn changed_serial_base_marks_the_whole_branch_conflict_before_uploading_candidates() {
        let (home, storage, db, keypair, root, pending) =
            serial_fixture("serial-changed-base").await;
        let other = competing_head(&storage, root, &keypair, "changed-base").await;
        storage
            .serial_coordination()
            .unwrap()
            .create_head(serial_head_key(), &other.to_bytes())
            .await
            .unwrap();
        let immutable_before = home.append_count();
        let (_temp, store_dir) = temp_store_dir();

        assert!(!prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("detect changed Serial base"));

        assert_eq!(home.append_count(), immutable_before);
        assert_eq!(home.head_mutation_count(), 1);
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Conflict(crate::SerializationConflict {
                    base: None,
                    current: Some(ref current),
                    ..
                }) if Some(current.clone()) == other.commit
            ));
        }
    }

    #[tokio::test]
    async fn lost_successful_serial_head_response_completes_from_the_exact_authoritative_tip() {
        let (home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-lost-success").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .unwrap());
        home.fail_next_head_mutation_after_visibility();

        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .expect("recognize exact tip after lost response"),
            2,
        );
        assert_eq!(home.head_mutation_count(), 1);
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Published(crate::PublishedPosition::Serial { .. })
            ));
        }
    }

    #[tokio::test]
    async fn different_tip_after_ambiguous_serial_response_conflicts_the_whole_branch() {
        let (home, storage, db, keypair, root, pending) =
            serial_fixture("serial-lost-to-other").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .unwrap());
        let other = competing_head(&storage, root, &keypair, "other-winner").await;
        home.replace_after_next_head_mutation(other.to_bytes());

        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .expect("record competing authoritative tip"),
            0,
        );
        assert_eq!(home.head_mutation_count(), 2);
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Conflict(crate::SerializationConflict {
                    current: Some(ref current),
                    ..
                }) if Some(current.clone()) == other.commit
            ));
        }
    }

    #[tokio::test]
    async fn serial_preparation_transport_failure_returns_the_reserved_branch_to_pending() {
        let (_home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-preparation-retry").await;
        let coordination = FailFirstCoordinationRead {
            inner: storage.serial_coordination().unwrap(),
            failed: AtomicBool::new(false),
        };
        let (_temp, store_dir) = temp_store_dir();

        let result = prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(&coordination),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await;

        assert!(matches!(result, Err(StoreOutboundError::Coordination(_))));
        for write in pending {
            assert_eq!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Pending
            );
        }
    }

    #[tokio::test]
    async fn serial_preparation_protocol_failure_blocks_the_reserved_branch() {
        let (_home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-preparation-blocked").await;
        db.delete_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
            .await
            .unwrap();
        let (_temp, store_dir) = temp_store_dir();

        let result = prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(StoreOutboundError::MissingState { .. })
        ));
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { .. })
            ));
        }
    }

    #[tokio::test]
    async fn write_arriving_during_serial_publication_rebases_after_activation() {
        let (_home, storage, db, keypair, _root, _pending) =
            serial_fixture("serial-publishing-success-suffix").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .unwrap());
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-race-c', 'third', NULL, 1, '0000000001002-0000-writer', '2026-01-01')",
        )
        .await;
        let suffix = db.pending_writes().await.unwrap().pop().unwrap().write_id;

        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .unwrap(),
            2
        );
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("prepare rebased suffix"));
        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .unwrap(),
            1
        );
        assert!(matches!(
            db.write_status(&suffix).await.unwrap(),
            crate::WriteStatus::Published(crate::PublishedPosition::Serial { position })
                if position.seq == 3
        ));
    }

    #[tokio::test]
    async fn write_arriving_during_serial_publication_conflicts_with_the_branch_on_cas_loss() {
        let (home, storage, db, keypair, root, pending) =
            serial_fixture("serial-publishing-lost-suffix").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .unwrap());
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-race-c', 'third', NULL, 1, '0000000001002-0000-writer', '2026-01-01')",
        )
        .await;
        let all_writes = db.pending_writes().await.unwrap();
        let other = competing_head(&storage, root, &keypair, "suffix-lost").await;
        home.replace_after_next_head_mutation(other.to_bytes());

        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .unwrap(),
            0
        );
        let expected_branch = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        assert_eq!(all_writes.len(), 3);
        for write in all_writes {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Conflict(crate::SerializationConflict { branch_id, .. })
                    if branch_id == expected_branch
            ));
        }
    }

    #[tokio::test]
    async fn missing_serial_head_fails_when_a_materialized_position_exists() {
        let (home, storage, db, keypair, _root, _pending) =
            serial_fixture("serial-missing-head-after-materialization").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .unwrap());
        drain_store_writes_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
        )
        .await
        .unwrap();
        home.remove(serial_head_key());

        assert!(matches!(
            current_serial_authorization(&db, &storage, storage.serial_coordination().unwrap())
                .await,
            Err(StoreOutboundError::InvalidState { .. })
        ));
    }

    struct PreparedWriteFixture {
        home: InMemoryCloudHome,
        storage: CloudSyncStorage,
        db: Database,
        write_id: crate::WriteId,
        position_hash: ObjectHash,
    }

    async fn prepared_write_fixture(copy_source: &str) -> PreparedWriteFixture {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "outbound-crash-test",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)));
        let db = open_test_db();
        publish_test_store_protocol_root(
            &db,
            &storage,
            "outbound-crash-test",
            "dev-writer",
            &keypair,
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'outbound', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("prepare outbound write"));
        let batch = db
            .oldest_prepared_store_write()
            .await
            .expect("read prepared write")
            .expect("prepared write exists");
        PreparedWriteFixture {
            home,
            storage,
            db,
            write_id: batch.commit.value.write_id.clone(),
            position_hash: batch.commit.value.commit_hash(),
        }
    }

    fn count_prefix(home: &InMemoryCloudHome, prefix: &str) -> usize {
        home.appended_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .count()
    }

    #[tokio::test]
    async fn failures_before_package_commit_and_head_keep_the_exact_prepared_write_retryable() {
        for failed_call in 1..=3 {
            let fixture = prepared_write_fixture(&format!("before-{failed_call}")).await;
            fixture.home.fail_append_before_call(failed_call);
            let first = drain_store_writes(&fixture.db, &fixture.storage).await;
            assert!(first.is_err(), "append call {failed_call} fails");
            assert_eq!(
                fixture.db.write_status(&fixture.write_id).await.unwrap(),
                crate::WriteStatus::Pending,
                "transport failure returns the write to Pending",
            );
            assert!(
                fixture
                    .db
                    .oldest_prepared_store_write()
                    .await
                    .unwrap()
                    .is_some(),
                "the exact prepared write remains after append call {failed_call}",
            );
            assert_eq!(
                fixture
                    .db
                    .exact_materialized_hash("dev-writer", 1)
                    .await
                    .unwrap(),
                None,
                "local position cannot advance before a verified head",
            );
            assert_eq!(
                count_prefix(&fixture.home, "store-v1/packages/dev-writer/1/"),
                usize::from(failed_call > 1),
            );
            assert_eq!(
                count_prefix(&fixture.home, "store-v1/commits/dev-writer/1/"),
                usize::from(failed_call > 2),
            );
            assert_eq!(
                count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
                0,
            );

            assert_eq!(
                drain_store_writes(&fixture.db, &fixture.storage)
                    .await
                    .expect("retry exact outbound batch"),
                1,
            );
            assert!(fixture
                .db
                .oldest_prepared_store_write()
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                fixture
                    .db
                    .exact_materialized_hash("dev-writer", 1)
                    .await
                    .unwrap(),
                Some(fixture.position_hash),
            );
            assert!(matches!(
                fixture.db.write_status(&fixture.write_id).await.unwrap(),
                crate::WriteStatus::Published(crate::PublishedPosition::MergeConcurrent {
                    device_id,
                    position,
                }) if device_id == "dev-writer"
                        && position.seq == 1
                        && position.commit_hash == fixture.position_hash
            ));
        }
    }

    #[tokio::test]
    async fn append_readback_mismatch_returns_the_prepared_write_to_pending() {
        let fixture = prepared_write_fixture("readback-mismatch").await;
        fixture.home.corrupt_append_readback_on_call(1);

        let result = drain_store_writes(&fixture.db, &fixture.storage).await;

        assert!(matches!(
            result,
            Err(StoreOutboundError::Object(
                StoreObjectError::AppendReadbackMismatch { .. }
            ))
        ));
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Pending,
            "a provider readback mismatch can be retried from the owned exact bytes",
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn ambiguous_failure_after_head_leaves_visible_head_and_retries_identical_bytes() {
        let fixture = prepared_write_fixture("after-head").await;
        fixture.home.fail_append_after_call(3);
        let first = drain_store_writes(&fixture.db, &fixture.storage).await;
        assert!(first.is_err());
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Pending,
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/packages/dev-writer/1/"),
            1
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/commits/dev-writer/1/"),
            1
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
            1
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            fixture
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            None
        );

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("retry ambiguous head append"),
            1
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/packages/dev-writer/1/"),
            2
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/commits/dev-writer/1/"),
            2
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
            2
        );
        let loaded = super::super::store_objects::load_commit_slot(
            &fixture.storage,
            fixture.db.required_store_root_hash().await.unwrap(),
            "dev-writer",
            1,
        )
        .await
        .expect("coalesce retry copies")
        .expect("commit exists");
        assert_eq!(loaded.copies.len(), 2);
        assert_eq!(loaded.semantic_hash, fixture.position_hash);
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(crate::PublishedPosition::MergeConcurrent {
                position,
                ..
            }) if position.seq == 1
                    && position.commit_hash == fixture.position_hash
        ));
    }

    #[tokio::test]
    async fn local_completion_failure_rolls_back_position_and_retries_after_visible_head() {
        let fixture = prepared_write_fixture("completion").await;
        fixture
            .db
            .call(|conn| {
                conn.execute_batch(
                    "CREATE TEMP TRIGGER fail_outbound_completion \
                     BEFORE UPDATE OF prepared ON store_writes \
                     WHEN OLD.prepared IS NOT NULL AND NEW.prepared IS NULL \
                     BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("install completion fault");
        let first = drain_store_writes(&fixture.db, &fixture.storage).await;
        assert!(matches!(first, Err(StoreOutboundError::Database(_))));
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Pending,
        );
        assert_eq!(
            count_prefix(&fixture.home, "store-v1/heads/dev-writer/1/"),
            1
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            fixture
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            None,
            "position and prepared-state clearing share the failed transaction",
        );

        fixture
            .db
            .call(|conn| {
                conn.execute_batch("DROP TRIGGER fail_outbound_completion")
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("remove completion fault");
        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("retry local completion"),
            1
        );
        assert_eq!(
            fixture
                .db
                .exact_materialized_hash("dev-writer", 1)
                .await
                .unwrap(),
            Some(fixture.position_hash),
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(crate::PublishedPosition::MergeConcurrent {
                position,
                ..
            }) if position.seq == 1
                    && position.commit_hash == fixture.position_hash
        ));
    }

    #[tokio::test]
    async fn restart_blocks_a_prepared_write_when_its_store_root_is_unusable() {
        for invalid_root in [None, Some("not-an-object-hash")] {
            let temp = tempfile::tempdir().expect("temp dir");
            let path = temp.path().join("store.sqlite3");
            let open = || {
                Database::open(
                    &path,
                    crate::sync::test_helpers::test_synced_tables(),
                    crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                    crate::blob::TransferLimits::serial(),
                    crate::WritePolicy::MergeConcurrent,
                    "dev-writer".to_string(),
                    &crate::sync::test_helpers::test_migrations(),
                )
                .expect("open test database")
                .0
            };
            let home = InMemoryCloudHome::new();
            let keypair = UserKeypair::generate();
            let storage = CloudSyncStorage::new(
                Arc::new(home),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "prepared-root-status",
                keypair.clone(),
            )
            .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("root-status")));
            let db = open();
            publish_test_store_protocol_root(
                &db,
                &storage,
                "prepared-root-status",
                "dev-writer",
                &keypair,
            )
            .await;
            host_exec(
                &db,
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('root-status', 'outbound', NULL, 1, \
                         '0000000001000-0000-writer', '2026-01-01')",
            )
            .await;
            let (_store_temp, store_dir) = temp_store_dir();
            assert!(prepare_pending_store_write(
                &db,
                &storage,
                "dev-writer",
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                None,
                None,
            )
            .await
            .expect("prepare write"));
            let write_id = db
                .oldest_prepared_store_write()
                .await
                .expect("load prepared write")
                .expect("prepared write exists")
                .commit
                .value
                .write_id;
            db.call(move |conn| {
                match invalid_root {
                    Some(value) => conn.execute(
                        "UPDATE protocol_state SET value = ?2 WHERE key = ?1",
                        [crate::database::STORE_ROOT_HASH_STATE_KEY, value],
                    ),
                    None => conn.execute(
                        "DELETE FROM protocol_state WHERE key = ?1",
                        [crate::database::STORE_ROOT_HASH_STATE_KEY],
                    ),
                }
                .map(|_| ())
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("make root unusable");
            drop(db);

            let reopened = open();
            let result = drain_store_writes(&reopened, &storage).await;
            match (invalid_root, result) {
                (None, Err(StoreOutboundError::MissingState { key })) => {
                    assert_eq!(key, crate::database::STORE_ROOT_HASH_STATE_KEY);
                }
                (Some(_), Err(StoreOutboundError::InvalidState { key, reason })) => {
                    assert_eq!(key, crate::database::STORE_ROOT_HASH_STATE_KEY);
                    assert!(!reason.is_empty());
                }
                (_, result) => panic!("unexpected Store root failure: {result:?}"),
            }
            assert!(matches!(
                reopened.write_status(&write_id).await.expect("write status"),
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { reason })
                    if reason.contains(crate::database::STORE_ROOT_HASH_STATE_KEY)
            ));
        }
    }

    #[tokio::test]
    async fn blocked_write_requires_explicit_retry_before_production_revalidates_it() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "blocked-retry",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("blocked-retry")));
        let db = open_test_db();
        let root = publish_test_store_protocol_root(
            &db,
            &storage,
            "blocked-retry",
            "dev-writer",
            &keypair,
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('blocked-first', 'first', NULL, 1, \
                     '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('blocked-second', 'second', NULL, 1, \
                     '0000000001001-0000-writer', '2026-01-01')",
        )
        .await;
        let writes = db.pending_writes().await.expect("load pending writes");
        let first = writes[0].write_id.clone();
        let second = writes[1].write_id.clone();
        db.delete_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
            .await
            .expect("remove protocol root");
        let (_store_temp, store_dir) = temp_store_dir();

        assert!(matches!(
            prepare_pending_store_write(
                &db,
                &storage,
                "dev-writer",
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                None,
                None,
            )
            .await,
            Err(StoreOutboundError::MissingState { .. })
        ));
        assert_eq!(
            db.blocked_writes().await.expect("inspect blocked writes")[0].write_id,
            first
        );
        assert!(!prepare_pending_store_write(
            &db,
            &storage,
            "dev-writer",
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("a blocked oldest write stays blocked"));
        assert_eq!(
            db.write_status(&second).await.unwrap(),
            crate::WriteStatus::Pending
        );

        assert_eq!(
            db.retry_blocked_write(&first).await.unwrap(),
            vec![first.clone()]
        );
        assert!(matches!(
            prepare_pending_store_write(
                &db,
                &storage,
                "dev-writer",
                "2026-01-01T00:00:02Z",
                &keypair,
                &store_dir,
                None,
                None,
            )
            .await,
            Err(StoreOutboundError::MissingState { .. })
        ));
        assert!(matches!(
            db.write_status(&first).await.unwrap(),
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { .. })
        ));

        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &root.to_string(),
        )
        .await
        .expect("restore protocol root");
        assert!(matches!(
            db.write_status(&first).await.unwrap(),
            crate::WriteStatus::Blocked(_)
        ));
        db.retry_blocked_write(&first)
            .await
            .expect("explicit retry after repair");
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            "dev-writer",
            "2026-01-01T00:00:03Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("revalidate repaired write"));
        assert_eq!(
            db.write_status(&first).await.unwrap(),
            crate::WriteStatus::Publishing
        );
    }

    #[tokio::test]
    async fn discarding_a_blocked_write_atomically_reverses_its_unpublished_suffix() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "blocked-discard",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("blocked-discard")));
        let db = open_test_db();
        let root = publish_test_store_protocol_root(
            &db,
            &storage,
            "blocked-discard",
            "dev-writer",
            &keypair,
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('discard-first', 'first', NULL, 1, \
                     '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('discard-second', 'second', NULL, 1, \
                     '0000000001001-0000-writer', '2026-01-01')",
        )
        .await;
        let writes = db.pending_writes().await.unwrap();
        let first = writes[0].write_id.clone();
        let second = writes[1].write_id.clone();
        db.delete_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
            .await
            .unwrap();
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .is_err());

        assert_eq!(
            db.discard_blocked_write(&first).await.unwrap(),
            vec![first.clone(), second.clone()]
        );
        let note_count: i64 = db
            .call(|conn| {
                conn.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))
                    .map_err(crate::database::DbError::from)
            })
            .await
            .unwrap();
        assert_eq!(note_count, 0);
        assert!(db.pending_writes().await.unwrap().is_empty());
        for write_id in [first, second] {
            assert_eq!(
                db.write_status(&write_id).await.unwrap(),
                crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
            );
        }

        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &root.to_string(),
        )
        .await
        .unwrap();
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('after-discard', 'after', NULL, 1, \
                     '0000000001002-0000-writer', '2026-01-01')",
        )
        .await;
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            "dev-writer",
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("prepare write after discarded blocked writes"));
        assert_eq!(drain_store_writes(&db, &storage).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn retrying_one_blocked_serial_write_revalidates_the_whole_ordered_branch() {
        let (_home, storage, db, keypair, root, blocked) =
            serial_fixture("serial-blocked-retry").await;
        db.delete_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
            .await
            .unwrap();
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .is_err());
        assert_eq!(db.blocked_writes().await.unwrap().len(), 2);

        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-retry-later', 'later', NULL, 1, \
                     '0000000001002-0000-writer', '2026-01-01')",
        )
        .await;
        let branch = db.pending_writes().await.unwrap();
        assert_eq!(branch.len(), 3);
        assert_eq!(branch[2].status, crate::WriteStatus::Pending);

        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &root.to_string(),
        )
        .await
        .unwrap();
        assert_eq!(
            db.retry_blocked_write(&blocked[1].write_id).await.unwrap(),
            blocked
                .iter()
                .map(|write| write.write_id.clone())
                .collect::<Vec<_>>()
        );
        db.delete_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
            .await
            .unwrap();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .is_err());
        assert_eq!(db.blocked_writes().await.unwrap().len(), 3);
        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &root.to_string(),
        )
        .await
        .unwrap();
        assert_eq!(
            db.retry_blocked_write(&branch[2].write_id).await.unwrap(),
            branch
                .iter()
                .map(|write| write.write_id.clone())
                .collect::<Vec<_>>()
        );
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:02Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("revalidate repaired Serial branch"));
        for write in branch {
            assert_eq!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Publishing
            );
        }
    }

    #[tokio::test]
    async fn discarding_a_blocked_serial_branch_allows_a_new_branch_to_publish() {
        let (_home, storage, db, keypair, root, blocked) =
            serial_fixture("serial-blocked-discard").await;
        db.delete_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
            .await
            .unwrap();
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .is_err());
        assert_eq!(
            db.discard_blocked_write(&blocked[0].write_id)
                .await
                .unwrap()
                .len(),
            2
        );
        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &root.to_string(),
        )
        .await
        .unwrap();
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('after-serial-discard', 'after', NULL, 1, \
                     '0000000001002-0000-writer', '2026-01-01')",
        )
        .await;
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            "dev-writer",
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            None,
            None,
        )
        .await
        .expect("prepare new branch after discarded Serial branch"));
        assert_eq!(
            drain_store_writes_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
            )
            .await
            .unwrap(),
            1
        );
    }
}
