use super::*;
use crate::database::{
    PreparedProtocolObject, PreparedSerialStoreBranch, SerialStoreWritePreparation,
    SerialStoreWritePreparationEntry,
};
use crate::sync::membership::SerialAuthorizationState;
use crate::sync::storage::{
    BlobWriteAuthority, CoordinationError, CreateHeadError, PreparedExactObject,
    ProtocolObjectContext, ProtocolObjectDomain, ReplaceHeadError, VersionToken, VersionedObject,
};
use crate::sync::store_commit::{
    commit_semantic_prefix, serial_head_key, CandidateFamilyId, CirclePackageInput,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreCommitOperationsInput,
    StoreCommitOrder, StorePackageInput, StoreSerialHead, StoreSerialHeadState,
    StoreSerialPredecessor, SERIAL_STREAM_ID,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::*;

pub(crate) enum SerialHeadObservation {
    Absent,
    Present {
        head: StoreSerialHead,
        bytes: Vec<u8>,
        version: VersionToken,
    },
}

#[derive(Clone)]
pub(crate) struct SerialAuthorizationSnapshot {
    pub base: Option<StoreBatchCommitRef>,
    pub base_head: VersionedObject,
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
    let root_ref = required_store_root(db).await?;
    let observed = observe_serial_head(db, coordination).await?;
    let head = observed.head().ok_or(StoreOutboundError::MissingState {
        key: SERIAL_COORDINATION_HEAD,
    })?;
    let authorization = super::pull::load_serial_authorization_at_head(storage, &root_ref, head)
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let base = match observed.predecessor()? {
        StoreSerialPredecessor::Genesis { .. } => None,
        StoreSerialPredecessor::Commit(commit) => Some(commit),
    };
    Ok(SerialAuthorizationSnapshot {
        base,
        base_head: observed
            .versioned()
            .ok_or(StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD,
            })?,
        authorization,
    })
}

pub(crate) async fn activate_serial_commit_head(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    base_head: &VersionedObject,
    commit: &StoreBatchCommit,
    commit_prepared: &PreparedExactObject,
    commit_ref: &StoreBatchCommitRef,
    head: &StoreSerialHead,
) -> Result<crate::sync::store_commit::VerifiedStoreDeviceOperations, StoreOutboundError> {
    let observed = observe_serial_head(db, coordination).await?;
    let head_bytes = head.to_bytes();
    if observed.bytes() == Some(head_bytes.as_slice()) {
        let context = ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let prefix = commit_semantic_prefix(
            commit.candidate_family(),
            SERIAL_STREAM_ID,
            commit.seq(),
            commit.commit_hash(),
        );
        let persisted = storage
            .read_protocol_object(&context, &commit_ref.object, &prefix)
            .await
            .map_err(StoreObjectError::from)?;
        if persisted != commit.to_bytes() {
            return Err(StoreOutboundError::InvalidOutbound(
                "activated Serial head names different commit bytes".to_string(),
            ));
        }
        let root = required_store_root(db).await?;
        return crate::sync::store_pull::load_local_commit_device_operations(
            db, storage, &root, commit,
        )
        .await
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()));
    }
    if observed.versioned().as_ref() != Some(base_head) {
        let StoreCommitOrder::Serial {
            predecessor: expected,
            ..
        } = &commit.order
        else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial activation carries a Merge commit".to_string(),
            ));
        };
        return Err(StoreOutboundError::SerialControlConflict {
            expected: Box::new(expected.clone()),
            current: Box::new(observed.predecessor()?),
        });
    }
    storage
        .create_protocol_object(commit_prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let commit_context = ProtocolObjectContext::signed_plaintext(
        commit.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        SERIAL_STREAM_ID,
        commit.seq(),
        commit.commit_hash(),
    );
    let opened = storage
        .read_protocol_object(&commit_context, &commit_ref.object, &commit_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != commit.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial control commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    let root = required_store_root(db).await?;
    let device_operations =
        crate::sync::store_pull::load_local_commit_device_operations(db, storage, &root, commit)
            .await
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let activation = match observed.version() {
        None => coordination
            .create_head(serial_head_key(), &head_bytes)
            .await
            .map_err(|error| match error {
                CreateHeadError::AlreadyExists => None,
                CreateHeadError::Coordination(error) => {
                    Some(StoreOutboundError::Coordination(error))
                }
            }),
        Some(version) => coordination
            .replace_head(serial_head_key(), version, &head_bytes)
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
        .is_ok_and(|activated| activated.bytes == head_bytes)
    {
        return Ok(device_operations);
    }
    let after = observe_serial_head(db, coordination).await?;
    if after.bytes() == Some(head_bytes.as_slice()) {
        return Ok(device_operations);
    }
    let StoreCommitOrder::Serial {
        predecessor: expected,
        ..
    } = &commit.order
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial activation carries a Merge commit".to_string(),
        ));
    };
    Err(StoreOutboundError::SerialControlConflict {
        expected: Box::new(expected.clone()),
        current: Box::new(after.predecessor()?),
    })
}

impl SerialHeadObservation {
    pub(crate) fn head(&self) -> Option<&StoreSerialHead> {
        match self {
            Self::Absent => None,
            Self::Present { head, .. } => Some(head),
        }
    }

    pub(crate) fn version(&self) -> Option<&VersionToken> {
        match self {
            Self::Absent => None,
            Self::Present { version, .. } => Some(version),
        }
    }

    pub(crate) fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Absent => None,
            Self::Present { bytes, .. } => Some(bytes),
        }
    }

    pub(crate) fn predecessor(&self) -> Result<StoreSerialPredecessor, StoreOutboundError> {
        match self {
            Self::Absent => Err(StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD,
            }),
            Self::Present { head, .. } => match &head.state {
                StoreSerialHeadState::Genesis {
                    root,
                    founder_registration,
                } => Ok(StoreSerialPredecessor::Genesis {
                    root: root.clone(),
                    founder_registration: founder_registration.clone(),
                }),
                StoreSerialHeadState::Commit { commit, .. } => {
                    Ok(StoreSerialPredecessor::Commit(commit.clone()))
                }
            },
        }
    }

    pub(crate) fn versioned(&self) -> Option<VersionedObject> {
        match self {
            Self::Absent => None,
            Self::Present { bytes, version, .. } => Some(VersionedObject {
                bytes: bytes.clone(),
                version: version.clone(),
            }),
        }
    }
}

pub async fn current_serial_head_ref(
    db: &Database,
    coordination: &dyn CoordinationStorage,
) -> Result<Option<StoreBatchCommitRef>, StoreOutboundError> {
    match observe_serial_head(db, coordination).await?.predecessor()? {
        StoreSerialPredecessor::Genesis { .. } => Ok(None),
        StoreSerialPredecessor::Commit(commit) => Ok(Some(commit)),
    }
}

pub(crate) async fn observe_serial_head(
    db: &Database,
    coordination: &dyn CoordinationStorage,
) -> Result<SerialHeadObservation, StoreOutboundError> {
    let store_root_hash = required_store_root(db).await?.store_root_hash;
    match coordination.read_head(serial_head_key()).await {
        Ok(object) => {
            let unverified: StoreSerialHead =
                serde_json::from_slice(&object.bytes).map_err(|error| {
                    StoreOutboundError::InvalidState {
                        key: STORE_ROOT_AUTHORITY,
                        reason: format!("Serial head: {error}"),
                    }
                })?;
            let executor_ref = match &unverified.state {
                StoreSerialHeadState::Genesis {
                    founder_registration,
                    ..
                } => founder_registration,
                StoreSerialHeadState::Commit {
                    author_registration,
                    ..
                } => author_registration,
            };
            let executor = db
                .activated_store_device_registration(executor_ref.clone())
                .await?;
            let head = StoreSerialHead::parse(&object.bytes, store_root_hash, &executor).map_err(
                |error| StoreOutboundError::InvalidState {
                    key: STORE_ROOT_AUTHORITY,
                    reason: format!("Serial head: {error}"),
                },
            )?;
            Ok(SerialHeadObservation::Present {
                head,
                bytes: object.bytes,
                version: object.version,
            })
        }
        Err(CoordinationError::NotFound(_)) => Ok(SerialHeadObservation::Absent),
        Err(error) => Err(StoreOutboundError::Coordination(error)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_serial_store_branch(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
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
        let current = SerialDatabase::new(db)
            .exact_predecessor(snapshot.base)
            .await?;
        db.mark_serial_branch_conflict(branch.branch_id, branch.base, current)
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
        let (root, registration_ref, registration, device_signer) =
            load_local_store_authority(db, device_id, keypair).await?;
        let store_root_hash = root.store_root_hash;
        let mut predecessor = branch.base.clone();
        let mut prepared = Vec::with_capacity(branch.writes.len());
        let mut resolved_device_state: Option<crate::sync::store_commit::ResolvedStoreDeviceState> =
            None;
        for write in branch.writes {
            if !write.changeset.is_empty() && write.inverse_changeset.is_empty() {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "Serial write {} has no inverse changeset",
                    write.write_id
                )));
            }
            let payload = crate::sync::service::prepare_store_payload(
                &write.blob_facts,
                keypair,
                store_dir,
                crate::sync::service::StorePayloadMembership::Serial,
            )
            .await
            .map_err(StoreOutboundError::Preparation)?;
            let seq = next_store_sequence(predecessor.as_ref())?;
            let coord = StoreCommitCoord::Serial { sequence: seq };
            let order = StoreCommitOrder::Serial {
                seq,
                predecessor: match &predecessor {
                    Some(reference) => StoreSerialPredecessor::Commit(reference.clone()),
                    None => StoreSerialPredecessor::Genesis {
                        root: root.clone(),
                        founder_registration: registration_ref.clone(),
                    },
                },
            };
            let resolved_devices = match resolved_device_state.as_ref() {
                Some(state) => state.clone(),
                None => {
                    let state = db.store_device_state_for_order(&order).await?.1;
                    resolved_device_state = Some(state.clone());
                    state
                }
            };
            let serial_position = match &order {
                StoreCommitOrder::Serial { predecessor, .. } => predecessor.clone(),
                StoreCommitOrder::MergeConcurrent { .. } => unreachable!(),
            };
            let device_state = crate::sync::store_commit::StoreDeviceStateRef::serial(
                serial_position.clone(),
                &resolved_devices,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let membership_state = crate::sync::circle_control::StoreMembershipStateRef::serial(
                serial_position,
                resolved_devices.recovery.clone(),
                &snapshot.authorization,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let candidate_family = CandidateFamilyId::derive(
                store_root_hash,
                &registration_ref,
                &write.write_id,
                &order,
            );
            let blob_write_authority = BlobWriteAuthority::new(&registration_ref, &registration)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let mut prepared_packages = Vec::new();
            if let Some(partition) = write.partitions.store {
                prepared_packages.push(
                    prepare_partition_package(
                        db,
                        storage,
                        store_root_hash,
                        candidate_family,
                        &write.write_id,
                        &coord,
                        db.schema_version(),
                        SERIAL_STREAM_ID.to_string(),
                        seq,
                        partition,
                        &write.blob_facts,
                        &blob_write_authority,
                        store_dir,
                    )
                    .await?,
                );
            }
            for partition in write.partitions.circles {
                prepared_packages.push(
                    prepare_partition_package(
                        db,
                        storage,
                        store_root_hash,
                        candidate_family,
                        &write.write_id,
                        &coord,
                        db.schema_version(),
                        SERIAL_STREAM_ID.to_string(),
                        seq,
                        partition,
                        &write.blob_facts,
                        &blob_write_authority,
                        store_dir,
                    )
                    .await?,
                );
            }
            let context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            );
            let store_package = prepared_packages
                .iter()
                .find(|package| package.audience == crate::sync::circle::Audience::Store)
                .map(|package| StorePackageInput {
                    candidate_family,
                    schema_version: db.schema_version(),
                    bytes: package.semantic_bytes.as_slice(),
                    object: package.prepared.reference().clone(),
                });
            let circle_packages = prepared_packages
                .iter()
                .filter_map(|package| {
                    let crate::sync::circle::Audience::Circle(circle_id) = package.audience else {
                        return None;
                    };
                    Some(CirclePackageInput {
                        circle_id,
                        control: package
                            .control
                            .as_ref()
                            .expect("Circle partition control")
                            .coordinate()
                            .clone(),
                        key_fingerprint: package
                            .key_fingerprint
                            .expect("Circle partition key fingerprint"),
                        package: StorePackageInput {
                            candidate_family,
                            schema_version: db.schema_version(),
                            bytes: package.semantic_bytes.as_slice(),
                            object: package.prepared.reference().clone(),
                        },
                    })
                })
                .collect::<Vec<_>>();
            let commit = StoreBatchCommit::signed_operations(
                store_root_hash,
                write.write_id.clone(),
                coord.clone(),
                registration_ref.clone(),
                &registration,
                order,
                membership_state,
                device_state,
                payload.membership_authority,
                StoreCommitOperationsInput {
                    acknowledgement: None,
                    control: None,
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    provider_access_withdrawals: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    stream_activations: Vec::new(),
                    circle_controls: Vec::new(),
                    store_package,
                    circle_packages: &circle_packages,
                },
                &device_signer,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let prefix = commit_semantic_prefix(
                commit.candidate_family(),
                SERIAL_STREAM_ID,
                seq,
                commit.commit_hash(),
            );
            let slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let commit_prepared = storage
                .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
                .map_err(StoreObjectError::from)?;
            let commit_ref = StoreBatchCommitRef::from_commit(
                &commit,
                coord,
                commit_prepared.reference().clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let (remote_objects, audiences) =
                close_prepared_packages(prepared_packages, &commit, &commit_ref)?;
            let local_cleanup =
                crate::sync::service::bind_local_cleanup(payload.local_cleanup, &audiences.blobs)
                    .map_err(StoreOutboundError::Preparation)?;
            predecessor = Some(commit_ref);
            prepared.push(SerialStoreWritePreparationEntry {
                write_id: write.write_id,
                remote_objects,
                audiences,
                commit: PreparedProtocolObject {
                    value: commit,
                    prepared: commit_prepared,
                },
                local_cleanup,
                completion: payload.completion,
            });
        }
        let tip_ref = predecessor
            .clone()
            .ok_or_else(|| StoreOutboundError::InvalidOutbound("Serial branch is empty".into()))?;
        let head = StoreSerialHead::signed(
            store_root_hash,
            StoreSerialHeadState::Commit {
                author_registration: registration_ref,
                commit: tip_ref,
            },
            &device_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        db.prepare_serial_store_branch_commit(SerialStoreWritePreparation {
            branch_id: branch.branch_id,
            base: branch.base,
            base_head: snapshot.base_head,
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
    base: Option<StoreBatchCommitRef>,
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
    observed.bytes() == Some(branch.head.bytes.as_slice())
}

enum PreparedSerialBaseObservation {
    Matches,
    Conflicts(StoreSerialPredecessor),
}

fn prepared_serial_base_observation(
    observed: &SerialHeadObservation,
    branch: &PreparedSerialStoreBranch,
) -> Result<PreparedSerialBaseObservation, StoreOutboundError> {
    let first = branch.writes.first().ok_or_else(|| {
        StoreOutboundError::InvalidOutbound("prepared Serial branch has no writes".to_string())
    })?;
    let StoreCommitOrder::Serial {
        predecessor: expected,
        ..
    } = &first.commit.value.order
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "prepared Serial branch carries a Merge commit".to_string(),
        ));
    };
    let current = observed.predecessor()?;
    if &current != expected {
        return Ok(PreparedSerialBaseObservation::Conflicts(current));
    }
    if observed.versioned().as_ref() != Some(&branch.base_head) {
        return Err(StoreOutboundError::InvalidState {
            key: SERIAL_COORDINATION_HEAD,
            reason: "bytes or provider version changed at the same exact predecessor".to_string(),
        });
    }
    Ok(PreparedSerialBaseObservation::Matches)
}

async fn conflict_serial_branch(
    db: &Database,
    branch: PreparedSerialStoreBranch,
    current: StoreSerialPredecessor,
) -> Result<u64, StoreOutboundError> {
    db.mark_serial_branch_conflict(branch.branch_id, branch.base, current)
        .await?;
    Ok(0)
}

pub(crate) async fn drain_store_writes(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
) -> Result<u64, StoreOutboundError> {
    if db.prepared_serial_candidate_abandonment().await?.is_some() {
        return Ok(0);
    }
    let Some(branch) = db.prepared_serial_store_branch().await? else {
        return Ok(0);
    };
    let observed = observe_serial_head(db, coordination).await?;
    if serial_head_activates_branch(&observed, &branch) {
        let accepted = observed.versioned().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "activated Serial head has no version receipt".to_string(),
            )
        })?;
        return SerialDatabase::new(db)
            .complete_prepared_branch(accepted)
            .await
            .map_err(Into::into);
    }
    if let PreparedSerialBaseObservation::Conflicts(current) =
        prepared_serial_base_observation(&observed, &branch)?
    {
        return conflict_serial_branch(db, branch, current).await;
    }
    let store_root_hash = required_store_root(db).await?.store_root_hash;
    for write in &branch.writes {
        publish_prepared_remote_objects(db, storage, &write.commit.value.write_id, store_root_hash)
            .await?;
        storage
            .create_protocol_object(&write.commit.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let prefix = commit_semantic_prefix(
            write.commit.value.candidate_family(),
            SERIAL_STREAM_ID,
            write.commit.value.seq(),
            write.commit.value.commit_hash(),
        );
        let opened = storage
            .read_protocol_object(&context, &write.commit.object, &prefix)
            .await
            .map_err(StoreObjectError::from)?;
        if opened != write.commit.bytes {
            return Err(StoreOutboundError::InvalidOutbound(
                "prepared Serial commit exact readback differs from its signed bytes".to_string(),
            ));
        }
        let commit_ref = StoreBatchCommitRef::from_commit(
            &write.commit.value,
            StoreCommitCoord::Serial {
                sequence: write.commit.value.seq(),
            },
            write.commit.object.clone(),
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        db.mark_candidate_commit_uploaded(commit_ref).await?;
    }
    let current = observe_serial_head(db, coordination).await?;
    if let PreparedSerialBaseObservation::Conflicts(current) =
        prepared_serial_base_observation(&current, &branch)?
    {
        return conflict_serial_branch(db, branch, current).await;
    }
    let activation = match current.version() {
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
    let accepted = match activation {
        Ok(activated) if activated.bytes == branch.head.bytes => activated,
        Ok(_) => {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial head readback differs from exact prepared bytes".to_string(),
            ));
        }
        Err(error) => {
            let after = observe_serial_head(db, coordination).await?;
            if serial_head_activates_branch(&after, &branch) {
                after.versioned().ok_or_else(|| {
                    StoreOutboundError::InvalidOutbound(
                        "accepted Serial head has no opaque version receipt".to_string(),
                    )
                })?
            } else {
                if let PreparedSerialBaseObservation::Conflicts(current) =
                    prepared_serial_base_observation(&after, &branch)?
                {
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
    };
    SerialDatabase::new(db)
        .complete_prepared_branch(accepted)
        .await
        .map_err(Into::into)
}
