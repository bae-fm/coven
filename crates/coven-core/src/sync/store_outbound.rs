//! Durable construction and ordered publication of local Store commits.

use super::causal_grants::AuthorStreamId;
use super::membership::{MembershipChain, SerialAuthorizationState};
use super::storage::{
    BlobWriteAuthority, CoordinationError, CoordinationStorage, CreateHeadError, ExactObjectRef,
    ReplaceHeadError, StorageError, SyncStorage, VersionToken, VersionedObject,
};
use super::storage::{PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain};
use super::store_commit::{
    circle_package_semantic_prefix, commit_semantic_prefix, head_slot_prefix,
    package_semantic_prefix, serial_head_key, ActivatedStoreDeviceRegistrationRef,
    CandidateCleanupManifest, CandidateFamilyId, CirclePackageInput, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, ObjectHash, StoreBatchCommit, StoreBatchCommitDeletionTarget,
    StoreBatchCommitRef, StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder,
    StoreControl, StoreDeviceHead, StoreDeviceHeadRef, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreHistoryCut, StorePackageInput, StoreRootRef, StoreSerialHead,
    StoreSerialHeadState, StoreSerialPredecessor, SuccessorLink, SERIAL_STREAM_ID,
};
use super::store_objects::StoreObjectError;

const STORE_ROOT_AUTHORITY: &str = "store_root_authority";
const SERIAL_COORDINATION_HEAD: &str = "serial_coordination_head";
use crate::database::{
    Database, MergeCandidateAbandonmentPreparation, PreparedAudienceBlob, PreparedAudienceObjects,
    PreparedAudiencePackage, PreparedProtocolObject, PreparedSerialStoreBranch, PreparedStoreWrite,
    SerialCandidateAbandonmentPreparation, SerialStoreWritePreparation,
    SerialStoreWritePreparationEntry, StoreWriteBase, StoreWriteBlobFact, StoreWriteBlobFacts,
    StoreWritePreparation,
};
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

struct PreparedPartitionPackage {
    audience: super::circle::Audience,
    control: Option<super::gate::CirclePartitionControl>,
    key_fingerprint: Option<crate::KeyFingerprint>,
    semantic_bytes: Vec<u8>,
    prepared: PreparedExactObject,
    blobs: Vec<PreparedPartitionBlob>,
}

pub(crate) struct PreparedPartitionBlob {
    pub(crate) audience: crate::blob::locator::RemoteAudience,
    pub(crate) stored: crate::blob::locator::StoredBlobRef,
    pub(crate) spool_path: Option<std::path::PathBuf>,
    pub(crate) uploaded_verified: bool,
}

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
        expected: Box<StoreSerialPredecessor>,
        current: Box<StoreSerialPredecessor>,
    },
    #[error("candidate cleanup: {0}")]
    CandidateCleanup(#[from] super::store_pull::StorePullError),
}

impl From<crate::database::DbError> for StoreOutboundError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeCandidateAbandonment {
    NotRequired,
    Abandoned,
    CandidateActivated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerialCandidateAbandonmentWinner {
    Authority {
        accepted: super::storage::VersionedObject,
    },
    OriginalBranch {
        accepted: super::storage::VersionedObject,
    },
    Other {
        current: StoreSerialPredecessor,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialBranchAbandonment {
    Discarded,
    OriginalBranchActivated,
}

pub(crate) async fn exact_next_announcement_slot(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    previous: Option<&StoreBatchCommitRef>,
) -> Result<
    (
        crate::storage::cloud::ObjectSlot,
        Option<StoreDeviceHeadRef>,
    ),
    StoreOutboundError,
> {
    let super::store_commit::StoreCommitAnchor::MergeConcurrent {
        announcements: super::store_commit::DeviceStreamAnchor::StoreAnnouncements { first_slot },
    } = &registration.store_commits
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Merge registration has no Store announcement anchor".to_string(),
        ));
    };
    let Some(target) = previous else {
        return Ok((first_slot.clone(), None));
    };
    let expected_stream = AuthorStreamId::store_announcements(root, registration_ref);
    if !matches!(
        target.coord,
        StoreCommitCoord::MergeConcurrent { stream_id, .. } if stream_id == expected_stream
    ) {
        return Err(StoreOutboundError::InvalidOutbound(
            "local predecessor belongs to another Store announcement stream".to_string(),
        ));
    }
    let activation =
        super::store_commit::StreamActivationId::store_announcements(root, registration_ref);
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let mut slot = first_slot.clone();
    let mut predecessor: Option<StoreDeviceHeadRef> = None;
    for sequence in 1..=target.coord.sequence() {
        let prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = storage
            .read_protocol_slot(&context, &slot, &prefix)
            .await
            .map_err(StoreObjectError::from)?;
        let unverified: StoreDeviceHead = serde_json::from_slice(&bytes).map_err(|error| {
            StoreOutboundError::InvalidOutbound(format!(
                "parse exact local Store head {sequence}: {error}"
            ))
        })?;
        if unverified.author_registration != *registration_ref
            || unverified.successor.activation != activation
            || unverified.successor.predecessor
                != predecessor
                    .as_ref()
                    .map(|reference| reference.object.clone())
        {
            return Err(StoreOutboundError::InvalidOutbound(format!(
                "local Store head {sequence} does not extend its exact activated predecessor"
            )));
        }
        super::store_objects::load_commit_ref(
            storage,
            root.store_root_hash,
            &unverified.commit,
            registration,
        )
        .await?;
        let head = StoreDeviceHead::parse_at(
            &bytes,
            root.store_root_hash,
            registration,
            &unverified.commit,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let reference = StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object,
        };
        if sequence == target.coord.sequence() {
            if head.commit != *target {
                return Err(StoreOutboundError::InvalidOutbound(
                    "durable local predecessor differs from its exact Store head".to_string(),
                ));
            }
            return Ok((head.successor.next_slot, Some(reference)));
        }
        slot = head.successor.next_slot;
        predecessor = Some(reference);
    }
    Err(StoreOutboundError::InvalidOutbound(
        "local Store predecessor traversal ended early".to_string(),
    ))
}

impl StoreOutboundError {
    pub(crate) fn definitely_uncommitted(&self) -> bool {
        match self {
            Self::Database(_) | Self::Coordination(_) | Self::CandidateCleanup(_) => false,
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
) -> Result<bool, StoreOutboundError> {
    prepare_pending_store_write_with_coordination(
        db, storage, None, device_id, timestamp, keypair, store_dir, membership,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_pending_store_write_with_coordination(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    _timestamp: &str,
    keypair: &UserKeypair,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<bool, StoreOutboundError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        return prepare_serial_store_branch(
            db,
            storage,
            coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?,
            device_id,
            keypair,
            store_dir,
        )
        .await;
    }
    let Some(PreparedStoreWrite {
        write_id,
        changeset,
        inverse_changeset,
        base,
        blob_facts,
        partitions,
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
        StoreWriteBase::MergeConcurrent { dependencies } => {
            super::store_commit::CommitFrontier::from_refs(
                crate::WritePolicy::MergeConcurrent,
                dependencies,
            )
            .and_then(|frontier| frontier.merge_commits().cloned())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
        }
        StoreWriteBase::Serial { .. } => {
            return Err(StoreOutboundError::InvalidOutbound(
                "serial Store write reached MergeConcurrent preparation".to_string(),
            ));
        }
    };
    let preparation = async {
        let payload =
            super::service::prepare_store_payload(&blob_facts, keypair, store_dir, membership)
                .await
                .map_err(StoreOutboundError::Preparation)?;
        let (root, registration_ref, registration, device_signer) =
            load_local_store_authority(db, device_id, keypair).await?;
        let blob_write_authority = BlobWriteAuthority::new(&registration_ref, &registration)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let store_root_hash = root.store_root_hash;
        let stream_id = AuthorStreamId::store_announcements(&root, &registration_ref);
        let previous = db.latest_local_store_position().await?;
        let seq = previous
            .as_ref()
            .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
        let coord = StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence: seq,
        };
        let order = StoreCommitOrder::MergeConcurrent {
            seq,
            predecessor: previous.clone(),
            dependencies,
        };
        let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
        let membership = membership.ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Merge Store write has no exact membership state".to_string(),
            )
        })?;
        let membership_state = match membership.status() {
            super::membership::MembershipStatus::Resolved(resolved) => {
                super::circle_control::StoreMembershipStateRef::merge_concurrent(
                    membership.head_refs().to_vec(),
                    membership.resolution_refs().to_vec(),
                    resolved_devices.recovery.clone(),
                    resolved.state_hash,
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
            }
            super::membership::MembershipStatus::Conflict(_) => {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Merge Store write requires resolved membership".to_string(),
                ));
            }
        };
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &registration_ref, &write_id, &order);
        let mut prepared_packages = Vec::new();
        if let Some(partition) = partitions.store {
            prepared_packages.push(
                prepare_partition_package(
                    db,
                    storage,
                    store_root_hash,
                    candidate_family,
                    &write_id,
                    &coord,
                    db.schema_version(),
                    stream_id.to_string(),
                    seq,
                    partition,
                    &blob_facts,
                    &blob_write_authority,
                    store_dir,
                )
                .await?,
            );
        }
        for partition in partitions.circles {
            prepared_packages.push(
                prepare_partition_package(
                    db,
                    storage,
                    store_root_hash,
                    candidate_family,
                    &write_id,
                    &coord,
                    db.schema_version(),
                    stream_id.to_string(),
                    seq,
                    partition,
                    &blob_facts,
                    &blob_write_authority,
                    store_dir,
                )
                .await?,
            );
        }
        let commit_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let head_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let device_id = registration_ref.device_id.to_string();
        let head_prefix = head_slot_prefix(&device_id, seq);
        let (head_slot, predecessor_head) = exact_next_announcement_slot(
            storage,
            &root,
            &registration_ref,
            &registration,
            previous.as_ref(),
        )
        .await?;
        let next_head_prefix = head_slot_prefix(&device_id, seq.saturating_add(1));
        let next_head_slot = storage
            .allocate_protocol_slot(&head_context, &next_head_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;

        let store_package = prepared_packages
            .iter()
            .find(|package| package.audience == super::circle::Audience::Store)
            .map(|package| StorePackageInput {
                candidate_family,
                schema_version: db.schema_version(),
                bytes: package.semantic_bytes.as_slice(),
                object: package.prepared.reference().clone(),
            });
        let circle_packages = prepared_packages
            .iter()
            .filter_map(|package| {
                let super::circle::Audience::Circle(circle_id) = package.audience else {
                    return None;
                };
                let control = package
                    .control
                    .as_ref()
                    .expect("Circle partition carries exact control");
                Some(CirclePackageInput {
                    circle_id,
                    control: control.coordinate().clone(),
                    key_fingerprint: package
                        .key_fingerprint
                        .expect("Circle partition carries exact key fingerprint"),
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
            write_id.clone(),
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
                circle_controls: Vec::new(),
                store_package,
                circle_packages: &circle_packages,
            },
            &device_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let commit_prefix = commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id.to_string(),
            seq,
            commit.commit_hash(),
        );
        let commit_slot = storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let commit_prepared = storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                commit.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let commit_ref =
            StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let activation =
            super::store_commit::StreamActivationId::store_announcements(&root, &registration_ref);
        let head = StoreDeviceHead::signed(
            store_root_hash,
            registration_ref,
            commit_ref.clone(),
            SuccessorLink {
                activation,
                predecessor: predecessor_head.map(|reference| reference.object),
                next_slot: next_head_slot,
            },
            &device_signer,
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        let head_prepared = storage
            .prepare_protocol_object(&head_context, head_slot, &head_prefix, head.to_bytes())
            .map_err(StoreObjectError::from)?;
        let (remote_objects, audience_objects) =
            close_prepared_packages(prepared_packages, &commit_ref)?;
        Ok::<_, StoreOutboundError>(StoreWritePreparation {
            write_id: write_id.clone(),
            remote_objects,
            audiences: audience_objects,
            commit: PreparedProtocolObject {
                value: commit,
                prepared: commit_prepared,
            },
            head: PreparedProtocolObject {
                value: head,
                prepared: head_prepared,
            },
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

pub(crate) async fn load_local_store_authority(
    db: &Database,
    expected_device_id: &str,
    identity_signer: &UserKeypair,
) -> Result<
    (
        super::store_commit::StoreRootRef,
        StoreDeviceRegistrationRef,
        StoreDeviceRegistration,
        UserKeypair,
    ),
    StoreOutboundError,
> {
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: STORE_ROOT_AUTHORITY,
        })?;
    let durable = db.latest_local_store_device_registration().await?.ok_or(
        StoreOutboundError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        },
    )?;
    if !durable.is_activated() || durable.device_id.to_string() != expected_device_id {
        return Err(StoreOutboundError::InvalidState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            reason: "local Store device registration is not the activated writer".to_string(),
        });
    }
    let registration =
        StoreDeviceRegistration::parse_at(&durable.registration_bytes, &root, durable.device_id)
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    if registration.registration_hash() != durable.registration_hash {
        return Err(StoreOutboundError::InvalidOutbound(
            "local Store device registration differs from its durable hash".to_string(),
        ));
    }
    let reference = StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let activated = db
        .activated_store_device_registration(reference.clone())
        .await?;
    if activated != registration {
        return Err(StoreOutboundError::InvalidOutbound(
            "local Store writer differs from its activated exact registration".to_string(),
        ));
    }
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok((root, reference, registration, device_signer))
}

#[allow(clippy::too_many_arguments)]
async fn prepare_partition_package(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    candidate_family: CandidateFamilyId,
    write_id: &crate::WriteId,
    coord: &StoreCommitCoord,
    schema_version: u32,
    stream_id: String,
    seq: u64,
    partition: super::gate::AudiencePartition,
    blob_facts: &StoreWriteBlobFacts,
    authority: &BlobWriteAuthority<'_>,
    store_dir: &StoreDir,
) -> Result<PreparedPartitionPackage, StoreOutboundError> {
    let blob_facts = partition_blob_facts(&partition.changeset, blob_facts)?;
    let (remote_audience, protection) = match partition.audience {
        super::circle::Audience::Store => (
            crate::blob::locator::RemoteAudience::Store,
            storage
                .store_blob_protection()
                .map_err(|source| StoreOutboundError::BlobStorage {
                    namespace: "store".to_string(),
                    id: "protection".to_string(),
                    source,
                })?,
        ),
        super::circle::Audience::Circle(circle_id) => {
            let control = partition.control.as_ref().ok_or_else(|| {
                StoreOutboundError::InvalidOutbound(format!(
                    "Circle partition {circle_id} has no exact control"
                ))
            })?;
            let (encryption, _) = db
                .circle_publication_context(circle_id, control.coordinate().clone())
                .await?;
            (
                crate::blob::locator::RemoteAudience::Circle(circle_id),
                super::storage::BlobSpoolProtection::Opaque(encryption),
            )
        }
        super::circle::Audience::Local => {
            return Err(StoreOutboundError::InvalidOutbound(
                "Local partition reached Store publication".to_string(),
            ));
        }
    };
    let mut prepared_blobs = Vec::with_capacity(blob_facts.len());
    let mut blob_bindings = Vec::with_capacity(blob_facts.len());
    for fact in blob_facts {
        let (binding, blob) = prepare_partition_blob(
            db,
            storage,
            fact,
            remote_audience.clone(),
            protection.clone(),
            authority,
            store_dir,
        )
        .await?;
        blob_bindings.push(binding);
        prepared_blobs.push(blob);
    }
    let (package, context, semantic_prefix, key_fingerprint) = match partition.audience {
        super::circle::Audience::Store => {
            if partition.control.is_some() {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Store partition carries Circle control".to_string(),
                ));
            }
            let package = super::audience_package::AudiencePackage::store(
                store_root_hash,
                candidate_family,
                write_id.clone(),
                coord.clone(),
                schema_version,
                partition.changeset.clone(),
                blob_bindings,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let bytes = package.to_bytes();
            let prefix = package_semantic_prefix(
                candidate_family,
                &stream_id,
                seq,
                ObjectHash::digest(&bytes),
            );
            (
                package,
                ProtocolObjectContext::store_encrypted(
                    store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                prefix,
                None,
            )
        }
        super::circle::Audience::Circle(circle_id) => {
            let control = partition.control.as_ref().ok_or_else(|| {
                StoreOutboundError::InvalidOutbound(format!(
                    "Circle partition {circle_id} has no exact control"
                ))
            })?;
            let (encryption, key_fingerprint) = db
                .circle_publication_context(circle_id, control.coordinate().clone())
                .await?;
            let package = super::audience_package::AudiencePackage::circle(
                store_root_hash,
                candidate_family,
                write_id.clone(),
                coord.clone(),
                schema_version,
                circle_id,
                control.coordinate().clone(),
                key_fingerprint,
                partition.changeset.clone(),
                blob_bindings,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let bytes = package.to_bytes();
            let prefix = circle_package_semantic_prefix(
                circle_id,
                candidate_family,
                &stream_id,
                seq,
                ObjectHash::digest(&bytes),
            );
            (
                package,
                ProtocolObjectContext::circle(
                    store_root_hash,
                    ProtocolObjectDomain::CirclePackage,
                    encryption,
                ),
                prefix,
                Some(key_fingerprint),
            )
        }
        super::circle::Audience::Local => {
            return Err(StoreOutboundError::InvalidOutbound(
                "Local partition reached Store publication".to_string(),
            ));
        }
    };
    let semantic_bytes = package.to_bytes();
    let slot = storage
        .allocate_protocol_slot(&context, &semantic_prefix, ".pkg")
        .await
        .map_err(StoreObjectError::from)?;
    let prepared = storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, semantic_bytes.clone())
        .map_err(StoreObjectError::from)?;
    Ok(PreparedPartitionPackage {
        audience: partition.audience,
        control: partition.control,
        key_fingerprint,
        semantic_bytes,
        prepared,
        blobs: prepared_blobs,
    })
}

fn partition_blob_facts<'a>(
    changeset: &[u8],
    facts: &'a StoreWriteBlobFacts,
) -> Result<Vec<&'a StoreWriteBlobFact>, StoreOutboundError> {
    let rows = crate::changeset::walk(changeset)
        .map_err(|error| {
            StoreOutboundError::InvalidOutbound(format!("read audience package blob rows: {error}"))
        })?
        .into_iter()
        .filter(|change| {
            matches!(
                change.op,
                crate::changeset::ChangeOp::Insert | crate::changeset::ChangeOp::Update
            )
        })
        .map(|change| {
            change
                .pk()
                .map(|row_id| (change.table.clone(), row_id.to_string()))
                .ok_or_else(|| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "audience package row in {:?} has no primary key",
                        change.table
                    ))
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    Ok(facts
        .blobs
        .iter()
        .filter(|fact| rows.contains(&(fact.table.clone(), fact.row_id.clone())))
        .collect())
}

pub(crate) async fn prepare_partition_blob(
    db: &Database,
    storage: &dyn SyncStorage,
    fact: &StoreWriteBlobFact,
    audience: crate::blob::locator::RemoteAudience,
    protection: super::storage::BlobSpoolProtection,
    authority: &BlobWriteAuthority<'_>,
    store_dir: &StoreDir,
) -> Result<
    (
        super::audience_package::RowBlobLocatorBinding,
        PreparedPartitionBlob,
    ),
    StoreOutboundError,
> {
    let locator = prepare_partition_blob_locator(fact, audience.clone(), &protection, authority)?;
    let spool_path = store_dir.outbound_blob_spool_path(locator.locator_hash());
    if let Some(previous) = &fact.previous {
        if previous.stored.locator() == &locator {
            let binding = super::audience_package::RowBlobLocatorBinding::new(
                fact.table.clone(),
                fact.row_id.clone(),
                fact.row_stamp.clone(),
                fact.column.clone(),
                previous.stored.clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            return Ok((
                binding,
                PreparedPartitionBlob {
                    audience,
                    stored: previous.stored.clone(),
                    spool_path: None,
                    uploaded_verified: true,
                },
            ));
        }
    }
    let host_path = match fact.blob.provenance {
        crate::blob::Provenance::HostProvided => Some(
            store_dir
                .local_blob_path(&fact.blob.namespace, &fact.blob.id)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
        ),
        crate::blob::Provenance::UserProvided => None,
    };
    let mut temporary_plaintext = false;
    let source_path = if let Some(path) = &fact.external_path {
        if fact.blob.provenance != crate::blob::Provenance::UserProvided {
            return Err(StoreOutboundError::InvalidOutbound(format!(
                "host-provided blob {}/{} carries an external path",
                fact.blob.namespace, fact.blob.id
            )));
        }
        path.clone()
    } else if let Some(path) = host_path {
        match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => path,
            Ok(_) => {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "host blob source is not a file: {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                temporary_plaintext = true;
                materialize_previous_blob(db, storage, fact, store_dir, locator.locator_hash())
                    .await?
            }
            Err(error) => {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "inspect host blob source {}: {error}",
                    path.display()
                )));
            }
        }
    } else {
        temporary_plaintext = true;
        materialize_previous_blob(db, storage, fact, store_dir, locator.locator_hash()).await?
    };
    if let Err(error) = storage
        .seal_blob_to_spool(&locator, authority, protection, &source_path, &spool_path)
        .await
        .map_err(|source| StoreOutboundError::BlobStorage {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
            source,
        })
    {
        return Err(cleanup_failed_partition_blob(
            &spool_path,
            temporary_plaintext.then_some(source_path.as_path()),
            false,
            error,
        )
        .await);
    }
    let prepared = async {
        if temporary_plaintext {
            tokio::fs::remove_file(&source_path)
                .await
                .map_err(|error| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "remove prepared plaintext {}: {error}",
                        source_path.display()
                    ))
                })?;
            crate::local_blob::sync_parent_dir(&source_path)
                .await
                .map_err(StoreOutboundError::InvalidOutbound)?;
        }
        let slot = storage
            .allocate_blob_slot(&locator, authority)
            .await
            .map_err(|source| StoreOutboundError::BlobStorage {
                namespace: fact.blob.namespace.clone(),
                id: fact.blob.id.clone(),
                source,
            })?;
        let stored = storage
            .prepare_blob_object(&locator, authority, slot, &spool_path)
            .await
            .map_err(|source| StoreOutboundError::BlobStorage {
                namespace: fact.blob.namespace.clone(),
                id: fact.blob.id.clone(),
                source,
            })?;
        let binding = super::audience_package::RowBlobLocatorBinding::new(
            fact.table.clone(),
            fact.row_id.clone(),
            fact.row_stamp.clone(),
            fact.column.clone(),
            stored.clone(),
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        Ok::<_, StoreOutboundError>((binding, stored))
    }
    .await;
    let (binding, stored) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            return Err(cleanup_failed_partition_blob(
                &spool_path,
                temporary_plaintext.then_some(source_path.as_path()),
                true,
                error,
            )
            .await);
        }
    };
    Ok((
        binding,
        PreparedPartitionBlob {
            audience,
            stored,
            spool_path: Some(spool_path),
            uploaded_verified: false,
        },
    ))
}

async fn cleanup_failed_partition_blob(
    spool_path: &std::path::Path,
    temporary_plaintext: Option<&std::path::Path>,
    spool_expected: bool,
    error: StoreOutboundError,
) -> StoreOutboundError {
    let mut failures = Vec::new();
    if let Some(path) = temporary_plaintext {
        match crate::local_blob::remove_file(path).await {
            Ok(removed) => {
                if removed {
                    if let Err(sync_error) = crate::local_blob::sync_parent_dir(path).await {
                        failures.push(format!(
                            "sync removed temporary plaintext {}: {sync_error}",
                            path.display()
                        ));
                    }
                }
            }
            Err(cleanup_error) => failures.push(format!(
                "remove temporary plaintext {}: {cleanup_error}",
                path.display()
            )),
        }
    }
    match crate::local_blob::remove_file(spool_path).await {
        Ok(true) => {}
        Ok(false) if !spool_expected => {}
        Ok(false) => failures.push(format!(
            "prepared blob spool {} is absent",
            spool_path.display()
        )),
        Err(cleanup_error) => failures.push(format!(
            "remove prepared blob spool {}: {cleanup_error}",
            spool_path.display()
        )),
    }
    if failures.is_empty() {
        error
    } else {
        StoreOutboundError::InvalidOutbound(format!(
            "blob preparation failed: {error}; cleanup failed: {}",
            failures.join("; ")
        ))
    }
}

pub(crate) fn prepare_partition_blob_locator(
    fact: &StoreWriteBlobFact,
    audience: crate::blob::locator::RemoteAudience,
    protection: &super::storage::BlobSpoolProtection,
    authority: &BlobWriteAuthority<'_>,
) -> Result<crate::blob::locator::BlobLocator, StoreOutboundError> {
    match protection {
        super::storage::BlobSpoolProtection::Opaque(encryption) => {
            crate::blob::locator::BlobLocator::opaque(
                fact.blob.namespace.clone(),
                fact.blob.id.clone(),
                authority.reference.clone(),
                audience,
                fact.blob.scope.clone(),
                encryption.seal_key_fingerprint(),
                fact.plaintext_size,
                fact.plaintext_hash,
            )
        }
        super::storage::BlobSpoolProtection::Browsable => {
            if audience != crate::blob::locator::RemoteAudience::Store {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Circle blob cannot use Browsable storage".to_string(),
                ));
            }
            crate::blob::locator::BlobLocator::browsable(
                fact.blob.namespace.clone(),
                fact.blob.id.clone(),
                authority.reference.clone(),
                fact.blob.cloud_path.clone().ok_or_else(|| {
                    StoreOutboundError::InvalidOutbound(format!(
                        "Browsable blob {}/{} has no readable path",
                        fact.blob.namespace, fact.blob.id
                    ))
                })?,
                fact.plaintext_size,
                fact.plaintext_hash,
            )
        }
    }
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
}

async fn materialize_previous_blob(
    db: &Database,
    storage: &dyn SyncStorage,
    fact: &StoreWriteBlobFact,
    store_dir: &StoreDir,
    destination_locator: ObjectHash,
) -> Result<std::path::PathBuf, StoreOutboundError> {
    let previous = fact
        .previous
        .as_ref()
        .ok_or_else(|| StoreOutboundError::MissingBlob {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
        })?;
    let authority = crate::blob::RowBlobAuthority::Remote(previous.authority.clone());
    let protection = crate::blob::cache::opening_protection_for_authority(
        db,
        storage,
        &authority,
        &previous.stored,
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let destination = store_dir
        .storage_dir()
        .join("outbound-blobs")
        .join(format!(".plaintext-{destination_locator}"));
    let staged = storage
        .stage_verified_blob_plaintext(&previous.stored, protection, &destination)
        .await
        .map_err(|source| StoreOutboundError::BlobStorage {
            namespace: fact.blob.namespace.clone(),
            id: fact.blob.id.clone(),
            source,
        })?;
    staged
        .commit()
        .await
        .map_err(StoreOutboundError::InvalidOutbound)?;
    Ok(destination)
}

fn close_prepared_packages(
    packages: Vec<PreparedPartitionPackage>,
    owner: &StoreBatchCommitRef,
) -> Result<
    (
        Vec<super::remote_object::RemoteObjectRecord>,
        PreparedAudienceObjects,
    ),
    StoreOutboundError,
> {
    let mut remote_objects = Vec::new();
    let mut indexed_packages = Vec::with_capacity(packages.len());
    let mut prepared_blobs = Vec::new();
    for package in packages {
        let object = package.prepared.reference().clone();
        let domain = match package.audience {
            super::circle::Audience::Store => {
                super::remote_object::CandidateExclusiveObjectDomain::StorePackage
            }
            super::circle::Audience::Circle(circle_id) => {
                super::remote_object::CandidateExclusiveObjectDomain::CirclePackage { circle_id }
            }
            super::circle::Audience::Local => unreachable!("Local partition was rejected"),
        };
        let remote = super::remote_object::RemoteObjectRecord::CandidateExclusive(
            super::remote_object::CandidateObjectRecord {
                identity: super::remote_object::CandidateExclusiveTarget {
                    family: super::audience_package::AudiencePackage::parse(
                        &package.semantic_bytes,
                    )
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
                    .candidate_family(),
                    domain,
                    semantic_hash: ObjectHash::digest(&package.semantic_bytes),
                    object: object.clone(),
                },
                bytes: super::remote_object::RemoteObjectBytes::inline(
                    package.semantic_bytes.clone(),
                    package.prepared.stored_bytes().to_vec(),
                    object.clone(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
                state: super::remote_object::CandidateObjectState::Prepared {
                    ownership: super::remote_object::PendingCandidateOwnership {
                        pending: std::collections::BTreeSet::from([owner.clone()]),
                        nonactivated: Vec::new(),
                    },
                },
            },
        );
        let remote_object_id = remote.object_id();
        indexed_packages.push(
            PreparedAudiencePackage::new(
                remote_object_id,
                package.semantic_bytes,
                package.prepared.stored_bytes().to_vec(),
                object,
            )
            .map_err(StoreOutboundError::from)?,
        );
        remote_objects.push(remote);
        prepared_blobs.extend(package.blobs);
    }
    let (blob_remotes, indexed_blobs) = close_prepared_blobs(prepared_blobs, owner)?;
    remote_objects.extend(blob_remotes);
    Ok((
        remote_objects,
        PreparedAudienceObjects {
            packages: indexed_packages,
            blobs: indexed_blobs,
        },
    ))
}

fn close_prepared_blobs(
    blobs: Vec<PreparedPartitionBlob>,
    owner: &StoreBatchCommitRef,
) -> Result<
    (
        Vec<super::remote_object::RemoteObjectRecord>,
        Vec<PreparedAudienceBlob>,
    ),
    StoreOutboundError,
> {
    let mut exact_blobs = std::collections::BTreeMap::new();
    for blob in blobs {
        let object_id = super::remote_object::remote_object_id(blob.stored.object());
        let key = (blob.audience.clone(), object_id);
        if let Some(existing) = exact_blobs.get_mut(&key) {
            merge_identical_prepared_blob(existing, blob)?;
        } else {
            exact_blobs.insert(key, blob);
        }
    }
    let mut remote_objects = Vec::with_capacity(exact_blobs.len());
    let mut indexed_blobs = Vec::with_capacity(exact_blobs.len());
    for blob in exact_blobs.into_values() {
        let locator_hash = blob.stored.locator().locator_hash();
        let remote = super::remote_object::RemoteObjectRecord::SharedLiveSet(
            super::remote_object::SharedObjectRecord {
                identity: super::remote_object::SharedLiveSetObjectRef {
                    domain: super::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                    semantic_hash: ObjectHash::digest(&blob.stored.locator().to_bytes()),
                    object: blob.stored.object().clone(),
                },
                bytes: super::remote_object::RemoteObjectBytes::blob(
                    blob.stored.locator().to_bytes(),
                    blob.stored.object().clone(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
                state: if blob.uploaded_verified {
                    super::remote_object::OwnedObjectState::UploadedVerified {
                        ownership: super::remote_object::SharedObjectOwnership {
                            pending: std::collections::BTreeSet::from([owner.clone()]),
                            activated: std::collections::BTreeSet::new(),
                            nonactivated: Vec::new(),
                        },
                    }
                } else {
                    super::remote_object::OwnedObjectState::Prepared {
                        ownership: super::remote_object::PendingCandidateOwnership {
                            pending: std::collections::BTreeSet::from([owner.clone()]),
                            nonactivated: Vec::new(),
                        },
                    }
                },
            },
        );
        let prepared = PreparedAudienceBlob::from_remote(
            blob.audience,
            &locator_hash.to_string(),
            remote.clone(),
            blob.spool_path,
        )?;
        indexed_blobs.push(prepared);
        remote_objects.push(remote);
    }
    Ok((remote_objects, indexed_blobs))
}

fn merge_identical_prepared_blob(
    existing: &mut PreparedPartitionBlob,
    duplicate: PreparedPartitionBlob,
) -> Result<(), StoreOutboundError> {
    if existing.audience != duplicate.audience || existing.stored != duplicate.stored {
        return Err(StoreOutboundError::InvalidOutbound(format!(
            "prepared blob object {} has conflicting exact references",
            super::remote_object::remote_object_id(existing.stored.object())
        )));
    }
    existing.spool_path = match (&existing.spool_path, duplicate.spool_path) {
        (Some(left), Some(right)) if left != &right => {
            return Err(StoreOutboundError::InvalidOutbound(format!(
                "prepared blob object {} has conflicting spool paths",
                super::remote_object::remote_object_id(existing.stored.object())
            )));
        }
        (Some(left), _) => Some(left.clone()),
        (None, right) => right,
    };
    existing.uploaded_verified |= duplicate.uploaded_verified;
    if !existing.uploaded_verified && existing.spool_path.is_none() {
        return Err(StoreOutboundError::InvalidOutbound(format!(
            "prepared blob object {} awaiting upload has no local spool",
            super::remote_object::remote_object_id(existing.stored.object())
        )));
    }
    Ok(())
}

pub async fn prepare_merge_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<bool, StoreOutboundError> {
    let Some(candidate) = db.blocked_merge_candidate(write_id.clone()).await? else {
        return Ok(false);
    };
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, identity_signer).await?;
    if candidate.commit.value.author_registration != registration_ref {
        return Err(StoreOutboundError::InvalidOutbound(
            "blocked Merge candidate belongs to another local registration".to_string(),
        ));
    }
    let coord = candidate.head.value.commit.coord.clone();
    let commit = StoreBatchCommit::signed_with_candidate_abandonment(
        root.store_root_hash,
        write_id.clone(),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        candidate.commit.value.order.clone(),
        candidate.commit.value.membership_state.clone(),
        candidate.commit.value.device_state.clone(),
        vec![CandidateCleanupManifest {
            candidate: StoreBatchCommitDeletionTarget {
                coord: coord.clone(),
                object: candidate.commit.object.clone(),
                canonical_signed_bytes: candidate.commit.bytes.clone(),
            },
        }],
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = coord.clone()
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate reached Merge abandonment".to_string(),
        ));
    };
    let commit_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id.to_string(),
        sequence,
        commit.commit_hash(),
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            commit.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreDeviceHead::signed(
        root.store_root_hash,
        registration_ref,
        commit_ref,
        candidate.head.value.successor,
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = head_slot_prefix(device_id, sequence);
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            candidate.head.object.slot().clone(),
            &head_prefix,
            head.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    db.prepare_merge_candidate_abandonment(MergeCandidateAbandonmentPreparation {
        write_id,
        commit: PreparedProtocolObject {
            value: commit,
            prepared: commit_prepared,
        },
        head: PreparedProtocolObject {
            value: head,
            prepared: head_prepared,
        },
    })
    .await?;
    Ok(true)
}

pub async fn prepare_serial_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    branch_id: crate::PendingBranchId,
) -> Result<bool, StoreOutboundError> {
    if let Some(prepared) = db.prepared_serial_candidate_abandonment().await? {
        if prepared.branch_id != branch_id {
            return Err(StoreOutboundError::InvalidOutbound(
                "another Serial branch already owns candidate abandonment".to_string(),
            ));
        }
        return Ok(false);
    }
    let branch = db.prepared_serial_store_branch().await?.ok_or_else(|| {
        StoreOutboundError::InvalidOutbound(
            "Serial candidate abandonment requires an exact prepared branch".to_string(),
        )
    })?;
    if branch.branch_id != branch_id {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate abandonment names another prepared branch".to_string(),
        ));
    }
    let candidate = branch.writes.first().ok_or_else(|| {
        StoreOutboundError::InvalidOutbound("prepared Serial branch has no candidates".to_string())
    })?;
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, identity_signer).await?;
    if candidate.commit.value.author_registration != registration_ref {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate belongs to another local registration".to_string(),
        ));
    }
    let coord = StoreCommitCoord::Serial {
        sequence: candidate.commit.value.seq(),
    };
    let candidate_ref = StoreBatchCommitRef::from_commit(
        &candidate.commit.value,
        coord.clone(),
        candidate.commit.object.clone(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let commit = StoreBatchCommit::signed_with_candidate_abandonment(
        root.store_root_hash,
        candidate.commit.value.write_id.clone(),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        candidate.commit.value.order.clone(),
        candidate.commit.value.membership_state.clone(),
        candidate.commit.value.device_state.clone(),
        vec![CandidateCleanupManifest {
            candidate: StoreBatchCommitDeletionTarget {
                coord: coord.clone(),
                object: candidate.commit.object.clone(),
                canonical_signed_bytes: candidate.commit.bytes.clone(),
            },
        }],
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        SERIAL_STREAM_ID,
        commit.seq(),
        commit.commit_hash(),
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
        .map_err(StoreObjectError::from)?;
    let authority_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreSerialHead::signed(
        root.store_root_hash,
        StoreSerialHeadState::Commit {
            author_registration: registration_ref,
            commit: authority_ref,
        },
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.prepare_serial_candidate_abandonment(SerialCandidateAbandonmentPreparation {
        branch_id,
        candidate: candidate_ref,
        commit: PreparedProtocolObject {
            value: commit,
            prepared: commit_prepared,
        },
        head,
        original_head_bytes: branch.head.bytes,
    })
    .await?;
    Ok(true)
}

pub async fn publish_serial_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    branch_id: crate::PendingBranchId,
) -> Result<SerialCandidateAbandonmentWinner, StoreOutboundError> {
    let prepared = db
        .prepared_serial_candidate_abandonment()
        .await?
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial candidate abandonment is not prepared".to_string(),
            )
        })?;
    if prepared.branch_id != branch_id {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment belongs to another branch".to_string(),
        ));
    }
    let classify = |observed: SerialHeadObservation| {
        let accepted = observed.versioned().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial abandonment observed no versioned coordination head".to_string(),
            )
        })?;
        if observed.bytes() == Some(prepared.head.bytes.as_slice()) {
            return Ok(SerialCandidateAbandonmentWinner::Authority { accepted });
        }
        if observed.bytes() == Some(prepared.original_head_bytes.as_slice()) {
            return Ok(SerialCandidateAbandonmentWinner::OriginalBranch { accepted });
        }
        Ok(SerialCandidateAbandonmentWinner::Other {
            current: observed.predecessor()?,
        })
    };
    let observed = observe_serial_head(db, coordination).await?;
    if observed.bytes() == Some(prepared.head.bytes.as_slice())
        || observed.bytes() == Some(prepared.original_head_bytes.as_slice())
        || observed.predecessor()? != db.exact_serial_predecessor(prepared.base.clone()).await?
    {
        return classify(observed);
    }
    if observed.bytes() != Some(prepared.base_head.bytes.as_slice())
        || observed.version() != Some(&prepared.base_head.version)
    {
        return Err(StoreOutboundError::InvalidState {
            key: SERIAL_COORDINATION_HEAD,
            reason: "bytes or provider version changed at the abandonment base".to_string(),
        });
    }
    storage
        .create_protocol_object(&prepared.authority.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let context = ProtocolObjectContext::signed_plaintext(
        prepared.authority.value.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        prepared.authority.value.candidate_family(),
        SERIAL_STREAM_ID,
        prepared.authority.value.seq(),
        prepared.authority.value.commit_hash(),
    );
    let opened = storage
        .read_protocol_object(&context, &prepared.authority.object, &prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != prepared.authority.bytes {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    let authority_ref = StoreBatchCommitRef::from_commit(
        &prepared.authority.value,
        StoreCommitCoord::Serial {
            sequence: prepared.authority.value.seq(),
        },
        prepared.authority.object.clone(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.mark_candidate_commit_uploaded(authority_ref).await?;
    let activation = coordination
        .replace_head(
            serial_head_key(),
            &prepared.base_head.version,
            &prepared.head.bytes,
        )
        .await;
    match activation {
        Ok(accepted) if accepted.bytes == prepared.head.bytes => {
            Ok(SerialCandidateAbandonmentWinner::Authority { accepted })
        }
        Ok(_) => Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment head readback differs from its signed bytes".to_string(),
        )),
        Err(ReplaceHeadError::Coordination(error)) => {
            let after = observe_serial_head(db, coordination).await?;
            if after.bytes() != Some(prepared.base_head.bytes.as_slice())
                || after.version() != Some(&prepared.base_head.version)
            {
                classify(after)
            } else {
                Err(StoreOutboundError::Coordination(error))
            }
        }
        Err(ReplaceHeadError::VersionMismatch) => {
            classify(observe_serial_head(db, coordination).await?)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn abandon_serial_branch(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    store_dir: &StoreDir,
    branch_id: crate::PendingBranchId,
) -> Result<SerialBranchAbandonment, StoreOutboundError> {
    prepare_serial_candidate_abandonment(
        db,
        storage,
        device_id,
        identity_signer,
        branch_id.clone(),
    )
    .await?;
    let prepared = db
        .prepared_serial_candidate_abandonment()
        .await?
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial candidate abandonment disappeared after preparation".to_string(),
            )
        })?;
    let winner =
        publish_serial_candidate_abandonment(db, storage, coordination, branch_id.clone()).await?;
    let root = required_store_root(db).await?;
    match winner {
        SerialCandidateAbandonmentWinner::OriginalBranch { accepted } => {
            let plan = super::store_pull::prepare_serial_resolution(
                db,
                storage,
                coordination,
                root.store_root_hash,
                store_dir,
                prepared.base,
                identity_signer,
            )
            .await?;
            super::store_pull::cleanup_serial_abandonment_authority(db, storage, &plan).await?;
            db.remove_losing_serial_abandonment_authority().await?;
            db.complete_prepared_serial_branch(accepted).await?;
            Ok(SerialBranchAbandonment::OriginalBranchActivated)
        }
        SerialCandidateAbandonmentWinner::Authority { .. } => {
            let authority_ref = StoreBatchCommitRef::from_commit(
                &prepared.authority.value,
                StoreCommitCoord::Serial {
                    sequence: prepared.authority.value.seq(),
                },
                prepared.authority.object,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            db.mark_serial_branch_conflict(
                branch_id.clone(),
                prepared.base.clone(),
                StoreSerialPredecessor::Commit(authority_ref),
            )
            .await?;
            finish_serial_branch_abandonment(
                db,
                storage,
                coordination,
                identity_signer,
                store_dir,
                root.store_root_hash,
                branch_id,
                prepared.base,
            )
            .await
        }
        SerialCandidateAbandonmentWinner::Other { current } => {
            db.mark_serial_branch_conflict(branch_id.clone(), prepared.base.clone(), current)
                .await?;
            finish_serial_branch_abandonment(
                db,
                storage,
                coordination,
                identity_signer,
                store_dir,
                root.store_root_hash,
                branch_id,
                prepared.base,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_serial_branch_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    identity_signer: &UserKeypair,
    store_dir: &StoreDir,
    store_root_hash: ObjectHash,
    branch_id: crate::PendingBranchId,
    branch_base: Option<StoreBatchCommitRef>,
) -> Result<SerialBranchAbandonment, StoreOutboundError> {
    let plan = super::store_pull::prepare_serial_resolution(
        db,
        storage,
        coordination,
        store_root_hash,
        store_dir,
        branch_base,
        identity_signer,
    )
    .await?;
    super::store_pull::cleanup_serial_candidates(db, storage, branch_id.clone(), &plan).await?;
    super::store_pull::cleanup_serial_abandonment_authority(db, storage, &plan).await?;
    db.discard_serial_branch_after_abandonment(branch_id, plan)
        .await?;
    Ok(SerialBranchAbandonment::Discarded)
}

pub async fn abandon_merge_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreOutboundError> {
    match db.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
                return Ok(MergeCandidateAbandonment::Abandoned);
            }
            if !prepare_merge_candidate_abandonment(
                db,
                storage,
                device_id,
                identity_signer,
                write_id.clone(),
            )
            .await?
            {
                return Ok(MergeCandidateAbandonment::NotRequired);
            }
        }
        crate::database::MergeAbandonmentState::Prepared => {}
        crate::database::MergeAbandonmentState::Accepted
        | crate::database::MergeAbandonmentState::CandidateWon
        | crate::database::MergeAbandonmentState::OtherWon => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
            }
            return finish_merge_abandonment(db, storage, write_id).await;
        }
    }
    drain_store_writes(db, storage).await?;
    if !db.merge_candidate_cleanup_pending(&write_id).await? {
        return Err(StoreOutboundError::InvalidOutbound(
            "accepted Merge abandonment has no exact cleanup transition".to_string(),
        ));
    }
    super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
    finish_merge_abandonment(db, storage, write_id).await
}

async fn finish_merge_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreOutboundError> {
    match db.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None
        | crate::database::MergeAbandonmentState::Accepted => {
            Ok(MergeCandidateAbandonment::Abandoned)
        }
        crate::database::MergeAbandonmentState::OtherWon => {
            db.finish_lost_merge_abandonment(write_id).await?;
            Ok(MergeCandidateAbandonment::Abandoned)
        }
        crate::database::MergeAbandonmentState::CandidateWon => {
            db.resume_winning_merge_candidate(write_id).await?;
            drain_store_writes(db, storage).await?;
            Ok(MergeCandidateAbandonment::CandidateActivated)
        }
        crate::database::MergeAbandonmentState::Prepared => {
            Err(StoreOutboundError::InvalidOutbound(
                "Merge abandonment has no accepted head outcome".to_string(),
            ))
        }
    }
}

/// Publish the exact prepared object graph in sequence order. Every remote object
/// is verified at its reserved slot before the exact head activates the commit.
async fn read_occupied_merge_head(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    expected: &StoreDeviceHead,
    slot: &crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
) -> Result<(StoreDeviceHead, PreparedExactObject), StoreOutboundError> {
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreHead);
    let (winner_bytes, winner_prepared) = storage
        .read_prepared_protocol_slot(&context, slot, semantic_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    let unverified: StoreDeviceHead = serde_json::from_slice(&winner_bytes).map_err(|error| {
        StoreOutboundError::InvalidOutbound(format!("parse competing Merge head: {error}"))
    })?;
    if unverified.author_registration != expected.author_registration
        || unverified.commit.coord != expected.commit.coord
        || unverified.successor.activation != expected.successor.activation
        || unverified.successor.predecessor != expected.successor.predecessor
    {
        return Err(StoreOutboundError::InvalidOutbound(
            "competing Merge head does not occupy the prepared successor point".to_string(),
        ));
    }
    let registration = db
        .activated_store_device_registration(expected.author_registration.clone())
        .await?;
    if unverified.commit != expected.commit {
        super::store_objects::load_commit_ref(
            storage,
            store_root_hash,
            &unverified.commit,
            &registration,
        )
        .await?;
    }
    let winner = StoreDeviceHead::parse_at(
        &winner_bytes,
        store_root_hash,
        &registration,
        &unverified.commit,
    )
    .map_err(|error| {
        StoreOutboundError::InvalidOutbound(format!("verify occupied Merge head: {error}"))
    })?;
    Ok((winner, winner_prepared))
}

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
            let store_root_hash = required_store_root(db).await?.store_root_hash;
            let commit = &batch.commit.value;
            if !matches!(
                commit.body,
                super::store_commit::StoreCommitBody::AbandonCandidates { .. }
            ) {
                publish_prepared_remote_objects(db, storage, &write_id, store_root_hash).await?;
            }
            let head = &batch.head.value;
            let stream_id = match &head.commit.coord {
                StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
                StoreCommitCoord::Serial { .. } => {
                    return Err(StoreOutboundError::InvalidOutbound(
                        "Serial commit reached MergeConcurrent drain".to_string(),
                    ));
                }
            };
            let commit_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            );
            let commit_prefix = commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id,
                commit.seq(),
                commit.commit_hash(),
            );
            storage
                .create_protocol_object(&batch.commit.prepared)
                .await
                .map_err(StoreObjectError::from)?;
            let opened_commit = storage
                .read_protocol_object(&commit_context, &batch.commit.object, &commit_prefix)
                .await
                .map_err(StoreObjectError::from)?;
            if opened_commit != batch.commit.bytes {
                return Err(StoreOutboundError::InvalidOutbound(
                    "prepared commit exact readback differs from its signed bytes".to_string(),
                ));
            }
            db.mark_candidate_commit_uploaded(head.commit.clone())
                .await?;
            let head_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let head_prefix = head_slot_prefix(
                &head.author_registration.device_id.to_string(),
                commit.seq(),
            );
            if let Err(error) = storage.create_protocol_object(&batch.head.prepared).await {
                if !matches!(error, StorageError::SlotCollision(_)) {
                    return Err(StoreObjectError::from(error).into());
                }
                let (winner, winner_prepared) = read_occupied_merge_head(
                    db,
                    storage,
                    store_root_hash,
                    head,
                    batch.head.object.slot(),
                    &head_prefix,
                )
                .await?;
                if winner.commit == head.commit {
                    db.adopt_alternate_merge_head(write_id.clone(), winner, winner_prepared)
                        .await?;
                    db.complete_prepared_store_write(head.commit.clone())
                        .await?;
                    return Ok::<bool, StoreOutboundError>(true);
                }
                let winner_ref = StoreDeviceHeadRef {
                    head_hash: winner.head_hash(),
                    object: winner_prepared.reference().clone(),
                };
                db.mark_merge_candidate_conflict(
                    write_id.clone(),
                    winner.commit.clone(),
                    winner_ref,
                )
                .await?;
                return Ok::<bool, StoreOutboundError>(false);
            }
            let opened_head = storage
                .read_protocol_object(&head_context, &batch.head.object, &head_prefix)
                .await
                .map_err(StoreObjectError::from)?;
            if opened_head != batch.head.bytes {
                return Err(StoreOutboundError::InvalidOutbound(
                    "prepared head exact readback differs from its signed bytes".to_string(),
                ));
            }
            db.mark_store_head_uploaded(StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: batch.head.object.clone(),
            })
            .await?;
            db.complete_prepared_store_write(head.commit.clone())
                .await?;
            Ok::<bool, StoreOutboundError>(true)
        }
        .await;
        match attempt {
            Ok(false) => return Ok(published),
            Ok(true) => {}
            Err(error) => {
                if let Some(block) = blocked_status(&error) {
                    db.set_write_status(&write_id, crate::WriteStatus::Blocked(block))
                        .await?;
                }
                return Err(error);
            }
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
        bytes: Vec<u8>,
        version: VersionToken,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedSerialControl {
    pub base_head: VersionedObject,
    pub commit: StoreBatchCommit,
    pub commit_prepared: PreparedExactObject,
    pub commit_ref: StoreBatchCommitRef,
    pub head: StoreSerialHead,
    pub authorization_after: SerialAuthorizationState,
}

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
    let authorization =
        super::store_pull::load_serial_authorization_at_head(storage, &root_ref, head)
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

pub(crate) async fn prepare_serial_control(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    control: StoreControl,
    keypair: &UserKeypair,
) -> Result<PreparedSerialControl, StoreOutboundError> {
    let snapshot = current_serial_authorization_snapshot(db, storage, coordination).await?;
    let base = snapshot.base;
    let base_head = snapshot.base_head;
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, keypair).await?;
    let store_root_hash = root.store_root_hash;
    let seq = base
        .as_ref()
        .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
    let coord = StoreCommitCoord::Serial { sequence: seq };
    let order = StoreCommitOrder::Serial {
        seq,
        predecessor: match &base {
            Some(reference) => StoreSerialPredecessor::Commit(reference.clone()),
            None => StoreSerialPredecessor::Genesis {
                root: root.clone(),
                founder_registration: registration_ref.clone(),
            },
        },
    };
    let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
    let serial_position = match &order {
        StoreCommitOrder::Serial { predecessor, .. } => predecessor.clone(),
        StoreCommitOrder::MergeConcurrent { .. } => unreachable!(),
    };
    let membership_state = super::circle_control::StoreMembershipStateRef::serial(
        serial_position,
        resolved_devices.recovery,
        &snapshot.authorization,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreCommit);
    let commit = StoreBatchCommit::signed_with_control(
        store_root_hash,
        db.new_write_id(),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        order,
        membership_state,
        device_state,
        None,
        Some(control),
        None,
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
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let root_value = super::store_objects::load_store_protocol_root(storage, &root)
        .await?
        .value;
    super::store_pull::validate_serial_provider_admin_control(
        storage,
        &root,
        &root_value,
        commit.control(),
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let authorization_after = snapshot
        .authorization
        .authorize_and_apply(&commit_ref, &commit, &registration)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreSerialHead::signed(
        store_root_hash,
        StoreSerialHeadState::Commit {
            author_registration: registration_ref,
            commit: commit_ref.clone(),
        },
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    Ok(PreparedSerialControl {
        base_head,
        commit,
        commit_prepared,
        commit_ref,
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
        &prepared.base_head,
        &prepared.commit,
        &prepared.commit_prepared,
        &prepared.commit_ref,
        &prepared.head,
    )
    .await?;
    db.materialize_serial_control_commit(
        prepared.commit.clone(),
        prepared.commit_ref.clone(),
        prepared.authorization_after.clone(),
    )
    .await?;
    Ok(())
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
) -> Result<(), StoreOutboundError> {
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
        return Ok(());
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
        return Ok(());
    }
    let after = observe_serial_head(db, coordination).await?;
    if after.bytes() == Some(head_bytes.as_slice()) {
        return Ok(());
    }
    if let Err(Some(error)) = activation {
        return Err(error);
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

    fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Absent => None,
            Self::Present { bytes, .. } => Some(bytes),
        }
    }

    fn predecessor(&self) -> Result<StoreSerialPredecessor, StoreOutboundError> {
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

    fn versioned(&self) -> Option<super::storage::VersionedObject> {
        match self {
            Self::Absent => None,
            Self::Present { bytes, version, .. } => Some(super::storage::VersionedObject {
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

async fn observe_serial_head(
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
async fn prepare_serial_store_branch(
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
        let current = db.exact_serial_predecessor(snapshot.base).await?;
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
        let mut resolved_device_state: Option<super::store_commit::ResolvedStoreDeviceState> = None;
        for write in branch.writes {
            if !write.changeset.is_empty() && write.inverse_changeset.is_empty() {
                return Err(StoreOutboundError::InvalidOutbound(format!(
                    "Serial write {} has no inverse changeset",
                    write.write_id
                )));
            }
            let payload =
                super::service::prepare_store_payload(&write.blob_facts, keypair, store_dir, None)
                    .await
                    .map_err(StoreOutboundError::Preparation)?;
            let seq = predecessor
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
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
            let device_state = super::store_commit::StoreDeviceStateRef::serial(
                serial_position.clone(),
                &resolved_devices,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let membership_state = super::circle_control::StoreMembershipStateRef::serial(
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
                .find(|package| package.audience == super::circle::Audience::Store)
                .map(|package| StorePackageInput {
                    candidate_family,
                    schema_version: db.schema_version(),
                    bytes: package.semantic_bytes.as_slice(),
                    object: package.prepared.reference().clone(),
                });
            let circle_packages = prepared_packages
                .iter()
                .filter_map(|package| {
                    let super::circle::Audience::Circle(circle_id) = package.audience else {
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
                close_prepared_packages(prepared_packages, &commit_ref)?;
            predecessor = Some(commit_ref);
            prepared.push(SerialStoreWritePreparationEntry {
                write_id: write.write_id,
                remote_objects,
                audiences,
                commit: PreparedProtocolObject {
                    value: commit,
                    prepared: commit_prepared,
                },
                local_cleanup: payload.local_cleanup,
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

async fn drain_serial_store_branch(
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
        return db
            .complete_prepared_serial_branch(accepted)
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
    db.complete_prepared_serial_branch(accepted)
        .await
        .map_err(Into::into)
}

fn blocked_status(error: &StoreOutboundError) -> Option<crate::WriteBlock> {
    match error {
        StoreOutboundError::Database(_)
        | StoreOutboundError::BlobStorage { .. }
        | StoreOutboundError::Coordination(_)
        | StoreOutboundError::CandidateCleanup(_) => None,
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
        StoreOutboundError::Object(StoreObjectError::Storage(_)) => None,
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
        StoreOutboundError::Preparation(super::service::SyncCycleError::MissingPreparedBlob {
            namespace,
            id,
        }) => Some(crate::WriteBlock::MissingBlob {
            namespace: namespace.clone(),
            id: id.clone(),
        }),
        StoreOutboundError::Preparation(super::service::SyncCycleError::Gate(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::AssetScan(_))
        | StoreOutboundError::Preparation(super::service::SyncCycleError::Database(_)) => {
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

async fn publish_prepared_remote_objects(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: &crate::WriteId,
    store_root_hash: ObjectHash,
) -> Result<(), StoreOutboundError> {
    for prepared in db.prepared_remote_objects(write_id).await? {
        let remote = prepared.record;
        let prepared_state = match &remote {
            super::remote_object::RemoteObjectRecord::CandidateCommit(record) => matches!(
                record.state,
                super::remote_object::CandidateCommitState::Prepared
            ),
            super::remote_object::RemoteObjectRecord::CandidateExclusive(record) => matches!(
                record.state,
                super::remote_object::CandidateObjectState::Prepared { .. }
            ),
            super::remote_object::RemoteObjectRecord::SharedLiveSet(record) => matches!(
                record.state,
                super::remote_object::OwnedObjectState::Prepared { .. }
            ),
            super::remote_object::RemoteObjectRecord::RetainedAuthority(_) => false,
        };
        match remote.bytes().stored() {
            super::remote_object::RemoteStoredRepresentation::Inline { bytes, object } => {
                let package = super::audience_package::AudiencePackage::parse(
                    remote.bytes().canonical_semantic_bytes(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let (stream_id, sequence) = match package.commit_coord() {
                    StoreCommitCoord::MergeConcurrent {
                        stream_id,
                        sequence,
                    } => (stream_id.to_string(), *sequence),
                    StoreCommitCoord::Serial { sequence } => {
                        (SERIAL_STREAM_ID.to_string(), *sequence)
                    }
                };
                let (context, prefix) = match package.audience() {
                    super::audience_package::PackageAudience::Store => (
                        ProtocolObjectContext::store_encrypted(
                            store_root_hash,
                            ProtocolObjectDomain::StorePackage,
                        ),
                        package_semantic_prefix(
                            package.candidate_family(),
                            &stream_id,
                            sequence,
                            ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                        ),
                    ),
                    super::audience_package::PackageAudience::Circle {
                        circle_id, control, ..
                    } => {
                        let (encryption, _) = db
                            .circle_publication_context(*circle_id, control.clone())
                            .await?;
                        (
                            ProtocolObjectContext::circle(
                                store_root_hash,
                                ProtocolObjectDomain::CirclePackage,
                                encryption,
                            ),
                            circle_package_semantic_prefix(
                                *circle_id,
                                package.candidate_family(),
                                &stream_id,
                                sequence,
                                ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                            ),
                        )
                    }
                };
                let prepared = PreparedExactObject::new(object.clone(), bytes.clone())
                    .map_err(StoreObjectError::from)?;
                if prepared_state {
                    storage
                        .create_protocol_object(&prepared)
                        .await
                        .map_err(StoreObjectError::from)?;
                }
                let opened = storage
                    .read_protocol_object(&context, object, &prefix)
                    .await
                    .map_err(StoreObjectError::from)?;
                if opened != remote.bytes().canonical_semantic_bytes() {
                    return Err(StoreOutboundError::InvalidOutbound(format!(
                        "remote package {} exact readback differs from its canonical bytes",
                        remote.object_id()
                    )));
                }
            }
            super::remote_object::RemoteStoredRepresentation::Blob { object } => {
                let locator = crate::blob::locator::BlobLocator::parse(
                    remote.bytes().canonical_semantic_bytes(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let uploader = locator.uploader().clone();
                let registration = db
                    .activated_store_device_registration(uploader.clone())
                    .await?;
                let authority = BlobWriteAuthority::new(&uploader, &registration)
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                let blob = crate::blob::locator::StoredBlobRef::new(locator, object.clone())
                    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
                if prepared_state {
                    let path = prepared.spool_path.as_deref().ok_or_else(|| {
                        StoreOutboundError::InvalidOutbound(format!(
                            "prepared blob {} awaiting upload has no local spool",
                            remote.object_id()
                        ))
                    })?;
                    storage
                        .create_blob_object_from_file(
                            &blob,
                            &authority,
                            path,
                            &crate::storage::cloud::no_progress(),
                        )
                        .await
                        .map_err(|source| StoreOutboundError::BlobStorage {
                            namespace: blob.locator().namespace().to_string(),
                            id: blob.locator().blob_id().to_string(),
                            source,
                        })?;
                }
                storage.verify_blob_object(&blob).await.map_err(|source| {
                    StoreOutboundError::BlobStorage {
                        namespace: blob.locator().namespace().to_string(),
                        id: blob.locator().blob_id().to_string(),
                        source,
                    }
                })?;
            }
        }
        if prepared_state {
            db.mark_remote_object_uploaded(remote).await?;
        }
    }
    Ok(())
}

async fn required_store_root(db: &Database) -> Result<StoreRootRef, StoreOutboundError> {
    db.local_store_root_ref()
        .await?
        .ok_or(StoreOutboundError::MissingState {
            key: STORE_ROOT_AUTHORITY,
        })
}

pub(crate) enum StoreOperationBatch {
    Acknowledgement(super::store_commit::StoreAckRef),
    ProviderAccessGrant(super::provider::StoreMemberProviderAccessGrantRef),
    Attempt(DeviceJoinAttemptRef),
    Abandonment(super::device_join::DeviceJoinAbandonmentRef),
    Outcome {
        outcome: DeviceJoinOutcomeRef,
        registration: Option<Box<DeviceJoinRegistrationActivation>>,
    },
    CleanupReceipt(super::device_join::DeviceJoinCleanupReceiptRef),
    DeviceExclusionProposal(super::store_commit::StoreDeviceExclusionProposalRef),
    DeviceExclusionOutcome(super::store_commit::StoreDeviceExclusionOutcomeRef),
    ReclaimAuthorization(Box<super::store_reclaim::ReclaimAuthorizationRef>),
    ReclaimReceipt(Box<super::store_reclaim::ReclaimReceiptRef>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeviceJoinRegistrationActivation {
    pub reference: ActivatedStoreDeviceRegistrationRef,
    pub registration: StoreDeviceRegistration,
    pub authority: super::store_commit::StoreDeviceRegistrationActivation,
}

pub(crate) struct StoreOperationCommitPlan {
    root: StoreRootRef,
    registration_ref: StoreDeviceRegistrationRef,
    registration: Box<StoreDeviceRegistration>,
    device_signer: UserKeypair,
    coord: StoreCommitCoord,
    order: StoreCommitOrder,
    membership_state: super::circle_control::StoreMembershipStateRef,
    device_state: super::store_commit::StoreDeviceStateRef,
    owner_grant: Option<super::membership::MembershipGrantId>,
    serial: Option<Box<StoreOperationSerialPlan>>,
}

struct StoreOperationSerialPlan {
    base_head: VersionedObject,
    authorization: SerialAuthorizationState,
}

impl StoreOperationCommitPlan {
    pub(crate) fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreOutboundError> {
        self.order
            .predecessor_cut()
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn membership_state(&self) -> &super::circle_control::StoreMembershipStateRef {
        &self.membership_state
    }

    pub(crate) fn device_state(&self) -> &super::store_commit::StoreDeviceStateRef {
        &self.device_state
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(crate) fn registration_ref(&self) -> &StoreDeviceRegistrationRef {
        &self.registration_ref
    }

    pub(crate) fn registration(&self) -> &StoreDeviceRegistration {
        &self.registration
    }

    pub(crate) fn device_signer(&self) -> &UserKeypair {
        &self.device_signer
    }

    pub(crate) fn owner_grant(&self) -> Option<&super::membership::MembershipGrantId> {
        self.owner_grant.as_ref()
    }

    pub(crate) fn validate_acknowledgement(
        &self,
        acknowledgement: &super::store_commit::StoreAck,
    ) -> Result<(), StoreOutboundError> {
        if acknowledgement.registration != self.registration_ref
            || acknowledgement.store_cut != self.predecessor_cut()?
            || acknowledgement.device_state != self.device_state
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "Store acknowledgement differs from its operation commit predecessor".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) async fn serial_successor_plan_for_test(
    db: &Database,
    device_id: &str,
    signer: &UserKeypair,
    predecessor: &PreparedStoreOperationCommit,
    base_head: VersionedObject,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    let StoreOperationPublication::Serial {
        head,
        authorization_after,
        ..
    } = &predecessor.publication
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "test Serial successor predecessor uses Merge publication".to_string(),
        ));
    };
    if base_head.bytes != head.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "test Serial successor receipt differs from its predecessor head".to_string(),
        ));
    }
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, signer).await?;
    let sequence = predecessor
        .reference
        .coord
        .sequence()
        .checked_add(1)
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "test Serial successor sequence overflow".to_string(),
            )
        })?;
    let position = StoreSerialPredecessor::Commit(predecessor.reference.clone());
    let membership_state = super::circle_control::StoreMembershipStateRef::serial(
        position.clone(),
        predecessor.commit.membership_state.recovery().to_vec(),
        authorization_after,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let device_state = super::store_commit::StoreDeviceStateRef::Serial {
        position: position.clone(),
        recovery: predecessor.commit.device_state.recovery().to_vec(),
        state_hash: predecessor.commit.device_state.state_hash(),
    };
    let owner_grant = authorization_after
        .membership
        .active_owner_grant(&registration.author_pubkey);
    Ok(StoreOperationCommitPlan {
        root,
        registration_ref,
        registration: Box::new(registration),
        device_signer,
        coord: StoreCommitCoord::Serial { sequence },
        order: StoreCommitOrder::Serial {
            seq: sequence,
            predecessor: position,
        },
        membership_state,
        device_state,
        owner_grant,
        serial: Some(Box::new(StoreOperationSerialPlan {
            base_head,
            authorization: authorization_after.clone(),
        })),
    })
}

pub(crate) async fn prepare_store_operation_commit(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    keypair: &UserKeypair,
    membership: Option<&MembershipChain>,
) -> Result<StoreOperationCommitPlan, StoreOutboundError> {
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, keypair).await?;
    let (coord, order, membership_state, device_state, owner_grant, serial) = match db
        .write_policy()
    {
        crate::WritePolicy::MergeConcurrent => {
            let previous = db.latest_local_store_position().await?;
            let dependencies = super::store_commit::CommitFrontier::from_refs(
                crate::WritePolicy::MergeConcurrent,
                db.materialized_frontier().await?,
            )
            .and_then(|frontier| frontier.merge_commits().cloned())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let seq = previous
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            let coord = StoreCommitCoord::MergeConcurrent {
                stream_id: AuthorStreamId::store_announcements(&root, &registration_ref),
                sequence: seq,
            };
            let order = StoreCommitOrder::MergeConcurrent {
                seq,
                predecessor: previous,
                dependencies,
            };
            let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
            let membership = membership.ok_or_else(|| {
                StoreOutboundError::InvalidOutbound(
                    "Merge device join commit has no exact membership state".to_string(),
                )
            })?;
            let super::membership::MembershipStatus::Resolved(resolved) = membership.status()
            else {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Merge device join commit requires resolved membership".to_string(),
                ));
            };
            let membership_state =
                super::circle_control::StoreMembershipStateRef::merge_concurrent(
                    membership.head_refs().to_vec(),
                    membership.resolution_refs().to_vec(),
                    resolved_devices.recovery,
                    resolved.state_hash,
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let owner_grant = membership.active_owner_grant(&registration.author_pubkey);
            (
                coord,
                order,
                membership_state,
                device_state,
                owner_grant,
                None,
            )
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?;
            let snapshot = current_serial_authorization_snapshot(db, storage, coordination).await?;
            let seq = snapshot
                .base
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            let predecessor = snapshot.base.clone().map_or_else(
                || StoreSerialPredecessor::Genesis {
                    root: root.clone(),
                    founder_registration: registration_ref.clone(),
                },
                StoreSerialPredecessor::Commit,
            );
            let coord = StoreCommitCoord::Serial { sequence: seq };
            let order = StoreCommitOrder::Serial {
                seq,
                predecessor: predecessor.clone(),
            };
            let (device_state, resolved_devices) = db.store_device_state_for_order(&order).await?;
            let membership_state = super::circle_control::StoreMembershipStateRef::serial(
                predecessor,
                resolved_devices.recovery,
                &snapshot.authorization,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let serial = StoreOperationSerialPlan {
                base_head: snapshot.base_head,
                authorization: snapshot.authorization,
            };
            let owner_grant = serial
                .authorization
                .membership
                .active_owner_grant(&registration.author_pubkey);
            (
                coord,
                order,
                membership_state,
                device_state,
                owner_grant,
                Some(Box::new(serial)),
            )
        }
    };
    Ok(StoreOperationCommitPlan {
        root,
        registration_ref,
        registration: Box::new(registration),
        device_signer,
        coord,
        order,
        membership_state,
        device_state,
        owner_grant,
        serial,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedStoreOperationCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) prepared: PreparedExactObject,
    pub(crate) reference: StoreBatchCommitRef,
    publication: StoreOperationPublication,
    registration_activation: Option<DeviceJoinRegistrationActivation>,
}

impl PreparedStoreOperationCommit {
    pub(crate) fn merge_publication(&self) -> Option<(&StoreDeviceHead, &PreparedExactObject)> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { head, prepared } => Some((head, prepared)),
            StoreOperationPublication::Serial { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn merge_publication_for_test(
        &self,
    ) -> Option<(&StoreDeviceHead, &PreparedExactObject)> {
        self.merge_publication()
    }

    #[cfg(test)]
    pub(crate) fn serial_publication_for_test(
        &self,
    ) -> Option<(&VersionedObject, &StoreSerialHead)> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { .. } => None,
            StoreOperationPublication::Serial {
                base_head, head, ..
            } => Some((base_head, head)),
        }
    }

    pub(crate) fn acknowledgement_remote_objects(
        &self,
        acknowledgement: &crate::database::ExactProtocolObject<super::store_commit::StoreAck>,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        let reference = self.commit.acknowledgement().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "prepared acknowledgement operation has no exact acknowledgement ref".to_string(),
            )
        })?;
        if reference.object != acknowledgement.object
            || reference.ack_hash != acknowledgement.value.ack_hash()
            || acknowledgement.value.to_bytes() != acknowledgement.bytes
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "prepared acknowledgement operation differs from its exact acknowledgement object"
                    .to_string(),
            ));
        }
        let authority =
            super::remote_object::RemoteObjectRecord::candidate_activated_store_acknowledgement(
                reference.clone(),
                acknowledgement.bytes.clone(),
                acknowledgement.prepared.stored_bytes().to_vec(),
                self.reference.clone(),
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
        self.retained_authority_remote_objects(vec![authority])
    }

    pub(crate) fn retained_authority_remote_objects(
        &self,
        authorities: Vec<super::remote_object::RemoteObjectRecord>,
    ) -> Result<Vec<super::remote_object::RemoteObjectRecord>, StoreOutboundError> {
        if authorities.is_empty() {
            return Err(StoreOutboundError::InvalidOutbound(
                "Store operation has no retained authority objects".to_string(),
            ));
        }
        let mut authority_ids = std::collections::BTreeSet::new();
        for authority in &authorities {
            authority
                .validate()
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            if !matches!(authority, super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(&record.state, super::remote_object::RetainedAuthorityObjectState::Prepared { ownership }
                    if ownership.pending == std::collections::BTreeSet::from([self.reference.clone()])))
            {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Store operation retained authority has different candidate ownership"
                        .to_string(),
                ));
            }
            if !authority_ids.insert(authority.object_id()) {
                return Err(StoreOutboundError::InvalidOutbound(
                    "Store operation repeats a retained authority object".to_string(),
                ));
            }
        }
        let mut objects = vec![super::remote_object::RemoteObjectRecord::candidate_commit(
            self.reference.clone(),
            self.commit.to_bytes(),
            self.prepared.stored_bytes().to_vec(),
        )
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?];
        objects.extend(authorities);
        if let StoreOperationPublication::MergeConcurrent { head, prepared } = &self.publication {
            objects.push(
                super::remote_object::RemoteObjectRecord::candidate_activated_store_head(
                    super::store_commit::StoreDeviceHeadRef {
                        head_hash: head.head_hash(),
                        object: prepared.reference().clone(),
                    },
                    head.to_bytes(),
                    prepared.stored_bytes().to_vec(),
                    self.reference.clone(),
                )
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
            );
        }
        Ok(objects)
    }

    pub(crate) fn merge_head_ref(&self) -> Option<StoreDeviceHeadRef> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { head, prepared } => {
                Some(StoreDeviceHeadRef {
                    head_hash: head.head_hash(),
                    object: prepared.reference().clone(),
                })
            }
            StoreOperationPublication::Serial { .. } => None,
        }
    }

    pub(crate) fn serial_base_head(&self) -> Option<&VersionedObject> {
        match &self.publication {
            StoreOperationPublication::MergeConcurrent { .. } => None,
            StoreOperationPublication::Serial { base_head, .. } => Some(base_head),
        }
    }

    pub(crate) fn adopt_serial_base_head(
        &mut self,
        observed: VersionedObject,
    ) -> Result<(), StoreOutboundError> {
        let StoreOperationPublication::Serial { base_head, .. } = &mut self.publication else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Merge Store operation cannot adopt a Serial head receipt".to_string(),
            ));
        };
        if *base_head == observed {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial Store operation already carries the observed head receipt".to_string(),
            ));
        }
        *base_head = observed;
        Ok(())
    }

    pub(crate) fn adopt_merge_head(
        &mut self,
        winner: StoreDeviceHead,
        prepared: PreparedExactObject,
    ) -> Result<(), StoreOutboundError> {
        let StoreOperationPublication::MergeConcurrent {
            head: current,
            prepared: current_prepared,
        } = &mut self.publication
        else {
            return Err(StoreOutboundError::InvalidOutbound(
                "Serial Store operation cannot adopt a Merge head".to_string(),
            ));
        };
        if winner.commit != self.reference
            || prepared.reference().slot() != current_prepared.reference().slot()
            || prepared.reference() == current_prepared.reference()
            || winner.author_registration != current.author_registration
            || winner.successor.activation != current.successor.activation
            || winner.successor.predecessor != current.successor.predecessor
        {
            return Err(StoreOutboundError::InvalidOutbound(
                "alternate Merge head differs from the prepared activation point".to_string(),
            ));
        }
        *current = winner;
        *current_prepared = prepared;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreOperationPublicationOutcome {
    Activated(StoreBatchCommitRef),
    Nonactivated(StoreBatchCommitRef),
    Reprepared,
    RepreparedCandidate(Box<PreparedStoreOperationCommit>),
    NonactivatedCandidate {
        candidate: Box<PreparedStoreOperationCommit>,
        proof: Box<super::remote_object::CandidateNonactivationProof>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum StoreOperationPublication {
    MergeConcurrent {
        head: StoreDeviceHead,
        prepared: PreparedExactObject,
    },
    Serial {
        base_head: VersionedObject,
        head: StoreSerialHead,
        authorization_after: SerialAuthorizationState,
    },
}

pub(crate) async fn prepare_store_operation_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<PreparedStoreOperationCommit, StoreOutboundError> {
    let store_root_hash = plan.root.store_root_hash;
    let registration_activation = match &batch {
        StoreOperationBatch::Outcome { registration, .. } => registration.as_deref().cloned(),
        _ => None,
    };
    let commit = match batch {
        StoreOperationBatch::Acknowledgement(acknowledgement) => {
            StoreBatchCommit::signed_operations(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                None,
                StoreCommitOperationsInput {
                    acknowledgement: Some(acknowledgement),
                    control: None,
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    provider_access_withdrawals: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    circle_controls: Vec::new(),
                    store_package: None,
                    circle_packages: &[],
                },
                &plan.device_signer,
            )
        }
        StoreOperationBatch::ProviderAccessGrant(grant) => {
            StoreBatchCommit::signed_with_provider_access(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                None,
                vec![grant],
                Vec::new(),
                &plan.device_signer,
            )
        }
        StoreOperationBatch::Attempt(attempt) => StoreBatchCommit::signed_with_join_attempts(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            None,
            vec![attempt],
            &plan.device_signer,
        ),
        StoreOperationBatch::Abandonment(abandonment) => {
            StoreBatchCommit::signed_with_join_abandonments(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                None,
                vec![abandonment],
                &plan.device_signer,
            )
        }
        StoreOperationBatch::Outcome {
            outcome,
            registration,
        } => StoreBatchCommit::signed_with_join_outcomes(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            None,
            vec![outcome],
            registration
                .into_iter()
                .map(|activation| activation.reference.clone())
                .collect(),
            &plan.device_signer,
        ),
        StoreOperationBatch::CleanupReceipt(receipt) => {
            StoreBatchCommit::signed_with_join_cleanup_receipts(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                None,
                vec![receipt],
                &plan.device_signer,
            )
        }
        StoreOperationBatch::DeviceExclusionProposal(proposal) => {
            StoreBatchCommit::signed_with_device_exclusions(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                None,
                vec![proposal],
                Vec::new(),
                &plan.device_signer,
            )
        }
        StoreOperationBatch::DeviceExclusionOutcome(outcome) => {
            StoreBatchCommit::signed_with_device_exclusions(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                None,
                Vec::new(),
                vec![outcome],
                &plan.device_signer,
            )
        }
        StoreOperationBatch::ReclaimAuthorization(authorization) => {
            StoreBatchCommit::signed_reclaim_authorization(
                store_root_hash,
                db.new_write_id(),
                plan.coord.clone(),
                plan.registration_ref.clone(),
                &plan.registration,
                plan.order.clone(),
                plan.membership_state.clone(),
                plan.device_state.clone(),
                *authorization,
                &plan.device_signer,
            )
        }
        StoreOperationBatch::ReclaimReceipt(receipt) => StoreBatchCommit::signed_reclaim_receipt(
            store_root_hash,
            db.new_write_id(),
            plan.coord.clone(),
            plan.registration_ref.clone(),
            &plan.registration,
            plan.order.clone(),
            plan.membership_state.clone(),
            plan.device_state.clone(),
            *receipt,
            &plan.device_signer,
        ),
    }
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreCommit);
    let stream_id = match plan.coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    };
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id,
        commit.seq(),
        commit.commit_hash(),
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, plan.coord.clone(), prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let publication = match plan.serial {
        Some(serial) => {
            let authorization_after = serial
                .authorization
                .authorize_and_apply(&commit_ref, &commit, &plan.registration)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let head = StoreSerialHead::signed(
                store_root_hash,
                StoreSerialHeadState::Commit {
                    author_registration: plan.registration_ref,
                    commit: commit_ref.clone(),
                },
                &plan.device_signer,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            StoreOperationPublication::Serial {
                base_head: serial.base_head,
                head,
                authorization_after,
            }
        }
        None => {
            let previous = plan.order.predecessor().cloned();
            let head_context = ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let device_id = plan.registration_ref.device_id.to_string();
            let (head_slot, predecessor_head) = exact_next_announcement_slot(
                storage,
                &plan.root,
                &plan.registration_ref,
                &plan.registration,
                previous.as_ref(),
            )
            .await?;
            let next_prefix = head_slot_prefix(&device_id, commit.seq().saturating_add(1));
            let next_slot = storage
                .allocate_protocol_slot(&head_context, &next_prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let head = StoreDeviceHead::signed(
                store_root_hash,
                plan.registration_ref.clone(),
                commit_ref.clone(),
                SuccessorLink {
                    activation: super::store_commit::StreamActivationId::store_announcements(
                        &plan.root,
                        &plan.registration_ref,
                    ),
                    predecessor: predecessor_head.map(|reference| reference.object),
                    next_slot,
                },
                &plan.device_signer,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            let head_prefix = head_slot_prefix(&device_id, commit.seq());
            let prepared_head = storage
                .prepare_protocol_object(&head_context, head_slot, &head_prefix, head.to_bytes())
                .map_err(StoreObjectError::from)?;
            StoreOperationPublication::MergeConcurrent {
                head,
                prepared: prepared_head,
            }
        }
    };
    Ok(PreparedStoreOperationCommit {
        commit,
        prepared,
        reference: commit_ref,
        publication,
        registration_activation,
    })
}

pub(crate) async fn publish_prepared_store_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    prepared: Box<PreparedStoreOperationCommit>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let root = required_store_root(db).await?;
    let device_operations = super::store_pull::load_local_commit_device_operations(
        db,
        storage,
        &root,
        &prepared.commit,
    )
    .await
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let retained_operation_objects = retained_store_operation_objects(&prepared.commit)?;
    let publication = prepared.publication.clone();
    let activation = PreparedStoreOperationActivation {
        candidate: prepared,
        device_operations,
        retained_operation_objects,
    };
    match publication {
        StoreOperationPublication::Serial {
            base_head,
            head,
            authorization_after,
        } => {
            let attempt = Box::pin(publish_prepared_serial_store_operation(
                db,
                storage,
                coordination,
                activation,
                base_head,
                head,
                authorization_after,
            ))
            .await?;
            match attempt {
                SerialStoreOperationAttempt::Activated(reference) => {
                    Ok(StoreOperationPublicationOutcome::Activated(reference))
                }
                SerialStoreOperationAttempt::Conflict {
                    activation,
                    commit,
                    reference,
                    authorization_after,
                } => {
                    Box::pin(resolve_serial_store_operation_conflict(
                        db,
                        storage,
                        coordination,
                        activation,
                        commit,
                        reference,
                        authorization_after,
                    ))
                    .await
                }
            }
        }
        StoreOperationPublication::MergeConcurrent {
            head,
            prepared: prepared_head,
        } => {
            Box::pin(publish_prepared_merge_store_operation(
                db,
                storage,
                activation,
                head,
                prepared_head,
            ))
            .await
        }
    }
}

struct PreparedStoreOperationActivation {
    candidate: Box<PreparedStoreOperationCommit>,
    device_operations: super::store_commit::VerifiedStoreDeviceOperations,
    retained_operation_objects: Vec<ExactObjectRef>,
}

enum SerialStoreOperationAttempt {
    Activated(StoreBatchCommitRef),
    Conflict {
        activation: PreparedStoreOperationActivation,
        commit: Box<StoreBatchCommit>,
        reference: StoreBatchCommitRef,
        authorization_after: Box<SerialAuthorizationState>,
    },
}

async fn publish_prepared_serial_store_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    activation: PreparedStoreOperationActivation,
    base_head: VersionedObject,
    head: StoreSerialHead,
    authorization_after: SerialAuthorizationState,
) -> Result<SerialStoreOperationAttempt, StoreOutboundError> {
    let coordination = coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?;
    let commit = activation.candidate.commit.clone();
    let prepared = activation.candidate.prepared.clone();
    let reference = activation.candidate.reference.clone();
    let head_activation = activate_serial_commit_head(
        db,
        storage,
        coordination,
        &base_head,
        &commit,
        &prepared,
        &reference,
        &head,
    )
    .await;
    if let Err(error) = head_activation {
        if !matches!(&error, StoreOutboundError::SerialControlConflict { .. }) {
            return Err(error);
        }
        return Ok(SerialStoreOperationAttempt::Conflict {
            activation,
            commit: Box::new(commit),
            reference,
            authorization_after: Box::new(authorization_after),
        });
    }
    Box::pin(record_activated_serial_store_operation(
        db,
        activation,
        Box::new(commit),
        reference.clone(),
        Box::new(authorization_after),
    ))
    .await?;
    Ok(SerialStoreOperationAttempt::Activated(reference))
}

async fn record_activated_serial_store_operation(
    db: &Database,
    activation: PreparedStoreOperationActivation,
    commit: Box<StoreBatchCommit>,
    reference: StoreBatchCommitRef,
    authorization_after: Box<SerialAuthorizationState>,
) -> Result<(), StoreOutboundError> {
    let operation_object_ids = (!activation.retained_operation_objects.is_empty()).then(|| {
        std::iter::once(super::remote_object::remote_object_id(&reference.object))
            .chain(
                activation
                    .retained_operation_objects
                    .iter()
                    .map(super::remote_object::remote_object_id),
            )
            .collect::<Vec<_>>()
    });
    if operation_object_ids.is_some() {
        db.mark_candidate_commit_uploaded(reference.clone()).await?;
    }
    let recorded_ref = reference.clone();
    let registration_activation = activation.candidate.registration_activation.clone();
    let device_operations = activation.device_operations;
    db.call(move |connection| {
        let tx = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        if let Some(object_ids) = operation_object_ids {
            Database::activate_store_operation_remote_objects_on(&tx, &recorded_ref, &object_ids)?;
        }
        if let Some(activation) = registration_activation {
            Database::record_activated_store_device_registrations_on(
                &tx,
                &commit,
                &[(activation.registration, activation.authority)],
            )?;
        }
        Database::record_materialized_serial_commit_with_device_operations_on(
            &tx,
            &commit,
            &recorded_ref,
            &authorization_after,
            &device_operations,
        )?;
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await?;
    Ok(())
}

async fn resolve_serial_store_operation_conflict(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    mut activation: PreparedStoreOperationActivation,
    commit: Box<StoreBatchCommit>,
    reference: StoreBatchCommitRef,
    authorization_after: Box<SerialAuthorizationState>,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let coordination = coordination.ok_or(StoreOutboundError::MissingSerialCoordination)?;
    let StoreCommitOrder::Serial { predecessor, .. } = &commit.order else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial acknowledgement activation carries Merge order".to_string(),
        ));
    };
    let root = required_store_root(db).await?;
    match super::store_pull::observe_serial_successors_after(
        storage,
        coordination,
        &root,
        predecessor,
    )
    .await?
    {
        super::store_pull::SerialSuccessorObservation::Unchanged(observed) => {
            if let Some(acknowledgement) = commit.acknowledgement().cloned() {
                db.adopt_outbound_store_ack_serial_base_head(acknowledgement, observed)
                    .await?;
                return Ok(StoreOperationPublicationOutcome::Reprepared);
            }
            activation.candidate.adopt_serial_base_head(observed)?;
            Ok(StoreOperationPublicationOutcome::RepreparedCandidate(
                activation.candidate,
            ))
        }
        super::store_pull::SerialSuccessorObservation::Advanced(suffix) => {
            if suffix.commits.first() == Some(&reference) {
                Box::pin(record_activated_serial_store_operation(
                    db,
                    activation,
                    commit,
                    reference.clone(),
                    authorization_after,
                ))
                .await?;
                return Ok(StoreOperationPublicationOutcome::Activated(reference));
            }
            let proof =
                super::remote_object::CandidateNonactivationProof::SerialImmediateSuccessor {
                    accepted_suffix: suffix,
                    losing_prefix: vec![StoreBatchCommitDeletionTarget {
                        coord: reference.coord.clone(),
                        object: reference.object.clone(),
                        canonical_signed_bytes: commit.to_bytes(),
                    }],
                };
            let Some(acknowledgement) = commit.acknowledgement().cloned() else {
                return Ok(StoreOperationPublicationOutcome::NonactivatedCandidate {
                    candidate: activation.candidate,
                    proof: Box::new(proof),
                });
            };
            db.begin_outbound_store_ack_nonactivation(acknowledgement.clone(), proof)
                .await?;
            finish_nonactivating_store_ack(db, storage, acknowledgement).await?;
            Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
        }
    }
}

async fn publish_prepared_merge_store_operation(
    db: &Database,
    storage: &dyn SyncStorage,
    activation: PreparedStoreOperationActivation,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let commit = activation.candidate.commit.clone();
    let prepared = activation.candidate.prepared.clone();
    let reference = activation.candidate.reference.clone();
    let commit_context = ProtocolObjectContext::signed_plaintext(
        commit.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let StoreCommitCoord::MergeConcurrent { stream_id, .. } = &reference.coord else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Merge operation candidate carries a Serial coordinate".to_string(),
        ));
    };
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id.to_string(),
        commit.seq(),
        commit.commit_hash(),
    );
    storage
        .create_protocol_object(&prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let opened = storage
        .read_protocol_object(&commit_context, &reference.object, &commit_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != commit.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "Store operation commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    if !activation.retained_operation_objects.is_empty() {
        db.mark_candidate_commit_uploaded(reference.clone()).await?;
    }
    let head_context = ProtocolObjectContext::signed_plaintext(
        commit.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = head_slot_prefix(
        &commit.author_registration.device_id.to_string(),
        commit.seq(),
    );
    match storage.create_protocol_object(&prepared_head).await {
        Ok(()) => {}
        Err(StorageError::SlotCollision(_)) => {
            return Box::pin(resolve_merge_store_operation_head_collision(
                db,
                storage,
                activation,
                commit,
                reference,
                head,
                prepared_head,
                head_prefix,
            ))
            .await;
        }
        Err(error) => return Err(StoreObjectError::from(error).into()),
    }
    let opened_head = storage
        .read_protocol_object(&head_context, prepared_head.reference(), &head_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened_head != head.to_bytes() {
        return Err(StoreOutboundError::InvalidOutbound(
            "Store operation head exact readback differs from its signed bytes".to_string(),
        ));
    }
    let operation_object_ids = if !activation.retained_operation_objects.is_empty() {
        let head_ref = StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: prepared_head.reference().clone(),
        };
        db.mark_store_head_uploaded(head_ref).await?;
        Some(
            std::iter::once(super::remote_object::remote_object_id(&reference.object))
                .chain(
                    activation
                        .retained_operation_objects
                        .iter()
                        .map(super::remote_object::remote_object_id),
                )
                .chain(std::iter::once(super::remote_object::remote_object_id(
                    prepared_head.reference(),
                )))
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let recorded_ref = reference.clone();
    let registration_activation = activation.candidate.registration_activation;
    let device_operations = activation.device_operations;
    db.call(move |connection| {
        let tx = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        if let Some(object_ids) = operation_object_ids {
            Database::activate_store_operation_remote_objects_on(&tx, &recorded_ref, &object_ids)?;
        }
        if let Some(activation) = registration_activation {
            Database::record_activated_store_device_registrations_on(
                &tx,
                &commit,
                &[(activation.registration, activation.authority)],
            )?;
        }
        Database::record_materialized_commit_with_device_operations_on(
            &tx,
            &commit,
            &recorded_ref,
            &device_operations,
        )?;
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await?;
    Ok(StoreOperationPublicationOutcome::Activated(reference))
}

async fn resolve_merge_store_operation_head_collision(
    db: &Database,
    storage: &dyn SyncStorage,
    mut activation: PreparedStoreOperationActivation,
    commit: StoreBatchCommit,
    reference: StoreBatchCommitRef,
    head: StoreDeviceHead,
    prepared_head: PreparedExactObject,
    head_prefix: String,
) -> Result<StoreOperationPublicationOutcome, StoreOutboundError> {
    let (winner, winner_prepared) = read_occupied_merge_head(
        db,
        storage,
        commit.store_root_hash,
        &head,
        prepared_head.reference().slot(),
        &head_prefix,
    )
    .await?;
    if winner.commit == reference {
        if let Some(acknowledgement) = commit.acknowledgement().cloned() {
            db.adopt_outbound_store_ack_merge_head(acknowledgement, winner, winner_prepared)
                .await?;
            return Ok(StoreOperationPublicationOutcome::Reprepared);
        }
        activation
            .candidate
            .adopt_merge_head(winner, winner_prepared)?;
        return Ok(StoreOperationPublicationOutcome::RepreparedCandidate(
            activation.candidate,
        ));
    }
    let winner_ref = StoreDeviceHeadRef {
        head_hash: winner.head_hash(),
        object: winner_prepared.reference().clone(),
    };
    let proof = super::remote_object::CandidateNonactivationProof::MergeWinner {
        winner_head: winner_ref,
    };
    let Some(acknowledgement) = commit.acknowledgement().cloned() else {
        return Ok(StoreOperationPublicationOutcome::NonactivatedCandidate {
            candidate: activation.candidate,
            proof: Box::new(proof),
        });
    };
    db.begin_outbound_store_ack_nonactivation(acknowledgement.clone(), proof)
        .await?;
    finish_nonactivating_store_ack(db, storage, acknowledgement).await?;
    Ok(StoreOperationPublicationOutcome::Nonactivated(reference))
}

fn retained_store_operation_objects(
    commit: &StoreBatchCommit,
) -> Result<Vec<ExactObjectRef>, StoreOutboundError> {
    let objects = commit
        .acknowledgement()
        .map(|reference| reference.object.clone())
        .into_iter()
        .chain(
            commit
                .device_exclusion_proposals()
                .iter()
                .map(|reference| reference.object.clone()),
        )
        .chain(
            commit
                .device_exclusion_outcomes()
                .iter()
                .map(|reference| reference.object().clone()),
        )
        .chain(
            commit
                .reclaim_authorization()
                .into_iter()
                .flat_map(|reference| {
                    [reference.evidence.object.clone(), reference.object.clone()]
                }),
        )
        .chain(
            commit
                .reclaim_receipt()
                .map(|reference| reference.object.clone()),
        )
        .collect::<Vec<_>>();
    if objects
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != objects.len()
    {
        return Err(StoreOutboundError::InvalidOutbound(
            "Store operation publication repeats a retained authority object".to_string(),
        ));
    }
    Ok(objects)
}

pub(crate) async fn finish_nonactivating_store_ack(
    db: &Database,
    storage: &dyn SyncStorage,
    acknowledgement: super::store_commit::StoreAckRef,
) -> Result<(), StoreOutboundError> {
    let targets = db
        .nonactivating_outbound_store_ack_cleanup_targets(acknowledgement.clone())
        .await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    db.complete_nonactivating_outbound_store_ack(acknowledgement)
        .await?;
    Ok(())
}

pub(crate) async fn activate_store_operation_commit(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    plan: StoreOperationCommitPlan,
    batch: StoreOperationBatch,
) -> Result<StoreBatchCommitRef, StoreOutboundError> {
    let prepared = prepare_store_operation_candidate(db, storage, plan, batch).await?;
    match publish_prepared_store_operation(db, storage, coordination, Box::new(prepared)).await? {
        StoreOperationPublicationOutcome::Activated(reference) => Ok(reference),
        StoreOperationPublicationOutcome::Nonactivated(reference) => {
            Err(StoreOutboundError::InvalidOutbound(format!(
                "Store operation candidate {} did not activate",
                reference.commit_hash
            )))
        }
        StoreOperationPublicationOutcome::Reprepared => Err(StoreOutboundError::InvalidOutbound(
            "Store operation was reprepared during immediate activation".to_string(),
        )),
        StoreOperationPublicationOutcome::RepreparedCandidate(_)
        | StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
            Err(StoreOutboundError::InvalidOutbound(
                "unpersisted Store operation encountered an activation conflict".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::storage::VersionedObject;
    use crate::sync::test_helpers::{
        create_exact_test_store, host_exec, open_serial_test_db, open_test_db, temp_store_dir,
        test_migrations, test_synced_tables,
    };

    fn exact_partition_blob(
        physical_id: &str,
        uploaded_verified: bool,
        spool_path: Option<&str>,
    ) -> PreparedPartitionBlob {
        let uploader_bytes = b"outbound exact-ref test uploader";
        let uploader = StoreDeviceRegistrationRef {
            device_id: "ab"
                .repeat(32)
                .parse()
                .expect("valid exact-ref test device id"),
            registration_hash: ObjectHash::digest(uploader_bytes),
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/test/exact-ref-uploader.json".to_string(),
                )
                .expect("valid exact-ref uploader slot"),
                uploader_bytes.len() as u64,
                ObjectHash::digest(uploader_bytes),
            ),
        };
        let locator = crate::blob::locator::BlobLocator::browsable(
            "images",
            "shared-blob",
            uploader,
            "photos/shared.bin",
            12,
            ObjectHash::digest(b"shared bytes"),
        )
        .expect("valid exact-ref test locator");
        let stored_bytes = b"stored exact-ref bytes";
        let object = crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::opaque(
                locator.semantic_key(),
                physical_id.to_string(),
            )
            .expect("valid exact-ref physical slot"),
            stored_bytes.len() as u64,
            ObjectHash::digest(stored_bytes),
        );
        PreparedPartitionBlob {
            audience: crate::blob::locator::RemoteAudience::Store,
            stored: crate::blob::locator::StoredBlobRef::new(locator, object)
                .expect("valid exact stored blob"),
            spool_path: spool_path.map(std::path::PathBuf::from),
            uploaded_verified,
        }
    }

    fn exact_blob_owner() -> StoreBatchCommitRef {
        StoreBatchCommitRef {
            coord: StoreCommitCoord::Serial { sequence: 1 },
            commit_hash: ObjectHash::digest(b"exact-ref owner"),
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/test/exact-ref-owner.json".to_string(),
                )
                .expect("valid exact-ref owner slot"),
                1,
                ObjectHash::digest(b"x"),
            ),
        }
    }

    #[test]
    fn blob_closure_deduplicates_only_identical_exact_refs_and_merges_state() {
        let owner = exact_blob_owner();
        let close = |reversed: bool| {
            let prepared = exact_partition_blob("physical-a", false, Some("/spool/shared"));
            let uploaded = exact_partition_blob("physical-a", true, None);
            let distinct = exact_partition_blob("physical-b", true, None);
            let blobs = if reversed {
                vec![distinct, uploaded, prepared]
            } else {
                vec![prepared, uploaded, distinct]
            };
            close_prepared_blobs(blobs, &owner).expect("close exact prepared blobs")
        };

        let forward = close(false);
        let reversed = close(true);
        assert_eq!(forward, reversed);
        assert_eq!(forward.0.len(), 2);
        assert_eq!(forward.1.len(), 2);
        let first_object = exact_partition_blob("physical-a", true, None)
            .stored
            .object()
            .clone();
        let first_id = super::super::remote_object::remote_object_id(&first_object);
        let first_remote = forward
            .0
            .iter()
            .find(|remote| remote.object_id() == first_id)
            .expect("identical exact ref remains indexed");
        assert!(matches!(
            first_remote,
            super::super::remote_object::RemoteObjectRecord::SharedLiveSet(record)
                if matches!(
                    record.state,
                    super::super::remote_object::OwnedObjectState::UploadedVerified { .. }
                )
        ));
        let first_index = forward
            .1
            .iter()
            .find(|blob| blob.remote_object_id() == first_id)
            .expect("identical exact ref retains its index");
        assert_eq!(
            first_index.spool_path(),
            Some(std::path::Path::new("/spool/shared"))
        );
        let conflict = close_prepared_blobs(
            vec![
                exact_partition_blob("physical-a", false, Some("/spool/first")),
                exact_partition_blob("physical-a", false, Some("/spool/second")),
            ],
            &owner,
        )
        .expect_err("one exact prepared object cannot own two spools");
        assert!(conflict.to_string().contains("conflicting spool paths"));
    }

    struct FailFirstCoordinationRead<'a> {
        inner: &'a dyn CoordinationStorage,
        failed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CoordinationStorage for FailFirstCoordinationRead<'_> {
        async fn provider_binding(
            &self,
        ) -> Result<crate::sync::storage::ResolvedProviderBinding, CoordinationError> {
            self.inner.provider_binding().await
        }

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

        async fn delete_head(&self, key: &str) -> Result<(), CoordinationError> {
            self.inner.delete_head(key).await
        }
    }

    async fn initialize_exact_store(
        db: &Database,
        storage: &CloudSyncStorage,
        store_id: &str,
        keypair: &UserKeypair,
    ) -> (StoreRootRef, String) {
        let root = create_exact_test_store(db, storage, store_id, keypair)
            .await
            .expect("create exact test Store");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local device id")
            .expect("exact Store has an activated local device");
        (root, device_id)
    }

    async fn local_device_id(db: &Database) -> String {
        db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read local device id")
            .expect("local device id exists")
    }

    async fn remove_exact_store_root(db: &Database) {
        db.call(|connection| {
            connection
                .execute("DELETE FROM store_protocol_root_authority", [])
                .map(|_| ())
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("remove exact Store root authority");
    }

    async fn reinstall_exact_store_root(
        db: &Database,
        storage: &dyn SyncStorage,
        root: &StoreRootRef,
    ) {
        let verified = super::super::store_objects::load_store_protocol_root(storage, root)
            .await
            .expect("load exact Store root authority");
        db.install_store_root_authority(root.clone(), verified.bytes)
            .await
            .expect("reinstall exact Store root authority");
    }

    async fn parse_serial_head(db: &Database, root: ObjectHash, bytes: &[u8]) -> StoreSerialHead {
        let unverified: StoreSerialHead = serde_json::from_slice(bytes).expect("parse Serial head");
        let registration_ref = match &unverified.state {
            StoreSerialHeadState::Genesis {
                founder_registration,
                ..
            } => founder_registration,
            StoreSerialHeadState::Commit {
                author_registration,
                ..
            } => author_registration,
        };
        let registration = db
            .activated_store_device_registration(registration_ref.clone())
            .await
            .expect("load Serial head author");
        StoreSerialHead::parse(bytes, root, &registration).expect("verify Serial head")
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
        .expect("in-memory home supports immutable copies")
        .with_test_serial_coordination(Arc::new(home.clone()));
        let db = open_serial_test_db();
        let (root, device_id) =
            initialize_exact_store(&db, &storage, "serial-outbound", &keypair).await;
        let store_root_hash = root.store_root_hash;
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
        let head_mutations_before = home.head_mutation_count();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &device_id,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
        assert_eq!(home.head_mutation_count(), head_mutations_before + 1);
        let first = db
            .exact_materialized_ref(SERIAL_STREAM_ID, 1)
            .await
            .unwrap()
            .expect("first Serial commit");
        let second = db
            .exact_materialized_ref(SERIAL_STREAM_ID, 2)
            .await
            .unwrap()
            .expect("second Serial commit");
        assert!(matches!(
            db.write_status(&pending[0].write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition::Serial { commit }
                        if commit.coord.sequence() == 1
                            && commit.commit_hash == first.commit_hash
                )
        ));
        assert!(matches!(
            db.write_status(&pending[1].write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition::Serial { commit }
                        if commit.coord.sequence() == 2
                            && commit.commit_hash == second.commit_hash
                )
        ));
        let head = storage
            .serial_coordination()
            .unwrap()
            .read_head(serial_head_key())
            .await
            .expect("read activated Serial head");
        let head = parse_serial_head(&db, store_root_hash, &head.bytes).await;
        assert!(matches!(
            head.state,
            StoreSerialHeadState::Commit { commit, .. }
                if commit == second && commit.commit_hash == second.commit_hash
        ));
    }

    async fn serial_fixture(
        name: &str,
    ) -> (
        InMemoryCloudHome,
        CloudSyncStorage,
        Database,
        UserKeypair,
        StoreRootRef,
        Vec<crate::PendingWrite>,
    ) {
        let name = name.to_string();
        tokio::spawn(async move {
            let home = InMemoryCloudHome::new();
            let keypair = UserKeypair::generate();
            let storage = CloudSyncStorage::new(
                Arc::new(home.clone()),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                &name,
                keypair.clone(),
            )
            .expect("in-memory home supports immutable copies")
            .with_test_serial_coordination(Arc::new(home.clone()));
            let db = open_serial_test_db();
            let (root, _) = initialize_exact_store(&db, &storage, &name, &keypair).await;
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
        })
        .await
        .expect("Serial Store write fixture task")
    }

    async fn competing_head(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
        marker: &str,
    ) -> StoreSerialHead {
        let authorization = current_serial_authorization(
            db,
            storage,
            storage.serial_coordination().expect("Serial coordination"),
        )
        .await
        .expect("load competing Serial authorization");
        let member = UserKeypair::generate();
        let entry = authorization
            .membership
            .signed_set_member(
                signer,
                crate::keys::public_key_hex(&member),
                None,
                crate::sync::membership::MemberRole::Member,
                marker.to_string(),
            )
            .expect("sign competing membership control");
        let prepared = prepare_serial_control(
            db,
            storage,
            storage.serial_coordination().expect("Serial coordination"),
            &local_device_id(db).await,
            StoreControl::SerialMembership { entry },
            signer,
        )
        .await
        .expect("prepare exact competing Serial control");
        storage
            .create_protocol_object(&prepared.commit_prepared)
            .await
            .expect("publish exact competing Serial commit");
        prepared.head
    }

    fn serial_commit_ref(head: &StoreSerialHead) -> Option<&StoreBatchCommitRef> {
        match &head.state {
            StoreSerialHeadState::Genesis { .. } => None,
            StoreSerialHeadState::Commit { commit, .. } => Some(commit),
        }
    }

    #[tokio::test]
    async fn changed_serial_base_marks_the_whole_branch_conflict_before_uploading_candidates() {
        let (home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-changed-base").await;
        let other = competing_head(&db, &storage, &keypair, "changed-base").await;
        let coordination = storage.serial_coordination().expect("Serial coordination");
        let current = coordination
            .read_head(serial_head_key())
            .await
            .expect("read founder Serial head");
        let head_mutations_before = home.head_mutation_count();
        coordination
            .replace_head(serial_head_key(), &current.version, &other.to_bytes())
            .await
            .expect("replace founder Serial head");
        home.fail_exact_create_before_call(1);
        let (_temp, store_dir) = temp_store_dir();

        assert!(!prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("detect changed Serial base"));

        assert_eq!(home.head_mutation_count(), head_mutations_before + 1);
        for write in pending {
            let status = db.write_status(&write.write_id).await.unwrap();
            assert!(matches!(
                status,
                crate::WriteStatus::Conflict(ref conflict)
                    if matches!(
                        conflict.as_ref(),
                        crate::SerializationConflict {
                            base: StoreSerialPredecessor::Genesis { .. },
                            current: StoreSerialPredecessor::Commit(current),
                            ..
                        } if Some(current) == serial_commit_ref(&other)
                    )
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .unwrap());
        let head_mutations_before = home.head_mutation_count();
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
        assert_eq!(home.head_mutation_count(), head_mutations_before + 1);
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Published(position)
                    if matches!(position.as_ref(), crate::PublishedPosition::Serial { .. })
            ));
        }
    }

    #[tokio::test]
    async fn serial_candidate_abandonment_persists_and_wins_the_branch_base() {
        let (_home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-candidate-abandonment").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("prepare exact Serial branch"));
        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        assert!(prepare_serial_candidate_abandonment(
            &db,
            &storage,
            &local_device_id(&db).await,
            &keypair,
            branch_id.clone(),
        )
        .await
        .expect("persist exact Serial abandonment"));
        let durable = db
            .prepared_serial_candidate_abandonment()
            .await
            .expect("reload Serial abandonment")
            .expect("Serial abandonment exists");
        assert_eq!(durable.branch_id, branch_id);
        assert_eq!(durable.authority.value.write_id, pending[0].write_id);
        assert!(matches!(
            durable.authority.value.body,
            super::super::store_commit::StoreCommitBody::AbandonCandidates { .. }
        ));

        let outcome = abandon_serial_branch(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            &local_device_id(&db).await,
            &keypair,
            &store_dir,
            branch_id,
        )
        .await
        .expect("activate and apply Serial abandonment");
        assert_eq!(outcome, SerialBranchAbandonment::Discarded);
        assert!(db
            .prepared_serial_candidate_abandonment()
            .await
            .expect("read completed abandonment")
            .is_none());
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
            ));
        }
    }

    #[tokio::test]
    async fn original_serial_branch_activation_wins_abandonment_race() {
        let (_home, storage, db, keypair, root, pending) =
            serial_fixture("serial-abandonment-original-wins").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("prepare exact Serial branch"));
        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        assert!(prepare_serial_candidate_abandonment(
            &db,
            &storage,
            &local_device_id(&db).await,
            &keypair,
            branch_id.clone(),
        )
        .await
        .expect("persist Serial abandonment"));
        let branch = db
            .prepared_serial_store_branch()
            .await
            .expect("load original Serial branch")
            .expect("original Serial branch exists");
        for write in &branch.writes {
            publish_prepared_remote_objects(
                &db,
                &storage,
                &write.commit.value.write_id,
                root.store_root_hash,
            )
            .await
            .expect("publish original candidate objects");
            storage
                .create_protocol_object(&write.commit.prepared)
                .await
                .expect("publish original candidate commit");
            let reference = StoreBatchCommitRef::from_commit(
                &write.commit.value,
                StoreCommitCoord::Serial {
                    sequence: write.commit.value.seq(),
                },
                write.commit.object.clone(),
            )
            .expect("reference original candidate");
            db.mark_candidate_commit_uploaded(reference)
                .await
                .expect("record original candidate upload");
        }
        let coordination = storage.serial_coordination().unwrap();
        let current = coordination
            .read_head(serial_head_key())
            .await
            .expect("read Serial base head");
        coordination
            .replace_head(serial_head_key(), &current.version, &branch.head.bytes)
            .await
            .expect("activate original Serial branch");

        assert_eq!(
            abandon_serial_branch(
                &db,
                &storage,
                coordination,
                &local_device_id(&db).await,
                &keypair,
                &store_dir,
                branch_id,
            )
            .await
            .expect("settle original Serial winner"),
            SerialBranchAbandonment::OriginalBranchActivated,
        );
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Published(_)
            ));
        }
        assert!(db
            .prepared_serial_candidate_abandonment()
            .await
            .expect("read completed abandonment")
            .is_none());
    }

    #[tokio::test]
    async fn third_serial_successor_discards_both_losing_candidate_families() {
        let (_home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-abandonment-third-wins").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("prepare exact Serial branch"));
        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        assert!(prepare_serial_candidate_abandonment(
            &db,
            &storage,
            &local_device_id(&db).await,
            &keypair,
            branch_id.clone(),
        )
        .await
        .expect("persist Serial abandonment"));
        competing_head(&db, &storage, &keypair, "third-winner").await;

        assert_eq!(
            abandon_serial_branch(
                &db,
                &storage,
                storage.serial_coordination().unwrap(),
                &local_device_id(&db).await,
                &keypair,
                &store_dir,
                branch_id,
            )
            .await
            .expect("settle third Serial winner"),
            SerialBranchAbandonment::Discarded,
        );
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
            ));
        }
        assert!(db
            .prepared_serial_candidate_abandonment()
            .await
            .expect("read completed abandonment")
            .is_none());
    }

    #[tokio::test]
    async fn serial_abandonment_retries_commit_publication_and_candidate_cleanup() {
        for failure in ["commit", "cleanup"] {
            let (home, storage, db, keypair, root, pending) =
                serial_fixture(&format!("serial-abandonment-retry-{failure}")).await;
            let (_temp, store_dir) = temp_store_dir();
            assert!(prepare_pending_store_write_with_coordination(
                &db,
                &storage,
                Some(storage.serial_coordination().unwrap()),
                &local_device_id(&db).await,
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                None,
            )
            .await
            .expect("prepare exact Serial branch"));
            let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
            if failure == "cleanup" {
                let branch = db
                    .prepared_serial_store_branch()
                    .await
                    .expect("load Serial branch")
                    .expect("Serial branch exists");
                let first = branch.writes.first().expect("branch has a first candidate");
                publish_prepared_remote_objects(
                    &db,
                    &storage,
                    &first.commit.value.write_id,
                    root.store_root_hash,
                )
                .await
                .expect("publish first candidate objects");
                storage
                    .create_protocol_object(&first.commit.prepared)
                    .await
                    .expect("publish first candidate commit");
                let reference = StoreBatchCommitRef::from_commit(
                    &first.commit.value,
                    StoreCommitCoord::Serial {
                        sequence: first.commit.value.seq(),
                    },
                    first.commit.object.clone(),
                )
                .expect("reference first candidate");
                db.mark_candidate_commit_uploaded(reference)
                    .await
                    .expect("record first candidate upload");
                home.fail_exact_delete_on_call(1);
            } else {
                home.fail_exact_create_before_call(1);
            }
            assert!(abandon_serial_branch(
                &db,
                &storage,
                storage.serial_coordination().unwrap(),
                &local_device_id(&db).await,
                &keypair,
                &store_dir,
                branch_id.clone(),
            )
            .await
            .is_err());
            assert_eq!(
                db.serial_branch_discard_state(&branch_id)
                    .await
                    .expect("read resumable Serial discard"),
                crate::database::SerialBranchDiscardState::Abandonment,
            );
            assert_eq!(
                abandon_serial_branch(
                    &db,
                    &storage,
                    storage.serial_coordination().unwrap(),
                    &local_device_id(&db).await,
                    &keypair,
                    &store_dir,
                    branch_id,
                )
                .await
                .expect("resume Serial abandonment"),
                SerialBranchAbandonment::Discarded,
            );
        }
    }

    #[tokio::test]
    async fn serial_abandonment_resumes_candidate_cleanup_after_database_reopen() {
        let database_temp = tempfile::tempdir().expect("temporary database directory");
        let database_path = database_temp.path().join("serial-abandonment.db");
        let tables = test_synced_tables();
        let migrations = test_migrations();
        let (db, _) = Database::open(
            &database_path,
            tables.clone(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "test-device".to_string(),
            &migrations,
        )
        .expect("open file-backed Serial database");
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "serial-abandonment-reopen",
            keypair.clone(),
        )
        .expect("create Serial storage")
        .with_test_serial_coordination(Arc::new(home.clone()));
        let (root, _) =
            initialize_exact_store(&db, &storage, "serial-abandonment-reopen", &keypair).await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('serial-reopen', 'first', NULL, 1, '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        let (_store_temp, store_dir) = temp_store_dir();
        let pending = db
            .pending_writes()
            .await
            .expect("read pending Serial write");
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().expect("Serial coordination")),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("prepare exact Serial branch"));
        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        let branch = db
            .prepared_serial_store_branch()
            .await
            .expect("load prepared Serial branch")
            .expect("prepared Serial branch exists");
        let first = branch.writes.first().expect("branch has a first candidate");
        publish_prepared_remote_objects(
            &db,
            &storage,
            &first.commit.value.write_id,
            root.store_root_hash,
        )
        .await
        .expect("publish first candidate objects");
        storage
            .create_protocol_object(&first.commit.prepared)
            .await
            .expect("publish first candidate commit");
        let reference = StoreBatchCommitRef::from_commit(
            &first.commit.value,
            StoreCommitCoord::Serial {
                sequence: first.commit.value.seq(),
            },
            first.commit.object.clone(),
        )
        .expect("reference first candidate");
        db.mark_candidate_commit_uploaded(reference)
            .await
            .expect("record first candidate upload");
        home.fail_exact_delete_on_call(1);
        assert!(abandon_serial_branch(
            &db,
            &storage,
            storage.serial_coordination().expect("Serial coordination"),
            &local_device_id(&db).await,
            &keypair,
            &store_dir,
            branch_id.clone(),
        )
        .await
        .is_err());
        drop(db);

        let (reopened, _) = Database::open(
            &database_path,
            tables,
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            crate::WritePolicy::Serial,
            "test-device".to_string(),
            &migrations,
        )
        .expect("reopen Serial database");
        assert_eq!(
            abandon_serial_branch(
                &reopened,
                &storage,
                storage.serial_coordination().expect("Serial coordination"),
                &local_device_id(&reopened).await,
                &keypair,
                &store_dir,
                branch_id,
            )
            .await
            .expect("resume Serial abandonment after reopen"),
            SerialBranchAbandonment::Discarded,
        );
        assert!(reopened
            .prepared_serial_candidate_abandonment()
            .await
            .expect("read completed abandonment")
            .is_none());
    }

    #[tokio::test]
    async fn serial_abandonment_settles_a_lost_success_response_by_head_readback() {
        let (home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-abandonment-lost-response").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("prepare exact Serial branch"));
        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        home.fail_next_head_mutation_after_visibility();

        assert_eq!(
            abandon_serial_branch(
                &db,
                &storage,
                storage.serial_coordination().unwrap(),
                &local_device_id(&db).await,
                &keypair,
                &store_dir,
                branch_id,
            )
            .await
            .expect("settle lost Serial abandonment response"),
            SerialBranchAbandonment::Discarded,
        );
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .unwrap());
        let losing_commit_keys = db
            .prepared_serial_store_branch()
            .await
            .unwrap()
            .expect("prepared losing branch")
            .writes
            .into_iter()
            .map(|write| write.commit.object.slot().logical_key().to_string())
            .collect::<Vec<_>>();
        let other = competing_head(&db, &storage, &keypair, "other-winner").await;
        let head_mutations_before = home.head_mutation_count();
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
        assert_eq!(home.head_mutation_count(), head_mutations_before + 2);
        for write in &pending {
            let status = db.write_status(&write.write_id).await.unwrap();
            assert!(matches!(
                status,
                crate::WriteStatus::Conflict(ref conflict)
                    if matches!(
                        conflict.as_ref(),
                        crate::SerializationConflict {
                            current: StoreSerialPredecessor::Commit(current),
                            ..
                        } if Some(current) == serial_commit_ref(&other)
                    )
            ));
        }
        let retained_prepared: i64 = db
            .call(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM store_writes WHERE prepared IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(crate::DbError::from)
            })
            .await
            .unwrap();
        assert_eq!(retained_prepared, 2);

        let branch_id = crate::PendingBranchId::from_first_write(pending[0].write_id.clone());
        let resolution = super::super::store_pull::prepare_serial_resolution(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            root.store_root_hash,
            &store_dir,
            None,
            &keypair,
        )
        .await
        .expect("prepare accepted successor resolution");
        home.fail_exact_delete_on_call(2);
        let interrupted = super::super::store_pull::cleanup_serial_candidates(
            &db,
            &storage,
            branch_id.clone(),
            &resolution,
        )
        .await;
        assert!(interrupted.is_err());
        db.discard_pending_serial_branch(branch_id.clone(), resolution)
            .await
            .expect_err("incomplete candidate cleanup must block resolution");

        let coordination = storage.serial_coordination().unwrap();
        let current = coordination
            .read_head(serial_head_key())
            .await
            .expect("read accepted Serial head");
        coordination
            .replace_head(serial_head_key(), &current.version, &current.bytes)
            .await
            .expect("refresh accepted Serial head version");

        let resolution = super::super::store_pull::prepare_serial_resolution(
            &db,
            &storage,
            storage.serial_coordination().unwrap(),
            root.store_root_hash,
            &store_dir,
            None,
            &keypair,
        )
        .await
        .expect("prepare retry resolution");
        super::super::store_pull::cleanup_serial_candidates(
            &db,
            &storage,
            branch_id.clone(),
            &resolution,
        )
        .await
        .expect("resume losing candidate cleanup");
        db.discard_pending_serial_branch(branch_id, resolution)
            .await
            .expect("discard cleaned branch");
        let deletes = home.deletes_seen();
        assert_eq!(
            &deletes[deletes.len() - losing_commit_keys.len()..],
            losing_commit_keys
        );
        for write in pending {
            assert!(matches!(
                db.write_status(&write.write_id).await.unwrap(),
                crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
        remove_exact_store_root(&db).await;
        let (_temp, store_dir) = temp_store_dir();

        let result = prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
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
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition::Serial { commit }
                        if commit.coord.sequence() == 3
                )
        ));
    }

    #[tokio::test]
    async fn write_arriving_during_serial_publication_conflicts_with_the_branch_on_cas_loss() {
        let (home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-publishing-lost-suffix").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
        let other = competing_head(&db, &storage, &keypair, "suffix-lost").await;
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
            let status = db.write_status(&write.write_id).await.unwrap();
            assert!(matches!(
                status,
                crate::WriteStatus::Conflict(ref conflict)
                    if conflict.branch_id == expected_branch
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
            Err(StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD
            })
        ));
        assert!(matches!(
            current_serial_head_ref(&db, storage.serial_coordination().unwrap()).await,
            Err(StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD
            })
        ));
    }

    #[tokio::test]
    async fn missing_serial_genesis_head_fails_before_the_first_commit() {
        let (home, storage, db, _keypair, _root, _pending) =
            serial_fixture("serial-missing-genesis-head").await;
        home.remove(serial_head_key());

        assert!(matches!(
            current_serial_authorization(&db, &storage, storage.serial_coordination().unwrap())
                .await,
            Err(StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD
            })
        ));
        assert!(matches!(
            current_serial_head_ref(&db, storage.serial_coordination().unwrap()).await,
            Err(StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD
            })
        ));
    }

    #[tokio::test]
    async fn missing_serial_head_during_activation_names_the_coordination_head() {
        let (home, storage, db, keypair, _root, pending) =
            serial_fixture("serial-missing-head-during-activation").await;
        let (_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .expect("prepare exact Serial write"));
        home.remove(serial_head_key());

        let error = drain_store_writes_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
        )
        .await
        .expect_err("an absent coordination head blocks activation");
        assert!(matches!(
            error,
            StoreOutboundError::MissingState {
                key: SERIAL_COORDINATION_HEAD
            }
        ));
        for write in pending {
            let status = db.write_status(&write.write_id).await.unwrap();
            assert!(
                matches!(status, crate::WriteStatus::Publishing),
                "unexpected missing-head status: {status:?}"
            );
        }
    }

    struct PreparedWriteFixture {
        home: InMemoryCloudHome,
        storage: CloudSyncStorage,
        db: Database,
        keypair: UserKeypair,
        root: StoreRootRef,
        device_id: String,
        write_id: crate::WriteId,
        commit_ref: StoreBatchCommitRef,
        package_object: crate::sync::storage::ExactObjectRef,
        head_object: crate::sync::storage::ExactObjectRef,
    }

    async fn prepared_write_fixture() -> PreparedWriteFixture {
        tokio::spawn(async {
            let home = InMemoryCloudHome::new();
            let keypair = UserKeypair::generate();
            let storage = CloudSyncStorage::new(
                Arc::new(home.clone()),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "outbound-crash-test",
                keypair.clone(),
            )
            .expect("in-memory home supports immutable copies");
            let db = open_test_db();
            let (root, device_id) =
                initialize_exact_store(&db, &storage, "outbound-crash-test", &keypair).await;
            let membership = super::super::membership_ops::load_and_persist_owner_anchor(
                &storage,
                &root,
                &crate::keys::public_key_hex(&keypair),
                &db,
            )
            .await
            .expect("load exact founder membership");
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
                &device_id,
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                Some(&membership),
            )
            .await
            .expect("prepare outbound write"));
            let batch = db
                .oldest_prepared_store_write()
                .await
                .expect("read prepared write")
                .expect("prepared write exists");
            let commit_ref = batch.head.value.commit.clone();
            let package_object = batch
                .commit
                .value
                .store_package()
                .as_ref()
                .expect("Store package")
                .object
                .clone();
            PreparedWriteFixture {
                home,
                storage,
                db,
                keypair,
                root,
                device_id,
                write_id: batch.commit.value.write_id.clone(),
                commit_ref,
                package_object,
                head_object: batch.head.object.clone(),
            }
        })
        .await
        .expect("prepared Store write fixture task")
    }

    async fn publish_competing_merge_head(fixture: &PreparedWriteFixture) -> StoreDeviceHeadRef {
        let batch = fixture
            .db
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists");
        let candidate = &batch.commit.value;
        let registration = fixture
            .db
            .activated_store_device_registration(candidate.author_registration.clone())
            .await
            .expect("load Merge author registration");
        let signer = registration
            .device_signer(&fixture.keypair)
            .expect("derive Merge device signer");
        let stream_id = match &candidate.order {
            StoreCommitOrder::MergeConcurrent { .. } => match &batch.head.value.commit.coord {
                StoreCommitCoord::MergeConcurrent { stream_id, .. } => *stream_id,
                StoreCommitCoord::Serial { .. } => unreachable!("fixture is MergeConcurrent"),
            },
            StoreCommitOrder::Serial { .. } => unreachable!("fixture is MergeConcurrent"),
        };
        let package = super::super::audience_package::AudiencePackage::store(
            fixture.root.store_root_hash,
            candidate.candidate_family(),
            candidate.write_id.clone(),
            batch.head.value.commit.coord.clone(),
            fixture.db.schema_version(),
            b"competing valid package".to_vec(),
            Vec::new(),
        )
        .expect("construct competing package");
        let package_bytes = package.to_bytes();
        let package_prefix = package_semantic_prefix(
            candidate.candidate_family(),
            &stream_id.to_string(),
            candidate.seq(),
            ObjectHash::digest(&package_bytes),
        );
        let package_context = ProtocolObjectContext::store_encrypted(
            fixture.root.store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        let package_slot = fixture
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("reserve competing package slot");
        let package_prepared = fixture
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare competing package");
        fixture
            .storage
            .create_protocol_object(&package_prepared)
            .await
            .expect("publish competing package");
        let winner = StoreBatchCommit::signed(
            fixture.root.store_root_hash,
            candidate.write_id.clone(),
            batch.head.value.commit.coord.clone(),
            candidate.author_registration.clone(),
            &registration,
            candidate.order.clone(),
            candidate.membership_state.clone(),
            candidate.device_state.clone(),
            candidate.membership_authority.clone(),
            StorePackageInput {
                candidate_family: candidate.candidate_family(),
                schema_version: fixture.db.schema_version(),
                bytes: &package_bytes,
                object: package_prepared.reference().clone(),
            },
            &signer,
        )
        .expect("sign competing commit");
        let commit_prefix = commit_semantic_prefix(
            winner.candidate_family(),
            &stream_id.to_string(),
            winner.seq(),
            winner.commit_hash(),
        );
        let commit_context = ProtocolObjectContext::signed_plaintext(
            fixture.root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let commit_slot = fixture
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("reserve competing commit slot");
        let commit_prepared = fixture
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                winner.to_bytes(),
            )
            .expect("prepare competing commit");
        fixture
            .storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish competing commit");
        let winner_ref = StoreBatchCommitRef::from_commit(
            &winner,
            batch.head.value.commit.coord.clone(),
            commit_prepared.reference().clone(),
        )
        .expect("reference competing commit");
        assert_ne!(winner_ref, batch.head.value.commit);
        let winner_head = StoreDeviceHead::signed(
            fixture.root.store_root_hash,
            candidate.author_registration.clone(),
            winner_ref,
            batch.head.value.successor.clone(),
            &signer,
        )
        .expect("sign competing head");
        let head_context = ProtocolObjectContext::signed_plaintext(
            fixture.root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = head_slot_prefix(&fixture.device_id, candidate.seq());
        let head_prepared = fixture
            .storage
            .prepare_protocol_object(
                &head_context,
                fixture.head_object.slot().clone(),
                &head_prefix,
                winner_head.to_bytes(),
            )
            .expect("prepare competing head");
        fixture
            .storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish competing head");
        StoreDeviceHeadRef {
            head_hash: winner_head.head_hash(),
            object: head_prepared.reference().clone(),
        }
    }

    async fn publish_alternate_head_for_prepared_commit(
        fixture: &PreparedWriteFixture,
    ) -> StoreDeviceHeadRef {
        let batch = fixture
            .db
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists");
        let registration = fixture
            .db
            .activated_store_device_registration(batch.head.value.author_registration.clone())
            .await
            .expect("load Merge author registration");
        let signer = registration
            .device_signer(&fixture.keypair)
            .expect("derive Merge device signer");
        let head_context = ProtocolObjectContext::signed_plaintext(
            fixture.root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let next_prefix = head_slot_prefix(&fixture.device_id, batch.commit.value.seq() + 1);
        let alternate_next = crate::storage::cloud::ObjectSlot::opaque(
            format!("{next_prefix}.json"),
            "alternate-successor".to_string(),
        )
        .expect("reserve alternate successor slot");
        let alternate = StoreDeviceHead::signed(
            fixture.root.store_root_hash,
            batch.head.value.author_registration.clone(),
            batch.head.value.commit.clone(),
            SuccessorLink {
                activation: batch.head.value.successor.activation,
                predecessor: batch.head.value.successor.predecessor.clone(),
                next_slot: alternate_next,
            },
            &signer,
        )
        .expect("sign alternate activating head");
        let head_prefix = head_slot_prefix(&fixture.device_id, batch.commit.value.seq());
        let prepared = fixture
            .storage
            .prepare_protocol_object(
                &head_context,
                fixture.head_object.slot().clone(),
                &head_prefix,
                alternate.to_bytes(),
            )
            .expect("prepare alternate activating head");
        fixture
            .storage
            .create_protocol_object(&prepared)
            .await
            .expect("publish alternate activating head");
        StoreDeviceHeadRef {
            head_hash: alternate.head_hash(),
            object: prepared.reference().clone(),
        }
    }

    fn exact_object_exists(
        home: &InMemoryCloudHome,
        object: &crate::sync::storage::ExactObjectRef,
    ) -> bool {
        home.get(object.slot().logical_key()).is_some()
    }

    fn commit_stream(reference: &StoreBatchCommitRef) -> String {
        match reference.coord {
            StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
            StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
        }
    }

    async fn stored_remote_object(
        db: &Database,
        object: &crate::sync::storage::ExactObjectRef,
    ) -> super::super::remote_object::RemoteObjectRecord {
        let object_id = super::super::remote_object::remote_object_id(object);
        db.call(move |conn| {
            let encoded: String = conn
                .query_row(
                    "SELECT state FROM remote_objects WHERE object_id = ?1",
                    [object_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(crate::DbError::from)?;
            serde_json::from_str(&encoded)
                .map_err(|error| crate::DbError::Message(error.to_string()))
        })
        .await
        .expect("load stored remote object")
    }

    async fn remote_object_exists(
        db: &Database,
        object: &crate::sync::storage::ExactObjectRef,
    ) -> bool {
        let object_id = super::super::remote_object::remote_object_id(object);
        db.call(move |conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)
        })
        .await
        .expect("check stored remote object")
    }

    #[tokio::test]
    async fn accepted_package_transfers_to_shared_live_set_ownership() {
        let fixture = prepared_write_fixture().await;

        assert!(
            remote_object_exists(&fixture.db, &fixture.head_object).await,
            "the prepared Merge head must have durable candidate ownership before publication",
        );

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("publish prepared Store package"),
            1,
        );

        let remote = stored_remote_object(&fixture.db, &fixture.package_object).await;
        assert!(matches!(
            remote,
            super::super::remote_object::RemoteObjectRecord::SharedLiveSet(record)
                if record.identity.domain
                    == super::super::remote_object::SharedLiveSetObjectDomain::StorePackage
                    && matches!(
                        &record.state,
                        super::super::remote_object::OwnedObjectState::UploadedVerified {
                            ownership
                        } if ownership.pending.is_empty()
                            && ownership.activated
                                == std::collections::BTreeSet::from([
                                    super::super::remote_object::SharedObjectOwner::StoreCommit(
                                        fixture.commit_ref.clone()
                                    )
                                ])
                    )
        ));
        let commit = stored_remote_object(&fixture.db, &fixture.commit_ref.object).await;
        assert!(matches!(
            commit,
            super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    super::super::remote_object::RetainedAuthorityObjectDomain::Commit {
                        reference
                    } if reference == &fixture.commit_ref
                ) && matches!(
                    &record.state,
                    super::super::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                        ownership
                    } if ownership.pending.is_empty()
                        && ownership.activated
                            == std::collections::BTreeSet::from([fixture.commit_ref.clone()])
                )
        ));
        let head = stored_remote_object(&fixture.db, &fixture.head_object).await;
        assert!(matches!(
            head,
            super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    super::super::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                        reference
                    } if reference.object == fixture.head_object
                ) && matches!(
                    &record.state,
                    super::super::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                        ownership
                    } if ownership.pending.is_empty()
                        && ownership.activated
                            == std::collections::BTreeSet::from([fixture.commit_ref.clone()])
                )
        ));
    }

    #[tokio::test]
    async fn failures_before_package_commit_and_head_keep_the_exact_prepared_write_retryable() {
        for failed_call in 1..=3 {
            let fixture = prepared_write_fixture().await;
            fixture.home.fail_exact_create_before_call(failed_call);
            let first = drain_store_writes(&fixture.db, &fixture.storage).await;
            assert!(first.is_err(), "exact create call {failed_call} fails");
            assert_eq!(
                fixture.db.write_status(&fixture.write_id).await.unwrap(),
                crate::WriteStatus::Publishing,
                "transport failure retains the exact prepared write for retry",
            );
            assert!(
                fixture
                    .db
                    .oldest_prepared_store_write()
                    .await
                    .unwrap()
                    .is_some(),
                "the exact prepared write remains after exact create call {failed_call}",
            );
            assert_eq!(
                fixture
                    .db
                    .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
                    .await
                    .unwrap(),
                None,
                "local position cannot advance before a verified head",
            );
            assert_eq!(
                exact_object_exists(&fixture.home, &fixture.package_object),
                failed_call > 1,
            );
            assert_eq!(
                exact_object_exists(&fixture.home, &fixture.commit_ref.object),
                failed_call > 2,
            );
            assert!(!exact_object_exists(&fixture.home, &fixture.head_object),);

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
                    .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
                    .await
                    .unwrap(),
                Some(fixture.commit_ref.clone()),
            );
            assert!(matches!(
                fixture.db.write_status(&fixture.write_id).await.unwrap(),
                crate::WriteStatus::Published(position)
                    if matches!(
                        position.as_ref(),
                        crate::PublishedPosition::MergeConcurrent { device_id, commit }
                            if device_id == &fixture.device_id
                                && commit.coord.sequence() == 1
                                && commit.commit_hash == fixture.commit_ref.commit_hash
                    )
            ));
        }
    }

    #[tokio::test]
    async fn competing_merge_head_blocks_the_candidate_with_durable_winner_evidence() {
        let fixture = prepared_write_fixture().await;
        let winner = publish_competing_merge_head(&fixture).await;

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("classify the occupied Merge successor slot"),
            0,
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { ref reason })
                if reason.contains(&winner.head_hash.to_string())
        ));
        let write_id = fixture.write_id.clone();
        let retains_prepared = fixture
            .db
            .call(move |conn| {
                conn.query_row(
                    "SELECT prepared IS NOT NULL FROM store_writes WHERE write_id = ?1",
                    [write_id.as_str()],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(crate::DbError::from)
            })
            .await
            .expect("check durable losing candidate");
        assert!(retains_prepared);
        let package = stored_remote_object(&fixture.db, &fixture.package_object).await;
        assert!(matches!(
            package,
            super::super::remote_object::RemoteObjectRecord::CandidateExclusive(record)
                if matches!(
                    &record.state,
                    super::super::remote_object::CandidateObjectState::CleanupPending {
                        former_candidates
                    } if matches!(
                        former_candidates.as_slice(),
                        [super::super::remote_object::CandidateNonactivation {
                            proof: super::super::remote_object::CandidateNonactivationProof::MergeWinner {
                                winner_head
                            },
                            ..
                        }] if winner_head == &winner
                    )
                )
        ));
        let commit = stored_remote_object(&fixture.db, &fixture.commit_ref.object).await;
        assert!(matches!(
            commit,
            super::super::remote_object::RemoteObjectRecord::CandidateCommit(record)
                if matches!(
                    &record.state,
                    super::super::remote_object::CandidateCommitState::CleanupPending {
                        proof: super::super::remote_object::CandidateNonactivationProof::MergeWinner {
                            winner_head
                        }
                    } if winner_head == &winner
                )
        ));
        let head = stored_remote_object(&fixture.db, &fixture.head_object).await;
        assert!(matches!(
            head,
            super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.state,
                    super::super::remote_object::RetainedAuthorityObjectState::UncreatedVerified {
                        former_candidates
                    } if matches!(
                        former_candidates.as_slice(),
                        [super::super::remote_object::CandidateNonactivation {
                            proof: super::super::remote_object::CandidateNonactivationProof::MergeWinner {
                                winner_head
                            },
                            ..
                        }] if winner_head == &winner
                    )
                )
        ));
        let discard_error = fixture
            .db
            .discard_blocked_write(&fixture.write_id)
            .await
            .expect_err("candidate cleanup must precede local Merge discard");
        assert!(
            discard_error.to_string().contains("cleanup"),
            "unexpected discard error: {discard_error}"
        );
        assert!(fixture
            .db
            .merge_candidate_cleanup_pending(&fixture.write_id)
            .await
            .expect("read pending Merge cleanup"));
        let retry_error = fixture
            .db
            .retry_blocked_write(&fixture.write_id)
            .await
            .expect_err("an occupied immutable Merge slot cannot be retried");
        assert!(
            retry_error.to_string().contains("winner"),
            "unexpected retry error: {retry_error}"
        );
        fixture.home.fail_exact_delete_on_call(2);
        assert!(super::super::store_pull::cleanup_merge_candidate(
            &fixture.db,
            &fixture.storage,
            fixture.write_id.clone(),
        )
        .await
        .is_err());
        assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        super::super::store_pull::cleanup_merge_candidate(
            &fixture.db,
            &fixture.storage,
            fixture.write_id.clone(),
        )
        .await
        .expect("resume exact losing Merge cleanup");
        assert!(!fixture
            .db
            .merge_candidate_cleanup_pending(&fixture.write_id)
            .await
            .expect("read completed Merge cleanup"));
        assert_eq!(
            fixture
                .db
                .discard_blocked_write(&fixture.write_id)
                .await
                .expect("discard the cleaned losing Merge write"),
            vec![fixture.write_id.clone()],
        );
        assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(!exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        assert!(exact_object_exists(&fixture.home, &winner.object));
    }

    #[tokio::test]
    async fn blocked_merge_candidate_is_abandoned_before_local_discard() {
        let fixture = prepared_write_fixture().await;
        fixture
            .db
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block prepared Merge candidate");

        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("publish candidate abandonment"),
            MergeCandidateAbandonment::Abandoned,
        );
        assert!(!fixture
            .db
            .merge_candidate_cleanup_pending(&fixture.write_id)
            .await
            .expect("candidate cleanup completed"));
        let authority = fixture
            .db
            .latest_local_store_position()
            .await
            .expect("read local Merge position")
            .expect("abandonment advances the local stream");
        assert_ne!(authority, fixture.commit_ref);
        let authority_remote = stored_remote_object(&fixture.db, &authority.object).await;
        assert!(matches!(
            authority_remote,
            super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    super::super::remote_object::RetainedAuthorityObjectDomain::Commit {
                        reference
                    } if reference == &authority
                )
        ));
        assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(!exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        assert_eq!(
            fixture
                .db
                .discard_blocked_write(&fixture.write_id)
                .await
                .expect("reverse the abandoned write"),
            vec![fixture.write_id.clone()]
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
        ));
        assert!(remote_object_exists(&fixture.db, &authority.object).await);
    }

    #[tokio::test]
    async fn prepared_merge_abandonment_resumes_after_restart() {
        let fixture = prepared_write_fixture().await;
        fixture
            .db
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block prepared Merge candidate");
        assert!(prepare_merge_candidate_abandonment(
            &fixture.db,
            &fixture.storage,
            &fixture.device_id,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("persist Merge abandonment"));

        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("resume Merge abandonment"),
            MergeCandidateAbandonment::Abandoned,
        );
    }

    #[tokio::test]
    async fn merge_abandonment_retries_commit_and_head_publication_failures() {
        for failed_call in 1..=2 {
            let fixture = prepared_write_fixture().await;
            fixture
                .db
                .set_write_status(
                    &fixture.write_id,
                    crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                        reason: "host chose discard".to_string(),
                    }),
                )
                .await
                .expect("block prepared Merge candidate");
            assert!(prepare_merge_candidate_abandonment(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("persist Merge abandonment"));
            fixture.home.fail_exact_create_before_call(failed_call);

            assert!(
                abandon_merge_candidate(
                    &fixture.db,
                    &fixture.storage,
                    &fixture.device_id,
                    &fixture.keypair,
                    fixture.write_id.clone(),
                )
                .await
                .is_err(),
                "exact create call {failed_call} fails",
            );
            assert_eq!(
                abandon_merge_candidate(
                    &fixture.db,
                    &fixture.storage,
                    &fixture.device_id,
                    &fixture.keypair,
                    fixture.write_id.clone(),
                )
                .await
                .expect("retry Merge abandonment publication"),
                MergeCandidateAbandonment::Abandoned,
            );
        }
    }

    #[tokio::test]
    async fn accepted_merge_abandonment_retries_losing_object_deletion() {
        let fixture = prepared_write_fixture().await;
        let batch = fixture
            .db
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists");
        publish_prepared_remote_objects(
            &fixture.db,
            &fixture.storage,
            &fixture.write_id,
            fixture.root.store_root_hash,
        )
        .await
        .expect("publish original candidate objects");
        fixture
            .storage
            .create_protocol_object(&batch.commit.prepared)
            .await
            .expect("publish original candidate commit");
        fixture
            .db
            .mark_candidate_commit_uploaded(fixture.commit_ref.clone())
            .await
            .expect("record uploaded original candidate commit");
        fixture
            .db
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block prepared Merge candidate");
        fixture.home.fail_exact_delete_on_call(1);

        assert!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .is_err(),
            "losing object deletion fails",
        );
        assert!(fixture
            .db
            .merge_candidate_cleanup_pending(&fixture.write_id)
            .await
            .expect("cleanup remains pending"));
        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("retry losing object deletion"),
            MergeCandidateAbandonment::Abandoned,
        );
    }

    #[tokio::test]
    async fn original_candidate_activation_wins_abandonment_race() {
        let fixture = prepared_write_fixture().await;
        let batch = fixture
            .db
            .oldest_prepared_store_write()
            .await
            .expect("load prepared Merge write")
            .expect("prepared Merge write exists");
        publish_prepared_remote_objects(
            &fixture.db,
            &fixture.storage,
            &fixture.write_id,
            fixture.root.store_root_hash,
        )
        .await
        .expect("publish original candidate objects");
        fixture
            .storage
            .create_protocol_object(&batch.commit.prepared)
            .await
            .expect("publish original candidate commit");
        fixture
            .storage
            .create_protocol_object(&batch.head.prepared)
            .await
            .expect("activate original candidate");
        fixture
            .db
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block locally unobserved Merge activation");

        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("settle activation that won abandonment race"),
            MergeCandidateAbandonment::CandidateActivated,
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if position.commit() == &fixture.commit_ref
        ));
        assert!(exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        assert!(exact_object_exists(&fixture.home, &fixture.head_object));
    }

    #[tokio::test]
    async fn third_candidate_wins_after_abandonment_preparation() {
        let fixture = prepared_write_fixture().await;
        fixture
            .db
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block prepared Merge candidate");
        assert!(prepare_merge_candidate_abandonment(
            &fixture.db,
            &fixture.storage,
            &fixture.device_id,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("persist Merge abandonment"));
        let authority = fixture
            .db
            .oldest_prepared_store_write()
            .await
            .expect("load prepared abandonment")
            .expect("prepared abandonment exists");
        let authority_commit = authority.commit.object.clone();
        let authority_head = authority.head.object.clone();
        let winner = publish_competing_merge_head(&fixture).await;

        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("settle third-candidate winner"),
            MergeCandidateAbandonment::Abandoned,
        );
        assert!(!fixture
            .db
            .merge_candidate_cleanup_pending(&fixture.write_id)
            .await
            .expect("all losing candidates are cleaned"));
        assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(!exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        assert!(exact_object_exists(&fixture.home, &winner.object));
        assert!(!remote_object_exists(&fixture.db, &authority_commit).await);
        assert!(!remote_object_exists(&fixture.db, &authority_head).await);
        assert_eq!(
            fixture
                .db
                .discard_blocked_write(&fixture.write_id)
                .await
                .expect("reverse the abandoned local write"),
            vec![fixture.write_id.clone()],
        );
    }

    #[tokio::test]
    async fn alternate_head_for_abandonment_authority_is_accepted() {
        let fixture = prepared_write_fixture().await;
        fixture
            .db
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block prepared Merge candidate");
        assert!(prepare_merge_candidate_abandonment(
            &fixture.db,
            &fixture.storage,
            &fixture.device_id,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("persist Merge abandonment"));
        let accepted_head = publish_alternate_head_for_prepared_commit(&fixture).await;

        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.device_id,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("accept alternate abandonment head"),
            MergeCandidateAbandonment::Abandoned,
        );
        assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(!exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        assert!(exact_object_exists(&fixture.home, &accepted_head.object));
    }

    #[tokio::test]
    async fn alternate_merge_head_for_the_exact_commit_completes_as_accepted() {
        let fixture = prepared_write_fixture().await;
        let accepted_head = publish_alternate_head_for_prepared_commit(&fixture).await;

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("accept the exact commit through the occupied Merge head"),
            1,
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if position.commit() == &fixture.commit_ref
        ));
        let head = stored_remote_object(&fixture.db, &accepted_head.object).await;
        assert!(matches!(
            head,
            super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    super::super::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                        reference
                    } if reference == &accepted_head
                )
        ));
    }

    #[tokio::test]
    async fn exact_create_readback_mismatch_retains_the_prepared_write_for_retry() {
        let fixture = prepared_write_fixture().await;
        fixture.home.corrupt_exact_readback_on_call(1);

        let result = drain_store_writes(&fixture.db, &fixture.storage).await;

        assert!(matches!(
            result,
            Err(StoreOutboundError::Object(StoreObjectError::Storage(_)))
        ));
        assert_eq!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Publishing,
            "a provider readback mismatch retains the exact prepared write for retry",
        );
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn lost_exact_head_response_is_settled_by_readback_and_completion_is_idempotent() {
        let fixture = prepared_write_fixture().await;
        fixture.home.fail_exact_create_after_call(3);
        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("settle lost head response by exact readback"),
            1
        );
        assert!(exact_object_exists(&fixture.home, &fixture.package_object));
        assert!(exact_object_exists(
            &fixture.home,
            &fixture.commit_ref.object
        ));
        assert!(exact_object_exists(&fixture.home, &fixture.head_object));
        assert_eq!(
            fixture
                .db
                .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
                .await
                .unwrap(),
            Some(fixture.commit_ref.clone())
        );

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage)
                .await
                .expect("already-completed exact batch is idempotent"),
            0
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition::MergeConcurrent { commit, .. }
                        if commit.coord.sequence() == 1
                            && commit.commit_hash == fixture.commit_ref.commit_hash
                )
        ));
    }

    #[tokio::test]
    async fn local_completion_failure_rolls_back_position_and_retries_after_visible_head() {
        let fixture = prepared_write_fixture().await;
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
            crate::WriteStatus::Publishing,
        );
        assert!(exact_object_exists(&fixture.home, &fixture.head_object));
        assert!(fixture
            .db
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            fixture
                .db
                .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
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
                .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
                .await
                .unwrap(),
            Some(fixture.commit_ref.clone()),
        );
        assert!(matches!(
            fixture.db.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition::MergeConcurrent { commit, .. }
                        if commit.coord.sequence() == 1
                            && commit.commit_hash == fixture.commit_ref.commit_hash
                )
        ));
    }

    #[tokio::test]
    async fn restart_fails_loud_when_a_prepared_write_has_no_usable_exact_root() {
        for invalid_root in [
            None,
            Some("0000000000000000000000000000000000000000000000000000000000000000"),
        ] {
            let temp = tempfile::tempdir().expect("temp dir");
            let path = temp.path().join("store.sqlite3");
            let open = || {
                Database::open(
                    &path,
                    crate::sync::test_helpers::test_synced_tables(),
                    crate::blob::BLOB_TOMBSTONE_GRACE,
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
            .expect("in-memory home supports immutable copies");
            let db = open();
            let (root, device_id) =
                initialize_exact_store(&db, &storage, "prepared-root-status", &keypair).await;
            let membership = super::super::membership_ops::load_and_persist_owner_anchor(
                &storage,
                &root,
                &crate::keys::public_key_hex(&keypair),
                &db,
            )
            .await
            .expect("load exact founder membership");
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
                &device_id,
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                Some(&membership),
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
                        "UPDATE store_protocol_root_authority SET store_root_hash = ?1",
                        [value],
                    ),
                    None => conn.execute("DELETE FROM store_protocol_root_authority", []),
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
                (None, Err(StoreOutboundError::Database(reason))) => {
                    assert!(reason.contains("exact Store root authority is absent"));
                }
                (Some(_), Err(StoreOutboundError::Database(reason))) => {
                    assert!(reason.contains("Store root authority hash differs"));
                }
                (_, result) => panic!("unexpected Store root failure: {result:?}"),
            }
            assert!(matches!(
                reopened
                    .write_status(&write_id)
                    .await
                    .expect("write status"),
                crate::WriteStatus::Publishing
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
        .expect("in-memory home supports immutable copies");
        let db = open_test_db();
        let (root, device_id) =
            initialize_exact_store(&db, &storage, "blocked-retry", &keypair).await;
        let membership = super::super::membership_ops::load_and_persist_owner_anchor(
            &storage,
            &root,
            &crate::keys::public_key_hex(&keypair),
            &db,
        )
        .await
        .expect("load exact founder membership");
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
        remove_exact_store_root(&db).await;
        let (_store_temp, store_dir) = temp_store_dir();

        assert!(matches!(
            prepare_pending_store_write(
                &db,
                &storage,
                &device_id,
                "2026-01-01T00:00:00Z",
                &keypair,
                &store_dir,
                Some(&membership),
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
            &device_id,
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            Some(&membership),
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
                &device_id,
                "2026-01-01T00:00:02Z",
                &keypair,
                &store_dir,
                Some(&membership),
            )
            .await,
            Err(StoreOutboundError::MissingState { .. })
        ));
        assert!(matches!(
            db.write_status(&first).await.unwrap(),
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { .. })
        ));

        reinstall_exact_store_root(&db, &storage, &root).await;
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
            &device_id,
            "2026-01-01T00:00:03Z",
            &keypair,
            &store_dir,
            Some(&membership),
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
        .expect("in-memory home supports immutable copies");
        let db = open_test_db();
        let (root, device_id) =
            initialize_exact_store(&db, &storage, "blocked-discard", &keypair).await;
        let membership = super::super::membership_ops::load_and_persist_owner_anchor(
            &storage,
            &root,
            &crate::keys::public_key_hex(&keypair),
            &db,
        )
        .await
        .expect("load exact founder membership");
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
        remove_exact_store_root(&db).await;
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write(
            &db,
            &storage,
            &device_id,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            Some(&membership),
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

        reinstall_exact_store_root(&db, &storage, &root).await;
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
            &device_id,
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            Some(&membership),
        )
        .await
        .expect("prepare write after discarded blocked writes"));
        assert_eq!(drain_store_writes(&db, &storage).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn retrying_one_blocked_serial_write_revalidates_the_whole_ordered_branch() {
        let (_home, storage, db, keypair, root, blocked) =
            serial_fixture("serial-blocked-retry").await;
        remove_exact_store_root(&db).await;
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .is_err());
        assert_eq!(db.blocked_writes().await.unwrap().len(), 2);

        reinstall_exact_store_root(&db, &storage, &root).await;
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

        assert_eq!(
            db.retry_blocked_write(&blocked[1].write_id).await.unwrap(),
            blocked
                .iter()
                .map(|write| write.write_id.clone())
                .collect::<Vec<_>>()
        );
        remove_exact_store_root(&db).await;
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
            None,
        )
        .await
        .is_err());
        assert_eq!(db.blocked_writes().await.unwrap().len(), 3);
        reinstall_exact_store_root(&db, &storage, &root).await;
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:02Z",
            &keypair,
            &store_dir,
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
        remove_exact_store_root(&db).await;
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(prepare_pending_store_write_with_coordination(
            &db,
            &storage,
            Some(storage.serial_coordination().unwrap()),
            &local_device_id(&db).await,
            "2026-01-01T00:00:00Z",
            &keypair,
            &store_dir,
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
        reinstall_exact_store_root(&db, &storage, &root).await;
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
            &local_device_id(&db).await,
            "2026-01-01T00:00:01Z",
            &keypair,
            &store_dir,
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
