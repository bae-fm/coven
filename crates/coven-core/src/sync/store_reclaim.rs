//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use super::membership::{MemberRole, MembershipChain, SerialMembershipState};
use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage};
use super::store_commit::{
    ack_slot_prefix, snapshot_image_semantic_prefix, snapshot_slot_prefix, CommitFrontier,
    ObjectHash, SnapshotMeta, StoreAck, StoreAckRef, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreHistoryCut, StoreRootRef,
    StoreSerialPredecessor, StoreSnapshotRef,
};
use super::store_objects::StoreObjectError;

#[derive(Debug, PartialEq, Eq)]
pub struct StoreReclaimResult {
    pub packages_deleted: u64,
    pub physical_copies_deleted: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreReclaimError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("no authorized complete Store snapshot is available for reclamation")]
    NoSnapshot,
    #[error("snapshot authorization history is invalid: {0}")]
    Authorization(String),
    #[error("Store reclamation proof uses the wrong write policy: {0}")]
    PolicyMismatch(String),
    #[error("active member {member:?} has no exact Store device registration")]
    MissingRegisteredDevice { member: String },
    #[error(
        "active Store device {device_id:?} for member {member:?} has no exact acknowledgement"
    )]
    MissingAcknowledgement { member: String, device_id: String },
    #[error(
        "Store device {device_id:?} acknowledgement author differs from its activated registration"
    )]
    AckAuthorMismatch { device_id: String },
    #[error("active member {member:?} device {ack_device_id:?} has no acknowledgement covering exact snapshot commit {snapshot_commit}")]
    StaleAcknowledgement {
        member: String,
        ack_device_id: String,
        snapshot_commit: ObjectHash,
    },
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
    #[error("deleting exact Store package owned by commit {commit_hash} failed: {source}")]
    Delete {
        commit_hash: ObjectHash,
        #[source]
        source: StorageError,
    },
}

#[derive(Clone, Copy)]
pub enum ReclaimMembership<'a> {
    MergeConcurrent {
        membership: &'a MembershipChain,
        discovery_proof: super::pull::MembershipDiscoveryProof,
    },
    Serial(&'a SerialMembershipState),
}

impl ReclaimMembership<'_> {
    fn write_policy(self) -> crate::WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => crate::WritePolicy::MergeConcurrent,
            Self::Serial(_) => crate::WritePolicy::Serial,
        }
    }

    fn is_owner(self, pubkey: &str) -> bool {
        match self {
            Self::MergeConcurrent { membership, .. } => membership.is_owner_now(pubkey),
            Self::Serial(membership) => membership.is_owner(pubkey),
        }
    }

    fn current_members(self) -> Vec<(String, MemberRole)> {
        match self {
            Self::MergeConcurrent { membership, .. } => membership.current_members(),
            Self::Serial(membership) => membership.current_members(),
        }
    }
}

pub async fn reclaim_store_packages(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    membership: ReclaimMembership<'_>,
) -> Result<StoreReclaimResult, StoreReclaimError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        .ok_or_else(|| StoreReclaimError::Authorization("Store root is absent".to_string()))?;
    if root.store_root_hash != store_root_hash {
        return Err(StoreReclaimError::Authorization(
            "reclamation root differs from the exact local Store root".to_string(),
        ));
    }
    let registrations = db
        .activated_store_device_registration_records()
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let snapshot = choose_snapshot(storage, &root, membership, &registrations).await?;
    let acknowledgements = load_latest_acknowledgements(storage, &root, &registrations).await?;
    require_registered_device_acks(
        storage,
        &root,
        membership,
        &snapshot,
        &registrations,
        &acknowledgements,
    )
    .await?;

    let targets = exact_package_targets(storage, &root, &snapshot.meta.coverage).await?;
    let mut packages_deleted = 0_u64;
    for (commit, package) in targets {
        storage
            .delete_protocol_object(&package.object)
            .await
            .map_err(|source| StoreReclaimError::Delete {
                commit_hash: commit.commit_hash,
                source,
            })?;
        packages_deleted = packages_deleted.checked_add(1).ok_or_else(|| {
            StoreReclaimError::Authorization("reclaimed package count exceeded u64".to_string())
        })?;
    }
    Ok(StoreReclaimResult {
        packages_deleted,
        physical_copies_deleted: packages_deleted,
    })
}

struct ExactSnapshot {
    reference: StoreSnapshotRef,
    meta: SnapshotMeta,
}

async fn choose_snapshot(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    membership: ReclaimMembership<'_>,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<ExactSnapshot, StoreReclaimError> {
    let mut authorized = Vec::new();
    for (registration_ref, registration) in registrations {
        for snapshot in load_snapshot_stream(storage, root, registration_ref, registration).await? {
            if snapshot.meta.coverage.policy() != membership.write_policy() {
                return Err(StoreReclaimError::PolicyMismatch(format!(
                    "snapshot coverage uses {:?}, Store uses {:?}",
                    snapshot.meta.coverage.policy(),
                    membership.write_policy()
                )));
            }
            let owner = match &snapshot.meta.coverage {
                CommitFrontier::MergeConcurrent(_) => {
                    membership.is_owner(&registration.author_pubkey)
                }
                CommitFrontier::Serial(position) => {
                    super::store_pull::load_serial_authorization_at_position(
                        storage,
                        root,
                        position.clone(),
                    )
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
                    .membership
                    .is_owner(&registration.author_pubkey)
                }
            };
            if owner {
                let context = ProtocolObjectContext::store(
                    root.store_root_hash,
                    ProtocolObjectDomain::StoreSnapshotImage,
                );
                let bytes = storage
                    .read_protocol_object(
                        &context,
                        &snapshot.meta.image.object,
                        &snapshot_image_semantic_prefix(
                            &registration.device_id.to_string(),
                            snapshot.meta.image.image_hash,
                        ),
                    )
                    .await
                    .map_err(StoreObjectError::from)?;
                if ObjectHash::digest(&bytes) != snapshot.meta.image.image_hash {
                    return Err(StoreReclaimError::Authorization(
                        "snapshot image differs from its signed exact reference".to_string(),
                    ));
                }
                authorized.push(snapshot);
            }
        }
    }
    authorized
        .into_iter()
        .max_by_key(|snapshot| snapshot.reference.snapshot_hash)
        .ok_or(StoreReclaimError::NoSnapshot)
}

async fn load_snapshot_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
) -> Result<Vec<ExactSnapshot>, StoreReclaimError> {
    let mut slot = match &registration.snapshots {
        super::store_commit::DeviceStreamAnchor::StoreSnapshots { first_slot } => {
            first_slot.clone()
        }
        _ => {
            return Err(StoreReclaimError::Authorization(
                "activated registration lacks a Store snapshot anchor".to_string(),
            ))
        }
    };
    let context = ProtocolObjectContext::store(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let mut sequence = 1_u64;
    let mut predecessor = None;
    let mut snapshots = Vec::new();
    loop {
        let prefix = snapshot_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(value) => value,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::from(error).into()),
        };
        let semantic_hash = SnapshotMeta::semantic_hash_from_bytes(&bytes)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let reference = StoreSnapshotRef {
            sequence,
            snapshot_hash: semantic_hash,
            object,
        };
        let meta = SnapshotMeta::parse_at(&bytes, root.store_root_hash, &reference, registration)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        if meta.author_registration != *registration_ref
            || meta.predecessor != predecessor
            || meta.successor.predecessor
                != predecessor
                    .as_ref()
                    .map(|value: &StoreSnapshotRef| value.object.clone())
        {
            return Err(StoreReclaimError::Authorization(
                "Store snapshot stream has an invalid exact link".to_string(),
            ));
        }
        slot = meta.successor.next_slot.clone();
        predecessor = Some(reference.clone());
        snapshots.push(ExactSnapshot { reference, meta });
        sequence = sequence.checked_add(1).ok_or_else(|| {
            StoreReclaimError::Authorization("snapshot sequence overflow".to_string())
        })?;
    }
    Ok(snapshots)
}

async fn load_latest_acknowledgements(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<BTreeMap<super::store_commit::StoreDeviceId, StoreAck>, StoreReclaimError> {
    let mut latest = BTreeMap::new();
    for (registration_ref, registration) in registrations {
        if let Some(ack) = load_ack_stream(storage, root, registration_ref, registration)
            .await?
            .pop()
        {
            latest.insert(registration.device_id, ack);
        }
    }
    Ok(latest)
}

async fn load_ack_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
) -> Result<Vec<StoreAck>, StoreReclaimError> {
    let mut slot = match &registration.acknowledgements {
        super::store_commit::DeviceStreamAnchor::StoreAcknowledgements { first_slot } => {
            first_slot.clone()
        }
        _ => {
            return Err(StoreReclaimError::Authorization(
                "activated registration lacks a Store acknowledgement anchor".to_string(),
            ))
        }
    };
    let context =
        ProtocolObjectContext::store(root.store_root_hash, ProtocolObjectDomain::StoreAck);
    let mut revision = 1_u64;
    let mut predecessor = None;
    let mut acknowledgements = Vec::new();
    loop {
        let prefix = ack_slot_prefix(&registration.device_id.to_string(), revision);
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(value) => value,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::from(error).into()),
        };
        let semantic_hash = StoreAck::semantic_hash_from_bytes(&bytes)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let reference = StoreAckRef {
            revision,
            ack_hash: semantic_hash,
            object,
        };
        let ack = StoreAck::parse_at(&bytes, root.store_root_hash, &reference, registration)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        if ack.author_registration != *registration_ref
            || ack.predecessor != predecessor
            || ack.successor.predecessor
                != predecessor
                    .as_ref()
                    .map(|value: &StoreAckRef| value.object.clone())
        {
            return Err(StoreReclaimError::Authorization(
                "Store acknowledgement stream has an invalid exact link".to_string(),
            ));
        }
        slot = ack.successor.next_slot.clone();
        predecessor = Some(reference);
        acknowledgements.push(ack);
        revision = revision.checked_add(1).ok_or_else(|| {
            StoreReclaimError::Authorization("acknowledgement revision overflow".to_string())
        })?;
    }
    Ok(acknowledgements)
}

async fn require_registered_device_acks(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    membership: ReclaimMembership<'_>,
    snapshot: &ExactSnapshot,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
    latest: &BTreeMap<super::store_commit::StoreDeviceId, StoreAck>,
) -> Result<(), StoreReclaimError> {
    let active = membership
        .current_members()
        .into_iter()
        .map(|(pubkey, _)| pubkey)
        .collect::<BTreeSet<_>>();
    let active_registrations = registrations
        .iter()
        .filter(|(_, registration)| active.contains(&registration.author_pubkey))
        .collect::<Vec<_>>();
    for member in &active {
        if !active_registrations
            .iter()
            .any(|(_, registration)| &registration.author_pubkey == member)
        {
            return Err(StoreReclaimError::MissingRegisteredDevice {
                member: member.clone(),
            });
        }
    }
    for (_, registration) in active_registrations {
        let device_id = registration.device_id;
        let ack =
            latest
                .get(&device_id)
                .ok_or_else(|| StoreReclaimError::MissingAcknowledgement {
                    member: registration.author_pubkey.clone(),
                    device_id: device_id.to_string(),
                })?;
        if ack.author_registration.device_id != device_id {
            return Err(StoreReclaimError::AckAuthorMismatch {
                device_id: device_id.to_string(),
            });
        }
        if ack.store_cut.policy() != membership.write_policy() {
            return Err(StoreReclaimError::PolicyMismatch(
                "snapshot and acknowledgement use different Store policies".to_string(),
            ));
        }
        match (&snapshot.meta.coverage, &ack.store_cut) {
            (
                CommitFrontier::MergeConcurrent(snapshot_commits),
                StoreHistoryCut::MergeConcurrent(ack_commits),
            ) => {
                for (stream_id, snapshot_commit) in snapshot_commits {
                    let covered = match ack_commits.get(stream_id) {
                        Some(ack_commit) => {
                            position_covers(storage, root, ack_commit, snapshot_commit).await?
                        }
                        None => false,
                    };
                    require_covered(covered, registration, device_id, snapshot_commit)?;
                }
            }
            (CommitFrontier::Serial(snapshot_commit), StoreHistoryCut::Serial(ack_cut)) => {
                if let Some(snapshot_commit) = snapshot_commit {
                    let covered = match ack_cut {
                        StoreSerialPredecessor::Commit(ack_commit) => {
                            position_covers(storage, root, ack_commit, snapshot_commit).await?
                        }
                        StoreSerialPredecessor::Genesis { .. } => false,
                    };
                    require_covered(covered, registration, device_id, snapshot_commit)?;
                }
            }
            _ => {
                return Err(StoreReclaimError::PolicyMismatch(
                    "snapshot and acknowledgement use different Store policies".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn require_covered(
    covered: bool,
    registration: &StoreDeviceRegistration,
    device_id: super::store_commit::StoreDeviceId,
    snapshot_commit: &StoreBatchCommitRef,
) -> Result<(), StoreReclaimError> {
    if covered {
        return Ok(());
    }
    Err(StoreReclaimError::StaleAcknowledgement {
        member: registration.author_pubkey.clone(),
        ack_device_id: device_id.to_string(),
        snapshot_commit: snapshot_commit.commit_hash,
    })
}

fn frontier_refs(frontier: &CommitFrontier) -> Vec<&StoreBatchCommitRef> {
    match frontier {
        CommitFrontier::MergeConcurrent(values) => values.values().collect(),
        CommitFrontier::Serial(Some(value)) => vec![value],
        CommitFrontier::Serial(None) => Vec::new(),
    }
}

async fn position_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    covering: &StoreBatchCommitRef,
    covered: &StoreBatchCommitRef,
) -> Result<bool, StoreReclaimError> {
    if !same_stream(&covering.coord, &covered.coord)
        || covering.coord.sequence() < covered.coord.sequence()
    {
        return Ok(false);
    }
    let mut cursor = covering.clone();
    while cursor.coord.sequence() > covered.coord.sequence() {
        let (commit, _) = super::store_pull::load_commit_with_author(storage, root, &cursor)
            .await
            .map_err(StoreReclaimError::Object)?;
        cursor = commit
            .order
            .predecessor()
            .cloned()
            .ok_or(StoreReclaimError::MissingAncestry {
                commit_hash: cursor.commit_hash,
            })?;
    }
    Ok(cursor == *covered)
}

fn same_stream(left: &StoreCommitCoord, right: &StoreCommitCoord) -> bool {
    match (left, right) {
        (
            StoreCommitCoord::MergeConcurrent {
                stream_id: left, ..
            },
            StoreCommitCoord::MergeConcurrent {
                stream_id: right, ..
            },
        ) => left == right,
        (StoreCommitCoord::Serial { .. }, StoreCommitCoord::Serial { .. }) => true,
        _ => false,
    }
}

async fn exact_package_targets(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &CommitFrontier,
) -> Result<Vec<(StoreBatchCommitRef, super::store_commit::StorePackageRef)>, StoreReclaimError> {
    let mut targets = BTreeMap::new();
    for tip in frontier_refs(coverage) {
        let mut cursor = Some(tip.clone());
        while let Some(reference) = cursor {
            if targets.contains_key(&reference) {
                break;
            }
            let (commit, _) = super::store_pull::load_commit_with_author(storage, root, &reference)
                .await
                .map_err(StoreReclaimError::Object)?;
            if let Some(package) = commit.store_package().cloned() {
                targets.insert(reference.clone(), package);
            }
            cursor = commit.order.predecessor().cloned();
        }
    }
    Ok(targets.into_iter().collect())
}
