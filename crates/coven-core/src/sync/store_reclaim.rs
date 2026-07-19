//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle_control::StoreMembershipStateRef;
use super::membership::{MemberRole, MembershipChain, MembershipGrantId, SerialMembershipState};
use super::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{
    ack_slot_prefix, snapshot_image_semantic_prefix, snapshot_slot_prefix, CommitFrontier,
    ObjectHash, SnapshotMeta, StoreAck, StoreAckRef, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreHistoryCut, StorePackageRef,
    StoreProtocolError, StoreRootRef, StoreSerialPredecessor, StoreSnapshotRef,
    STORE_PROTOCOL_VERSION,
};
use super::store_objects::StoreObjectError;
use crate::keys::{self, UserKeypair};

const RECLAIM_EVIDENCE_DOMAIN: &[u8] = b"coven.store-reclaim-evidence.v1\0";
const RECLAIM_AUTHORIZATION_DOMAIN: &[u8] = b"coven.store-reclaim-authorization.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageReclaimClaim {
    pub package: StorePackageRef,
    pub activation: StoreBatchCommitRef,
    pub covering_snapshot: StoreSnapshotRef,
    pub acknowledgements: Vec<StoreAckRef>,
}

impl StorePackageReclaimClaim {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        if self.acknowledgements.is_empty() {
            return Err(StoreProtocolError::Malformed(
                "Store package reclaim evidence has no acknowledgements".to_string(),
            ));
        }
        if self
            .acknowledgements
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreProtocolError::Malformed(
                "Store package reclaim acknowledgements are not strictly sorted and unique"
                    .to_string(),
            ));
        }
        let mut registrations = BTreeSet::new();
        if self
            .acknowledgements
            .iter()
            .any(|acknowledgement| !registrations.insert(&acknowledgement.registration))
        {
            return Err(StoreProtocolError::Malformed(
                "Store package reclaim evidence repeats a device registration".to_string(),
            ));
        }
        if self.package.object == self.activation.object
            || self.package.object == self.covering_snapshot.object
            || self
                .acknowledgements
                .iter()
                .any(|acknowledgement| acknowledgement.object == self.package.object)
        {
            return Err(StoreProtocolError::Malformed(
                "Store package reclaim target aliases proof authority".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimEvidenceRef {
    pub evidence_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl ReclaimEvidenceRef {
    pub fn from_evidence(evidence: &ReclaimEvidence, object: ExactObjectRef) -> Self {
        Self {
            evidence_hash: evidence.evidence_hash(),
            object,
        }
    }

    pub fn verify(&self, evidence: &ReclaimEvidence) -> Result<(), StoreProtocolError> {
        let actual = evidence.evidence_hash();
        if actual != self.evidence_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: self.evidence_hash,
                actual,
            });
        }
        evidence.verify()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimEvidence {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub claim: StorePackageReclaimClaim,
    pub author_pubkey: String,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimEvidenceSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    claim: &'a StorePackageReclaimClaim,
    author_pubkey: &'a str,
}

impl ReclaimEvidence {
    pub fn signed(
        store_root_hash: ObjectHash,
        mut claim: StorePackageReclaimClaim,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        claim.acknowledgements.sort();
        claim.validate()?;
        let mut evidence = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            claim,
            author_pubkey: keys::public_key_hex(signer),
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &evidence.canonical_signed_bytes());
        evidence.signature = signature;
        Ok(evidence)
    }

    pub fn canonical_signed_bytes(&self) -> Vec<u8> {
        super::store_commit::domain_json(
            RECLAIM_EVIDENCE_DOMAIN,
            &ReclaimEvidenceSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                claim: &self.claim,
                author_pubkey: &self.author_pubkey,
            },
        )
    }

    pub fn evidence_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ReclaimEvidence serialization cannot fail")
    }

    pub fn verify(&self) -> Result<(), StoreProtocolError> {
        if self.version != STORE_PROTOCOL_VERSION {
            return Err(StoreProtocolError::UnsupportedVersion(self.version));
        }
        self.claim.validate()?;
        if !keys::verify_signature_hex(
            &self.author_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreReclaimAuthority {
    pub membership: StoreMembershipStateRef,
    pub owner_grant: MembershipGrantId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimAuthorizationRef {
    pub authorization_hash: ObjectHash,
    pub evidence: ReclaimEvidenceRef,
    pub object: ExactObjectRef,
}

impl ReclaimAuthorizationRef {
    pub fn from_authorization(
        authorization: &ReclaimAuthorization,
        object: ExactObjectRef,
    ) -> Self {
        Self {
            authorization_hash: authorization.authorization_hash(),
            evidence: authorization.evidence.clone(),
            object,
        }
    }

    pub fn verify(
        &self,
        authorization: &ReclaimAuthorization,
        owner_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        let actual = authorization.authorization_hash();
        if actual != self.authorization_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: self.authorization_hash,
                actual,
            });
        }
        if authorization.evidence != self.evidence {
            return Err(StoreProtocolError::Malformed(
                "reclaim authorization evidence differs from its exact reference".to_string(),
            ));
        }
        authorization.verify(owner_pubkey)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimAuthorization {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub target: StorePackageRef,
    pub evidence: ReclaimEvidenceRef,
    pub authority: StoreReclaimAuthority,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimAuthorizationSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    target: &'a StorePackageRef,
    evidence: &'a ReclaimEvidenceRef,
    authority: &'a StoreReclaimAuthority,
}

impl ReclaimAuthorization {
    pub fn signed(
        store_root_hash: ObjectHash,
        target: StorePackageRef,
        evidence: ReclaimEvidenceRef,
        authority: StoreReclaimAuthority,
        signer: &UserKeypair,
    ) -> Self {
        let mut authorization = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            target,
            evidence,
            authority,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &authorization.canonical_signed_bytes());
        authorization.signature = signature;
        authorization
    }

    pub fn canonical_signed_bytes(&self) -> Vec<u8> {
        super::store_commit::domain_json(
            RECLAIM_AUTHORIZATION_DOMAIN,
            &ReclaimAuthorizationSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                target: &self.target,
                evidence: &self.evidence,
                authority: &self.authority,
            },
        )
    }

    pub fn authorization_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ReclaimAuthorization serialization cannot fail")
    }

    pub fn verify(&self, owner_pubkey: &str) -> Result<(), StoreProtocolError> {
        if self.version != STORE_PROTOCOL_VERSION {
            return Err(StoreProtocolError::UnsupportedVersion(self.version));
        }
        if !keys::verify_signature_hex(
            owner_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }
}

pub fn reclaim_evidence_semantic_prefix(evidence_hash: ObjectHash) -> String {
    format!("store-v1/reclaim/evidence/{evidence_hash}")
}

pub fn reclaim_authorization_semantic_prefix(authorization_hash: ObjectHash) -> String {
    format!("store-v1/reclaim/authorizations/{authorization_hash}")
}

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
                let context = ProtocolObjectContext::store_encrypted(
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
    let context = ProtocolObjectContext::signed_plaintext(
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
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let mut sequence = 1_u64;
    let mut predecessor = None;
    let mut acknowledgements = Vec::new();
    loop {
        let prefix = ack_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(value) => value,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::from(error).into()),
        };
        let semantic_hash = StoreAck::semantic_hash_from_bytes(&bytes)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let reference = StoreAckRef {
            registration: registration_ref.clone(),
            sequence,
            ack_hash: semantic_hash,
            object,
        };
        let ack = StoreAck::parse_at(&bytes, root, &reference, registration)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        if ack.registration != *registration_ref
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
        sequence = sequence.checked_add(1).ok_or_else(|| {
            StoreReclaimError::Authorization("acknowledgement sequence overflow".to_string())
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
        if ack.registration.device_id != device_id {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::ObjectSlot;

    fn proof_object(path: &str) -> ExactObjectRef {
        let bytes = path.as_bytes();
        ExactObjectRef::new(
            ObjectSlot::logical(path.to_string()).expect("valid proof slot"),
            u64::try_from(bytes.len()).expect("proof length fits u64"),
            ObjectHash::digest(bytes),
        )
    }

    #[tokio::test]
    async fn exact_reclaim_authorization_opens_its_encrypted_evidence() {
        let db = crate::sync::test_helpers::open_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "signed-reclaim-authority",
            UserKeypair::generate(),
        )
        .await
        .expect("create Store");
        let changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim', '2026-01-01')",
            ],
        )
        .await;
        let activation = store
            .publish_changeset("founder", 1, &changeset, db.schema_version())
            .await
            .expect("publish package activation");
        let (founder_ref, founder, _) = store
            .founder_device_authority()
            .await
            .expect("load founder authority");
        let activated = super::super::store_objects::load_commit_ref(
            &store.storage,
            store.root.store_root_hash,
            &activation,
            &founder,
        )
        .await
        .expect("load package activation")
        .value;
        let package = activated
            .store_package()
            .expect("activation carries Store package")
            .clone();
        let evidence = ReclaimEvidence::signed(
            store.root.store_root_hash,
            StorePackageReclaimClaim {
                package: package.clone(),
                activation,
                covering_snapshot: StoreSnapshotRef {
                    sequence: 1,
                    snapshot_hash: ObjectHash::digest(b"covering snapshot"),
                    object: proof_object("store-v1/snapshots/founder/covering"),
                },
                acknowledgements: vec![StoreAckRef {
                    registration: founder_ref,
                    sequence: 1,
                    ack_hash: ObjectHash::digest(b"acknowledgement"),
                    object: proof_object("store-v1/acks/founder/1.json"),
                }],
            },
            &store.signer,
        )
        .expect("sign reclaim evidence");
        let evidence_context = ProtocolObjectContext::store_encrypted(
            store.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimEvidence,
        );
        let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
        let evidence_slot = store
            .storage
            .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
            .await
            .expect("allocate evidence slot");
        let prepared_evidence = store
            .storage
            .prepare_protocol_object(
                &evidence_context,
                evidence_slot,
                &evidence_prefix,
                evidence.to_bytes(),
            )
            .expect("prepare evidence");
        store
            .storage
            .create_protocol_object(&prepared_evidence)
            .await
            .expect("create evidence");
        let evidence_ref =
            ReclaimEvidenceRef::from_evidence(&evidence, prepared_evidence.reference().clone());
        let authorization = ReclaimAuthorization::signed(
            store.root.store_root_hash,
            package,
            evidence_ref,
            StoreReclaimAuthority {
                membership: activated.membership_state,
                owner_grant: store.protocol_root.descriptor.founder_grant.clone(),
            },
            &store.signer,
        );
        let authorization_context = ProtocolObjectContext::signed_plaintext(
            store.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimAuthorization,
        );
        let authorization_prefix =
            reclaim_authorization_semantic_prefix(authorization.authorization_hash());
        let authorization_slot = store
            .storage
            .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
            .await
            .expect("allocate authorization slot");
        let prepared_authorization = store
            .storage
            .prepare_protocol_object(
                &authorization_context,
                authorization_slot,
                &authorization_prefix,
                authorization.to_bytes(),
            )
            .expect("prepare authorization");
        store
            .storage
            .create_protocol_object(&prepared_authorization)
            .await
            .expect("create authorization");
        let authorization_ref = ReclaimAuthorizationRef::from_authorization(
            &authorization,
            prepared_authorization.reference().clone(),
        );

        let opened = super::super::store_objects::load_reclaim_authorization_ref(
            &store.storage,
            &store.root,
            &authorization_ref,
        )
        .await
        .expect("open exact reclaim authority graph");

        assert_eq!(opened.authorization.value, authorization);
        assert_eq!(opened.evidence.value, evidence);
        let mut relocated = authorization.clone();
        relocated.target.object =
            proof_object("store-v1/candidates/family/packages/device/1/another-package.pkg");
        assert!(authorization
            .verify(&keys::public_key_hex(&store.signer))
            .is_ok());
        assert!(matches!(
            relocated.verify(&keys::public_key_hex(&store.signer)),
            Err(StoreProtocolError::InvalidSignature)
        ));
    }
}
