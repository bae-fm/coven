//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle_control::StoreMembershipStateRef;
use super::membership::{MemberRole, MembershipChain, MembershipGrantId, SerialMembershipState};
use super::storage::{
    CoordinationStorage, ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
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
const RECLAIM_RECEIPT_DOMAIN: &[u8] = b"coven.store-reclaim-receipt.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageReclaimTarget {
    pub package: StorePackageRef,
    pub activation: StoreBatchCommitRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageReclaimClaim {
    pub target: StorePackageReclaimTarget,
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
        if self.target.package.object == self.target.activation.object
            || self.target.package.object == self.covering_snapshot.object
            || self
                .acknowledgements
                .iter()
                .any(|acknowledgement| acknowledgement.object == self.target.package.object)
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
    pub target: Box<StorePackageReclaimTarget>,
    pub object: ExactObjectRef,
}

impl ReclaimEvidenceRef {
    pub fn from_evidence(evidence: &ReclaimEvidence, object: ExactObjectRef) -> Self {
        Self {
            evidence_hash: evidence.evidence_hash(),
            target: Box::new(evidence.claim.target.clone()),
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
        if evidence.claim.target != *self.target {
            return Err(StoreProtocolError::Malformed(
                "reclaim target differs from its exact evidence reference".to_string(),
            ));
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

    pub(crate) fn verify_identity(
        &self,
        authorization: &ReclaimAuthorization,
    ) -> Result<(), StoreProtocolError> {
        let actual = authorization.authorization_hash();
        if actual != self.authorization_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: self.authorization_hash,
                actual,
            });
        }
        if authorization.evidence != self.evidence
            || authorization.target != self.evidence.target.package
        {
            return Err(StoreProtocolError::Malformed(
                "reclaim authorization target or evidence differs from its exact reference"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn target(&self) -> &StorePackageRef {
        &self.evidence.target.package
    }

    pub(crate) fn target_activation(&self) -> &StoreBatchCommitRef {
        &self.evidence.target.activation
    }

    pub fn verify(
        &self,
        authorization: &ReclaimAuthorization,
        owner_pubkey: &str,
    ) -> Result<(), StoreProtocolError> {
        self.verify_identity(authorization)?;
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimReceiptRef {
    pub receipt_hash: ObjectHash,
    pub authorization: ReclaimAuthorizationRef,
    pub object: ExactObjectRef,
}

impl ReclaimReceiptRef {
    pub fn from_receipt(receipt: &ReclaimReceipt, object: ExactObjectRef) -> Self {
        Self {
            receipt_hash: receipt.receipt_hash(),
            authorization: receipt.authorization.clone(),
            object,
        }
    }

    pub(crate) fn verify_identity(
        &self,
        receipt: &ReclaimReceipt,
    ) -> Result<(), StoreProtocolError> {
        let actual = receipt.receipt_hash();
        if actual != self.receipt_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: self.receipt_hash,
                actual,
            });
        }
        if receipt.authorization != self.authorization {
            return Err(StoreProtocolError::Malformed(
                "reclaim receipt authorization differs from its exact reference".to_string(),
            ));
        }
        Ok(())
    }

    pub fn verify(
        &self,
        receipt: &ReclaimReceipt,
        executor: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.verify_identity(receipt)?;
        receipt.verify(executor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimReceipt {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub authorization: ReclaimAuthorizationRef,
    pub provider_admin_state: StoreMembershipStateRef,
    pub provider_admin_grant: super::provider::ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimReceiptSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    authorization: &'a ReclaimAuthorizationRef,
    provider_admin_state: &'a StoreMembershipStateRef,
    provider_admin_grant: &'a super::provider::ProviderAdminGrantId,
    executor: &'a StoreDeviceRegistrationRef,
}

impl ReclaimReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        authorization: ReclaimAuthorizationRef,
        provider_admin_state: StoreMembershipStateRef,
        provider_admin_grant: super::provider::ProviderAdminGrantId,
        executor: StoreDeviceRegistrationRef,
        executor_registration: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        executor.verify_registration(executor_registration)?;
        if executor_registration.store_root.store_root_hash != store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: store_root_hash,
                actual: executor_registration.store_root.store_root_hash,
            });
        }
        if keys::public_key_hex(signer) != executor_registration.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut receipt = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            authorization,
            provider_admin_state,
            provider_admin_grant,
            executor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &receipt.canonical_signed_bytes());
        receipt.signature = signature;
        Ok(receipt)
    }

    pub fn canonical_signed_bytes(&self) -> Vec<u8> {
        super::store_commit::domain_json(
            RECLAIM_RECEIPT_DOMAIN,
            &ReclaimReceiptSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                authorization: &self.authorization,
                provider_admin_state: &self.provider_admin_state,
                provider_admin_grant: &self.provider_admin_grant,
                executor: &self.executor,
            },
        )
    }

    pub fn receipt_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ReclaimReceipt serialization cannot fail")
    }

    pub fn verify(&self, executor: &StoreDeviceRegistration) -> Result<(), StoreProtocolError> {
        if self.version != STORE_PROTOCOL_VERSION {
            return Err(StoreProtocolError::UnsupportedVersion(self.version));
        }
        self.executor.verify_registration(executor)?;
        if executor.store_root.store_root_hash != self.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: self.store_root_hash,
                actual: executor.store_root.store_root_hash,
            });
        }
        if !keys::verify_signature_hex(
            &executor.device_signing_pubkey,
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

pub fn reclaim_receipt_semantic_prefix(receipt_hash: ObjectHash) -> String {
    format!("store-v1/reclaim/receipts/{receipt_hash}")
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
    #[error(transparent)]
    Database(#[from] crate::database::DbError),
    #[error(transparent)]
    Outbound(#[from] super::store_outbound::StoreOutboundError),
    #[error("Store reclaim journal: {0}")]
    Journal(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
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
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    identity_signer: &UserKeypair,
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
    let mut packages_deleted = Box::pin(resume_store_reclaim_operations(
        db,
        storage,
        coordination,
        device_id,
        identity_signer,
        membership,
    ))
    .await?;
    if !membership.is_owner(&keys::public_key_hex(identity_signer)) {
        return Ok(StoreReclaimResult {
            packages_deleted,
            physical_copies_deleted: packages_deleted,
        });
    }
    let registrations = db
        .activated_store_device_registration_records()
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let snapshot = Box::pin(choose_snapshot(storage, &root, membership, &registrations)).await?;
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
    for (commit, package) in targets {
        if reclaim_target_is_recorded(db, &package).await? {
            continue;
        }
        let acknowledgements =
            active_acknowledgement_refs(membership, &registrations, &acknowledgements);
        Box::pin(prepare_reclaim_authorization(
            db,
            storage,
            coordination,
            device_id,
            identity_signer,
            membership,
            &root,
            StorePackageReclaimClaim {
                target: StorePackageReclaimTarget {
                    package,
                    activation: commit,
                },
                covering_snapshot: snapshot.reference.clone(),
                acknowledgements,
            },
        ))
        .await?;
    }
    packages_deleted = packages_deleted
        .checked_add(
            Box::pin(resume_store_reclaim_operations(
                db,
                storage,
                coordination,
                device_id,
                identity_signer,
                membership,
            ))
            .await?,
        )
        .ok_or_else(|| {
            StoreReclaimError::Authorization("reclaimed package count exceeded u64".to_string())
        })?;
    Ok(StoreReclaimResult {
        packages_deleted,
        physical_copies_deleted: packages_deleted,
    })
}

fn active_acknowledgement_refs(
    membership: ReclaimMembership<'_>,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
    acknowledgements: &BTreeMap<super::store_commit::StoreDeviceId, (StoreAckRef, StoreAck)>,
) -> Vec<StoreAckRef> {
    let active = membership
        .current_members()
        .into_iter()
        .map(|(pubkey, _)| pubkey)
        .collect::<BTreeSet<_>>();
    let mut refs = registrations
        .iter()
        .filter(|(_, registration)| active.contains(&registration.author_pubkey))
        .filter_map(|(_, registration)| {
            acknowledgements
                .get(&registration.device_id)
                .map(|(reference, _)| reference.clone())
        })
        .collect::<Vec<_>>();
    refs.sort();
    refs
}

async fn reclaim_target_is_recorded(
    db: &crate::database::Database,
    target: &StorePackageRef,
) -> Result<bool, StoreReclaimError> {
    for operation in db.store_reclaim_operations().await? {
        if operation.authorization().target() == target {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn prepare_reclaim_authorization(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: ReclaimMembership<'_>,
    root: &StoreRootRef,
    claim: StorePackageReclaimClaim,
) -> Result<(), StoreReclaimError> {
    let plan = Box::pin(super::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        device_id,
        identity_signer,
        match membership {
            ReclaimMembership::MergeConcurrent { membership, .. } => Some(membership),
            ReclaimMembership::Serial(_) => None,
        },
    ))
    .await?;
    let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
        StoreReclaimError::Authorization(
            "Store reclaim authorization requires an active Owner grant".to_string(),
        )
    })?;
    let evidence = ReclaimEvidence::signed(root.store_root_hash, claim, identity_signer)
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let evidence_context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimEvidence,
    );
    let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
    let evidence_slot = storage
        .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
        .await?;
    let evidence_prepared = storage.prepare_protocol_object(
        &evidence_context,
        evidence_slot,
        &evidence_prefix,
        evidence.to_bytes(),
    )?;
    let evidence_ref =
        ReclaimEvidenceRef::from_evidence(&evidence, evidence_prepared.reference().clone());
    let authorization = ReclaimAuthorization::signed(
        root.store_root_hash,
        evidence.claim.target.package.clone(),
        evidence_ref.clone(),
        StoreReclaimAuthority {
            membership: plan.membership_state().clone(),
            owner_grant,
        },
        identity_signer,
    );
    let authorization_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimAuthorization,
    );
    let authorization_prefix =
        reclaim_authorization_semantic_prefix(authorization.authorization_hash());
    let authorization_slot = storage
        .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
        .await?;
    let authorization_prepared = storage.prepare_protocol_object(
        &authorization_context,
        authorization_slot,
        &authorization_prefix,
        authorization.to_bytes(),
    )?;
    let authorization_ref = ReclaimAuthorizationRef::from_authorization(
        &authorization,
        authorization_prepared.reference().clone(),
    );
    let candidate = Box::pin(super::store_outbound::prepare_store_operation_candidate(
        db,
        storage,
        plan,
        super::store_outbound::StoreOperationBatch::ReclaimAuthorization(Box::new(
            authorization_ref.clone(),
        )),
    ))
    .await?;
    let operation =
        super::store_reclaim_journal::DurableStoreReclaimOperation::AuthorizationCandidate {
            object: Box::new(
                super::store_reclaim_journal::DurableStoreReclaimObject::Authorization {
                    evidence_ref,
                    evidence,
                    evidence_prepared,
                    authorization_ref,
                    authorization,
                    authorization_prepared,
                },
            ),
            candidate: Box::new(candidate),
        };
    Box::pin(db.begin_store_reclaim_operation(operation)).await?;
    Ok(())
}

async fn resume_store_reclaim_operations(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: ReclaimMembership<'_>,
) -> Result<u64, StoreReclaimError> {
    let mut completed = 0_u64;
    loop {
        let operations = db.store_reclaim_operations().await?;
        let mut progressed = false;
        for operation in operations {
            match &operation {
                super::store_reclaim_journal::DurableStoreReclaimOperation::AuthorizationCandidate {
                    ..
                }
                | super::store_reclaim_journal::DurableStoreReclaimOperation::ReceiptCandidate {
                    ..
                } => {
                    Box::pin(drive_reclaim_candidate(
                        db,
                        storage,
                        coordination,
                        device_id,
                        identity_signer,
                        membership,
                        operation,
                    ))
                    .await?;
                    progressed = true;
                }
                super::store_reclaim_journal::DurableStoreReclaimOperation::AuthorizationReplacing {
                    ..
                }
                | super::store_reclaim_journal::DurableStoreReclaimOperation::ReceiptReplacing {
                    ..
                } => {
                    Box::pin(finish_reclaim_candidate_replacement(
                        db, storage, operation,
                    ))
                    .await?;
                    progressed = true;
                }
                super::store_reclaim_journal::DurableStoreReclaimOperation::Authorized { .. } => {
                    Box::pin(execute_reclaim_delete(db, storage, operation)).await?;
                    completed = completed.checked_add(1).ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaimed package count exceeded u64".to_string(),
                        )
                    })?;
                    progressed = true;
                }
                super::store_reclaim_journal::DurableStoreReclaimOperation::AbsentVerified {
                    ..
                } => {
                    Box::pin(prepare_reclaim_receipt(
                        db,
                        storage,
                        coordination,
                        device_id,
                        identity_signer,
                        membership,
                        operation,
                    ))
                    .await?;
                    progressed = true;
                }
                super::store_reclaim_journal::DurableStoreReclaimOperation::Completed { .. } => {}
            }
        }
        if !progressed {
            return Ok(completed);
        }
    }
}

async fn execute_reclaim_delete(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    operation: super::store_reclaim_journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    let super::store_reclaim_journal::DurableStoreReclaimOperation::Authorized {
        authorization,
        ..
    } = &operation
    else {
        return Err(StoreReclaimError::Authorization(
            "only an authorized reclaim can delete its target".to_string(),
        ));
    };
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| StoreReclaimError::Authorization("Store root is absent".to_string()))?;
    let opened =
        super::store_objects::load_reclaim_authorization_ref(storage, &root, authorization).await?;
    let target = opened.authorization.value.target.clone();
    storage
        .delete_protocol_object(&target.object)
        .await
        .map_err(|source| StoreReclaimError::Delete {
            commit_hash: opened.evidence.value.claim.target.activation.commit_hash,
            source,
        })?;
    verify_reclaim_target_absent(storage, &root, &opened).await?;
    db.mark_store_reclaim_target_absent(operation, target)
        .await?;
    Ok(())
}

async fn drive_reclaim_candidate(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: ReclaimMembership<'_>,
    mut operation: super::store_reclaim_journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    loop {
        let (object, candidate) = match &operation {
            super::store_reclaim_journal::DurableStoreReclaimOperation::AuthorizationCandidate {
                object,
                candidate,
            }
            | super::store_reclaim_journal::DurableStoreReclaimOperation::ReceiptCandidate {
                object,
                candidate,
                ..
            } => (object.clone(), candidate.clone()),
            _ => {
                return Err(StoreReclaimError::Authorization(
                    "Store reclaim journal has no publication candidate".to_string(),
                ));
            }
        };
        Box::pin(object.create_exact_objects(storage))
            .await
            .map_err(|error| StoreReclaimError::Journal(error.to_string()))?;
        for remote in object
            .remote_objects(&candidate)
            .map_err(|error| StoreReclaimError::Journal(error.to_string()))?
        {
            if matches!(
                &remote,
                super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        record.identity.domain,
                        super::remote_object::RetainedAuthorityObjectDomain::ReclaimEvidence { .. }
                            | super::remote_object::RetainedAuthorityObjectDomain::ReclaimAuthorization { .. }
                            | super::remote_object::RetainedAuthorityObjectDomain::ReclaimReceipt { .. }
                    )
            ) {
                db.mark_reusable_retained_authority_uploaded(remote).await?;
            }
        }
        match Box::pin(super::store_outbound::publish_prepared_store_operation(
            db,
            storage,
            coordination,
            candidate,
        ))
        .await?
        {
            super::store_outbound::StoreOperationPublicationOutcome::Activated(_) => {
                return Ok(());
            }
            super::store_outbound::StoreOperationPublicationOutcome::RepreparedCandidate(
                replacement,
            ) => {
                operation =
                    Box::pin(db.replace_store_reclaim_candidate(operation, *replacement)).await?;
            }
            super::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate {
                proof,
                ..
            } => {
                let plan = Box::pin(super::store_outbound::prepare_store_operation_commit(
                    db,
                    storage,
                    coordination,
                    device_id,
                    identity_signer,
                    match membership {
                        ReclaimMembership::MergeConcurrent { membership, .. } => Some(membership),
                        ReclaimMembership::Serial(_) => None,
                    },
                ))
                .await?;
                let batch = match &*object {
                    super::store_reclaim_journal::DurableStoreReclaimObject::Authorization {
                        authorization_ref,
                        ..
                    } => super::store_outbound::StoreOperationBatch::ReclaimAuthorization(
                        Box::new(authorization_ref.clone()),
                    ),
                    super::store_reclaim_journal::DurableStoreReclaimObject::Receipt {
                        receipt_ref,
                        ..
                    } => super::store_outbound::StoreOperationBatch::ReclaimReceipt(Box::new(
                        receipt_ref.clone(),
                    )),
                };
                let replacement =
                    Box::pin(super::store_outbound::prepare_store_operation_candidate(
                        db, storage, plan, batch,
                    ))
                    .await?;
                operation = Box::pin(db.begin_store_reclaim_candidate_replacement(
                    operation,
                    replacement,
                    *proof,
                ))
                .await?;
                Box::pin(finish_reclaim_candidate_replacement(db, storage, operation)).await?;
                return Ok(());
            }
            super::store_outbound::StoreOperationPublicationOutcome::Nonactivated(_)
            | super::store_outbound::StoreOperationPublicationOutcome::Reprepared => {
                return Err(StoreReclaimError::Authorization(
                    "Store reclaim publication returned acknowledgement-only state".to_string(),
                ));
            }
        }
    }
}

async fn finish_reclaim_candidate_replacement(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    operation: super::store_reclaim_journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    for target in db
        .store_reclaim_replacement_cleanup_targets(operation.clone())
        .await?
    {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    db.complete_store_reclaim_candidate_replacement(operation)
        .await?;
    Ok(())
}

async fn prepare_reclaim_receipt(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: ReclaimMembership<'_>,
    operation: super::store_reclaim_journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    let super::store_reclaim_journal::DurableStoreReclaimOperation::AbsentVerified {
        authorization,
        target,
        ..
    } = &operation
    else {
        return Err(StoreReclaimError::Authorization(
            "only an authorized reclaim can be executed".to_string(),
        ));
    };
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| StoreReclaimError::Authorization("Store root is absent".to_string()))?;
    let opened =
        super::store_objects::load_reclaim_authorization_ref(storage, &root, authorization).await?;
    if &opened.authorization.value.target != target {
        return Err(StoreReclaimError::Authorization(
            "durable absent target differs from its signed authorization".to_string(),
        ));
    }

    let plan = Box::pin(super::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        device_id,
        identity_signer,
        match membership {
            ReclaimMembership::MergeConcurrent { membership, .. } => Some(membership),
            ReclaimMembership::Serial(_) => None,
        },
    ))
    .await?;
    let provider_admin = match membership {
        ReclaimMembership::MergeConcurrent { membership, .. } => {
            let super::membership::MembershipStatus::Resolved(resolved) = membership.status()
            else {
                return Err(StoreReclaimError::Authorization(
                    "provider execution requires resolved Store membership".to_string(),
                ));
            };
            resolved.provider_admin.combined_state().clone()
        }
        ReclaimMembership::Serial(_) => {
            db.serial_authorization_state()
                .await?
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "Serial provider administrator state is absent".to_string(),
                    )
                })?
                .provider_admin
        }
    };
    let provider_admin_grant = provider_admin
        .active()
        .into_iter()
        .find(|grant| provider_admin.authorizes(grant, plan.registration_ref()))
        .ok_or_else(|| {
            StoreReclaimError::Authorization(
                "local Store device is not an effective provider administrator".to_string(),
            )
        })?;
    let receipt = ReclaimReceipt::signed(
        root.store_root_hash,
        authorization.clone(),
        plan.membership_state().clone(),
        provider_admin_grant,
        plan.registration_ref().clone(),
        plan.registration(),
        plan.device_signer(),
    )
    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreReclaimReceipt,
    );
    let prefix = reclaim_receipt_semantic_prefix(receipt.receipt_hash());
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await?;
    let prepared = storage.prepare_protocol_object(&context, slot, &prefix, receipt.to_bytes())?;
    let receipt_ref = ReclaimReceiptRef::from_receipt(&receipt, prepared.reference().clone());
    let candidate = Box::pin(super::store_outbound::prepare_store_operation_candidate(
        db,
        storage,
        plan,
        super::store_outbound::StoreOperationBatch::ReclaimReceipt(Box::new(receipt_ref.clone())),
    ))
    .await?;
    Box::pin(db.begin_store_reclaim_receipt(
        operation,
        super::store_reclaim_journal::DurableStoreReclaimObject::Receipt {
            receipt_ref,
            receipt,
            receipt_prepared: prepared,
        },
        candidate,
    ))
    .await?;
    Ok(())
}

async fn verify_reclaim_target_absent(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    opened: &super::store_objects::VerifiedReclaimAuthorization,
) -> Result<(), StoreReclaimError> {
    let claim = &opened.evidence.value.claim;
    let stream_id = match &claim.target.activation.coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => super::store_commit::SERIAL_STREAM_ID.to_string(),
    };
    let prefix = super::store_commit::package_semantic_prefix(
        claim.target.package.candidate_family,
        &stream_id,
        claim.target.activation.coord.sequence(),
        claim.target.package.content_hash,
    );
    let context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    match storage
        .read_protocol_object(&context, &claim.target.package.object, &prefix)
        .await
    {
        Err(StorageError::NotFound(_)) => Ok(()),
        Ok(_) => Err(StoreReclaimError::Authorization(
            "reclaim target remains readable after exact deletion".to_string(),
        )),
        Err(error) => Err(StoreReclaimError::Storage(error)),
    }
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
) -> Result<BTreeMap<super::store_commit::StoreDeviceId, (StoreAckRef, StoreAck)>, StoreReclaimError>
{
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
) -> Result<Vec<(StoreAckRef, StoreAck)>, StoreReclaimError> {
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
        predecessor = Some(reference.clone());
        acknowledgements.push((reference, ack));
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
    latest: &BTreeMap<super::store_commit::StoreDeviceId, (StoreAckRef, StoreAck)>,
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
        let (_, ack) =
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
    async fn exact_reclaim_receipt_opens_its_authorization_and_encrypted_evidence() {
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
        let (founder_ref, founder, device_signer) = store
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
                target: StorePackageReclaimTarget {
                    package: package.clone(),
                    activation,
                },
                covering_snapshot: StoreSnapshotRef {
                    sequence: 1,
                    snapshot_hash: ObjectHash::digest(b"covering snapshot"),
                    object: proof_object("store-v1/snapshots/founder/covering"),
                },
                acknowledgements: vec![StoreAckRef {
                    registration: founder_ref.clone(),
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

        let receipt = ReclaimReceipt::signed(
            store.root.store_root_hash,
            authorization_ref,
            authorization.authority.membership.clone(),
            store
                .protocol_root
                .descriptor
                .founder_provider_admin
                .grant_id
                .clone(),
            founder_ref,
            &founder,
            &device_signer,
        )
        .expect("sign reclaim receipt");
        let receipt_context = ProtocolObjectContext::signed_plaintext(
            store.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimReceipt,
        );
        let receipt_prefix = reclaim_receipt_semantic_prefix(receipt.receipt_hash());
        let receipt_slot = store
            .storage
            .allocate_protocol_slot(&receipt_context, &receipt_prefix, ".json")
            .await
            .expect("allocate receipt slot");
        let prepared_receipt = store
            .storage
            .prepare_protocol_object(
                &receipt_context,
                receipt_slot,
                &receipt_prefix,
                receipt.to_bytes(),
            )
            .expect("prepare receipt");
        store
            .storage
            .create_protocol_object(&prepared_receipt)
            .await
            .expect("create receipt");
        let receipt_ref =
            ReclaimReceiptRef::from_receipt(&receipt, prepared_receipt.reference().clone());

        let opened_receipt = super::super::store_objects::load_reclaim_receipt_ref(
            &store.storage,
            &store.root,
            &receipt_ref,
        )
        .await
        .expect("open exact reclaim receipt graph");

        assert_eq!(opened_receipt.receipt.value, receipt);
        assert_eq!(
            opened_receipt.authorization.authorization.value,
            authorization
        );
        assert_eq!(opened_receipt.authorization.evidence.value, evidence);
        let mut reassigned = receipt.clone();
        reassigned.provider_admin_grant =
            super::super::provider::ProviderAdminGrantId(ObjectHash::digest(b"another admin"));
        assert!(matches!(
            reassigned.verify(&founder),
            Err(StoreProtocolError::InvalidSignature)
        ));
    }
}
