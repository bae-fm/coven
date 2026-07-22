//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::circle_control::StoreMembershipStateRef;
use super::membership::{MembershipChain, MembershipGrantId, SerialMembershipState};
use super::storage::{
    CoordinationStorage, ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use super::store_commit::{
    snapshot_image_semantic_prefix, CommitFrontier, ObjectHash, StoreAckRef, StoreBatchCommitRef,
    StoreCommitCoord, StoreDeviceRegistration, StoreDeviceRegistrationRef, StorePackageRef,
    StoreProtocolError, StoreRootRef, StoreSnapshotLocator, STORE_PROTOCOL_VERSION,
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
    pub covering_snapshot: StoreSnapshotLocator,
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
            || self.target.package.object == self.covering_snapshot.snapshot.object
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

impl<'a> ReclaimMembership<'a> {
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

    fn store_operation_preparation(
        self,
        coordination: Option<&'a dyn CoordinationStorage>,
    ) -> Result<super::store_outbound::StoreOperationPreparation<'a>, StoreReclaimError> {
        match (self, coordination) {
            (Self::MergeConcurrent { membership, .. }, None) => Ok(
                super::store_outbound::StoreOperationPreparation::MergeConcurrent { membership },
            ),
            (Self::Serial(_), Some(coordination)) => {
                Ok(super::store_outbound::StoreOperationPreparation::Serial { coordination })
            }
            (Self::MergeConcurrent { .. }, Some(_)) => Err(StoreReclaimError::PolicyMismatch(
                "Merge reclamation received Serial coordination".to_string(),
            )),
            (Self::Serial(_), None) => Err(StoreReclaimError::Outbound(
                super::store_outbound::StoreOutboundError::MissingSerialCoordination,
            )),
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
    let snapshot = Box::pin(choose_snapshot(
        storage,
        coordination,
        &root,
        membership,
        &registrations,
    ))
    .await?;

    let targets = exact_package_targets(storage, &root, &snapshot.snapshot.meta.coverage).await?;
    for (commit, package) in targets {
        if reclaim_target_is_recorded(db, &package).await? {
            continue;
        }
        if db
            .store_package_is_retained_for_replay(package.clone(), commit.clone())
            .await?
        {
            continue;
        }
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
                covering_snapshot: StoreSnapshotLocator {
                    author_registration: snapshot.snapshot.meta.author_registration.clone(),
                    snapshot: snapshot.snapshot.reference.clone(),
                },
                acknowledgements: snapshot.acknowledgements.clone(),
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
        membership.store_operation_preparation(coordination)?,
        device_id,
        identity_signer,
    ))
    .await?;
    let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
        StoreReclaimError::Authorization(
            "Store reclaim authorization requires an active Owner grant".to_string(),
        )
    })?;
    let evidence = ReclaimEvidence::signed(root.store_root_hash, claim, identity_signer)
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    verify_store_package_reclaim_evidence(storage, coordination, root, &evidence).await?;
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
                    Box::pin(execute_reclaim_delete(
                        db,
                        storage,
                        coordination,
                        operation,
                    ))
                    .await?;
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
    coordination: Option<&dyn CoordinationStorage>,
    operation: super::store_reclaim_journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    let super::store_reclaim_journal::DurableStoreReclaimOperation::Authorized {
        authorization,
        activation,
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
    let verified = verify_authorized_store_package_reclaim(
        db,
        storage,
        coordination,
        &root,
        authorization,
        activation,
    )
    .await?;
    let target = verified.target;
    if db
        .store_package_is_retained_for_replay(target.package.clone(), target.activation.clone())
        .await?
    {
        return Err(StoreReclaimError::Authorization(
            "Store package remains retained for accepted replay".to_string(),
        ));
    }
    storage
        .delete_protocol_object(&target.package.object)
        .await
        .map_err(|source| StoreReclaimError::Delete {
            commit_hash: target.activation.commit_hash,
            source,
        })?;
    verify_reclaim_target_absent(storage, &root, &target).await?;
    db.mark_store_reclaim_target_absent(operation, target.package)
        .await?;
    Ok(())
}

struct VerifiedAuthorizedStorePackageReclaim {
    target: StorePackageReclaimTarget,
}

async fn verify_authorized_store_package_reclaim(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    authorization_ref: &ReclaimAuthorizationRef,
    activation: &super::store_reclaim_journal::ReclaimCommitActivation,
) -> Result<VerifiedAuthorizedStorePackageReclaim, StoreReclaimError> {
    let opened =
        super::store_objects::load_reclaim_authorization_ref(storage, root, authorization_ref)
            .await?;
    verify_reclaim_authorization_activation(
        db,
        storage,
        coordination,
        root,
        authorization_ref,
        activation,
    )
    .await?;
    let verified =
        verify_store_package_reclaim_evidence(storage, coordination, root, &opened.evidence.value)
            .await?;
    Ok(VerifiedAuthorizedStorePackageReclaim {
        target: verified.target,
    })
}

async fn verify_reclaim_authorization_activation(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    authorization: &ReclaimAuthorizationRef,
    activation: &super::store_reclaim_journal::ReclaimCommitActivation,
) -> Result<(), StoreReclaimError> {
    activation
        .validate()
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let commit_ref = activation.commit();
    let (commit_value, author) =
        super::store_pull::load_commit_with_author(storage, root, commit_ref)
            .await
            .map_err(StoreReclaimError::Object)?;
    if commit_value.reclaim_authorization() != Some(authorization) {
        return Err(StoreReclaimError::Authorization(
            "reclaim activation commit names another authorization".to_string(),
        ));
    }
    match activation {
        super::store_reclaim_journal::ReclaimCommitActivation::MergeConcurrent { commit, head } => {
            if coordination.is_some() {
                return Err(StoreReclaimError::PolicyMismatch(
                    "Merge reclaim activation received Serial coordination".to_string(),
                ));
            }
            let opened = super::store_objects::load_head_ref(
                storage,
                root.store_root_hash,
                head,
                &author,
                commit,
            )
            .await?;
            if opened.value.commit != *commit {
                return Err(StoreReclaimError::Authorization(
                    "Merge reclaim head activates another commit".to_string(),
                ));
            }
            let (_, accepted_head) = super::store_outbound::exact_next_announcement_slot(
                storage,
                root,
                &commit_value.author_registration,
                &author,
                Some(commit),
            )
            .await?;
            if accepted_head.as_ref() != Some(head) {
                return Err(StoreReclaimError::Authorization(
                    "Merge reclaim activation head is not the exact accepted stream position"
                        .to_string(),
                ));
            }
            super::store_pull::verify_merge_commit_currently_materialized(db, storage, root, commit)
                .await
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
        }
        super::store_reclaim_journal::ReclaimCommitActivation::Serial { commit } => {
            let coordination = coordination.ok_or_else(|| {
                StoreReclaimError::PolicyMismatch(
                    "Serial reclaim activation requires coordination".to_string(),
                )
            })?;
            super::store_pull::observe_serial_successors_after(
                storage,
                coordination,
                root,
                &super::store_commit::StoreSerialPredecessor::Commit(commit.clone()),
            )
            .await
            .map(|_| ())
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
        }
    }
}

struct VerifiedStorePackageReclaimEvidence {
    target: StorePackageReclaimTarget,
}

async fn verify_store_package_reclaim_evidence(
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    evidence: &ReclaimEvidence,
) -> Result<VerifiedStorePackageReclaimEvidence, StoreReclaimError> {
    evidence
        .verify()
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    if evidence.store_root_hash != root.store_root_hash {
        return Err(StoreReclaimError::Authorization(
            "reclaim evidence belongs to another Store root".to_string(),
        ));
    }
    let claim = &evidence.claim;
    let author = super::store_objects::load_registration_ref(
        storage,
        root,
        &claim.covering_snapshot.author_registration,
    )
    .await?;
    let (reference, metadata) = super::store_snapshot::load_store_snapshot_ref(
        storage,
        root,
        &claim.covering_snapshot.author_registration,
        &author.value,
        &claim.covering_snapshot.snapshot,
    )
    .await
    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let snapshot = crate::database::PublishedStoreSnapshot {
        reference,
        successor_slot: metadata.successor.next_slot.clone(),
        meta: metadata,
    };
    let authority = match super::store_pull::verify_store_snapshot_stability(
        storage,
        coordination,
        root,
        &snapshot,
    )
    .await
    {
        Ok(stability) => stability.into_authority(),
        Err(super::store_pull::StorePullError::SnapshotNotStable { member, device_id }) => {
            return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
        }
        Err(
            super::store_pull::StorePullError::SnapshotAuthorInactive
            | super::store_pull::StorePullError::SnapshotAuthorNotOwner,
        ) => return Err(StoreReclaimError::NoSnapshot),
        Err(error) => return Err(StoreReclaimError::Authorization(error.to_string())),
    };
    let mut expected_acknowledgements = authority
        .acknowledgements
        .values()
        .map(|acknowledgement| {
            acknowledgement
                .latest()
                .map(|(reference, _)| reference.clone())
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "snapshot stability acknowledgement proof chain is empty".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    expected_acknowledgements.sort();
    if claim.acknowledgements != expected_acknowledgements {
        return Err(StoreReclaimError::Authorization(
            "reclaim evidence acknowledgements differ from the activated snapshot stability proof"
                .to_string(),
        ));
    }
    let (commit, _) =
        super::store_pull::load_commit_with_author(storage, root, &claim.target.activation).await?;
    if commit.store_package() != Some(&claim.target.package)
        || !snapshot_covers_target(
            storage,
            root,
            &snapshot.meta.coverage,
            &claim.target.activation,
        )
        .await?
    {
        return Err(StoreReclaimError::Authorization(
            "reclaim target is not the exact Store package covered by its snapshot".to_string(),
        ));
    }
    Ok(VerifiedStorePackageReclaimEvidence {
        target: claim.target.clone(),
    })
}

async fn snapshot_covers_target(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    target: &StoreBatchCommitRef,
) -> Result<bool, StoreReclaimError> {
    let covering = match (coverage, &target.coord) {
        (
            CommitFrontier::MergeConcurrent(frontier),
            StoreCommitCoord::MergeConcurrent { stream_id, .. },
        ) => frontier.get(stream_id),
        (CommitFrontier::Serial(covering), StoreCommitCoord::Serial { .. }) => covering.as_ref(),
        _ => None,
    };
    match covering {
        Some(covering) => position_covers(storage, root, covering, target).await,
        None => Ok(false),
    }
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
            super::store_outbound::StoreOperationPublicationMode::from_dependencies(
                db.write_policy(),
                coordination,
            )?,
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
                nonactivation,
                ..
            } => {
                let plan = Box::pin(super::store_outbound::prepare_store_operation_commit(
                    db,
                    storage,
                    membership.store_operation_preparation(coordination)?,
                    device_id,
                    identity_signer,
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
                    *nonactivation,
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
        membership.store_operation_preparation(coordination)?,
        device_id,
        identity_signer,
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
    target: &StorePackageReclaimTarget,
) -> Result<(), StoreReclaimError> {
    let stream_id = match &target.activation.coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => super::store_commit::SERIAL_STREAM_ID.to_string(),
    };
    let prefix = super::store_commit::package_semantic_prefix(
        target.package.candidate_family,
        &stream_id,
        target.activation.coord.sequence(),
        target.package.content_hash,
    );
    let context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    match storage
        .read_protocol_object(&context, &target.package.object, &prefix)
        .await
    {
        Err(StorageError::NotFound(_)) => Ok(()),
        Ok(_) => Err(StoreReclaimError::Authorization(
            "reclaim target remains readable after exact deletion".to_string(),
        )),
        Err(error) => Err(StoreReclaimError::Storage(error)),
    }
}

struct VerifiedReclaimSnapshot {
    snapshot: crate::database::PublishedStoreSnapshot,
    acknowledgements: Vec<StoreAckRef>,
}

async fn choose_snapshot(
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    membership: ReclaimMembership<'_>,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<VerifiedReclaimSnapshot, StoreReclaimError> {
    let mut authorized = Vec::new();
    for (registration_ref, registration) in registrations {
        for snapshot in super::store_snapshot::load_store_snapshot_stream(
            storage,
            root,
            registration_ref,
            registration,
        )
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        {
            if snapshot.meta.coverage.policy() != membership.write_policy() {
                return Err(StoreReclaimError::PolicyMismatch(format!(
                    "snapshot coverage uses {:?}, Store uses {:?}",
                    snapshot.meta.coverage.policy(),
                    membership.write_policy()
                )));
            }
            authorized.push(snapshot);
        }
    }
    let selected = match super::store_snapshot::select_maximal_stable_store_snapshot(
        storage,
        coordination,
        root,
        authorized,
    )
    .await
    {
        Ok(Some(selected)) => selected,
        Ok(None) => return Err(StoreReclaimError::NoSnapshot),
        Err(super::store_pull::StorePullError::SnapshotNotStable { member, device_id }) => {
            return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
        }
        Err(
            super::store_pull::StorePullError::SnapshotAuthorInactive
            | super::store_pull::StorePullError::SnapshotAuthorNotOwner,
        ) => return Err(StoreReclaimError::NoSnapshot),
        Err(error) => return Err(StoreReclaimError::Authorization(error.to_string())),
    };
    let snapshot = selected.snapshot;
    let image = storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                root.store_root_hash,
                ProtocolObjectDomain::StoreSnapshotImage,
            ),
            &snapshot.meta.image.object,
            &snapshot_image_semantic_prefix(
                &snapshot.meta.author_registration.device_id.to_string(),
                snapshot.meta.image.image_hash,
            ),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if ObjectHash::digest(&image) != snapshot.meta.image.image_hash {
        return Err(StoreReclaimError::Authorization(
            "snapshot image differs from its signed exact reference".to_string(),
        ));
    }
    let authority = selected.stability.into_authority();
    let mut acknowledgements = authority
        .acknowledgements
        .values()
        .map(|acknowledgement| {
            acknowledgement
                .latest()
                .map(|(reference, _)| reference.clone())
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "snapshot stability acknowledgement proof chain is empty".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    acknowledgements.sort();
    Ok(VerifiedReclaimSnapshot {
        snapshot,
        acknowledgements,
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
    super::store_pull::commit_position_covers(storage, root, covering, covered)
        .await
        .map_err(|error| match error {
            super::store_pull::CommitCoverageError::Object(error) => {
                StoreReclaimError::Object(error)
            }
            super::store_pull::CommitCoverageError::MissingAncestry { commit_hash } => {
                StoreReclaimError::MissingAncestry { commit_hash }
            }
        })
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
    async fn reclaim_selects_an_older_stable_snapshot_over_a_newer_unacknowledged_snapshot() {
        let db = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "reclaim-stable-snapshot-selection",
            signer.clone(),
        )
        .await
        .expect("create Store");
        let first_changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('stable-snapshot-row', 'stable', NULL, \
                 '0000000001000-0000-stable-snapshot', '2026-01-01')",
            ],
        )
        .await;
        let first_commit = store
            .publish_changeset("founder", 1, &first_changeset, db.schema_version())
            .await
            .expect("publish first Store position");
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = first_commit.coord else {
            unreachable!("fixture uses Merge")
        };
        let first_coverage =
            CommitFrontier::MergeConcurrent(BTreeMap::from([(stream_id, first_commit.clone())]));
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load Store membership");
        let chain = membership
            .chain
            .as_ref()
            .expect("initialized Store has membership");
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            b"stable reclaim snapshot".to_vec(),
            first_coverage.clone(),
            &signer,
            Some(chain),
            &db,
        )
        .await
        .expect("publish stable snapshot");
        crate::sync::test_helpers::publish_store_ack_fixture(
            &db,
            &store.storage,
            None,
            first_coverage,
            &signer,
            Some(chain),
        )
        .await
        .expect("acknowledge stable snapshot");
        let stable = db
            .latest_local_store_snapshot()
            .await
            .expect("load stable snapshot")
            .expect("stable snapshot exists");

        let second_changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('unstable-snapshot-row', 'unstable', NULL, \
                 '0000000002000-0000-unstable-snapshot', '2026-01-01')",
            ],
        )
        .await;
        let second_commit = store
            .publish_changeset("founder", 3, &second_changeset, db.schema_version())
            .await
            .expect("publish second Store position");
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            b"unacknowledged reclaim snapshot".to_vec(),
            CommitFrontier::MergeConcurrent(BTreeMap::from([(stream_id, second_commit)])),
            &signer,
            Some(chain),
            &db,
        )
        .await
        .expect("publish unacknowledged snapshot");
        let registrations = db
            .activated_store_device_registration_records()
            .await
            .expect("load active registrations");

        let selected = choose_snapshot(
            &store.storage,
            None,
            &store.root,
            ReclaimMembership::MergeConcurrent {
                membership: chain,
                discovery_proof: membership.discovery_proof,
            },
            &registrations,
        )
        .await
        .expect("select the stable reclaim snapshot");

        assert_eq!(selected.snapshot.reference, stable.reference);
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
                covering_snapshot: StoreSnapshotLocator {
                    author_registration: founder_ref.clone(),
                    snapshot: super::super::store_commit::StoreSnapshotRef {
                        generation: 0,
                        snapshot_hash: ObjectHash::digest(b"covering snapshot"),
                        object: proof_object("store-v1/snapshots/founder/covering"),
                    },
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

        db.call(|connection| {
            let transaction = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            crate::database::Database::remove_retained_replay_ownership_from_snapshot_on(
                &transaction,
            )?;
            transaction.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("release retained replay package ownership");
        let mut authorization_activation = opened.evidence.value.claim.target.activation.clone();
        authorization_activation.coord = StoreCommitCoord::MergeConcurrent {
            stream_id: match &authorization_activation.coord {
                StoreCommitCoord::MergeConcurrent { stream_id, .. } => *stream_id,
                StoreCommitCoord::Serial { .. } => unreachable!("fixture uses Merge"),
            },
            sequence: authorization_activation.coord.sequence() + 1,
        };
        authorization_activation.commit_hash = ObjectHash::digest(b"reclaim authorization commit");
        authorization_activation.object =
            proof_object("store-v1/commits/reclaim-authorization.json");
        let operation =
            super::super::store_reclaim_journal::DurableStoreReclaimOperation::Authorized {
                authorization: receipt.authorization.clone(),
                activation:
                    super::super::store_reclaim_journal::ReclaimCommitActivation::merge_concurrent(
                        authorization_activation,
                        super::super::store_commit::StoreDeviceHeadRef {
                            head_hash: ObjectHash::digest(b"reclaim authorization head"),
                            object: proof_object("store-v1/heads/reclaim-authorization.json"),
                        },
                    )
                    .expect("valid reclaim activation"),
            };
        let deletion = execute_reclaim_delete(&db, &store.storage, None, operation).await;
        assert!(
            deletion.is_err(),
            "nonexistent snapshot and acknowledgement refs must not authorize deletion"
        );
        let target = &opened.evidence.value.claim.target;
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = target.activation.coord else {
            unreachable!("fixture uses Merge")
        };
        store
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                &target.package.object,
                &super::super::store_commit::package_semantic_prefix(
                    target.package.candidate_family,
                    &stream_id.to_string(),
                    target.activation.coord.sequence(),
                    target.package.content_hash,
                ),
            )
            .await
            .expect("unverified reclaim proof must leave its target readable");
    }

    #[tokio::test]
    async fn missing_or_retracted_merge_activation_blocks_reclaim_deletion() {
        let db = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "reclaim-activation-head",
            signer.clone(),
        )
        .await
        .expect("create Store");
        let changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('reclaim-head-row', 'reclaim', NULL, \
                 '0000000001000-0000-reclaim-head', '2026-01-01')",
            ],
        )
        .await;
        let target_activation = store
            .publish_changeset("founder", 1, &changeset, db.schema_version())
            .await
            .expect("publish target package activation");
        let (target_commit, _) = super::super::store_pull::load_commit_with_author(
            &store.storage,
            &store.root,
            &target_activation,
        )
        .await
        .expect("load target activation");
        let target_package = target_commit
            .store_package()
            .expect("target activation carries a Store package")
            .clone();
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = target_activation.coord else {
            unreachable!("fixture uses Merge")
        };
        let coverage = CommitFrontier::MergeConcurrent(BTreeMap::from([(
            stream_id,
            target_activation.clone(),
        )]));
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load reclaim membership");
        let chain = membership
            .chain
            .as_ref()
            .expect("initialized Store has membership");
        crate::sync::test_helpers::publish_snapshot_fixture(
            &store.storage,
            &store.root,
            b"reclaim activation snapshot".to_vec(),
            coverage.clone(),
            &signer,
            Some(chain),
            &db,
        )
        .await
        .expect("publish covering snapshot");
        crate::sync::test_helpers::publish_store_ack_fixture(
            &db,
            &store.storage,
            None,
            coverage,
            &signer,
            Some(chain),
        )
        .await
        .expect("publish covering acknowledgement");
        let snapshot = db
            .latest_local_store_snapshot()
            .await
            .expect("load covering snapshot")
            .expect("covering snapshot exists");
        let acknowledgement = db
            .latest_local_store_ack()
            .await
            .expect("load covering acknowledgement")
            .expect("covering acknowledgement exists")
            .reference;
        db.call(|connection| {
            let transaction = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            crate::database::Database::remove_retained_replay_ownership_from_snapshot_on(
                &transaction,
            )?;
            transaction.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("release target retained replay ownership");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load local device id")
            .expect("local device id exists");
        let reclaim_membership = ReclaimMembership::MergeConcurrent {
            membership: chain,
            discovery_proof: membership.discovery_proof,
        };
        prepare_reclaim_authorization(
            &db,
            &store.storage,
            None,
            &device_id,
            &signer,
            reclaim_membership,
            &store.root,
            StorePackageReclaimClaim {
                target: StorePackageReclaimTarget {
                    package: target_package.clone(),
                    activation: target_activation.clone(),
                },
                covering_snapshot: StoreSnapshotLocator {
                    author_registration: snapshot.meta.author_registration.clone(),
                    snapshot: snapshot.reference.clone(),
                },
                acknowledgements: vec![acknowledgement],
            },
        )
        .await
        .expect("prepare reclaim authorization");
        let candidate = db
            .store_reclaim_operations()
            .await
            .expect("load reclaim candidate")
            .into_iter()
            .next()
            .expect("reclaim candidate exists");
        let prepared_candidate = candidate
            .candidate()
            .expect("reclaim operation has a candidate");
        let activation_head = prepared_candidate
            .merge_head_ref()
            .expect("Merge reclaim candidate has an activation head");
        let (_, activation_head_prepared) = prepared_candidate
            .merge_publication_for_test()
            .expect("Merge reclaim candidate has a prepared activation head");
        let activation_head_prepared = activation_head_prepared.clone();
        drive_reclaim_candidate(
            &db,
            &store.storage,
            None,
            &device_id,
            &signer,
            reclaim_membership,
            candidate,
        )
        .await
        .expect("activate reclaim authorization");
        store
            .storage
            .delete_protocol_object(&activation_head.object)
            .await
            .expect("remove reclaim activation head");
        let authorized = db
            .store_reclaim_operations()
            .await
            .expect("load activated reclaim")
            .into_iter()
            .next()
            .expect("activated reclaim exists");

        let deletion = execute_reclaim_delete(&db, &store.storage, None, authorized.clone()).await;

        assert!(
            deletion.is_err(),
            "a reclaim authorization without its exact Merge activation head must not delete"
        );
        store
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                &target_package.object,
                &super::super::store_commit::package_semantic_prefix(
                    target_package.candidate_family,
                    &stream_id.to_string(),
                    target_activation.coord.sequence(),
                    target_package.content_hash,
                ),
            )
            .await
            .expect("missing activation authority leaves target readable");

        store
            .storage
            .create_protocol_object(&activation_head_prepared)
            .await
            .expect("restore exact reclaim activation head");
        let activation_commit = match &authorized {
            super::super::store_reclaim_journal::DurableStoreReclaimOperation::Authorized {
                activation,
                ..
            } => activation.commit().clone(),
            _ => unreachable!("fixture has an activated reclaim"),
        };
        let StoreCommitCoord::MergeConcurrent {
            stream_id: activation_stream,
            sequence: activation_sequence,
        } = activation_commit.coord
        else {
            unreachable!("fixture uses Merge")
        };
        db.call(move |connection| {
            let removed = connection
                .execute(
                    "DELETE FROM materialized_commits \
                     WHERE device_id = ?1 AND seq = ?2 AND commit_ref = ?3",
                    rusqlite::params![
                        activation_stream.to_string(),
                        i64::try_from(activation_sequence).expect("activation sequence fits i64"),
                        serde_json::to_string(&activation_commit).expect("serialize activation"),
                    ],
                )
                .map_err(crate::database::DbError::from)?;
            if removed != 1 {
                return Err(crate::database::DbError::Message(
                    "reclaim activation materialization was not removed".to_string(),
                ));
            }
            Ok(())
        })
        .await
        .expect("retract reclaim activation materialization");

        assert!(
            execute_reclaim_delete(&db, &store.storage, None, authorized)
                .await
                .is_err(),
            "a retracted Merge reclaim activation must not delete"
        );
        store
            .storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                &target_package.object,
                &super::super::store_commit::package_semantic_prefix(
                    target_package.candidate_family,
                    &stream_id.to_string(),
                    target_activation.coord.sequence(),
                    target_package.content_hash,
                ),
            )
            .await
            .expect("retracted activation authority leaves target readable");
    }

    #[tokio::test]
    async fn serial_reclaim_activation_requires_the_live_coordinated_chain() {
        let db = crate::sync::test_helpers::open_serial_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "serial-reclaim-activation",
            signer.clone(),
        )
        .await
        .expect("create Serial Store");
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load Serial device id")
            .expect("Serial device id exists");
        let changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('serial-reclaim-row', 'reclaim', NULL, \
                 '0000000001000-0000-serial-reclaim', '2026-01-01')",
            ],
        )
        .await;
        db.enqueue_store_changeset_for_test(changeset)
            .await
            .expect("enqueue Serial target package");
        let (_directory, store_dir) = crate::sync::test_helpers::temp_store_dir();
        super::super::store_outbound::prepare_pending_store_write_with_coordination(
            &db,
            &store.storage,
            Some(&store.storage),
            &device_id,
            "2026-07-16T00:00:00Z",
            &signer,
            &store_dir,
            None,
        )
        .await
        .expect("prepare Serial target package");
        assert_eq!(
            super::super::store_outbound::drain_store_writes_with_coordination(
                &db,
                &store.storage,
                Some(&store.storage),
            )
            .await
            .expect("publish Serial target package"),
            1
        );
        let target_activation = db
            .latest_local_store_position()
            .await
            .expect("load Serial target activation")
            .expect("Serial target activation exists");
        let (target_commit, _) = super::super::store_pull::load_commit_with_author(
            &store.storage,
            &store.root,
            &target_activation,
        )
        .await
        .expect("load Serial target commit");
        let target = target_commit
            .store_package()
            .expect("Serial target commit carries a Store package")
            .clone();
        let (founder_ref, _, _) = store
            .founder_device_authority()
            .await
            .expect("load Serial founder authority");
        let plan = super::super::store_outbound::prepare_store_operation_commit(
            &db,
            &store.storage,
            super::super::store_outbound::StoreOperationPreparation::Serial {
                coordination: &store.storage,
            },
            &device_id,
            &signer,
        )
        .await
        .expect("prepare Serial reclaim activation");
        let evidence = ReclaimEvidence::signed(
            store.root.store_root_hash,
            StorePackageReclaimClaim {
                target: StorePackageReclaimTarget {
                    package: target.clone(),
                    activation: target_activation,
                },
                covering_snapshot: StoreSnapshotLocator {
                    author_registration: founder_ref.clone(),
                    snapshot: super::super::store_commit::StoreSnapshotRef {
                        generation: 0,
                        snapshot_hash: ObjectHash::digest(b"Serial reclaim snapshot"),
                        object: proof_object("store-v1/snapshots/serial/reclaim.json"),
                    },
                },
                acknowledgements: vec![StoreAckRef {
                    registration: founder_ref,
                    sequence: 1,
                    ack_hash: ObjectHash::digest(b"Serial reclaim acknowledgement"),
                    object: proof_object("store-v1/acks/serial/reclaim.json"),
                }],
            },
            &signer,
        )
        .expect("sign Serial reclaim evidence");
        let evidence_context = ProtocolObjectContext::store_encrypted(
            store.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimEvidence,
        );
        let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
        let evidence_slot = store
            .storage
            .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
            .await
            .expect("allocate Serial reclaim evidence");
        let evidence_prepared = store
            .storage
            .prepare_protocol_object(
                &evidence_context,
                evidence_slot,
                &evidence_prefix,
                evidence.to_bytes(),
            )
            .expect("prepare Serial reclaim evidence");
        store
            .storage
            .create_protocol_object(&evidence_prepared)
            .await
            .expect("publish Serial reclaim evidence");
        let evidence_ref =
            ReclaimEvidenceRef::from_evidence(&evidence, evidence_prepared.reference().clone());
        let authorization = ReclaimAuthorization::signed(
            store.root.store_root_hash,
            target,
            evidence_ref,
            StoreReclaimAuthority {
                membership: plan.membership_state().clone(),
                owner_grant: plan
                    .owner_grant()
                    .expect("Serial reclaim plan has an Owner grant")
                    .clone(),
            },
            &signer,
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
            .expect("allocate Serial reclaim authorization");
        let authorization_prepared = store
            .storage
            .prepare_protocol_object(
                &authorization_context,
                authorization_slot,
                &authorization_prefix,
                authorization.to_bytes(),
            )
            .expect("prepare Serial reclaim authorization");
        store
            .storage
            .create_protocol_object(&authorization_prepared)
            .await
            .expect("publish Serial reclaim authorization");
        let authorization_ref = ReclaimAuthorizationRef::from_authorization(
            &authorization,
            authorization_prepared.reference().clone(),
        );
        let candidate = super::super::store_outbound::prepare_store_operation_candidate(
            &db,
            &store.storage,
            plan,
            super::super::store_outbound::StoreOperationBatch::ReclaimAuthorization(Box::new(
                authorization_ref.clone(),
            )),
        )
        .await
        .expect("prepare Serial reclaim candidate");
        let (base_head, accepted_head) = candidate
            .serial_publication_for_test()
            .expect("Serial reclaim candidate has coordinated publication");
        let base_head = base_head.clone();
        let accepted_head = accepted_head.clone();
        store
            .storage
            .create_protocol_object(&candidate.prepared)
            .await
            .expect("publish Serial reclaim commit");
        let accepted = CoordinationStorage::replace_head(
            &store.storage,
            super::super::store_commit::serial_head_key(),
            &base_head.version,
            &accepted_head.to_bytes(),
        )
        .await
        .expect("activate Serial reclaim commit");
        let activation = super::super::store_reclaim_journal::ReclaimCommitActivation::serial(
            candidate.reference,
        )
        .expect("valid Serial reclaim activation");

        verify_reclaim_authorization_activation(
            &db,
            &store.storage,
            Some(&store.storage),
            &store.root,
            &authorization_ref,
            &activation,
        )
        .await
        .expect("live coordinated chain accepts reclaim activation");

        CoordinationStorage::replace_head(
            &store.storage,
            super::super::store_commit::serial_head_key(),
            &accepted.version,
            &base_head.bytes,
        )
        .await
        .expect("replace Serial head with branch omitting reclaim activation");
        assert!(
            verify_reclaim_authorization_activation(
                &db,
                &store.storage,
                Some(&store.storage),
                &store.root,
                &authorization_ref,
                &activation,
            )
            .await
            .is_err(),
            "a Serial reclaim commit absent from the live coordinated chain is not authority"
        );
    }
}
