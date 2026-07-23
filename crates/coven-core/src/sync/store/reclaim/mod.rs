//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::database::StoreDatabase;
use super::AuthorizedStore;
use crate::keys::{self, UserKeypair};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MembershipChain, MembershipGrantId};
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::sync::store_commit::{
    snapshot_image_semantic_prefix, CommitFrontier, ObjectHash, StoreAckRef, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StorePackageRef, StoreProtocolError,
    StoreRootRef, StoreSnapshotLocator, STORE_PROTOCOL_VERSION,
};
use crate::sync::store_objects::StoreObjectError;

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
        crate::sync::store_commit::domain_json(
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
        crate::sync::store_commit::domain_json(
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
    pub provider_admin_grant: crate::sync::provider::ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimReceiptSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    authorization: &'a ReclaimAuthorizationRef,
    provider_admin_state: &'a StoreMembershipStateRef,
    provider_admin_grant: &'a crate::sync::provider::ProviderAdminGrantId,
    executor: &'a StoreDeviceRegistrationRef,
}

impl ReclaimReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        authorization: ReclaimAuthorizationRef,
        provider_admin_state: StoreMembershipStateRef,
        provider_admin_grant: crate::sync::provider::ProviderAdminGrantId,
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
        crate::sync::store_commit::domain_json(
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
    Outbound(#[from] super::StoreError),
    #[error("Store reclaim journal: {0}")]
    Journal(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("no authorized complete Store snapshot is available for reclamation")]
    NoSnapshot,
    #[error("snapshot authorization history is invalid: {0}")]
    Authorization(String),
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

impl AuthorizedStore<'_> {
    pub(crate) async fn reclaim_packages(
        &self,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<StoreReclaimResult, StoreReclaimError> {
        reclaim_store_packages(
            self.db(),
            self.storage(),
            device_id,
            identity,
            self.store_root().store_root_hash,
            self.membership(),
        )
        .await
    }
}

async fn reclaim_store_packages(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    store_root_hash: ObjectHash,
    membership: &MembershipChain,
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
        device_id,
        identity_signer,
        membership,
    ))
    .await?;
    if !membership.is_owner_now(&keys::public_key_hex(identity_signer)) {
        return Ok(StoreReclaimResult {
            packages_deleted,
            physical_copies_deleted: packages_deleted,
        });
    }
    let registrations = db
        .activated_store_device_registration_records()
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let snapshot = Box::pin(choose_snapshot(storage, &root, &registrations)).await?;

    let targets = exact_package_targets(storage, &root, &snapshot.snapshot.meta.coverage).await?;
    for (commit, package) in targets {
        if reclaim_target_is_recorded(db, &package).await? {
            continue;
        }
        if StoreDatabase::new(db)
            .store_package_is_retained_for_replay(package.clone(), commit.clone())
            .await?
        {
            continue;
        }
        Box::pin(prepare_reclaim_authorization(
            db,
            storage,
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
    for operation in StoreDatabase::new(db).store_reclaim_operations().await? {
        if operation.authorization().target() == target {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn prepare_reclaim_authorization(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    root: &StoreRootRef,
    claim: StorePackageReclaimClaim,
) -> Result<(), StoreReclaimError> {
    let plan = Box::pin(super::operations::prepare_plan(
        db,
        storage,
        membership,
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
    verify_store_package_reclaim_evidence(storage, root, &evidence).await?;
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
    let candidate = Box::pin(super::operations::prepare_candidate(
        db,
        storage,
        plan,
        super::operations::StoreOperationBatch::ReclaimAuthorization(Box::new(
            authorization_ref.clone(),
        )),
    ))
    .await?;
    let operation = journal::DurableStoreReclaimOperation::AuthorizationCandidate {
        object: Box::new(journal::DurableStoreReclaimObject::Authorization {
            evidence_ref,
            evidence,
            evidence_prepared,
            authorization_ref,
            authorization,
            authorization_prepared,
        }),
        candidate: Box::new(candidate),
    };
    Box::pin(StoreDatabase::new(db).begin_store_reclaim_operation(operation)).await?;
    Ok(())
}

async fn resume_store_reclaim_operations(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
) -> Result<u64, StoreReclaimError> {
    let mut completed = 0_u64;
    loop {
        let operations = StoreDatabase::new(db).store_reclaim_operations().await?;
        let mut progressed = false;
        for operation in operations {
            match &operation {
                journal::DurableStoreReclaimOperation::AuthorizationCandidate { .. }
                | journal::DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                    Box::pin(drive_reclaim_candidate(
                        db,
                        storage,
                        device_id,
                        identity_signer,
                        membership,
                        operation,
                    ))
                    .await?;
                    progressed = true;
                }
                journal::DurableStoreReclaimOperation::AuthorizationReplacing { .. }
                | journal::DurableStoreReclaimOperation::ReceiptReplacing { .. } => {
                    Box::pin(finish_reclaim_candidate_replacement(db, storage, operation)).await?;
                    progressed = true;
                }
                journal::DurableStoreReclaimOperation::Authorized { .. } => {
                    Box::pin(execute_reclaim_delete(db, storage, operation)).await?;
                    completed = completed.checked_add(1).ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaimed package count exceeded u64".to_string(),
                        )
                    })?;
                    progressed = true;
                }
                journal::DurableStoreReclaimOperation::AbsentVerified { .. } => {
                    Box::pin(prepare_reclaim_receipt(
                        db,
                        storage,
                        device_id,
                        identity_signer,
                        membership,
                        operation,
                    ))
                    .await?;
                    progressed = true;
                }
                journal::DurableStoreReclaimOperation::Completed { .. } => {}
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
    operation: journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    let journal::DurableStoreReclaimOperation::Authorized {
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
    let verified =
        verify_authorized_store_package_reclaim(db, storage, &root, authorization, activation)
            .await?;
    let target = verified.target;
    if StoreDatabase::new(db)
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
    StoreDatabase::new(db)
        .mark_store_reclaim_target_absent(operation, target.package)
        .await?;
    Ok(())
}

struct VerifiedAuthorizedStorePackageReclaim {
    target: StorePackageReclaimTarget,
}

async fn verify_authorized_store_package_reclaim(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    authorization_ref: &ReclaimAuthorizationRef,
    activation: &journal::ReclaimCommitActivation,
) -> Result<VerifiedAuthorizedStorePackageReclaim, StoreReclaimError> {
    let opened = crate::sync::store_objects::load_reclaim_authorization_ref(
        storage,
        root,
        authorization_ref,
    )
    .await?;
    verify_reclaim_authorization_activation(db, storage, root, authorization_ref, activation)
        .await?;
    let verified =
        verify_store_package_reclaim_evidence(storage, root, &opened.evidence.value).await?;
    Ok(VerifiedAuthorizedStorePackageReclaim {
        target: verified.target,
    })
}

async fn verify_reclaim_authorization_activation(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    authorization: &ReclaimAuthorizationRef,
    activation: &journal::ReclaimCommitActivation,
) -> Result<(), StoreReclaimError> {
    activation
        .validate()
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let commit_ref = activation.commit();
    let (commit_value, author) = super::pull::load_commit_with_author(storage, root, commit_ref)
        .await
        .map_err(StoreReclaimError::Object)?;
    if commit_value.reclaim_authorization() != Some(authorization) {
        return Err(StoreReclaimError::Authorization(
            "reclaim activation commit names another authorization".to_string(),
        ));
    }
    let commit = &activation.commit;
    let head = &activation.head;
    let opened = crate::sync::store_objects::load_head_ref(
        storage,
        root.store_root_hash,
        head,
        &author,
        commit,
    )
    .await?;
    if opened.value.commit != *commit {
        return Err(StoreReclaimError::Authorization(
            "reclaim head activates another commit".to_string(),
        ));
    }
    let (_, accepted_head) = super::operations::exact_next_announcement_slot(
        storage,
        root,
        &commit_value.author_registration,
        &author,
        Some(commit),
    )
    .await?;
    if accepted_head.as_ref() != Some(head) {
        return Err(StoreReclaimError::Authorization(
            "reclaim activation head is not the exact accepted stream position".to_string(),
        ));
    }
    super::pull::verify_merge_commit_currently_materialized(db, storage, root, commit)
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
}

struct VerifiedStorePackageReclaimEvidence {
    target: StorePackageReclaimTarget,
}

async fn verify_store_package_reclaim_evidence(
    storage: &dyn SyncStorage,
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
    let author = crate::sync::store_objects::load_registration_ref(
        storage,
        root,
        &claim.covering_snapshot.author_registration,
    )
    .await?;
    let (reference, metadata) = super::snapshot::load_store_snapshot_ref(
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
    let authority = match super::verify_store_snapshot_stability(storage, root, &snapshot).await {
        Ok(stability) => stability.into_authority(),
        Err(super::pull::StorePullError::SnapshotNotStable { member, device_id }) => {
            return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
        }
        Err(
            super::pull::StorePullError::SnapshotAuthorInactive
            | super::pull::StorePullError::SnapshotAuthorNotOwner,
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
        super::pull::load_commit_with_author(storage, root, &claim.target.activation).await?;
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
    let covering = coverage.0.get(&target.coord.stream_id);
    match covering {
        Some(covering) => position_covers(storage, root, covering, target).await,
        None => Ok(false),
    }
}

async fn drive_reclaim_candidate(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    mut operation: journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    loop {
        let (object, candidate) = match &operation {
            journal::DurableStoreReclaimOperation::AuthorizationCandidate { object, candidate }
            | journal::DurableStoreReclaimOperation::ReceiptCandidate {
                object, candidate, ..
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
                crate::sync::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                    if matches!(
                        record.identity.domain,
                        crate::sync::remote_object::RetainedAuthorityObjectDomain::ReclaimEvidence { .. }
                            | crate::sync::remote_object::RetainedAuthorityObjectDomain::ReclaimAuthorization { .. }
                            | crate::sync::remote_object::RetainedAuthorityObjectDomain::ReclaimReceipt { .. }
                    )
            ) {
                crate::sync::store::database::StoreDatabase::new(db)
                    .mark_reusable_retained_authority_uploaded(remote)
                    .await?;
            }
        }
        match Box::pin(super::operations::publish_prepared_store_operation(
            db, storage, candidate,
        ))
        .await?
        {
            super::operations::StoreOperationPublicationOutcome::Activated(_) => {
                return Ok(());
            }
            super::operations::StoreOperationPublicationOutcome::RepreparedCandidate(
                replacement,
            ) => {
                operation = Box::pin(
                    StoreDatabase::new(db).replace_store_reclaim_candidate(operation, *replacement),
                )
                .await?;
            }
            super::operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                nonactivation,
                ..
            } => {
                let plan = Box::pin(super::operations::prepare_plan(
                    db,
                    storage,
                    membership,
                    device_id,
                    identity_signer,
                ))
                .await?;
                let batch = match &*object {
                    journal::DurableStoreReclaimObject::Authorization {
                        authorization_ref, ..
                    } => super::operations::StoreOperationBatch::ReclaimAuthorization(Box::new(
                        authorization_ref.clone(),
                    )),
                    journal::DurableStoreReclaimObject::Receipt { receipt_ref, .. } => {
                        super::operations::StoreOperationBatch::ReclaimReceipt(Box::new(
                            receipt_ref.clone(),
                        ))
                    }
                };
                let replacement = Box::pin(super::operations::prepare_candidate(
                    db, storage, plan, batch,
                ))
                .await?;
                operation = Box::pin(
                    StoreDatabase::new(db).begin_store_reclaim_candidate_replacement(
                        operation,
                        replacement,
                        *nonactivation,
                    ),
                )
                .await?;
                Box::pin(finish_reclaim_candidate_replacement(db, storage, operation)).await?;
                return Ok(());
            }
            super::operations::StoreOperationPublicationOutcome::Nonactivated(_)
            | super::operations::StoreOperationPublicationOutcome::Reprepared => {
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
    operation: journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    for target in StoreDatabase::new(db)
        .store_reclaim_replacement_cleanup_targets(operation.clone())
        .await?
    {
        crate::sync::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    StoreDatabase::new(db)
        .complete_store_reclaim_candidate_replacement(operation)
        .await?;
    Ok(())
}

async fn prepare_reclaim_receipt(
    db: &crate::database::Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    operation: journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    let journal::DurableStoreReclaimOperation::AbsentVerified {
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
        crate::sync::store_objects::load_reclaim_authorization_ref(storage, &root, authorization)
            .await?;
    if &opened.authorization.value.target != target {
        return Err(StoreReclaimError::Authorization(
            "durable absent target differs from its signed authorization".to_string(),
        ));
    }

    let plan = Box::pin(super::operations::prepare_plan(
        db,
        storage,
        membership,
        device_id,
        identity_signer,
    ))
    .await?;
    let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StoreReclaimError::Authorization(
            "provider execution requires resolved Store membership".to_string(),
        ));
    };
    let provider_admin = resolved.provider_admin.combined_state().clone();
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
    let candidate = Box::pin(super::operations::prepare_candidate(
        db,
        storage,
        plan,
        super::operations::StoreOperationBatch::ReclaimReceipt(Box::new(receipt_ref.clone())),
    ))
    .await?;
    Box::pin(StoreDatabase::new(db).begin_store_reclaim_receipt(
        operation,
        journal::DurableStoreReclaimObject::Receipt {
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
    let stream_id = target.activation.coord.stream_id.to_string();
    let prefix = crate::sync::store_commit::package_semantic_prefix(
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
    root: &StoreRootRef,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<VerifiedReclaimSnapshot, StoreReclaimError> {
    let mut authorized = Vec::new();
    for (registration_ref, registration) in registrations {
        for snapshot in super::snapshot::load_store_snapshot_stream(
            storage,
            root,
            registration_ref,
            registration,
        )
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        {
            authorized.push(snapshot);
        }
    }
    let selected = match super::snapshot::select_maximal_stable_store_snapshot(
        storage, root, authorized,
    )
    .await
    {
        Ok(Some(selected)) => selected,
        Ok(None) => return Err(StoreReclaimError::NoSnapshot),
        Err(super::pull::StorePullError::SnapshotNotStable { member, device_id }) => {
            return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
        }
        Err(
            super::pull::StorePullError::SnapshotAuthorInactive
            | super::pull::StorePullError::SnapshotAuthorNotOwner,
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
    frontier.0.values().collect()
}

async fn position_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    covering: &StoreBatchCommitRef,
    covered: &StoreBatchCommitRef,
) -> Result<bool, StoreReclaimError> {
    super::pull::commit_position_covers(storage, root, covering, covered)
        .await
        .map_err(|error| match error {
            super::pull::CommitCoverageError::Object(error) => StoreReclaimError::Object(error),
            super::pull::CommitCoverageError::MissingAncestry { commit_hash } => {
                StoreReclaimError::MissingAncestry { commit_hash }
            }
        })
}

async fn exact_package_targets(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &CommitFrontier,
) -> Result<
    Vec<(
        StoreBatchCommitRef,
        crate::sync::store_commit::StorePackageRef,
    )>,
    StoreReclaimError,
> {
    let mut targets = BTreeMap::new();
    for tip in frontier_refs(coverage) {
        let mut cursor = Some(tip.clone());
        while let Some(reference) = cursor {
            if targets.contains_key(&reference) {
                break;
            }
            let (commit, _) = super::pull::load_commit_with_author(storage, root, &reference)
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

pub(super) mod journal;

#[cfg(test)]
mod tests;
