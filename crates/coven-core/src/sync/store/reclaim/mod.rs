//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::database::StoreDatabase;
use super::AuthorizedStore;
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{CircleControlCoord, CircleId};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MembershipChain, MembershipGrantId};
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::sync::store_commit::{
    snapshot_image_semantic_prefix, CircleAckRef, CirclePackageRef, CircleSnapshotRef,
    CommitFrontier, ObjectHash, StoreAckRef, StoreBatchCommitRef, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StorePackageRef, StoreProtocolError, StoreRootRef,
    StoreSnapshotLocator, STORE_PROTOCOL_VERSION,
};
use crate::sync::store_objects::StoreObjectError;

const RECLAIM_EVIDENCE_DOMAIN: &[u8] = b"coven.store-reclaim-evidence.v1\0";
const RECLAIM_AUTHORIZATION_DOMAIN: &[u8] = b"coven.store-reclaim-authorization.v1\0";
const RECLAIM_RECEIPT_DOMAIN: &[u8] = b"coven.store-reclaim-receipt.v1\0";

/// The exact object a reclaim authorizes the deletion of, together with the
/// kind-specific locator needed to physically delete it and confirm its absence.
/// Every kind shares one signed evidence → authorization → receipt chain; the
/// kind selects only the eligibility proof and the readback prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReclaimTarget {
    StorePackage(StorePackageReclaimTarget),
    CirclePackage(CirclePackageReclaimTarget),
}

impl ReclaimTarget {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::StorePackage(target) => &target.package.object,
            Self::CirclePackage(target) => &target.package.package.object,
        }
    }

    pub(crate) fn activation(&self) -> &StoreBatchCommitRef {
        match self {
            Self::StorePackage(target) => &target.activation,
            Self::CirclePackage(target) => &target.activation,
        }
    }
}

/// The eligibility proof an Owner signs to authorize one reclaim. The claim kind
/// matches its `ReclaimTarget` kind and carries the exact coverage and
/// acknowledgement references verified before the target is deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReclaimClaim {
    StorePackage(StorePackageReclaimClaim),
    CirclePackage(CirclePackageReclaimClaim),
}

impl ReclaimClaim {
    pub(crate) fn target(&self) -> ReclaimTarget {
        match self {
            Self::StorePackage(claim) => ReclaimTarget::StorePackage(claim.target.clone()),
            Self::CirclePackage(claim) => ReclaimTarget::CirclePackage(claim.target.clone()),
        }
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        match self {
            Self::StorePackage(claim) => claim.validate(),
            Self::CirclePackage(claim) => claim.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageReclaimTarget {
    pub package: StorePackageRef,
    pub activation: StoreBatchCommitRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CirclePackageReclaimTarget {
    pub package: CirclePackageRef,
    pub activation: StoreBatchCommitRef,
}

/// The exact author, Circle, control, and standalone-snapshot reference of the
/// stable Circle snapshot whose cut covers a reclaimed Circle package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleSnapshotLocator {
    pub author_registration: StoreDeviceRegistrationRef,
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub snapshot: CircleSnapshotRef,
}

/// Evidence that one Circle package is covered by an acknowledgement-stable
/// Circle snapshot: the snapshot's cut covers the package's activating commit,
/// and every device holding active Circle access has acknowledged coverage that
/// dominates the cut. The acknowledgements are exact per-device references,
/// readable by the Owner as a Circle member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CirclePackageReclaimClaim {
    pub target: CirclePackageReclaimTarget,
    pub covering_snapshot: CircleSnapshotLocator,
    pub acknowledgements: Vec<CircleAckRef>,
}

impl CirclePackageReclaimClaim {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        if self.acknowledgements.is_empty() {
            return Err(StoreProtocolError::Malformed(
                "Circle package reclaim evidence has no acknowledgements".to_string(),
            ));
        }
        if self
            .acknowledgements
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreProtocolError::Malformed(
                "Circle package reclaim acknowledgements are not strictly sorted and unique"
                    .to_string(),
            ));
        }
        let circle_id = self.target.package.circle_id;
        if self.covering_snapshot.circle_id != circle_id
            || self.target.package.control != self.covering_snapshot.control
        {
            return Err(StoreProtocolError::Malformed(
                "Circle package reclaim target, snapshot, and control name different Circles"
                    .to_string(),
            ));
        }
        let mut registrations = BTreeSet::new();
        if self.acknowledgements.iter().any(|acknowledgement| {
            acknowledgement.circle_id != circle_id
                || !registrations.insert(&acknowledgement.registration)
        }) {
            return Err(StoreProtocolError::Malformed(
                "Circle package reclaim acknowledgement names another Circle or repeats a device"
                    .to_string(),
            ));
        }
        let target_object = &self.target.package.package.object;
        if *target_object == self.target.activation.object
            || *target_object == self.covering_snapshot.snapshot.object
            || self
                .acknowledgements
                .iter()
                .any(|acknowledgement| acknowledgement.object == *target_object)
        {
            return Err(StoreProtocolError::Malformed(
                "Circle package reclaim target aliases proof authority".to_string(),
            ));
        }
        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimEvidenceRef {
    pub evidence_hash: ObjectHash,
    pub target: Box<ReclaimTarget>,
    pub object: ExactObjectRef,
}

impl ReclaimEvidenceRef {
    pub fn from_evidence(evidence: &ReclaimEvidence, object: ExactObjectRef) -> Self {
        Self {
            evidence_hash: evidence.evidence_hash(),
            target: Box::new(evidence.claim.target()),
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
        if evidence.claim.target() != *self.target {
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
    pub claim: ReclaimClaim,
    pub author_pubkey: String,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimEvidenceSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    claim: &'a ReclaimClaim,
    author_pubkey: &'a str,
}

impl ReclaimEvidence {
    pub fn signed(
        store_root_hash: ObjectHash,
        claim: ReclaimClaim,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        if authorization.evidence != self.evidence || authorization.target != *self.evidence.target
        {
            return Err(StoreProtocolError::Malformed(
                "reclaim authorization target or evidence differs from its exact reference"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn target(&self) -> &ReclaimTarget {
        &self.evidence.target
    }

    pub(crate) fn target_activation(&self) -> &StoreBatchCommitRef {
        self.evidence.target.activation()
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
    pub target: ReclaimTarget,
    pub evidence: ReclaimEvidenceRef,
    pub authority: StoreReclaimAuthority,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimAuthorizationSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    target: &'a ReclaimTarget,
    evidence: &'a ReclaimEvidenceRef,
    authority: &'a StoreReclaimAuthority,
}

impl ReclaimAuthorization {
    pub fn signed(
        store_root_hash: ObjectHash,
        target: ReclaimTarget,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            self.database(),
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
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    store_root_hash: ObjectHash,
    membership: &MembershipChain,
) -> Result<StoreReclaimResult, StoreReclaimError> {
    let root = database
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
        database,
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
    let registrations = database
        .activated_store_device_registration_records()
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    // A missing or unstable Store snapshot leaves Store packages uncovered but must
    // not block Circle package reclamation, which carries its own Circle coverage.
    let store_targets = match Box::pin(choose_snapshot(storage, &root, &registrations)).await {
        Ok(snapshot) => exact_package_targets(storage, &root, &snapshot.snapshot.meta.coverage)
            .await?
            .into_iter()
            .map(|(commit, package)| (commit, package, snapshot.clone()))
            .collect::<Vec<_>>(),
        Err(
            StoreReclaimError::NoSnapshot
            | StoreReclaimError::MissingAcknowledgement { .. }
            | StoreReclaimError::StaleAcknowledgement { .. }
            | StoreReclaimError::MissingRegisteredDevice { .. },
        ) => Vec::new(),
        Err(error) => return Err(error),
    };
    for (commit, package, snapshot) in store_targets {
        let target = ReclaimTarget::StorePackage(StorePackageReclaimTarget {
            package: package.clone(),
            activation: commit.clone(),
        });
        if reclaim_target_is_recorded(database, &target).await? {
            continue;
        }
        if database
            .store_package_is_retained_for_replay(package.clone(), commit.clone())
            .await?
        {
            continue;
        }
        Box::pin(prepare_reclaim_authorization(
            database,
            storage,
            device_id,
            identity_signer,
            membership,
            &root,
            ReclaimClaim::StorePackage(StorePackageReclaimClaim {
                target: StorePackageReclaimTarget {
                    package,
                    activation: commit,
                },
                covering_snapshot: StoreSnapshotLocator {
                    author_registration: snapshot.snapshot.meta.author_registration.clone(),
                    snapshot: snapshot.snapshot.reference.clone(),
                },
                acknowledgements: snapshot.acknowledgements.clone(),
            }),
        ))
        .await?;
    }
    Box::pin(prepare_circle_reclaim_authorizations(
        database,
        storage,
        device_id,
        identity_signer,
        membership,
        &root,
        &registrations,
    ))
    .await?;
    packages_deleted = packages_deleted
        .checked_add(
            Box::pin(resume_store_reclaim_operations(
                database,
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

/// Enumerate every reclaimable Circle package: for each Circle this device holds
/// active access to, select the maximal acknowledgement-stable Circle snapshot,
/// and authorize each package its cut covers that is not still a retained replay
/// input. Mirrors the Store package pass over Circle coverage evidence.
#[allow(clippy::too_many_arguments)]
async fn prepare_circle_reclaim_authorizations(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    root: &StoreRootRef,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<(), StoreReclaimError> {
    for input in database.circle_acknowledgement_publication_inputs().await? {
        let circle_id = input.circle_id;
        let control = input.control;
        let Some(selected) = Box::pin(choose_circle_snapshot(
            database,
            storage,
            root,
            circle_id,
            &control,
            registrations,
        ))
        .await?
        else {
            continue;
        };
        let targets = exact_circle_package_targets(
            storage,
            root,
            circle_id,
            &selected.meta.bootstrap.coverage,
        )
        .await?;
        for (commit, package) in targets {
            let target = ReclaimTarget::CirclePackage(CirclePackageReclaimTarget {
                package: package.clone(),
                activation: commit.clone(),
            });
            if reclaim_target_is_recorded(database, &target).await? {
                continue;
            }
            if database
                .circle_package_is_retained_for_replay(package.clone(), commit.clone())
                .await?
            {
                continue;
            }
            Box::pin(prepare_reclaim_authorization(
                database,
                storage,
                device_id,
                identity_signer,
                membership,
                root,
                ReclaimClaim::CirclePackage(CirclePackageReclaimClaim {
                    target: CirclePackageReclaimTarget {
                        package,
                        activation: commit,
                    },
                    covering_snapshot: CircleSnapshotLocator {
                        author_registration: selected.author_registration.clone(),
                        circle_id,
                        control: selected.meta.control.clone(),
                        snapshot: selected.reference.clone(),
                    },
                    acknowledgements: selected.acknowledgements.clone(),
                }),
            ))
            .await?;
        }
    }
    Ok(())
}

struct SelectedCircleSnapshot {
    author_registration: StoreDeviceRegistrationRef,
    reference: CircleSnapshotRef,
    meta: crate::sync::store_commit::CircleSnapshotMeta,
    acknowledgements: Vec<CircleAckRef>,
}

/// The maximal acknowledgement-stable Circle snapshot across every active device's
/// stream: its cut is dominated by no other stable snapshot, and every device
/// holding active Circle access has acknowledged coverage dominating that cut.
async fn choose_circle_snapshot(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    circle_id: CircleId,
    current_control: &CircleControlCoord,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<Option<SelectedCircleSnapshot>, StoreReclaimError> {
    // Read snapshot metadata under the current control's retained keyring, which
    // resolves every epoch the reader holds — a device's stream may span epochs
    // that rotated away, and a single-epoch key cannot decrypt the older ones.
    let encryption = database
        .circle_package_access(circle_id, current_control.clone())
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} snapshot key is not resolvable from retained controls"
            ))
        })?
        .encryption;
    let mut stable: Vec<SelectedCircleSnapshot> = Vec::new();
    for (registration_ref, registration) in registrations {
        // A device's snapshot stream is a create-once chain sealed under the epoch
        // active when each generation was authored. A generation sealed under an
        // epoch key this reader cannot resolve (one that rotated away before the
        // reader held it) is not current-epoch coverage; skip the stream — reclaim
        // is best-effort and idempotent, so a later cycle retries once coverage is
        // resolvable. Only a decryptable stream contributes coverage evidence.
        let stream = match super::snapshot::load_circle_snapshot_stream_refs(
            storage,
            root,
            circle_id,
            encryption.clone(),
            registration_ref,
            registration,
        )
        .await
        {
            Ok(stream) => stream,
            Err(error) => {
                tracing::debug!(
                    circle_id = %circle_id,
                    device_id = %registration.device_id,
                    "skip Circle snapshot stream for reclaim coverage: {error}"
                );
                continue;
            }
        };
        for (reference, meta) in stream {
            if let Some(acknowledgements) =
                super::acknowledgements::stable_circle_acks_dominating_on(
                    database,
                    storage,
                    root,
                    circle_id,
                    current_control,
                    &meta.bootstrap.coverage,
                )
                .await
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
            {
                stable.push(SelectedCircleSnapshot {
                    author_registration: registration_ref.clone(),
                    reference,
                    meta,
                    acknowledgements,
                });
            }
        }
    }
    let coverages: Vec<CommitFrontier> = stable
        .iter()
        .map(|candidate| candidate.meta.bootstrap.coverage.clone())
        .collect();
    stable.retain(|candidate| {
        !coverages.iter().any(|other| {
            super::snapshot::coverage_dominates(other, &candidate.meta.bootstrap.coverage)
        })
    });
    stable.sort_by_key(|candidate| candidate.reference.snapshot_hash);
    Ok(stable.pop())
}

/// Every Circle package addressed to `circle_id` whose activating commit lies at
/// or before `coverage`, paired with its exact activation. Walks the covered
/// commit history exactly like the Store package enumeration.
async fn exact_circle_package_targets(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    circle_id: CircleId,
    coverage: &CommitFrontier,
) -> Result<Vec<(StoreBatchCommitRef, CirclePackageRef)>, StoreReclaimError> {
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
            if let Some(package) = commit
                .circle_packages()
                .iter()
                .find(|package| package.circle_id == circle_id)
            {
                targets.insert(reference.clone(), package.clone());
            }
            cursor = commit.order.predecessor().cloned();
        }
    }
    Ok(targets.into_iter().collect())
}

async fn reclaim_target_is_recorded(
    database: &StoreDatabase,
    target: &ReclaimTarget,
) -> Result<bool, StoreReclaimError> {
    for operation in database.store_reclaim_operations().await? {
        if operation.authorization().target() == target {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn prepare_reclaim_authorization(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    root: &StoreRootRef,
    claim: ReclaimClaim,
) -> Result<(), StoreReclaimError> {
    let plan = Box::pin(super::operations::prepare_plan(
        database,
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
    verify_reclaim_evidence(database, storage, root, &evidence).await?;
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
        evidence.claim.target(),
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
        database,
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
    Box::pin(database.begin_store_reclaim_operation(operation)).await?;
    Ok(())
}

async fn resume_store_reclaim_operations(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
) -> Result<u64, StoreReclaimError> {
    let mut completed = 0_u64;
    loop {
        let operations = database.store_reclaim_operations().await?;
        let mut progressed = false;
        for operation in operations {
            match &operation {
                journal::DurableStoreReclaimOperation::AuthorizationCandidate { .. }
                | journal::DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                    Box::pin(drive_reclaim_candidate(
                        database,
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
                    Box::pin(finish_reclaim_candidate_replacement(
                        database, storage, operation,
                    ))
                    .await?;
                    progressed = true;
                }
                journal::DurableStoreReclaimOperation::Authorized { .. } => {
                    Box::pin(execute_reclaim_delete(database, storage, operation)).await?;
                    completed = completed.checked_add(1).ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaimed package count exceeded u64".to_string(),
                        )
                    })?;
                    progressed = true;
                }
                journal::DurableStoreReclaimOperation::AbsentVerified { .. } => {
                    Box::pin(prepare_reclaim_receipt(
                        database,
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
    database: &StoreDatabase,
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
    let root = database
        .local_store_root_ref()
        .await?
        .ok_or_else(|| StoreReclaimError::Authorization("Store root is absent".to_string()))?;
    let target =
        verify_authorized_reclaim(database, storage, &root, authorization, activation).await?;
    if target_is_retained_for_replay(database, &target).await? {
        return Err(StoreReclaimError::Authorization(
            "reclaim target remains retained for accepted replay".to_string(),
        ));
    }
    storage
        .delete_protocol_object(target.object())
        .await
        .map_err(|source| StoreReclaimError::Delete {
            commit_hash: target.activation().commit_hash,
            source,
        })?;
    verify_reclaim_target_absent(database, storage, &root, &target).await?;
    database
        .mark_store_reclaim_target_absent(operation, target)
        .await?;
    Ok(())
}

/// Whether the reclaim target is still an accepted-replay input for its audience.
/// The authority spine never becomes eligible while replay names it.
async fn target_is_retained_for_replay(
    database: &StoreDatabase,
    target: &ReclaimTarget,
) -> Result<bool, StoreReclaimError> {
    match target {
        ReclaimTarget::StorePackage(target) => Ok(database
            .store_package_is_retained_for_replay(target.package.clone(), target.activation.clone())
            .await?),
        ReclaimTarget::CirclePackage(target) => Ok(database
            .circle_package_is_retained_for_replay(
                target.package.clone(),
                target.activation.clone(),
            )
            .await?),
    }
}

async fn verify_authorized_reclaim(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    authorization_ref: &ReclaimAuthorizationRef,
    activation: &journal::ReclaimCommitActivation,
) -> Result<ReclaimTarget, StoreReclaimError> {
    let opened = crate::sync::store_objects::load_reclaim_authorization_ref(
        storage,
        root,
        authorization_ref,
    )
    .await?;
    verify_reclaim_authorization_activation(database, storage, root, authorization_ref, activation)
        .await?;
    verify_reclaim_evidence(database, storage, root, &opened.evidence.value).await
}

async fn verify_reclaim_authorization_activation(
    database: &StoreDatabase,
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
    super::pull::verify_merge_commit_currently_materialized(database, storage, root, commit)
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
}

/// Re-verify a reclaim's eligibility from its signed evidence and current live
/// state, returning the exact object the authorization may delete. Runs both at
/// prepare time and again before deletion, so a change in coverage, acknowledgement,
/// or replay retention since authoring fails the delete loud.
async fn verify_reclaim_evidence(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    evidence: &ReclaimEvidence,
) -> Result<ReclaimTarget, StoreReclaimError> {
    evidence
        .verify()
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    if evidence.store_root_hash != root.store_root_hash {
        return Err(StoreReclaimError::Authorization(
            "reclaim evidence belongs to another Store root".to_string(),
        ));
    }
    match &evidence.claim {
        ReclaimClaim::StorePackage(claim) => Ok(ReclaimTarget::StorePackage(
            verify_store_package_reclaim_claim(storage, root, claim).await?,
        )),
        ReclaimClaim::CirclePackage(claim) => Ok(ReclaimTarget::CirclePackage(
            verify_circle_package_reclaim_claim(database, storage, root, claim).await?,
        )),
    }
}

async fn verify_store_package_reclaim_claim(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    claim: &StorePackageReclaimClaim,
) -> Result<StorePackageReclaimTarget, StoreReclaimError> {
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
    Ok(claim.target.clone())
}

/// Re-verify a Circle package reclaim: a stable Circle snapshot on the same
/// Circle covers the package's activating commit, and every device holding
/// active Circle access has acknowledged coverage dominating the snapshot cut.
/// The acknowledgements are read under the Circle's current control, whose
/// retained keyring resolves epochs sealed before a rotation.
async fn verify_circle_package_reclaim_claim(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    claim: &CirclePackageReclaimClaim,
) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
    let circle_id = claim.target.package.circle_id;
    let snapshot_control = &claim.covering_snapshot.control;
    // Read snapshot metadata under the current control's retained keyring so a
    // snapshot sealed before a rotation still resolves its epoch key.
    let current_control = database
        .current_circle_control(circle_id)
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} has no active control for reclaim stability"
            ))
        })?;
    let access = database
        .circle_package_access(circle_id, current_control.clone())
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} snapshot key is not resolvable from retained controls"
            ))
        })?;
    let author = crate::sync::store_objects::load_registration_ref(
        storage,
        root,
        &claim.covering_snapshot.author_registration,
    )
    .await?;
    let stream = super::snapshot::load_circle_snapshot_stream_refs(
        storage,
        root,
        circle_id,
        access.encryption,
        &claim.covering_snapshot.author_registration,
        &author.value,
    )
    .await
    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let (_, snapshot) = stream
        .into_iter()
        .find(|(reference, _)| *reference == claim.covering_snapshot.snapshot)
        .ok_or(StoreReclaimError::NoSnapshot)?;
    if snapshot.circle_id != circle_id
        || snapshot.control != *snapshot_control
        || snapshot.author_registration != claim.covering_snapshot.author_registration
    {
        return Err(StoreReclaimError::Authorization(
            "Circle reclaim snapshot differs from its exact locator".to_string(),
        ));
    }
    let cut = &snapshot.bootstrap.coverage;
    let expected = super::acknowledgements::stable_circle_acks_dominating_on(
        database,
        storage,
        root,
        circle_id,
        &current_control,
        cut,
    )
    .await
    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
    .ok_or_else(|| {
        StoreReclaimError::Authorization(
            "Circle snapshot is not acknowledgement-stable across every active-access device"
                .to_string(),
        )
    })?;
    if claim.acknowledgements != expected {
        return Err(StoreReclaimError::Authorization(
            "Circle reclaim acknowledgements differ from the active-access stability proof"
                .to_string(),
        ));
    }
    let (commit, _) =
        super::pull::load_commit_with_author(storage, root, &claim.target.activation).await?;
    if !commit.circle_packages().contains(&claim.target.package)
        || !snapshot_covers_target(storage, root, cut, &claim.target.activation).await?
    {
        return Err(StoreReclaimError::Authorization(
            "Circle reclaim target is not the exact package covered by its snapshot".to_string(),
        ));
    }
    Ok(claim.target.clone())
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
    database: &StoreDatabase,
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
                database
                    .mark_reusable_retained_authority_uploaded(remote)
                    .await?;
            }
        }
        match Box::pin(super::operations::publish_prepared_store_operation(
            database, storage, candidate,
        ))
        .await?
        {
            super::operations::StoreOperationPublicationOutcome::Activated(_) => {
                return Ok(());
            }
            super::operations::StoreOperationPublicationOutcome::RepreparedCandidate(
                replacement,
            ) => {
                operation =
                    Box::pin(database.replace_store_reclaim_candidate(operation, *replacement))
                        .await?;
            }
            super::operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                nonactivation,
                ..
            } => {
                let plan = Box::pin(super::operations::prepare_plan(
                    database,
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
                    database, storage, plan, batch,
                ))
                .await?;
                operation = Box::pin(database.begin_store_reclaim_candidate_replacement(
                    operation,
                    replacement,
                    *nonactivation,
                ))
                .await?;
                Box::pin(finish_reclaim_candidate_replacement(
                    database, storage, operation,
                ))
                .await?;
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
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    operation: journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    for target in database
        .store_reclaim_replacement_cleanup_targets(operation.clone())
        .await?
    {
        crate::sync::store_objects::delete_exact_object(storage, &target.object).await?;
        database
            .sqlite()
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    database
        .complete_store_reclaim_candidate_replacement(operation)
        .await?;
    Ok(())
}

async fn prepare_reclaim_receipt(
    database: &StoreDatabase,
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
    let root = database
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
        database,
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
        database,
        storage,
        plan,
        super::operations::StoreOperationBatch::ReclaimReceipt(Box::new(receipt_ref.clone())),
    ))
    .await?;
    Box::pin(database.begin_store_reclaim_receipt(
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
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    target: &ReclaimTarget,
) -> Result<(), StoreReclaimError> {
    let (context, prefix) = match target {
        ReclaimTarget::StorePackage(target) => (
            ProtocolObjectContext::store_encrypted(
                root.store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            crate::sync::store_commit::package_semantic_prefix(
                target.package.candidate_family,
                &target.activation.coord.stream_id.to_string(),
                target.activation.coord.sequence(),
                target.package.content_hash,
            ),
        ),
        ReclaimTarget::CirclePackage(target) => {
            // A Circle package is sealed under the Circle epoch key; resolving its
            // access only builds the read context — a deleted object reads back
            // absent before any decryption.
            let access = database
                .circle_package_access(target.package.circle_id, target.package.control.clone())
                .await?
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaim target Circle package key is not resolvable".to_string(),
                    )
                })?;
            (
                ProtocolObjectContext::circle(
                    root.store_root_hash,
                    ProtocolObjectDomain::CirclePackage,
                    access.encryption,
                ),
                crate::sync::store_commit::circle_package_semantic_prefix(
                    target.package.circle_id,
                    target.package.package.candidate_family,
                    &target.activation.coord.stream_id.to_string(),
                    target.activation.coord.sequence(),
                    target.package.package.content_hash,
                ),
            )
        }
    };
    match storage
        .read_protocol_object(&context, target.object(), &prefix)
        .await
    {
        Err(StorageError::NotFound(_)) => Ok(()),
        Ok(_) => Err(StoreReclaimError::Authorization(
            "reclaim target remains readable after exact deletion".to_string(),
        )),
        Err(error) => Err(StoreReclaimError::Storage(error)),
    }
}

#[derive(Clone)]
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
