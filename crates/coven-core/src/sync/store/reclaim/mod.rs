//! Proof-gated deletion of exact Store packages covered by an exact snapshot.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::database::StoreDatabase;
use super::pull::StoreCommitVerifier;
use super::AuthorizedStore;
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{
    CircleBootstrapCoverageRef, CircleControlCoord, CircleControlState, CircleEpochOrigin, CircleId,
};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MembershipChain, MembershipGrantId};
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::sync::store_commit::{
    snapshot_image_semantic_prefix, CircleAckRef, CirclePackageRef, CircleSnapshotRef,
    CommitFrontier, ObjectHash, SnapshotImageRef, StoreAckRef, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StorePackageRef, StoreProtocolError,
    StoreSnapshotLocator, VerifiedStoreBatchCommit, STORE_PROTOCOL_VERSION,
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
    CircleBootstrapImage(CircleBootstrapImageReclaimTarget),
    CircleSnapshotImage(CircleSnapshotImageReclaimTarget),
    AudienceBlob(AudienceBlobReclaimTarget),
}

impl ReclaimTarget {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::StorePackage(target) => &target.package.object,
            Self::CirclePackage(target) => &target.package.package.object,
            Self::CircleBootstrapImage(target) => &target.coverage.bootstrap.image.object,
            Self::CircleSnapshotImage(target) => &target.image.object,
            Self::AudienceBlob(target) => target.blob.object(),
        }
    }

    pub(crate) fn activation(&self) -> ReclaimActivation<'_> {
        match self {
            Self::StorePackage(target) => ReclaimActivation::Commit(&target.activation),
            Self::CirclePackage(target) => ReclaimActivation::Commit(&target.activation),
            Self::CircleBootstrapImage(target) => {
                ReclaimActivation::Commit(&target.coverage.activation_commit)
            }
            Self::CircleSnapshotImage(target) => {
                ReclaimActivation::CircleSnapshotMetadata(CircleSnapshotStreamActivation {
                    circle_id: target.circle_id,
                    author_registration: &target.snapshot_author,
                    snapshot: &target.snapshot,
                })
            }
            Self::AudienceBlob(target) => {
                ReclaimActivation::PackageBlobBinding(PackageBlobBindingActivation {
                    package: &target.package,
                    activation: &target.activation,
                })
            }
        }
    }
}

/// The signed statement that put a reclaim target into the shared live set — the
/// authority a verifier re-reads to confirm the Owner is deleting what its claim
/// says. It follows how the object was published: a Store commit names packages
/// and the bootstrap images its Circle-control activations carry; a device's
/// per-Circle snapshot stream names its own images through signed metadata that
/// rides no commit at all; and a row blob is named by the bindings of the package
/// that published the row, not by the commit body.
pub(crate) enum ReclaimActivation<'a> {
    Commit(&'a StoreBatchCommitRef),
    CircleSnapshotMetadata(CircleSnapshotStreamActivation<'a>),
    PackageBlobBinding(PackageBlobBindingActivation<'a>),
}

impl ReclaimActivation<'_> {
    /// The exact object carrying the activating signature. Reclaim identity checks
    /// use it to refuse a target that aliases its own authority.
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Commit(commit) => &commit.object,
            Self::CircleSnapshotMetadata(activation) => &activation.snapshot.object,
            Self::PackageBlobBinding(activation) => activation.package.object(),
        }
    }
}

/// The exact package whose row-blob bindings carry a reclaimed blob's locator,
/// together with the Store commit that activated it. A blob rides inside a package
/// addressed to one audience and is never named by the commit body, so the package
/// is the signed statement a verifier re-reads to confirm the blob was published
/// where the claim says.
pub(crate) struct PackageBlobBindingActivation<'a> {
    pub package: &'a AudienceBlobBindingPackage,
    pub activation: &'a StoreBatchCommitRef,
}

/// One generation of a device's per-Circle snapshot stream, named by the exact
/// metadata object whose signature vouches for the image that generation
/// published. The stream is anchored on the author's Store device registration and
/// the Circle, which is all a Store member outside the Circle can check; a member
/// inside re-walks the stream itself.
pub(crate) struct CircleSnapshotStreamActivation<'a> {
    pub circle_id: CircleId,
    pub author_registration: &'a StoreDeviceRegistrationRef,
    pub snapshot: &'a CircleSnapshotRef,
}

/// The eligibility proof an Owner signs to authorize one reclaim. The claim kind
/// matches its `ReclaimTarget` kind and carries the exact coverage and
/// acknowledgement references verified before the target is deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReclaimClaim {
    StorePackage(StorePackageReclaimClaim),
    CirclePackage(CirclePackageReclaimClaim),
    CircleBootstrapImage(CircleBootstrapImageReclaimClaim),
    CircleSnapshotImage(CircleSnapshotImageReclaimClaim),
    AudienceBlob(AudienceBlobReclaimClaim),
}

impl ReclaimClaim {
    pub(crate) fn target(&self) -> ReclaimTarget {
        match self {
            Self::StorePackage(claim) => ReclaimTarget::StorePackage(claim.target.clone()),
            Self::CirclePackage(claim) => ReclaimTarget::CirclePackage(claim.target().clone()),
            Self::CircleBootstrapImage(claim) => {
                ReclaimTarget::CircleBootstrapImage(claim.target.clone())
            }
            Self::CircleSnapshotImage(claim) => {
                ReclaimTarget::CircleSnapshotImage(claim.target.clone())
            }
            Self::AudienceBlob(claim) => ReclaimTarget::AudienceBlob(claim.target.clone()),
        }
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        match self {
            Self::StorePackage(claim) => claim.validate(),
            Self::CirclePackage(claim) => claim.validate(),
            Self::CircleBootstrapImage(claim) => claim.validate(),
            Self::CircleSnapshotImage(claim) => claim.validate(),
            Self::AudienceBlob(claim) => claim.validate(),
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

/// The two ways one Circle package stops being live history. Either a stable
/// Circle snapshot covers it and every active-access device acknowledged that
/// coverage, or the package lies beyond its epoch's accepted close cutoff — in
/// which case it never materialized anywhere and needs no coverage evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CirclePackageReclaimClaim {
    SnapshotCovered(CirclePackageSnapshotCoverageClaim),
    BeyondEpochCutoff(CirclePackageBeyondCutoffClaim),
}

impl CirclePackageReclaimClaim {
    pub(crate) fn target(&self) -> &CirclePackageReclaimTarget {
        match self {
            Self::SnapshotCovered(claim) => &claim.target,
            Self::BeyondEpochCutoff(claim) => &claim.target,
        }
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        match self {
            Self::SnapshotCovered(claim) => claim.validate(),
            Self::BeyondEpochCutoff(claim) => claim.validate(),
        }
    }
}

/// Evidence that one Circle package lies beyond the accepted cutoff of the epoch
/// it was addressed to: the named successor control activated with a closed-epoch
/// origin whose cutoff does not cover the package's activating commit. Such a
/// package is invalid by construction — no device materializes it — so it needs no
/// snapshot coverage or acknowledgement evidence. The successor control is an
/// exact coordinate the verifier re-resolves from retained activations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CirclePackageBeyondCutoffClaim {
    pub target: CirclePackageReclaimTarget,
    pub successor_control: CircleControlCoord,
}

impl CirclePackageBeyondCutoffClaim {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        self.successor_control
            .validate()
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if self.successor_control == self.target.package.control {
            return Err(StoreProtocolError::Malformed(
                "Circle package beyond-cutoff successor is the package's own control".to_string(),
            ));
        }
        if self.target.package.package.object == self.target.activation.object {
            return Err(StoreProtocolError::Malformed(
                "Circle package reclaim target aliases proof authority".to_string(),
            ));
        }
        Ok(())
    }
}

/// Evidence that one Circle package is covered by an acknowledgement-stable
/// Circle snapshot: the snapshot's cut covers the package's activating commit,
/// and every device holding active Circle access has acknowledged coverage that
/// dominates the cut. The acknowledgements are exact per-device references,
/// readable by the Owner as a Circle member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CirclePackageSnapshotCoverageClaim {
    pub target: CirclePackageReclaimTarget,
    pub covering_snapshot: CircleSnapshotLocator,
    pub acknowledgements: Vec<CircleAckRef>,
}

impl CirclePackageSnapshotCoverageClaim {
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

/// The exact Circle bootstrap image a reclaim deletes: the retained bootstrap
/// coverage a recipient device's live projection was seeded from names the image
/// object, its activating Store commit, and the cut the seed covers. The coverage
/// is recovered from the recipient's own signed acknowledgement (`seeded_from`),
/// never fabricated by the reclaiming Owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleBootstrapImageReclaimTarget {
    pub coverage: CircleBootstrapCoverageRef,
}

/// The two proofs an Owner can present that a recipient no longer needs its seed
/// image. Both carry the recipient device's own activated Circle acknowledgement,
/// whose `seeded_from` names the target coverage — binding the proof to the exact
/// image being deleted. The authorization verifier re-loads and re-checks the
/// acknowledgement; nothing here is trusted from the claim alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleBootstrapReclaimProof {
    /// The recipient advanced past its seed: its acknowledgement's accepted Store
    /// frontier strictly dominates the bootstrap's cut, and its owner still holds
    /// active Circle access.
    RecipientCoverage { acknowledgement: CircleAckRef },
    /// The recipient lost Circle authority: its owner is absent from the roster of
    /// an activated successor control that strictly covers the seed's control.
    LostAuthority {
        acknowledgement: CircleAckRef,
        successor_control: CircleControlCoord,
    },
}

impl CircleBootstrapReclaimProof {
    fn acknowledgement(&self) -> &CircleAckRef {
        match self {
            Self::RecipientCoverage { acknowledgement }
            | Self::LostAuthority {
                acknowledgement, ..
            } => acknowledgement,
        }
    }
}

/// Evidence that one Circle bootstrap image is no longer a live seed for its
/// recipient: the target image and the recipient-coverage or lost-authority proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleBootstrapImageReclaimClaim {
    pub target: CircleBootstrapImageReclaimTarget,
    pub proof: CircleBootstrapReclaimProof,
}

impl CircleBootstrapImageReclaimClaim {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        let circle_id = self.target.coverage.circle_id;
        let acknowledgement = self.proof.acknowledgement();
        if acknowledgement.circle_id != circle_id {
            return Err(StoreProtocolError::Malformed(
                "Circle bootstrap reclaim acknowledgement names another Circle".to_string(),
            ));
        }
        let image = &self.target.coverage.bootstrap.image.object;
        if *image == self.target.coverage.activation_commit.object
            || *image == acknowledgement.object
        {
            return Err(StoreProtocolError::Malformed(
                "Circle bootstrap reclaim target aliases proof authority".to_string(),
            ));
        }
        if let CircleBootstrapReclaimProof::LostAuthority {
            successor_control, ..
        } = &self.proof
        {
            successor_control
                .validate()
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            if *successor_control == self.target.coverage.control {
                return Err(StoreProtocolError::Malformed(
                    "Circle bootstrap lost-authority successor is the seed control".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// The exact image of one generation of a device's standalone Circle snapshot
/// stream.
///
/// Only the image ciphertext is ever a reclaim target. A reader reconstructs the
/// stream by walking it from generation zero along each metadata object's
/// create-once successor slot and stopping at the first slot that is absent, so
/// deleting any generation's metadata hides every later generation from every
/// reader — the metadata chain is permanent regardless of how superseded the
/// generation is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleSnapshotImageReclaimTarget {
    pub circle_id: CircleId,
    pub snapshot_author: StoreDeviceRegistrationRef,
    pub control: CircleControlCoord,
    pub snapshot: CircleSnapshotRef,
    pub image: SnapshotImageRef,
}

impl CircleSnapshotImageReclaimTarget {
    /// The ownership record's owner for this image: the device-authorized
    /// activation of the author's per-Circle snapshot stream, at this generation.
    /// Derived from the target's own identity rather than carried in it, so an
    /// ownership record can only close against the generation that published it.
    pub(crate) fn snapshot_owner(
        &self,
        store_root_hash: ObjectHash,
    ) -> Result<crate::sync::remote_object::SnapshotObjectOwner, StoreProtocolError> {
        Ok(crate::sync::remote_object::SnapshotObjectOwner {
            activation: crate::sync::store_commit::circle_snapshot_stream_activation(
                store_root_hash,
                &self.snapshot_author,
                self.circle_id,
                &self.snapshot_author.device_id.to_string(),
            )?,
            generation: self.snapshot.generation,
        })
    }
}

/// Evidence that a later generation of the same device's Circle snapshot stream
/// supersedes the reclaimed one. The claim names only the exact superseding
/// generation — that generation's own signed metadata, its stability against every
/// active-access device's acknowledgement, and its coverage of the reclaimed cut
/// are all re-derived from live state at verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleSnapshotImageReclaimClaim {
    pub target: CircleSnapshotImageReclaimTarget,
    pub superseding: CircleSnapshotRef,
}

impl CircleSnapshotImageReclaimClaim {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        if self.superseding.generation <= self.target.snapshot.generation {
            return Err(StoreProtocolError::Malformed(
                "Circle snapshot reclaim names a superseding generation that is not later"
                    .to_string(),
            ));
        }
        let image = &self.target.image.object;
        if *image == self.target.snapshot.object || *image == self.superseding.object {
            return Err(StoreProtocolError::Malformed(
                "Circle snapshot reclaim target aliases proof authority".to_string(),
            ));
        }
        Ok(())
    }
}

/// The exact package whose row-blob bindings published one blob, in whichever
/// audience the row was written to. Reading the package back needs its audience:
/// a Store package is sealed to the Store, a Circle package to the Circle epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AudienceBlobBindingPackage {
    Store(StorePackageRef),
    Circle(CirclePackageRef),
}

impl AudienceBlobBindingPackage {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Store(package) => &package.object,
            Self::Circle(package) => &package.package.object,
        }
    }

    pub(crate) fn remote_audience(&self) -> crate::blob::locator::RemoteAudience {
        match self {
            Self::Store(_) => crate::blob::locator::RemoteAudience::Store,
            Self::Circle(package) => {
                crate::blob::locator::RemoteAudience::Circle(package.circle_id)
            }
        }
    }
}

/// The exact ciphertext of one row blob that no live row still binds in its
/// audience. Moving a row to another audience republishes its blob under a new
/// locator and drops the old binding, leaving the source ciphertext addressed to
/// an audience nothing reads from any more.
///
/// The blob reference is self-binding: its object's logical key is derived from
/// the locator, which names the audience and the uploading device, so a target
/// cannot describe one object while naming another's addressing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudienceBlobReclaimTarget {
    pub blob: crate::blob::locator::StoredBlobRef,
    pub package: AudienceBlobBindingPackage,
    pub activation: StoreBatchCommitRef,
}

/// Evidence that a row blob is no longer bound by any live row. The claim carries
/// nothing but the target: the verifier re-reads the publishing package to confirm
/// it bound this blob, then re-derives from its own materialized rows that none
/// still binds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudienceBlobReclaimClaim {
    pub target: AudienceBlobReclaimTarget,
}

impl AudienceBlobReclaimClaim {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        let blob = self.target.blob.object();
        if blob == self.target.package.object() || *blob == self.target.activation.object {
            return Err(StoreProtocolError::Malformed(
                "audience blob reclaim target aliases proof authority".to_string(),
            ));
        }
        if self.target.blob.locator().audience() != self.target.package.remote_audience() {
            return Err(StoreProtocolError::Malformed(
                "audience blob reclaim target names a package for another audience".to_string(),
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
        crate::sync::store_commit::require_version(self.version)?;
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

    pub(crate) fn target_activation(&self) -> ReclaimActivation<'_> {
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
        crate::sync::store_commit::require_version(self.version)?;
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
        crate::sync::store_objects::verify_store_root(
            store_root_hash,
            executor_registration.store_root.store_root_hash,
        )?;
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
        crate::sync::store_commit::require_version(self.version)?;
        self.executor.verify_registration(executor)?;
        crate::sync::store_objects::verify_store_root(
            self.store_root_hash,
            executor.store_root.store_root_hash,
        )?;
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
    #[error("active member {member:?} device {ack_device_id:?} has no acknowledgement covering exact snapshot commit {snapshot_commit}")]
    StaleAcknowledgement {
        member: String,
        ack_device_id: String,
        snapshot_commit: ObjectHash,
    },
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
    #[error("deleting the exact object activated by {activation} failed: {source}")]
    Delete {
        activation: ObjectHash,
        #[source]
        source: StorageError,
    },
}

impl AuthorizedStore<'_> {
    pub(crate) async fn reclaim_packages(
        &mut self,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<StoreReclaimResult, StoreReclaimError> {
        let authority = self.operation_authority();
        reclaim_store_packages_with_history(
            authority.database,
            authority.history_verifier,
            device_id,
            identity,
            &*authority.membership,
        )
        .await
    }
}

#[cfg(test)]
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
    let mut history_verifier = super::pull::MergeHistoryVerifier::new(storage, &root)
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    reclaim_store_packages_with_history(
        database,
        &mut history_verifier,
        device_id,
        identity_signer,
        membership,
    )
    .await
}

async fn reclaim_store_packages_with_history(
    database: &StoreDatabase,
    history_verifier: &mut super::pull::MergeHistoryVerifier<'_>,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
) -> Result<StoreReclaimResult, StoreReclaimError> {
    let mut packages_deleted = Box::pin(resume_store_reclaim_operations(
        database,
        device_id,
        identity_signer,
        membership,
        history_verifier.commit_verifier(),
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
    let store_targets = match Box::pin(choose_snapshot(history_verifier, &registrations)).await {
        Ok(snapshot) => exact_package_targets(
            history_verifier.commit_verifier(),
            &snapshot.snapshot.meta.coverage,
        )
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
        if database
            .store_package_is_retained_for_replay(package.clone(), commit.clone())
            .await?
        {
            continue;
        }
        Box::pin(prepare_reclaim_authorization(
            database,
            device_id,
            identity_signer,
            membership,
            history_verifier.commit_verifier(),
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
        device_id,
        identity_signer,
        membership,
        &registrations,
        history_verifier.commit_verifier(),
    ))
    .await?;
    Box::pin(prepare_audience_blob_reclaim_authorizations(
        database,
        device_id,
        identity_signer,
        membership,
        history_verifier.commit_verifier(),
    ))
    .await?;
    packages_deleted = packages_deleted
        .checked_add(
            Box::pin(resume_store_reclaim_operations(
                database,
                device_id,
                identity_signer,
                membership,
                history_verifier.commit_verifier(),
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

/// Authorize deletion of every Circle package the Circle's current epoch cutoff
/// excludes. A package addressed to a closed epoch whose activating commit the
/// close cutoff does not accept never materializes on any device — it is invalid
/// by construction — so it is eligible once the successor epoch activated, with no
/// snapshot coverage or acknowledgement evidence required. Enumerated from this
/// device's accepted history rather than a snapshot cut, because such a package is
/// by definition outside every snapshot's coverage.
#[allow(clippy::too_many_arguments)]
async fn prepare_beyond_cutoff_circle_reclaim_authorizations(
    database: &StoreDatabase,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    circle_id: CircleId,
    current_control: &CircleControlCoord,
    commit_verifier: &mut StoreCommitVerifier<'_>,
) -> Result<(), StoreReclaimError> {
    let successor = database
        .verified_circle_activation(circle_id, current_control.clone())
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} current control is not a retained activation"
            ))
        })?;
    // Only a control that closed a predecessor epoch carries a cutoff; a Circle
    // whose current epoch closed nothing has no beyond-cutoff package to enumerate.
    if !matches!(
        successor.control.value.state(),
        CircleControlState::ActiveEpoch(active)
            if matches!(active.common.origin, CircleEpochOrigin::Closed { .. })
    ) {
        return Ok(());
    }
    let frontier = CommitFrontier::from_refs(database.materialized_frontier().await?)
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let epochs = database.circle_replay_epoch_index().await?;
    for (commit, package) in
        exact_circle_package_targets(commit_verifier, circle_id, &frontier).await?
    {
        // `permits` is the same predicate the pull path applies; a package it
        // accepts is live history. A package whose control it cannot resolve, or
        // that conflicts with the cutoff, errors rather than being reclaimed.
        if epochs
            .permits(&commit, circle_id, &package.control)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        {
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
            device_id,
            identity_signer,
            membership,
            commit_verifier,
            ReclaimClaim::CirclePackage(CirclePackageReclaimClaim::BeyondEpochCutoff(
                CirclePackageBeyondCutoffClaim {
                    target: CirclePackageReclaimTarget {
                        package,
                        activation: commit,
                    },
                    successor_control: current_control.clone(),
                },
            )),
        ))
        .await?;
    }
    Ok(())
}

/// The package in one commit that could have published a blob of this audience.
/// The blob's own locator names its audience, and a commit carries at most one
/// package per audience, so the audience selects the package outright.
fn audience_blob_binding_package(
    commit: &crate::sync::store_commit::StoreBatchCommit,
    audience: crate::blob::locator::RemoteAudience,
) -> Option<AudienceBlobBindingPackage> {
    match audience {
        crate::blob::locator::RemoteAudience::Store => commit
            .store_package()
            .cloned()
            .map(AudienceBlobBindingPackage::Store),
        crate::blob::locator::RemoteAudience::Circle(circle_id) => commit
            .circle_packages()
            .iter()
            .find(|package| package.circle_id == circle_id)
            .cloned()
            .map(AudienceBlobBindingPackage::Circle),
    }
}

/// Authorize deletion of every row blob no live row still binds in its audience.
/// Moving a row to another audience republishes its blob under a new locator and
/// drops the old binding, stranding the source ciphertext; nothing else ever
/// deletes it. The same orphan test the member-signed tombstone path applies
/// decides eligibility, and an image that still pins the blob holds it back.
async fn prepare_audience_blob_reclaim_authorizations(
    database: &StoreDatabase,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    commit_verifier: &mut StoreCommitVerifier<'_>,
) -> Result<(), StoreReclaimError> {
    for (blob, owners) in database.stored_blob_reclaim_candidates().await? {
        if !database.stored_blob_is_row_orphaned(blob.clone()).await? {
            continue;
        }
        if database
            .audience_blob_is_retained_for_replay(blob.clone())
            .await?
        {
            continue;
        }
        // Which owning commit's package carries the binding is the one thing no
        // local state records — the audience picks the package within a commit, but
        // not which commit. Probe only that dimension.
        let mut binding = None;
        for owner in &owners {
            let commit = commit_verifier
                .load_ref(owner)
                .await
                .map_err(StoreReclaimError::Object)?;
            if let Some(package) =
                audience_blob_binding_package(commit.value(), blob.locator().audience())
            {
                binding = Some((package, owner.clone()));
                break;
            }
        }
        let Some((package, activation)) = binding else {
            tracing::debug!(
                blob = %crate::sync::remote_object::remote_object_id(blob.object()),
                "skip orphaned blob whose owning commits name no package for its audience",
            );
            continue;
        };
        let target = AudienceBlobReclaimTarget {
            blob,
            package,
            activation,
        };
        Box::pin(prepare_reclaim_authorization(
            database,
            device_id,
            identity_signer,
            membership,
            commit_verifier,
            ReclaimClaim::AudienceBlob(AudienceBlobReclaimClaim { target }),
        ))
        .await?;
    }
    Ok(())
}

/// Authorize deletion of every Circle snapshot image a later generation of the
/// same device's stream has superseded: that later generation's cut strictly
/// dominates the reclaimed one and every device holding active Circle access has
/// acknowledged it, so no reader will ever install from the older image again.
/// Only images are enumerated — the metadata chain is what a reader walks to find
/// any generation at all, so it is never a target.
#[allow(clippy::too_many_arguments)]
async fn prepare_circle_snapshot_image_reclaim_authorizations(
    database: &StoreDatabase,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    circle_id: CircleId,
    streams: &[CircleSnapshotStream],
    stable: &[SelectedCircleSnapshot],
    commit_verifier: &mut StoreCommitVerifier<'_>,
) -> Result<(), StoreReclaimError> {
    for stream in streams {
        for (reference, meta) in &stream.generations {
            let Some(superseding) = stable.iter().find(|candidate| {
                candidate.author_registration == stream.author_registration
                    && candidate.reference.generation > reference.generation
                    && snapshot_supersedes_seed(
                        &candidate.meta.bootstrap.coverage,
                        &meta.bootstrap.coverage,
                    )
            }) else {
                continue;
            };
            let target = CircleSnapshotImageReclaimTarget {
                circle_id,
                snapshot_author: stream.author_registration.clone(),
                control: meta.control.clone(),
                snapshot: reference.clone(),
                image: meta.bootstrap.image.clone(),
            };
            if database
                .circle_image_is_retained_for_replay(circle_id, target.image.clone())
                .await?
            {
                continue;
            }
            Box::pin(prepare_reclaim_authorization(
                database,
                device_id,
                identity_signer,
                membership,
                commit_verifier,
                ReclaimClaim::CircleSnapshotImage(CircleSnapshotImageReclaimClaim {
                    target,
                    superseding: superseding.reference.clone(),
                }),
            ))
            .await?;
        }
    }
    Ok(())
}

/// Enumerate every reclaimable Circle package: for each Circle this device holds
/// active access to, select the maximal acknowledgement-stable Circle snapshot,
/// and authorize each package its cut covers that is not still a retained replay
/// input. Mirrors the Store package pass over Circle coverage evidence.
#[allow(clippy::too_many_arguments)]
async fn prepare_circle_reclaim_authorizations(
    database: &StoreDatabase,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
    commit_verifier: &mut StoreCommitVerifier<'_>,
) -> Result<(), StoreReclaimError> {
    for input in database.circle_acknowledgement_publication_inputs().await? {
        let circle_id = input.circle_id;
        let control = input.control;
        // A package beyond its epoch's accepted cutoff never materializes anywhere,
        // so it needs no snapshot coverage and is enumerated whether or not this
        // Circle has a stable snapshot.
        Box::pin(prepare_beyond_cutoff_circle_reclaim_authorizations(
            database,
            device_id,
            identity_signer,
            membership,
            circle_id,
            &control,
            commit_verifier,
        ))
        .await?;
        // Both remaining passes read the same evidence: every device's snapshot
        // stream and which of its generations every active-access device has
        // acknowledged. Read it once.
        let streams = Box::pin(load_circle_snapshot_streams(
            database,
            commit_verifier,
            circle_id,
            &control,
            registrations,
        ))
        .await?;
        let stable = Box::pin(stable_circle_snapshots(
            database,
            commit_verifier,
            circle_id,
            &control,
            &streams,
        ))
        .await?;
        let selected = maximal_stable_circle_snapshot(&stable);
        Box::pin(prepare_circle_bootstrap_reclaim_authorizations(
            database,
            device_id,
            identity_signer,
            membership,
            circle_id,
            &control,
            selected,
            commit_verifier,
        ))
        .await?;
        // A superseded snapshot generation's image is reclaimable on its own
        // stream's evidence, independent of which snapshot covers the packages.
        Box::pin(prepare_circle_snapshot_image_reclaim_authorizations(
            database,
            device_id,
            identity_signer,
            membership,
            circle_id,
            &streams,
            &stable,
            commit_verifier,
        ))
        .await?;
        let Some(selected) = selected else {
            continue;
        };
        let targets = exact_circle_package_targets(
            commit_verifier,
            circle_id,
            &selected.meta.bootstrap.coverage,
        )
        .await?;
        for (commit, package) in targets {
            if database
                .circle_package_is_retained_for_replay(package.clone(), commit.clone())
                .await?
            {
                continue;
            }
            Box::pin(prepare_reclaim_authorization(
                database,
                device_id,
                identity_signer,
                membership,
                commit_verifier,
                ReclaimClaim::CirclePackage(CirclePackageReclaimClaim::SnapshotCovered(
                    CirclePackageSnapshotCoverageClaim {
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
                    },
                )),
            ))
            .await?;
        }
    }
    Ok(())
}

#[derive(Clone)]
struct SelectedCircleSnapshot {
    author_registration: StoreDeviceRegistrationRef,
    reference: CircleSnapshotRef,
    meta: crate::sync::store_commit::CircleSnapshotMeta,
    acknowledgements: Vec<CircleAckRef>,
}

/// One device's per-Circle snapshot stream, read in generation order.
struct CircleSnapshotStream {
    author_registration: StoreDeviceRegistrationRef,
    generations: Vec<(
        CircleSnapshotRef,
        crate::sync::store_commit::CircleSnapshotMeta,
    )>,
}

/// Every active device's Circle snapshot stream, decrypted under the current
/// control's retained keyring — which resolves every epoch the reader holds, since
/// a device's stream may span epochs that rotated away and a single-epoch key
/// cannot decrypt the older ones.
async fn load_circle_snapshot_streams(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    circle_id: CircleId,
    current_control: &CircleControlCoord,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<Vec<CircleSnapshotStream>, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    let encryption = database
        .circle_package_access(circle_id, current_control.clone())
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} snapshot key is not resolvable from retained controls"
            ))
        })?
        .encryption;
    let mut streams = Vec::new();
    for (registration_ref, registration) in registrations {
        // A device's snapshot stream is a create-once chain sealed under the epoch
        // active when each generation was authored. A generation sealed under an
        // epoch key this reader cannot resolve (one that rotated away before the
        // reader held it) is not current-epoch coverage; skip the stream — reclaim
        // is best-effort and idempotent, so a later cycle retries once coverage is
        // resolvable. Only a decryptable stream contributes coverage evidence.
        let generations = match super::snapshot::load_circle_snapshot_stream_refs(
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
        streams.push(CircleSnapshotStream {
            author_registration: registration_ref.clone(),
            generations,
        });
    }
    Ok(streams)
}

/// Every Circle snapshot generation, across every active device's stream, whose
/// cut every device holding active Circle access has acknowledged.
async fn stable_circle_snapshots(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    circle_id: CircleId,
    current_control: &CircleControlCoord,
    streams: &[CircleSnapshotStream],
) -> Result<Vec<SelectedCircleSnapshot>, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    let mut stable = Vec::new();
    for stream in streams {
        for (reference, meta) in &stream.generations {
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
                    author_registration: stream.author_registration.clone(),
                    reference: reference.clone(),
                    meta: meta.clone(),
                    acknowledgements,
                });
            }
        }
    }
    Ok(stable)
}

/// The maximal stable Circle snapshot: the one whose cut no other stable
/// snapshot strictly dominates.
fn maximal_stable_circle_snapshot(
    stable: &[SelectedCircleSnapshot],
) -> Option<&SelectedCircleSnapshot> {
    let coverages: Vec<&CommitFrontier> = stable
        .iter()
        .map(|candidate| &candidate.meta.bootstrap.coverage)
        .collect();
    stable
        .iter()
        .filter(|candidate| {
            !coverages.iter().any(|other| {
                super::snapshot::coverage_dominates(other, &candidate.meta.bootstrap.coverage)
            })
        })
        .max_by_key(|candidate| candidate.reference.snapshot_hash)
}

/// The maximal acknowledgement-stable Circle snapshot across every active device's
/// stream: its cut is dominated by no other stable snapshot, and every device
/// holding active Circle access has acknowledged coverage dominating that cut.
async fn choose_circle_snapshot(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    circle_id: CircleId,
    current_control: &CircleControlCoord,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<Option<SelectedCircleSnapshot>, StoreReclaimError> {
    let streams = Box::pin(load_circle_snapshot_streams(
        database,
        commit_verifier,
        circle_id,
        current_control,
        registrations,
    ))
    .await?;
    let stable = Box::pin(stable_circle_snapshots(
        database,
        commit_verifier,
        circle_id,
        current_control,
        &streams,
    ))
    .await?;
    Ok(maximal_stable_circle_snapshot(&stable).cloned())
}

/// Every Circle package addressed to `circle_id` whose activating commit lies at
/// or before `coverage`, paired with its exact activation. Walks the covered
/// commit history exactly like the Store package enumeration.
async fn exact_circle_package_targets(
    commit_verifier: &mut StoreCommitVerifier<'_>,
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
            let commit = commit_verifier
                .load_ref(&reference)
                .await
                .map_err(StoreReclaimError::Object)?;
            if let Some(package) = commit
                .value()
                .circle_packages()
                .iter()
                .find(|package| package.circle_id == circle_id)
            {
                targets.insert(reference.clone(), package.clone());
            }
            cursor = commit.value().order.predecessor().cloned();
        }
    }
    Ok(targets.into_iter().collect())
}

/// A stable Circle snapshot strictly supersedes a bootstrap seed when its cut
/// covers the seed and is not equal to it — the recipient has moved to a later
/// sufficient snapshot, not merely re-published coverage at the seed's own cut.
/// The strict inequality is load-bearing: a snapshot whose cut equals the seed
/// leaves the recipient exactly at its bootstrap and must not reclaim it.
fn snapshot_supersedes_seed(cut: &CommitFrontier, seed: &CommitFrontier) -> bool {
    cut.covers(seed) && cut != seed
}

/// Authorize deletion of every Circle bootstrap image no recipient still needs.
/// Each device's activated acknowledgement names the exact coverage its
/// projection was seeded from; the seed image is superseded either when a stable
/// Circle snapshot's cut strictly dominates it — the "later sufficient snapshot"
/// every active-access device acknowledged — while the recipient still holds
/// access, or when the recipient's owner lost Circle authority under a successor
/// control. The coverage the Owner deletes is taken from the recipient's own
/// signed acknowledgement, never fabricated.
#[allow(clippy::too_many_arguments)]
async fn prepare_circle_bootstrap_reclaim_authorizations(
    database: &StoreDatabase,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    circle_id: CircleId,
    current_control: &CircleControlCoord,
    selected: Option<&SelectedCircleSnapshot>,
    commit_verifier: &mut StoreCommitVerifier<'_>,
) -> Result<(), StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
    let roster = database.circle_current_roster_members(circle_id).await?;
    let retained_controls = database.retained_circle_control_coords(circle_id).await?;
    // The maximal acknowledgement-stable Circle snapshot cut, if any. A seed a
    // still-active recipient holds is superseded only when this cut strictly
    // dominates it — a later sufficient snapshot every active device acknowledged.
    let stable_cut = selected.map(|selected| &selected.meta.bootstrap.coverage);
    for acknowledgement in database.activated_circle_acks(circle_id).await? {
        let ack =
            match super::acknowledgements::load_circle_acknowledgement_under_retained_controls(
                database,
                storage,
                &root,
                &acknowledgement,
                current_control,
                &retained_controls,
            )
            .await
            {
                Ok(ack) => ack,
                Err(error) => {
                    // A device stream sealed under an epoch this reader cannot resolve
                    // is not current coverage evidence; reclaim is best-effort and a
                    // later cycle retries once the key is resolvable.
                    tracing::debug!(
                        circle_id = %circle_id,
                        "skip Circle acknowledgement for bootstrap reclaim: {error}"
                    );
                    continue;
                }
            };
        let Some(coverage) = ack.seeded_from.clone() else {
            // A founder/source device never seeded from an image — nothing to reclaim.
            continue;
        };
        let recipient = database
            .activated_store_device_registration(acknowledgement.registration.clone())
            .await?;
        let recipient_active = roster.contains(&recipient.author_pubkey);
        let seed = &coverage.bootstrap.coverage;
        let superseded_by_snapshot = stable_cut
            .as_ref()
            .is_some_and(|cut| snapshot_supersedes_seed(cut, seed));
        let proof = if recipient_active {
            if superseded_by_snapshot {
                CircleBootstrapReclaimProof::RecipientCoverage {
                    acknowledgement: acknowledgement.clone(),
                }
            } else {
                // No later sufficient snapshot supersedes the recipient's live seed.
                continue;
            }
        } else if database
            .circle_control_covers_strictly(circle_id, current_control, &coverage.control)
            .await?
        {
            CircleBootstrapReclaimProof::LostAuthority {
                acknowledgement: acknowledgement.clone(),
                successor_control: current_control.clone(),
            }
        } else {
            continue;
        };
        let target = CircleBootstrapImageReclaimTarget { coverage };
        if database
            .circle_bootstrap_image_is_retained_for_replay(target.coverage.clone())
            .await?
        {
            continue;
        }
        Box::pin(prepare_reclaim_authorization(
            database,
            device_id,
            identity_signer,
            membership,
            commit_verifier,
            ReclaimClaim::CircleBootstrapImage(CircleBootstrapImageReclaimClaim { target, proof }),
        ))
        .await?;
    }
    Ok(())
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
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    claim: ReclaimClaim,
) -> Result<(), StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
    if reclaim_target_is_recorded(database, &claim.target()).await? {
        return Ok(());
    }
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
    verify_reclaim_evidence(database, commit_verifier, &evidence).await?;
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
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    commit_verifier: &mut StoreCommitVerifier<'_>,
) -> Result<u64, StoreReclaimError> {
    let storage = commit_verifier.storage();
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
                    Box::pin(execute_reclaim_delete(database, commit_verifier, operation)).await?;
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
                        commit_verifier,
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
    commit_verifier: &mut StoreCommitVerifier<'_>,
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
    let storage = commit_verifier.storage();
    let target =
        verify_authorized_reclaim(database, commit_verifier, authorization, activation).await?;
    if target_is_retained_for_replay(database, &target).await? {
        return Err(StoreReclaimError::Authorization(
            "reclaim target remains retained for accepted replay".to_string(),
        ));
    }
    // A row blob has no protocol domain: it is addressed by its locator, so its
    // exact delete goes through the blob primitive rather than the protocol one.
    match &target {
        ReclaimTarget::AudienceBlob(blob) => storage.delete_blob_object(&blob.blob).await,
        _ => storage.delete_protocol_object(target.object()).await,
    }
    .map_err(|source| StoreReclaimError::Delete {
        activation: target.activation().object().stored_hash(),
        source,
    })?;
    verify_reclaim_target_absent(database, commit_verifier, &target).await?;
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
        ReclaimTarget::CircleBootstrapImage(target) => Ok(database
            .circle_bootstrap_image_is_retained_for_replay(target.coverage.clone())
            .await?),
        ReclaimTarget::CircleSnapshotImage(target) => Ok(database
            .circle_image_is_retained_for_replay(target.circle_id, target.image.clone())
            .await?),
        ReclaimTarget::AudienceBlob(target) => Ok(database
            .audience_blob_is_retained_for_replay(target.blob.clone())
            .await?),
    }
}

async fn verify_authorized_reclaim(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    authorization_ref: &ReclaimAuthorizationRef,
    activation: &journal::ReclaimCommitActivation,
) -> Result<ReclaimTarget, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
    let opened = crate::sync::store_objects::load_reclaim_authorization_ref(
        storage,
        &root,
        authorization_ref,
    )
    .await?;
    verify_reclaim_authorization_activation(
        database,
        commit_verifier,
        authorization_ref,
        activation,
    )
    .await?;
    verify_reclaim_evidence(database, commit_verifier, &opened.evidence.value).await
}

async fn verify_reclaim_authorization_activation(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    authorization: &ReclaimAuthorizationRef,
    activation: &journal::ReclaimCommitActivation,
) -> Result<(), StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
    activation
        .validate()
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    let commit_ref = activation.commit();
    let verified_commit = commit_verifier
        .load_ref(commit_ref)
        .await
        .map_err(StoreReclaimError::Object)?;
    let commit_value = verified_commit.value();
    let author = verified_commit.author();
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
        author,
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
        &root,
        &commit_value.author_registration,
        author,
        commit_verifier,
        Some(&verified_commit),
    )
    .await?;
    if accepted_head.as_ref() != Some(head) {
        return Err(StoreReclaimError::Authorization(
            "reclaim activation head is not the exact accepted stream position".to_string(),
        ));
    }
    super::pull::verify_merge_commit_currently_materialized(database, commit_verifier, commit)
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
}

/// Re-verify a reclaim's eligibility from its signed evidence and current live
/// state, returning the exact object the authorization may delete. Runs both at
/// prepare time and again before deletion, so a change in coverage, acknowledgement,
/// or replay retention since authoring fails the delete loud.
async fn verify_reclaim_evidence(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    evidence: &ReclaimEvidence,
) -> Result<ReclaimTarget, StoreReclaimError> {
    let root = commit_verifier.root().clone();
    evidence
        .verify()
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    if evidence.store_root_hash != root.store_root_hash {
        return Err(StoreReclaimError::Authorization(
            "reclaim evidence belongs to another Store root".to_string(),
        ));
    }
    match &evidence.claim {
        ReclaimClaim::StorePackage(claim) => {
            let activation = commit_verifier.load_ref(&claim.target.activation).await?;
            Ok(ReclaimTarget::StorePackage(
                verify_store_package_reclaim_claim(commit_verifier, &activation, claim).await?,
            ))
        }
        ReclaimClaim::CirclePackage(claim) => {
            let activation = commit_verifier.load_ref(&claim.target().activation).await?;
            Ok(ReclaimTarget::CirclePackage(
                verify_circle_package_reclaim_claim(database, commit_verifier, &activation, claim)
                    .await?,
            ))
        }
        ReclaimClaim::CircleBootstrapImage(claim) => {
            let activation = commit_verifier
                .load_ref(&claim.target.coverage.activation_commit)
                .await?;
            Ok(ReclaimTarget::CircleBootstrapImage(
                verify_circle_bootstrap_image_reclaim_claim(
                    database,
                    commit_verifier,
                    &activation,
                    claim,
                )
                .await?,
            ))
        }
        ReclaimClaim::CircleSnapshotImage(claim) => Ok(ReclaimTarget::CircleSnapshotImage(
            verify_circle_snapshot_image_reclaim_claim(database, commit_verifier, claim).await?,
        )),
        ReclaimClaim::AudienceBlob(claim) => {
            let activation = commit_verifier.load_ref(&claim.target.activation).await?;
            Ok(ReclaimTarget::AudienceBlob(
                verify_audience_blob_reclaim_claim(database, commit_verifier, &activation, claim)
                    .await?,
            ))
        }
    }
}

/// Re-verify that a row blob is free. The package the claim names is re-read from
/// storage and must itself bind this exact blob — the signed statement that
/// published it — and the orphan test is re-run against this device's own
/// materialized rows rather than taken from the claim.
async fn verify_audience_blob_reclaim_claim(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    activation: &VerifiedStoreBatchCommit,
    claim: &AudienceBlobReclaimClaim,
) -> Result<AudienceBlobReclaimTarget, StoreReclaimError> {
    if audience_blob_binding_package(activation.value(), claim.target.blob.locator().audience())
        .as_ref()
        != Some(&claim.target.package)
    {
        return Err(StoreReclaimError::Authorization(
            "audience blob reclaim activation names another package".to_string(),
        ));
    }
    let package = read_audience_blob_binding_package(
        database,
        commit_verifier,
        &claim.target.package,
        &claim.target.activation,
    )
    .await?;
    if !package
        .blob_bindings()
        .iter()
        .any(|binding| binding.blob() == &claim.target.blob)
    {
        return Err(StoreReclaimError::Authorization(
            "audience blob reclaim package does not bind the target blob".to_string(),
        ));
    }
    if !database
        .stored_blob_is_row_orphaned(claim.target.blob.clone())
        .await?
    {
        return Err(StoreReclaimError::Authorization(
            "a live row still binds the audience blob as a remote reference".to_string(),
        ));
    }
    Ok(claim.target.clone())
}

/// Read back the exact package body that published a blob. A Store package is
/// sealed to the Store and a Circle package to its epoch, so the audience selects
/// both the read context and the semantic prefix.
async fn read_audience_blob_binding_package(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    package: &AudienceBlobBindingPackage,
    activation: &StoreBatchCommitRef,
) -> Result<crate::sync::audience_package::AudiencePackage, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    let (context, prefix, object) = match package {
        AudienceBlobBindingPackage::Store(package) => (
            ProtocolObjectContext::store_encrypted(
                root.store_root_hash,
                ProtocolObjectDomain::StorePackage,
            ),
            crate::sync::store_commit::package_semantic_prefix(
                package.candidate_family,
                &activation.coord.stream_id.to_string(),
                activation.coord.sequence(),
                package.content_hash,
            ),
            &package.object,
        ),
        AudienceBlobBindingPackage::Circle(package) => {
            let access = database
                .circle_package_access(package.circle_id, package.control.clone())
                .await?
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "audience blob reclaim package key is not resolvable".to_string(),
                    )
                })?;
            (
                ProtocolObjectContext::circle(
                    root.store_root_hash,
                    ProtocolObjectDomain::CirclePackage,
                    access.encryption,
                ),
                crate::sync::store_commit::circle_package_semantic_prefix(
                    package.circle_id,
                    package.package.candidate_family,
                    &activation.coord.stream_id.to_string(),
                    activation.coord.sequence(),
                    package.package.content_hash,
                ),
                &package.package.object,
            )
        }
    };
    let bytes = storage
        .read_protocol_object(&context, object, &prefix)
        .await?;
    crate::sync::audience_package::AudiencePackage::parse(&bytes)
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
}

/// Re-verify that a later generation of the reclaimed image's own stream
/// supersedes it. The stream is re-walked from generation zero, so both the
/// reclaimed generation and the named superseding one are re-read from their own
/// signed metadata; the superseding generation must carry a cut that strictly
/// dominates the reclaimed one, and every device holding active Circle access must
/// have acknowledged that cut. Nothing the claim asserts about coverage,
/// stability, or the image itself is taken on trust.
async fn verify_circle_snapshot_image_reclaim_claim(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    claim: &CircleSnapshotImageReclaimClaim,
) -> Result<CircleSnapshotImageReclaimTarget, StoreReclaimError> {
    let circle_id = claim.target.circle_id;
    let current_control = database
        .current_circle_control(circle_id)
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} has no active control for snapshot image reclaim"
            ))
        })?;
    let author = database
        .activated_store_device_registration(claim.target.snapshot_author.clone())
        .await?;
    let author_stream = [(claim.target.snapshot_author.clone(), author)];
    let streams = Box::pin(load_circle_snapshot_streams(
        database,
        commit_verifier,
        circle_id,
        &current_control,
        &author_stream,
    ))
    .await?;
    let [stream] = streams.as_slice() else {
        return Err(StoreReclaimError::Authorization(
            "Circle snapshot reclaim author's stream is not readable".to_string(),
        ));
    };
    let generation = stream
        .generations
        .iter()
        .find(|(reference, _)| *reference == claim.target.snapshot)
        .ok_or_else(|| {
            StoreReclaimError::Authorization(
                "Circle snapshot reclaim target is absent from its author's stream".to_string(),
            )
        })?;
    if generation.1.circle_id != circle_id
        || generation.1.control != claim.target.control
        || generation.1.bootstrap.image != claim.target.image
    {
        return Err(StoreReclaimError::Authorization(
            "Circle snapshot reclaim target differs from its own signed generation".to_string(),
        ));
    }
    let superseding = stream
        .generations
        .iter()
        .find(|(reference, _)| *reference == claim.superseding)
        .ok_or_else(|| {
            StoreReclaimError::Authorization(
                "Circle snapshot reclaim superseding generation is absent from the same stream"
                    .to_string(),
            )
        })?;
    if !snapshot_supersedes_seed(
        &superseding.1.bootstrap.coverage,
        &generation.1.bootstrap.coverage,
    ) {
        return Err(StoreReclaimError::Authorization(
            "Circle snapshot reclaim superseding generation does not strictly dominate the reclaimed cut"
                .to_string(),
        ));
    }
    if super::acknowledgements::stable_circle_acks_dominating_on(
        database,
        commit_verifier.storage(),
        commit_verifier.root(),
        circle_id,
        &current_control,
        &superseding.1.bootstrap.coverage,
    )
    .await
    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
    .is_none()
    {
        return Err(StoreReclaimError::Authorization(
            "Circle snapshot reclaim superseding generation is not acknowledgement-stable"
                .to_string(),
        ));
    }
    Ok(claim.target.clone())
}

async fn verify_store_package_reclaim_claim(
    commit_verifier: &mut StoreCommitVerifier<'_>,
    activation: &VerifiedStoreBatchCommit,
    claim: &StorePackageReclaimClaim,
) -> Result<StorePackageReclaimTarget, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
    let author = crate::sync::store_objects::load_registration_ref(
        storage,
        &root,
        &claim.covering_snapshot.author_registration,
    )
    .await?;
    let (reference, metadata) = super::snapshot::load_store_snapshot_ref(
        storage,
        &root,
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
    let authority = match super::verify_store_snapshot_stability(storage, &root, &snapshot).await {
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
    if activation.value().store_package() != Some(&claim.target.package)
        || !snapshot_covers_target(
            commit_verifier,
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

async fn verify_circle_package_reclaim_claim(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    activation: &VerifiedStoreBatchCommit,
    claim: &CirclePackageReclaimClaim,
) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
    match claim {
        CirclePackageReclaimClaim::SnapshotCovered(claim) => {
            Box::pin(verify_circle_package_snapshot_coverage_claim(
                database,
                commit_verifier,
                activation,
                claim,
            ))
            .await
        }
        CirclePackageReclaimClaim::BeyondEpochCutoff(claim) => {
            verify_circle_package_beyond_cutoff_claim(database, activation, claim).await
        }
    }
}

/// Re-verify that a Circle package lies beyond its epoch's accepted close cutoff.
/// The named successor control must be a retained activation whose closed-epoch
/// origin names the epoch the package's own control belongs to, and the same
/// replay-epoch predicate the pull path applies must refuse the package. A package
/// the cutoff accepts, or one whose control the cutoff conflicts with, is not
/// eligible under this arm and fails loud rather than falling back to coverage.
async fn verify_circle_package_beyond_cutoff_claim(
    database: &StoreDatabase,
    activation: &VerifiedStoreBatchCommit,
    claim: &CirclePackageBeyondCutoffClaim,
) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
    if !activation
        .value()
        .circle_packages()
        .contains(&claim.target.package)
    {
        return Err(StoreReclaimError::Authorization(
            "Circle package reclaim activation names another package".to_string(),
        ));
    }
    let circle_id = claim.target.package.circle_id;
    let successor = database
        .verified_circle_activation(circle_id, claim.successor_control.clone())
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} beyond-cutoff successor control is not a retained activation"
            ))
        })?;
    let CircleControlState::ActiveEpoch(active) = successor.control.value.state() else {
        return Err(StoreReclaimError::Authorization(
            "Circle beyond-cutoff successor control is not an activated epoch".to_string(),
        ));
    };
    let CircleEpochOrigin::Closed {
        closed_epoch_id, ..
    } = &active.common.origin
    else {
        return Err(StoreReclaimError::Authorization(
            "Circle beyond-cutoff successor epoch did not close a predecessor".to_string(),
        ));
    };
    // The package must be addressed to the epoch that close cut off, not to some
    // other epoch that merely happens to precede the successor.
    let package_control = database
        .verified_circle_activation(circle_id, claim.target.package.control.clone())
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} package control is not a retained activation"
            ))
        })?;
    if package_control.control.value.epoch_id() != *closed_epoch_id {
        return Err(StoreReclaimError::Authorization(
            "Circle beyond-cutoff package belongs to another epoch than the one closed".to_string(),
        ));
    }
    // The same predicate the pull path uses to skip a package beyond its accepted
    // cutoff; a package it permits is live history and is never eligible here.
    if database
        .circle_replay_epoch_index()
        .await?
        .permits(
            &claim.target.activation,
            circle_id,
            &claim.target.package.control,
        )
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
    {
        return Err(StoreReclaimError::Authorization(
            "Circle package lies within its accepted epoch cutoff and is not reclaimable as beyond-cutoff"
                .to_string(),
        ));
    }
    Ok(claim.target.clone())
}

/// Re-verify a Circle package reclaim: a stable Circle snapshot on the same
/// Circle covers the package's activating commit, and every device holding
/// active Circle access has acknowledged coverage dominating the snapshot cut.
/// The acknowledgements are read under the Circle's current control, whose
/// retained keyring resolves epochs sealed before a rotation.
async fn verify_circle_package_snapshot_coverage_claim(
    database: &StoreDatabase,
    commit_verifier: &mut StoreCommitVerifier<'_>,
    activation: &VerifiedStoreBatchCommit,
    claim: &CirclePackageSnapshotCoverageClaim,
) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root().clone();
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
        &root,
        &claim.covering_snapshot.author_registration,
    )
    .await?;
    let stream = super::snapshot::load_circle_snapshot_stream_refs(
        storage,
        &root,
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
        &root,
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
    if !activation
        .value()
        .circle_packages()
        .contains(&claim.target.package)
        || !snapshot_covers_target(commit_verifier, cut, &claim.target.activation).await?
    {
        return Err(StoreReclaimError::Authorization(
            "Circle reclaim target is not the exact package covered by its snapshot".to_string(),
        ));
    }
    Ok(claim.target.clone())
}

/// Re-verify a Circle bootstrap image reclaim: the recipient's own activated
/// acknowledgement names the exact coverage being deleted (`seeded_from`), and
/// either the recipient advanced strictly past that seed while still holding
/// active access, or it lost authority under an activated successor control. The
/// acknowledgement is read under the Circle's current control, whose retained
/// keyring resolves the epoch it was sealed under even after a rotation.
async fn verify_circle_bootstrap_image_reclaim_claim(
    database: &StoreDatabase,
    commit_verifier: &StoreCommitVerifier<'_>,
    activation: &VerifiedStoreBatchCommit,
    claim: &CircleBootstrapImageReclaimClaim,
) -> Result<CircleBootstrapImageReclaimTarget, StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    if !activation
        .value()
        .circle_controls()
        .iter()
        .flat_map(|control| control.objects.access.iter())
        .any(|access| access.bootstrap.as_ref() == Some(&claim.target.coverage.bootstrap.image))
    {
        return Err(StoreReclaimError::Authorization(
            "Circle bootstrap reclaim activation names another image".to_string(),
        ));
    }
    let circle_id = claim.target.coverage.circle_id;
    let current_control = database
        .current_circle_control(circle_id)
        .await?
        .ok_or_else(|| {
            StoreReclaimError::Authorization(format!(
                "Circle {circle_id} has no active control for bootstrap reclaim"
            ))
        })?;
    let retained_controls = database.retained_circle_control_coords(circle_id).await?;
    let acknowledgement_ref = claim.proof.acknowledgement();
    let acknowledgement =
        super::acknowledgements::load_circle_acknowledgement_under_retained_controls(
            database,
            storage,
            root,
            acknowledgement_ref,
            &current_control,
            &retained_controls,
        )
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
    // The recipient's signed acknowledgement is the sole authority for the coverage
    // the Owner deletes: the target must be exactly what the recipient said it was
    // seeded from, so the Owner never fabricates the image, cut, or activation.
    if acknowledgement.seeded_from.as_ref() != Some(&claim.target.coverage) {
        return Err(StoreReclaimError::Authorization(
            "Circle bootstrap reclaim target differs from the recipient's signed seed coverage"
                .to_string(),
        ));
    }
    let recipient = database
        .activated_store_device_registration(acknowledgement_ref.registration.clone())
        .await?;
    let roster = database.circle_current_roster_members(circle_id).await?;
    match &claim.proof {
        CircleBootstrapReclaimProof::RecipientCoverage { .. } => {
            if !roster.contains(&recipient.author_pubkey) {
                return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap recipient-coverage proof names a device outside the current roster"
                        .to_string(),
                ));
            }
            // Re-derive the maximal acknowledgement-stable Circle snapshot and require
            // its cut to strictly dominate the seed: the later sufficient snapshot the
            // recipient (with every active device) acknowledged past its bootstrap.
            let registrations = database
                .activated_store_device_registration_records()
                .await
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
            let seed = &claim.target.coverage.bootstrap.coverage;
            let superseded = Box::pin(choose_circle_snapshot(
                database,
                commit_verifier,
                circle_id,
                &current_control,
                &registrations,
            ))
            .await?
            .is_some_and(|selected| {
                snapshot_supersedes_seed(&selected.meta.bootstrap.coverage, seed)
            });
            if !superseded {
                return Err(StoreReclaimError::Authorization(
                    "no acknowledgement-stable Circle snapshot strictly dominates the recipient's seed coverage"
                        .to_string(),
                ));
            }
        }
        CircleBootstrapReclaimProof::LostAuthority {
            successor_control, ..
        } => {
            if roster.contains(&recipient.author_pubkey) {
                return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority proof names a device still in the current roster"
                        .to_string(),
                ));
            }
            if *successor_control != current_control {
                return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority successor is not the current activated control"
                        .to_string(),
                ));
            }
            if !database
                .circle_control_covers_strictly(
                    circle_id,
                    successor_control,
                    &claim.target.coverage.control,
                )
                .await?
            {
                return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority successor does not strictly cover the seed control"
                        .to_string(),
                ));
            }
        }
    }
    Ok(claim.target.clone())
}

async fn snapshot_covers_target(
    commit_verifier: &mut super::pull::StoreCommitVerifier<'_>,
    coverage: &CommitFrontier,
    target: &StoreBatchCommitRef,
) -> Result<bool, StoreReclaimError> {
    let covering = coverage.0.get(&target.coord.stream_id);
    match covering {
        Some(covering) => position_covers(commit_verifier, covering, target).await,
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
        // Scoped to the publication alone: the arms below re-derive a plan,
        // which takes this same turn.
        let outcome = {
            let _authorship = database.author_own_stream().await;
            Box::pin(super::operations::publish_prepared_store_operation(
                database, storage, candidate,
            ))
            .await?
        };
        match outcome {
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
    commit_verifier: &StoreCommitVerifier<'_>,
    device_id: &str,
    identity_signer: &UserKeypair,
    membership: &MembershipChain,
    operation: journal::DurableStoreReclaimOperation,
) -> Result<(), StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
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
    let opened =
        crate::sync::store_objects::load_reclaim_authorization_ref(storage, root, authorization)
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
    commit_verifier: &StoreCommitVerifier<'_>,
    target: &ReclaimTarget,
) -> Result<(), StoreReclaimError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    // A row blob is addressed by its locator, not by a protocol domain prefix, so
    // its absence is confirmed through the same exact-object verification the blob
    // primitives use.
    if let ReclaimTarget::AudienceBlob(blob) = target {
        return match storage.verify_blob_object(&blob.blob).await {
            Err(StorageError::NotFound(_)) => Ok(()),
            Ok(()) => Err(StoreReclaimError::Authorization(
                "reclaim target remains readable after exact deletion".to_string(),
            )),
            Err(error) => Err(StoreReclaimError::Storage(error)),
        };
    }
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
        ReclaimTarget::CircleBootstrapImage(target) => {
            // A Circle bootstrap image is sealed under the Circle epoch key of the
            // control it activated under; resolving that access only builds the read
            // context — a deleted object reads back absent before any decryption. Its
            // readback prefix is the image object's own logical key without the domain
            // extension, so no recipient-sealed leaf field is needed to confirm absence.
            let access = database
                .circle_package_access(target.coverage.circle_id, target.coverage.control.clone())
                .await?
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaim target Circle bootstrap image key is not resolvable".to_string(),
                    )
                })?;
            (
                ProtocolObjectContext::circle(
                    root.store_root_hash,
                    ProtocolObjectDomain::CircleBootstrapImage,
                    access.encryption,
                ),
                crate::sync::store_commit::semantic_prefix_from_exact_object(
                    &target.coverage.bootstrap.image.object,
                    crate::sync::storage::ProtectedObjectDomain::CircleBootstrapImage.extension(),
                )
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?,
            )
        }
        ReclaimTarget::CircleSnapshotImage(target) => {
            // A standalone Circle snapshot image is sealed under the epoch key of the
            // control its generation was authored under; resolving that access only
            // builds the read context — a deleted object reads back absent before any
            // decryption.
            let access = database
                .circle_package_access(target.circle_id, target.control.clone())
                .await?
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaim target Circle snapshot image key is not resolvable".to_string(),
                    )
                })?;
            (
                ProtocolObjectContext::circle(
                    root.store_root_hash,
                    ProtocolObjectDomain::CircleSnapshotImage,
                    access.encryption,
                ),
                crate::sync::store_commit::semantic_prefix_from_exact_object(
                    &target.image.object,
                    crate::sync::storage::ProtectedObjectDomain::CircleSnapshotImage.extension(),
                )
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?,
            )
        }
        ReclaimTarget::AudienceBlob(_) => {
            return Err(StoreReclaimError::Authorization(
                "audience blob reclaim target has no protocol object prefix".to_string(),
            ));
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
    history_verifier: &mut super::pull::MergeHistoryVerifier<'_>,
    registrations: &[(StoreDeviceRegistrationRef, StoreDeviceRegistration)],
) -> Result<VerifiedReclaimSnapshot, StoreReclaimError> {
    let storage = history_verifier.storage();
    let root = history_verifier.root().clone();
    let mut authorized = Vec::new();
    for (registration_ref, registration) in registrations {
        for snapshot in super::snapshot::load_store_snapshot_stream(
            storage,
            &root,
            registration_ref,
            registration,
        )
        .await
        .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        {
            authorized.push(snapshot);
        }
    }
    let selected = match super::snapshot::select_maximal_stable_store_snapshot_with_history(
        history_verifier,
        authorized,
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
    commit_verifier: &mut super::pull::StoreCommitVerifier<'_>,
    covering: &StoreBatchCommitRef,
    covered: &StoreBatchCommitRef,
) -> Result<bool, StoreReclaimError> {
    super::pull::commit_position_covers(commit_verifier, covering, covered)
        .await
        .map_err(|error| match error {
            super::pull::CommitCoverageError::Object(error) => StoreReclaimError::Object(error),
            super::pull::CommitCoverageError::MissingAncestry { commit_hash } => {
                StoreReclaimError::MissingAncestry { commit_hash }
            }
        })
}

async fn exact_package_targets(
    commit_verifier: &mut StoreCommitVerifier<'_>,
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
            let commit = commit_verifier
                .load_ref(&reference)
                .await
                .map_err(StoreReclaimError::Object)?;
            if let Some(package) = commit.value().store_package().cloned() {
                targets.insert(reference.clone(), package);
            }
            cursor = commit.value().order.predecessor().cloned();
        }
    }
    Ok(targets.into_iter().collect())
}

pub(super) mod journal;

#[cfg(test)]
mod tests;
