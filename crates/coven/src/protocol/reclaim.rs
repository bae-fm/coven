//! Signed reclaim targets, claims, evidence, authorizations, and receipts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::keys::{self, UserKeypair};
use crate::protocol::circle::{CircleBootstrapCoverageRef, CircleControlCoord, CircleId};
use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::MembershipGrantId;
use crate::protocol::objects::ExactObjectRef;
use crate::protocol::store_commit::{
    CircleAckRef, CirclePackageRef, CircleSnapshotRef, ObjectHash, SnapshotImageRef, StoreAckRef,
    StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef, StorePackageRef,
    StoreProtocolError, StoreSnapshotLocator, STORE_PROTOCOL_VERSION,
};

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
    pub(crate) fn acknowledgement(&self) -> &CircleAckRef {
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
    ) -> Result<crate::protocol::remote_object::SnapshotObjectOwner, StoreProtocolError> {
        Ok(crate::protocol::remote_object::SnapshotObjectOwner {
            activation: crate::protocol::store_commit::circle_snapshot_stream_activation(
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

    pub(crate) fn remote_audience(&self) -> crate::protocol::blob::locator::RemoteAudience {
        match self {
            Self::Store(_) => crate::protocol::blob::locator::RemoteAudience::Store,
            Self::Circle(package) => {
                crate::protocol::blob::locator::RemoteAudience::Circle(package.circle_id)
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
    pub blob: crate::protocol::blob::locator::StoredBlobRef,
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
        crate::protocol::store_commit::domain_json(
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
        crate::protocol::store_commit::require_version(self.version)?;
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
        crate::protocol::store_commit::domain_json(
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
        crate::protocol::store_commit::require_version(self.version)?;
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
    pub provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
    pub signature: String,
}

#[derive(Serialize)]
struct ReclaimReceiptSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    authorization: &'a ReclaimAuthorizationRef,
    provider_admin_state: &'a StoreMembershipStateRef,
    provider_admin_grant: &'a crate::protocol::provider::ProviderAdminGrantId,
    executor: &'a StoreDeviceRegistrationRef,
}

impl ReclaimReceipt {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        authorization: ReclaimAuthorizationRef,
        provider_admin_state: StoreMembershipStateRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
        executor: StoreDeviceRegistrationRef,
        executor_registration: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        executor.verify_registration(executor_registration)?;
        crate::protocol::objects::verify_store_root(
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
        crate::protocol::store_commit::domain_json(
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
        crate::protocol::store_commit::require_version(self.version)?;
        self.executor.verify_registration(executor)?;
        crate::protocol::objects::verify_store_root(
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

pub(crate) fn reclaim_evidence_semantic_prefix(evidence_hash: ObjectHash) -> String {
    format!("store-v1/reclaim/evidence/{evidence_hash}")
}

pub(crate) fn reclaim_authorization_semantic_prefix(authorization_hash: ObjectHash) -> String {
    format!("store-v1/reclaim/authorizations/{authorization_hash}")
}

pub(crate) fn reclaim_receipt_semantic_prefix(receipt_hash: ObjectHash) -> String {
    format!("store-v1/reclaim/receipts/{receipt_hash}")
}

#[cfg(test)]
mod tests;
