use serde::{Deserialize, Serialize};

use crate::membership::MembershipGrantId;
use crate::objects::{ExactObjectRef, ObjectSlot};
use crate::provider::{
    ActivatedStoreMemberProviderAccessGrant, CrossPrincipalProbeChallenge,
    CrossPrincipalProbeReceipt, CrossPrincipalProbeResponse,
    DeviceJoinChallengePublicationAuthorization, ProviderAdminGrantRecord,
};
use crate::store_commit::{Signed, SignedBody};
use crate::{ProviderDeviceBinding, StoreProviderBinding};
use coven_keys::keys::{self, UserKeypair};

use super::device_join::{DeviceJoinAbandonmentRef, DeviceJoinAttemptId};
use super::{StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreRootRef};

use super::*;

/// A signed join-exchange value that contradicts itself, its signer, or the
/// exchange it extends. Workflow errors wrap it at the operation boundary.
#[derive(Debug, thiserror::Error)]
pub enum DeviceJoinExchangeError {
    #[error("device join signature is invalid")]
    InvalidSignature,
    #[error("device join offer does not name one active Store/member/provider authority")]
    OfferMismatch,
    #[error("device provider approval differs from its request or grant")]
    ApprovalMismatch,
    #[error("device registration request differs from its offer, approval, or reserved slots")]
    RegistrationRequestMismatch,
    #[error("device join attempt differs from its signed exchange")]
    AttemptMismatch,
    #[error("device join cleanup does not contain the unconditional canonical slot set")]
    CleanupMismatch,
    #[error("device join reserved slots are not distinct")]
    DuplicateReservedSlot,
    #[error("provider: {0}")]
    Provider(#[from] crate::provider::ProviderProbeError),
    #[error("{0}")]
    Storage(#[from] crate::objects::StorageError),
    #[error("{0}")]
    Protocol(#[from] super::StoreProtocolError),
}

const OFFER_DOMAIN: &[u8] = b"coven.device-join-offer.v1\0";
const ACCESS_REQUEST_DOMAIN: &[u8] = b"coven.device-provider-access-request.v1\0";
const APPROVAL_DOMAIN: &[u8] = b"coven.device-provider-admission-approval.v1\0";
const REGISTRATION_REQUEST_DOMAIN: &[u8] = b"coven.device-registration-request.v1\0";
const ABANDONMENT_DOMAIN: &[u8] = b"coven.device-join-abandonment.v1\0";

/// The wire body of a device-join offer. Every field here is signed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOfferBody {
    pub attempt_id: DeviceJoinAttemptId,
    pub member_pubkey: String,
    pub store_root: StoreRootRef,
    pub provider: StoreProviderBinding,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub provider_admin: Box<ProviderAdminGrantRecord>,
}

impl SignedBody for DeviceJoinOfferBody {
    const DOMAIN: &'static [u8] = OFFER_DOMAIN;
}

pub type DeviceJoinOffer = Signed<DeviceJoinOfferBody>;

impl DeviceJoinOfferBody {
    fn validate_shape(&self) -> Result<(), DeviceJoinExchangeError> {
        if self.member_pubkey.is_empty()
            || self.provider_admin.administrator != self.owner_registration
        {
            return Err(DeviceJoinExchangeError::OfferMismatch);
        }
        self.provider.validate()?;
        self.provider_admin
            .provider
            .validate_for(&self.provider)
            .map_err(DeviceJoinExchangeError::Storage)?;
        if let crate::provider::ProviderAdminGrantOrigin::Founder { root } =
            &self.provider_admin.created_at
        {
            if root != &self.store_root {
                return Err(DeviceJoinExchangeError::OfferMismatch);
            }
        }
        Ok(())
    }
}

impl DeviceJoinOffer {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        attempt_id: DeviceJoinAttemptId,
        member_pubkey: String,
        store_root: StoreRootRef,
        provider: StoreProviderBinding,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        provider_admin: ProviderAdminGrantRecord,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let body = DeviceJoinOfferBody {
            attempt_id,
            member_pubkey,
            store_root,
            provider,
            owner_registration,
            owner_grant,
            provider_admin: Box::new(provider_admin),
        };
        body.validate_shape()?;
        Ok(Signed::sign(body, owner_device_signer))
    }

    pub fn verify(&self, owner: &StoreDeviceRegistration) -> Result<(), DeviceJoinExchangeError> {
        self.body().validate_shape()?;
        self.owner_registration.verify_registration(owner)?;
        self.verify_by(&owner.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }

    pub(crate) fn offer_hash(&self) -> ObjectHash {
        self.hash()
    }
}

/// The wire body of a joining device's provider-access request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProviderAccessRequestBody {
    pub offer: Box<DeviceJoinOffer>,
    pub peer_provider: ProviderDeviceBinding,
    pub expected_registration: StoreDeviceRegistration,
    pub registration_slot: ObjectSlot,
}

impl SignedBody for DeviceProviderAccessRequestBody {
    const DOMAIN: &'static [u8] = ACCESS_REQUEST_DOMAIN;
}

pub type DeviceProviderAccessRequest = Signed<DeviceProviderAccessRequestBody>;

impl DeviceProviderAccessRequest {
    pub fn signed(
        offer: DeviceJoinOffer,
        peer_provider: ProviderDeviceBinding,
        expected_registration: StoreDeviceRegistration,
        registration_slot: ObjectSlot,
        member_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        if keys::public_key_hex(member_signer) != offer.member_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let body = DeviceProviderAccessRequestBody {
            offer: Box::new(offer),
            peer_provider,
            expected_registration,
            registration_slot,
        };
        body.validate_shape()?;
        Ok(Signed::sign(body, member_signer))
    }

    pub fn verify(&self, owner: &StoreDeviceRegistration) -> Result<(), DeviceJoinExchangeError> {
        self.offer.verify(owner)?;
        self.body().validate_shape()?;
        self.verify_by(&self.offer.member_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }

    pub fn request_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn cross_challenge_context(&self) -> crate::provider::CrossPrincipalChallengeContext {
        crate::provider::CrossPrincipalChallengeContext {
            root: self.offer.store_root.clone(),
            attempt_id: self.offer.attempt_id,
            access_request_hash: self.request_hash(),
            provider_admin_grant: self.offer.provider_admin.grant_id.clone(),
            owner_registration: self.offer.owner_registration.clone(),
            member_pubkey: self.offer.member_pubkey.clone(),
            administrator_binding: self.offer.provider_admin.provider.clone(),
            peer_binding: self.peer_provider.clone(),
        }
    }
}

impl DeviceProviderAccessRequestBody {
    fn validate_shape(&self) -> Result<(), DeviceJoinExchangeError> {
        let offer = &self.offer;
        self.peer_provider.validate_for(&offer.provider)?;
        if self.expected_registration.store_root != offer.store_root
            || self.expected_registration.author_pubkey != offer.member_pubkey
            || self.expected_registration.provider != self.peer_provider
        {
            return Err(DeviceJoinExchangeError::RegistrationRequestMismatch);
        }
        match &self.expected_registration.origin {
            crate::store_commit::StoreDeviceRegistrationOrigin::Join { attempt_id }
                if *attempt_id == offer.attempt_id => {}
            _ => return Err(DeviceJoinExchangeError::RegistrationRequestMismatch),
        }
        let slots = vec![
            self.registration_slot.clone(),
            self.expected_registration
                .acknowledgements
                .first_slot()
                .clone(),
        ];
        require_distinct_slots(&slots)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderAdmission {
    SamePrincipal,
    CrossPrincipal {
        access_grant: Box<ActivatedStoreMemberProviderAccessGrant>,
        challenge: CrossPrincipalProbeChallenge,
    },
}

/// The wire body of a provider administrator's admission approval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProviderAdmissionApprovalBody {
    pub request: Box<DeviceProviderAccessRequest>,
    pub admission: DeviceProviderAdmission,
}

impl SignedBody for DeviceProviderAdmissionApprovalBody {
    const DOMAIN: &'static [u8] = APPROVAL_DOMAIN;
}

pub type DeviceProviderAdmissionApproval = Signed<DeviceProviderAdmissionApprovalBody>;

impl DeviceProviderAdmissionApprovalBody {
    fn validate_shape(
        &self,
        store_root: &crate::objects::VerifiedObject<StoreProtocolRoot>,
        owner: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        let offer = &self.request.offer;
        if store_root.object != offer.store_root.object
            || store_root.value.object_hash() != offer.store_root.store_root_hash
            || store_root.value.descriptor.store_root_id() != offer.store_root.store_root_id
            || store_root.value.descriptor.provider != offer.provider
        {
            return Err(DeviceJoinExchangeError::ApprovalMismatch);
        }
        let same_principal = offer.provider_admin.provider == self.request.peer_provider;
        match &self.admission {
            DeviceProviderAdmission::SamePrincipal if same_principal => {}
            DeviceProviderAdmission::CrossPrincipal { access_grant, .. }
                if !same_principal
                    && access_grant.grant.member_pubkey == offer.member_pubkey
                    && access_grant.grant.provider == self.request.peer_provider
                    && access_grant.grant_ref.grant_id == access_grant.grant.grant_id
                    && access_grant.grant_ref.grant_hash == access_grant.grant.grant_hash()
                    && access_grant.grant.administrator_grant == offer.provider_admin.grant_id
                    && access_grant.grant.administrator == offer.provider_admin.administrator =>
            {
                access_grant.grant.verify(&offer.provider, owner)?;
            }
            _ => return Err(DeviceJoinExchangeError::ApprovalMismatch),
        }
        Ok(())
    }
}

impl DeviceProviderAdmissionApproval {
    pub fn access_grant(&self) -> Option<&ActivatedStoreMemberProviderAccessGrant> {
        match &self.admission {
            DeviceProviderAdmission::SamePrincipal => None,
            DeviceProviderAdmission::CrossPrincipal { access_grant, .. } => Some(access_grant),
        }
    }

    pub fn signed(
        request: DeviceProviderAccessRequest,
        admission: DeviceProviderAdmission,
        store_root: &crate::objects::VerifiedObject<StoreProtocolRoot>,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let body = DeviceProviderAdmissionApprovalBody {
            request: Box::new(request),
            admission,
        };
        body.validate_shape(store_root, owner)?;
        Ok(Signed::sign(body, owner_device_signer))
    }

    /// One registration answers the whole approval: the device that signed the
    /// offer is the device that holds the store's provider-administrator grant,
    /// so it is also the signer of this approval and of the access grant inside
    /// it.
    pub fn verify(
        &self,
        store_root: &crate::objects::VerifiedObject<StoreProtocolRoot>,
        owner: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        self.request.verify(owner)?;
        self.body().validate_shape(store_root, owner)?;
        self.verify_by(&owner.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_without_shape_validation_for_test(
        request: DeviceProviderAccessRequest,
        admission: DeviceProviderAdmission,
        owner_device_signer: &UserKeypair,
    ) -> Self {
        Signed::sign(
            DeviceProviderAdmissionApprovalBody {
                request: Box::new(request),
                admission,
            },
            owner_device_signer,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderResponseReservation {
    SamePrincipal,
    CrossPrincipal { response_slot: ObjectSlot },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossPrincipalDeviceRegistrationRequestBody {
    pub approval: Box<DeviceProviderAdmissionApproval>,
    pub response_slot: ObjectSlot,
}

impl SignedBody for CrossPrincipalDeviceRegistrationRequestBody {
    const DOMAIN: &'static [u8] = REGISTRATION_REQUEST_DOMAIN;
}

pub type CrossPrincipalDeviceRegistrationRequest =
    Signed<CrossPrincipalDeviceRegistrationRequestBody>;

/// A same-provider registration needs no second signature: the joining
/// device's access request already signed the complete registration. A
/// cross-provider registration additionally signs the response slot allocated
/// after the administrator publishes its challenge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceRegistrationRequest {
    SamePrincipal {
        approval: Box<DeviceProviderAdmissionApproval>,
    },
    CrossPrincipal(CrossPrincipalDeviceRegistrationRequest),
}

impl DeviceRegistrationRequest {
    pub fn same_principal(
        approval: DeviceProviderAdmissionApproval,
    ) -> Result<Self, DeviceJoinExchangeError> {
        let request = Self::SamePrincipal {
            approval: Box::new(approval),
        };
        request.verify()?;
        Ok(request)
    }

    pub fn cross_principal(
        approval: DeviceProviderAdmissionApproval,
        response_slot: ObjectSlot,
        member_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        if keys::public_key_hex(member_signer) != approval.request.offer.member_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let signed = Signed::sign(
            CrossPrincipalDeviceRegistrationRequestBody {
                approval: Box::new(approval),
                response_slot,
            },
            member_signer,
        );
        let request = Self::CrossPrincipal(signed);
        request.verify()?;
        Ok(request)
    }

    pub fn verify(&self) -> Result<(), DeviceJoinExchangeError> {
        self.approval().request.body().validate_shape()?;
        match self {
            Self::SamePrincipal { approval }
                if matches!(approval.admission, DeviceProviderAdmission::SamePrincipal) =>
            {
                Ok(())
            }
            Self::CrossPrincipal(request)
                if matches!(
                    request.approval.admission,
                    DeviceProviderAdmission::CrossPrincipal { .. }
                ) =>
            {
                let member_pubkey = request.approval.request.offer.member_pubkey.clone();
                request
                    .verify_by(&member_pubkey)
                    .map_err(|_| DeviceJoinExchangeError::InvalidSignature)?;
                let mut slots = vec![
                    request.approval.request.registration_slot.clone(),
                    request
                        .approval
                        .request
                        .expected_registration
                        .acknowledgements
                        .first_slot()
                        .clone(),
                    request.response_slot.clone(),
                ];
                if let DeviceProviderAdmission::CrossPrincipal { challenge, .. } =
                    &request.approval.admission
                {
                    slots.push(challenge.administrator_object.slot.clone());
                }
                require_distinct_slots(&slots)
            }
            _ => Err(DeviceJoinExchangeError::RegistrationRequestMismatch),
        }
    }

    pub fn approval(&self) -> &DeviceProviderAdmissionApproval {
        match self {
            Self::SamePrincipal { approval } => approval,
            Self::CrossPrincipal(request) => &request.approval,
        }
    }

    pub fn expected_registration(&self) -> &StoreDeviceRegistration {
        &self.approval().request.expected_registration
    }

    pub fn registration_slot(&self) -> &ObjectSlot {
        &self.approval().request.registration_slot
    }

    pub fn response(&self) -> DeviceProviderResponseReservation {
        match self {
            Self::SamePrincipal { .. } => DeviceProviderResponseReservation::SamePrincipal,
            Self::CrossPrincipal(request) => DeviceProviderResponseReservation::CrossPrincipal {
                response_slot: request.response_slot.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisionalDeviceBootstrap {
    pub request: Box<DeviceRegistrationRequest>,
    pub publication_authorization: DeviceJoinChallengePublicationAuthorization,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderChallengePublication {
    SamePrincipal,
    CrossPrincipal {
        challenge: CrossPrincipalProbeChallenge,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReadyDeviceBootstrap {
    pub bootstrap: Box<ProvisionalDeviceBootstrap>,
    pub challenge_publication: DeviceProviderChallengePublication,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderReadiness {
    SamePrincipal,
    CrossPrincipal(CrossPrincipalProbeResponse),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinReadiness {
    pub proof: DeviceReadinessProof,
    pub provider: DeviceProviderReadiness,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderAdmissionCompletion {
    SamePrincipal {
        bootstrap: Box<ProviderReadyDeviceBootstrap>,
    },
    CrossPrincipal {
        /// The bootstrap this completion answers. It carries the joining
        /// device's signed registration request, which is where the expected
        /// registration and its reserved slot come from now that no separate
        /// attempt file restates them.
        bootstrap: Box<ProviderReadyDeviceBootstrap>,
        readiness: Box<DeviceJoinReadiness>,
        receipt: CrossPrincipalProbeReceipt,
    },
}

impl DeviceProviderAdmissionCompletion {
    pub fn attempt_id(&self) -> DeviceJoinAttemptId {
        match self {
            Self::SamePrincipal { bootstrap } | Self::CrossPrincipal { bootstrap, .. } => {
                bootstrap.bootstrap.publication_authorization.attempt_id
            }
        }
    }

    /// The bootstrap this completion answers, whichever provider shape it took.
    pub fn bootstrap(&self) -> &ProviderReadyDeviceBootstrap {
        match self {
            Self::SamePrincipal { bootstrap } | Self::CrossPrincipal { bootstrap, .. } => bootstrap,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinActivation {
    pub attempt_id: DeviceJoinAttemptId,
    pub outcome_activation: StoreBatchCommitRef,
}

/// The canonical evidence for one Merge commit required to install a device
/// join. Every value is re-verified against its exact reference before the
/// joining database accepts it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinBootstrapCommitClosure {
    pub reference: StoreBatchCommitRef,
    pub canonical_commit: Vec<u8>,
    pub author: ReferencedStoreDeviceRegistration,
    pub registrations: RetainedStoreDeviceRegistrationActivations,
    pub device_operations: RetainedStoreDeviceOperations,
    pub activation_head: StoreDeviceHead,
    pub activation_object: ExactObjectRef,
    pub history_evidence: RetainedMergeCommitEvidence,
}

/// The exact verified history required after the selected snapshot. This is a
/// transfer representation; the joining database reconstructs its verified
/// bootstrap plan before mutating any Store state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinBootstrapClosure {
    pub founder: ReferencedStoreDeviceRegistration,
    pub genesis: ResolvedStoreDeviceState,
    pub membership: crate::membership::MembershipFloor,
    pub commits: Vec<DeviceJoinBootstrapCommitClosure>,
}

/// The signed snapshot authority and exact Merge closure a same-provider
/// joining device needs. The snapshot image remains in provider storage and is
/// the only Store object downloaded after this response arrives.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamePrincipalStoreInstallation {
    pub store_root: StoreProtocolRoot,
    pub snapshot: StoreSnapshotRef,
    pub metadata: SnapshotMeta,
    pub authority: RetainedReplaySnapshotAuthority,
    pub bootstrap: DeviceJoinBootstrapClosure,
}

/// The complete response when the Store and joining device use the same
/// provider principal. The one activation commit both publishes the attempt
/// bootstrap and activates the joining registration, so the joiner can install
/// that exact history and finish without a second transport wait or catch-up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamePrincipalDeviceJoin {
    pub bootstrap: ProviderReadyDeviceBootstrap,
    pub activation: DeviceJoinActivation,
    pub installation: Box<SamePrincipalStoreInstallation>,
}

impl SamePrincipalDeviceJoin {
    pub fn verified(
        bootstrap: ProviderReadyDeviceBootstrap,
        activation: DeviceJoinActivation,
        installation: SamePrincipalStoreInstallation,
    ) -> Result<Self, DeviceJoinExchangeError> {
        let join = Self {
            bootstrap,
            activation,
            installation: Box::new(installation),
        };
        join.verify_shape()?;
        Ok(join)
    }

    /// Check the parts of a same-provider join against each other.
    ///
    /// What this can settle is agreement between the pieces the joining device
    /// already holds: the request it signed, the authorization the admitting
    /// device signed over it, and the snapshot authority. That the joining
    /// device was really registered is not settled here and never was — it is
    /// settled by the bootstrap closure, whose activation commit names the
    /// registration and is verified commit by commit against the Store's own
    /// history.
    pub fn verify_shape(&self) -> Result<(), DeviceJoinExchangeError> {
        let bootstrap = &self.bootstrap;
        let activation = &self.activation;
        let installation = &self.installation;
        bootstrap.bootstrap.request.verify()?;
        let authorization = &bootstrap.bootstrap.publication_authorization;
        if !matches!(
            bootstrap.challenge_publication,
            DeviceProviderChallengePublication::SamePrincipal
        ) || activation.attempt_id != authorization.attempt_id
            || activation.outcome_activation != authorization.attempt_activation
            || installation.authority.store_root
                != bootstrap
                    .bootstrap
                    .request
                    .approval()
                    .request
                    .offer
                    .store_root
            || installation.snapshot != installation.authority.snapshot
            || installation.metadata != installation.authority.metadata
            || installation.store_root.descriptor.store_root_id()
                != installation.authority.store_root.store_root_id
            || installation.store_root.object_hash()
                != installation.authority.store_root.store_root_hash
        {
            return Err(DeviceJoinExchangeError::AttemptMismatch);
        }
        Ok(())
    }
}

/// The wire body of an owner's abandonment of a join attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAbandonmentBody {
    pub store_root_hash: ObjectHash,
    pub offer_hash: ObjectHash,
    pub attempt_id: DeviceJoinAttemptId,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for DeviceJoinAbandonmentBody {
    const DOMAIN: &'static [u8] = ABANDONMENT_DOMAIN;
}

pub type DeviceJoinAbandonmentObject = Signed<DeviceJoinAbandonmentBody>;

impl DeviceJoinAbandonmentObject {
    pub fn signed(
        offer: &DeviceJoinOffer,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        offer.verify(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        Ok(Signed::sign(
            DeviceJoinAbandonmentBody {
                store_root_hash: offer.store_root.store_root_hash,
                offer_hash: offer.offer_hash(),
                attempt_id: offer.attempt_id,
                owner_registration: offer.owner_registration.clone(),
                owner_grant: offer.owner_grant.clone(),
            },
            owner_device_signer,
        ))
    }

    pub fn abandonment_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAbandonment {
    pub abandonment: DeviceJoinAbandonmentRef,
    pub abandonment_activation: StoreBatchCommitRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinedStore {
    pub store_root: StoreRootRef,
    pub registration: StoreDeviceRegistrationRef,
    pub activation: DeviceJoinActivation,
}

impl DeviceJoinAbandonmentRef {
    pub fn verify(
        &self,
        abandonment: &DeviceJoinAbandonmentObject,
        owner: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        abandonment.owner_registration.verify_registration(owner)?;
        if self.attempt_id != abandonment.attempt_id
            || self.abandonment_hash != abandonment.abandonment_hash()
        {
            return Err(DeviceJoinExchangeError::AttemptMismatch);
        }
        abandonment
            .verify_by(&owner.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }
}

pub(crate) fn require_distinct_slots(
    slots: &[crate::objects::ObjectSlot],
) -> Result<(), DeviceJoinExchangeError> {
    let unique = slots.iter().collect::<std::collections::BTreeSet<_>>();
    if unique.len() == slots.len() {
        Ok(())
    } else {
        Err(DeviceJoinExchangeError::DuplicateReservedSlot)
    }
}
