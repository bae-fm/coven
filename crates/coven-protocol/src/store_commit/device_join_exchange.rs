use serde::{Deserialize, Serialize};

use crate::circle_control::StoreMembershipStateRef;
use crate::membership::MembershipGrantId;
use crate::objects::{ExactObjectRef, ObjectSlot};
use crate::provider::{
    ActivatedStoreMemberProviderAccessGrant, CrossPrincipalProbeChallenge,
    CrossPrincipalProbeReceipt, CrossPrincipalProbeResponse,
    DeviceJoinChallengePublicationAuthorization, ProviderAccessWithdrawal, ProviderAdminGrantId,
    ProviderAdminGrantRecord, StoreMemberProviderAccessGrantRef,
};
use crate::store_commit::{Signed, SignedBody};
use crate::{ProviderDeviceBinding, StoreProviderBinding};
use coven_keys::keys::{self, UserKeypair};

use super::device_join::{
    DeviceJoinAbandonmentRef, DeviceJoinAttempt, DeviceJoinAttemptId, DeviceJoinCleanupReceiptRef,
    DeviceJoinOutcomeRef,
};
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
    Provider(String),
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
const PROVIDER_CLOSURE_DOMAIN: &[u8] = b"coven.device-join-provider-closure.v1\0";
const JOINER_CLOSURE_DOMAIN: &[u8] = b"coven.device-join-joiner-closure.v1\0";
const WRITE_REVOCATION_DOMAIN: &[u8] = b"coven.device-join-write-revocation.v1\0";
const CLEANUP_RECEIPT_DOMAIN: &[u8] = b"coven.device-join-cleanup-receipt.v1\0";

/// The wire body of a device-join offer. Every field here is signed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOfferBody {
    pub attempt_id: DeviceJoinAttemptId,
    pub member_pubkey: String,
    pub store_root: StoreRootRef,
    pub provider: StoreProviderBinding,
    pub attempt_slot: ObjectSlot,
    pub outcome_slot: ObjectSlot,
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
                && self.provider_admin.administrator.device_id == self.owner_registration.device_id
            || self.attempt_slot == self.outcome_slot
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
        attempt_slot: ObjectSlot,
        outcome_slot: ObjectSlot,
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
            attempt_slot,
            outcome_slot,
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
}

impl SignedBody for DeviceProviderAccessRequestBody {
    const DOMAIN: &'static [u8] = ACCESS_REQUEST_DOMAIN;
}

pub type DeviceProviderAccessRequest = Signed<DeviceProviderAccessRequestBody>;

impl DeviceProviderAccessRequest {
    pub fn signed(
        offer: DeviceJoinOffer,
        peer_provider: ProviderDeviceBinding,
        member_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        if keys::public_key_hex(member_signer) != offer.member_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        peer_provider.validate_for(&offer.provider)?;
        Ok(Signed::sign(
            DeviceProviderAccessRequestBody {
                offer: Box::new(offer),
                peer_provider,
            },
            member_signer,
        ))
    }

    pub fn verify(&self, owner: &StoreDeviceRegistration) -> Result<(), DeviceJoinExchangeError> {
        self.offer.verify(owner)?;
        self.peer_provider.validate_for(&self.offer.provider)?;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderAdmissionChallenge {
    SamePrincipal,
    CrossPrincipal(CrossPrincipalProbeChallenge),
}

/// The wire body of a provider administrator's admission approval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProviderAdmissionApprovalBody {
    pub request: Box<DeviceProviderAccessRequest>,
    pub access_grant: ActivatedStoreMemberProviderAccessGrant,
    pub admission: DeviceProviderAdmissionChallenge,
}

impl SignedBody for DeviceProviderAdmissionApprovalBody {
    const DOMAIN: &'static [u8] = APPROVAL_DOMAIN;
}

pub type DeviceProviderAdmissionApproval = Signed<DeviceProviderAdmissionApprovalBody>;

impl DeviceProviderAdmissionApprovalBody {
    fn validate_shape(
        &self,
        store_root: &crate::objects::VerifiedObject<StoreProtocolRoot>,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        let offer = &self.request.offer;
        if store_root.object != offer.store_root.object
            || store_root.value.object_hash() != offer.store_root.store_root_hash
            || store_root.value.descriptor.store_root_id() != offer.store_root.store_root_id
            || store_root.value.descriptor.provider != offer.provider
            || self.access_grant.grant.member_pubkey != offer.member_pubkey
            || self.access_grant.grant.provider != self.request.peer_provider
            || self.access_grant.grant_ref.grant_id != self.access_grant.grant.grant_id
            || self.access_grant.grant_ref.grant_hash != self.access_grant.grant.grant_hash()
            || self.access_grant.grant.administrator_grant != offer.provider_admin.grant_id
            || self.access_grant.grant.administrator != offer.provider_admin.administrator
        {
            return Err(DeviceJoinExchangeError::ApprovalMismatch);
        }
        self.access_grant
            .grant
            .verify(&offer.provider, administrator)
            .map_err(|error| DeviceJoinExchangeError::Provider(error.to_string()))?;
        let same_principal = offer.provider_admin.provider == self.request.peer_provider;
        if same_principal
            != matches!(
                self.admission,
                DeviceProviderAdmissionChallenge::SamePrincipal
            )
        {
            return Err(DeviceJoinExchangeError::ApprovalMismatch);
        }
        Ok(())
    }
}

impl DeviceProviderAdmissionApproval {
    pub fn signed(
        request: DeviceProviderAccessRequest,
        access_grant: ActivatedStoreMemberProviderAccessGrant,
        admission: DeviceProviderAdmissionChallenge,
        store_root: &crate::objects::VerifiedObject<StoreProtocolRoot>,
        administrator: &StoreDeviceRegistration,
        administrator_device_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        if keys::public_key_hex(administrator_device_signer) != administrator.device_signing_pubkey
        {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let body = DeviceProviderAdmissionApprovalBody {
            request: Box::new(request),
            access_grant,
            admission,
        };
        body.validate_shape(store_root, administrator)?;
        Ok(Signed::sign(body, administrator_device_signer))
    }

    pub fn verify(
        &self,
        store_root: &crate::objects::VerifiedObject<StoreProtocolRoot>,
        owner: &StoreDeviceRegistration,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        self.request.verify(owner)?;
        self.body().validate_shape(store_root, administrator)?;
        self.verify_by(&administrator.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_without_shape_validation_for_test(
        request: DeviceProviderAccessRequest,
        access_grant: ActivatedStoreMemberProviderAccessGrant,
        admission: DeviceProviderAdmissionChallenge,
        administrator_device_signer: &UserKeypair,
    ) -> Self {
        Signed::sign(
            DeviceProviderAdmissionApprovalBody {
                request: Box::new(request),
                access_grant,
                admission,
            },
            administrator_device_signer,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderResponseReservation {
    SamePrincipal,
    CrossPrincipal { response_slot: ObjectSlot },
}

/// The wire body of a joining device's registration request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceRegistrationRequestBody {
    pub approval: Box<DeviceProviderAdmissionApproval>,
    pub expected_registration: StoreDeviceRegistration,
    pub registration_slot: ObjectSlot,
    pub response: DeviceProviderResponseReservation,
}

impl SignedBody for DeviceRegistrationRequestBody {
    const DOMAIN: &'static [u8] = REGISTRATION_REQUEST_DOMAIN;
}

pub type DeviceRegistrationRequest = Signed<DeviceRegistrationRequestBody>;

impl DeviceRegistrationRequestBody {
    fn validate_shape(&self) -> Result<(), DeviceJoinExchangeError> {
        let offer = &self.approval.request.offer;
        if self.expected_registration.store_root != offer.store_root
            || self.expected_registration.author_pubkey != offer.member_pubkey
            || self.expected_registration.provider != self.approval.request.peer_provider
            || self.registration_slot == offer.attempt_slot
            || self.registration_slot == offer.outcome_slot
        {
            return Err(DeviceJoinExchangeError::RegistrationRequestMismatch);
        }
        match (
            &self.approval.admission,
            &self.response,
            &self.expected_registration.origin,
        ) {
            (
                DeviceProviderAdmissionChallenge::SamePrincipal,
                DeviceProviderResponseReservation::SamePrincipal,
                crate::store_commit::StoreDeviceRegistrationOrigin::Join {
                    attempt_id,
                    attempt_slot,
                    outcome_slot,
                },
            )
            | (
                DeviceProviderAdmissionChallenge::CrossPrincipal(_),
                DeviceProviderResponseReservation::CrossPrincipal { .. },
                crate::store_commit::StoreDeviceRegistrationOrigin::Join {
                    attempt_id,
                    attempt_slot,
                    outcome_slot,
                },
            ) if *attempt_id == offer.attempt_id
                && attempt_slot == &offer.attempt_slot
                && outcome_slot == &offer.outcome_slot => {}
            _ => return Err(DeviceJoinExchangeError::RegistrationRequestMismatch),
        }
        let mut slots = vec![
            offer.attempt_slot.clone(),
            offer.outcome_slot.clone(),
            self.registration_slot.clone(),
            self.expected_registration
                .acknowledgements
                .first_slot()
                .clone(),
        ];
        if let DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
            &self.approval.admission
        {
            slots.push(challenge.administrator_object.slot.clone());
        }
        if let DeviceProviderResponseReservation::CrossPrincipal { response_slot } = &self.response
        {
            slots.push(response_slot.clone());
        }
        require_distinct_slots(&slots)?;
        Ok(())
    }
}

impl DeviceRegistrationRequest {
    pub fn signed(
        approval: DeviceProviderAdmissionApproval,
        expected_registration: StoreDeviceRegistration,
        registration_slot: ObjectSlot,
        response: DeviceProviderResponseReservation,
        member_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        if keys::public_key_hex(member_signer) != approval.request.offer.member_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let body = DeviceRegistrationRequestBody {
            approval: Box::new(approval),
            expected_registration,
            registration_slot,
            response,
        };
        body.validate_shape()?;
        Ok(Signed::sign(body, member_signer))
    }

    pub fn verify(&self) -> Result<(), DeviceJoinExchangeError> {
        self.body().validate_shape()?;
        let member_pubkey = self.approval.request.offer.member_pubkey.clone();
        self.verify_by(&member_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
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
pub enum DeviceProviderAdmission {
    SamePrincipal,
    CrossPrincipal(CrossPrincipalProbeReceipt),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProviderAdmissionCompletion {
    pub readiness: Box<DeviceJoinReadiness>,
    pub admission: DeviceProviderAdmission,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinActivation {
    pub outcome: DeviceJoinOutcomeRef,
    pub outcome_activation: StoreBatchCommitRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCancellation {
    pub outcome: DeviceJoinOutcomeRef,
    pub outcome_activation: StoreBatchCommitRef,
}

/// The wire body of an owner's abandonment of a join attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAbandonmentBody {
    pub store_root_hash: ObjectHash,
    pub offer_hash: ObjectHash,
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_slot: ObjectSlot,
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
                attempt_slot: offer.attempt_slot.clone(),
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
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderChallengeDisposition {
    SamePrincipal,
    NeverCreated,
    Created(ExactObjectRef),
}

/// The wire body of a provider administrator's closure of a cancelled attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminJoinClosureBody {
    pub cancellation: DeviceJoinOutcomeRef,
    pub administrator_registration: StoreDeviceRegistrationRef,
    pub challenge: ProviderChallengeDisposition,
    pub prior_state_hash: ObjectHash,
}

impl SignedBody for ProviderAdminJoinClosureBody {
    const DOMAIN: &'static [u8] = PROVIDER_CLOSURE_DOMAIN;
}

pub type ProviderAdminJoinClosure = Signed<ProviderAdminJoinClosureBody>;

impl ProviderAdminJoinClosure {
    pub fn signed(
        cancellation: DeviceJoinOutcomeRef,
        administrator_registration: StoreDeviceRegistrationRef,
        challenge: ProviderChallengeDisposition,
        prior_state_hash: ObjectHash,
        administrator: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        require_cancelled_outcome(&cancellation)?;
        administrator_registration.verify_registration(administrator)?;
        if keys::public_key_hex(signer) != administrator.device_signing_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        Ok(Signed::sign(
            ProviderAdminJoinClosureBody {
                cancellation,
                administrator_registration,
                challenge,
                prior_state_hash,
            },
            signer,
        ))
    }

    pub fn verify(
        &self,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        require_cancelled_outcome(&self.cancellation)?;
        self.administrator_registration
            .verify_registration(administrator)?;
        self.verify_by(&administrator.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SlotDisposition {
    NeverCreated,
    Created(ExactObjectRef),
}

impl From<Option<ExactObjectRef>> for SlotDisposition {
    fn from(object: Option<ExactObjectRef>) -> Self {
        match object {
            Some(object) => Self::Created(object),
            None => Self::NeverCreated,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinerResponseDisposition {
    SamePrincipal,
    Slot(SlotDisposition),
}

/// The wire body of a joining device's closure of a cancelled attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinerJoinClosureBody {
    pub cancellation: DeviceJoinOutcomeRef,
    pub expected_registration: StoreDeviceRegistration,
    pub registration: SlotDisposition,
    pub initial_ack: SlotDisposition,
    pub response: JoinerResponseDisposition,
    pub prior_state_hash: ObjectHash,
}

impl SignedBody for JoinerJoinClosureBody {
    const DOMAIN: &'static [u8] = JOINER_CLOSURE_DOMAIN;
}

pub type JoinerJoinClosure = Signed<JoinerJoinClosureBody>;

impl JoinerJoinClosure {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        cancellation: DeviceJoinOutcomeRef,
        expected_registration: StoreDeviceRegistration,
        registration: SlotDisposition,
        initial_ack: SlotDisposition,
        response: JoinerResponseDisposition,
        prior_state_hash: ObjectHash,
        signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        require_cancelled_outcome(&cancellation)?;
        if keys::public_key_hex(signer) != expected_registration.device_signing_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        Ok(Signed::sign(
            JoinerJoinClosureBody {
                cancellation,
                expected_registration,
                registration,
                initial_ack,
                response,
                prior_state_hash,
            },
            signer,
        ))
    }

    pub fn verify(&self) -> Result<(), DeviceJoinExchangeError> {
        require_cancelled_outcome(&self.cancellation)?;
        let pubkey = self.expected_registration.device_signing_pubkey.clone();
        self.verify_by(&pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceJoinProducer {
    ProviderAdministrator,
    Joiner,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderWriteAuthorityRef {
    ProviderAdministrator(ProviderAdminGrantId),
    MemberAccess(StoreMemberProviderAccessGrantRef),
}

/// The wire body of a write-authority revocation against a join producer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinProducerWriteRevocationBody {
    pub cancellation: DeviceJoinOutcomeRef,
    pub producer: DeviceJoinProducer,
    pub authority: ProviderWriteAuthorityRef,
    pub protected_slots: Vec<ObjectSlot>,
    pub withdrawal: ProviderAccessWithdrawal,
    pub executor_grant: ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
}

impl SignedBody for DeviceJoinProducerWriteRevocationBody {
    const DOMAIN: &'static [u8] = WRITE_REVOCATION_DOMAIN;
}

pub type DeviceJoinProducerWriteRevocation = Signed<DeviceJoinProducerWriteRevocationBody>;

impl DeviceJoinProducerWriteRevocation {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        cancellation: DeviceJoinOutcomeRef,
        producer: DeviceJoinProducer,
        authority: ProviderWriteAuthorityRef,
        mut protected_slots: Vec<ObjectSlot>,
        withdrawal: ProviderAccessWithdrawal,
        executor_grant: ProviderAdminGrantId,
        executor: StoreDeviceRegistrationRef,
        executor_registration: &StoreDeviceRegistration,
        executor_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        require_cancelled_outcome(&cancellation)?;
        executor.verify_registration(executor_registration)?;
        if keys::public_key_hex(executor_signer) != executor_registration.device_signing_pubkey {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        protected_slots.sort();
        validate_protected_slots(&protected_slots)?;
        Ok(Signed::sign(
            DeviceJoinProducerWriteRevocationBody {
                cancellation,
                producer,
                authority,
                protected_slots,
                withdrawal,
                executor_grant,
                executor,
            },
            executor_signer,
        ))
    }

    pub fn verify(
        &self,
        executor: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        require_cancelled_outcome(&self.cancellation)?;
        validate_protected_slots(&self.protected_slots)?;
        self.executor.verify_registration(executor)?;
        self.verify_by(&executor.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminJoinTerminal {
    Completed(DeviceProviderAdmissionCompletion),
    Cancelled(ProviderAdminJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinerJoinTerminal {
    Ready(DeviceJoinReadiness),
    Cancelled(JoinerJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
}

/// The wire body of a cleanup receipt: what the unwind deleted, and under whose
/// authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCleanupReceiptBody {
    pub store_root_hash: ObjectHash,
    pub cancellation: DeviceJoinOutcomeRef,
    pub administrator_terminal: ProviderAdminJoinTerminal,
    pub joiner_terminal: JoinerJoinTerminal,
    pub deleted_slots: Vec<ObjectSlot>,
    pub membership: StoreMembershipStateRef,
    pub provider_admin_grant: ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
}

impl SignedBody for DeviceJoinCleanupReceiptBody {
    const DOMAIN: &'static [u8] = CLEANUP_RECEIPT_DOMAIN;
}

pub type DeviceJoinCleanupReceiptObject = Signed<DeviceJoinCleanupReceiptBody>;

impl DeviceJoinCleanupReceiptBody {
    /// A pure check: the receipt is in canonical form or it is not this receipt.
    /// Sorting here instead would accept a second encoding of one receipt, since
    /// the signature covers the order the bytes actually carry.
    fn validate_shape(&self, attempt: &DeviceJoinAttempt) -> Result<(), DeviceJoinExchangeError> {
        validate_terminals(
            &self.cancellation,
            &self.administrator_terminal,
            &self.joiner_terminal,
        )?;
        if self.deleted_slots != canonical_cleanup_slots(attempt)? {
            return Err(DeviceJoinExchangeError::CleanupMismatch);
        }
        Ok(())
    }
}

impl DeviceJoinCleanupReceiptObject {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        attempt: &DeviceJoinAttempt,
        cancellation: DeviceJoinOutcomeRef,
        administrator_terminal: ProviderAdminJoinTerminal,
        joiner_terminal: JoinerJoinTerminal,
        deleted_slots: Vec<ObjectSlot>,
        membership: StoreMembershipStateRef,
        provider_admin_grant: ProviderAdminGrantId,
        executor: StoreDeviceRegistrationRef,
        executor_registration: &StoreDeviceRegistration,
        executor_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinExchangeError> {
        require_cancelled_outcome(&cancellation)?;
        if cancellation.attempt().attempt_id != attempt.attempt_id {
            return Err(DeviceJoinExchangeError::AttemptMismatch);
        }
        executor.verify_registration(executor_registration)?;
        if executor_registration.store_root != attempt.store_root
            || keys::public_key_hex(executor_signer) != executor_registration.device_signing_pubkey
        {
            return Err(DeviceJoinExchangeError::InvalidSignature);
        }
        let mut deleted_slots = deleted_slots;
        deleted_slots.sort();
        let body = DeviceJoinCleanupReceiptBody {
            store_root_hash: attempt.store_root.store_root_hash,
            cancellation,
            administrator_terminal,
            joiner_terminal,
            deleted_slots,
            membership,
            provider_admin_grant,
            executor,
        };
        body.validate_shape(attempt)?;
        Ok(Signed::sign(body, executor_signer))
    }

    pub fn receipt_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn verify(
        &self,
        attempt: &DeviceJoinAttempt,
        executor: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        if self.store_root_hash != attempt.store_root.store_root_hash {
            return Err(DeviceJoinExchangeError::CleanupMismatch);
        }
        self.body().validate_shape(attempt)?;
        self.verify_by(&executor.device_signing_pubkey)
            .map_err(|_| DeviceJoinExchangeError::InvalidSignature)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCleanupReceipt {
    pub receipt: DeviceJoinCleanupReceiptRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCleanupActivation {
    pub receipt: DeviceJoinCleanupReceiptRef,
    pub activation: StoreBatchCommitRef,
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

impl DeviceJoinCleanupReceiptRef {
    pub fn verify(
        &self,
        receipt: &DeviceJoinCleanupReceiptObject,
        executor: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinExchangeError> {
        receipt.executor.verify_registration(executor)?;
        if self.attempt_id != receipt.cancellation.attempt().attempt_id
            || self.receipt_hash != receipt.receipt_hash()
        {
            return Err(DeviceJoinExchangeError::CleanupMismatch);
        }
        receipt
            .verify_by(&executor.device_signing_pubkey)
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

pub fn require_cancelled_outcome(
    outcome: &DeviceJoinOutcomeRef,
) -> Result<(), DeviceJoinExchangeError> {
    if matches!(outcome, DeviceJoinOutcomeRef::Cancelled { .. }) {
        Ok(())
    } else {
        Err(DeviceJoinExchangeError::AttemptMismatch)
    }
}

/// Protected slots are a set, carried in one order: sorted, non-empty, and
/// without repeats. A verifier checks the form the bytes actually carry, so one
/// revocation has exactly one encoding that verifies.
fn validate_protected_slots(slots: &[ObjectSlot]) -> Result<(), DeviceJoinExchangeError> {
    if slots.is_empty() || slots.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(DeviceJoinExchangeError::CleanupMismatch);
    }
    Ok(())
}

pub fn validate_terminals(
    cancellation: &DeviceJoinOutcomeRef,
    administrator: &ProviderAdminJoinTerminal,
    joiner: &JoinerJoinTerminal,
) -> Result<(), DeviceJoinExchangeError> {
    let administrator_cancellation = match administrator {
        ProviderAdminJoinTerminal::Completed(completion) => {
            if completion.readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinExchangeError::AttemptMismatch);
            }
            None
        }
        ProviderAdminJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        ProviderAdminJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    let joiner_cancellation = match joiner {
        JoinerJoinTerminal::Ready(readiness) => {
            if readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinExchangeError::AttemptMismatch);
            }
            None
        }
        JoinerJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        JoinerJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    if administrator_cancellation.is_some_and(|value| value != cancellation)
        || joiner_cancellation.is_some_and(|value| value != cancellation)
    {
        return Err(DeviceJoinExchangeError::AttemptMismatch);
    }
    Ok(())
}

pub fn canonical_cleanup_slots(
    attempt: &DeviceJoinAttempt,
) -> Result<Vec<ObjectSlot>, DeviceJoinExchangeError> {
    let mut slots = vec![
        attempt.registration_slot.clone(),
        attempt
            .expected_registration
            .acknowledgements
            .first_slot()
            .clone(),
    ];
    match (
        &attempt.provider_approval.admission,
        &attempt.provider_response,
    ) {
        (
            DeviceProviderAdmissionChallenge::SamePrincipal,
            DeviceProviderResponseReservation::SamePrincipal,
        ) => {}
        (
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
            DeviceProviderResponseReservation::CrossPrincipal { response_slot },
        ) => {
            slots.push(challenge.administrator_object.slot.clone());
            slots.push(response_slot.clone());
        }
        _ => return Err(DeviceJoinExchangeError::AttemptMismatch),
    }
    slots.sort();
    require_distinct_slots(&slots)?;
    Ok(slots)
}
