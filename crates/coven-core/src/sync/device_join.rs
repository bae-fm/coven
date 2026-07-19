//! Provider-aware Store device admission and cancellation protocol.
//!
//! The wire values in this module are the transfer boundaries of the join
//! exchange. Each value contains the exact signed value from the preceding
//! boundary. Durable role journals store one closed progress value and advance
//! only from the exact adjacent predecessor.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{ExactSlotStorage, ObjectSlot};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MemberRole, MembershipChain, MembershipGrantId};
use crate::sync::provider::{
    ActivatedStoreMemberProviderAccessGrant, CrossPrincipalProbeChallenge,
    CrossPrincipalProbeReceipt, CrossPrincipalProbeResponse,
    DeviceJoinChallengePublicationAuthorization, ProviderAccessGrantId, ProviderAccessWithdrawal,
    ProviderAdminGrantId, ProviderAdminGrantRecord, StoreMemberProviderAccessGrant,
    StoreMemberProviderAccessGrantRef,
};
use crate::sync::storage::{
    CoordinationStorage, ExactObjectRef, ProtocolObjectDomain, ProviderDeviceBinding,
    StoreProviderBinding, SyncStorage,
};
use crate::sync::store_commit::{
    DeviceJoinAttempt, DeviceJoinAttemptId, DeviceJoinAttemptRef, DeviceJoinOutcomeRef,
    DeviceReadinessProof, ObjectHash, StoreBatchCommitRef, StoreDeviceRegistration,
    StoreDeviceRegistrationRef, StoreRootRef, STORE_PROTOCOL_VERSION,
};

const OFFER_DOMAIN: &[u8] = b"coven.device-join-offer.v1\0";
const ACCESS_REQUEST_DOMAIN: &[u8] = b"coven.device-provider-access-request.v1\0";
const APPROVAL_DOMAIN: &[u8] = b"coven.device-provider-admission-approval.v1\0";
const REGISTRATION_REQUEST_DOMAIN: &[u8] = b"coven.device-registration-request.v1\0";
const ABANDONMENT_DOMAIN: &[u8] = b"coven.device-join-abandonment.v1\0";
const PROVIDER_CLOSURE_DOMAIN: &[u8] = b"coven.device-join-provider-closure.v1\0";
const JOINER_CLOSURE_DOMAIN: &[u8] = b"coven.device-join-joiner-closure.v1\0";
const WRITE_REVOCATION_DOMAIN: &[u8] = b"coven.device-join-write-revocation.v1\0";
const CLEANUP_RECEIPT_DOMAIN: &[u8] = b"coven.device-join-cleanup-receipt.v1\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOffer {
    pub version: u32,
    pub attempt_id: DeviceJoinAttemptId,
    pub member_pubkey: String,
    pub store_root: StoreRootRef,
    pub provider: StoreProviderBinding,
    pub attempt_slot: ObjectSlot,
    pub outcome_slot: ObjectSlot,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub provider_admin: Box<ProviderAdminGrantRecord>,
    pub signature: String,
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
    ) -> Result<Self, DeviceJoinError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            version: STORE_PROTOCOL_VERSION,
            attempt_id,
            member_pubkey,
            store_root,
            provider,
            attempt_slot,
            outcome_slot,
            owner_registration,
            owner_grant,
            provider_admin: Box::new(provider_admin),
            signature: String::new(),
        };
        value.validate_shape()?;
        value.signature = sign(owner_device_signer, OFFER_DOMAIN, &value.signed_fields());
        Ok(value)
    }

    pub fn verify(&self, owner: &StoreDeviceRegistration) -> Result<(), DeviceJoinError> {
        self.validate_shape()?;
        self.owner_registration.verify_registration(owner)?;
        verify_signature(
            &owner.device_signing_pubkey,
            &self.signature,
            OFFER_DOMAIN,
            &self.signed_fields(),
        )
    }

    pub fn offer_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(OFFER_DOMAIN, &self.signed_fields()))
    }

    fn validate_shape(&self) -> Result<(), DeviceJoinError> {
        if self.version != STORE_PROTOCOL_VERSION
            || self.member_pubkey.is_empty()
            || self.provider_admin.administrator != self.owner_registration
                && self.provider_admin.administrator.device_id == self.owner_registration.device_id
            || self.attempt_slot == self.outcome_slot
        {
            return Err(DeviceJoinError::OfferMismatch);
        }
        self.provider.validate()?;
        self.provider_admin
            .provider
            .validate_for(&self.provider)
            .map_err(DeviceJoinError::Storage)?;
        if let crate::sync::provider::ProviderAdminGrantOrigin::Founder { root } =
            &self.provider_admin.created_at
        {
            if root != &self.store_root {
                return Err(DeviceJoinError::OfferMismatch);
            }
        }
        Ok(())
    }

    fn signed_fields(&self) -> DeviceJoinOfferFields<'_> {
        DeviceJoinOfferFields {
            version: self.version,
            attempt_id: self.attempt_id,
            member_pubkey: &self.member_pubkey,
            store_root: &self.store_root,
            provider: &self.provider,
            attempt_slot: &self.attempt_slot,
            outcome_slot: &self.outcome_slot,
            owner_registration: &self.owner_registration,
            owner_grant: &self.owner_grant,
            provider_admin: &self.provider_admin,
        }
    }
}

#[derive(Serialize)]
struct DeviceJoinOfferFields<'a> {
    version: u32,
    attempt_id: DeviceJoinAttemptId,
    member_pubkey: &'a str,
    store_root: &'a StoreRootRef,
    provider: &'a StoreProviderBinding,
    attempt_slot: &'a ObjectSlot,
    outcome_slot: &'a ObjectSlot,
    owner_registration: &'a StoreDeviceRegistrationRef,
    owner_grant: &'a MembershipGrantId,
    provider_admin: &'a ProviderAdminGrantRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProviderAccessRequest {
    pub offer: Box<DeviceJoinOffer>,
    pub peer_provider: ProviderDeviceBinding,
    pub signature: String,
}

impl DeviceProviderAccessRequest {
    pub fn signed(
        offer: DeviceJoinOffer,
        peer_provider: ProviderDeviceBinding,
        member_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinError> {
        if keys::public_key_hex(member_signer) != offer.member_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        peer_provider.validate_for(&offer.provider)?;
        let mut request = Self {
            offer: Box::new(offer),
            peer_provider,
            signature: String::new(),
        };
        request.signature = sign(
            member_signer,
            ACCESS_REQUEST_DOMAIN,
            &request.signed_fields(),
        );
        Ok(request)
    }

    pub fn verify(&self, owner: &StoreDeviceRegistration) -> Result<(), DeviceJoinError> {
        self.offer.verify(owner)?;
        self.peer_provider.validate_for(&self.offer.provider)?;
        verify_signature(
            &self.offer.member_pubkey,
            &self.signature,
            ACCESS_REQUEST_DOMAIN,
            &self.signed_fields(),
        )
    }

    pub fn request_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(ACCESS_REQUEST_DOMAIN, &self.signed_fields()))
    }

    fn signed_fields(&self) -> (&DeviceJoinOffer, &ProviderDeviceBinding) {
        (&self.offer, &self.peer_provider)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceProviderAdmissionChallenge {
    SamePrincipal,
    CrossPrincipal(CrossPrincipalProbeChallenge),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceProviderAdmissionApproval {
    pub request: Box<DeviceProviderAccessRequest>,
    pub access_grant: ActivatedStoreMemberProviderAccessGrant,
    pub admission: DeviceProviderAdmissionChallenge,
    pub signature: String,
}

impl DeviceProviderAdmissionApproval {
    pub fn signed(
        request: DeviceProviderAccessRequest,
        access_grant: ActivatedStoreMemberProviderAccessGrant,
        admission: DeviceProviderAdmissionChallenge,
        administrator: &StoreDeviceRegistration,
        administrator_device_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinError> {
        if keys::public_key_hex(administrator_device_signer) != administrator.device_signing_pubkey
        {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            request: Box::new(request),
            access_grant,
            admission,
            signature: String::new(),
        };
        value.validate_shape(administrator)?;
        value.signature = sign(
            administrator_device_signer,
            APPROVAL_DOMAIN,
            &value.signed_fields(),
        );
        Ok(value)
    }

    pub fn verify(
        &self,
        owner: &StoreDeviceRegistration,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        self.request.verify(owner)?;
        self.validate_shape(administrator)?;
        verify_signature(
            &administrator.device_signing_pubkey,
            &self.signature,
            APPROVAL_DOMAIN,
            &self.signed_fields(),
        )
    }

    fn validate_shape(
        &self,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        let offer = &self.request.offer;
        if self.access_grant.grant.member_pubkey != offer.member_pubkey
            || self.access_grant.grant.provider != self.request.peer_provider
            || self.access_grant.grant_ref.grant_id != self.access_grant.grant.grant_id
            || self.access_grant.grant_ref.grant_hash != self.access_grant.grant.grant_hash()
            || self.access_grant.grant.administrator_grant != offer.provider_admin.grant_id
            || self.access_grant.grant.administrator != offer.provider_admin.administrator
            || !self
                .access_grant
                .activation
                .coord
                .policy()
                .eq(&offer.store_root_policy_from_admin()?)
        {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        self.access_grant
            .grant
            .verify(&offer.provider, administrator)
            .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
        let same_principal = offer.provider_admin.provider == self.request.peer_provider;
        if same_principal
            != matches!(
                self.admission,
                DeviceProviderAdmissionChallenge::SamePrincipal
            )
        {
            return Err(DeviceJoinError::ApprovalMismatch);
        }
        Ok(())
    }

    fn signed_fields(
        &self,
    ) -> (
        &DeviceProviderAccessRequest,
        &ActivatedStoreMemberProviderAccessGrant,
        &DeviceProviderAdmissionChallenge,
    ) {
        (&self.request, &self.access_grant, &self.admission)
    }
}

trait OfferPolicy {
    fn store_root_policy_from_admin(&self) -> Result<crate::WritePolicy, DeviceJoinError>;
}

impl OfferPolicy for DeviceJoinOffer {
    fn store_root_policy_from_admin(&self) -> Result<crate::WritePolicy, DeviceJoinError> {
        match &self.provider_admin.created_at {
            crate::sync::provider::ProviderAdminGrantOrigin::Founder { .. } => Ok(self
                .provider_admin
                .capability
                .serial_coordination
                .as_ref()
                .map_or(crate::WritePolicy::MergeConcurrent, |_| {
                    crate::WritePolicy::Serial
                })),
            crate::sync::provider::ProviderAdminGrantOrigin::MergeMembership { .. } => {
                Ok(crate::WritePolicy::MergeConcurrent)
            }
            crate::sync::provider::ProviderAdminGrantOrigin::SerialCommit { .. } => {
                Ok(crate::WritePolicy::Serial)
            }
        }
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
pub struct DeviceRegistrationRequest {
    pub approval: Box<DeviceProviderAdmissionApproval>,
    pub expected_registration: StoreDeviceRegistration,
    pub registration_slot: ObjectSlot,
    pub response: DeviceProviderResponseReservation,
    pub signature: String,
}

impl DeviceRegistrationRequest {
    pub fn signed(
        approval: DeviceProviderAdmissionApproval,
        expected_registration: StoreDeviceRegistration,
        registration_slot: ObjectSlot,
        response: DeviceProviderResponseReservation,
        member_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinError> {
        if keys::public_key_hex(member_signer) != approval.request.offer.member_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            approval: Box::new(approval),
            expected_registration,
            registration_slot,
            response,
            signature: String::new(),
        };
        value.validate_shape()?;
        value.signature = sign(
            member_signer,
            REGISTRATION_REQUEST_DOMAIN,
            &value.signed_fields(),
        );
        Ok(value)
    }

    pub fn verify(&self) -> Result<(), DeviceJoinError> {
        self.validate_shape()?;
        verify_signature(
            &self.approval.request.offer.member_pubkey,
            &self.signature,
            REGISTRATION_REQUEST_DOMAIN,
            &self.signed_fields(),
        )
    }

    fn validate_shape(&self) -> Result<(), DeviceJoinError> {
        let offer = &self.approval.request.offer;
        if self.expected_registration.store_root != offer.store_root
            || self.expected_registration.author_pubkey != offer.member_pubkey
            || self.expected_registration.provider != self.approval.request.peer_provider
            || self.registration_slot == offer.attempt_slot
            || self.registration_slot == offer.outcome_slot
        {
            return Err(DeviceJoinError::RegistrationRequestMismatch);
        }
        match (
            &self.approval.admission,
            &self.response,
            &self.expected_registration.origin,
        ) {
            (
                DeviceProviderAdmissionChallenge::SamePrincipal,
                DeviceProviderResponseReservation::SamePrincipal,
                crate::sync::store_commit::StoreDeviceRegistrationOrigin::Join {
                    attempt_id,
                    attempt_slot,
                    outcome_slot,
                },
            )
            | (
                DeviceProviderAdmissionChallenge::CrossPrincipal(_),
                DeviceProviderResponseReservation::CrossPrincipal { .. },
                crate::sync::store_commit::StoreDeviceRegistrationOrigin::Join {
                    attempt_id,
                    attempt_slot,
                    outcome_slot,
                },
            ) if *attempt_id == offer.attempt_id
                && attempt_slot == &offer.attempt_slot
                && outcome_slot == &offer.outcome_slot => {}
            _ => return Err(DeviceJoinError::RegistrationRequestMismatch),
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

    fn signed_fields(
        &self,
    ) -> (
        &DeviceProviderAdmissionApproval,
        &StoreDeviceRegistration,
        &ObjectSlot,
        &DeviceProviderResponseReservation,
    ) {
        (
            &self.approval,
            &self.expected_registration,
            &self.registration_slot,
            &self.response,
        )
    }
}

trait DeviceStreamFirstSlot {
    fn first_slot(&self) -> &ObjectSlot;
}

impl DeviceStreamFirstSlot for crate::sync::store_commit::DeviceStreamAnchor {
    fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreAnnouncements { first_slot }
            | Self::StoreAcknowledgements { first_slot }
            | Self::StoreSnapshots { first_slot }
            | Self::CircleAcknowledgements { first_slot, .. }
            | Self::CircleSnapshots { first_slot, .. } => first_slot,
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAbandonmentRef {
    pub attempt_id: DeviceJoinAttemptId,
    pub abandonment_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl DeviceJoinAbandonmentRef {
    pub(crate) fn verify(
        &self,
        abandonment: &DeviceJoinAbandonmentObject,
        owner: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        abandonment.owner_registration.verify_registration(owner)?;
        if self.attempt_id != abandonment.attempt_id
            || self.abandonment_hash != abandonment.abandonment_hash()
        {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        verify_signature(
            &owner.device_signing_pubkey,
            &abandonment.signature,
            ABANDONMENT_DOMAIN,
            &abandonment.signed_fields(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAbandonmentObject {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub offer_hash: ObjectHash,
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_slot: ObjectSlot,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

impl DeviceJoinAbandonmentObject {
    pub fn signed(
        offer: &DeviceJoinOffer,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinError> {
        offer.verify(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: offer.store_root.store_root_hash,
            offer_hash: offer.offer_hash(),
            attempt_id: offer.attempt_id,
            attempt_slot: offer.attempt_slot.clone(),
            owner_registration: offer.owner_registration.clone(),
            owner_grant: offer.owner_grant.clone(),
            signature: String::new(),
        };
        value.signature = sign(
            owner_device_signer,
            ABANDONMENT_DOMAIN,
            &value.signed_fields(),
        );
        Ok(value)
    }

    pub fn abandonment_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(ABANDONMENT_DOMAIN, &self.signed_fields()))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("device join abandonment serialization cannot fail")
    }

    fn signed_fields(
        &self,
    ) -> (
        u32,
        ObjectHash,
        ObjectHash,
        DeviceJoinAttemptId,
        &ObjectSlot,
        &StoreDeviceRegistrationRef,
        &MembershipGrantId,
    ) {
        (
            self.version,
            self.store_root_hash,
            self.offer_hash,
            self.attempt_id,
            &self.attempt_slot,
            &self.owner_registration,
            &self.owner_grant,
        )
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
    AlreadyDeleted(ExactObjectRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdminJoinClosure {
    pub cancellation: DeviceJoinOutcomeRef,
    pub administrator_registration: StoreDeviceRegistrationRef,
    pub challenge: ProviderChallengeDisposition,
    pub prior_state_hash: ObjectHash,
    pub signature: String,
}

impl ProviderAdminJoinClosure {
    pub fn signed(
        cancellation: DeviceJoinOutcomeRef,
        administrator_registration: StoreDeviceRegistrationRef,
        challenge: ProviderChallengeDisposition,
        prior_state_hash: ObjectHash,
        administrator: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, DeviceJoinError> {
        require_cancelled_outcome(&cancellation)?;
        administrator_registration.verify_registration(administrator)?;
        if keys::public_key_hex(signer) != administrator.device_signing_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            cancellation,
            administrator_registration,
            challenge,
            prior_state_hash,
            signature: String::new(),
        };
        value.signature = sign(signer, PROVIDER_CLOSURE_DOMAIN, &value.signed_fields());
        Ok(value)
    }

    pub fn verify(&self, administrator: &StoreDeviceRegistration) -> Result<(), DeviceJoinError> {
        require_cancelled_outcome(&self.cancellation)?;
        self.administrator_registration
            .verify_registration(administrator)?;
        verify_signature(
            &administrator.device_signing_pubkey,
            &self.signature,
            PROVIDER_CLOSURE_DOMAIN,
            &self.signed_fields(),
        )
    }

    fn signed_fields(
        &self,
    ) -> (
        &DeviceJoinOutcomeRef,
        &StoreDeviceRegistrationRef,
        &ProviderChallengeDisposition,
        ObjectHash,
    ) {
        (
            &self.cancellation,
            &self.administrator_registration,
            &self.challenge,
            self.prior_state_hash,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SlotDisposition {
    NeverCreated,
    Created(ExactObjectRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinerResponseDisposition {
    SamePrincipal,
    Slot(SlotDisposition),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinerJoinClosure {
    pub cancellation: DeviceJoinOutcomeRef,
    pub expected_registration: StoreDeviceRegistration,
    pub registration: SlotDisposition,
    pub initial_ack: SlotDisposition,
    pub response: JoinerResponseDisposition,
    pub prior_state_hash: ObjectHash,
    pub signature: String,
}

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
    ) -> Result<Self, DeviceJoinError> {
        require_cancelled_outcome(&cancellation)?;
        if keys::public_key_hex(signer) != expected_registration.device_signing_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            cancellation,
            expected_registration,
            registration,
            initial_ack,
            response,
            prior_state_hash,
            signature: String::new(),
        };
        value.signature = sign(signer, JOINER_CLOSURE_DOMAIN, &value.signed_fields());
        Ok(value)
    }

    pub fn verify(&self) -> Result<(), DeviceJoinError> {
        require_cancelled_outcome(&self.cancellation)?;
        verify_signature(
            &self.expected_registration.device_signing_pubkey,
            &self.signature,
            JOINER_CLOSURE_DOMAIN,
            &self.signed_fields(),
        )
    }

    fn signed_fields(
        &self,
    ) -> (
        &DeviceJoinOutcomeRef,
        &StoreDeviceRegistration,
        &SlotDisposition,
        &SlotDisposition,
        &JoinerResponseDisposition,
        ObjectHash,
    ) {
        (
            &self.cancellation,
            &self.expected_registration,
            &self.registration,
            &self.initial_ack,
            &self.response,
            self.prior_state_hash,
        )
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinProducerWriteRevocation {
    pub cancellation: DeviceJoinOutcomeRef,
    pub producer: DeviceJoinProducer,
    pub authority: ProviderWriteAuthorityRef,
    pub protected_slots: Vec<ObjectSlot>,
    pub withdrawal: ProviderAccessWithdrawal,
    pub executor_grant: ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
    pub signature: String,
}

impl DeviceJoinProducerWriteRevocation {
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
    ) -> Result<Self, DeviceJoinError> {
        require_cancelled_outcome(&cancellation)?;
        executor.verify_registration(executor_registration)?;
        if keys::public_key_hex(executor_signer) != executor_registration.device_signing_pubkey {
            return Err(DeviceJoinError::InvalidSignature);
        }
        protected_slots.sort();
        if protected_slots.is_empty() || protected_slots.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DeviceJoinError::CleanupMismatch);
        }
        let mut value = Self {
            cancellation,
            producer,
            authority,
            protected_slots,
            withdrawal,
            executor_grant,
            executor,
            signature: String::new(),
        };
        value.signature = sign(
            executor_signer,
            WRITE_REVOCATION_DOMAIN,
            &value.signed_fields(),
        );
        Ok(value)
    }

    pub fn verify(&self, executor: &StoreDeviceRegistration) -> Result<(), DeviceJoinError> {
        require_cancelled_outcome(&self.cancellation)?;
        self.executor.verify_registration(executor)?;
        verify_signature(
            &executor.device_signing_pubkey,
            &self.signature,
            WRITE_REVOCATION_DOMAIN,
            &self.signed_fields(),
        )
    }

    fn signed_fields(
        &self,
    ) -> (
        &DeviceJoinOutcomeRef,
        DeviceJoinProducer,
        &ProviderWriteAuthorityRef,
        &[ObjectSlot],
        &ProviderAccessWithdrawal,
        &ProviderAdminGrantId,
        &StoreDeviceRegistrationRef,
    ) {
        (
            &self.cancellation,
            self.producer,
            &self.authority,
            &self.protected_slots,
            &self.withdrawal,
            &self.executor_grant,
            &self.executor,
        )
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCleanupReceiptRef {
    pub attempt_id: DeviceJoinAttemptId,
    pub receipt_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl DeviceJoinCleanupReceiptRef {
    pub(crate) fn verify(
        &self,
        receipt: &DeviceJoinCleanupReceiptObject,
        executor: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        receipt.executor.verify_registration(executor)?;
        if self.attempt_id != receipt.cancellation.attempt().attempt_id
            || self.receipt_hash != receipt.receipt_hash()
        {
            return Err(DeviceJoinError::CleanupMismatch);
        }
        verify_signature(
            &executor.device_signing_pubkey,
            &receipt.signature,
            CLEANUP_RECEIPT_DOMAIN,
            &receipt.signed_fields(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCleanupReceiptObject {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub cancellation: DeviceJoinOutcomeRef,
    pub administrator_terminal: ProviderAdminJoinTerminal,
    pub joiner_terminal: JoinerJoinTerminal,
    pub deleted_slots: Vec<ObjectSlot>,
    pub membership: StoreMembershipStateRef,
    pub provider_admin_grant: ProviderAdminGrantId,
    pub executor: StoreDeviceRegistrationRef,
    pub signature: String,
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
    ) -> Result<Self, DeviceJoinError> {
        require_cancelled_outcome(&cancellation)?;
        if cancellation.attempt().attempt_id != attempt.attempt_id {
            return Err(DeviceJoinError::AttemptMismatch);
        }
        executor.verify_registration(executor_registration)?;
        if executor_registration.store_root != attempt.store_root
            || keys::public_key_hex(executor_signer) != executor_registration.device_signing_pubkey
        {
            return Err(DeviceJoinError::InvalidSignature);
        }
        let mut value = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: attempt.store_root.store_root_hash,
            cancellation,
            administrator_terminal,
            joiner_terminal,
            deleted_slots,
            membership,
            provider_admin_grant,
            executor,
            signature: String::new(),
        };
        value.validate_shape(attempt)?;
        value.signature = sign(
            executor_signer,
            CLEANUP_RECEIPT_DOMAIN,
            &value.signed_fields(),
        );
        Ok(value)
    }

    pub fn receipt_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(CLEANUP_RECEIPT_DOMAIN, &self.signed_fields()))
    }

    pub(crate) fn verify(
        &self,
        attempt: &DeviceJoinAttempt,
        executor: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        if self.version != STORE_PROTOCOL_VERSION
            || self.store_root_hash != attempt.store_root.store_root_hash
        {
            return Err(DeviceJoinError::CleanupMismatch);
        }
        let mut verified = self.clone();
        verified.validate_shape(attempt)?;
        verify_signature(
            &executor.device_signing_pubkey,
            &self.signature,
            CLEANUP_RECEIPT_DOMAIN,
            &self.signed_fields(),
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("device join cleanup receipt serialization cannot fail")
    }

    fn validate_shape(&mut self, attempt: &DeviceJoinAttempt) -> Result<(), DeviceJoinError> {
        validate_terminals(
            &self.cancellation,
            &self.administrator_terminal,
            &self.joiner_terminal,
        )?;
        let expected = canonical_cleanup_slots(attempt)?;
        self.deleted_slots.sort();
        if self.deleted_slots != expected {
            return Err(DeviceJoinError::CleanupMismatch);
        }
        Ok(())
    }

    fn signed_fields(
        &self,
    ) -> (
        u32,
        ObjectHash,
        &DeviceJoinOutcomeRef,
        &ProviderAdminJoinTerminal,
        &JoinerJoinTerminal,
        &[ObjectSlot],
        &StoreMembershipStateRef,
        &ProviderAdminGrantId,
        &StoreDeviceRegistrationRef,
    ) {
        (
            self.version,
            self.store_root_hash,
            &self.cancellation,
            &self.administrator_terminal,
            &self.joiner_terminal,
            &self.deleted_slots,
            &self.membership,
            &self.provider_admin_grant,
            &self.executor,
        )
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinCleanupProgress {
    AwaitingBoth,
    AwaitingAdministrator {
        joiner: JoinerJoinTerminal,
    },
    AwaitingJoiner {
        administrator: ProviderAdminJoinTerminal,
    },
    Ready {
        administrator: ProviderAdminJoinTerminal,
        joiner: JoinerJoinTerminal,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinStatus {
    OperationInProgress {
        attempt_id: DeviceJoinAttemptId,
    },
    AwaitingAccessRequest {
        offer: DeviceJoinOffer,
    },
    AwaitingProviderAdmission {
        request: DeviceProviderAccessRequest,
    },
    AwaitingRegistrationRequest {
        approval: DeviceProviderAdmissionApproval,
    },
    AwaitingBootstrap {
        request: DeviceRegistrationRequest,
    },
    AwaitingChallengePublication {
        bootstrap: ProvisionalDeviceBootstrap,
    },
    AwaitingReadiness {
        bootstrap: ProviderReadyDeviceBootstrap,
    },
    AwaitingProviderCompletion {
        readiness: DeviceJoinReadiness,
    },
    AwaitingActivation {
        completion: DeviceProviderAdmissionCompletion,
    },
    AwaitingCompletion {
        activation: DeviceJoinActivation,
    },
    Activated {
        store: JoinedStore,
    },
    Abandoned {
        abandonment: DeviceJoinAbandonment,
    },
    ProviderAccessGrantCreatePending {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
    },
    AbandonmentCreatePending {
        abandonment: DeviceJoinAbandonmentRef,
    },
    CancellationCreatePending {
        cancellation: DeviceJoinOutcomeRef,
    },
    ProviderClosurePending {
        cancellation: DeviceJoinCancellation,
        producer: DeviceJoinProducer,
    },
    ProviderClosed {
        terminal: ProviderAdminJoinTerminal,
    },
    JoinerClosed {
        terminal: JoinerJoinTerminal,
    },
    CleanupReceiptCreatePending {
        cancellation: DeviceJoinCancellation,
        receipt: DeviceJoinCleanupReceiptRef,
    },
    Cancelled {
        cancellation: DeviceJoinCancellation,
    },
    CleanupPending {
        cancellation: DeviceJoinCancellation,
        progress: DeviceJoinCleanupProgress,
    },
    AwaitingCleanupActivation {
        receipt: DeviceJoinCleanupReceipt,
    },
    CleanupActivated {
        activation: DeviceJoinCleanupActivation,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinAction {
    TransferOffer(DeviceJoinOffer),
    TransferProviderAccessRequest(DeviceProviderAccessRequest),
    TransferProviderAdmissionApproval(DeviceProviderAdmissionApproval),
    TransferRegistrationRequest(DeviceRegistrationRequest),
    TransferProvisionalBootstrap(ProvisionalDeviceBootstrap),
    TransferProviderReadyBootstrap(ProviderReadyDeviceBootstrap),
    TransferReadiness(DeviceJoinReadiness),
    TransferProviderAdmissionCompletion(DeviceProviderAdmissionCompletion),
    TransferActivation(DeviceJoinActivation),
    TransferAbandonment(DeviceJoinAbandonment),
    TransferCancellation(DeviceJoinCancellation),
    TransferProviderAdminClosure(ProviderAdminJoinClosure),
    TransferJoinerClosure(JoinerJoinClosure),
    TransferCleanupReceipt(DeviceJoinCleanupReceipt),
    TransferCleanupActivation(DeviceJoinCleanupActivation),
    Complete(DeviceJoinActivation),
    RetryProviderOperation { attempt_id: DeviceJoinAttemptId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerJoinProgress {
    Offered(DeviceJoinOffer),
    RegistrationRequested(DeviceRegistrationRequest),
    AbandonmentCreateIntent {
        offer: DeviceJoinOffer,
        abandonment: DeviceJoinAbandonmentRef,
        prepared: PreparedDeviceJoinObject,
    },
    AttemptActivated(ProvisionalDeviceBootstrap),
    ProviderReady(ProviderReadyDeviceBootstrap),
    AdmissionCompleted(DeviceProviderAdmissionCompletion),
    CancellationCreateIntent {
        attempt: DeviceJoinAttemptRef,
        cancellation: DeviceJoinOutcomeRef,
        prepared: PreparedDeviceJoinObject,
    },
    ActivationPrepared(DeviceJoinActivation),
    Activated(JoinedStore),
    Abandoned(DeviceJoinAbandonment),
    Cancelled(DeviceJoinCancellation),
    CleanupReceiptCreateIntent {
        cancellation: DeviceJoinCancellation,
        receipt: DeviceJoinCleanupReceiptRef,
        prepared: PreparedDeviceJoinObject,
    },
    CleanupReceipt(DeviceJoinCleanupReceipt),
    CleanupActivated(DeviceJoinCleanupActivation),
    CancelledComplete(DeviceJoinCleanupActivation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdminJoinProgress {
    AccessRequested(DeviceProviderAccessRequest),
    AccessGrantPrepared {
        request: DeviceProviderAccessRequest,
        grant: StoreMemberProviderAccessGrant,
        prepared: PreparedDeviceJoinObject,
    },
    ApprovalPrepared(DeviceProviderAdmissionApproval),
    AttemptObserved(ProvisionalDeviceBootstrap),
    ChallengeCreateIntent(ProvisionalDeviceBootstrap),
    ProviderReady(ProviderReadyDeviceBootstrap),
    ResponseObserved(DeviceJoinReadiness),
    CleanupIntent {
        cancellation: DeviceJoinCancellation,
        challenge: ProviderChallengeDisposition,
        prior_state_hash: ObjectHash,
    },
    Completed(DeviceProviderAdmissionCompletion),
    Cancelled(ProviderAdminJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedDeviceJoinObject {
    pub object: ExactObjectRef,
    pub stored_bytes: Vec<u8>,
}

impl PreparedDeviceJoinObject {
    fn from_prepared(prepared: &crate::sync::storage::PreparedExactObject) -> Self {
        Self {
            object: prepared.reference().clone(),
            stored_bytes: prepared.stored_bytes().to_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinerJoinProgress {
    OfferReceived(DeviceJoinOffer),
    AccessRequested(DeviceProviderAccessRequest),
    ApprovalReceived(DeviceProviderAdmissionApproval),
    RegistrationPrepared(DeviceRegistrationRequest),
    ProviderReady(ProviderReadyDeviceBootstrap),
    RegistrationCreateIntent(ProviderReadyDeviceBootstrap),
    RegistrationCreated(StoreDeviceRegistrationRef),
    AckCreateIntent(StoreDeviceRegistrationRef),
    AckCreated(crate::sync::store_commit::StoreAckRef),
    ResponseCreateIntent(DeviceJoinReadiness),
    Ready(DeviceJoinReadiness),
    ActivationObserved(DeviceJoinActivation),
    Activated(JoinedStore),
    Abandoned(DeviceJoinAbandonment),
    CleanupIntent {
        cancellation: DeviceJoinCancellation,
        registration: SlotDisposition,
        initial_ack: SlotDisposition,
        response: JoinerResponseDisposition,
        prior_state_hash: ObjectHash,
    },
    Cancelled(JoinerJoinClosure),
    WriteRevoked(DeviceJoinProducerWriteRevocation),
    CleanupActivated(DeviceJoinCleanupActivation),
    CancelledComplete(DeviceJoinCleanupActivation),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinRoleProgress {
    Owner(OwnerJoinProgress),
    ProviderAdministrator(ProviderAdminJoinProgress),
    Joiner(JoinerJoinProgress),
}

impl DeviceJoinRoleProgress {
    fn role_name(&self) -> &'static str {
        match self {
            Self::Owner(_) => "owner",
            Self::ProviderAdministrator(_) => "provider_administrator",
            Self::Joiner(_) => "joiner",
        }
    }

    fn validate_transition(&self, next: &Self) -> Result<(), DeviceJoinError> {
        let adjacent = match (self, next) {
            (Self::Owner(previous), Self::Owner(next)) => owner_adjacent(previous, next),
            (Self::ProviderAdministrator(previous), Self::ProviderAdministrator(next)) => {
                provider_admin_adjacent(previous, next)
            }
            (Self::Joiner(previous), Self::Joiner(next)) => joiner_adjacent(previous, next),
            _ => false,
        };
        if adjacent {
            Ok(())
        } else {
            Err(DeviceJoinError::NonAdjacentJournalTransition)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinJournalRecord {
    pub attempt_id: DeviceJoinAttemptId,
    pub progress: Box<DeviceJoinRoleProgress>,
}

/// Durable role journal. Each row stores a closed progress value; SQLite's
/// compare-and-swap update rejects stale or skipped transitions.
#[derive(Clone, Debug)]
pub struct DeviceJoinJournalDatabase {
    path: PathBuf,
}

impl DeviceJoinJournalDatabase {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DeviceJoinError> {
        let path = path.as_ref().to_path_buf();
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS device_join_journals (
                 attempt_id TEXT NOT NULL,
                 role TEXT NOT NULL,
                 payload TEXT NOT NULL,
                 PRIMARY KEY (attempt_id, role)
             ) STRICT, WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS pending_join_transfers (
                 attempt_id TEXT PRIMARY KEY,
                 payload_hash TEXT NOT NULL,
                 payload TEXT NOT NULL
             ) STRICT, WITHOUT ROWID;",
        )?;
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub fn begin(
        &self,
        record: DeviceJoinJournalRecord,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        validate_initial_progress(&record.progress)?;
        let connection = Connection::open(&self.path)?;
        let tx = connection.unchecked_transaction()?;
        let attempt_id = attempt_key(record.attempt_id);
        let role = record.progress.role_name();
        let payload = serde_json::to_string(&record)?;
        tx.execute(
            "INSERT OR IGNORE INTO device_join_journals (attempt_id, role, payload)
             VALUES (?1, ?2, ?3)",
            (&attempt_id, role, &payload),
        )?;
        let actual: String = tx.query_row(
            "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
            (&attempt_id, role),
            |row| row.get(0),
        )?;
        tx.commit()?;
        let actual = serde_json::from_str::<DeviceJoinJournalRecord>(&actual)?;
        if actual != record {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(actual)
    }

    pub fn load(
        &self,
        attempt_id: DeviceJoinAttemptId,
        role: DeviceJoinRole,
    ) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
        let connection = Connection::open(&self.path)?;
        let raw = connection
            .query_row(
                "SELECT payload FROM device_join_journals WHERE attempt_id = ?1 AND role = ?2",
                (attempt_key(attempt_id), role.as_str()),
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        raw.map(|value| serde_json::from_str(&value).map_err(DeviceJoinError::from))
            .transpose()
    }

    pub fn advance(
        &self,
        previous: &DeviceJoinJournalRecord,
        next: DeviceJoinJournalRecord,
    ) -> Result<(), DeviceJoinError> {
        if previous.attempt_id != next.attempt_id {
            return Err(DeviceJoinError::JournalConflict);
        }
        previous.progress.validate_transition(&next.progress)?;
        let previous_payload = serde_json::to_string(previous)?;
        let next_payload = serde_json::to_string(&next)?;
        let connection = Connection::open(&self.path)?;
        let changed = connection.execute(
            "UPDATE device_join_journals SET payload = ?1
             WHERE attempt_id = ?2 AND role = ?3 AND payload = ?4",
            (
                &next_payload,
                attempt_key(previous.attempt_id),
                previous.progress.role_name(),
                &previous_payload,
            ),
        )?;
        if changed != 1 {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(())
    }

    pub fn stage_pending_transfer(
        &self,
        record: &DeviceJoinJournalRecord,
    ) -> Result<ObjectHash, DeviceJoinError> {
        if !matches!(*record.progress, DeviceJoinRoleProgress::Joiner(_)) {
            return Err(DeviceJoinError::JournalConflict);
        }
        let payload = serde_json::to_string(record)?;
        let payload_hash = ObjectHash::digest(payload.as_bytes());
        let connection = Connection::open(&self.path)?;
        connection.execute(
            "INSERT OR IGNORE INTO pending_join_transfers (attempt_id, payload_hash, payload)
             VALUES (?1, ?2, ?3)",
            (
                attempt_key(record.attempt_id),
                payload_hash.to_string(),
                &payload,
            ),
        )?;
        let actual: (String, String) = connection.query_row(
            "SELECT payload_hash, payload FROM pending_join_transfers WHERE attempt_id = ?1",
            [attempt_key(record.attempt_id)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if actual != (payload_hash.to_string(), payload) {
            return Err(DeviceJoinError::JournalConflict);
        }
        Ok(payload_hash)
    }

    /// Move one pending joiner payload into the installed Store journal in one
    /// SQLite transaction spanning the source and destination databases.
    pub fn transfer_pending_to(
        &self,
        destination: &DeviceJoinJournalDatabase,
        attempt_id: DeviceJoinAttemptId,
    ) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
        let destination_connection = Connection::open(&destination.path)?;
        destination_connection.execute(
            "ATTACH DATABASE ?1 AS pending_join_source",
            [self.path.to_string_lossy().as_ref()],
        )?;
        let tx = destination_connection.unchecked_transaction()?;
        let attempt = attempt_key(attempt_id);
        let (payload_hash, payload): (String, String) = tx.query_row(
            "SELECT payload_hash, payload FROM pending_join_source.pending_join_transfers
             WHERE attempt_id = ?1",
            [&attempt],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let calculated = ObjectHash::digest(payload.as_bytes()).to_string();
        if calculated != payload_hash {
            return Err(DeviceJoinError::PendingTransferHashMismatch);
        }
        let record: DeviceJoinJournalRecord = serde_json::from_str(&payload)?;
        if record.attempt_id != attempt_id
            || !matches!(*record.progress, DeviceJoinRoleProgress::Joiner(_))
        {
            return Err(DeviceJoinError::JournalConflict);
        }
        tx.execute(
            "INSERT OR IGNORE INTO device_join_journals (attempt_id, role, payload)
             VALUES (?1, 'joiner', ?2)",
            (&attempt, &payload),
        )?;
        let installed: String = tx.query_row(
            "SELECT payload FROM device_join_journals
             WHERE attempt_id = ?1 AND role = 'joiner'",
            [&attempt],
            |row| row.get(0),
        )?;
        if installed != payload {
            return Err(DeviceJoinError::JournalConflict);
        }
        tx.execute(
            "DELETE FROM pending_join_source.pending_join_transfers WHERE attempt_id = ?1",
            [&attempt],
        )?;
        tx.commit()?;
        destination_connection.execute_batch("DETACH DATABASE pending_join_source")?;
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceJoinRole {
    Owner,
    ProviderAdministrator,
    Joiner,
}

impl DeviceJoinRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::ProviderAdministrator => "provider_administrator",
            Self::Joiner => "joiner",
        }
    }
}

#[async_trait::async_trait]
pub trait DeviceProviderAccessAdministrator: Send + Sync {
    async fn grant_member_access(
        &self,
        member_pubkey: &str,
        provider_account_email: Option<&str>,
        peer: &ProviderDeviceBinding,
    ) -> Result<crate::sync::provider::ProviderAccessLocator, DeviceJoinError>;
}

#[derive(Clone, Debug)]
pub enum DeviceJoinAuthorization {
    MergeConcurrent(MembershipChain),
    Serial(crate::sync::membership::SerialAuthorizationState),
}

impl DeviceJoinAuthorization {
    fn policy(&self) -> crate::WritePolicy {
        match self {
            Self::MergeConcurrent(_) => crate::WritePolicy::MergeConcurrent,
            Self::Serial(_) => crate::WritePolicy::Serial,
        }
    }

    fn current_members(&self) -> Vec<(String, crate::sync::membership::MemberRole)> {
        match self {
            Self::MergeConcurrent(membership) => membership.current_members(),
            Self::Serial(authorization) => authorization.membership.current_members(),
        }
    }

    pub(crate) fn active_owner_grant(&self, pubkey: &str) -> Option<MembershipGrantId> {
        match self {
            Self::MergeConcurrent(membership) => membership.active_owner_grant(pubkey),
            Self::Serial(authorization) => authorization.membership.active_owner_grant(pubkey),
        }
    }

    fn is_owner_now(&self, pubkey: &str) -> bool {
        match self {
            Self::MergeConcurrent(membership) => membership.is_owner_now(pubkey),
            Self::Serial(authorization) => authorization.membership.is_owner(pubkey),
        }
    }

    fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        match self {
            Self::MergeConcurrent(membership) => membership.current_member_provider_email(pubkey),
            Self::Serial(authorization) => authorization
                .membership
                .current_member_provider_email(pubkey),
        }
    }

    pub(crate) fn merge_chain(&self) -> Option<&MembershipChain> {
        match self {
            Self::MergeConcurrent(membership) => Some(membership),
            Self::Serial(_) => None,
        }
    }

    fn resolved_provider_admin(
        &self,
        grant_id: &ProviderAdminGrantId,
    ) -> Result<ProviderAdminGrantRecord, DeviceJoinError> {
        let state = match self {
            Self::MergeConcurrent(membership) => {
                let crate::sync::membership::MembershipStatus::Resolved(resolved) =
                    membership.status()
                else {
                    return Err(DeviceJoinError::MembershipConflict);
                };
                resolved.provider_admin.combined_state()
            }
            Self::Serial(authorization) => &authorization.provider_admin,
        };
        state
            .records()
            .get(grant_id)
            .filter(|record| state.authorizes(grant_id, &record.administrator))
            .cloned()
            .ok_or(DeviceJoinError::ProviderAdministratorRequired)
    }
}

pub async fn load_current_device_join_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
) -> Result<DeviceJoinAuthorization, DeviceJoinError> {
    match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => {
            let membership = crate::sync::pull::load_cycle_membership(storage, db)
                .await
                .map_err(|error| DeviceJoinError::Store(error.to_string()))?;
            let chain = membership
                .chain
                .ok_or(DeviceJoinError::MembershipConflict)?;
            Ok(DeviceJoinAuthorization::MergeConcurrent(chain))
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or(DeviceJoinError::MembershipConflict)?;
            Ok(DeviceJoinAuthorization::Serial(
                crate::sync::store_outbound::current_serial_authorization(
                    db,
                    storage,
                    coordination,
                )
                .await?,
            ))
        }
    }
}

async fn require_authorization_policy(
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &DeviceJoinAuthorization,
) -> Result<(), DeviceJoinError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let root_value = crate::sync::store_objects::load_store_protocol_root(storage, &root)
        .await?
        .value;
    if root_value.descriptor.write_policy != authorization.policy()
        || db.write_policy() != root_value.descriptor.write_policy
    {
        return Err(DeviceJoinError::OfferMismatch);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn begin_device_join(
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    member_pubkey: &str,
    provider_admin_grant: ProviderAdminGrantId,
) -> Result<DeviceJoinOffer, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    validate_member_for_join(member_pubkey, &authorization.current_members())?;
    let owner_pubkey = keys::public_key_hex(identity_signer);
    let owner_grant = authorization
        .active_owner_grant(&owner_pubkey)
        .ok_or(DeviceJoinError::OwnerAuthorityRequired)?;
    let provider_admin = authorization.resolved_provider_admin(&provider_admin_grant)?;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let (root, owner_registration, owner, owner_device_signer) =
        crate::sync::store_outbound::load_local_store_authority(db, &device_id, identity_signer)
            .await?;
    let binding = storage.provider_binding().await?;
    let attempt_id =
        DeviceJoinAttemptId::from_hash(ObjectHash::digest(db.new_write_id().as_str().as_bytes()));
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_slot = storage
        .allocate_protocol_slot(
            &attempt_context,
            &crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_id),
            ".json",
        )
        .await?;
    let outcome_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinOutcome,
    );
    let outcome_slot = storage
        .allocate_protocol_slot(
            &outcome_context,
            &crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_id),
            ".json",
        )
        .await?;
    let offer = DeviceJoinOffer::signed(
        attempt_id,
        member_pubkey.to_string(),
        root,
        binding.store,
        attempt_slot,
        outcome_slot,
        owner_registration,
        owner_grant,
        provider_admin,
        &owner,
        &owner_device_signer,
    )?;
    begin_store_journal(
        db,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
                offer.clone(),
            ))),
        },
    )
    .await?;
    Ok(offer)
}

#[allow(clippy::too_many_arguments)]
pub async fn abandon_device_join(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    offer: DeviceJoinOffer,
) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    let current = load_store_journal(db, offer.attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(existing)) =
        &*current.progress
    {
        return Ok(existing.clone());
    }
    let owner = db
        .activated_store_device_registration(offer.owner_registration.clone())
        .await
        .map_err(database_error)?;
    offer.verify(&owner)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if owner.device_id.to_string() != local_device_id
        || !authorization.is_owner_now(&keys::public_key_hex(identity_signer))
    {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let owner_signer = owner.device_signer(identity_signer)?;
    let abandonment_object = DeviceJoinAbandonmentObject::signed(&offer, &owner, &owner_signer)?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        offer.store_root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAbandonment,
    );
    let prefix =
        crate::sync::store_commit::device_join_abandonment_semantic_prefix(offer.attempt_id);
    let prepared = storage.prepare_protocol_object(
        &context,
        offer.attempt_slot.clone(),
        &prefix,
        abandonment_object.to_bytes(),
    )?;
    let abandonment_ref = DeviceJoinAbandonmentRef {
        attempt_id: offer.attempt_id,
        abandonment_hash: abandonment_object.abandonment_hash(),
        object: prepared.reference().clone(),
    };
    let intent = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::AbandonmentCreateIntent {
                offer: offer.clone(),
                abandonment: abandonment_ref.clone(),
                prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
            },
        )),
    };
    match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(durable)) if durable == &offer => {
            advance_store_journal(db, &current, intent.clone()).await?;
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request))
            if request.approval.request.offer.as_ref() == &offer =>
        {
            advance_store_journal(db, &current, intent.clone()).await?;
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AbandonmentCreateIntent {
            offer: durable_offer,
            abandonment,
            prepared: durable_prepared,
        }) if durable_offer == &offer
            && abandonment == &abandonment_ref
            && durable_prepared == &PreparedDeviceJoinObject::from_prepared(&prepared) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != abandonment_object.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    abandonment_ref.verify(&abandonment_object, &owner)?;
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let activation = crate::sync::store_outbound::activate_store_operation_commit(
        db,
        storage,
        coordination,
        plan,
        crate::sync::store_outbound::StoreOperationBatch::Abandonment(abandonment_ref.clone()),
    )
    .await?;
    let abandonment = DeviceJoinAbandonment {
        abandonment: abandonment_ref,
        abandonment_activation: activation,
    };
    advance_store_journal(
        db,
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(
                abandonment.clone(),
            ))),
        },
    )
    .await?;
    Ok(abandonment)
}

pub async fn prepare_device_provider_access_request(
    pending: &DeviceJoinJournalDatabase,
    provider_binding: crate::sync::storage::ResolvedProviderBinding,
    identity_signer: &UserKeypair,
    offer: DeviceJoinOffer,
) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
    if let Some(record) = pending.load(offer.attempt_id, DeviceJoinRole::Joiner)? {
        return match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request))
                if *request.offer == offer =>
            {
                Ok(request.clone())
            }
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(durable))
                if durable == &offer =>
            {
                prepare_new_access_request(
                    pending,
                    provider_binding,
                    identity_signer,
                    record.clone(),
                    durable.clone(),
                )
                .await
            }
            _ => Err(DeviceJoinError::JournalConflict),
        };
    }
    let initial = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::OfferReceived(offer.clone()),
        )),
    };
    let durable = pending.begin(initial.clone())?;
    if durable != initial {
        return Err(DeviceJoinError::JournalConflict);
    }
    prepare_new_access_request(pending, provider_binding, identity_signer, initial, offer).await
}

async fn prepare_new_access_request(
    pending: &DeviceJoinJournalDatabase,
    provider_binding: crate::sync::storage::ResolvedProviderBinding,
    identity_signer: &UserKeypair,
    initial: DeviceJoinJournalRecord,
    offer: DeviceJoinOffer,
) -> Result<DeviceProviderAccessRequest, DeviceJoinError> {
    if provider_binding.store != offer.provider {
        return Err(DeviceJoinError::OfferMismatch);
    }
    let request =
        DeviceProviderAccessRequest::signed(offer, provider_binding.device, identity_signer)?;
    pending.advance(
        &initial,
        DeviceJoinJournalRecord {
            attempt_id: request.offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::AccessRequested(request.clone()),
            )),
        },
    )?;
    Ok(request)
}

pub async fn prepare_device_registration_request(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    peer_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    approval: DeviceProviderAdmissionApproval,
) -> Result<DeviceRegistrationRequest, DeviceJoinError> {
    let attempt_id = approval.request.offer.attempt_id;
    if let Some(record) = pending.load(attempt_id, DeviceJoinRole::Joiner)? {
        if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request)) =
            *record.progress
        {
            if *request.approval == approval {
                return Ok(request);
            }
            return Err(DeviceJoinError::JournalConflict);
        }
    }
    let owner = crate::sync::store_objects::load_registration_ref(
        storage,
        &approval.request.offer.store_root,
        &approval.request.offer.owner_registration,
    )
    .await?
    .value;
    let administrator = crate::sync::store_objects::load_registration_ref(
        storage,
        &approval.request.offer.store_root,
        &approval.request.offer.provider_admin.administrator,
    )
    .await?
    .value;
    approval.verify(&owner, &administrator)?;
    let live = storage.provider_binding().await?;
    if live.store != approval.request.offer.provider
        || live.device != approval.request.peer_provider
    {
        return Err(DeviceJoinError::ApprovalMismatch);
    }
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    let access_request = match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request))
            if request == &*approval.request =>
        {
            request.clone()
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(existing))
            if existing == &approval =>
        {
            *approval.request.clone()
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    let approval_record = if matches!(
        *current.progress,
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(_))
    ) {
        current
    } else {
        let next = DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::ApprovalReceived(approval.clone()),
            )),
        };
        pending.advance(&current, next.clone())?;
        next
    };
    let root_value = crate::sync::store_objects::load_store_protocol_root(
        storage,
        &approval.request.offer.store_root,
    )
    .await?
    .value;
    let origin = crate::sync::store_commit::StoreDeviceRegistrationOrigin::Join {
        attempt_id,
        attempt_slot: approval.request.offer.attempt_slot.clone(),
        outcome_slot: approval.request.offer.outcome_slot.clone(),
    };
    let device_id = crate::sync::store_commit::StoreDeviceId::derive(
        &approval.request.offer.store_root,
        &origin,
    );
    let registration_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        approval.request.offer.store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let registration_slot = storage
        .allocate_protocol_slot(
            &registration_context,
            &crate::sync::store_commit::registration_semantic_prefix(&device_id.to_string()),
            ".json",
        )
        .await?;
    let store_commits = match root_value.descriptor.write_policy {
        crate::WritePolicy::MergeConcurrent => {
            let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
                approval.request.offer.store_root.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let first_slot = storage
                .allocate_protocol_slot(
                    &context,
                    &crate::sync::store_commit::head_slot_prefix(&device_id.to_string(), 1),
                    ".json",
                )
                .await?;
            crate::sync::store_commit::StoreCommitAnchor::MergeConcurrent {
                announcements: crate::sync::store_commit::DeviceStreamAnchor::StoreAnnouncements {
                    first_slot,
                },
            }
        }
        crate::WritePolicy::Serial => crate::sync::store_commit::StoreCommitAnchor::Serial,
    };
    let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        approval.request.offer.store_root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let first_ack = storage
        .allocate_protocol_slot(
            &ack_context,
            &crate::sync::store_commit::ack_slot_prefix(&device_id.to_string(), 1),
            ".json",
        )
        .await?;
    let snapshot_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        approval.request.offer.store_root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let first_snapshot = storage
        .allocate_protocol_slot(
            &snapshot_context,
            &crate::sync::store_commit::snapshot_slot_prefix(&device_id.to_string(), 1),
            ".json",
        )
        .await?;
    let (response, response_slot) = match &approval.admission {
        DeviceProviderAdmissionChallenge::SamePrincipal => {
            (DeviceProviderResponseReservation::SamePrincipal, None)
        }
        DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
            let exact = peer_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
            let logical = crate::sync::provider::cross_peer_logical_key(challenge.probe_id);
            let slot = exact
                .allocate_slot(&logical)
                .await
                .map_err(provider_error)?;
            if slot.logical_key() != logical {
                return Err(DeviceJoinError::RegistrationRequestMismatch);
            }
            (
                DeviceProviderResponseReservation::CrossPrincipal {
                    response_slot: slot.clone(),
                },
                Some(slot),
            )
        }
    };
    let _ = response_slot;
    let (registration, _) = crate::sync::store_registration::prepare_registration_for_origin(
        storage,
        identity_signer,
        root_value.descriptor.write_policy,
        approval.request.offer.store_root.clone(),
        origin,
        registration_slot.clone(),
        live.device,
        store_commits,
        crate::sync::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: first_ack,
        },
        crate::sync::store_commit::DeviceStreamAnchor::StoreSnapshots {
            first_slot: first_snapshot,
        },
    )
    .await?;
    if access_request != *approval.request {
        return Err(DeviceJoinError::JournalConflict);
    }
    let request = DeviceRegistrationRequest::signed(
        approval,
        registration,
        registration_slot,
        response,
        identity_signer,
    )?;
    pending.advance(
        &approval_record,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::RegistrationPrepared(request.clone()),
            )),
        },
    )?;
    Ok(request)
}

#[allow(clippy::too_many_arguments)]
pub async fn accept_device_registration_request(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    request: DeviceRegistrationRequest,
) -> Result<ProvisionalDeviceBootstrap, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    request.verify()?;
    let offer = &request.approval.request.offer;
    let owner = Box::pin(db.activated_store_device_registration(offer.owner_registration.clone()))
        .await
        .map_err(database_error)?;
    let administrator = Box::pin(
        db.activated_store_device_registration(offer.provider_admin.administrator.clone()),
    )
    .await
    .map_err(database_error)?;
    request.approval.verify(&owner, &administrator)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if owner.device_id.to_string() != local_device_id
        || !authorization.is_owner_now(&keys::public_key_hex(identity_signer))
    {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let owner_signer = owner.device_signer(identity_signer)?;
    let offered = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(
            *offer.clone(),
        ))),
    };
    let durable = begin_store_journal(db, offered.clone()).await?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) =
        *durable.progress
    {
        if *bootstrap.request == request {
            return Ok(bootstrap);
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    if durable != offered {
        return Err(DeviceJoinError::JournalConflict);
    }
    let requested = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::RegistrationRequested(request.clone()),
        )),
    };
    advance_store_journal(db, &offered, requested.clone()).await?;
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let cut = plan.predecessor_cut()?;
    if !history_cut_contains(&cut, &request.approval.access_grant.activation) {
        return Err(DeviceJoinError::ApprovalActivationMissing);
    }
    let attempt = DeviceJoinAttempt::signed(
        offer.store_root.clone(),
        offer.attempt_id,
        offer.attempt_slot.clone(),
        request.expected_registration.clone(),
        request.registration_slot.clone(),
        offer.outcome_slot.clone(),
        cut,
        plan.membership_state().clone(),
        offer.provider_admin.grant_id.clone(),
        *request.approval.clone(),
        request.response.clone(),
        offer.owner_registration.clone(),
        offer.owner_grant.clone(),
        &owner,
        &owner_signer,
    )?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        offer.store_root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let prefix = crate::sync::store_commit::device_join_attempt_semantic_prefix(offer.attempt_id);
    let prepared = storage.prepare_protocol_object(
        &context,
        offer.attempt_slot.clone(),
        &prefix,
        attempt.to_bytes(),
    )?;
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != attempt.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let attempt_ref = DeviceJoinAttemptRef {
        attempt_id: offer.attempt_id,
        attempt_hash: attempt.attempt_hash(),
        object: prepared.reference().clone(),
    };
    let activation = crate::sync::store_outbound::activate_store_operation_commit(
        db,
        storage,
        coordination,
        plan,
        crate::sync::store_outbound::StoreOperationBatch::Attempt(attempt_ref.clone()),
    )
    .await?;
    let attempt_id = offer.attempt_id;
    let bootstrap = ProvisionalDeviceBootstrap {
        request: Box::new(request),
        publication_authorization: DeviceJoinChallengePublicationAuthorization {
            attempt: attempt_ref,
            attempt_activation: activation,
        },
    };
    advance_store_journal(
        db,
        &requested,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::AttemptActivated(bootstrap.clone()),
            )),
        },
    )
    .await?;
    Ok(bootstrap)
}

fn history_cut_contains(
    cut: &crate::sync::store_commit::StoreHistoryCut,
    expected: &StoreBatchCommitRef,
) -> bool {
    match cut {
        crate::sync::store_commit::StoreHistoryCut::MergeConcurrent(frontier) => {
            frontier.values().any(|reference| reference == expected)
        }
        crate::sync::store_commit::StoreHistoryCut::Serial(
            crate::sync::store_commit::StoreSerialPredecessor::Commit(reference),
        ) => reference == expected,
        crate::sync::store_commit::StoreHistoryCut::Serial(
            crate::sync::store_commit::StoreSerialPredecessor::Genesis { .. },
        ) => false,
    }
}

pub async fn publish_device_provider_challenge(
    db: &Database,
    storage: &dyn SyncStorage,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    bootstrap: ProvisionalDeviceBootstrap,
) -> Result<ProviderReadyDeviceBootstrap, DeviceJoinError> {
    let offer = &bootstrap.request.approval.request.offer;
    let owner = Box::pin(db.activated_store_device_registration(offer.owner_registration.clone()))
        .await
        .map_err(database_error)?;
    let administrator = Box::pin(
        db.activated_store_device_registration(offer.provider_admin.administrator.clone()),
    )
    .await
    .map_err(database_error)?;
    bootstrap.request.approval.verify(&owner, &administrator)?;
    let challenge_publication = match &bootstrap.request.approval.admission {
        DeviceProviderAdmissionChallenge::SamePrincipal => {
            DeviceProviderChallengePublication::SamePrincipal
        }
        DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
            let exact = administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
            let context = cross_challenge_context(&bootstrap.request.approval.request);
            let (_, activation_author) =
                Box::pin(crate::sync::store_pull::load_commit_with_author(
                    storage,
                    &offer.store_root,
                    &bootstrap.publication_authorization.attempt_activation,
                ))
                .await?;
            let authorization = DeviceJoinChallengePublicationAuthorization {
                attempt: bootstrap.publication_authorization.attempt.clone(),
                attempt_activation: bootstrap
                    .publication_authorization
                    .attempt_activation
                    .clone(),
            };
            let published = Box::pin(crate::sync::provider::publish_cross_principal_challenge(
                storage,
                exact,
                db,
                &authorization,
                challenge,
                &context,
                &offer.provider,
                &owner,
                &activation_author,
                &administrator.device_signing_pubkey,
            ))
            .await
            .map_err(provider_error)?;
            DeviceProviderChallengePublication::CrossPrincipal {
                challenge: published,
            }
        }
    };
    let attempt_id = offer.attempt_id;
    let ready = ProviderReadyDeviceBootstrap {
        bootstrap: Box::new(bootstrap),
        challenge_publication,
    };
    if let Some(current) = Box::pin(load_store_journal(
        db,
        attempt_id,
        DeviceJoinRole::ProviderAdministrator,
    ))
    .await?
    {
        match &*current.progress {
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ProviderReady(existing),
            ) if existing == &ready => return Ok(ready),
            DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ApprovalPrepared(approval),
            ) if approval == &*ready.bootstrap.request.approval => {
                let observed = DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::AttemptObserved(*ready.bootstrap.clone()),
                    )),
                };
                Box::pin(advance_store_journal(db, &current, observed.clone())).await?;
                let intent = DeviceJoinJournalRecord {
                    attempt_id,
                    progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                        ProviderAdminJoinProgress::ChallengeCreateIntent(*ready.bootstrap.clone()),
                    )),
                };
                Box::pin(advance_store_journal(db, &observed, intent.clone())).await?;
                Box::pin(advance_store_journal(
                    db,
                    &intent,
                    DeviceJoinJournalRecord {
                        attempt_id,
                        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                            ProviderAdminJoinProgress::ProviderReady(ready.clone()),
                        )),
                    },
                ))
                .await?;
            }
            _ => return Err(DeviceJoinError::JournalConflict),
        }
    }
    Ok(ready)
}

#[allow(clippy::too_many_arguments)]
pub async fn bootstrap_pending_device(
    db: &Database,
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    peer_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    bootstrap: ProviderReadyDeviceBootstrap,
    published_at: &str,
) -> Result<DeviceJoinReadiness, DeviceJoinError> {
    let offer = &bootstrap.bootstrap.request.approval.request.offer;
    let attempt_owner = Box::pin(crate::sync::store_objects::load_registration_ref(
        storage,
        &offer.store_root,
        &offer.owner_registration,
    ))
    .await?
    .value;
    let administrator = Box::pin(crate::sync::store_objects::load_registration_ref(
        storage,
        &offer.store_root,
        &offer.provider_admin.administrator,
    ))
    .await?
    .value;
    let verified_attempt = Box::pin(crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &offer.store_root,
        &bootstrap.bootstrap.publication_authorization.attempt,
        &attempt_owner,
    ))
    .await?;
    if verified_attempt.value.expected_registration
        != bootstrap.bootstrap.request.expected_registration
    {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let bootstrap_authorization =
        Box::pin(crate::sync::store_pull::load_device_join_authorization(
            storage,
            &offer.store_root,
            &verified_attempt.value.membership,
        ))
        .await?;
    let bootstrap_plan = Box::pin(crate::sync::store_pull::prepare_device_join_bootstrap(
        storage,
        &offer.store_root,
        &verified_attempt.value.bootstrap_cut,
        &bootstrap
            .bootstrap
            .publication_authorization
            .attempt_activation,
        &bootstrap_authorization,
    ))
    .await?;
    let proof = Box::pin(crate::sync::store_registration::bootstrap_pending_device(
        db,
        storage,
        identity_signer,
        bootstrap
            .bootstrap
            .publication_authorization
            .attempt
            .clone(),
        verified_attempt,
        bootstrap_plan,
        bootstrap
            .bootstrap
            .publication_authorization
            .attempt_activation
            .clone(),
        &attempt_owner,
        published_at,
    ))
    .await?;
    let provider = match (
        &bootstrap.bootstrap.request.approval.admission,
        &bootstrap.bootstrap.request.response,
        &bootstrap.challenge_publication,
    ) {
        (
            DeviceProviderAdmissionChallenge::SamePrincipal,
            DeviceProviderResponseReservation::SamePrincipal,
            DeviceProviderChallengePublication::SamePrincipal,
        ) => DeviceProviderReadiness::SamePrincipal,
        (
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
            DeviceProviderResponseReservation::CrossPrincipal { response_slot },
            DeviceProviderChallengePublication::CrossPrincipal {
                challenge: published,
            },
        ) if challenge == published => {
            let exact = peer_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
            let context = crate::sync::provider::CrossPrincipalResponseContext {
                challenge: cross_challenge_context(&bootstrap.bootstrap.request.approval.request),
                expected_registration_hash: bootstrap
                    .bootstrap
                    .request
                    .expected_registration
                    .registration_hash(),
                response_slot: response_slot.clone(),
            };
            DeviceProviderReadiness::CrossPrincipal(
                Box::pin(crate::sync::provider::create_cross_principal_response(
                    exact,
                    challenge,
                    &context,
                    &offer.provider,
                    &administrator.device_signing_pubkey,
                    identity_signer,
                ))
                .await
                .map_err(provider_error)?,
            )
        }
        _ => return Err(DeviceJoinError::AttemptMismatch),
    };
    let readiness = DeviceJoinReadiness { proof, provider };
    let pending = pending.clone();
    let bootstrap = Box::new(bootstrap);
    let readiness = Box::new(readiness);
    tokio::task::spawn_blocking(move || record_joiner_readiness(&pending, *bootstrap, *readiness))
        .await
        .map_err(|error| {
            DeviceJoinError::Store(format!("joiner readiness journal task failed: {error}"))
        })?
}

fn record_joiner_readiness(
    pending: &DeviceJoinJournalDatabase,
    bootstrap: ProviderReadyDeviceBootstrap,
    readiness: DeviceJoinReadiness,
) -> Result<DeviceJoinReadiness, DeviceJoinError> {
    let offer = &bootstrap.bootstrap.request.approval.request.offer;
    let current = pending
        .load(offer.attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    let prepared = match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request))
            if request == &*bootstrap.bootstrap.request =>
        {
            current
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(existing))
            if existing == &readiness =>
        {
            return Ok(readiness)
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    let provider_ready = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::ProviderReady(bootstrap.clone()),
        )),
    };
    pending.advance(&prepared, provider_ready.clone())?;
    let registration_intent = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::RegistrationCreateIntent(bootstrap.clone()),
        )),
    };
    pending.advance(&provider_ready, registration_intent.clone())?;
    let registration_created = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::RegistrationCreated(readiness.proof.registration.clone()),
        )),
    };
    pending.advance(&registration_intent, registration_created.clone())?;
    let ack_intent = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::AckCreateIntent(readiness.proof.registration.clone()),
        )),
    };
    pending.advance(&registration_created, ack_intent.clone())?;
    let ack_created = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::AckCreated(readiness.proof.initial_ack.clone()),
        )),
    };
    pending.advance(&ack_intent, ack_created.clone())?;
    let ready_record = DeviceJoinJournalRecord {
        attempt_id: offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(
            readiness.clone(),
        ))),
    };
    match readiness.provider {
        DeviceProviderReadiness::SamePrincipal => pending.advance(&ack_created, ready_record)?,
        DeviceProviderReadiness::CrossPrincipal(_) => {
            let response_intent = DeviceJoinJournalRecord {
                attempt_id: offer.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::ResponseCreateIntent(readiness.clone()),
                )),
            };
            pending.advance(&ack_created, response_intent.clone())?;
            pending.advance(&response_intent, ready_record)?;
        }
    }
    Ok(readiness)
}

fn cross_challenge_context(
    request: &DeviceProviderAccessRequest,
) -> crate::sync::provider::CrossPrincipalChallengeContext {
    crate::sync::provider::CrossPrincipalChallengeContext {
        root: request.offer.store_root.clone(),
        attempt_id: request.offer.attempt_id,
        access_request_hash: request.request_hash(),
        provider_admin_grant: request.offer.provider_admin.grant_id.clone(),
        owner_registration: request.offer.owner_registration.clone(),
        member_pubkey: request.offer.member_pubkey.clone(),
        administrator_binding: request.offer.provider_admin.provider.clone(),
        peer_binding: request.peer_provider.clone(),
    }
}

pub async fn complete_device_provider_admission(
    db: &Database,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    readiness: DeviceJoinReadiness,
) -> Result<DeviceProviderAdmissionCompletion, DeviceJoinError> {
    let attempt_id = readiness.proof.attempt.attempt_id;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::ProviderAdministrator)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
        existing,
    )) = &*current.progress
    {
        if *existing.readiness == readiness {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let bootstrap = match &*current.progress {
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ProviderReady(bootstrap),
        ) => bootstrap.clone(),
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    if readiness.proof.attempt != bootstrap.bootstrap.publication_authorization.attempt {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let offer = &bootstrap.bootstrap.request.approval.request.offer;
    let administrator = db
        .activated_store_device_registration(offer.provider_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let administrator_signer = administrator.device_signer(identity_signer)?;
    let admission = match (
        &bootstrap.bootstrap.request.approval.admission,
        &bootstrap.bootstrap.request.response,
        &readiness.provider,
    ) {
        (
            DeviceProviderAdmissionChallenge::SamePrincipal,
            DeviceProviderResponseReservation::SamePrincipal,
            DeviceProviderReadiness::SamePrincipal,
        ) => DeviceProviderAdmission::SamePrincipal,
        (
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
            DeviceProviderResponseReservation::CrossPrincipal { response_slot },
            DeviceProviderReadiness::CrossPrincipal(response),
        ) => {
            let exact = administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
            let context = crate::sync::provider::CrossPrincipalResponseContext {
                challenge: cross_challenge_context(&bootstrap.bootstrap.request.approval.request),
                expected_registration_hash: bootstrap
                    .bootstrap
                    .request
                    .expected_registration
                    .registration_hash(),
                response_slot: response_slot.clone(),
            };
            DeviceProviderAdmission::CrossPrincipal(
                crate::sync::provider::complete_cross_principal_probe(
                    exact,
                    db,
                    challenge,
                    response,
                    &context,
                    &offer.provider,
                    &administrator_signer,
                    &offer.member_pubkey,
                )
                .await
                .map_err(provider_error)?,
            )
        }
        _ => return Err(DeviceJoinError::AttemptMismatch),
    };
    let completion = DeviceProviderAdmissionCompletion {
        readiness: Box::new(readiness.clone()),
        admission,
    };
    let observed = DeviceJoinJournalRecord {
        attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ResponseObserved(readiness),
        )),
    };
    advance_store_journal(db, &current, observed.clone()).await?;
    advance_store_journal(
        db,
        &observed,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::Completed(completion.clone()),
            )),
        },
    )
    .await?;
    Ok(completion)
}

#[allow(clippy::too_many_arguments)]
pub async fn cancel_device_join(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    attempt_ref: DeviceJoinAttemptRef,
) -> Result<DeviceJoinCancellation, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    let current = load_store_journal(db, attempt_ref.attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(existing)) =
        &*current.progress
    {
        if existing.outcome.attempt() == &attempt_ref {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let expected_attempt = match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => {
            &bootstrap.publication_authorization.attempt
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(bootstrap)) => {
            &bootstrap.bootstrap.publication_authorization.attempt
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AdmissionCompleted(completion)) => {
            &completion.readiness.proof.attempt
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
            attempt,
            ..
        }) => attempt,
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    if expected_attempt != &attempt_ref {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_prefix =
        crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id);
    let attempt_bytes = storage
        .read_protocol_object(&attempt_context, &attempt_ref.object, &attempt_prefix)
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = db
        .activated_store_device_registration(unverified_attempt.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if owner.device_id.to_string() != local_device_id
        || !authorization.is_owner_now(&keys::public_key_hex(identity_signer))
    {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let owner_signer = owner.device_signer(identity_signer)?;
    let outcome = crate::sync::store_commit::DeviceJoinOutcome::signed(
        attempt_ref.clone(),
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled,
        attempt.owner_registration.clone(),
        attempt.owner_grant.clone(),
        &owner,
        &owner_signer,
    )?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinOutcome,
    );
    let prefix =
        crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_ref.attempt_id);
    let prepared = storage.prepare_protocol_object(
        &context,
        attempt.outcome_slot.clone(),
        &prefix,
        outcome.to_bytes(),
    )?;
    let outcome_ref = DeviceJoinOutcomeRef::Cancelled {
        attempt: attempt_ref.clone(),
        outcome_hash: outcome.outcome_hash(),
        object: prepared.reference().clone(),
    };
    let intent = DeviceJoinJournalRecord {
        attempt_id: attempt_ref.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::CancellationCreateIntent {
                attempt: attempt_ref.clone(),
                cancellation: outcome_ref.clone(),
                prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
            },
        )),
    };
    match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(_))
        | DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(_))
        | DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AdmissionCompleted(_)) => {
            advance_store_journal(db, &current, intent.clone()).await?;
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
            attempt,
            cancellation,
            prepared: durable_prepared,
        }) if attempt == &attempt_ref
            && cancellation == &outcome_ref
            && durable_prepared == &PreparedDeviceJoinObject::from_prepared(&prepared) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != outcome.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let verified_outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &outcome_ref,
        &owner,
    )
    .await?;
    if verified_outcome.value != outcome {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let outcome_activation = crate::sync::store_outbound::activate_store_operation_commit(
        db,
        storage,
        coordination,
        plan,
        crate::sync::store_outbound::StoreOperationBatch::Outcome {
            outcome: outcome_ref.clone(),
            registration: None,
        },
    )
    .await?;
    let cancellation = DeviceJoinCancellation {
        outcome: outcome_ref,
        outcome_activation,
    };
    advance_store_journal(
        db,
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: attempt_ref.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(
                cancellation.clone(),
            ))),
        },
    )
    .await?;
    Ok(cancellation)
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_device_join_cleanup(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    executor_exact: &dyn ExactSlotStorage,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    administrator_terminal: ProviderAdminJoinTerminal,
    joiner_terminal: JoinerJoinTerminal,
) -> Result<DeviceJoinCleanupReceipt, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let current = load_store_journal(db, attempt_ref.attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(existing)) =
        &*current.progress
    {
        return Ok(existing.clone());
    }
    let durable_cancellation = match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(durable)) => durable,
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
            cancellation: durable,
            ..
        }) => durable,
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    if durable_cancellation != &cancellation {
        return Err(DeviceJoinError::JournalConflict);
    }
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_prefix =
        crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id);
    let attempt_bytes = storage
        .read_protocol_object(&attempt_context, &attempt_ref.object, &attempt_prefix)
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = db
        .activated_store_device_registration(unverified_attempt.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &cancellation.outcome,
        &owner,
    )
    .await?
    .value;
    if !matches!(
        outcome.body,
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
    ) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    validate_terminals(
        &cancellation.outcome,
        &administrator_terminal,
        &joiner_terminal,
    )?;
    verify_cleanup_terminals(db, &administrator_terminal, &joiner_terminal).await?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let (local_root, executor_ref, executor, executor_signer) =
        crate::sync::store_outbound::load_local_store_authority(
            db,
            &local_device_id,
            identity_signer,
        )
        .await?;
    if local_root != root || !authorization.is_owner_now(&executor.author_pubkey) {
        return Err(DeviceJoinError::OwnerAuthorityRequired);
    }
    let effective_executor = authorization.resolved_provider_admin(
        &attempt
            .provider_approval
            .request
            .offer
            .provider_admin
            .grant_id,
    )?;
    if effective_executor != *attempt.provider_approval.request.offer.provider_admin
        || effective_executor.administrator != executor_ref
        || effective_executor.provider != executor.provider
    {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let deleted_slots = canonical_cleanup_slots(&attempt)?;
    let receipt_object = DeviceJoinCleanupReceiptObject::signed(
        &attempt,
        cancellation.outcome.clone(),
        administrator_terminal,
        joiner_terminal,
        deleted_slots.clone(),
        plan.membership_state().clone(),
        attempt
            .provider_approval
            .request
            .offer
            .provider_admin
            .grant_id
            .clone(),
        executor_ref,
        &executor,
        &executor_signer,
    )?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinCleanupReceipt,
    );
    let prefix = crate::sync::store_commit::device_join_cleanup_receipt_semantic_prefix(
        attempt_ref.attempt_id,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await?;
    let prepared =
        storage.prepare_protocol_object(&context, slot, &prefix, receipt_object.to_bytes())?;
    let receipt_ref = DeviceJoinCleanupReceiptRef {
        attempt_id: attempt_ref.attempt_id,
        receipt_hash: receipt_object.receipt_hash(),
        object: prepared.reference().clone(),
    };
    let intent = DeviceJoinJournalRecord {
        attempt_id: attempt_ref.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::CleanupReceiptCreateIntent {
                cancellation: cancellation.clone(),
                receipt: receipt_ref.clone(),
                prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
            },
        )),
    };
    match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(_)) => {
            advance_store_journal(db, &current, intent.clone()).await?;
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
            receipt,
            prepared: durable_prepared,
            ..
        }) if receipt == &receipt_ref
            && durable_prepared == &PreparedDeviceJoinObject::from_prepared(&prepared) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    for slot in &deleted_slots {
        ensure_exact_slot_absent(executor_exact, slot).await?;
    }
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != receipt_object.to_bytes() {
        return Err(DeviceJoinError::CleanupMismatch);
    }
    receipt_ref.verify(&receipt_object, &executor)?;
    let receipt = DeviceJoinCleanupReceipt {
        receipt: receipt_ref,
    };
    advance_store_journal(
        db,
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: attempt_ref.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::CleanupReceipt(receipt.clone()),
            )),
        },
    )
    .await?;
    Ok(receipt)
}

pub async fn close_device_provider_admission(
    db: &Database,
    storage: &dyn SyncStorage,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let current = load_store_journal(
        db,
        attempt_ref.attempt_id,
        DeviceJoinRole::ProviderAdministrator,
    )
    .await?
    .ok_or(DeviceJoinError::JournalConflict)?;
    match &*current.progress {
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => return Ok(ProviderAdminJoinTerminal::Completed(completion.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => return Ok(ProviderAdminJoinTerminal::Cancelled(closure.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => return Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation.clone())),
        _ => {}
    }
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_prefix =
        crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id);
    let attempt_bytes = storage
        .read_protocol_object(&attempt_context, &attempt_ref.object, &attempt_prefix)
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = db
        .activated_store_device_registration(unverified_attempt.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &cancellation.outcome,
        &owner,
    )
    .await?
    .value;
    if !matches!(
        outcome.body,
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
    ) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let offer = &attempt.provider_approval.request.offer;
    let administrator = db
        .activated_store_device_registration(offer.provider_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if administrator.device_id.to_string() != local_device_id {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let administrator_signer = administrator.device_signer(identity_signer)?;
    let (challenge, prior_state_hash) = match &*current.progress {
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::CleanupIntent {
                cancellation: durable,
                challenge,
                prior_state_hash,
            },
        ) if durable == &cancellation => (challenge.clone(), *prior_state_hash),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(_)
            | ProviderAdminJoinProgress::AttemptObserved(_)
            | ProviderAdminJoinProgress::ChallengeCreateIntent(_)
            | ProviderAdminJoinProgress::ProviderReady(_)
            | ProviderAdminJoinProgress::ResponseObserved(_),
        ) => {
            let challenge = match &attempt.provider_approval.admission {
                DeviceProviderAdmissionChallenge::SamePrincipal => {
                    ProviderChallengeDisposition::SamePrincipal
                }
                DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                    let exact =
                        administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?;
                    match exact.read_at(&challenge.administrator_object.slot).await {
                        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => {
                            ProviderChallengeDisposition::NeverCreated
                        }
                        Ok(bytes) => {
                            challenge
                                .administrator_object
                                .object
                                .verify(&bytes)
                                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
                            ProviderChallengeDisposition::Created(
                                challenge.administrator_object.object.clone(),
                            )
                        }
                        Err(error) => return Err(DeviceJoinError::Provider(error.to_string())),
                    }
                }
            };
            let prior_state_hash = ObjectHash::digest(&serde_json::to_vec(&current.progress)?);
            let intent = DeviceJoinJournalRecord {
                attempt_id: attempt_ref.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::CleanupIntent {
                        cancellation: cancellation.clone(),
                        challenge: challenge.clone(),
                        prior_state_hash,
                    },
                )),
            };
            advance_store_journal(db, &current, intent).await?;
            (challenge, prior_state_hash)
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    if let DeviceProviderAdmissionChallenge::CrossPrincipal(probe) =
        &attempt.provider_approval.admission
    {
        ensure_exact_slot_absent(
            administrator_exact.ok_or(DeviceJoinError::ExactSlotStorageRequired)?,
            &probe.administrator_object.slot,
        )
        .await?;
    }
    let closure = ProviderAdminJoinClosure::signed(
        cancellation.outcome,
        offer.provider_admin.administrator.clone(),
        challenge,
        prior_state_hash,
        &administrator,
        &administrator_signer,
    )?;
    let intent = load_store_journal(
        db,
        attempt_ref.attempt_id,
        DeviceJoinRole::ProviderAdministrator,
    )
    .await?
    .ok_or(DeviceJoinError::JournalConflict)?;
    advance_store_journal(
        db,
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: attempt_ref.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::Cancelled(closure.clone()),
            )),
        },
    )
    .await?;
    Ok(ProviderAdminJoinTerminal::Cancelled(closure))
}

pub async fn close_joining_device(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    peer_exact: &dyn ExactSlotStorage,
    root: &StoreRootRef,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
) -> Result<JoinerJoinTerminal, DeviceJoinError> {
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let current = pending
        .load(attempt_ref.attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            return Ok(JoinerJoinTerminal::Cancelled(closure.clone()));
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            return Ok(JoinerJoinTerminal::WriteRevoked(revocation.clone()));
        }
        _ => {}
    }
    let allowed = matches!(
        &*current.progress,
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::RegistrationPrepared(_)
                | JoinerJoinProgress::ProviderReady(_)
                | JoinerJoinProgress::RegistrationCreateIntent(_)
                | JoinerJoinProgress::RegistrationCreated(_)
                | JoinerJoinProgress::AckCreateIntent(_)
                | JoinerJoinProgress::AckCreated(_)
                | JoinerJoinProgress::ResponseCreateIntent(_)
                | JoinerJoinProgress::Ready(_)
                | JoinerJoinProgress::CleanupIntent { .. }
        )
    );
    if !allowed {
        return Err(DeviceJoinError::JournalConflict);
    }
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_prefix =
        crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id);
    let attempt_bytes = storage
        .read_protocol_object(&attempt_context, &attempt_ref.object, &attempt_prefix)
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = crate::sync::store_objects::load_registration_ref(
        storage,
        root,
        &unverified_attempt.owner_registration,
    )
    .await?
    .value;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        root,
        &cancellation.outcome,
        &owner,
    )
    .await?
    .value;
    if !matches!(
        outcome.body,
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
    ) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let joining_device_signer = attempt
        .expected_registration
        .device_signer(identity_signer)?;
    let (registration, initial_ack, response, prior_state_hash, intent) = match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupIntent {
            cancellation: durable,
            registration,
            initial_ack,
            response,
            prior_state_hash,
        }) if durable == &cancellation => (
            registration.clone(),
            initial_ack.clone(),
            response.clone(),
            *prior_state_hash,
            current.clone(),
        ),
        _ => {
            let registration = observe_exact_slot(peer_exact, &attempt.registration_slot).await?;
            let initial_ack = observe_exact_slot(
                peer_exact,
                attempt.expected_registration.acknowledgements.first_slot(),
            )
            .await?;
            let response = match &attempt.provider_response {
                DeviceProviderResponseReservation::SamePrincipal => {
                    JoinerResponseDisposition::SamePrincipal
                }
                DeviceProviderResponseReservation::CrossPrincipal { response_slot } => {
                    JoinerResponseDisposition::Slot(
                        observe_exact_slot(peer_exact, response_slot).await?,
                    )
                }
            };
            let prior_state_hash = ObjectHash::digest(&serde_json::to_vec(&current.progress)?);
            let intent = DeviceJoinJournalRecord {
                attempt_id: attempt_ref.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::CleanupIntent {
                        cancellation: cancellation.clone(),
                        registration: registration.clone(),
                        initial_ack: initial_ack.clone(),
                        response: response.clone(),
                        prior_state_hash,
                    },
                )),
            };
            pending.advance(&current, intent.clone())?;
            (
                registration,
                initial_ack,
                response,
                prior_state_hash,
                intent,
            )
        }
    };
    for slot in canonical_cleanup_slots(&attempt)? {
        ensure_exact_slot_absent(peer_exact, &slot).await?;
    }
    let closure = JoinerJoinClosure::signed(
        cancellation.outcome,
        attempt.expected_registration,
        registration,
        initial_ack,
        response,
        prior_state_hash,
        &joining_device_signer,
    )?;
    pending.advance(
        &intent,
        DeviceJoinJournalRecord {
            attempt_id: attempt_ref.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::Cancelled(closure.clone()),
            )),
        },
    )?;
    Ok(JoinerJoinTerminal::Cancelled(closure))
}

#[allow(clippy::too_many_arguments)]
async fn sign_device_join_producer_write_revocation(
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    producer: DeviceJoinProducer,
    withdrawal: ProviderAccessWithdrawal,
    executor_grant: ProviderAdminGrantId,
) -> Result<DeviceJoinProducerWriteRevocation, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    require_cancelled_outcome(&cancellation.outcome)?;
    let attempt_ref = cancellation.outcome.attempt().clone();
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_prefix =
        crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id);
    let attempt_bytes = storage
        .read_protocol_object(&attempt_context, &attempt_ref.object, &attempt_prefix)
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = db
        .activated_store_device_registration(unverified_attempt.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &cancellation.outcome,
        &owner,
    )
    .await?
    .value;
    if !matches!(
        outcome.body,
        crate::sync::store_commit::DeviceJoinOutcomeBody::Cancelled
    ) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let executor_admin = authorization.resolved_provider_admin(&executor_grant)?;
    let executor = db
        .activated_store_device_registration(executor_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if executor.device_id.to_string() != local_device_id {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let executor_signer = executor.device_signer(identity_signer)?;
    let (authority, protected_slots, locator) = match producer {
        DeviceJoinProducer::ProviderAdministrator => {
            let DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) =
                &attempt.provider_approval.admission
            else {
                return Err(DeviceJoinError::CleanupMismatch);
            };
            (
                ProviderWriteAuthorityRef::ProviderAdministrator(
                    attempt
                        .provider_approval
                        .request
                        .offer
                        .provider_admin
                        .grant_id
                        .clone(),
                ),
                vec![challenge.administrator_object.slot.clone()],
                &attempt
                    .provider_approval
                    .request
                    .offer
                    .provider_admin
                    .access,
            )
        }
        DeviceJoinProducer::Joiner => {
            let mut slots = vec![
                attempt.registration_slot.clone(),
                attempt
                    .expected_registration
                    .acknowledgements
                    .first_slot()
                    .clone(),
            ];
            if let DeviceProviderResponseReservation::CrossPrincipal { response_slot } =
                &attempt.provider_response
            {
                slots.push(response_slot.clone());
            }
            (
                ProviderWriteAuthorityRef::MemberAccess(
                    attempt.provider_approval.access_grant.grant_ref.clone(),
                ),
                slots,
                &attempt.provider_approval.access_grant.grant.locator,
            )
        }
    };
    if !withdrawal_matches_locator(&withdrawal, locator) {
        return Err(DeviceJoinError::CleanupMismatch);
    }
    DeviceJoinProducerWriteRevocation::signed(
        cancellation.outcome,
        producer,
        authority,
        protected_slots,
        withdrawal,
        executor_grant,
        executor_admin.administrator,
        &executor,
        &executor_signer,
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn revoke_device_provider_admission_writes(
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    withdrawal: ProviderAccessWithdrawal,
    executor_grant: ProviderAdminGrantId,
) -> Result<ProviderAdminJoinTerminal, DeviceJoinError> {
    let attempt_id = cancellation.outcome.attempt().attempt_id;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::ProviderAdministrator)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    match &*current.progress {
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => return Ok(ProviderAdminJoinTerminal::Completed(completion.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => return Ok(ProviderAdminJoinTerminal::Cancelled(closure.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => return Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation.clone())),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(_)
            | ProviderAdminJoinProgress::AttemptObserved(_)
            | ProviderAdminJoinProgress::ChallengeCreateIntent(_)
            | ProviderAdminJoinProgress::ProviderReady(_)
            | ProviderAdminJoinProgress::ResponseObserved(_)
            | ProviderAdminJoinProgress::CleanupIntent { .. },
        ) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    let revocation = Box::pin(sign_device_join_producer_write_revocation(
        db,
        storage,
        authorization,
        identity_signer,
        cancellation,
        DeviceJoinProducer::ProviderAdministrator,
        withdrawal,
        executor_grant,
    ))
    .await?;
    advance_store_journal(
        db,
        &current,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::WriteRevoked(revocation.clone()),
            )),
        },
    )
    .await?;
    Ok(ProviderAdminJoinTerminal::WriteRevoked(revocation))
}

#[allow(clippy::too_many_arguments)]
pub async fn revoke_joining_device_writes(
    pending: &DeviceJoinJournalDatabase,
    db: &Database,
    storage: &dyn SyncStorage,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    cancellation: DeviceJoinCancellation,
    withdrawal: ProviderAccessWithdrawal,
    executor_grant: ProviderAdminGrantId,
) -> Result<JoinerJoinTerminal, DeviceJoinError> {
    let attempt_id = cancellation.outcome.attempt().attempt_id;
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            return Ok(JoinerJoinTerminal::Ready(readiness.clone()));
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            return Ok(JoinerJoinTerminal::Cancelled(closure.clone()));
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            return Ok(JoinerJoinTerminal::WriteRevoked(revocation.clone()));
        }
        DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::RegistrationPrepared(_)
            | JoinerJoinProgress::ProviderReady(_)
            | JoinerJoinProgress::RegistrationCreateIntent(_)
            | JoinerJoinProgress::RegistrationCreated(_)
            | JoinerJoinProgress::AckCreateIntent(_)
            | JoinerJoinProgress::AckCreated(_)
            | JoinerJoinProgress::ResponseCreateIntent(_)
            | JoinerJoinProgress::CleanupIntent { .. },
        ) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    let revocation = Box::pin(sign_device_join_producer_write_revocation(
        db,
        storage,
        authorization,
        identity_signer,
        cancellation,
        DeviceJoinProducer::Joiner,
        withdrawal,
        executor_grant,
    ))
    .await?;
    pending.advance(
        &current,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::WriteRevoked(revocation.clone()),
            )),
        },
    )?;
    Ok(JoinerJoinTerminal::WriteRevoked(revocation))
}

#[allow(clippy::too_many_arguments)]
pub async fn activate_device_join_cleanup(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    attempt_id: DeviceJoinAttemptId,
    receipt: DeviceJoinCleanupReceipt,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(existing)) =
        &*current.progress
    {
        if existing.receipt == receipt.receipt {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(durable)) =
        &*current.progress
    else {
        return Err(DeviceJoinError::JournalConflict);
    };
    if durable != &receipt {
        return Err(DeviceJoinError::JournalConflict);
    }
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinCleanupReceipt,
    );
    let prefix = crate::sync::store_commit::device_join_cleanup_receipt_semantic_prefix(
        receipt.receipt.attempt_id,
    );
    let bytes = storage
        .read_protocol_object(&context, &receipt.receipt.object, &prefix)
        .await?;
    let receipt_object: DeviceJoinCleanupReceiptObject = serde_json::from_slice(&bytes)?;
    let executor = db
        .activated_store_device_registration(receipt_object.executor.clone())
        .await
        .map_err(database_error)?;
    receipt.receipt.verify(&receipt_object, &executor)?;
    if receipt_object.store_root_hash != root.store_root_hash
        || plan.membership_state() != &receipt_object.membership
    {
        return Err(DeviceJoinError::CleanupMismatch);
    }
    let activation_ref = crate::sync::store_outbound::activate_store_operation_commit(
        db,
        storage,
        coordination,
        plan,
        crate::sync::store_outbound::StoreOperationBatch::CleanupReceipt(receipt.receipt.clone()),
    )
    .await?;
    let activation = DeviceJoinCleanupActivation {
        receipt: receipt.receipt,
        activation: activation_ref,
    };
    advance_store_journal(
        db,
        &current,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::CleanupActivated(activation.clone()),
            )),
        },
    )
    .await?;
    Ok(activation)
}

pub async fn complete_owner_device_join_cleanup(
    db: &Database,
    attempt_id: DeviceJoinAttemptId,
    activation: DeviceJoinCleanupActivation,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancelledComplete(existing)) =
        &*current.progress
    {
        if existing == &activation {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(durable)) =
        &*current.progress
    else {
        return Err(DeviceJoinError::JournalConflict);
    };
    if durable != &activation {
        return Err(DeviceJoinError::JournalConflict);
    }
    advance_store_journal(
        db,
        &current,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::CancelledComplete(activation.clone()),
            )),
        },
    )
    .await?;
    Ok(activation)
}

pub async fn accept_joiner_device_join_cleanup(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activation: DeviceJoinCleanupActivation,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let attempt_id = activation.receipt.attempt_id;
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(existing)) =
        &*current.progress
    {
        if existing == &activation {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(existing)) =
        &*current.progress
    {
        if existing == &activation {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let terminal = match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            JoinerJoinTerminal::Cancelled(closure.clone())
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            JoinerJoinTerminal::WriteRevoked(revocation.clone())
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    crate::sync::store_pull::verify_device_join_cleanup_activation(
        storage,
        root,
        &activation,
        &terminal,
    )
    .await?;
    let activated = DeviceJoinJournalRecord {
        attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::CleanupActivated(activation.clone()),
        )),
    };
    pending.advance(&current, activated)?;
    Ok(activation)
}

pub fn complete_joiner_device_join_cleanup(
    pending: &DeviceJoinJournalDatabase,
    activation: DeviceJoinCleanupActivation,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinError> {
    let attempt_id = activation.receipt.attempt_id;
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(existing)) =
        &*current.progress
    {
        if existing == &activation {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(durable)) =
        &*current.progress
    else {
        return Err(DeviceJoinError::JournalConflict);
    };
    if durable != &activation {
        return Err(DeviceJoinError::JournalConflict);
    }
    pending.advance(
        &current,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::CancelledComplete(activation.clone()),
            )),
        },
    )?;
    Ok(activation)
}

pub fn device_join_status(record: &DeviceJoinJournalRecord) -> DeviceJoinStatus {
    match &*record.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(offer))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(offer)) => {
            DeviceJoinStatus::AwaitingAccessRequest {
                offer: offer.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(request),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(request)) => {
            DeviceJoinStatus::AwaitingProviderAdmission {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(approval),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(approval)) => {
            DeviceJoinStatus::AwaitingRegistrationRequest {
                approval: approval.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::RegistrationRequested(request))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationPrepared(request)) => {
            DeviceJoinStatus::AwaitingBootstrap {
                request: request.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap))
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AttemptObserved(bootstrap),
        )
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ChallengeCreateIntent(bootstrap),
        ) => DeviceJoinStatus::AwaitingChallengePublication {
            bootstrap: bootstrap.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ProviderReady(bootstrap))
        | DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ProviderReady(bootstrap),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ProviderReady(bootstrap))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::RegistrationCreateIntent(bootstrap)) => {
            DeviceJoinStatus::AwaitingReadiness {
                bootstrap: bootstrap.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ResponseObserved(readiness),
        )
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(readiness)) => {
            DeviceJoinStatus::AwaitingProviderCompletion {
                readiness: readiness.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AdmissionCompleted(completion))
        | DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Completed(
            completion,
        )) => DeviceJoinStatus::AwaitingActivation {
            completion: completion.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ActivationObserved(activation)) => {
            DeviceJoinStatus::AwaitingCompletion {
                activation: activation.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Activated(store))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Activated(store)) => {
            DeviceJoinStatus::Activated {
                store: store.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Abandoned(abandonment))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(abandonment)) => {
            DeviceJoinStatus::Abandoned {
                abandonment: abandonment.clone(),
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessGrantPrepared { request, grant, .. },
        ) => DeviceJoinStatus::ProviderAccessGrantCreatePending {
            request: request.clone(),
            grant: grant.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AbandonmentCreateIntent {
            abandonment,
            ..
        }) => DeviceJoinStatus::AbandonmentCreatePending {
            abandonment: abandonment.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancellationCreateIntent {
            cancellation,
            ..
        }) => DeviceJoinStatus::CancellationCreatePending {
            cancellation: cancellation.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Cancelled(cancellation)) => {
            DeviceJoinStatus::CleanupPending {
                cancellation: cancellation.clone(),
                progress: DeviceJoinCleanupProgress::AwaitingBoth,
            }
        }
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::CleanupIntent { cancellation, .. },
        ) => DeviceJoinStatus::ProviderClosurePending {
            cancellation: cancellation.clone(),
            producer: DeviceJoinProducer::ProviderAdministrator,
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupIntent {
            cancellation, ..
        }) => DeviceJoinStatus::ProviderClosurePending {
            cancellation: cancellation.clone(),
            producer: DeviceJoinProducer::Joiner,
        },
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::Cancelled(
            closure,
        )) => DeviceJoinStatus::ProviderClosed {
            terminal: ProviderAdminJoinTerminal::Cancelled(closure.clone()),
        },
        DeviceJoinRoleProgress::ProviderAdministrator(ProviderAdminJoinProgress::WriteRevoked(
            revocation,
        )) => DeviceJoinStatus::ProviderClosed {
            terminal: ProviderAdminJoinTerminal::WriteRevoked(revocation.clone()),
        },
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Cancelled(closure)) => {
            DeviceJoinStatus::JoinerClosed {
                terminal: JoinerJoinTerminal::Cancelled(closure.clone()),
            }
        }
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::WriteRevoked(revocation)) => {
            DeviceJoinStatus::JoinerClosed {
                terminal: JoinerJoinTerminal::WriteRevoked(revocation.clone()),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceiptCreateIntent {
            cancellation,
            receipt,
            ..
        }) => DeviceJoinStatus::CleanupReceiptCreatePending {
            cancellation: cancellation.clone(),
            receipt: receipt.clone(),
        },
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupReceipt(receipt)) => {
            DeviceJoinStatus::AwaitingCleanupActivation {
                receipt: receipt.clone(),
            }
        }
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CleanupActivated(activation))
        | DeviceJoinRoleProgress::Owner(OwnerJoinProgress::CancelledComplete(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CleanupActivated(activation))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::CancelledComplete(activation)) => {
            DeviceJoinStatus::CleanupActivated {
                activation: activation.clone(),
            }
        }
        _ => DeviceJoinStatus::OperationInProgress {
            attempt_id: record.attempt_id,
        },
    }
}

pub async fn load_store_device_join_status(
    db: &Database,
    attempt_id: DeviceJoinAttemptId,
    role: DeviceJoinRole,
) -> Result<Option<DeviceJoinStatus>, DeviceJoinError> {
    load_store_journal(db, attempt_id, role)
        .await
        .map(|record| record.as_ref().map(device_join_status))
}

pub fn load_pending_device_join_status(
    pending: &DeviceJoinJournalDatabase,
    attempt_id: DeviceJoinAttemptId,
) -> Result<Option<DeviceJoinStatus>, DeviceJoinError> {
    pending
        .load(attempt_id, DeviceJoinRole::Joiner)
        .map(|record| record.as_ref().map(device_join_status))
}

pub async fn observe_device_join_abandonment(
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    abandonment: DeviceJoinAbandonment,
) -> Result<DeviceJoinAbandonment, DeviceJoinError> {
    let current = pending
        .load(abandonment.abandonment.attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Abandoned(existing)) =
        &*current.progress
    {
        if existing == &abandonment {
            return Ok(existing.clone());
        }
        return Err(DeviceJoinError::JournalConflict);
    }
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAbandonment,
    );
    let prefix = crate::sync::store_commit::device_join_abandonment_semantic_prefix(
        abandonment.abandonment.attempt_id,
    );
    let bytes = storage
        .read_protocol_object(&context, &abandonment.abandonment.object, &prefix)
        .await?;
    let object: DeviceJoinAbandonmentObject = serde_json::from_slice(&bytes)?;
    let owner = crate::sync::store_objects::load_registration_ref(
        storage,
        root,
        &object.owner_registration,
    )
    .await?
    .value;
    abandonment.abandonment.verify(&object, &owner)?;
    let (activation, author) = crate::sync::store_pull::load_commit_with_author(
        storage,
        root,
        &abandonment.abandonment_activation,
    )
    .await?;
    if author != owner
        || activation
            .device_join_abandonments()
            .binary_search(&abandonment.abandonment)
            .is_err()
    {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    match &*current.progress {
        DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::AccessRequested(_))
        | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::ApprovalReceived(_)) => {}
        _ => return Err(DeviceJoinError::JournalConflict),
    }
    pending.advance(
        &current,
        DeviceJoinJournalRecord {
            attempt_id: abandonment.abandonment.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(
                JoinerJoinProgress::Abandoned(abandonment.clone()),
            )),
        },
    )?;
    Ok(abandonment)
}

#[allow(clippy::too_many_arguments)]
pub async fn finalize_device_join(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    completion: DeviceProviderAdmissionCompletion,
) -> Result<DeviceJoinActivation, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    let attempt_ref = completion.readiness.proof.attempt.clone();
    let attempt_id = attempt_ref.attempt_id;
    let current = load_store_journal(db, attempt_id, DeviceJoinRole::Owner)
        .await?
        .ok_or(DeviceJoinError::JournalConflict)?;
    if let DeviceJoinRoleProgress::Owner(OwnerJoinProgress::ActivationPrepared(existing)) =
        &*current.progress
    {
        return Ok(existing.clone());
    }
    let provisional = match &*current.progress {
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::AttemptActivated(bootstrap)) => {
            bootstrap.clone()
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    let offer = &provisional.request.approval.request.offer;
    let owner = db
        .activated_store_device_registration(offer.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let owner_signer = owner.device_signer(identity_signer)?;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &offer.store_root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let registration = crate::sync::store_objects::load_registration_ref(
        storage,
        &offer.store_root,
        &completion.readiness.proof.registration,
    )
    .await?
    .value;
    let ack = crate::sync::store_objects::load_store_ack_ref(
        storage,
        &offer.store_root,
        &completion.readiness.proof.initial_ack,
        &registration,
    )
    .await?
    .value;
    completion.readiness.proof.verify(
        &attempt_ref,
        &attempt,
        &registration,
        &completion.readiness.proof.initial_ack,
        &ack,
    )?;
    match (&attempt.provider_approval.admission, &completion.admission) {
        (
            DeviceProviderAdmissionChallenge::SamePrincipal,
            DeviceProviderAdmission::SamePrincipal,
        ) => {}
        (
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge),
            DeviceProviderAdmission::CrossPrincipal(receipt),
        ) => {
            let administrator = db
                .activated_store_device_registration(offer.provider_admin.administrator.clone())
                .await
                .map_err(database_error)?;
            let response_slot = match &attempt.provider_response {
                DeviceProviderResponseReservation::CrossPrincipal { response_slot } => {
                    response_slot.clone()
                }
                DeviceProviderResponseReservation::SamePrincipal => {
                    return Err(DeviceJoinError::AttemptMismatch);
                }
            };
            let context = crate::sync::provider::CrossPrincipalResponseContext {
                challenge: cross_challenge_context(&attempt.provider_approval.request),
                expected_registration_hash: attempt.expected_registration.registration_hash(),
                response_slot,
            };
            receipt
                .verify(
                    &context,
                    &offer.provider,
                    &administrator.device_signing_pubkey,
                    &offer.member_pubkey,
                )
                .map_err(provider_error)?;
            if &receipt.transcript.challenge != challenge {
                return Err(DeviceJoinError::AttemptMismatch);
            }
        }
        _ => return Err(DeviceJoinError::AttemptMismatch),
    }
    let outcome = crate::sync::store_commit::DeviceJoinOutcome::signed(
        attempt_ref.clone(),
        crate::sync::store_commit::DeviceJoinOutcomeBody::Activated {
            readiness: completion.readiness.proof.clone(),
        },
        offer.owner_registration.clone(),
        offer.owner_grant.clone(),
        &owner,
        &owner_signer,
    )?;
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        offer.store_root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinOutcome,
    );
    let prefix = crate::sync::store_commit::device_join_outcome_semantic_prefix(attempt_id);
    let prepared = storage.prepare_protocol_object(
        &context,
        attempt.outcome_slot.clone(),
        &prefix,
        outcome.to_bytes(),
    )?;
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != outcome.to_bytes() {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let outcome_ref = DeviceJoinOutcomeRef::Activated {
        attempt: attempt_ref,
        outcome_hash: outcome.outcome_hash(),
        object: prepared.reference().clone(),
    };
    let activated_registration = crate::sync::store_outbound::DeviceJoinRegistrationActivation {
        reference: crate::sync::store_commit::ActivatedStoreDeviceRegistrationRef {
            registration: completion.readiness.proof.registration.clone(),
            authority: crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Join {
                attempt_id,
                outcome: outcome_ref.clone(),
            },
        },
        registration: attempt.expected_registration.clone(),
        authority: crate::sync::store_commit::StoreDeviceRegistrationActivation::Join {
            attempt_id,
            outcome: outcome_ref.clone(),
        },
    };
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let activation_ref = crate::sync::store_outbound::activate_store_operation_commit(
        db,
        storage,
        coordination,
        plan,
        crate::sync::store_outbound::StoreOperationBatch::Outcome {
            outcome: outcome_ref.clone(),
            registration: Some(Box::new(activated_registration)),
        },
    )
    .await?;
    let activation = DeviceJoinActivation {
        outcome: outcome_ref,
        outcome_activation: activation_ref,
    };
    let provider_ready = ProviderReadyDeviceBootstrap {
        bootstrap: Box::new(provisional),
        challenge_publication: match &attempt.provider_approval.admission {
            DeviceProviderAdmissionChallenge::SamePrincipal => {
                DeviceProviderChallengePublication::SamePrincipal
            }
            DeviceProviderAdmissionChallenge::CrossPrincipal(challenge) => {
                DeviceProviderChallengePublication::CrossPrincipal {
                    challenge: challenge.clone(),
                }
            }
        },
    };
    let ready_record = DeviceJoinJournalRecord {
        attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::ProviderReady(provider_ready),
        )),
    };
    advance_store_journal(db, &current, ready_record.clone()).await?;
    let completion_record = DeviceJoinJournalRecord {
        attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Owner(
            OwnerJoinProgress::AdmissionCompleted(completion),
        )),
    };
    advance_store_journal(db, &ready_record, completion_record.clone()).await?;
    advance_store_journal(
        db,
        &completion_record,
        DeviceJoinJournalRecord {
            attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::Owner(
                OwnerJoinProgress::ActivationPrepared(activation.clone()),
            )),
        },
    )
    .await?;
    Ok(activation)
}

pub async fn materialize_device_join_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    activation: DeviceJoinActivation,
) -> Result<JoinedStore, DeviceJoinError> {
    if !matches!(activation.outcome, DeviceJoinOutcomeRef::Activated { .. }) {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    let root = db
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let attempt_ref = activation.outcome.attempt().clone();
    let attempt_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::DeviceJoinAttempt,
    );
    let attempt_bytes = storage
        .read_protocol_object(
            &attempt_context,
            &attempt_ref.object,
            &crate::sync::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id),
        )
        .await?;
    let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)?;
    let owner = db
        .activated_store_device_registration(unverified_attempt.owner_registration.clone())
        .await
        .map_err(database_error)?;
    let attempt = crate::sync::store_objects::load_device_join_attempt_ref(
        storage,
        &root,
        &attempt_ref,
        &owner,
    )
    .await?
    .value;
    let authorization = Box::pin(crate::sync::store_pull::load_device_join_authorization(
        storage,
        &root,
        &attempt.membership,
    ))
    .await?;
    Box::pin(crate::sync::store_pull::materialize_device_join_activation(
        db,
        storage,
        &root,
        &activation.outcome_activation,
        &activation.outcome,
        &authorization,
    ))
    .await?;
    let outcome = crate::sync::store_objects::load_device_join_outcome_ref(
        storage,
        &root,
        &activation.outcome,
        &owner,
    )
    .await?
    .value;
    let crate::sync::store_commit::DeviceJoinOutcomeBody::Activated { readiness } = outcome.body
    else {
        return Err(DeviceJoinError::AttemptMismatch);
    };
    let local = db
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if !local.is_activated()
        || local.registration_hash != readiness.registration.registration_hash
        || local.device_id != readiness.registration.device_id
        || attempt.expected_registration.to_bytes() != local.registration_bytes
    {
        return Err(DeviceJoinError::ActivationNotMaterialized);
    }
    let joined = JoinedStore {
        store_root: root,
        registration: readiness.registration.clone(),
        activation,
    };
    Ok(joined)
}

pub async fn complete_device_join(
    db: &Database,
    pending: &DeviceJoinJournalDatabase,
    storage: &dyn SyncStorage,
    activation: DeviceJoinActivation,
) -> Result<JoinedStore, DeviceJoinError> {
    let attempt_id = activation.outcome.attempt().attempt_id;
    let joined = materialize_device_join_activation(db, storage, activation).await?;
    if let Some(record) = load_store_journal(db, attempt_id, DeviceJoinRole::Joiner).await? {
        return match &*record.progress {
            DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Activated(existing))
                if existing == &joined =>
            {
                Ok(joined)
            }
            _ => Err(DeviceJoinError::JournalConflict),
        };
    }
    let current = pending
        .load(attempt_id, DeviceJoinRole::Joiner)?
        .ok_or(DeviceJoinError::JournalConflict)?;
    let DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(current_readiness)) =
        &*current.progress
    else {
        return Err(DeviceJoinError::JournalConflict);
    };
    if current_readiness.proof.registration != joined.registration {
        return Err(DeviceJoinError::JournalConflict);
    }
    let activated_record = DeviceJoinJournalRecord {
        attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::Joiner(
            JoinerJoinProgress::Activated(joined.clone()),
        )),
    };
    let store_key = store_journal_key(attempt_id, DeviceJoinRole::Joiner.as_str());
    let store_payload = serde_json::to_string(&activated_record)?;
    let pending_path = pending.path().to_string_lossy().into_owned();
    let pending_attempt = attempt_key(attempt_id);
    let expected_pending = serde_json::to_string(&current)?;
    db.call(move |connection| {
        connection
            .execute("ATTACH DATABASE ?1 AS pending_join_source", [&pending_path])
            .map_err(crate::database::DbError::from)?;
        let tx = connection
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        let actual: String = tx
            .query_row(
                "SELECT payload FROM pending_join_source.device_join_journals
                 WHERE attempt_id = ?1 AND role = 'joiner'",
                [&pending_attempt],
                |row| row.get(0),
            )
            .map_err(crate::database::DbError::from)?;
        if actual != expected_pending {
            return Err(crate::database::DbError::Message(
                "pending join journal changed before activation".to_string(),
            ));
        }
        tx.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE value = excluded.value",
            (&store_key, &store_payload),
        )
        .map_err(crate::database::DbError::from)?;
        tx.execute(
            "DELETE FROM pending_join_source.device_join_journals
             WHERE attempt_id = ?1 AND role = 'joiner' AND payload = ?2",
            (&pending_attempt, &expected_pending),
        )
        .map_err(crate::database::DbError::from)?;
        tx.commit().map_err(crate::database::DbError::from)?;
        connection
            .execute_batch("DETACH DATABASE pending_join_source")
            .map_err(crate::database::DbError::from)
    })
    .await
    .map_err(database_error)?;
    Ok(joined)
}

#[allow(clippy::too_many_arguments)]
pub async fn authorize_device_provider_access(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    administrator_exact: Option<&dyn ExactSlotStorage>,
    access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    authorization: &DeviceJoinAuthorization,
    identity_signer: &UserKeypair,
    request: DeviceProviderAccessRequest,
) -> Result<DeviceProviderAdmissionApproval, DeviceJoinError> {
    require_authorization_policy(db, storage, authorization).await?;
    let owner = db
        .activated_store_device_registration(request.offer.owner_registration.clone())
        .await
        .map_err(database_error)?;
    request.verify(&owner)?;
    let provider_admin =
        authorization.resolved_provider_admin(&request.offer.provider_admin.grant_id)?;
    if provider_admin != *request.offer.provider_admin {
        return Err(DeviceJoinError::OfferMismatch);
    }
    let administrator = db
        .activated_store_device_registration(provider_admin.administrator.clone())
        .await
        .map_err(database_error)?;
    let local_device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(database_error)?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    if administrator.device_id.to_string() != local_device_id {
        return Err(DeviceJoinError::ProviderAdministratorRequired);
    }
    let administrator_signer = administrator.device_signer(identity_signer)?;
    let initial = DeviceJoinJournalRecord {
        attempt_id: request.offer.attempt_id,
        progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(request.clone()),
        )),
    };
    let durable = begin_store_journal(db, initial.clone()).await?;
    let (grant, prepared, prepared_progress) = match &*durable.progress {
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::ApprovalPrepared(approval),
        ) => return Ok(approval.clone()),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessGrantPrepared {
                request: durable_request,
                grant,
                prepared,
            },
        ) if durable_request == &request => (
            grant.clone(),
            crate::sync::storage::PreparedExactObject::new(
                prepared.object.clone(),
                prepared.stored_bytes.clone(),
            )?,
            durable.clone(),
        ),
        DeviceJoinRoleProgress::ProviderAdministrator(
            ProviderAdminJoinProgress::AccessRequested(durable_request),
        ) if durable_request == &request => {
            let locator = if provider_admin.provider == request.peer_provider {
                provider_admin.access.clone()
            } else {
                let administrator =
                    access_administrator.ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
                administrator
                    .grant_member_access(
                        &request.offer.member_pubkey,
                        authorization.current_member_provider_email(&request.offer.member_pubkey),
                        &request.peer_provider,
                    )
                    .await?
            };
            let grant_id = ProviderAccessGrantId::from_random_bytes(
                *ObjectHash::digest(db.new_write_id().as_str().as_bytes()).as_bytes(),
            );
            let grant = StoreMemberProviderAccessGrant::signed(
                grant_id,
                request.offer.member_pubkey.clone(),
                request.peer_provider.clone(),
                locator,
                provider_admin.grant_id.clone(),
                provider_admin.administrator.clone(),
                &request.offer.provider,
                &administrator,
                &administrator_signer,
            )
            .map_err(provider_error)?;
            let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
                request.offer.store_root.store_root_hash,
                ProtocolObjectDomain::ProviderAccessGrant,
            );
            let prefix =
                crate::sync::store_commit::provider_access_grant_semantic_prefix(&grant.grant_id);
            let slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await?;
            let prepared =
                storage.prepare_protocol_object(&context, slot, &prefix, grant.to_bytes())?;
            let prepared_progress = DeviceJoinJournalRecord {
                attempt_id: request.offer.attempt_id,
                progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                    ProviderAdminJoinProgress::AccessGrantPrepared {
                        request: request.clone(),
                        grant: grant.clone(),
                        prepared: PreparedDeviceJoinObject::from_prepared(&prepared),
                    },
                )),
            };
            advance_store_journal(db, &initial, prepared_progress.clone()).await?;
            (grant, prepared, prepared_progress)
        }
        _ => return Err(DeviceJoinError::JournalConflict),
    };
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        request.offer.store_root.store_root_hash,
        ProtocolObjectDomain::ProviderAccessGrant,
    );
    let prefix = crate::sync::store_commit::provider_access_grant_semantic_prefix(&grant.grant_id);
    storage.create_protocol_object(&prepared).await?;
    let opened = storage
        .read_protocol_object(&context, prepared.reference(), &prefix)
        .await?;
    if opened != grant.to_bytes() {
        return Err(DeviceJoinError::Provider(
            "provider access grant exact readback differs from its signed bytes".to_string(),
        ));
    }
    let grant_ref =
        StoreMemberProviderAccessGrantRef::from_grant(&grant, prepared.reference().clone());
    let plan = crate::sync::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        coordination,
        &local_device_id,
        identity_signer,
        authorization.merge_chain(),
    )
    .await?;
    let activation = crate::sync::store_outbound::activate_store_operation_commit(
        db,
        storage,
        coordination,
        plan,
        crate::sync::store_outbound::StoreOperationBatch::ProviderAccessGrant(grant_ref.clone()),
    )
    .await?;
    let admission = if provider_admin.provider == request.peer_provider {
        DeviceProviderAdmissionChallenge::SamePrincipal
    } else {
        let exact = administrator_exact.ok_or(DeviceJoinError::ProviderAdministratorRequired)?;
        let challenge_context = crate::sync::provider::CrossPrincipalChallengeContext {
            root: request.offer.store_root.clone(),
            attempt_id: request.offer.attempt_id,
            access_request_hash: request.request_hash(),
            provider_admin_grant: provider_admin.grant_id.clone(),
            owner_registration: request.offer.owner_registration.clone(),
            member_pubkey: request.offer.member_pubkey.clone(),
            administrator_binding: provider_admin.provider.clone(),
            peer_binding: request.peer_provider.clone(),
        };
        let probe_id = crate::sync::provider::ProviderProbeId::from_bytes(
            *ObjectHash::digest(db.new_write_id().as_str().as_bytes()).as_bytes(),
        );
        DeviceProviderAdmissionChallenge::CrossPrincipal(
            crate::sync::provider::prepare_cross_principal_challenge(
                exact,
                db,
                probe_id,
                &request.offer.provider,
                &challenge_context,
                &administrator_signer,
            )
            .await
            .map_err(provider_error)?,
        )
    };
    let approval = DeviceProviderAdmissionApproval::signed(
        request,
        ActivatedStoreMemberProviderAccessGrant {
            grant,
            grant_ref,
            activation,
        },
        admission,
        &administrator,
        &administrator_signer,
    )?;
    advance_store_journal(
        db,
        &prepared_progress,
        DeviceJoinJournalRecord {
            attempt_id: approval.request.offer.attempt_id,
            progress: Box::new(DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::ApprovalPrepared(approval.clone()),
            )),
        },
    )
    .await?;
    Ok(approval)
}

async fn begin_store_journal(
    db: &Database,
    record: DeviceJoinJournalRecord,
) -> Result<DeviceJoinJournalRecord, DeviceJoinError> {
    validate_initial_progress(&record.progress)?;
    let key = store_journal_key(record.attempt_id, record.progress.role_name());
    let value = serde_json::to_string(&record)?;
    db.call(move |connection| {
        connection
            .execute(
                "INSERT OR IGNORE INTO protocol_state (key, value) VALUES (?1, ?2)",
                (&key, &value),
            )
            .map_err(crate::database::DbError::from)?;
        let actual = connection
            .query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [&key],
                |row| row.get::<_, String>(0),
            )
            .map_err(crate::database::DbError::from)?;
        serde_json::from_str(&actual)
            .map_err(|error| crate::database::DbError::Message(error.to_string()))
    })
    .await
    .map_err(database_error)
}

async fn load_store_journal(
    db: &Database,
    attempt_id: DeviceJoinAttemptId,
    role: DeviceJoinRole,
) -> Result<Option<DeviceJoinJournalRecord>, DeviceJoinError> {
    let key = store_journal_key(attempt_id, role.as_str());
    let value = db.get_protocol_state(&key).await.map_err(database_error)?;
    value
        .map(|value| serde_json::from_str(&value).map_err(DeviceJoinError::from))
        .transpose()
}

async fn advance_store_journal(
    db: &Database,
    previous: &DeviceJoinJournalRecord,
    next: DeviceJoinJournalRecord,
) -> Result<(), DeviceJoinError> {
    if previous.attempt_id != next.attempt_id {
        return Err(DeviceJoinError::JournalConflict);
    }
    previous.progress.validate_transition(&next.progress)?;
    let key = store_journal_key(previous.attempt_id, previous.progress.role_name());
    let previous = serde_json::to_string(previous)?;
    let next = serde_json::to_string(&next)?;
    let changed = db
        .call(move |connection| {
            connection
                .execute(
                    "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                    (&next, &key, &previous),
                )
                .map_err(crate::database::DbError::from)
        })
        .await
        .map_err(database_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(DeviceJoinError::JournalConflict)
    }
}

fn store_journal_key(attempt_id: DeviceJoinAttemptId, role: &str) -> String {
    format!("device_join/{}/{role}", attempt_key(attempt_id))
}

fn database_error(error: crate::database::DbError) -> DeviceJoinError {
    DeviceJoinError::Store(error.into_message())
}

fn provider_error(error: impl std::fmt::Display) -> DeviceJoinError {
    DeviceJoinError::Provider(error.to_string())
}

pub fn validate_member_for_join(
    member_pubkey: &str,
    members: &[(String, MemberRole)],
) -> Result<(), DeviceJoinError> {
    if members
        .iter()
        .any(|(pubkey, role)| pubkey == member_pubkey && role.can_write())
    {
        Ok(())
    } else {
        Err(DeviceJoinError::MemberNotEligible)
    }
}

pub fn canonical_cleanup_slots(
    attempt: &DeviceJoinAttempt,
) -> Result<Vec<ObjectSlot>, DeviceJoinError> {
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
        _ => return Err(DeviceJoinError::AttemptMismatch),
    }
    slots.sort();
    require_distinct_slots(&slots)?;
    Ok(slots)
}

fn require_cancelled_outcome(outcome: &DeviceJoinOutcomeRef) -> Result<(), DeviceJoinError> {
    if matches!(outcome, DeviceJoinOutcomeRef::Cancelled { .. }) {
        Ok(())
    } else {
        Err(DeviceJoinError::AttemptMismatch)
    }
}

fn validate_terminals(
    cancellation: &DeviceJoinOutcomeRef,
    administrator: &ProviderAdminJoinTerminal,
    joiner: &JoinerJoinTerminal,
) -> Result<(), DeviceJoinError> {
    let administrator_cancellation = match administrator {
        ProviderAdminJoinTerminal::Completed(completion) => {
            if completion.readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinError::AttemptMismatch);
            }
            None
        }
        ProviderAdminJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        ProviderAdminJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    let joiner_cancellation = match joiner {
        JoinerJoinTerminal::Ready(readiness) => {
            if readiness.proof.attempt != *cancellation.attempt() {
                return Err(DeviceJoinError::AttemptMismatch);
            }
            None
        }
        JoinerJoinTerminal::Cancelled(closure) => Some(&closure.cancellation),
        JoinerJoinTerminal::WriteRevoked(revocation) => Some(&revocation.cancellation),
    };
    if administrator_cancellation.is_some_and(|value| value != cancellation)
        || joiner_cancellation.is_some_and(|value| value != cancellation)
    {
        return Err(DeviceJoinError::AttemptMismatch);
    }
    Ok(())
}

async fn verify_cleanup_terminals(
    db: &Database,
    administrator: &ProviderAdminJoinTerminal,
    joiner: &JoinerJoinTerminal,
) -> Result<(), DeviceJoinError> {
    match administrator {
        ProviderAdminJoinTerminal::Completed(_) => {}
        ProviderAdminJoinTerminal::Cancelled(closure) => {
            let registration = db
                .activated_store_device_registration(closure.administrator_registration.clone())
                .await
                .map_err(database_error)?;
            closure.verify(&registration)?;
        }
        ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
            let registration = db
                .activated_store_device_registration(revocation.executor.clone())
                .await
                .map_err(database_error)?;
            revocation.verify(&registration)?;
        }
    }
    match joiner {
        JoinerJoinTerminal::Ready(_) => {}
        JoinerJoinTerminal::Cancelled(closure) => closure.verify()?,
        JoinerJoinTerminal::WriteRevoked(revocation) => {
            let registration = db
                .activated_store_device_registration(revocation.executor.clone())
                .await
                .map_err(database_error)?;
            revocation.verify(&registration)?;
        }
    }
    Ok(())
}

async fn ensure_exact_slot_absent(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
) -> Result<(), DeviceJoinError> {
    match storage.read_at(slot).await {
        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => Ok(()),
        Ok(_) => {
            storage
                .delete_at(slot)
                .await
                .map_err(|error| DeviceJoinError::Provider(error.to_string()))?;
            match storage.read_at(slot).await {
                Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => Ok(()),
                Ok(_) => Err(DeviceJoinError::CleanupMismatch),
                Err(error) => Err(DeviceJoinError::Provider(error.to_string())),
            }
        }
        Err(error) => Err(DeviceJoinError::Provider(error.to_string())),
    }
}

async fn observe_exact_slot(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
) -> Result<SlotDisposition, DeviceJoinError> {
    match storage.read_at(slot).await {
        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => {
            Ok(SlotDisposition::NeverCreated)
        }
        Ok(bytes) => Ok(SlotDisposition::Created(ExactObjectRef::new(
            slot.clone(),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        ))),
        Err(error) => Err(DeviceJoinError::Provider(error.to_string())),
    }
}

fn withdrawal_matches_locator(
    withdrawal: &ProviderAccessWithdrawal,
    locator: &crate::sync::provider::ProviderAccessLocator,
) -> bool {
    match (withdrawal, locator) {
        (
            ProviderAccessWithdrawal::Direct {
                locator: withdrawn,
                verified_absent: true,
            },
            expected,
        ) => withdrawn == expected,
        (
            ProviderAccessWithdrawal::S3CredentialRotation {
                retired_generation,
                active_generation,
                retired_credential_verified_rejected: true,
            },
            crate::sync::provider::ProviderAccessLocator::S3SharedCredentialGeneration {
                generation,
                ..
            },
        ) => retired_generation == generation && *active_generation == generation.saturating_add(1),
        _ => false,
    }
}

fn owner_adjacent(previous: &OwnerJoinProgress, next: &OwnerJoinProgress) -> bool {
    matches!(
        (previous, next),
        (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::RegistrationRequested(_)
        ) | (
            OwnerJoinProgress::Offered(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AttemptActivated(_)
        ) | (
            OwnerJoinProgress::RegistrationRequested(_),
            OwnerJoinProgress::AbandonmentCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AbandonmentCreateIntent { .. },
            OwnerJoinProgress::Abandoned(_)
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::ProviderReady(_)
        ) | (
            OwnerJoinProgress::AttemptActivated(_),
            OwnerJoinProgress::CancellationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::ProviderReady(_),
            OwnerJoinProgress::AdmissionCompleted(_)
        ) | (
            OwnerJoinProgress::ProviderReady(_),
            OwnerJoinProgress::CancellationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::AdmissionCompleted(_),
            OwnerJoinProgress::ActivationPrepared(_)
        ) | (
            OwnerJoinProgress::AdmissionCompleted(_),
            OwnerJoinProgress::CancellationCreateIntent { .. }
        ) | (
            OwnerJoinProgress::CancellationCreateIntent { .. },
            OwnerJoinProgress::Cancelled(_)
        ) | (
            OwnerJoinProgress::ActivationPrepared(_),
            OwnerJoinProgress::Activated(_)
        ) | (
            OwnerJoinProgress::Cancelled(_),
            OwnerJoinProgress::CleanupReceiptCreateIntent { .. }
        ) | (
            OwnerJoinProgress::CleanupReceiptCreateIntent { .. },
            OwnerJoinProgress::CleanupReceipt(_)
        ) | (
            OwnerJoinProgress::CleanupReceipt(_),
            OwnerJoinProgress::CleanupActivated(_)
        ) | (
            OwnerJoinProgress::CleanupActivated(_),
            OwnerJoinProgress::CancelledComplete(_)
        )
    )
}

fn provider_admin_adjacent(
    previous: &ProviderAdminJoinProgress,
    next: &ProviderAdminJoinProgress,
) -> bool {
    matches!(
        (previous, next),
        (
            ProviderAdminJoinProgress::AccessRequested(_),
            ProviderAdminJoinProgress::AccessGrantPrepared { .. }
        ) | (
            ProviderAdminJoinProgress::AccessGrantPrepared { .. },
            ProviderAdminJoinProgress::ApprovalPrepared(_)
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::AttemptObserved(_)
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::ChallengeCreateIntent(_)
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::ProviderReady(_)
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::ResponseObserved(_)
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::Completed(_)
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::CleanupIntent { .. }
        ) | (
            ProviderAdminJoinProgress::ApprovalPrepared(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::AttemptObserved(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ChallengeCreateIntent(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ProviderReady(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::ResponseObserved(_),
            ProviderAdminJoinProgress::WriteRevoked(_)
        ) | (
            ProviderAdminJoinProgress::CleanupIntent { .. },
            ProviderAdminJoinProgress::Cancelled(_)
        ) | (
            ProviderAdminJoinProgress::CleanupIntent { .. },
            ProviderAdminJoinProgress::WriteRevoked(_)
        )
    )
}

fn joiner_adjacent(previous: &JoinerJoinProgress, next: &JoinerJoinProgress) -> bool {
    matches!(
        (previous, next),
        (
            JoinerJoinProgress::OfferReceived(_),
            JoinerJoinProgress::AccessRequested(_)
        ) | (
            JoinerJoinProgress::AccessRequested(_),
            JoinerJoinProgress::ApprovalReceived(_)
        ) | (
            JoinerJoinProgress::AccessRequested(_),
            JoinerJoinProgress::Abandoned(_)
        ) | (
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::RegistrationPrepared(_)
        ) | (
            JoinerJoinProgress::ApprovalReceived(_),
            JoinerJoinProgress::Abandoned(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::ProviderReady(_)
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::ProviderReady(_),
            JoinerJoinProgress::RegistrationCreateIntent(_)
        ) | (
            JoinerJoinProgress::ProviderReady(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationCreateIntent(_),
            JoinerJoinProgress::RegistrationCreated(_)
        ) | (
            JoinerJoinProgress::RegistrationCreateIntent(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationCreated(_),
            JoinerJoinProgress::AckCreateIntent(_)
        ) | (
            JoinerJoinProgress::RegistrationCreated(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::AckCreateIntent(_),
            JoinerJoinProgress::AckCreated(_)
        ) | (
            JoinerJoinProgress::AckCreateIntent(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::ResponseCreateIntent(_)
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::Ready(_)
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::ResponseCreateIntent(_),
            JoinerJoinProgress::Ready(_)
        ) | (
            JoinerJoinProgress::ResponseCreateIntent(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::ActivationObserved(_)
        ) | (
            JoinerJoinProgress::Ready(_),
            JoinerJoinProgress::CleanupIntent { .. }
        ) | (
            JoinerJoinProgress::RegistrationPrepared(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::ProviderReady(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::RegistrationCreateIntent(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::RegistrationCreated(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::AckCreateIntent(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::AckCreated(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::ResponseCreateIntent(_),
            JoinerJoinProgress::WriteRevoked(_)
        ) | (
            JoinerJoinProgress::CleanupIntent { .. },
            JoinerJoinProgress::Cancelled(_)
        ) | (
            JoinerJoinProgress::ActivationObserved(_),
            JoinerJoinProgress::Activated(_)
        ) | (
            JoinerJoinProgress::Cancelled(_),
            JoinerJoinProgress::CleanupActivated(_)
        ) | (
            JoinerJoinProgress::WriteRevoked(_),
            JoinerJoinProgress::CleanupActivated(_)
        ) | (
            JoinerJoinProgress::CleanupActivated(_),
            JoinerJoinProgress::CancelledComplete(_)
        )
    )
}

fn validate_initial_progress(progress: &DeviceJoinRoleProgress) -> Result<(), DeviceJoinError> {
    if matches!(
        progress,
        DeviceJoinRoleProgress::Owner(OwnerJoinProgress::Offered(_))
            | DeviceJoinRoleProgress::ProviderAdministrator(
                ProviderAdminJoinProgress::AccessRequested(_)
            )
            | DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::OfferReceived(_))
    ) {
        Ok(())
    } else {
        Err(DeviceJoinError::NonAdjacentJournalTransition)
    }
}

fn require_distinct_slots(slots: &[ObjectSlot]) -> Result<(), DeviceJoinError> {
    let unique = slots.iter().collect::<BTreeSet<_>>();
    if unique.len() == slots.len() {
        Ok(())
    } else {
        Err(DeviceJoinError::DuplicateReservedSlot)
    }
}

fn attempt_key(attempt_id: DeviceJoinAttemptId) -> String {
    serde_json::to_value(attempt_id)
        .expect("device join attempt id serialization cannot fail")
        .as_str()
        .expect("device join attempt id serializes as a string")
        .to_string()
}

fn sign<T: Serialize>(signer: &UserKeypair, domain: &[u8], value: &T) -> String {
    let digest = ObjectHash::digest(&domain_json(domain, value));
    hex::encode(signer.sign(digest.as_bytes()))
}

fn verify_signature<T: Serialize>(
    public_key: &str,
    signature: &str,
    domain: &[u8],
    value: &T,
) -> Result<(), DeviceJoinError> {
    let digest = ObjectHash::digest(&domain_json(domain, value));
    if keys::verify_signature_hex(public_key, signature, digest.as_bytes()) {
        Ok(())
    } else {
        Err(DeviceJoinError::InvalidSignature)
    }
}

fn domain_json<T: Serialize>(domain: &[u8], value: &T) -> Vec<u8> {
    let mut bytes = domain.to_vec();
    bytes.extend(serde_json::to_vec(value).expect("closed device join serialization cannot fail"));
    bytes
}

#[derive(Debug, thiserror::Error)]
pub enum DeviceJoinError {
    #[error("device join signature is invalid")]
    InvalidSignature,
    #[error("device join offer does not name one active Store/member/provider authority")]
    OfferMismatch,
    #[error(
        "device provider admission approval differs from its request or activated access grant"
    )]
    ApprovalMismatch,
    #[error("device registration request differs from its offer, approval, or reserved slots")]
    RegistrationRequestMismatch,
    #[error("device join attempt differs from its signed exchange")]
    AttemptMismatch,
    #[error("device join cleanup does not contain the unconditional canonical slot set")]
    CleanupMismatch,
    #[error("device join journal transition is not the declared adjacent transition")]
    NonAdjacentJournalTransition,
    #[error("device join journal has a different durable value for this role and attempt")]
    JournalConflict,
    #[error("pending join payload hash differs from the exact transferred payload")]
    PendingTransferHashMismatch,
    #[error("device join reserved slots are not pairwise distinct")]
    DuplicateReservedSlot,
    #[error("device join requires an existing Member identity")]
    MemberNotEligible,
    #[error("provider operation failed: {0}")]
    Provider(String),
    #[error("Store device join state: {0}")]
    Store(String),
    #[error("device join requires an activated local Store device")]
    ActiveDeviceRequired,
    #[error("device join requires the active local Owner authority")]
    OwnerAuthorityRequired,
    #[error("device join requires the selected effective provider administrator")]
    ProviderAdministratorRequired,
    #[error("device join requires resolved Store membership")]
    MembershipConflict,
    #[error("device join requires the provider's exact-slot adapter")]
    ExactSlotStorageRequired,
    #[error("device join attempt cut does not include its provider-access activation")]
    ApprovalActivationMissing,
    #[error("device join activation is not materialized in the installed Store database")]
    ActivationNotMaterialized,
    #[error(transparent)]
    Object(#[from] crate::sync::store_objects::StoreObjectError),
    #[error(transparent)]
    Registration(#[from] crate::sync::store_registration::StoreRegistrationError),
    #[error(transparent)]
    Pull(#[from] crate::sync::store_pull::StorePullError),
    #[error(transparent)]
    Outbound(#[from] crate::sync::store_outbound::StoreOutboundError),
    #[error(transparent)]
    Protocol(#[from] crate::sync::store_commit::StoreProtocolError),
    #[error(transparent)]
    Storage(#[from] crate::sync::storage::StorageError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}
