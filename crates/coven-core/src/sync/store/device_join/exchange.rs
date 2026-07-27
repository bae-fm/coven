use super::cleanup::{require_cancelled_outcome, validate_terminals};
use super::journal::require_distinct_slots;
use super::*;

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

    pub(crate) fn offer_hash(&self) -> ObjectHash {
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
    pub(crate) fn signed(
        request: DeviceProviderAccessRequest,
        access_grant: ActivatedStoreMemberProviderAccessGrant,
        admission: DeviceProviderAdmissionChallenge,
        store_root: &crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
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
        value.validate_shape(store_root, administrator)?;
        value.sign_with(administrator_device_signer);
        Ok(value)
    }

    #[cfg(test)]
    pub(crate) fn signed_without_shape_validation_for_test(
        request: DeviceProviderAccessRequest,
        access_grant: ActivatedStoreMemberProviderAccessGrant,
        admission: DeviceProviderAdmissionChallenge,
        administrator_device_signer: &UserKeypair,
    ) -> Self {
        let mut value = Self {
            request: Box::new(request),
            access_grant,
            admission,
            signature: String::new(),
        };
        value.sign_with(administrator_device_signer);
        value
    }

    fn sign_with(&mut self, administrator_device_signer: &UserKeypair) {
        self.signature = sign(
            administrator_device_signer,
            APPROVAL_DOMAIN,
            &self.signed_fields(),
        );
    }

    pub(in crate::sync::store) fn verify(
        &self,
        store_root: &crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
        owner: &StoreDeviceRegistration,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
        self.request.verify(owner)?;
        self.validate_shape(store_root, administrator)?;
        verify_signature(
            &administrator.device_signing_pubkey,
            &self.signature,
            APPROVAL_DOMAIN,
            &self.signed_fields(),
        )
    }

    fn validate_shape(
        &self,
        store_root: &crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), DeviceJoinError> {
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
    pub(in crate::sync::store) fn verify(
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
pub(in crate::sync::store) struct DeviceJoinAbandonmentObject {
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
    pub(in crate::sync::store) fn signed(
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

    pub(in crate::sync::store) fn abandonment_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(ABANDONMENT_DOMAIN, &self.signed_fields()))
    }

    pub(in crate::sync::store) fn to_bytes(&self) -> Vec<u8> {
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
    pub(in crate::sync::store) fn verify(
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
pub(in crate::sync::store) struct DeviceJoinCleanupReceiptObject {
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
    pub(in crate::sync::store) fn signed(
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

    pub(in crate::sync::store) fn receipt_hash(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(CLEANUP_RECEIPT_DOMAIN, &self.signed_fields()))
    }

    pub(in crate::sync::store) fn verify(
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

    pub(in crate::sync::store) fn to_bytes(&self) -> Vec<u8> {
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
