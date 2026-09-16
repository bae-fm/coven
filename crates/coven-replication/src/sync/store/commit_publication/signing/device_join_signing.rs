use super::*;

impl LocalStoreWriter {
    pub(crate) fn sign_device_join_offer(
        &self,
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
        member_pubkey: String,
        root: coven_protocol::store_commit::StoreRootRef,
        provider: coven_protocol::objects::StoreProviderBinding,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer::signed(
            attempt_id,
            member_pubkey,
            root,
            provider,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_join_offer(
        &self,
        offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<(), crate::sync::store::device_join::DeviceJoinError> {
        offer
            .verify(self.registration.value())
            .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    pub(crate) fn sign_device_join_abandonment(
        &self,
        offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonmentObject,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonmentObject::signed(
            offer,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_join_abandonment(
        &self,
        reference: &coven_protocol::store_commit::DeviceJoinAbandonmentRef,
        value: &coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonmentObject,
    ) -> Result<(), crate::sync::store::device_join::DeviceJoinError> {
        reference
            .verify(value, self.registration.value())
            .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    /// Check an approval this device signed while admitting a join. The offer's
    /// owner and the approval's signer are one registration, so the local one
    /// answers both.
    pub(crate) fn verify_own_device_admission_approval(
        &self,
        approval: &coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        root: &coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreProtocolRoot,
        >,
    ) -> Result<(), crate::sync::store::device_join::DeviceJoinError> {
        approval
            .verify(root, self.registration.value())
            .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    pub(crate) fn sign_device_admission_approval(
        &self,
        request: coven_protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
        admission: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission,
        root: &coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreProtocolRoot,
        >,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval::signed(
            request,
            admission,
            root,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_cross_principal_challenge(
        &self,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        store: &coven_protocol::objects::StoreProviderBinding,
    ) -> Result<(), coven_protocol::provider::ProviderProbeError> {
        challenge.verify(
            context,
            store,
            &self.registration.value().device_signing_pubkey,
        )
    }
}
