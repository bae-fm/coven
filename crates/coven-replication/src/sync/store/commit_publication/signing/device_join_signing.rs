use super::*;

impl LocalStoreWriter {
    pub(crate) fn sign_device_join_offer(
        &self,
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
        member_pubkey: String,
        root: coven_protocol::store_commit::StoreRootRef,
        provider: coven_protocol::objects::StoreProviderBinding,
        attempt_slot: coven_protocol::objects::ObjectSlot,
        outcome_slot: coven_protocol::objects::ObjectSlot,
        owner_grant: coven_protocol::membership::MembershipGrantId,
        provider_admin: coven_protocol::provider::ProviderAdminGrantRecord,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer::signed(
            attempt_id,
            member_pubkey,
            root,
            provider,
            attempt_slot,
            outcome_slot,
            self.registration.reference().clone(),
            owner_grant,
            provider_admin,
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_attempt(
        &self,
        store_root: coven_protocol::store_commit::StoreRootRef,
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
        attempt_slot: coven_protocol::objects::ObjectSlot,
        expected_registration: coven_protocol::store_commit::StoreDeviceRegistration,
        registration_slot: coven_protocol::objects::ObjectSlot,
        outcome_slot: coven_protocol::objects::ObjectSlot,
        bootstrap_cut: coven_protocol::store_commit::StoreHistoryCut,
        membership: coven_protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
        provider_approval: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        provider_response: coven_protocol::store_commit::device_join_exchange::DeviceProviderResponseReservation,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<
        coven_protocol::store_commit::DeviceJoinAttempt,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::DeviceJoinAttempt::signed(
            store_root,
            attempt_id,
            attempt_slot,
            expected_registration,
            registration_slot,
            outcome_slot,
            bootstrap_cut,
            membership,
            provider_admin_grant,
            provider_approval,
            provider_response,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) async fn load_verified_device_join_attempt(
        &self,
        history: &mut crate::sync::store::device_join::history::DeviceJoinHistory<'_, '_>,
        reference: &coven_protocol::store_commit::DeviceJoinAttemptRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<coven_protocol::store_commit::DeviceJoinAttempt>,
        crate::sync::store::pull::StorePullError,
    > {
        history
            .load_verified_attempt(reference, self.registration.value())
            .await
    }

    pub(crate) fn sign_device_join_outcome(
        &self,
        attempt: coven_protocol::store_commit::DeviceJoinAttemptRef,
        body: coven_protocol::store_commit::DeviceJoinDisposition,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<
        coven_protocol::store_commit::DeviceJoinOutcome,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::DeviceJoinOutcome::signed(
            attempt,
            body,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) async fn load_own_device_join_outcome(
        &self,
        history: &crate::sync::store::device_join::history::DeviceJoinHistory<'_, '_>,
        reference: &coven_protocol::store_commit::DeviceJoinOutcomeRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<coven_protocol::store_commit::DeviceJoinOutcome>,
        coven_protocol::objects::StoreObjectError,
    > {
        history
            .load_outcome(reference, self.registration.value())
            .await
    }

    pub(crate) fn is_effective_provider_administrator(
        &self,
        record: &coven_protocol::provider::ProviderAdminGrantRecord,
    ) -> bool {
        record.administrator == *self.registration.reference()
            && record.provider == self.registration.value().provider
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_cleanup_receipt(
        &self,
        attempt: &coven_protocol::store_commit::DeviceJoinAttempt,
        cancellation: coven_protocol::store_commit::DeviceJoinOutcomeRef,
        administrator_terminal: coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
        joiner_terminal: coven_protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
        deleted_slots: Vec<coven_protocol::objects::ObjectSlot>,
        membership: coven_protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceiptObject,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceiptObject::signed(
            attempt,
            cancellation,
            administrator_terminal,
            joiner_terminal,
            deleted_slots,
            membership,
            provider_admin_grant,
            self.registration.reference().clone(),
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_join_cleanup_receipt(
        &self,
        reference: &coven_protocol::store_commit::DeviceJoinCleanupReceiptRef,
        receipt: &coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceiptObject,
        attempt: &coven_protocol::store_commit::DeviceJoinAttempt,
    ) -> Result<(), crate::sync::store::device_join::DeviceJoinError> {
        receipt.verify(attempt, self.registration.value())?;
        reference
            .verify(receipt, self.registration.value())
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_provider_access_grant(
        &self,
        grant_id: coven_protocol::provider::ProviderAccessGrantId,
        member_pubkey: String,
        peer_provider: coven_protocol::objects::ProviderDeviceBinding,
        locator: coven_protocol::provider::ProviderAccessLocator,
        provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
        provider_admin_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        store_provider: &coven_protocol::objects::StoreProviderBinding,
    ) -> Result<
        coven_protocol::provider::StoreMemberProviderAccessGrant,
        coven_protocol::provider::ProviderProbeError,
    > {
        coven_protocol::provider::StoreMemberProviderAccessGrant::signed(
            grant_id,
            member_pubkey,
            peer_provider,
            locator,
            provider_admin_grant,
            provider_admin_registration,
            store_provider,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) fn sign_provider_join_closure(
        &self,
        cancellation: coven_protocol::store_commit::DeviceJoinOutcomeRef,
        administrator_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        challenge: coven_protocol::store_commit::device_join_exchange::ProviderChallengeDisposition,
        prior_state_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinClosure,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinClosure::signed(
            cancellation,
            administrator_registration,
            challenge,
            prior_state_hash,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_write_revocation(
        &self,
        cancellation: coven_protocol::store_commit::DeviceJoinOutcomeRef,
        authority: coven_protocol::provider::StoreMemberProviderAccessGrantRef,
        protected_slots: Vec<coven_protocol::objects::ObjectSlot>,
        withdrawal: coven_protocol::provider::ProviderAccessWithdrawal,
        executor_grant: coven_protocol::provider::ProviderAdminGrantId,
        executor_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinProducerWriteRevocation,
        crate::sync::store::device_join::DeviceJoinError,
    > {
        coven_protocol::store_commit::device_join_exchange::DeviceJoinProducerWriteRevocation::signed(
            cancellation,
            authority,
            protected_slots,
            withdrawal,
            executor_grant,
            executor_registration,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::device_join::DeviceJoinError::from)
    }
}
