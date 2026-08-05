use super::*;

impl LocalStoreWriter {
    pub(crate) fn sign_device_join_offer(
        &self,
        attempt_id: crate::protocol::store_commit::DeviceJoinAttemptId,
        member_pubkey: String,
        root: crate::protocol::store_commit::StoreRootRef,
        provider: crate::protocol::objects::StoreProviderBinding,
        attempt_slot: crate::protocol::objects::ObjectSlot,
        outcome_slot: crate::protocol::objects::ObjectSlot,
        owner_grant: crate::protocol::membership::MembershipGrantId,
        provider_admin: crate::protocol::provider::ProviderAdminGrantRecord,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer::signed(
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
        .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_join_offer(
        &self,
        offer: &crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        offer
            .verify(self.registration.value())
            .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn sign_device_join_abandonment(
        &self,
        offer: &crate::protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::DeviceJoinAbandonmentObject,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::protocol::store_commit::device_join_exchange::DeviceJoinAbandonmentObject::signed(
            offer,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_join_abandonment(
        &self,
        reference: &crate::protocol::store_commit::DeviceJoinAbandonmentRef,
        value: &crate::protocol::store_commit::device_join_exchange::DeviceJoinAbandonmentObject,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        reference
            .verify(value, self.registration.value())
            .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_admission_approval_as_owner(
        &self,
        approval: &crate::protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        root: &crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreProtocolRoot,
        >,
        administrator: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        approval
            .verify(root, self.registration.value(), administrator)
            .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_attempt(
        &self,
        store_root: crate::protocol::store_commit::StoreRootRef,
        attempt_id: crate::protocol::store_commit::DeviceJoinAttemptId,
        attempt_slot: crate::protocol::objects::ObjectSlot,
        expected_registration: crate::protocol::store_commit::StoreDeviceRegistration,
        registration_slot: crate::protocol::objects::ObjectSlot,
        outcome_slot: crate::protocol::objects::ObjectSlot,
        bootstrap_cut: crate::protocol::store_commit::StoreHistoryCut,
        membership: crate::protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
        provider_approval: crate::protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        provider_response: crate::protocol::store_commit::device_join_exchange::DeviceProviderResponseReservation,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<
        crate::protocol::store_commit::DeviceJoinAttempt,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::DeviceJoinAttempt::signed(
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
        history: &mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'_>,
        reference: &crate::protocol::store_commit::DeviceJoinAttemptRef,
    ) -> Result<
        crate::protocol::objects::VerifiedObject<crate::protocol::store_commit::DeviceJoinAttempt>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        history
            .load_verified_device_join_attempt(reference, self.registration.value())
            .await
    }

    pub(crate) fn sign_device_join_outcome(
        &self,
        attempt: crate::protocol::store_commit::DeviceJoinAttemptRef,
        body: crate::protocol::store_commit::DeviceJoinDisposition,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<
        crate::protocol::store_commit::DeviceJoinOutcome,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::DeviceJoinOutcome::signed(
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
        history: &crate::sync::store::owner::verified_history::MergeHistoryVerifier<'_>,
        reference: &crate::protocol::store_commit::DeviceJoinOutcomeRef,
    ) -> Result<
        crate::protocol::objects::VerifiedObject<crate::protocol::store_commit::DeviceJoinOutcome>,
        crate::protocol::objects::StoreObjectError,
    > {
        history
            .load_device_join_outcome(reference, self.registration.value())
            .await
    }

    pub(crate) fn is_effective_provider_administrator(
        &self,
        record: &crate::protocol::provider::ProviderAdminGrantRecord,
    ) -> bool {
        record.administrator == *self.registration.reference()
            && record.provider == self.registration.value().provider
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_cleanup_receipt(
        &self,
        attempt: &crate::protocol::store_commit::DeviceJoinAttempt,
        cancellation: crate::protocol::store_commit::DeviceJoinOutcomeRef,
        administrator_terminal: crate::protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
        joiner_terminal: crate::protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
        deleted_slots: Vec<crate::protocol::objects::ObjectSlot>,
        membership: crate::protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceiptObject,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceiptObject::signed(
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
        .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_join_cleanup_receipt(
        &self,
        reference: &crate::protocol::store_commit::DeviceJoinCleanupReceiptRef,
        receipt: &crate::protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceiptObject,
        attempt: &crate::protocol::store_commit::DeviceJoinAttempt,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        receipt.verify(attempt, self.registration.value())?;
        reference
            .verify(receipt, self.registration.value())
            .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_device_admission_approval_as_administrator(
        &self,
        approval: &crate::protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        root: &crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreProtocolRoot,
        >,
        owner: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        approval
            .verify(root, owner, self.registration.value())
            .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn sign_device_admission_approval(
        &self,
        request: crate::protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
        access_grant: crate::protocol::provider::ActivatedStoreMemberProviderAccessGrant,
        admission: crate::protocol::store_commit::device_join_exchange::DeviceProviderAdmissionChallenge,
        root: &crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreProtocolRoot,
        >,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval::signed(
            request,
            access_grant,
            admission,
            root,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    pub(crate) fn verify_cross_principal_challenge(
        &self,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        store: &crate::protocol::objects::StoreProviderBinding,
    ) -> Result<(), crate::protocol::provider::ProviderProbeError> {
        challenge.verify(
            context,
            store,
            &self.registration.value().device_signing_pubkey,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_provider_access_grant(
        &self,
        grant_id: crate::protocol::provider::ProviderAccessGrantId,
        member_pubkey: String,
        peer_provider: crate::protocol::objects::ProviderDeviceBinding,
        locator: crate::protocol::provider::ProviderAccessLocator,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
        provider_admin_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        store_provider: &crate::protocol::objects::StoreProviderBinding,
    ) -> Result<
        crate::protocol::provider::StoreMemberProviderAccessGrant,
        crate::protocol::provider::ProviderProbeError,
    > {
        crate::protocol::provider::StoreMemberProviderAccessGrant::signed(
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
        cancellation: crate::protocol::store_commit::DeviceJoinOutcomeRef,
        administrator_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        challenge: crate::protocol::store_commit::device_join_exchange::ProviderChallengeDisposition,
        prior_state_hash: crate::protocol::store_commit::ObjectHash,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::ProviderAdminJoinClosure,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::protocol::store_commit::device_join_exchange::ProviderAdminJoinClosure::signed(
            cancellation,
            administrator_registration,
            challenge,
            prior_state_hash,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_write_revocation(
        &self,
        cancellation: crate::protocol::store_commit::DeviceJoinOutcomeRef,
        producer: crate::protocol::store_commit::device_join_exchange::DeviceJoinProducer,
        authority: crate::protocol::store_commit::device_join_exchange::ProviderWriteAuthorityRef,
        protected_slots: Vec<crate::protocol::objects::ObjectSlot>,
        withdrawal: crate::protocol::provider::ProviderAccessWithdrawal,
        executor_grant: crate::protocol::provider::ProviderAdminGrantId,
        executor_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::protocol::store_commit::device_join_exchange::DeviceJoinProducerWriteRevocation,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::protocol::store_commit::device_join_exchange::DeviceJoinProducerWriteRevocation::signed(
            cancellation,
            producer,
            authority,
            protected_slots,
            withdrawal,
            executor_grant,
            executor_registration,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::owner::device_join::DeviceJoinError::from)
    }
}
