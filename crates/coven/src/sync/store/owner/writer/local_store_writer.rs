use super::StoreKeyrings;
use crate::keys::UserKeypair;
use std::sync::Arc;

pub(super) struct StoreOperationSigningContext {
    pub(super) root: crate::protocol::store_commit::StoreRootRef,
    pub(super) coord: crate::protocol::store_commit::StoreCommitCoord,
    pub(super) order: crate::protocol::store_commit::StoreCommitOrder,
    pub(super) membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
    pub(super) device_state: crate::protocol::store_commit::StoreDeviceStateRef,
    pub(super) membership_authority:
        crate::protocol::store_commit::StoreOperationMembershipAuthority,
}

pub(crate) struct LocalStoreWriter {
    identity: UserKeypair,
    registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
    device_signer: UserKeypair,
}

impl LocalStoreWriter {
    pub(crate) fn from_verified_parts(
        identity: UserKeypair,
        registration: crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> Self {
        Self {
            identity,
            registration,
            device_signer,
        }
    }

    pub(crate) fn author_pubkey(&self) -> String {
        crate::keys::public_key_hex(&self.identity)
    }

    pub(crate) fn circle_device_id(&self) -> String {
        self.registration.value().device_id.to_string()
    }

    pub(crate) fn circle_grant_authorized_stream_id(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        owner_grant: &crate::protocol::membership::MembershipGrantId,
        domain: crate::protocol::store_commit::StreamAnchorDomain,
    ) -> crate::protocol::membership::AuthorStreamId {
        crate::protocol::store_commit::StreamActivation::grant_authorized_stream_id(
            root_hash,
            self.registration.reference(),
            owner_grant,
            domain,
        )
    }

    pub(crate) fn circle_grant_authorized_activation(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        owner_grant: crate::protocol::membership::MembershipGrantId,
        anchor: crate::protocol::store_commit::GrantStreamAnchor,
    ) -> crate::protocol::store_commit::StreamActivation {
        crate::protocol::store_commit::StreamActivation::grant_authorized(
            root_hash,
            self.registration.reference().clone(),
            owner_grant,
            anchor,
        )
    }

    pub(crate) fn verify_circle_roster_head(
        &self,
        head: &crate::protocol::circle::CircleRosterHead,
    ) -> bool {
        head.verify_for_registration(self.registration.value())
    }

    pub(crate) fn verify_circle_metadata_head(
        &self,
        head: &crate::protocol::circle::CircleMetadataHead,
    ) -> bool {
        head.verify_for_registration(self.registration.value())
    }

    pub(crate) fn sign_circle_roster_head(
        &self,
        entry: &crate::protocol::circle::CircleRosterEntry,
        tip: crate::protocol::objects::ExactObjectRef,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> crate::protocol::circle::CircleRosterHead {
        crate::protocol::circle::CircleRosterHead::signed(
            entry,
            tip,
            successor,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_circle_metadata_head(
        &self,
        metadata: &crate::protocol::circle::CircleMetadata,
        tip: crate::protocol::objects::ExactObjectRef,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> crate::protocol::circle::CircleMetadataHead {
        crate::protocol::circle::CircleMetadataHead::signed(
            metadata,
            tip,
            successor,
            &self.device_signer,
        )
    }

    pub(crate) fn sign_circle_control_head(
        &self,
        control: &crate::protocol::circle::CircleControl,
        entry: crate::protocol::objects::ExactObjectRef,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> crate::protocol::circle::CircleControlHead {
        crate::protocol::circle::CircleControlHead::signed(
            control,
            entry,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize_circle_epoch_close(
        &self,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        metadata_stamp: &str,
        store_membership: crate::protocol::circle_control::StoreMembershipStateRef,
        membership_authority: crate::protocol::membership::MembershipGrantCreationAuthority,
        store_members: Vec<(String, crate::protocol::membership::MemberRole)>,
        close_control: &crate::protocol::circle::PreparedCircleControl,
        current_roster: &crate::protocol::circle::CircleMaterializedRoster,
        current_roster_chain: crate::protocol::circle::CircleRosterChain,
        current_metadata: &crate::protocol::circle::CircleMetadata,
        keyring: &str,
        intent: crate::protocol::circle::CircleEpochCloseIntent,
        responses: Vec<crate::protocol::circle::CircleEpochCloseSettlement>,
        ids: &dyn crate::id_provider::IdProvider,
    ) -> Result<
        crate::protocol::circle::CircleTransitionDraft,
        crate::protocol::circle::CircleTransitionError,
    > {
        crate::protocol::circle::CircleTransitionDraft::finalize_epoch_close(
            candidate_family,
            &self.circle_device_id(),
            self.registration.reference(),
            metadata_stamp,
            store_membership,
            membership_authority,
            store_members,
            close_control,
            current_roster,
            current_roster_chain,
            current_metadata,
            keyring,
            intent,
            responses,
            ids,
            self,
        )
    }

    pub(super) fn device_id(&self) -> &crate::protocol::store_commit::StoreDeviceId {
        &self.registration.value().device_id
    }

    pub(crate) fn is_authored_by_registration(
        &self,
        registration: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> bool {
        self.registration.reference() == registration
    }

    pub(super) fn matches_author(
        &self,
        registration: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        author_pubkey: &str,
    ) -> bool {
        self.registration.reference() == registration
            && self.registration.value().author_pubkey == author_pubkey
    }

    pub(super) async fn authorize_retained_outbound(
        &self,
        history: &super::AuthorizedStoreHistory<'_>,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<super::verified_history::MergeOutboundAuthorization, super::pull::StorePullError>
    {
        history
            .authorize_retained_outbound(order, membership_heads, self.registration.reference())
            .await
    }

    pub(super) async fn authorize_retained_conflict_resolution(
        &self,
        history: &super::AuthorizedStoreHistory<'_>,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<super::history::MergeConflictResolutionAuthorization, super::pull::StorePullError>
    {
        history
            .authorize_retained_conflict_resolution(
                order,
                membership_heads,
                self.registration.reference(),
                &self.registration.value().author_pubkey,
            )
            .await
    }

    pub(super) async fn prepare_merge_snapshot_history_summary(
        &self,
        history: &super::AuthorizedStoreHistory<'_>,
        coverage: &crate::protocol::store_commit::CommitFrontier,
        membership: &crate::protocol::membership::MembershipChain,
        state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
    ) -> Result<
        crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        super::pull::StorePullError,
    > {
        history
            .prepare_merge_snapshot_history_summary(
                coverage,
                membership,
                state,
                self.registration.reference(),
                self.registration.value(),
            )
            .await
    }

    pub(super) async fn retain_acknowledgement(
        &self,
        history: &super::AuthorizedStoreHistory<'_>,
        activating_commit: &crate::protocol::store_commit::StoreBatchCommitRef,
        activating_commit_value: &crate::protocol::store_commit::StoreBatchCommit,
        reference: crate::protocol::store_commit::StoreAckRef,
        value: crate::protocol::store_commit::StoreAck,
    ) -> Result<
        crate::protocol::store_commit::RetainedVerifiedActivatedAck,
        super::pull::StorePullError,
    > {
        history
            .retain_acknowledgement(
                activating_commit,
                activating_commit_value,
                self.registration.value(),
                reference,
                value,
            )
            .await
    }

    pub(super) fn announcement_stream_id(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
    ) -> crate::protocol::membership::AuthorStreamId {
        crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
            store_root_hash,
            self.registration.reference(),
            crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        )
    }

    pub(super) fn grant_authorized_stream_id(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        grant: &crate::protocol::membership::MembershipGrantId,
        domain: crate::protocol::store_commit::StreamAnchorDomain,
    ) -> crate::protocol::membership::AuthorStreamId {
        crate::protocol::store_commit::StreamActivation::grant_authorized_stream_id(
            store_root_hash,
            self.registration.reference(),
            grant,
            domain,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_conflict_resolution(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        selection: crate::protocol::membership::MembershipConflictSelection,
        replacement_grant: crate::protocol::membership::MembershipGrantId,
        membership: crate::protocol::store_commit::GrantStreamAnchor,
        recovery: crate::protocol::store_commit::GrantStreamAnchor,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
    ) -> Result<
        crate::protocol::membership::StoreMembershipConflictResolution,
        crate::sync::store::membership::InviteError,
    > {
        let acceptance = crate::protocol::store_commit::OwnerConflictResolutionAcceptance::signed(
            store_root_hash,
            replacement_grant,
            self.registration.reference().clone(),
            membership.clone(),
            recovery,
            device_state,
            self.registration.value(),
            &self.identity,
        )
        .map_err(|error| {
            crate::sync::store::membership::InviteError::InvalidDurableMutation(error.to_string())
        })?;
        chain
            .signed_conflict_resolution(
                store_root_hash,
                selection,
                membership,
                acceptance,
                &self.identity,
            )
            .map_err(crate::sync::store::membership::InviteError::from)
    }

    pub(super) fn sign_conflict_resolution_activation(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        stream_id: crate::protocol::membership::AuthorStreamId,
        reference: crate::protocol::membership::StoreMembershipConflictResolutionRef,
        resolution: &crate::protocol::membership::StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<
        crate::protocol::membership::MembershipEntry,
        crate::protocol::membership::MembershipError,
    > {
        chain.signed_resolution_activation_in_stream(
            store_root_hash,
            &self.identity,
            stream_id,
            reference,
            resolution,
            created_at,
        )
    }

    pub(crate) fn is_current_owner(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> bool {
        membership.is_owner_now(&self.registration.value().author_pubkey)
    }

    pub(crate) fn provider_administrator_grants(
        &self,
        state: &crate::protocol::provider::ProviderAdminState,
    ) -> std::collections::BTreeMap<
        crate::protocol::provider::ProviderAdminGrantId,
        crate::protocol::provider::ProviderAdminGrantRecord,
    > {
        state
            .records()
            .iter()
            .filter(|(grant_id, record)| {
                record.administrator == *self.registration.reference()
                    && state.authorizes(grant_id, &record.administrator)
            })
            .map(|(grant_id, record)| (grant_id.clone(), record.clone()))
            .collect()
    }

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
        crate::sync::store::owner::device_join::DeviceJoinOffer,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::sync::store::owner::device_join::DeviceJoinOffer::signed(
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
    }

    pub(crate) fn verify_device_join_offer(
        &self,
        offer: &crate::sync::store::owner::device_join::DeviceJoinOffer,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        offer.verify(self.registration.value())
    }

    pub(crate) fn sign_device_join_abandonment(
        &self,
        offer: &crate::sync::store::owner::device_join::DeviceJoinOffer,
    ) -> Result<
        crate::sync::store::owner::device_join::DeviceJoinAbandonmentObject,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::sync::store::owner::device_join::DeviceJoinAbandonmentObject::signed(
            offer,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) fn verify_device_join_abandonment(
        &self,
        reference: &crate::protocol::store_commit::DeviceJoinAbandonmentRef,
        value: &crate::sync::store::owner::device_join::DeviceJoinAbandonmentObject,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        reference.verify(value, self.registration.value())
    }

    pub(crate) fn verify_device_admission_approval_as_owner(
        &self,
        approval: &crate::sync::store::owner::device_join::DeviceProviderAdmissionApproval,
        root: &crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreProtocolRoot,
        >,
        administrator: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        approval.verify(root, self.registration.value(), administrator)
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
        provider_approval: crate::sync::store::owner::device_join::DeviceProviderAdmissionApproval,
        provider_response: crate::sync::store::owner::device_join::DeviceProviderResponseReservation,
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
        history: &mut crate::sync::store::owner::device_join::history::DeviceJoinHistory<'_, '_>,
        reference: &crate::protocol::store_commit::DeviceJoinAttemptRef,
    ) -> Result<
        crate::protocol::objects::VerifiedObject<crate::protocol::store_commit::DeviceJoinAttempt>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        history
            .load_verified_attempt(reference, self.registration.value())
            .await
    }

    pub(crate) fn sign_device_join_outcome(
        &self,
        attempt: crate::protocol::store_commit::DeviceJoinAttemptRef,
        body: crate::protocol::store_commit::DeviceJoinOutcomeBody,
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
        history: &crate::sync::store::owner::device_join::history::DeviceJoinHistory<'_, '_>,
        reference: &crate::protocol::store_commit::DeviceJoinOutcomeRef,
    ) -> Result<
        crate::protocol::objects::VerifiedObject<crate::protocol::store_commit::DeviceJoinOutcome>,
        crate::protocol::objects::StoreObjectError,
    > {
        history
            .load_outcome(reference, self.registration.value())
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
        administrator_terminal: crate::sync::store::owner::device_join::ProviderAdminJoinTerminal,
        joiner_terminal: crate::sync::store::owner::device_join::JoinerJoinTerminal,
        deleted_slots: Vec<crate::protocol::objects::ObjectSlot>,
        membership: crate::protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    ) -> Result<
        crate::sync::store::owner::device_join::DeviceJoinCleanupReceiptObject,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::sync::store::owner::device_join::DeviceJoinCleanupReceiptObject::signed(
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
    }

    pub(crate) fn verify_device_join_cleanup_receipt(
        &self,
        reference: &crate::protocol::store_commit::DeviceJoinCleanupReceiptRef,
        receipt: &crate::sync::store::owner::device_join::DeviceJoinCleanupReceiptObject,
        attempt: &crate::protocol::store_commit::DeviceJoinAttempt,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        receipt.verify(attempt, self.registration.value())?;
        reference.verify(receipt, self.registration.value())
    }

    pub(crate) fn sign_circle_epoch_close_exclusion(
        &self,
        control: &crate::protocol::circle::PreparedCircleControl,
        excluded: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::protocol::circle::CircleEpochCloseExclusion,
        crate::protocol::circle::CircleTransitionError,
    > {
        crate::protocol::circle::CircleEpochCloseExclusion::signed(
            control,
            excluded,
            &self.identity,
        )
    }

    pub(crate) fn verify_device_admission_approval_as_administrator(
        &self,
        approval: &crate::sync::store::owner::device_join::DeviceProviderAdmissionApproval,
        root: &crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreProtocolRoot,
        >,
        owner: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), crate::sync::store::owner::device_join::DeviceJoinError> {
        approval.verify(root, owner, self.registration.value())
    }

    pub(crate) fn sign_device_admission_approval(
        &self,
        request: crate::sync::store::owner::device_join::DeviceProviderAccessRequest,
        access_grant: crate::protocol::provider::ActivatedStoreMemberProviderAccessGrant,
        admission: crate::sync::store::owner::device_join::DeviceProviderAdmissionChallenge,
        root: &crate::protocol::objects::VerifiedObject<
            crate::protocol::store_commit::StoreProtocolRoot,
        >,
    ) -> Result<
        crate::sync::store::owner::device_join::DeviceProviderAdmissionApproval,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::sync::store::owner::device_join::DeviceProviderAdmissionApproval::signed(
            request,
            access_grant,
            admission,
            root,
            self.registration.value(),
            &self.device_signer,
        )
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
        challenge: crate::sync::store::owner::device_join::ProviderChallengeDisposition,
        prior_state_hash: crate::protocol::store_commit::ObjectHash,
    ) -> Result<
        crate::sync::store::owner::device_join::ProviderAdminJoinClosure,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::sync::store::owner::device_join::ProviderAdminJoinClosure::signed(
            cancellation,
            administrator_registration,
            challenge,
            prior_state_hash,
            self.registration.value(),
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_join_write_revocation(
        &self,
        cancellation: crate::protocol::store_commit::DeviceJoinOutcomeRef,
        producer: crate::sync::store::owner::device_join::DeviceJoinProducer,
        authority: crate::sync::store::owner::device_join::ProviderWriteAuthorityRef,
        protected_slots: Vec<crate::protocol::objects::ObjectSlot>,
        withdrawal: crate::protocol::provider::ProviderAccessWithdrawal,
        executor_grant: crate::protocol::provider::ProviderAdminGrantId,
        executor_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::sync::store::owner::device_join::DeviceJoinProducerWriteRevocation,
        crate::sync::store::owner::device_join::DeviceJoinError,
    > {
        crate::sync::store::owner::device_join::DeviceJoinProducerWriteRevocation::signed(
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
    }

    pub(crate) async fn load_circle_activations(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        verified: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<
        crate::sync::store::circle_controls::VerifiedCircleActivations,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        history
            .activations()
            .load(verified, &self.identity, routing_key)
            .await
    }

    pub(crate) fn circle_ack_semantic_prefix(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        sequence: u64,
    ) -> String {
        crate::protocol::store_commit::circle_ack_slot_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            sequence,
        )
    }

    pub(crate) fn circle_ack_first_slot(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<crate::protocol::objects::ObjectSlot, crate::protocol::objects::StorageError> {
        crate::protocol::objects::ObjectSlot::logical(format!(
            "{}.json",
            self.circle_ack_semantic_prefix(circle_id, 1)
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_acknowledgement(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        circle_id: crate::protocol::circle::CircleId,
        sequence: u64,
        frontier: crate::protocol::store_commit::CommitFrontier,
        control: crate::protocol::circle::CircleControlCoord,
        epoch_id: crate::protocol::circle::CircleEpochId,
        key_fingerprint: crate::encryption::KeyFingerprint,
        seeded_from: Option<crate::protocol::circle::CircleBootstrapCoverageRef>,
        sync_time: String,
        predecessor: Option<crate::protocol::objects::ExactObjectRef>,
        next_slot: crate::protocol::objects::ObjectSlot,
    ) -> Result<
        crate::protocol::store_commit::CircleAck,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        let activation = crate::protocol::store_commit::StreamActivation::device_authorized(
            root_hash,
            self.registration.reference().clone(),
            crate::protocol::store_commit::DeviceStreamAnchor::CircleAcknowledgements {
                circle_id,
                first_slot: self.circle_ack_first_slot(circle_id).map_err(|error| {
                    crate::protocol::store_commit::StoreProtocolError::Malformed(error.to_string())
                })?,
            },
        )
        .activation_id();
        crate::protocol::store_commit::CircleAck::signed(
            root_hash,
            circle_id,
            self.registration.reference().clone(),
            sequence,
            frontier,
            control,
            epoch_id,
            key_fingerprint,
            seeded_from,
            sync_time,
            crate::protocol::store_commit::SuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            &self.device_signer,
        )
    }

    pub(crate) fn local_circle_close_participant(
        &self,
        close: &crate::protocol::circle::CircleEpochClose,
    ) -> Option<crate::protocol::circle::CircleEpochCloseParticipant> {
        close
            .participants
            .iter()
            .find(|participant| participant.registration == *self.registration.reference())
            .cloned()
    }

    pub(crate) fn sign_circle_epoch_close_response(
        &self,
        control: &crate::protocol::circle::PreparedCircleControl,
        frontier: crate::protocol::store_commit::CommitFrontier,
    ) -> Result<
        crate::protocol::circle::CircleEpochCloseResponse,
        crate::protocol::circle::CircleTransitionError,
    > {
        crate::protocol::circle::CircleEpochCloseResponse::signed(
            control,
            self.registration.reference().clone(),
            frontier,
            self.registration.value(),
            &self.device_signer,
        )
    }

    pub(crate) fn verify_local_circle_epoch_close_response(
        &self,
        response: &crate::protocol::circle::CircleEpochCloseResponse,
        control: &crate::protocol::circle::PreparedCircleControl,
    ) -> bool {
        response.verify_for(control, self.registration.value())
    }

    pub(crate) fn circle_snapshot_semantic_prefix(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        generation: u64,
    ) -> String {
        crate::protocol::store_commit::circle_snapshot_slot_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            generation,
        )
    }

    pub(crate) fn circle_snapshot_image_semantic_prefix(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        image_hash: crate::protocol::store_commit::ObjectHash,
    ) -> String {
        crate::protocol::store_commit::circle_snapshot_image_semantic_prefix(
            circle_id,
            &self.registration.value().device_id.to_string(),
            image_hash,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_snapshot_meta(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        circle_id: crate::protocol::circle::CircleId,
        control: crate::protocol::circle::CircleControlCoord,
        epoch_id: crate::protocol::circle::CircleEpochId,
        key_fingerprint: crate::KeyFingerprint,
        generation: u64,
        bootstrap: crate::protocol::circle::CircleBootstrapRef,
        created_at: String,
        predecessor: Option<crate::protocol::store_commit::CircleSnapshotRef>,
        next_slot: crate::protocol::objects::ObjectSlot,
    ) -> Result<
        crate::protocol::store_commit::CircleSnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        let activation = crate::protocol::store_commit::circle_snapshot_stream_activation(
            root_hash,
            self.registration.reference(),
            circle_id,
            &self.registration.value().device_id.to_string(),
        )?;
        crate::protocol::store_commit::CircleSnapshotMeta::signed(
            root_hash,
            circle_id,
            self.registration.reference().clone(),
            control,
            epoch_id,
            key_fingerprint,
            generation,
            bootstrap,
            created_at,
            crate::protocol::store_commit::CircleSnapshotSuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            &self.device_signer,
        )
    }

    #[cfg(test)]
    pub(crate) async fn load_local_circle_snapshot_refs(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        circle_id: crate::protocol::circle::CircleId,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::CircleSnapshotRef,
            crate::protocol::store_commit::CircleSnapshotMeta,
        )>,
        crate::sync::store::owner::snapshot::SnapshotError,
    > {
        history
            .snapshots()
            .load_stream_refs(
                circle_id,
                access,
                self.registration.reference(),
                self.registration.value(),
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_local_circle_snapshots(
        &self,
        history: &mut crate::sync::store::owner::circles::VerifiedCircleHistory<'_, '_>,
        circle_id: crate::protocol::circle::CircleId,
        access: &crate::protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<crate::protocol::store_commit::CircleSnapshotMeta>,
        crate::sync::store::owner::snapshot::SnapshotError,
    > {
        Ok(history
            .snapshots()
            .load_stream_refs(
                circle_id,
                access,
                self.registration.reference(),
                self.registration.value(),
            )
            .await?
            .into_iter()
            .map(|(_, snapshot)| snapshot)
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn sign_circle_commit_for_test(
        &self,
        old_commit: &crate::protocol::store_commit::StoreBatchCommit,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        reference: crate::protocol::store_commit::CircleControlRef,
        stream_activations: Vec<crate::protocol::store_commit::StreamActivation>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        if old_commit.author_registration != *self.registration.reference() {
            return Err(
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    "test Circle commit is not authored by the local writer".to_string(),
                ),
            );
        }
        self.sign_circle_commit(
            old_commit.store_root_hash,
            old_commit.write_id.clone(),
            coord,
            old_commit.order.clone(),
            old_commit.membership_state.clone(),
            old_commit.device_state.clone(),
            old_commit
                .operations_membership_authority()
                .map_err(|error| {
                    crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                        format!(
                            "prepared Circle commit has no validated operations authority: {error}"
                        ),
                    )
                })?,
            reference,
            stream_activations,
        )
    }

    #[cfg(test)]
    pub(crate) fn resign_store_commit_for_test(
        &self,
        commit: &mut crate::protocol::store_commit::StoreBatchCommit,
    ) {
        commit.signature =
            crate::keys::sign_hex(&self.device_signer, &commit.canonical_signed_bytes()).1;
    }

    pub(super) fn sign_reclaim_evidence(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        claim: crate::protocol::reclaim::ReclaimClaim,
    ) -> Result<
        crate::protocol::reclaim::ReclaimEvidence,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::reclaim::ReclaimEvidence::signed(store_root_hash, claim, &self.identity)
    }

    pub(super) fn sign_reclaim_authorization(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        target: crate::protocol::reclaim::ReclaimTarget,
        evidence: crate::protocol::reclaim::ReclaimEvidenceRef,
        authority: crate::protocol::reclaim::StoreReclaimAuthority,
    ) -> crate::protocol::reclaim::ReclaimAuthorization {
        crate::protocol::reclaim::ReclaimAuthorization::signed(
            store_root_hash,
            target,
            evidence,
            authority,
            &self.identity,
        )
    }

    pub(super) fn effective_provider_admin_grant(
        &self,
        state: &crate::protocol::provider::ProviderAdminState,
    ) -> Option<crate::protocol::provider::ProviderAdminGrantId> {
        state
            .active()
            .into_iter()
            .find(|grant| state.authorizes(grant, self.registration.reference()))
    }

    pub(crate) fn candidate_family_id(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: &crate::WriteId,
        order: &crate::protocol::store_commit::StoreCommitOrder,
    ) -> crate::protocol::store_commit::CandidateFamilyId {
        crate::protocol::store_commit::CandidateFamilyId::derive(
            store_root_hash,
            self.registration.reference(),
            write_id,
            order,
        )
    }

    pub(crate) fn announcement_activation_id(
        &self,
    ) -> Result<
        crate::protocol::store_commit::StreamActivationId,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        self.registration
            .value()
            .store_announcement_activation(self.registration.reference())
            .map(|activation| activation.activation_id())
    }

    pub(super) fn acknowledgement_activation_id(
        &self,
    ) -> Result<
        crate::protocol::store_commit::StreamActivationId,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        self.registration
            .value()
            .store_acknowledgement_activation(self.registration.reference())
            .map(|activation| activation.activation_id())
    }

    pub(super) fn snapshot_activation_id(
        &self,
    ) -> Result<
        crate::protocol::store_commit::StreamActivationId,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        self.registration
            .value()
            .store_snapshot_activation(self.registration.reference())
            .map(|activation| activation.activation_id())
    }

    pub(super) fn first_snapshot_slot(&self) -> crate::protocol::objects::ObjectSlot {
        self.registration.value().snapshots.first_slot().clone()
    }

    pub(super) fn first_acknowledgement_slot(&self) -> crate::protocol::objects::ObjectSlot {
        self.registration
            .value()
            .acknowledgements
            .first_slot()
            .clone()
    }

    pub(super) async fn pull(
        &self,
        history: &mut super::AuthorizedStoreHistory<'_>,
        membership: &crate::protocol::membership::MembershipChain,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<super::pull::StorePullExecution, super::pull::StorePullError> {
        history
            .pull(membership, Some(&self.identity), routing_encryption)
            .await
    }

    #[cfg(test)]
    pub(super) async fn load_own_snapshot(
        &self,
        history: &mut super::AuthorizedStoreHistory<'_>,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::objects::StoreObjectError,
    > {
        history
            .load_store_snapshot(
                self.registration.reference(),
                self.registration.value(),
                reference,
            )
            .await
            .map(|(_, meta)| meta)
    }

    #[cfg(test)]
    pub(super) fn resign_snapshot(
        &self,
        meta: crate::protocol::store_commit::SnapshotMeta,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::signed(
            meta.store_root_hash,
            self.registration.reference().clone(),
            meta.generation,
            meta.predecessor,
            meta.image,
            meta.coverage,
            meta.state,
            meta.history_summary,
            meta.schema_version,
            meta.created_at,
            meta.successor,
            &self.device_signer,
        )
    }

    #[cfg(test)]
    pub(super) fn parse_snapshot(
        &self,
        bytes: &[u8],
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::parse_at(
            bytes,
            store_root_hash,
            reference,
            self.registration.value(),
        )
    }

    #[cfg(test)]
    pub(super) fn parse_snapshot_stream_entry(
        &self,
        bytes: &[u8],
        root: &crate::protocol::store_commit::StoreRootRef,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::parse_stream_entry_at(
            bytes,
            root,
            self.registration.reference(),
            self.registration.value(),
            reference,
        )
    }

    #[cfg(test)]
    pub(super) fn registration_reference_for_test(
        &self,
    ) -> crate::protocol::store_commit::StoreDeviceRegistrationRef {
        self.registration.reference().clone()
    }

    pub(super) fn sign_device_acknowledgement(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        sequence: u64,
        history_cut: crate::protocol::store_commit::StoreHistoryCut,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        snapshot: Option<crate::protocol::store_commit::StoreSnapshotLocator>,
        exclusions: crate::protocol::store_commit::StoreAckExclusionState,
        sync_time: String,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<
        crate::protocol::store_commit::StoreAck,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::StoreAck::signed(
            store_root_hash,
            self.registration.reference().clone(),
            sequence,
            history_cut,
            device_state,
            snapshot,
            exclusions,
            sync_time,
            successor,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_store_write_commit(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: crate::WriteId,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        membership_authority: crate::protocol::store_commit::StoreOperationMembershipAuthority,
        operations: crate::protocol::store_commit::StoreCommitOperationsInput<'_>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            write_id,
            coord,
            self.registration.reference().clone(),
            self.registration.value(),
            order,
            membership_state,
            device_state,
            membership_authority,
            operations,
            &self.device_signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_circle_commit(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: crate::WriteId,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        membership_authority: crate::protocol::store_commit::StoreOperationMembershipAuthority,
        circle_reference: crate::protocol::store_commit::CircleControlRef,
        stream_activations: Vec<crate::protocol::store_commit::StreamActivation>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.sign_store_write_commit(
            store_root_hash,
            write_id,
            coord,
            order,
            membership_state,
            device_state,
            membership_authority,
            crate::protocol::store_commit::StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations,
                circle_controls: vec![circle_reference],
                store_package: None,
                circle_packages: &[],
            },
        )
        .map_err(|error| {
            crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                error.to_string(),
            )
        })
    }

    pub(crate) fn verify_prepared_circle_commit(
        &self,
        bytes: &[u8],
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        object: crate::protocol::objects::ExactObjectRef,
    ) -> Result<
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.verify_prepared_commit(bytes, store_root_hash, coord, object)
            .map_err(|error| {
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    error.to_string(),
                )
            })
    }

    pub(crate) fn sign_circle_store_head(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceHead,
        crate::sync::store::circle_controls::CircleOperationError,
    > {
        self.sign_device_head(root_hash, commit, history_summary, successor)
            .map_err(|error| {
                crate::sync::store::circle_controls::CircleOperationError::InvalidState(
                    error.to_string(),
                )
            })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_candidate_abandonment(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        write_id: crate::WriteId,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        cleanup: Vec<crate::protocol::store_commit::CandidateCleanupManifest>,
    ) -> Result<
        crate::protocol::store_commit::StoreBatchCommit,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::StoreBatchCommit::signed_with_candidate_abandonment(
            store_root_hash,
            write_id,
            coord,
            self.registration.reference().clone(),
            self.registration.value(),
            order,
            membership_state,
            device_state,
            cleanup,
            &self.device_signer,
        )
    }

    pub(super) fn verify_prepared_commit(
        &self,
        bytes: &[u8],
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        object: crate::protocol::objects::ExactObjectRef,
    ) -> Result<
        crate::protocol::store_commit::VerifiedStoreBatchCommit,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::VerifiedStoreBatchCommit::parse_prepared(
            bytes,
            store_root_hash,
            coord,
            object,
            self.registration.value(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_snapshot(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        generation: u64,
        predecessor: Option<crate::protocol::store_commit::StoreSnapshotRef>,
        image: crate::protocol::store_commit::SnapshotImageRef,
        coverage: crate::protocol::store_commit::CommitFrontier,
        state: crate::protocol::store_commit::StoreSnapshotState,
        history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        schema_version: u32,
        created_at: String,
        successor: crate::protocol::store_commit::SnapshotSuccessorLink,
    ) -> Result<
        crate::protocol::store_commit::SnapshotMeta,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::SnapshotMeta::signed(
            store_root_hash,
            self.registration.reference().clone(),
            generation,
            predecessor,
            image,
            coverage,
            state,
            history_summary,
            schema_version,
            created_at,
            successor,
            &self.device_signer,
        )
    }

    pub(super) fn blob_write_authority(&self) -> crate::protocol::objects::BlobWriteAuthority<'_> {
        crate::protocol::objects::BlobWriteAuthority::new(&self.registration)
    }

    pub(super) async fn seal_keyring_for_member(
        &self,
        store_id: String,
        recipient: String,
        recipient_key: [u8; crate::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: crate::encryption::EncryptionService,
    ) -> Result<
        crate::protocol::wrapped_store_key::WrappedStoreKey,
        crate::sync::store::membership::InviteError,
    > {
        let signer = self.identity.clone();
        crate::blocking::run(move || {
            crate::protocol::wrapped_store_key::WrappedStoreKey::seal_keyring(
                &store_id,
                &recipient,
                &recipient_key,
                &keyring,
                &signer,
            )
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(format!(
                    "serialize invited member keyring: {error}"
                ))
            })
        })
        .await
        .map_err(|error| {
            crate::sync::store::membership::InviteError::Crypto(format!(
                "seal invited member Store key: {error}"
            ))
        })?
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_set_member(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        stream_id: crate::protocol::membership::AuthorStreamId,
        member_pubkey: String,
        member_email: Option<String>,
        role: crate::protocol::membership::MemberRole,
        wrapped_key: crate::protocol::wrapped_store_key::WrappedStoreKeyRef,
        timestamp: String,
    ) -> Result<
        crate::protocol::membership::MembershipEntry,
        crate::protocol::membership::MembershipError,
    > {
        chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
            &self.identity,
            stream_id,
            member_pubkey,
            member_email,
            role,
            None,
            wrapped_key,
            timestamp,
        )
    }

    pub(super) fn seal_keyring(
        &self,
        store_id: &str,
        recipient: &str,
        recipient_key: &[u8; crate::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: &crate::encryption::EncryptionService,
    ) -> Result<
        crate::protocol::wrapped_store_key::WrappedStoreKey,
        crate::encryption::EncryptionError,
    > {
        crate::protocol::wrapped_store_key::WrappedStoreKey::seal_keyring(
            store_id,
            recipient,
            recipient_key,
            keyring,
            &self.identity,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_owner_barrier_removal(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        stream_id: crate::protocol::membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<crate::protocol::wrapped_store_key::WrappedStoreKeyRef>,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        timestamp: String,
    ) -> Result<
        crate::protocol::membership::MembershipEntry,
        crate::protocol::membership::MembershipError,
    > {
        chain.signed_remove_member_with_owner_barrier_state(
            &self.identity,
            stream_id,
            revokee_pubkey,
            wrapped_keys,
            device_state,
            timestamp,
        )
    }

    pub(super) fn sign_direct_removal(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        stream_id: crate::protocol::membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<crate::protocol::wrapped_store_key::WrappedStoreKeyRef>,
        timestamp: String,
    ) -> Result<
        crate::protocol::membership::MembershipEntry,
        crate::protocol::membership::MembershipError,
    > {
        chain.signed_remove_member_with_wrapped_keys_in_stream(
            &self.identity,
            stream_id,
            revokee_pubkey,
            wrapped_keys,
            timestamp,
        )
    }

    pub(super) async fn load_membership_head(
        &self,
        verifier: crate::sync::store::owner::verification::StoreMembershipObjectVerifier<'_, '_>,
        reference: &crate::protocol::membership::MembershipHeadRef,
    ) -> Result<
        crate::protocol::objects::VerifiedObject<crate::protocol::membership::AuthorHead>,
        crate::protocol::objects::StoreObjectError,
    > {
        verifier
            .load_head_for_registration(reference, self.registration.value())
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_membership_transition(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        entry: &crate::protocol::membership::MembershipEntry,
        entry_ref: crate::protocol::membership::MembershipEntryRef,
        predecessor: Option<crate::protocol::membership::MembershipHeadRef>,
        anchor: crate::protocol::store_commit::GrantStreamAnchor,
        next_slot: crate::protocol::objects::ObjectSlot,
        head_slot: crate::protocol::objects::ObjectSlot,
    ) -> Result<
        crate::protocol::membership::MergeMembershipHeadTransition,
        crate::sync::store::membership::InviteError,
    > {
        if self.registration.value().author_pubkey != entry.author_pubkey
            || self.registration.reference().device_id != self.registration.value().device_id
        {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "membership author differs from the active exact device registration"
                        .to_string(),
                ),
            );
        }
        let coord = entry.coord();
        Ok(crate::protocol::membership::MergeMembershipHeadTransition {
            body: crate::protocol::membership::MembershipHeadBody {
                author_registration: self.registration.reference().clone(),
                entry: entry_ref,
                predecessor: predecessor.clone(),
                resolutions: entry.resolution_dependencies.clone(),
                successor: crate::protocol::store_commit::SuccessorLink {
                    activation: crate::protocol::store_commit::StreamActivation::grant_authorized(
                        store_root_hash,
                        self.registration.reference().clone(),
                        coord.author_owner_grant.clone(),
                        anchor,
                    )
                    .activation_id(),
                    predecessor: predecessor.map(|reference| reference.object),
                    next_slot,
                },
            },
            head_slot,
        })
    }

    pub(super) fn sign_membership_head(
        &self,
        entry: &crate::protocol::membership::MembershipEntry,
        transition: &crate::protocol::membership::MergeMembershipHeadTransition,
        activation: crate::protocol::membership::MembershipHeadActivation,
    ) -> Result<crate::protocol::membership::AuthorHead, crate::sync::store::membership::InviteError>
    {
        if self.registration.value().author_pubkey != entry.author_pubkey
            || self.registration.reference() != &transition.body.author_registration
        {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "membership transition author differs from the active exact device registration"
                        .to_string(),
                ),
            );
        }
        Ok(crate::protocol::membership::AuthorHead::signed(
            entry.store_id.clone(),
            transition.body.clone(),
            activation,
            &self.device_signer,
        ))
    }

    pub(super) fn verify_membership_head(
        &self,
        head: &crate::protocol::membership::AuthorHead,
    ) -> bool {
        head.verify(self.registration.value())
    }

    pub(super) fn attach_merge_membership_proof(
        &self,
        candidate: &mut super::operation::operations::PreparedStoreOperationCommit,
        publication: &crate::protocol::membership_mutation::PreparedMembershipPublication,
        resolution: Option<&crate::protocol::membership::StoreMembershipConflictResolution>,
        prepare_head: impl FnOnce(
            &crate::protocol::objects::ProtocolObjectContext,
            crate::protocol::objects::ObjectSlot,
            &str,
            Vec<u8>,
        ) -> Result<
            crate::protocol::objects::PreparedExactObject,
            crate::protocol::objects::StoreObjectError,
        >,
    ) -> Result<(), crate::sync::store::StoreError> {
        candidate
            .attach_merge_membership_proof_with(
                publication,
                resolution,
                &self.identity,
                prepare_head,
            )
            .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn drain_tombstones(
        &self,
        database: &crate::database::StoreDatabase,
        storage: &dyn crate::storage::SyncStorage,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        store_id: &str,
        clock: &dyn crate::clock::Clock,
    ) -> Result<usize, String> {
        crate::blob::delete::TombstoneDrain::new(
            database,
            storage,
            cipher,
            pending_rotation,
            store_id,
            &self.identity,
            clock,
        )
        .drain()
        .await
    }

    pub(super) fn sign_operation_batch(
        &self,
        write_id: crate::WriteId,
        context: StoreOperationSigningContext,
        batch: super::operation::operations::StoreOperationBatch,
    ) -> Result<
        (
            crate::protocol::store_commit::StoreBatchCommit,
            Option<crate::protocol::store_commit::ActivatedStoreDeviceRegistration>,
        ),
        crate::sync::store::StoreError,
    > {
        use super::operation::operations::StoreOperationBatch;
        use crate::protocol::store_commit::{
            StoreBatchCommit, StoreCommitOperationsInput, StoreControl,
        };

        let registration_activation = match &batch {
            StoreOperationBatch::Outcome { registration, .. } => registration.as_deref().cloned(),
            _ => None,
        };
        let registration_ref = self.registration.reference().clone();
        let registration = self.registration.value();
        let signer = &self.device_signer;
        let root_hash = context.root.store_root_hash;
        let commit = match batch {
            StoreOperationBatch::Acknowledgement {
                reference: acknowledgement,
                value: _,
                circle_acknowledgements,
            } => StoreBatchCommit::signed_operations(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                StoreCommitOperationsInput {
                    acknowledgement: Some(acknowledgement),
                    circle_acknowledgements: circle_acknowledgements
                        .iter()
                        .map(|circle| circle.reference.clone())
                        .collect(),
                    control: None,
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    stream_activations: Vec::new(),
                    circle_controls: Vec::new(),
                    store_package: None,
                    circle_packages: &[],
                },
                signer,
            ),
            StoreOperationBatch::ProviderAccessGrant(grant) => {
                StoreBatchCommit::signed_with_provider_access(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![grant],
                    signer,
                )
            }
            StoreOperationBatch::Attempt(attempt) => StoreBatchCommit::signed_with_join_attempts(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                vec![attempt],
                signer,
            ),
            StoreOperationBatch::Abandonment(abandonment) => {
                StoreBatchCommit::signed_with_join_abandonments(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![abandonment],
                    signer,
                )
            }
            StoreOperationBatch::Outcome {
                outcome,
                registration: activation,
            } => StoreBatchCommit::signed_with_join_outcomes(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                vec![outcome],
                activation
                    .into_iter()
                    .map(|activation| {
                        activation.activated_reference().map_err(|error| {
                            crate::sync::store::StoreError::InvalidOutbound(error.to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                signer,
            ),
            StoreOperationBatch::CleanupReceipt(receipt) => {
                StoreBatchCommit::signed_with_join_cleanup_receipts(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![receipt],
                    signer,
                )
            }
            StoreOperationBatch::DeviceExclusionProposal(proposal) => {
                StoreBatchCommit::signed_with_device_exclusions(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    vec![proposal.reference().clone()],
                    Vec::new(),
                    signer,
                )
            }
            StoreOperationBatch::DeviceExclusionOutcome(outcome) => {
                StoreBatchCommit::signed_with_device_exclusions(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    Vec::new(),
                    vec![outcome.wire_reference()],
                    signer,
                )
            }
            StoreOperationBatch::ReclaimAuthorization(authorization) => {
                StoreBatchCommit::signed_reclaim_authorization(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    *authorization,
                    signer,
                )
            }
            StoreOperationBatch::ReclaimReceipt(receipt) => {
                StoreBatchCommit::signed_reclaim_receipt(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    *receipt,
                    signer,
                )
            }
            StoreOperationBatch::OwnerPromotionRequest(request) => {
                StoreBatchCommit::signed_with_owner_promotion_request(
                    root_hash,
                    write_id,
                    context.coord,
                    registration_ref,
                    registration,
                    context.order,
                    context.membership_state,
                    context.device_state,
                    context.membership_authority,
                    request,
                    signer,
                )
            }
            StoreOperationBatch::MergeMembershipActivation {
                transition,
                stream_activations,
            } => StoreBatchCommit::signed_operations(
                root_hash,
                write_id,
                context.coord,
                registration_ref,
                registration,
                context.order,
                context.membership_state,
                context.device_state,
                context.membership_authority,
                StoreCommitOperationsInput {
                    acknowledgement: None,
                    circle_acknowledgements: Vec::new(),
                    control: Some(StoreControl { transition }),
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    stream_activations,
                    circle_controls: Vec::new(),
                    store_package: None,
                    circle_packages: &[],
                },
                signer,
            ),
        }
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))?;
        Ok((commit, registration_activation))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_owner_promotion_request(
        &self,
        promotion_id: crate::protocol::store_commit::OwnerPromotionId,
        root: &crate::protocol::store_commit::StoreRootRef,
        promoter_owner_grant: crate::protocol::membership::MembershipGrantId,
        member_pubkey: String,
        member_grant: crate::protocol::membership::MembershipGrantId,
        member_registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        finalization: crate::protocol::store_commit::OwnerPromotionFinalization,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionRequest, crate::sync::store::StoreError>
    {
        crate::protocol::store_commit::OwnerPromotionRequest::signed(
            promotion_id,
            root,
            self.registration.reference().clone(),
            self.registration.value(),
            promoter_owner_grant,
            member_pubkey,
            member_grant,
            member_registration,
            membership_state,
            device_state,
            finalization,
            &self.identity,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(super) fn sign_owner_promotion_acceptance(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
        activation: crate::protocol::store_commit::OwnerPromotionRequestActivation,
        anchors: crate::protocol::store_commit::OwnerPromotionAnchors,
    ) -> Result<
        crate::protocol::store_commit::OwnerPromotionAcceptance,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::OwnerPromotionAcceptance::signed(
            request,
            activation,
            anchors,
            self.registration.value(),
            &self.identity,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_finalize_owner_promotion(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
        root: &crate::protocol::store_commit::StoreRootRef,
        candidate: &crate::protocol::store_commit::StoreDeviceRegistration,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
        wrapped_key: crate::protocol::wrapped_store_key::WrappedStoreKeyRef,
        timestamp: String,
    ) -> Result<
        crate::protocol::membership::MembershipEntry,
        crate::protocol::membership::MembershipError,
    > {
        membership.signed_finalize_owner_promotion_in_stream(
            root,
            self.registration.value(),
            candidate,
            acceptance,
            &self.identity,
            wrapped_key,
            timestamp,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_device_exclusion_proposal(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        proposal_id: crate::protocol::store_commit::StoreDeviceExclusionProposalId,
        target: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        outcome_slot: crate::protocol::objects::ObjectSlot,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceExclusionProposal,
        crate::sync::store::StoreError,
    > {
        crate::protocol::store_commit::StoreDeviceExclusionProposal::signed(
            root_hash,
            proposal_id,
            target,
            target_registration,
            device_state,
            outcome_slot,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(super) fn sign_device_exclusion_cancellation(
        &self,
        proposal: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &crate::protocol::store_commit::StoreDeviceExclusionProposal,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceExclusionCancellation,
        crate::sync::store::StoreError,
    > {
        crate::protocol::store_commit::StoreDeviceExclusionCancellation::signed(
            proposal,
            proposal_value,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(super) fn retain_device_exclusion_proposal(
        &self,
        reference: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposal,
        target: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        crate::protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::RetainedStoreDeviceExclusionProposal::from_exact(
            reference,
            proposal,
            target,
            self.registration.value(),
        )
    }

    pub(super) fn retain_device_exclusion_outcome(
        &self,
        reference: &crate::protocol::store_commit::StoreDeviceExclusionOutcomeRef,
        proposal: crate::protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        outcome: &crate::protocol::store_commit::StoreDeviceExclusionOutcome,
    ) -> Result<
        crate::protocol::store_commit::RetainedStoreDeviceExclusionOutcome,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::RetainedStoreDeviceExclusionOutcome::from_exact(
            reference,
            proposal,
            outcome,
            self.registration.value(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_device_exclusion(
        &self,
        proposal: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &crate::protocol::store_commit::StoreDeviceExclusionProposal,
        target: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        proof: crate::protocol::store_commit::StoreDeviceExclusionProof,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<crate::protocol::store_commit::StoreDeviceExclusion, crate::sync::store::StoreError>
    {
        crate::protocol::store_commit::StoreDeviceExclusion::signed(
            proposal,
            proposal_value,
            target,
            target_registration,
            proof,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_device_head(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
    {
        crate::protocol::store_commit::StoreDeviceHead::signed(
            root_hash,
            self.registration.reference().clone(),
            commit,
            history_summary,
            successor,
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(super) fn sign_reclaim_receipt(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        authorization: crate::protocol::reclaim::ReclaimAuthorizationRef,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    ) -> Result<crate::protocol::reclaim::ReclaimReceipt, crate::sync::store::StoreError> {
        crate::protocol::reclaim::ReclaimReceipt::signed(
            root_hash,
            authorization,
            membership_state,
            provider_admin_grant,
            self.registration.reference().clone(),
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }
}

impl crate::keys::DeviceSigningAuthority for LocalStoreWriter {
    fn public_key_hex(&self) -> String {
        crate::keys::public_key_hex(&self.device_signer)
    }

    fn sign(&self, message: &[u8]) -> [u8; crate::keys::SIGN_BYTES] {
        self.device_signer.sign(message)
    }
}

impl crate::keys::IdentityKeyAuthority for LocalStoreWriter {
    fn public_key(&self) -> [u8; crate::keys::SIGN_PUBLICKEYBYTES] {
        self.identity.public_key()
    }

    fn sign(&self, message: &[u8]) -> [u8; crate::keys::SIGN_BYTES] {
        self.identity.sign(message)
    }

    fn to_x25519_secret_key(&self) -> [u8; crate::keys::CURVE25519_SECRETKEYBYTES] {
        self.identity.to_x25519_secret_key()
    }
}

pub(crate) struct LocalWriterKeyrings<'storage> {
    writer: Arc<LocalStoreWriter>,
    keyrings: Arc<StoreKeyrings<'storage>>,
}

impl<'storage> LocalWriterKeyrings<'storage> {
    pub(crate) fn new(
        writer: Arc<LocalStoreWriter>,
        keyrings: Arc<StoreKeyrings<'storage>>,
    ) -> Self {
        Self { writer, keyrings }
    }

    pub(super) async fn open(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.keyrings.open(self.writer.as_ref(), membership).await
    }

    pub(super) async fn open_or(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
        initial: &crate::encryption::EncryptionService,
    ) -> Result<crate::encryption::EncryptionService, crate::sync::store::membership::InviteError>
    {
        self.keyrings
            .open_or(self.writer.as_ref(), membership, initial)
            .await
    }

    pub(super) async fn prepare(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        crate::protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::protocol::objects::StorageError,
    > {
        self.keyrings.prepare(recipient, value).await
    }
}
