use super::*;

impl LocalStoreWriter {
    pub(crate) fn membership_publication_signer(&self) -> crate::sync::store::authorization::history::membership_publication::MembershipPublicationSigner<'_>{
        crate::sync::store::authorization::history::membership_publication::MembershipPublicationSigner::device(&self.registration, &self.device_signer)
    }

    pub(crate) fn sign_authority_change(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        stream_id: coven_protocol::membership::AuthorStreamId,
        change: coven_protocol::membership::StoreAuthorityChange,
        timestamp: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
    > {
        chain.signed_change_in_stream(&self.identity, stream_id, change, timestamp)
    }

    pub(crate) async fn seal_keyring_for_member(
        &self,
        store_id: String,
        recipient: String,
        recipient_key: [u8; coven_keys::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: coven_keys::encryption::EncryptionService,
    ) -> Result<
        coven_protocol::wrapped_store_key::WrappedStoreKey,
        crate::sync::store::membership::MembershipMutationError,
    > {
        let signer = self.identity.clone();
        coven_foundation::blocking::run(move || {
            coven_protocol::wrapped_store_key::WrappedStoreKey::seal_keyring(
                &store_id,
                &recipient,
                &recipient_key,
                &keyring,
                &signer,
            )
            .map_err(crate::sync::store::membership::MembershipMutationError::Encryption)
        })
        .await
        .map_err(crate::sync::store::membership::MembershipMutationError::Blocking)?
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_set_member(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        stream_id: coven_protocol::membership::AuthorStreamId,
        member_pubkey: String,
        member_email: Option<String>,
        role: coven_protocol::membership::MemberRole,
        wrapped_key: coven_protocol::wrapped_store_key::WrappedStoreKeyRef,
        timestamp: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
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

    pub(crate) fn seal_keyring(
        &self,
        store_id: &str,
        recipient: &str,
        recipient_key: &[u8; coven_keys::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: &coven_keys::encryption::EncryptionService,
    ) -> Result<
        coven_protocol::wrapped_store_key::WrappedStoreKey,
        coven_keys::encryption::EncryptionError,
    > {
        coven_protocol::wrapped_store_key::WrappedStoreKey::seal_keyring(
            store_id,
            recipient,
            recipient_key,
            keyring,
            &self.identity,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_owner_barrier_removal(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        stream_id: coven_protocol::membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<coven_protocol::wrapped_store_key::WrappedStoreKeyRef>,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        timestamp: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
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

    pub(crate) fn sign_member_removal(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        stream_id: coven_protocol::membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<coven_protocol::wrapped_store_key::WrappedStoreKeyRef>,
        timestamp: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
    > {
        chain.signed_remove_member_with_wrapped_keys_in_stream(
            &self.identity,
            stream_id,
            revokee_pubkey,
            wrapped_keys,
            timestamp,
        )
    }

    pub(crate) async fn load_membership_head(
        &self,
        verifier: crate::sync::store::commit_verification::commit::StoreMembershipObjectVerifier<
            '_,
            '_,
        >,
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<coven_protocol::membership::AuthorHead>,
        coven_protocol::objects::StoreObjectError,
    > {
        verifier
            .load_head_for_registration(reference, self.registration.value())
            .await
    }

    pub(crate) fn verify_membership_head(
        &self,
        head: &coven_protocol::membership::AuthorHead,
    ) -> bool {
        head.verify(self.registration.value())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_owner_promotion_request(
        &self,
        promotion_id: coven_protocol::store_commit::OwnerPromotionId,
        root: &coven_protocol::store_commit::StoreRootRef,
        promoter_owner_grant: coven_protocol::membership::MembershipGrantId,
        member_pubkey: String,
        member_grant: coven_protocol::membership::MembershipGrantId,
        member_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        membership_state: coven_protocol::circle_control::StoreMembershipStateRef,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        finalization: coven_protocol::store_commit::OwnerPromotionFinalization,
        publication_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<coven_protocol::store_commit::OwnerPromotionRequest, crate::sync::store::StoreError>
    {
        coven_protocol::store_commit::OwnerPromotionRequest::signed(
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
            publication_slot,
            &self.identity,
        )
        .map_err(crate::sync::store::StoreError::from)
    }

    pub(crate) fn sign_owner_promotion_acceptance(
        &self,
        request: coven_protocol::store_commit::OwnerPromotionRequest,
        activation: coven_protocol::store_commit::OwnerPromotionRequestActivation,
        anchors: coven_protocol::store_commit::OwnerPromotionAnchors,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionAcceptance,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::OwnerPromotionAcceptance::signed(
            request,
            activation,
            anchors,
            self.registration.value(),
            &self.identity,
        )
    }

    pub(crate) fn sign_owner_promotion_request_publication(
        &self,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
        accepted: &coven_database::AcceptedStoreCommitPublication,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionRequestPublication,
        crate::sync::store::StoreError,
    > {
        coven_protocol::store_commit::OwnerPromotionRequestPublication::signed(
            commit,
            accepted.entry(),
            accepted.reference(),
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::StoreError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_finalize_owner_promotion(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
        root: &coven_protocol::store_commit::StoreRootRef,
        candidate: &coven_protocol::store_commit::StoreDeviceRegistration,
        acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
        wrapped_key: coven_protocol::wrapped_store_key::WrappedStoreKeyRef,
        timestamp: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
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
}
