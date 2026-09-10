use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(crate) fn sign_owner_promotion_request_publication(
        &self,
        commit: &coven_protocol::store_commit::StoreBatchCommit,
        accepted: &coven_database::AcceptedStoreCommitPublication,
    ) -> Result<coven_protocol::store_commit::OwnerPromotionRequestPublication, StoreError> {
        self.writer
            .sign_owner_promotion_request_publication(commit, accepted)
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
        self.writer
            .sign_owner_promotion_acceptance(request, activation, anchors)
    }

    pub(crate) async fn prepare_member_wrapped_key(
        &self,
        membership: &MembershipChain,
        initial: &coven_keys::encryption::EncryptionService,
        recipient: &str,
    ) -> Result<PreparedWrappedStoreKey, MembershipMutationError> {
        let store_id = self.store_root().store_root_id.to_string();
        if membership.store_id() != Some(store_id.as_str()) {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "wrapped-key membership belongs to another Store".into(),
            ));
        }
        let recipient_key = coven_keys::keys::ed25519_hex_to_x25519_public_key(recipient)?;
        let keyring = self.keyrings.open_or(membership, initial).await?;
        let signed = self
            .writer
            .seal_keyring_for_member(store_id, recipient.to_string(), recipient_key, keyring)
            .await?;
        self.keyrings
            .prepare(recipient, signed)
            .await
            .map_err(MembershipMutationError::from)
    }

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
        self.writer.sign_finalize_owner_promotion(
            membership,
            root,
            candidate,
            acceptance,
            wrapped_key,
            timestamp,
        )
    }

    pub(super) async fn prepare_replacement_wrapped_key(
        &self,
        store_id: &str,
        recipient: &str,
        recipient_key: &[u8; coven_keys::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: &coven_keys::encryption::EncryptionService,
    ) -> Result<PreparedWrappedStoreKey, MembershipMutationError> {
        let wrapped = self
            .writer
            .seal_keyring(store_id, recipient, recipient_key, keyring)
            .map_err(MembershipMutationError::Encryption)?;
        self.prepare_wrapped_key(recipient, wrapped)
            .await
            .map_err(MembershipMutationError::from)
    }

    pub(super) fn sign_owner_barrier_removal(
        &self,
        chain: &MembershipChain,
        stream_id: membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        timestamp: String,
    ) -> Result<MembershipEntry, MembershipMutationError> {
        self.writer
            .sign_owner_barrier_removal(
                chain,
                stream_id,
                revokee_pubkey,
                wrapped_keys,
                device_state,
                timestamp,
            )
            .map_err(MembershipMutationError::from)
    }

    pub(super) fn sign_member_removal(
        &self,
        chain: &MembershipChain,
        stream_id: membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        timestamp: String,
    ) -> Result<MembershipEntry, MembershipMutationError> {
        self.writer
            .sign_member_removal(chain, stream_id, revokee_pubkey, wrapped_keys, timestamp)
            .map_err(MembershipMutationError::from)
    }

    pub(super) async fn set_membership_access(
        &self,
        state: coven_storage::cloud::CloudAccessState,
    ) -> Result<coven_storage::cloud::CloudAccessOutcome, MembershipMutationError> {
        self.storage
            .set_member_access(state)
            .await
            .map_err(MembershipMutationError::from)
    }
}
