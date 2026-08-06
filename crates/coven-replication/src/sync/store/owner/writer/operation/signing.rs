use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
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

    pub(crate) fn seal_local_keyring(
        &self,
        store_id: &str,
        recipient: &str,
        recipient_key: &[u8; coven_keys::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: &coven_keys::encryption::EncryptionService,
    ) -> Result<
        coven_protocol::wrapped_store_key::WrappedStoreKey,
        coven_keys::encryption::EncryptionError,
    > {
        self.writer
            .seal_keyring(store_id, recipient, recipient_key, keyring)
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
    ) -> Result<PreparedWrappedStoreKey, InviteError> {
        let wrapped = self
            .writer
            .seal_keyring(store_id, recipient, recipient_key, keyring)
            .map_err(|error| InviteError::Crypto(format!("serialize rotated keyring: {error}")))?;
        self.prepare_wrapped_key(recipient, wrapped)
            .await
            .map_err(InviteError::from)
    }

    pub(super) fn sign_owner_barrier_removal(
        &self,
        chain: &MembershipChain,
        stream_id: membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        timestamp: String,
    ) -> Result<MembershipEntry, InviteError> {
        self.writer
            .sign_owner_barrier_removal(
                chain,
                stream_id,
                revokee_pubkey,
                wrapped_keys,
                device_state,
                timestamp,
            )
            .map_err(InviteError::from)
    }

    pub(super) fn sign_direct_removal(
        &self,
        chain: &MembershipChain,
        stream_id: membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        timestamp: String,
    ) -> Result<MembershipEntry, InviteError> {
        self.writer
            .sign_direct_removal(chain, stream_id, revokee_pubkey, wrapped_keys, timestamp)
            .map_err(InviteError::from)
    }

    pub(crate) fn attach_merge_membership_proof(
        &self,
        candidate: &mut operations::PreparedStoreOperationCommit,
        publication: &PreparedMembershipPublication,
        resolution: Option<&membership::StoreMembershipConflictResolution>,
    ) -> Result<(), StoreError> {
        self.writer.attach_merge_membership_proof(
            candidate,
            publication,
            resolution,
            |context, slot, prefix, bytes| {
                self.storage
                    .prepare_protocol_object(context, slot, prefix, bytes)
                    .map_err(StoreObjectError::from)
            },
        )
    }

    pub(super) fn attach_membership_proof(
        &self,
        candidate: &mut operations::PreparedStoreOperationCommit,
        publication: &PreparedMembershipPublication,
    ) -> Result<(), InviteError> {
        self.attach_merge_membership_proof(candidate, publication, None)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
    }

    pub(super) async fn set_membership_access(
        &self,
        state: coven_storage::cloud::CloudAccessState,
    ) -> Result<coven_storage::cloud::CloudAccessOutcome, InviteError> {
        self.storage
            .set_member_access(state)
            .await
            .map_err(InviteError::from)
    }
}
