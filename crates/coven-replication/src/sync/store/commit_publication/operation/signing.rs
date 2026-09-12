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

    pub(crate) fn seal_member_keyring(
        &self,
        membership: &MembershipChain,
        initial: &coven_keys::encryption::EncryptionService,
        recipient: &str,
    ) -> Result<SealedStoreKey, MembershipMutationError> {
        let store_id = self.store_root().store_root_id.to_string();
        if membership.store_id() != Some(store_id.as_str()) {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "sealed-key membership belongs to another Store".into(),
            ));
        }
        let keyring = self.keyrings.open_or(membership, initial)?;
        SealedStoreKey::seal(recipient, &keyring).map_err(MembershipMutationError::SealedKeySeal)
    }

    pub(crate) fn sign_finalize_owner_promotion(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
        root: &coven_protocol::store_commit::StoreRootRef,
        candidate: &coven_protocol::store_commit::StoreDeviceRegistration,
        acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
        sealed_key: SealedStoreKey,
        timestamp: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
    > {
        self.writer.sign_finalize_owner_promotion(
            membership, root, candidate, acceptance, sealed_key, timestamp,
        )
    }

    pub(super) fn sign_owner_barrier_removal(
        &self,
        chain: &MembershipChain,
        stream_id: membership::AuthorStreamId,
        revokee_pubkey: String,
        sealed_keys: BTreeMap<String, SealedStoreKey>,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        timestamp: String,
    ) -> Result<MembershipEntry, MembershipMutationError> {
        self.writer
            .sign_owner_barrier_removal(
                chain,
                stream_id,
                revokee_pubkey,
                sealed_keys,
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
        sealed_keys: BTreeMap<String, SealedStoreKey>,
        timestamp: String,
    ) -> Result<MembershipEntry, MembershipMutationError> {
        self.writer
            .sign_member_removal(chain, stream_id, revokee_pubkey, sealed_keys, timestamp)
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
