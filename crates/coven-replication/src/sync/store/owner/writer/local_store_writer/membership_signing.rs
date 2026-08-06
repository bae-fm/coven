use super::*;

impl LocalStoreWriter {
    pub(crate) async fn seal_keyring_for_member(
        &self,
        store_id: String,
        recipient: String,
        recipient_key: [u8; coven_keys::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: coven_keys::encryption::EncryptionService,
    ) -> Result<
        coven_protocol::wrapped_store_key::WrappedStoreKey,
        crate::sync::store::membership::InviteError,
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

    pub(crate) fn sign_direct_removal(
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
        verifier: crate::sync::store::owner::verification::StoreMembershipObjectVerifier<'_, '_>,
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<coven_protocol::membership::AuthorHead>,
        coven_protocol::objects::StoreObjectError,
    > {
        verifier
            .load_head_for_registration(reference, self.registration.value())
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_membership_transition(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        entry: &coven_protocol::membership::MembershipEntry,
        entry_ref: coven_protocol::membership::MembershipEntryRef,
        predecessor: Option<coven_protocol::membership::MembershipHeadRef>,
        anchor: coven_protocol::store_commit::GrantStreamAnchor,
        next_slot: coven_protocol::objects::ObjectSlot,
        head_slot: coven_protocol::objects::ObjectSlot,
    ) -> Result<
        coven_protocol::membership::MergeMembershipHeadTransition,
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
        Ok(coven_protocol::membership::MergeMembershipHeadTransition {
            body: coven_protocol::membership::MembershipHeadBody {
                author_registration: self.registration.reference().clone(),
                entry: entry_ref,
                predecessor: predecessor.clone(),
                resolutions: entry.resolution_dependencies.clone(),
                successor: coven_protocol::store_commit::SuccessorLink {
                    activation: coven_protocol::store_commit::StreamActivation::grant_authorized(
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

    pub(crate) fn sign_membership_head(
        &self,
        entry: &coven_protocol::membership::MembershipEntry,
        transition: &coven_protocol::membership::MergeMembershipHeadTransition,
        activation: coven_protocol::membership::MembershipHeadActivation,
    ) -> Result<coven_protocol::membership::AuthorHead, crate::sync::store::membership::InviteError>
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
        Ok(coven_protocol::membership::AuthorHead::signed(
            entry.store_id.clone(),
            transition.body.clone(),
            activation,
            &self.device_signer,
        ))
    }

    pub(crate) fn verify_membership_head(
        &self,
        head: &coven_protocol::membership::AuthorHead,
    ) -> bool {
        head.verify(self.registration.value())
    }

    pub(crate) fn attach_merge_membership_proof(
        &self,
        candidate: &mut crate::sync::store::owner::writer::operation::operations::PreparedStoreOperationCommit,
        publication: &coven_protocol::membership_mutation::PreparedMembershipPublication,
        resolution: Option<&coven_protocol::membership::StoreMembershipConflictResolution>,
        prepare_head: impl FnOnce(
            &coven_protocol::objects::ProtocolObjectContext,
            coven_protocol::objects::ObjectSlot,
            &str,
            Vec<u8>,
        ) -> Result<
            coven_protocol::objects::PreparedExactObject,
            coven_protocol::objects::StoreObjectError,
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
            &self.identity,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
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
