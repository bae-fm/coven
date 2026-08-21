use super::StoreKeyrings;
use coven_keys::keys::UserKeypair;
use std::sync::Arc;

pub(crate) struct StoreOperationSigningContext {
    pub(super) root: coven_protocol::store_commit::StoreRootRef,
    pub(super) coord: coven_protocol::store_commit::StoreCommitCoord,
    pub(super) order: coven_protocol::store_commit::StoreCommitOrder,
    pub(super) membership_state: coven_protocol::circle_control::StoreMembershipStateRef,
    pub(super) device_state: coven_protocol::store_commit::StoreDeviceStateRef,
    pub(super) membership_authority:
        coven_protocol::store_commit::StoreOperationMembershipAuthority,
}

mod circle_signing;
mod commit_signing;
mod device_join_signing;
mod exclusion_reclaim_signing;
mod membership_signing;
mod snapshot_support;

pub(crate) struct LocalStoreWriter {
    identity: UserKeypair,
    registration: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
    device_signer: UserKeypair,
}

impl LocalStoreWriter {
    pub(crate) fn from_verified_parts(
        identity: UserKeypair,
        registration: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> Self {
        Self {
            identity,
            registration,
            device_signer,
        }
    }

    pub(crate) fn author_pubkey(&self) -> String {
        coven_keys::keys::public_key_hex(&self.identity)
    }

    pub(crate) fn circle_device_id(&self) -> String {
        self.registration.value().device_id.to_string()
    }

    pub(crate) fn circle_grant_authorized_stream_id(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        owner_grant: &coven_protocol::membership::MembershipGrantId,
        domain: coven_protocol::store_commit::StreamAnchorDomain,
    ) -> coven_protocol::membership::AuthorStreamId {
        coven_protocol::store_commit::StreamActivation::grant_authorized_stream_id(
            root_hash,
            self.registration.reference(),
            owner_grant,
            domain,
        )
    }

    pub(crate) fn circle_grant_authorized_activation(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        owner_grant: coven_protocol::membership::MembershipGrantId,
        anchor: coven_protocol::store_commit::GrantStreamAnchor,
    ) -> coven_protocol::store_commit::StreamActivation {
        coven_protocol::store_commit::StreamActivation::grant_authorized(
            root_hash,
            self.registration.reference().clone(),
            owner_grant,
            anchor,
        )
    }

    pub(super) fn device_id(&self) -> &coven_protocol::store_commit::StoreDeviceId {
        &self.registration.value().device_id
    }

    pub(super) fn registration_ref(
        &self,
    ) -> &coven_protocol::store_commit::StoreDeviceRegistrationRef {
        self.registration.reference()
    }

    pub(crate) fn is_authored_by_registration(
        &self,
        registration: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> bool {
        self.registration.reference() == registration
    }

    pub(super) fn matches_author(
        &self,
        registration: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        author_pubkey: &str,
    ) -> bool {
        self.registration.reference() == registration
            && self.registration.value().author_pubkey == author_pubkey
    }

    pub(super) async fn authorize_retained_outbound(
        &self,
        history: &super::AuthorizedStoreHistory<'_>,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::MergeOutboundAuthorization,
        super::pull::StorePullError,
    > {
        history
            .authorize_retained_outbound(order, membership_heads, self.registration.reference())
            .await
    }

    pub(super) async fn authorize_retained_conflict_resolution(
        &self,
        history: &super::AuthorizedStoreHistory<'_>,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<
        crate::sync::store::merge_conflict::MergeConflictResolutionAuthorization,
        super::pull::StorePullError,
    > {
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
        coverage: &coven_protocol::store_commit::CommitFrontier,
        membership: &coven_protocol::membership::MembershipChain,
        state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
    ) -> Result<
        coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
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
        activating_commit: &coven_protocol::store_commit::StoreBatchCommitRef,
        activating_commit_value: &coven_protocol::store_commit::StoreBatchCommit,
        reference: coven_protocol::store_commit::StoreAckRef,
        value: coven_protocol::store_commit::StoreAck,
    ) -> Result<
        coven_protocol::store_commit::RetainedVerifiedActivatedAck,
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
        store_root_hash: coven_protocol::store_commit::ObjectHash,
    ) -> coven_protocol::membership::AuthorStreamId {
        coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            store_root_hash,
            self.registration.reference(),
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        )
    }

    pub(super) fn grant_authorized_stream_id(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        grant: &coven_protocol::membership::MembershipGrantId,
        domain: coven_protocol::store_commit::StreamAnchorDomain,
    ) -> coven_protocol::membership::AuthorStreamId {
        coven_protocol::store_commit::StreamActivation::grant_authorized_stream_id(
            store_root_hash,
            self.registration.reference(),
            grant,
            domain,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_conflict_resolution(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        selection: coven_protocol::membership::MembershipConflictSelection,
        replacement_grant: coven_protocol::membership::MembershipGrantId,
        membership: coven_protocol::store_commit::GrantStreamAnchor,
        recovery: coven_protocol::store_commit::GrantStreamAnchor,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
    ) -> Result<
        coven_protocol::membership::StoreMembershipConflictResolution,
        crate::sync::store::membership::MembershipMutationError,
    > {
        let acceptance = coven_protocol::store_commit::OwnerConflictResolutionAcceptance::signed(
            store_root_hash,
            replacement_grant,
            self.registration.reference().clone(),
            membership.clone(),
            recovery,
            device_state,
            self.registration.value(),
            &self.identity,
        )
        .map_err(crate::sync::store::membership::MembershipMutationError::from)?;
        chain
            .signed_conflict_resolution(
                store_root_hash,
                selection,
                membership,
                acceptance,
                &self.identity,
            )
            .map_err(crate::sync::store::membership::MembershipMutationError::from)
    }

    pub(super) fn sign_conflict_resolution_activation(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        stream_id: coven_protocol::membership::AuthorStreamId,
        reference: coven_protocol::membership::StoreMembershipConflictResolutionRef,
        resolution: &coven_protocol::membership::StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
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
        membership: &coven_protocol::membership::MembershipChain,
    ) -> bool {
        membership.is_owner_now(&self.registration.value().author_pubkey)
    }

    pub(crate) fn provider_administrator_grants(
        &self,
        state: &coven_protocol::provider::ProviderAdminState,
    ) -> std::collections::BTreeMap<
        coven_protocol::provider::ProviderAdminGrantId,
        coven_protocol::provider::ProviderAdminGrantRecord,
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

    pub(super) fn effective_provider_admin_grant(
        &self,
        state: &coven_protocol::provider::ProviderAdminState,
    ) -> Option<coven_protocol::provider::ProviderAdminGrantId> {
        state
            .active()
            .into_iter()
            .find(|grant| state.authorizes(grant, self.registration.reference()))
    }

    pub(crate) fn candidate_family_id(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        write_id: &coven_protocol::write::WriteId,
        order: &coven_protocol::store_commit::StoreCommitOrder,
    ) -> coven_protocol::store_commit::CandidateFamilyId {
        coven_protocol::store_commit::CandidateFamilyId::derive(
            store_root_hash,
            self.registration.reference(),
            write_id,
            order,
        )
    }

    pub(crate) fn announcement_activation_id(
        &self,
    ) -> Result<
        coven_protocol::store_commit::StreamActivationId,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        self.registration
            .value()
            .store_announcement_activation(self.registration.reference())
            .map(|activation| activation.activation_id())
    }

    pub(crate) fn acknowledgement_activation_id(
        &self,
    ) -> Result<
        coven_protocol::store_commit::StreamActivationId,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        self.registration
            .value()
            .store_acknowledgement_activation(self.registration.reference())
            .map(|activation| activation.activation_id())
    }

    pub(crate) fn snapshot_activation_id(
        &self,
    ) -> Result<
        coven_protocol::store_commit::StreamActivationId,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        self.registration
            .value()
            .store_snapshot_activation(self.registration.reference())
            .map(|activation| activation.activation_id())
    }

    pub(crate) fn first_snapshot_slot(&self) -> coven_protocol::objects::ObjectSlot {
        self.registration.value().snapshots.first_slot().clone()
    }

    pub(crate) fn first_acknowledgement_slot(&self) -> coven_protocol::objects::ObjectSlot {
        self.registration
            .value()
            .acknowledgements
            .first_slot()
            .clone()
    }

    pub(crate) fn blob_write_authority(&self) -> coven_protocol::objects::BlobWriteAuthority<'_> {
        coven_protocol::objects::BlobWriteAuthority::new(&self.registration)
    }
}

impl coven_keys::keys::DeviceSigningAuthority for LocalStoreWriter {
    fn public_key_hex(&self) -> String {
        coven_keys::keys::public_key_hex(&self.device_signer)
    }

    fn sign(&self, message: &[u8]) -> [u8; coven_keys::keys::SIGN_BYTES] {
        self.device_signer.sign(message)
    }
}

impl coven_keys::keys::IdentityKeyAuthority for LocalStoreWriter {
    fn public_key(&self) -> [u8; coven_keys::keys::SIGN_PUBLICKEYBYTES] {
        self.identity.public_key()
    }

    fn sign(&self, message: &[u8]) -> [u8; coven_keys::keys::SIGN_BYTES] {
        self.identity.sign(message)
    }

    fn to_x25519_secret_key(&self) -> [u8; coven_keys::keys::CURVE25519_SECRETKEYBYTES] {
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
        membership: &coven_protocol::membership::MembershipChain,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::membership::MembershipMutationError,
    > {
        self.keyrings.open(self.writer.as_ref(), membership).await
    }

    pub(super) async fn open_or(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
        initial: &coven_keys::encryption::EncryptionService,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::membership::MembershipMutationError,
    > {
        self.keyrings
            .open_or(self.writer.as_ref(), membership, initial)
            .await
    }

    pub(super) async fn prepare(
        &self,
        recipient: &str,
        value: coven_protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        coven_protocol::wrapped_store_key::PreparedWrappedStoreKey,
        coven_protocol::objects::StorageError,
    > {
        self.keyrings.prepare(recipient, value).await
    }
}
