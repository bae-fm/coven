use super::*;
use crate::database::VerifiedMergeMembershipObjects;
use crate::protocol::membership::{
    self, MembershipChain, MembershipChange, MembershipEntry, MembershipError, MembershipHeadRef,
};
use crate::protocol::objects::{
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, StoreObjectError,
};
use crate::protocol::store_commit::{
    self, commit_semantic_prefix, head_slot_prefix, membership_head_slot_prefix,
    StoreBatchCommitDeletionTarget, StoreDeviceHeadRef,
};
use crate::protocol::wrapped_store_key::{PreparedWrappedStoreKey, WrappedStoreKeyRef};
use crate::storage as store_objects;
use crate::sync::store::membership::InviteError;
use crate::sync::store::owner::load_wrapped_store_key;
use crate::sync::store::owner::verification::StoreMembershipObjectVerifier;
use std::sync::Arc;

mod abandonment;
pub(crate) mod acknowledgements;
mod blob_lifecycle;
mod blob_preparation;
mod blob_upload;
pub(super) mod membership_mutation;
pub(super) mod membership_mutation_journal;
pub(crate) mod operations;
mod preparation;
pub(crate) mod reclaim;
pub(crate) mod snapshot;

pub(super) use blob_preparation::close_prepared_packages;
pub(crate) use blob_preparation::prepare_partition_blob_locator;

use membership_mutation_journal::{
    decode_membership_mutation, exact_owned_remote, InviteMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence, ReplacementWrappedKey, ResolveMutationPlan,
    RevokeMembershipPublication, RevokeMutationPlan,
};

pub(crate) struct MergeConflictResolutionCommitPlan {
    authorship: crate::database::store::OwnStreamAuthorship,
    writer: Arc<LocalStoreWriter>,
    root: crate::protocol::store_commit::StoreRootRef,
    coord: crate::protocol::store_commit::StoreCommitCoord,
    order: crate::protocol::store_commit::StoreCommitOrder,
    membership: crate::protocol::membership::MembershipChain,
    device_state: crate::protocol::store_commit::StoreDeviceStateRef,
    device_state_value: crate::protocol::store_commit::ResolvedStoreDeviceState,
}

impl MergeConflictResolutionCommitPlan {
    #[allow(clippy::too_many_arguments)]
    fn new(
        authorship: crate::database::store::OwnStreamAuthorship,
        writer: Arc<LocalStoreWriter>,
        root: crate::protocol::store_commit::StoreRootRef,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        authorization: super::history::MergeConflictResolutionAuthorization,
    ) -> Self {
        Self {
            authorship,
            writer,
            root,
            coord,
            order,
            membership: authorization.membership,
            device_state: authorization.device_state_ref,
            device_state_value: authorization.device_state,
        }
    }

    pub(super) fn root(&self) -> &crate::protocol::store_commit::StoreRootRef {
        &self.root
    }

    pub(super) fn membership(&self) -> &crate::protocol::membership::MembershipChain {
        &self.membership
    }

    pub(super) fn grant_authorized_stream_id(
        &self,
        grant: &crate::protocol::membership::MembershipGrantId,
        domain: crate::protocol::store_commit::StreamAnchorDomain,
    ) -> crate::protocol::membership::AuthorStreamId {
        self.writer
            .grant_authorized_stream_id(self.root.store_root_hash, grant, domain)
    }

    pub(super) fn sign_conflict_resolution(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        selection: crate::protocol::membership::MembershipConflictSelection,
        replacement_grant: crate::protocol::membership::MembershipGrantId,
        membership: crate::protocol::store_commit::GrantStreamAnchor,
        recovery: crate::protocol::store_commit::GrantStreamAnchor,
    ) -> Result<crate::protocol::membership::StoreMembershipConflictResolution, InviteError> {
        self.writer.sign_conflict_resolution(
            chain,
            self.root.store_root_hash,
            selection,
            replacement_grant,
            membership,
            recovery,
            self.device_state.clone(),
        )
    }

    pub(super) fn sign_conflict_resolution_activation(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
        stream_id: crate::protocol::membership::AuthorStreamId,
        reference: crate::protocol::membership::StoreMembershipConflictResolutionRef,
        resolution: &crate::protocol::membership::StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<
        crate::protocol::membership::MembershipEntry,
        crate::protocol::membership::MembershipError,
    > {
        self.writer.sign_conflict_resolution_activation(
            chain,
            self.root.store_root_hash,
            stream_id,
            reference,
            resolution,
            created_at,
        )
    }

    pub(super) fn finish(
        self,
        membership: &crate::protocol::membership::MembershipChain,
        resolution: &crate::protocol::membership::StoreMembershipConflictResolutionRef,
    ) -> Result<operations::StoreOperationCommitPlan, StoreError> {
        let crate::protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(StoreError::InvalidOutbound(
                "conflict-resolution candidate membership remains conflicted".to_string(),
            ));
        };
        if membership
            .resolution_refs()
            .binary_search(resolution)
            .is_err()
        {
            return Err(StoreError::InvalidOutbound(
                "conflict-resolution candidate membership omits its exact resolution".to_string(),
            ));
        }
        let replacement_grant = crate::protocol::membership::derive_store_resolution_grant(
            &resolution.conflict_hash,
            &resolution.resolver_pubkey,
        );
        let authority =
            crate::protocol::membership::MembershipGrantCreationAuthority::ConflictResolution(
                resolution.clone(),
            );
        if membership
            .active_grant(&replacement_grant)
            .is_none_or(|record| {
                record.member_pubkey != self.writer.author_pubkey()
                    || record.creation_authority != authority
            })
        {
            return Err(StoreError::InvalidOutbound(
                "conflict-resolution candidate is not authorized by its replacement Owner grant"
                    .to_string(),
            ));
        }
        let membership_state =
            crate::protocol::circle_control::StoreMembershipStateRef::from_parts(
                membership.head_refs().to_vec(),
                membership.resolution_refs().to_vec(),
                self.device_state.recovery().to_vec(),
                resolved.state_hash,
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let common = operations::StoreOperationPlanCommon::new(
            self.authorship,
            self.writer,
            self.root,
            self.coord,
            self.order,
            membership_state,
            self.device_state,
            crate::protocol::store_commit::StoreOperationMembershipAuthority {
                predecessor: authority,
            },
            Some(replacement_grant),
        );
        Ok(operations::StoreOperationCommitPlan::new(
            common,
            membership.clone(),
            self.device_state_value,
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreWriterAuthorizationError {
    #[error("Store authority: {0}")]
    StoreAuthority(SyncCycleFailure),
    #[error("Store writer registration: {0}")]
    Registration(StoreRegistrationError),
}

#[derive(Debug, thiserror::Error)]
enum AuthorizationRefreshError {
    #[error("select this device's wrapped-key authority: {0}")]
    Membership(#[source] crate::protocol::membership::MembershipError),
    #[error("read this device's wrapped key: {0}")]
    WrappedKey(#[source] crate::sync::store::membership::InviteError),
    #[error("refresh state is invalid: {0}")]
    InvalidState(String),
    #[error("rotation gate database state: {0}")]
    Database(#[source] crate::database::DbError),
    #[error("merge this device's live and selected keyrings: {0}")]
    InvalidKeyring(#[source] crate::encryption::EncryptionError),
    #[error("adopt committed store-key rotation: {0}")]
    KeyAdoption(#[source] crate::keys::KeyError),
}

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(super) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'storage> {
        self.history.membership_objects()
    }

    pub(crate) fn store_root(&self) -> &crate::protocol::store_commit::StoreRootRef {
        self.history.root()
    }

    pub(crate) async fn snapshot_publication(&self) -> snapshot::AuthorizedSnapshotPublication<'_> {
        snapshot::AuthorizedSnapshotPublication::begin(&self.database, self.storage.as_ref()).await
    }

    pub(crate) async fn resume_snapshot_publication(
        &self,
    ) -> Result<Option<crate::protocol::store_commit::SnapshotMeta>, snapshot::SnapshotError> {
        self.snapshot_publication().await.resume_store().await
    }

    fn protocol_root(&self) -> &crate::protocol::store_commit::StoreProtocolRoot {
        &self.history.verified_root_object().value
    }

    fn resolved_membership(
        &self,
    ) -> Result<
        &crate::protocol::membership::MembershipChain,
        crate::sync::store::membership::MembershipOpsError,
    > {
        match self.membership.conflict() {
            Some(conflict) => Err(
                crate::sync::store::membership::MembershipOpsError::SemanticConflict(Box::new(
                    conflict.clone(),
                )),
            ),
            None => Ok(&self.membership),
        }
    }

    async fn open_keyring(
        &self,
    ) -> Result<crate::encryption::EncryptionService, super::membership::InviteError> {
        self.keyrings.open(&self.membership).await
    }

    pub(super) async fn open_keyring_for_membership(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> Result<crate::encryption::EncryptionService, super::membership::InviteError> {
        self.keyrings.open(membership).await
    }

    pub(crate) async fn open_keyring_or_for_membership(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
        initial: &crate::encryption::EncryptionService,
    ) -> Result<crate::encryption::EncryptionService, super::membership::InviteError> {
        self.keyrings.open_or(membership, initial).await
    }

    pub(crate) async fn prepare_wrapped_key(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        crate::protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::protocol::objects::StorageError,
    > {
        self.keyrings.prepare(recipient, value).await
    }

    /// Select the exact author stream without overwriting its committed prefix.
    /// Streams are persisted per database, so independently restored devices use
    /// different streams; copied state that reuses one exposes an immutable fork.
    pub(super) async fn select_membership_author_stream(
        &self,
        chain: &crate::protocol::membership::MembershipChain,
    ) -> Result<crate::protocol::membership::AuthorStreamId, super::membership::InviteError> {
        let author = self.writer.author_pubkey();
        let grant = chain.active_owner_grant(&author).ok_or_else(|| {
            crate::protocol::membership::MembershipError::SignerIsNotOwner(author.clone())
        })?;
        let mut reusable = chain.reusable_author_streams(&author, &grant);
        if let Some(anchored) = chain.membership_stream_id(&grant) {
            reusable.insert(anchored);
        }
        Ok(self
            .database
            .select_membership_author_stream(&author, &grant, reusable)
            .await?)
    }

    pub(super) async fn verify_membership_publication_author(
        &self,
        publication: &PreparedMembershipPublication,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceRegistration,
        crate::sync::store::membership::InviteError,
    > {
        let author = self
            .history
            .load_registration(&publication.head.body.author_registration)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?
            .value;
        if !publication.head.verify(&author) {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "prepared membership head has an invalid certified-device signature"
                        .to_string(),
                ),
            );
        }
        Ok(author)
    }

    pub(super) async fn blocked_candidate_nonactivation(
        &mut self,
        candidate: &crate::database::BlockedMergeCandidate,
    ) -> Result<Option<crate::protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let verified = self
            .history
            .authenticate_blocked_candidate(candidate)
            .await?;
        self.history
            .excluded_candidate_nonactivation(
                &verified,
                &candidate.head.value,
                &candidate.head.object,
            )
            .await
    }

    pub(super) async fn cleanup_merge_candidate_history(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.history.cleanup_merge_candidate(write_id).await
    }

    pub(super) async fn select_acknowledgement_snapshot(
        &mut self,
        frontier: &crate::protocol::store_commit::CommitFrontier,
        device_state: &crate::protocol::store_commit::StoreDeviceStateRef,
    ) -> Result<
        Option<crate::protocol::store_commit::StoreSnapshotLocator>,
        acknowledgements::StoreAckError,
    > {
        self.history
            .select_acknowledgement_snapshot(frontier, device_state)
            .await
    }

    pub(super) async fn stage_verified_blob_plaintext(
        &self,
        authority: &crate::protocol::blob::RowBlobAuthority,
        stored: &crate::protocol::blob::locator::StoredBlobRef,
        destination: &std::path::Path,
    ) -> Result<crate::local_file::AtomicStagedFile, crate::sync::BlobCacheError> {
        self.history
            .stage_verified_blob_plaintext(authority, stored, destination)
            .await
    }

    pub(super) async fn authorize_retained_outbound(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<super::verified_history::MergeOutboundAuthorization, super::pull::StorePullError>
    {
        self.writer
            .authorize_retained_outbound(&self.history, order, membership_heads)
            .await
    }

    pub(super) async fn prepare_merge_history_successor(
        &self,
        commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &crate::protocol::membership::MembershipChain,
        recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: crate::protocol::store_commit::ResolvedStoreDeviceState,
        evidence: super::verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<super::verified_history::PreparedMergeHistorySuccessor, super::pull::StorePullError>
    {
        self.history
            .prepare_merge_history_successor(
                commit,
                membership,
                recovery_author,
                state_after,
                evidence,
            )
            .await
    }

    pub(super) async fn observe_occupied_merge_head(
        &mut self,
        expected: &crate::protocol::store_commit::StoreDeviceHead,
        expected_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        slot: &crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<super::history::abandonment::VerifiedMergeWinner, StoreError> {
        self.history
            .observe_occupied_merge_head(expected, expected_commit, slot, semantic_prefix)
            .await
    }

    pub(super) async fn upload_commit(
        &self,
        candidate: &operations::PreparedStoreOperationCommit,
    ) -> Result<(), StoreError> {
        let stream_id = candidate.reference.coord.stream_id;
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            candidate.commit.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let prefix = crate::protocol::store_commit::commit_semantic_prefix(
            candidate.commit.candidate_family(),
            &stream_id.to_string(),
            candidate.commit.seq(),
            candidate.commit.commit_hash(),
        );
        self.storage
            .as_ref()
            .create_protocol_object(&candidate.prepared)
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        let opened = self
            .storage
            .as_ref()
            .read_protocol_object(&context, &candidate.reference.object, &prefix)
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        if opened != candidate.commit.to_bytes() {
            return Err(StoreError::InvalidOutbound(
                "Store operation commit exact readback differs from its signed bytes".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn load_own_snapshot_for_test(
        &mut self,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, snapshot::SnapshotError> {
        self.writer
            .load_own_snapshot(&mut self.history, reference)
            .await
            .map_err(snapshot::SnapshotError::StoreObject)
    }

    pub(crate) async fn pull(
        &mut self,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, SyncCycleFailure> {
        let membership = self.membership.clone();
        let execution = self
            .writer
            .pull(&mut self.history, &membership, routing_encryption)
            .await
            .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) async fn should_stop_before_pull(&self) -> Result<bool, SyncCycleFailure> {
        Ok(false)
    }

    pub(super) fn require_current_owner(&self, author_pubkey: &str) -> Result<(), String> {
        if self.membership.is_owner_now(author_pubkey) {
            Ok(())
        } else {
            Err(format!("author {author_pubkey} is not a current owner"))
        }
    }

    #[cfg(test)]
    pub(crate) fn sign_device_head_for_test(
        &self,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        self.writer.sign_device_head(
            self.store_root().store_root_hash,
            commit,
            history_summary,
            successor,
        )
    }

    #[cfg(test)]
    pub(crate) fn resign_snapshot_meta_for_test(
        &self,
        meta: crate::protocol::store_commit::SnapshotMeta,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        if meta.store_root_hash != self.store_root().store_root_hash
            || !self
                .writer
                .is_authored_by_registration(&meta.author_registration)
        {
            return Err(StoreError::InvalidOutbound(
                "snapshot test input belongs to another Store writer".to_string(),
            ));
        }
        self.writer
            .resign_snapshot(meta)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn parse_snapshot_meta_for_test(
        &self,
        bytes: &[u8],
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        self.writer
            .parse_snapshot(bytes, self.store_root().store_root_hash, reference)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn local_registration_ref_for_test(
        &self,
    ) -> crate::protocol::store_commit::StoreDeviceRegistrationRef {
        self.writer.registration_reference_for_test()
    }

    pub(super) fn reclaim_history(&mut self) -> super::history::ReclaimHistory<'_, 'storage> {
        self.history.reclaim()
    }

    pub(crate) fn owner_promotion(
        &mut self,
    ) -> super::owner_promotion::AuthorizedOwnerPromotion<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        super::owner_promotion::AuthorizedOwnerPromotion::new(
            self, database, storage, root, membership,
        )
    }

    pub(crate) fn owner_promotion_history(
        &mut self,
    ) -> super::history::OwnerPromotionHistory<'_, 'storage> {
        self.history.owner_promotion()
    }

    pub(crate) async fn refresh_authorization_state(
        &self,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        security: Option<&crate::store_security::StoreSecurity>,
    ) -> Result<(), SyncCycleFailure> {
        let result = async {
            if cipher.snapshot().is_plaintext() {
                tracing::debug!("refresh: plaintext home, nothing to refresh");
                return Ok(());
            }

            let recipient = self.writer.author_pubkey();
            let wrapped_keys = self
                .membership
                .wrapped_key_authority_for(&recipient)
                .map_err(AuthorizationRefreshError::Membership)?;
            let live_keyring = match cipher.snapshot() {
                crate::storage::CloudCipher::Encrypted(encryption) => encryption,
                crate::storage::CloudCipher::Plaintext => {
                    return Err(AuthorizationRefreshError::InvalidState(
                        "plaintext home cannot enter encrypted key refresh".to_string(),
                    ));
                }
            };
            if wrapped_keys.is_empty() {
                tracing::debug!(
                    "refresh: no activated wrapped key for this device; keeping the live key"
                );
                return Ok(());
            }

            match self.open_keyring().await {
                Ok(new_encryption) => {
                    let merged = live_keyring
                        .merged_with(&new_encryption)
                        .map_err(AuthorizationRefreshError::InvalidKeyring)?;
                    if merged.key_count() == live_keyring.key_count() {
                        if pending_rotation.gate().is_some() {
                            let gate = self
                                .database
                                .complete_peer_rotation_adoption(live_keyring.current_generation())
                                .await
                                .map_err(AuthorizationRefreshError::Database)?;
                            pending_rotation.install_durable_gate(gate);
                        }
                        tracing::debug!(
                            "refresh: wrapped store key is already held by the live keyring"
                        );
                    } else {
                        let gate = self
                            .database
                            .record_peer_rotation(merged.current_generation())
                            .await
                            .map_err(AuthorizationRefreshError::Database)?;
                        pending_rotation.install_durable_gate(Some(gate));
                        match security {
                            None => {
                                tracing::info!(
                                    committed_generation = merged.current_generation(),
                                    "refresh: found a rotated store key but this cycle has no \
                                     master-key custody to adopt it; sealing is paused until a \
                                     cycle with custody adopts it"
                                );
                            }
                            Some(security) => {
                                let fingerprint = security
                                    .adopt_key_rotation(cipher, &new_encryption)
                                    .map_err(AuthorizationRefreshError::KeyAdoption)?;
                                let adopted_generation = match cipher.snapshot() {
                                    crate::storage::CloudCipher::Encrypted(encryption) => {
                                        encryption.current_generation()
                                    }
                                    crate::storage::CloudCipher::Plaintext => {
                                        return Err(AuthorizationRefreshError::InvalidState(
                                            "encrypted key refresh produced a plaintext cipher"
                                                .to_string(),
                                        ));
                                    }
                                };
                                let gate = self
                                    .database
                                    .complete_peer_rotation_adoption(adopted_generation)
                                    .await
                                    .map_err(AuthorizationRefreshError::Database)?;
                                pending_rotation.install_durable_gate(gate);
                                tracing::info!(%fingerprint, "Adopted rotated store key");
                            }
                        }
                    }
                }
                Err(error) => return Err(AuthorizationRefreshError::WrappedKey(error)),
            }

            Ok(())
        }
        .await;

        result.map_err(|error| SyncCycleFailure::operation("refresh authorization state", error))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &mut self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::joining::InviteCode, crate::sync::store::membership::MembershipOpsError>
    {
        if role == crate::protocol::membership::MemberRole::Owner {
            return Err(crate::sync::store::membership::MembershipOpsError::Invite(
                crate::sync::store::membership::InviteError::Membership(
                    crate::protocol::membership::MembershipError::OwnerPromotionRequired,
                ),
            ));
        }
        if public_key_hex == self.writer.author_pubkey() {
            return Err(crate::sync::store::membership::MembershipOpsError::SelfInvite);
        }
        self.resolved_membership()?;
        let root = self.store_root().clone();
        let protocol_store_id = root.store_root_id.to_string();
        let invite_timestamp = self.database.stamp();
        let (join_info, wrapped_key, validated_chain) = async {
            let storage = self.storage.clone();
            let database = self.database.clone();
            let chain = self.membership.clone();
            let _mutation = database.membership_mutation_permit().await;
            let (plan, mut progress, intent_hash) =
                match database.outbound_membership_mutation().await? {
                Some(row) => {
                    let intent_hash = row.intent_hash;
                    let (pending, progress) = decode_membership_mutation(row)?;
                    let MembershipMutationPlan::Invite(plan) = pending else {
                        return Err(crate::sync::store::membership::InviteError::PendingMutation(
                            "a member removal is pending".to_string(),
                        ));
                    };
                    if !plan.matches_request(
                        &self.writer_pubkey(),
                        public_key_hex,
                        invitee_email,
                        &role,
                        &protocol_store_id,
                    ) {
                        return Err(crate::sync::store::membership::InviteError::PendingMutation(
                            "the pending invitation has different immutable inputs".to_string(),
                        ));
                    }
                    (plan, progress, intent_hash)
                }
                None => {
                    let stream_id = self.select_membership_author_stream(&chain).await?;
                    let invitee_x25519_pk =
                        crate::keys::ed25519_hex_to_x25519_public_key(public_key_hex)?;
                    let authorized_keyring = self
                        .open_keyring_or_for_membership(&chain, encryption)
                        .await?;
                    let signed = self
                        .writer
                        .seal_keyring_for_member(
                            protocol_store_id.clone(),
                            public_key_hex.to_string(),
                            invitee_x25519_pk,
                            authorized_keyring,
                        )
                        .await?;
                    let wrapped_key = self.prepare_wrapped_key(public_key_hex, signed).await?;
                    let entry = self.writer.sign_set_member(
                        &chain,
                        stream_id,
                        public_key_hex.to_string(),
                        invitee_email.map(str::to_string),
                        role.clone(),
                        wrapped_key.reference.clone(),
                        invite_timestamp.clone(),
                    )?;
                    let publication = self
                        .prepare_membership_publication(&chain, entry)
                        .await?;
                    let plan = InviteMutationPlan {
                        publication,
                        invitee_pubkey: public_key_hex.to_string(),
                        invitee_email: invitee_email.map(str::to_string),
                        role,
                        desired_access: crate::storage::cloud::CloudAccessState::Present {
                            member_pubkey: public_key_hex.to_string(),
                            provider_account_email: invitee_email.map(str::to_string),
                        },
                        wrapped_key,
                    };
                    let encoded = MembershipMutationPlan::Invite(plan.clone()).encode()?;
                    let progress = MembershipMutationProgress::Pending;
                    let intent_hash = database
                        .stage_membership_mutation(
                            encoded,
                            progress.encode()?,
                            None,
                        )
                        .await?;
                    (plan, progress, intent_hash)
                }
                };
        plan.publication.validate()?;
        let mut validated_chain = chain.with_exact_entry(&plan.publication.entry)?;
        let author = self
            .verify_membership_publication_author(&plan.publication)
            .await?;
        let wrapped = plan.wrapped_key.validate()?;
        let authority_matches = matches!(
            &plan.publication.entry.change,
            crate::protocol::membership::MembershipChange::SetMember { wrapped_key, .. }
                if wrapped_key == &plan.wrapped_key.reference
        );
        if !authority_matches
            || wrapped.author_pubkey != plan.publication.entry.author_pubkey
            || wrapped
                .verify_and_unwrap(
                    &plan.publication.entry.store_id,
                    &plan.invitee_pubkey,
                    std::iter::once(plan.publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "planned invitation wrap is not bound to its exact entry, recipient, and author"
                        .to_string(),
                ),
            );
        }
        let outcome = storage
            .set_member_access(plan.desired_access.clone())
            .await?;
        let crate::storage::cloud::CloudAccessOutcome::Present(observed_join_info) = outcome else {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "provider returned absent outcome for present access request".to_string(),
                ),
            );
        };
        let persistence = self.membership_mutation_persistence(intent_hash);
        let join_info = match progress {
            MembershipMutationProgress::Pending => {
                progress = MembershipMutationProgress::InviteGranted {
                    join_info: observed_join_info.clone(),
                };
                persistence.record_progress(&progress).await?;
                observed_join_info
            }
            MembershipMutationProgress::InviteGranted { join_info } => {
                if join_info != observed_join_info {
                    return Err(
                        crate::sync::store::membership::InviteError::InvalidDurableMutation(
                            "provider returned different join information while verifying persisted access"
                                .to_string(),
                        ),
                    );
                }
                join_info
            }
            MembershipMutationProgress::RevokeAccessRemoved
            | MembershipMutationProgress::RevokeCandidateNonactivating { .. }
            | MembershipMutationProgress::ResolutionCandidateNonactivating { .. }
            | MembershipMutationProgress::RevokeActivated { .. }
            | MembershipMutationProgress::ResolutionActivated { .. } => {
                return Err(
                    crate::sync::store::membership::InviteError::InvalidDurableMutation(
                        "invitation carries member-removal progress".to_string(),
                    ),
                );
            }
        };
        storage
            .as_ref()
            .create_protocol_object(&plan.wrapped_key.object)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        storage
            .as_ref()
            .create_protocol_object(&plan.publication.entry_object)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        self.membership_objects()
            .load_entry(&plan.publication.entry_ref)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        storage
            .as_ref()
            .create_protocol_object(&plan.publication.head_object)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        self.membership_objects()
            .load_head_for_registration(&plan.publication.head_ref, &author)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        validated_chain.activate_head_ref(plan.publication.head_ref.clone())?;
        persistence.complete().await?;
        let wrapped_key = plan.wrapped_key.reference;
        Ok::<_, crate::sync::store::membership::InviteError>((
            join_info,
            wrapped_key,
            validated_chain,
        ))
        }
        .await?;
        self.membership = validated_chain;
        let owner_pubkey = self
            .membership
            .founder_pubkey()
            .ok_or(crate::sync::store::membership::MembershipOpsError::ChainHasNoFounder)?
            .to_string();
        if self.protocol_root().descriptor.store_root_id() != root.store_root_id
            || self.protocol_root().descriptor.founder_pubkey != owner_pubkey
        {
            return Err(crate::sync::store::membership::MembershipOpsError::Chain(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "Store protocol root differs from the invite authority".to_string(),
                ),
            ));
        }
        Ok(crate::joining::InviteCode {
            v: crate::joining::INVITE_CODE_VERSION,
            store_id: store_id.to_string(),
            store_name: store_name.to_string(),
            join_info,
            owner_pubkey,
            wrapped_key,
            store_root: root,
            membership_floor: crate::joining::MembershipFloor(self.membership.head_refs().to_vec()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &mut self,
        public_key_hex: &str,
        current_encryption: &crate::encryption::EncryptionService,
        security: &crate::store_security::StoreSecurity,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let timestamp = self.database.stamp();
        let new_key = self
            .revoke_member_without_local_adoption(
                public_key_hex,
                &timestamp,
                current_encryption,
                pending_rotation,
            )
            .await?;
        let generation = new_key.current_generation();
        let fingerprint = security
            .adopt_key_rotation(cipher, &new_key)
        .map_err(|source| {
            crate::sync::store::membership::MembershipOpsError::RotationCommittedAdoptionFailed {
                source,
            }
        })?;
        self.complete_revoke_rotation_adoption(pending_rotation, generation)
            .await?;
        Ok(fingerprint)
    }

    async fn revoke_member_without_local_adoption(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &crate::encryption::EncryptionService,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
    ) -> Result<
        crate::encryption::EncryptionService,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let mut membership = self.resolved_membership()?.clone();
        let store_id = self.store_root().store_root_id.to_string();
        let new_key = membership_mutation::AuthorizedMembershipRevocation::begin(
            self,
            &mut membership,
            public_key_hex,
            &store_id,
            timestamp,
            current_encryption,
            pending_rotation,
        )
        .await
        .execute()
        .await?;
        self.membership = membership;
        Ok(new_key)
    }

    #[cfg(test)]
    pub(crate) async fn revoke_member_without_local_adoption_for_test(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &crate::encryption::EncryptionService,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
    ) -> Result<
        crate::encryption::EncryptionService,
        crate::sync::store::membership::MembershipOpsError,
    > {
        self.revoke_member_without_local_adoption(
            public_key_hex,
            timestamp,
            current_encryption,
            pending_rotation,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn complete_revoke_rotation_adoption_for_test(
        &self,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::InviteError> {
        self.complete_revoke_rotation_adoption(pending_rotation, adopted_generation)
            .await
    }

    async fn complete_revoke_rotation_adoption(
        &self,
        pending_rotation: &dyn crate::storage::CloudRotationAccess,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::InviteError> {
        let _mutation = self.database.membership_mutation_permit().await;
        let row = self
            .database
            .outbound_membership_mutation()
            .await?
            .ok_or_else(|| {
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "activated removal journal is absent during key adoption".to_string(),
                )
            })?;
        let intent_hash =
            membership_mutation::validate_revoke_rotation_adoption(row, adopted_generation)?;
        let gate = self
            .database
            .complete_local_rotation_adoption(intent_hash, adopted_generation)
            .await?;
        pending_rotation.install_durable_gate(gate);
        Ok(())
    }

    async fn build_resolution_mutation(
        &mut self,
        chain: &MembershipChain,
        conflict_hash: store_commit::ObjectHash,
        selection: membership::MembershipConflictSelection,
        created_at: &str,
    ) -> Result<ResolveMutationPlan, InviteError> {
        let base = self
            .prepare_conflict_resolution_plan(chain.head_refs())
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let chain = base.membership().clone();
        let membership::MembershipStatus::Conflict(conflict) = chain.status() else {
            return Err(MembershipError::Conflict.into());
        };
        let current = match conflict {
            membership::MembershipConflict::ConcurrentMemberAssignments {
                conflict_hash, ..
            }
            | membership::MembershipConflict::RevocationCycle { conflict_hash, .. } => {
                *conflict_hash
            }
        };
        if current != conflict_hash {
            return Err(MembershipError::InvalidConflictResolution.into());
        }
        let resolver_pubkey = self.writer.author_pubkey();
        let replacement_grant =
            membership::derive_store_resolution_grant(&conflict_hash, &resolver_pubkey);
        let stream_id = base.grant_authorized_stream_id(
            &replacement_grant,
            store_commit::StreamAnchorDomain::StoreMembership,
        );
        let membership_context = ProtocolObjectContext::signed_plaintext(
            base.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let membership_slot = self
            .storage
            .as_ref()
            .allocate_protocol_slot(
                &membership_context,
                &membership_head_slot_prefix(&resolver_pubkey, &replacement_grant, stream_id, 1),
                ".json",
            )
            .await?;
        let recovery_context = ProtocolObjectContext::signed_plaintext(
            base.root().store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let recovery_slot = self
            .storage
            .as_ref()
            .allocate_protocol_slot(
                &recovery_context,
                &store_commit::owner_recovery_semantic_prefix(
                    &resolver_pubkey,
                    replacement_grant.clone(),
                    1,
                ),
                ".json",
            )
            .await?;
        let membership = store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: membership_slot,
        };
        let recovery = store_commit::GrantStreamAnchor::OwnerRecovery {
            first_slot: recovery_slot,
        };
        let resolution = base.sign_conflict_resolution(
            &chain,
            selection,
            replacement_grant,
            membership,
            recovery,
        )?;
        let resolution_bytes = serde_json::to_vec(&resolution).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize membership resolution: {error}"))
        })?;
        let resolution_context = ProtocolObjectContext::signed_plaintext(
            base.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipResolution,
        );
        let resolution_hash = resolution.resolution_hash();
        let resolution_prefix = store_commit::membership_resolution_semantic_prefix(
            conflict_hash,
            &resolver_pubkey,
            resolution_hash,
        );
        let resolution_slot = self
            .storage
            .as_ref()
            .allocate_protocol_slot(&resolution_context, &resolution_prefix, ".json")
            .await?;
        let resolution_object = self.storage.as_ref().prepare_protocol_object(
            &resolution_context,
            resolution_slot,
            &resolution_prefix,
            resolution_bytes,
        )?;
        let reference = resolution.resolution_ref(resolution_object.reference().clone());
        let mut resolved_chain = chain.clone();
        resolved_chain.apply_resolutions(
            base.root().store_root_hash,
            &[(reference.clone(), resolution.clone())],
        )?;
        let entry = base.sign_conflict_resolution_activation(
            &resolved_chain,
            stream_id,
            reference.clone(),
            &resolution,
            created_at.to_string(),
        )?;
        let transition = self
            .prepare_membership_transition(&resolved_chain, entry)
            .await?;
        let operation_plan = base
            .finish(&resolved_chain, &reference)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let mut stream_activations = vec![
            store_commit::StreamActivation::grant_authorized(
                resolution.store_root_hash,
                resolution.replacement_acceptance.owner_registration.clone(),
                resolution.replacement_grant.clone(),
                resolution.replacement_acceptance.membership.clone(),
            ),
            store_commit::StreamActivation::grant_authorized(
                resolution.store_root_hash,
                resolution.replacement_acceptance.owner_registration.clone(),
                resolution.replacement_grant.clone(),
                resolution.replacement_acceptance.recovery.clone(),
            ),
        ];
        stream_activations.sort();
        let mut candidate = self
            .prepare_candidate(
                operation_plan,
                operations::StoreOperationBatch::MergeMembershipActivation {
                    transition: transition.transition.clone(),
                    stream_activations,
                },
            )
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let publication = self
            .finish_membership_transition(
                transition.clone(),
                membership::MembershipHeadActivation::StoreCommit {
                    commit: candidate.reference.clone(),
                },
            )
            .await?;
        self.attach_merge_membership_proof(&mut candidate, &publication, Some(&resolution))
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let plan = ResolveMutationPlan {
            resolution,
            reference,
            resolution_object,
            transition: Box::new(transition),
            candidate: Box::new(candidate),
            publication: Box::new(publication),
        };
        plan.validate_closed_shape()?;
        Ok(plan)
    }

    pub(crate) async fn resolve_membership_conflict(
        &mut self,
        choice: &crate::protocol::membership::MembershipConflictChoice,
        created_at: &str,
    ) -> Result<
        crate::protocol::membership::StoreMembershipConflictResolutionRef,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let mut membership = self.membership.clone();
        let valid_choice = match (membership.status(), choice.selection()) {
            (
                crate::protocol::membership::MembershipStatus::Conflict(
                    crate::protocol::membership::MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        conflicting_grants,
                        ..
                    },
                ),
                crate::protocol::membership::MembershipConflictSelection::MemberAssignment {
                    grant,
                },
            ) => conflict_hash == &choice.conflict_hash() && conflicting_grants.contains_key(grant),
            (
                crate::protocol::membership::MembershipStatus::Conflict(
                    crate::protocol::membership::MembershipConflict::RevocationCycle {
                        conflict_hash,
                        maximal_valid_branches,
                        ..
                    },
                ),
                crate::protocol::membership::MembershipConflictSelection::RevocationBranch {
                    heads,
                },
            ) => {
                conflict_hash == &choice.conflict_hash()
                    && maximal_valid_branches
                        .iter()
                        .any(|branch| branch.heads == *heads)
            }
            _ => false,
        };
        if !valid_choice {
            return Err(crate::sync::store::membership::InviteError::Membership(
                crate::protocol::membership::MembershipError::InvalidConflictResolution,
            )
            .into());
        }
        let conflict_hash = choice.conflict_hash();
        let selection = choice.selection().clone();
        let database = self.database.clone();
        let signer_pubkey = self.writer.author_pubkey();
        let _mutation = database.membership_mutation_permit().await;
        let (mut plan, progress, intent_hash) = match database
            .outbound_membership_mutation()
            .await
            .map_err(InviteError::from)?
        {
            Some(row) => {
                let intent_hash = row.intent_hash;
                let (pending, progress) = decode_membership_mutation(row)?;
                let MembershipMutationPlan::Resolve(plan) = pending else {
                    return Err(InviteError::PendingMutation(
                        "another membership mutation is pending".to_string(),
                    )
                    .into());
                };
                if plan.resolution.conflict_hash != conflict_hash
                    || plan.resolution.resolver_pubkey != signer_pubkey
                    || plan.resolution.selection != selection
                {
                    return Err(InviteError::PendingMutation(
                        "the pending resolution has different immutable inputs".to_string(),
                    )
                    .into());
                }
                (plan, progress, intent_hash)
            }
            None => {
                let plan = self
                    .build_resolution_mutation(&membership, conflict_hash, selection, created_at)
                    .await?;
                let bytes = MembershipMutationPlan::Resolve(plan.clone()).encode()?;
                let progress = MembershipMutationProgress::Pending;
                let intent_hash = database
                    .stage_membership_candidate_mutation(
                        bytes,
                        progress.encode()?,
                        plan.remote_objects()?,
                        None,
                    )
                    .await
                    .map_err(InviteError::from)?;
                (plan, progress, intent_hash)
            }
        };
        let mut persistence = self.membership_mutation_persistence(intent_hash);
        plan.validate_closed_shape()?;
        if let MembershipMutationProgress::ResolutionCandidateNonactivating { nonactivation } =
            &progress
        {
            if nonactivation
                .reference()
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
                != plan.candidate.reference
            {
                return Err(InviteError::InvalidDurableMutation(
                    "resolution nonactivation names another candidate".to_string(),
                )
                .into());
            }
            persistence.finish_nonactivating_resolution(&plan).await?;
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution candidate did not activate".to_string(),
            )
            .into());
        }
        if let MembershipMutationProgress::ResolutionActivated { candidate } = &progress {
            if candidate != &plan.candidate.reference {
                return Err(InviteError::InvalidDurableMutation(
                    "resolution activation names another candidate".to_string(),
                )
                .into());
            }
            membership
                .apply_resolutions(
                    plan.resolution.store_root_hash,
                    &[(plan.reference.clone(), plan.resolution.clone())],
                )
                .map_err(InviteError::from)?;
            membership
                .add_entry(plan.publication.entry.clone())
                .map_err(InviteError::from)?;
            membership
                .activate_head_ref(plan.publication.head_ref.clone())
                .map_err(InviteError::from)?;
            self.membership = membership;
            return Ok(plan.reference);
        }
        if !matches!(progress, MembershipMutationProgress::Pending) {
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution carries another mutation's progress".to_string(),
            )
            .into());
        }
        membership
            .apply_resolutions(
                plan.resolution.store_root_hash,
                &[(plan.reference.clone(), plan.resolution.clone())],
            )
            .map_err(InviteError::from)?;
        membership
            .add_entry(plan.publication.entry.clone())
            .map_err(InviteError::from)?;
        let remotes = plan.remote_objects()?;
        self.storage
            .as_ref()
            .create_protocol_object(&plan.resolution_object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        self.membership_objects()
            .load_resolution(&plan.reference)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        persistence
            .mark_remote_object_uploaded(exact_owned_remote(&remotes, &plan.reference.object)?)
            .await?;
        self.publish_membership_authority(&plan.transition, &[])
            .await?;
        persistence
            .mark_remote_object_uploaded(exact_owned_remote(
                &remotes,
                &plan.transition.entry_ref.object,
            )?)
            .await?;
        self.upload_commit(&plan.candidate)
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        persistence
            .mark_remote_object_uploaded(exact_owned_remote(
                &remotes,
                &plan.candidate.reference.object,
            )?)
            .await?;
        loop {
            let previous = plan.candidate.as_ref().clone();
            let current_remotes = plan.remote_objects()?;
            let outcome = self
                .publish_membership_activation(
                    &plan.transition,
                    &plan.publication,
                    plan.candidate.clone(),
                    operations::StoreMembershipJournalCompletion::Mutation {
                        intent_hash: persistence.intent_hash(),
                        progress_bytes: MembershipMutationProgress::ResolutionActivated {
                            candidate: plan.candidate.reference.clone(),
                        }
                        .encode()?,
                        remote_objects: current_remotes.clone(),
                    },
                )
                .await?;
            match outcome {
                operations::StoreOperationPublicationOutcome::Activated(reference)
                    if reference == plan.candidate.reference =>
                {
                    membership
                        .activate_head_ref(plan.publication.head_ref.clone())
                        .map_err(InviteError::from)?;
                    self.membership = membership;
                    return Ok(plan.reference);
                }
                operations::StoreOperationPublicationOutcome::RepreparedCandidate(replacement)
                    if replacement.reference == plan.candidate.reference =>
                {
                    let previous_remotes = plan.remote_objects()?;
                    let previous_head = previous.head_ref();
                    plan.candidate = replacement;
                    let replacement_remotes = plan.remote_objects()?;
                    let replacement_head = plan.candidate.head_ref();
                    let bytes = MembershipMutationPlan::Resolve(plan.clone()).encode()?;
                    persistence
                        .adopt_candidate_head(
                            bytes,
                            exact_owned_remote(&previous_remotes, &previous_head.object)?,
                            exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                            None,
                        )
                        .await?;
                }
                operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                    candidate,
                    nonactivation,
                } if candidate.as_ref() == plan.candidate.as_ref() => {
                    persistence
                        .begin_nonactivating_resolution(&plan, *nonactivation)
                        .await?;
                    return Err(InviteError::InvalidDurableMutation(
                        "membership resolution candidate did not activate".to_string(),
                    )
                    .into());
                }
                _ => {
                    return Err(InviteError::InvalidDurableMutation(
                        "membership resolution returned an inapplicable publication outcome"
                            .to_string(),
                    )
                    .into())
                }
            }
        }
    }

    pub(crate) fn circles(&mut self) -> super::AuthorizedCircleWriter<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let store_dir = self.store_dir;
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        let local_writer = Arc::clone(&self.writer);
        super::AuthorizedCircleWriter::from_parts(
            self,
            database,
            storage,
            store_dir,
            root,
            membership,
            local_writer,
        )
    }

    pub(crate) fn circle_history(&mut self) -> super::circles::VerifiedCircleHistory<'_, 'storage> {
        self.history.circles()
    }

    pub(crate) fn device_join_history(
        &mut self,
    ) -> super::device_join::history::DeviceJoinHistory<'_, 'storage> {
        self.history.device_join()
    }

    pub(crate) fn device_exclusion_history(
        &mut self,
    ) -> super::device_exclusion::DeviceExclusionHistory<'_, 'storage> {
        self.history.device_exclusion()
    }

    pub(crate) fn device_exclusion(
        &mut self,
    ) -> super::device_exclusion::AuthorizedDeviceExclusion<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        super::device_exclusion::AuthorizedDeviceExclusion::new(self, database, storage)
    }

    pub(crate) fn join_operation(&mut self) -> super::device_join::AuthorizedJoin<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let root = self.store_root().clone();
        let protocol_root = self.protocol_root().clone();
        let verified_root = self.history.verified_root_object().clone();
        let membership = self.membership.clone();
        let local_writer = Arc::clone(&self.writer);
        super::device_join::AuthorizedJoin::from_parts(
            self,
            database,
            storage,
            root,
            protocol_root,
            verified_root,
            membership,
            local_writer,
        )
    }

    pub(crate) fn provider_administrator_join(
        &mut self,
    ) -> Result<
        super::device_join::AuthorizedProviderAdministratorJoin<'_, 'storage>,
        super::device_join::DeviceJoinError,
    > {
        self.join_operation().into_provider_administrator()
    }

    async fn membership_mutation_permit(&self) -> crate::database::store::MembershipMutationPermit {
        self.database.membership_mutation_permit().await
    }

    async fn outbound_membership_mutation(
        &self,
    ) -> Result<Option<crate::database::DurableMembershipMutation>, InviteError> {
        self.database
            .outbound_membership_mutation()
            .await
            .map_err(InviteError::from)
    }

    async fn stage_membership_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Option<Vec<crate::protocol::remote_object::RemoteObjectRecord>>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<crate::protocol::store_commit::ObjectHash, InviteError> {
        match remote_objects {
            Some(remote_objects) => self
                .database
                .stage_membership_candidate_mutation(
                    plan_bytes,
                    progress_bytes,
                    remote_objects,
                    pending_rotation_generation,
                )
                .await
                .map_err(InviteError::from),
            None => self
                .database
                .stage_membership_mutation(plan_bytes, progress_bytes, pending_rotation_generation)
                .await
                .map_err(InviteError::from),
        }
    }

    fn membership_mutation_persistence(
        &self,
        intent_hash: crate::protocol::store_commit::ObjectHash,
    ) -> MutationPersistence {
        MutationPersistence::new(
            self.database.clone(),
            std::sync::Arc::clone(self.storage),
            intent_hash,
        )
    }

    fn writer_pubkey(&self) -> String {
        self.writer.author_pubkey()
    }

    pub(crate) fn local_author_pubkey(&self) -> String {
        self.writer.author_pubkey()
    }

    pub(crate) fn is_local_registration(
        &self,
        registration: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> bool {
        self.writer.is_authored_by_registration(registration)
    }

    pub(crate) fn is_current_owner(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> bool {
        self.writer.is_current_owner(membership)
    }

    pub(crate) fn matches_local_author(
        &self,
        registration: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        author_pubkey: &str,
    ) -> bool {
        self.writer.matches_author(registration, author_pubkey)
    }

    pub(crate) fn grant_authorized_stream_id(
        &self,
        grant: &crate::protocol::membership::MembershipGrantId,
        domain: crate::protocol::store_commit::StreamAnchorDomain,
    ) -> crate::protocol::membership::AuthorStreamId {
        self.writer
            .grant_authorized_stream_id(self.store_root().store_root_hash, grant, domain)
    }

    pub(crate) fn sign_owner_promotion_acceptance(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
        activation: crate::protocol::store_commit::OwnerPromotionRequestActivation,
        anchors: crate::protocol::store_commit::OwnerPromotionAnchors,
    ) -> Result<
        crate::protocol::store_commit::OwnerPromotionAcceptance,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        self.writer
            .sign_owner_promotion_acceptance(request, activation, anchors)
    }

    pub(crate) fn seal_local_keyring(
        &self,
        store_id: &str,
        recipient: &str,
        recipient_key: &[u8; crate::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: &crate::encryption::EncryptionService,
    ) -> Result<
        crate::protocol::wrapped_store_key::WrappedStoreKey,
        crate::encryption::EncryptionError,
    > {
        self.writer
            .seal_keyring(store_id, recipient, recipient_key, keyring)
    }

    pub(crate) fn sign_finalize_owner_promotion(
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
        self.writer.sign_finalize_owner_promotion(
            membership,
            root,
            candidate,
            acceptance,
            wrapped_key,
            timestamp,
        )
    }

    async fn prepare_replacement_wrapped_key(
        &self,
        store_id: &str,
        recipient: &str,
        recipient_key: &[u8; crate::keys::CURVE25519_PUBLICKEYBYTES],
        keyring: &crate::encryption::EncryptionService,
    ) -> Result<PreparedWrappedStoreKey, InviteError> {
        let wrapped = self
            .writer
            .seal_keyring(store_id, recipient, recipient_key, keyring)
            .map_err(|error| InviteError::Crypto(format!("serialize rotated keyring: {error}")))?;
        self.prepare_wrapped_key(recipient, wrapped)
            .await
            .map_err(InviteError::from)
    }

    fn sign_owner_barrier_removal(
        &self,
        chain: &MembershipChain,
        stream_id: membership::AuthorStreamId,
        revokee_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
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

    fn sign_direct_removal(
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

    fn attach_membership_proof(
        &self,
        candidate: &mut operations::PreparedStoreOperationCommit,
        publication: &PreparedMembershipPublication,
    ) -> Result<(), InviteError> {
        self.attach_merge_membership_proof(candidate, publication, None)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
    }

    async fn set_membership_access(
        &self,
        state: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, InviteError> {
        self.storage
            .set_member_access(state)
            .await
            .map_err(InviteError::from)
    }

    async fn publish_direct_membership_authority(
        &mut self,
        wraps: &[ReplacementWrappedKey],
        publication: &PreparedMembershipPublication,
    ) -> Result<(), InviteError> {
        for wrapped in wraps {
            self.storage
                .as_ref()
                .create_protocol_object(&wrapped.prepared.object)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
            load_wrapped_store_key(
                self.storage.as_ref(),
                self.store_root().store_root_hash,
                &wrapped.prepared.reference,
            )
            .await?;
        }
        self.storage
            .as_ref()
            .create_protocol_object(&publication.entry_object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        self.membership_objects()
            .load_entry(&publication.entry_ref)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        Ok(())
    }

    async fn publish_direct_membership_head(
        &mut self,
        publication: &PreparedMembershipPublication,
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), InviteError> {
        self.storage
            .as_ref()
            .create_protocol_object(&publication.head_object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        self.membership_objects()
            .load_head_for_registration(&publication.head_ref, author)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        Ok(())
    }

    pub(crate) async fn prepare_membership_transition(
        &mut self,
        chain: &MembershipChain,
        entry: MembershipEntry,
    ) -> Result<PreparedMembershipTransition, InviteError> {
        let root = self.store_root().clone();
        let storage = self.storage.as_ref();
        let (entry_object, entry_ref) =
            store_objects::prepare_membership_entry(storage, root.store_root_hash, &entry)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
        let coord = entry.coord();
        let predecessor = chain
            .head_ref_for_stream(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
            )
            .cloned();
        let current_slot = match predecessor.as_ref() {
            Some(reference) => {
                let loaded = self
                    .writer
                    .load_membership_head(self.membership_objects(), reference)
                    .await
                    .map_err(|error| InviteError::Crypto(error.to_string()))?;
                loaded.value.body.successor.next_slot.clone()
            }
            None => match chain.membership_anchor(&coord.author_owner_grant) {
                Some(store_commit::GrantStreamAnchor::StoreMembership { first_slot }) => {
                    first_slot.clone()
                }
                Some(
                    store_commit::GrantStreamAnchor::OwnerRecovery { .. }
                    | store_commit::GrantStreamAnchor::CircleControl { .. }
                    | store_commit::GrantStreamAnchor::CircleRoster { .. }
                    | store_commit::GrantStreamAnchor::CircleMetadata { .. },
                ) => {
                    return Err(InviteError::InvalidDurableMutation(format!(
                        "Owner grant {} uses another domain's anchor as its membership stream",
                        coord.author_owner_grant
                    )));
                }
                None => {
                    return Err(InviteError::InvalidDurableMutation(format!(
                        "Owner grant {} has no activated membership stream anchor",
                        coord.author_owner_grant
                    )));
                }
            },
        };
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
            InviteError::InvalidDurableMutation("membership head sequence overflow".to_string())
        })?;
        let next_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            next_sequence,
        );
        let next_slot = storage
            .allocate_protocol_slot(&context, &next_prefix, ".json")
            .await?;
        let anchor = chain
            .membership_anchor(&coord.author_owner_grant)
            .ok_or_else(|| {
                InviteError::InvalidDurableMutation(format!(
                    "Owner grant {} has no activated membership stream anchor",
                    coord.author_owner_grant
                ))
            })?;
        let transition = self.writer.build_membership_transition(
            root.store_root_hash,
            &entry,
            entry_ref.clone(),
            predecessor,
            anchor.clone(),
            next_slot,
            current_slot,
        )?;
        Ok(PreparedMembershipTransition {
            entry,
            entry_ref,
            entry_object,
            transition,
        })
    }

    pub(crate) async fn prepare_membership_publication(
        &mut self,
        chain: &MembershipChain,
        entry: MembershipEntry,
    ) -> Result<PreparedMembershipPublication, InviteError> {
        let prepared = self.prepare_membership_transition(chain, entry).await?;
        self.finish_membership_transition(prepared, membership::MembershipHeadActivation::Direct)
            .await
    }

    pub(crate) async fn finish_membership_transition(
        &mut self,
        prepared: PreparedMembershipTransition,
        activation: membership::MembershipHeadActivation,
    ) -> Result<PreparedMembershipPublication, InviteError> {
        let root = self.store_root().clone();
        let head =
            self.writer
                .sign_membership_head(&prepared.entry, &prepared.transition, activation)?;
        let coord = prepared.entry.coord();
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let head_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        );
        let head_bytes = serde_json::to_vec(&head).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize membership head: {error}"))
        })?;
        let head_object = self.storage.as_ref().prepare_protocol_object(
            &context,
            prepared.transition.head_slot.clone(),
            &head_prefix,
            head_bytes,
        )?;
        let head_ref = MembershipHeadRef {
            coord,
            head_hash: head.head_hash(),
            object: head_object.reference().clone(),
        };
        let publication = PreparedMembershipPublication {
            entry: prepared.entry,
            entry_ref: prepared.entry_ref,
            entry_object: prepared.entry_object,
            head,
            head_ref,
            head_object,
        };
        publication.validate()?;
        Ok(publication)
    }

    pub(crate) async fn publish_membership_authority(
        &mut self,
        transition: &PreparedMembershipTransition,
        wraps: &[PreparedWrappedStoreKey],
    ) -> Result<(), InviteError> {
        transition.validate()?;
        let expected_wraps: Vec<&WrappedStoreKeyRef> = match &transition.entry.change {
            MembershipChange::SetMember { wrapped_key, .. } => vec![wrapped_key],
            MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.iter().collect(),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => Vec::new(),
        };
        if expected_wraps.len() != wraps.len()
            || expected_wraps
                .iter()
                .zip(wraps)
                .any(|(expected, prepared)| **expected != prepared.reference)
        {
            return Err(InviteError::InvalidDurableMutation(
                "prepared Merge membership wraps differ from their exact transition".to_string(),
            ));
        }
        for prepared in wraps {
            prepared.validate()?;
            self.storage
                .as_ref()
                .create_protocol_object(&prepared.object)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
            load_wrapped_store_key(
                self.storage.as_ref(),
                self.store_root().store_root_hash,
                &prepared.reference,
            )
            .await?;
        }
        self.storage
            .as_ref()
            .create_protocol_object(&transition.entry_object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        self.membership_objects()
            .load_entry(&transition.entry_ref)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        Ok(())
    }

    pub(crate) async fn publish_membership_activation(
        &mut self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        candidate: Box<operations::PreparedStoreOperationCommit>,
        completion: operations::StoreMembershipJournalCompletion,
    ) -> Result<operations::StoreOperationPublicationOutcome, InviteError> {
        transition.validate()?;
        publication.validate()?;
        candidate
            .validate_closed_shape()
            .map_err(InviteError::InvalidDurableMutation)?;
        if candidate.commit.control()
            != Some(&store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &candidate.reference
            )
            || !self.writer.verify_membership_head(&publication.head)
        {
            return Err(InviteError::InvalidDurableMutation(
                "prepared Merge membership head differs from its exact Store activation"
                    .to_string(),
            ));
        }
        self.storage
            .as_ref()
            .create_protocol_object(&publication.head_object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        self.writer
            .load_membership_head(self.membership_objects(), &publication.head_ref)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        let database = self.database.clone();
        database
            .mark_remote_object_uploaded(
                completion
                    .remote_object(&publication.head_ref.object)
                    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?,
            )
            .await
            .map_err(|error| {
                InviteError::InvalidDurableMutation(format!(
                    "record uploaded Merge membership head: {error}"
                ))
            })?;
        let membership_objects = VerifiedMergeMembershipObjects::verify(
            &candidate.commit,
            &candidate.reference,
            &transition.entry,
            &publication.head,
            publication.head_ref.clone(),
        )?;
        let _authorship = database.author_own_stream().await;
        self.publish_prepared(candidate, Some(membership_objects), Some(completion))
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
    }

    pub(super) async fn reject_excluded_merge_candidate(
        &self,
        candidate: &crate::protocol::store_commit::StoreBatchCommitRef,
        author: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<(), StoreError> {
        if self
            .database
            .author_exclusion_activation_for_candidate(
                self.history.root().clone(),
                candidate.clone(),
                author.clone(),
            )
            .await?
            .is_some()
        {
            return Err(StoreError::AuthorExcluded {
                device_id: author.device_id,
            });
        }
        Ok(())
    }

    pub(crate) async fn prepare_plan(
        &mut self,
    ) -> Result<operations::StoreOperationCommitPlan, StoreError> {
        let root = self.store_root().clone();
        let candidate_membership_heads = self.membership.head_refs().to_vec();
        let author = self.writer.author_pubkey();
        let stream_id = self.announcement_stream_id();
        let base = self.database.local_commit_base(stream_id).await?;
        let previous = base.predecessor;
        let dependencies = crate::protocol::store_commit::CommitFrontier::from_refs(base.frontier)
            .map(|frontier| frontier.commits().clone())
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let seq = operations::next_store_sequence(previous.as_ref())?;
        let coord = crate::protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence: seq,
        };
        let order = crate::protocol::store_commit::StoreCommitOrder {
            seq,
            predecessor: previous,
            dependencies,
        };
        let authorization = self
            .writer
            .authorize_retained_outbound(&self.history, &order, &candidate_membership_heads)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let owner_grant = authorization.membership.active_owner_grant(&author);
        let predecessor = authorization
            .membership
            .write_grant_authority(&author)
            .ok_or_else(|| {
                StoreError::InvalidOutbound(format!(
                    "Merge Store operation author {author} has no active write grant"
                ))
            })?;
        Ok(operations::StoreOperationCommitPlan::new(
            operations::StoreOperationPlanCommon::new(
                base.authorship,
                Arc::clone(&self.writer),
                root,
                coord,
                order,
                authorization.membership_state,
                authorization.device_state_ref,
                crate::protocol::store_commit::StoreOperationMembershipAuthority { predecessor },
                owner_grant,
            ),
            authorization.membership,
            authorization.device_state,
        ))
    }

    pub(super) fn membership_authority(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> Result<crate::protocol::store_commit::StoreOperationMembershipAuthority, StoreError> {
        let writer = self.writer.author_pubkey();
        let predecessor = membership.write_grant_authority(&writer).ok_or_else(|| {
            StoreError::Preparation(crate::sync::store::StorePreparationError::Gate(format!(
                "Store writer {writer} has no active membership grant"
            )))
        })?;
        Ok(crate::protocol::store_commit::StoreOperationMembershipAuthority { predecessor })
    }

    pub(crate) async fn prepare_conflict_resolution_plan(
        &mut self,
        candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<MergeConflictResolutionCommitPlan, StoreError> {
        let root = self.store_root().clone();
        let stream_id = self.announcement_stream_id();
        let base = self.database.local_commit_base(stream_id).await?;
        let previous = base.predecessor;
        let dependencies = crate::protocol::store_commit::CommitFrontier::from_refs(base.frontier)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let seq = operations::next_store_sequence(previous.as_ref())?;
        let coord = crate::protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence: seq,
        };
        let order = crate::protocol::store_commit::StoreCommitOrder {
            seq,
            predecessor: previous,
            dependencies: dependencies.0,
        };
        let authorization = self
            .writer
            .authorize_retained_conflict_resolution(
                &self.history,
                &order,
                candidate_membership_heads,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(MergeConflictResolutionCommitPlan::new(
            base.authorship,
            Arc::clone(&self.writer),
            root,
            coord,
            order,
            authorization,
        ))
    }

    pub(crate) async fn prepare_candidate(
        &mut self,
        plan: operations::StoreOperationCommitPlan,
        batch: operations::StoreOperationBatch,
    ) -> Result<operations::PreparedStoreOperationCommit, StoreError> {
        self.prepare_candidate_borrowed(&plan, batch).await
    }

    pub(crate) async fn activate(
        &mut self,
        plan: operations::StoreOperationCommitPlan,
        batch: operations::StoreOperationBatch,
    ) -> Result<crate::protocol::store_commit::StoreBatchCommitRef, StoreError> {
        let prepared = self.prepare_candidate_borrowed(&plan, batch).await?;
        match self.publish_prepared(Box::new(prepared), None, None).await? {
            operations::StoreOperationPublicationOutcome::Activated(reference) => Ok(reference),
            operations::StoreOperationPublicationOutcome::Nonactivated(reference) => {
                Err(StoreError::InvalidOutbound(format!(
                    "Store operation candidate {} did not activate",
                    reference.commit_hash
                )))
            }
            operations::StoreOperationPublicationOutcome::Reprepared => {
                Err(StoreError::InvalidOutbound(
                    "Store operation was reprepared during immediate activation".to_string(),
                ))
            }
            operations::StoreOperationPublicationOutcome::RepreparedCandidate(_) => {
                Err(StoreError::InvalidOutbound(
                    "Store operation adopted a published head for a candidate composed in this call"
                        .to_string(),
                ))
            }
            operations::StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
                Err(StoreError::ActivationConflict)
            }
        }
    }

    pub(crate) async fn publish_prepared(
        &mut self,
        candidate: Box<operations::PreparedStoreOperationCommit>,
        membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<operations::StoreMembershipJournalCompletion>,
    ) -> Result<operations::StoreOperationPublicationOutcome, StoreError> {
        let retained_operation_objects = candidate
            .commit
            .retained_operation_objects()
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head = candidate.head.clone();
        let prepared_head = candidate.prepared_head.clone();
        let history_summary = candidate.history_summary.clone();
        self.publish(
            operations::PreparedStoreOperationActivation {
                candidate,
                retained_operation_objects,
            },
            head,
            prepared_head,
            history_summary,
            membership_objects,
            membership_completion,
        )
        .await
    }

    async fn publish(
        &mut self,
        mut activation: operations::PreparedStoreOperationActivation,
        head: crate::protocol::store_commit::StoreDeviceHead,
        prepared_head: crate::protocol::objects::PreparedExactObject,
        history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<operations::StoreMembershipJournalCompletion>,
    ) -> Result<operations::StoreOperationPublicationOutcome, StoreError> {
        let database = self.database.clone();
        let root = self.store_root().clone();
        let reference = activation.candidate.reference.clone();
        let verified_commit = self
            .history
            .authenticate_commit_bytes(&reference, &activation.candidate.commit.to_bytes())
            .await?;
        let commit = verified_commit.value().clone();
        let circle_activations = if commit.control().is_some() {
            self.history
                .verify_membership_control(&verified_commit)
                .await
                .map_err(StoreError::InvalidOutbound)?
        } else {
            crate::sync::store::circle_controls::activation::VerifiedCircleActivations::none(
                &commit, &reference,
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        };
        self.upload_commit(&activation.candidate).await?;
        let membership_heads = &commit.membership_state.heads;
        let authorization = self
            .history
            .authorize_retained_outbound(
                &commit.order,
                membership_heads,
                &commit.author_registration,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let device_operations = self
            .history
            .load_local_device_operations(
                &verified_commit,
                &authorization.membership,
                &authorization.device_state_ref,
                authorization.device_state,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let has_tracked_remote_objects =
            !activation.retained_operation_objects.is_empty() || membership_completion.is_some();
        if has_tracked_remote_objects {
            database
                .mark_candidate_commit_uploaded(reference.clone())
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!("record uploaded Store candidate: {error}"))
                })?;
        }
        let head_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = crate::protocol::store_commit::head_slot_prefix(
            &commit.author_registration.device_id.to_string(),
            commit.seq(),
        );
        match self
            .storage
            .as_ref()
            .create_protocol_object(&prepared_head)
            .await
        {
            Ok(()) => {}
            Err(crate::protocol::objects::StorageError::SlotCollision(_)) => {
                return self
                    .resolve_head_collision(
                        activation.candidate,
                        verified_commit,
                        reference,
                        head,
                        prepared_head,
                        head_prefix,
                    )
                    .await;
            }
            Err(error) => {
                return Err(crate::protocol::objects::StoreObjectError::from(error).into());
            }
        }
        let opened_head = self
            .storage
            .as_ref()
            .read_protocol_object(&head_context, prepared_head.reference(), &head_prefix)
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        if opened_head != head.to_bytes() {
            return Err(StoreError::InvalidOutbound(
                "Store operation head exact readback differs from its signed bytes".to_string(),
            ));
        }
        let activation_head = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: prepared_head.reference().clone(),
        };
        let operation_object_ids = if has_tracked_remote_objects {
            database
                .mark_store_head_uploaded(activation_head.clone())
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!("record uploaded Store head: {error}"))
                })?;
            membership_completion.is_none().then(|| {
                std::iter::once(crate::protocol::remote_object::remote_object_id(
                    &reference.object,
                ))
                .chain(
                    activation
                        .retained_operation_objects
                        .iter()
                        .map(crate::protocol::remote_object::remote_object_id),
                )
                .chain(std::iter::once(
                    crate::protocol::remote_object::remote_object_id(prepared_head.reference()),
                ))
                .collect::<Vec<_>>()
            })
        } else {
            None
        };
        if let Some(completion) = &membership_completion {
            let completion_ids = completion
                .object_refs()
                .iter()
                .map(crate::protocol::remote_object::remote_object_id)
                .collect::<std::collections::BTreeSet<_>>();
            if completion_ids.is_empty()
                || !completion_ids.contains(&crate::protocol::remote_object::remote_object_id(
                    &reference.object,
                ))
                || !completion_ids.contains(&crate::protocol::remote_object::remote_object_id(
                    prepared_head.reference(),
                ))
            {
                return Err(StoreError::InvalidOutbound(
                    "membership journal completion does not cover its exact Store candidate"
                        .to_string(),
                ));
            }
        }
        let registrations = activation
            .candidate
            .registration_activation
            .take()
            .into_iter()
            .collect::<Vec<_>>();
        database
            .materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                head,
                activation_head.object,
                history_summary,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )
            .await?;
        Ok(operations::StoreOperationPublicationOutcome::Activated(
            reference,
        ))
    }

    async fn prepare_candidate_borrowed(
        &mut self,
        plan: &operations::StoreOperationCommitPlan,
        batch: operations::StoreOperationBatch,
    ) -> Result<operations::PreparedStoreOperationCommit, StoreError> {
        let storage = self.storage.as_ref();
        let acknowledgement_evidence = match &batch {
            operations::StoreOperationBatch::Acknowledgement {
                reference, value, ..
            } => Some((reference.clone(), value.clone())),
            _ => None,
        };
        let retained_registration_evidence = match &batch {
            operations::StoreOperationBatch::Outcome {
                registration: Some(registration),
                ..
            } => vec![registration.registration().clone()],
            _ => Vec::new(),
        };
        let retained_device_operations = match &batch {
            operations::StoreOperationBatch::DeviceExclusionProposal(proposal) => Some(
                crate::protocol::store_commit::RetainedStoreDeviceOperations::from_sources(
                    vec![proposal.clone()],
                    Vec::new(),
                ),
            ),
            operations::StoreOperationBatch::DeviceExclusionOutcome(outcome) => Some(
                crate::protocol::store_commit::RetainedStoreDeviceOperations::from_sources(
                    Vec::new(),
                    vec![outcome.clone()],
                ),
            ),
            _ => None,
        };
        let (commit, registration_activation) =
            plan.sign_batch(self.database.new_store_write_id(), batch)?;
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let stream_id = plan.coord().stream_id.to_string();
        let prefix = crate::protocol::store_commit::commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id,
            commit.seq(),
            commit.commit_hash(),
        );
        let slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        let prepared = storage
            .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        let verified_commit =
            plan.verify_prepared_commit(&commit.to_bytes(), prepared.reference().clone())?;
        let common = operations::PreparedStoreOperationCommon {
            reference: verified_commit.reference().clone(),
            commit,
            prepared,
            registration_activation,
        };
        let acknowledgement = match acknowledgement_evidence {
            Some((reference, value)) => Some(
                plan.retain_acknowledgement(
                    &self.history,
                    &common.reference,
                    &common.commit,
                    reference,
                    value,
                )
                .await
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            ),
            None => None,
        };
        let merge_history_evidence = super::verified_history::MergeHistorySuccessorEvidence {
            registrations: retained_registration_evidence,
            acknowledgement,
            membership_proof: None,
        };
        let registrations = common
            .registration_activation
            .as_ref()
            .map(|activation| vec![activation.clone()])
            .unwrap_or_default();
        let device_operations = match retained_device_operations {
            Some(retained) => retained
                .verify_for(plan.root(), &common.commit)
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            None => {
                crate::protocol::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
                    &common.commit,
                )
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            }
        };
        let state_after = Box::pin(self.history.derive_local_post_device_state(
            &common.commit,
            plan.predecessor_state().clone(),
            &registrations,
            device_operations,
        ))
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head_context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            common.commit.store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let device_id = plan.device_id().to_string();
        let successor = self
            .history
            .prepare_merge_history_successor(
                &verified_commit,
                plan.membership(),
                None,
                state_after,
                merge_history_evidence,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let next_prefix = crate::protocol::store_commit::head_slot_prefix(
            &device_id,
            operations::successor_store_sequence(common.commit.seq())?,
        );
        let next_slot = storage
            .allocate_protocol_slot(&head_context, &next_prefix, ".json")
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        let head = plan.sign_device_head(
            common.reference.clone(),
            successor.summary.digest(),
            crate::protocol::store_commit::SuccessorLink {
                activation: plan.announcement_activation_id()?,
                predecessor: successor.predecessor_head.map(|reference| reference.object),
                next_slot,
            },
        )?;
        let head_prefix =
            crate::protocol::store_commit::head_slot_prefix(&device_id, common.commit.seq());
        let prepared_head = storage
            .prepare_protocol_object(
                &head_context,
                successor.head_slot,
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        Ok(operations::PreparedStoreOperationCommit {
            common,
            head,
            prepared_head,
            history_summary: successor.summary,
        })
    }

    pub(super) async fn finish_nonactivating_acknowledgement(
        &self,
        acknowledgement: crate::protocol::store_commit::StoreAckRef,
    ) -> Result<(), StoreError> {
        if let Some(target) = self
            .database
            .acknowledgement_cleanup_target(acknowledgement.clone())
            .await?
        {
            self.storage
                .as_ref()
                .delete_protocol_object(&target.object)
                .await
                .map_err(crate::protocol::objects::StoreObjectError::from)?;
            self.database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        self.database
            .complete_nonactivating_acknowledgement(acknowledgement)
            .await?;
        Ok(())
    }

    async fn resolve_head_collision(
        &mut self,
        mut candidate: Box<operations::PreparedStoreOperationCommit>,
        commit: crate::protocol::store_commit::VerifiedStoreBatchCommit,
        reference: crate::protocol::store_commit::StoreBatchCommitRef,
        head: crate::protocol::store_commit::StoreDeviceHead,
        prepared_head: crate::protocol::objects::PreparedExactObject,
        head_prefix: String,
    ) -> Result<operations::StoreOperationPublicationOutcome, StoreError> {
        let database = self.database.clone();
        let observation = self
            .history
            .observe_occupied_merge_head(
                &head,
                &commit,
                prepared_head.reference().slot(),
                &head_prefix,
            )
            .await?;
        if observation.winner().commit == reference {
            let (winner, winner_prepared) = observation.into_head();
            if let Some(acknowledgement) = commit.acknowledgement().cloned() {
                database
                    .adopt_acknowledgement_head(acknowledgement, winner, winner_prepared)
                    .await?;
                return Ok(operations::StoreOperationPublicationOutcome::Reprepared);
            }
            candidate.adopt_merge_head(winner, winner_prepared)?;
            return Ok(
                operations::StoreOperationPublicationOutcome::RepreparedCandidate(candidate),
            );
        }
        let registration = database
            .activated_store_device_registration(commit.author_registration.clone())
            .await?;
        let nonactivation = observation
            .verified_nonactivation(
                crate::protocol::store_commit::StoreBatchCommitDeletionTarget {
                    coord: reference.coord.clone(),
                    object: reference.object.clone(),
                    canonical_signed_bytes: commit.to_bytes(),
                },
                registration.value(),
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let Some(acknowledgement) = commit.acknowledgement().cloned() else {
            return Ok(
                operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                    candidate,
                    nonactivation: Box::new(nonactivation),
                },
            );
        };
        database
            .begin_acknowledgement_nonactivation(acknowledgement.clone(), nonactivation)
            .await?;
        self.finish_nonactivating_acknowledgement(acknowledgement)
            .await?;
        Ok(operations::StoreOperationPublicationOutcome::Nonactivated(
            reference,
        ))
    }

    pub(super) fn local_device_id(&self) -> &crate::protocol::store_commit::StoreDeviceId {
        self.writer.device_id()
    }

    pub(crate) fn announcement_stream_id(&self) -> crate::protocol::membership::AuthorStreamId {
        self.writer
            .announcement_stream_id(self.store_root().store_root_hash)
    }

    pub(crate) async fn latest_local_store_position(
        &self,
    ) -> Result<Option<crate::protocol::store_commit::StoreBatchCommitRef>, crate::database::DbError>
    {
        self.database
            .latest_local_store_position(self.announcement_stream_id())
            .await
    }

    pub(super) async fn drain_prepared_store_writes(&mut self) -> Result<u64, StoreError> {
        let operation = self;
        let database = &operation.database;
        // Each candidate here takes a position on this device's own stream by
        // publishing its head, so this waits its turn behind any operation composing
        // against that same position. Queued writes are the one composer that can
        // lose a position safely — they re-prepare against the winner — but nothing
        // else can, so they must not be the ones to take it out from under an
        // operation that is mid-activation.
        let _authorship = database.author_own_stream().await;
        database.retire_uploaded_blob_spools().await?;
        let Some(first) = database.oldest_prepared_store_write().await? else {
            return Ok(0);
        };
        let database = operation.database.clone();
        let storage = operation.storage.as_ref();
        #[cfg(test)]
        let db = &database;
        let mut published = 0_u64;
        let mut next = Some(first);
        while let Some(batch) = next {
            let root = operation.store_root().clone();
            let write_id = batch.commit.value.write_id.clone();
            database
                .set_write_status(&write_id, crate::WriteStatus::Publishing)
                .await?;
            let attempt = async {
                Box::pin(operation.reject_excluded_merge_candidate(
                    &batch.head.value.commit,
                    &batch.commit.value.author_registration,
                ))
                .await?;
                let store_root_hash = root.store_root_hash;
                let commit = &batch.commit.value;
                if !matches!(
                    commit.body,
                    crate::protocol::store_commit::StoreCommitBody::AbandonCandidates { .. }
                ) {
                    operation.publish_prepared_remote_objects(&write_id).await?;
                    database.retire_uploaded_blob_spools().await?;
                }
                let head = &batch.head.value;
                let stream_id = head.commit.coord.stream_id.to_string();
                let commit_context = ProtocolObjectContext::signed_plaintext(
                    store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                );
                let commit_prefix = commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id,
                    commit.seq(),
                    commit.commit_hash(),
                );
                storage
                    .create_protocol_object(&batch.commit.prepared)
                    .await
                    .map_err(StoreObjectError::from)?;
                let opened_commit = storage
                    .read_protocol_object(&commit_context, &batch.commit.object, &commit_prefix)
                    .await
                    .map_err(StoreObjectError::from)?;
                if opened_commit != batch.commit.bytes {
                    return Err(StoreError::InvalidOutbound(
                        "prepared commit exact readback differs from its signed bytes".to_string(),
                    ));
                }
                database
                    .mark_candidate_commit_uploaded(head.commit.clone())
                    .await?;
                #[cfg(test)]
                db.reach_test_point(
                    crate::database::DatabaseTestPoint::StoreWriteCommitUploaded {
                        write_id: write_id.clone(),
                    },
                )
                .await;
                Box::pin(
                    operation
                        .reject_excluded_merge_candidate(&head.commit, &commit.author_registration),
                )
                .await?;
                let head_prefix = head_slot_prefix(
                    &head.author_registration.device_id.to_string(),
                    commit.seq(),
                );
                if let Err(error) = storage.create_protocol_object(&batch.head.prepared).await {
                    if !matches!(error, StorageError::SlotCollision(_)) {
                        return Err(StoreObjectError::from(error).into());
                    }
                    let observation = operation
                        .observe_occupied_merge_head(
                            head,
                            commit,
                            batch.head.object.slot(),
                            &head_prefix,
                        )
                        .await?;
                    if observation.winner().commit == head.commit {
                        let registration = database
                            .activated_store_device_registration(head.author_registration.clone())
                            .await?;
                        let nonactivations = observation.verified_nonactivations(
                            commit
                                .abandoned_candidates()
                                .iter()
                                .map(|manifest| manifest.candidate.clone()),
                            registration.value(),
                        )?;
                        let (winner, winner_prepared) = observation.into_head();
                        database
                            .adopt_alternate_merge_head(write_id.clone(), winner, winner_prepared)
                            .await?;
                        #[cfg(test)]
                        db.reach_test_point(
                            crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                                write_id: write_id.clone(),
                            },
                        )
                        .await;
                        match database
                            .complete_prepared_store_write(
                                root.clone(),
                                head.commit.clone(),
                                nonactivations,
                            )
                            .await?
                        {
                            crate::database::CompletePreparedStoreWriteOutcome::Published => {}
                            crate::database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                                device_id,
                            } => return Err(StoreError::AuthorExcluded { device_id }),
                        }
                        return Ok::<bool, StoreError>(true);
                    }
                    let registration = database
                        .activated_store_device_registration(head.author_registration.clone())
                        .await?;
                    let nonactivations = observation.verified_nonactivations(
                        std::iter::once(StoreBatchCommitDeletionTarget {
                            coord: head.commit.coord.clone(),
                            object: head.commit.object.clone(),
                            canonical_signed_bytes: commit.to_bytes(),
                        })
                        .chain(
                            commit
                                .abandoned_candidates()
                                .iter()
                                .map(|manifest| manifest.candidate.clone()),
                        ),
                        registration.value(),
                    )?;
                    database
                        .mark_merge_candidate_conflict(write_id.clone(), nonactivations)
                        .await?;
                    return Ok::<bool, StoreError>(false);
                }
                let observation = operation
                    .observe_occupied_merge_head(
                        head,
                        commit,
                        batch.head.object.slot(),
                        &head_prefix,
                    )
                    .await?;
                if observation.winner() != head
                    || observation.winner_prepared().reference() != &batch.head.object
                {
                    return Err(StoreError::InvalidOutbound(
                        "prepared head exact readback differs from its signed bytes".to_string(),
                    ));
                }
                let registration = database
                    .activated_store_device_registration(head.author_registration.clone())
                    .await?;
                let nonactivations = observation.verified_nonactivations(
                    commit
                        .abandoned_candidates()
                        .iter()
                        .map(|manifest| manifest.candidate.clone()),
                    registration.value(),
                )?;
                database
                    .mark_store_head_uploaded(StoreDeviceHeadRef {
                        head_hash: head.head_hash(),
                        object: batch.head.object.clone(),
                    })
                    .await?;
                #[cfg(test)]
                db.reach_test_point(crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                    write_id: write_id.clone(),
                })
                .await;
                match database
                    .complete_prepared_store_write(root, head.commit.clone(), nonactivations)
                    .await?
                {
                    crate::database::CompletePreparedStoreWriteOutcome::Published => {}
                    crate::database::CompletePreparedStoreWriteOutcome::AuthorExcluded {
                        device_id,
                    } => return Err(StoreError::AuthorExcluded { device_id }),
                }
                Ok::<bool, StoreError>(true)
            }
            .await;
            match attempt {
                Ok(false) => return Ok(published),
                Ok(true) => {}
                Err(error) => {
                    if let Some(block) = error.write_block() {
                        database.block_write_if_unresolved(&write_id, block).await?;
                    }
                    return Err(error);
                }
            }
            published = published
                .checked_add(1)
                .ok_or_else(|| StoreError::Database("publish count exceeded u64".into()))?;
            next = database.oldest_prepared_store_write().await?;
        }
        Ok(published)
    }

    #[cfg(test)]
    pub(crate) async fn prepare_pending_store_write(&mut self) -> Result<bool, StoreError> {
        self.prepare_store_write().await
    }

    #[cfg(test)]
    pub(crate) async fn drain_store_writes(&mut self) -> Result<u64, StoreError> {
        self.drain_prepared_store_writes().await
    }

    pub(crate) async fn publish_pending_store_writes(&mut self) -> Result<u64, SyncCycleFailure> {
        let mut published = 0_u64;
        loop {
            if !self
                .prepare_store_write()
                .await
                .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))?
            {
                return Ok(published);
            }
            let drained = self
                .drain_prepared_store_writes()
                .await
                .map_err(|error| SyncCycleFailure::operation("publish Store write", error))?;
            published = published.checked_add(drained).ok_or_else(|| {
                SyncCycleFailure::operation(
                    "publish Store write",
                    StoreError::Database("published Store write count exceeded u64".to_string()),
                )
            })?;
        }
    }

    pub(crate) async fn publish_prepared_store_writes(&mut self) -> Result<u64, SyncCycleFailure> {
        self.drain_prepared_store_writes()
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store write", error))
    }

    pub(crate) async fn reclaim_packages(
        &mut self,
    ) -> Result<reclaim::StoreReclaimResult, reclaim::StoreReclaimError> {
        self.reclaim().run().await
    }

    fn reclaim(&mut self) -> reclaim::AuthorizedReclaim<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        reclaim::AuthorizedReclaim::new(self, database, storage, root, membership)
    }

    pub(crate) async fn resume_operations(
        &mut self,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), SyncCycleFailure> {
        self.device_exclusion()
            .resume()
            .await
            .map_err(|error| SyncCycleFailure::operation("resume device exclusion", error))?;
        let routing_key = routing_encryption
            .map(|encryption| {
                crate::protocol::circle::derive_row_routing_key(
                    encryption,
                    self.store_root().store_root_hash,
                )
            })
            .transpose()
            .map_err(|error| {
                SyncCycleFailure::operation("derive Circle operation routing key", error)
            })?;
        self.circles()
            .resume_circle_operations(routing_key.as_ref())
            .await
            .map_err(|error| SyncCycleFailure::operation("resume circle operations", error))
    }

    pub(super) async fn publish_prepared_remote_objects(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<(), StoreError> {
        use crate::protocol::objects::{
            BlobWriteAuthority, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain,
            StoreObjectError,
        };
        use crate::protocol::store_commit::{
            circle_package_semantic_prefix, package_semantic_prefix, ObjectHash,
        };

        let database = &self.database;
        let storage = self.storage.as_ref();
        let store_root_hash = self.store_root().store_root_hash;
        for prepared in database.prepared_remote_objects(write_id).await? {
            let remote = prepared.record;
            let prepared_state = match &remote {
                crate::protocol::remote_object::RemoteObjectRecord::CandidateCommit(record) => {
                    matches!(
                        record.state,
                        crate::protocol::remote_object::CandidateCommitState::Prepared
                    )
                }
                crate::protocol::remote_object::RemoteObjectRecord::CandidateExclusive(record) => {
                    matches!(
                        record.state,
                        crate::protocol::remote_object::CandidateObjectState::Prepared { .. }
                    )
                }
                crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) => {
                    matches!(
                        record.state,
                        crate::protocol::remote_object::OwnedObjectState::Prepared { .. }
                    )
                }
                crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(_) => false,
            };
            match remote.bytes().stored() {
                crate::protocol::remote_object::RemoteStoredRepresentation::Inline {
                    bytes,
                    object,
                } => {
                    let package = crate::protocol::audience_package::AudiencePackage::parse(
                        remote.bytes().canonical_semantic_bytes(),
                    )
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                    let stream_id = package.commit_coord().stream_id.to_string();
                    let sequence = package.commit_coord().sequence;
                    let (context, prefix) = match package.audience() {
                        crate::protocol::audience_package::PackageAudience::Store => (
                            ProtocolObjectContext::store_encrypted(
                                store_root_hash,
                                ProtocolObjectDomain::StorePackage,
                            ),
                            package_semantic_prefix(
                                package.candidate_family(),
                                &stream_id,
                                sequence,
                                ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                            ),
                        ),
                        crate::protocol::audience_package::PackageAudience::Circle {
                            circle_id,
                            control,
                            ..
                        } => {
                            let access = database
                                .circle_publication_context(*circle_id, control.clone())
                                .await?;
                            (
                                access.protocol_context(
                                    store_root_hash,
                                    ProtocolObjectDomain::CirclePackage,
                                ),
                                circle_package_semantic_prefix(
                                    *circle_id,
                                    package.candidate_family(),
                                    &stream_id,
                                    sequence,
                                    ObjectHash::digest(remote.bytes().canonical_semantic_bytes()),
                                ),
                            )
                        }
                    };
                    let prepared = PreparedExactObject::new(object.clone(), bytes.clone())
                        .map_err(StoreObjectError::from)?;
                    if prepared_state {
                        storage
                            .create_protocol_object(&prepared)
                            .await
                            .map_err(StoreObjectError::from)?;
                    }
                    let opened = storage
                        .read_protocol_object(&context, object, &prefix)
                        .await
                        .map_err(StoreObjectError::from)?;
                    if opened != remote.bytes().canonical_semantic_bytes() {
                        return Err(StoreError::InvalidOutbound(format!(
                            "remote package {} exact readback differs from its canonical bytes",
                            remote.object_id()
                        )));
                    }
                }
                crate::protocol::remote_object::RemoteStoredRepresentation::Blob { object } => {
                    let locator = crate::protocol::blob::locator::BlobLocator::parse(
                        remote.bytes().canonical_semantic_bytes(),
                    )
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                    let uploader = locator.uploader().clone();
                    let registration = database
                        .activated_store_device_registration(uploader.clone())
                        .await?;
                    let authority = BlobWriteAuthority::new(&registration);
                    let blob =
                        crate::protocol::blob::locator::StoredBlobRef::new(locator, object.clone())
                            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
                    if prepared_state {
                        let path = prepared.spool_path.as_deref().ok_or_else(|| {
                            StoreError::InvalidOutbound(format!(
                                "prepared blob {} awaiting upload has no local spool",
                                remote.object_id()
                            ))
                        })?;
                        storage
                            .create_blob_object_from_file(
                                &blob,
                                &authority,
                                path,
                                &crate::storage::cloud::no_progress(),
                            )
                            .await
                            .map_err(|source| StoreError::BlobStorage {
                                namespace: blob.locator().namespace().to_string(),
                                id: blob.locator().blob_id().to_string(),
                                source,
                            })?;
                    }
                    storage.verify_blob_object(&blob).await.map_err(|source| {
                        StoreError::BlobStorage {
                            namespace: blob.locator().namespace().to_string(),
                            id: blob.locator().blob_id().to_string(),
                            source,
                        }
                    })?;
                }
                crate::protocol::remote_object::RemoteStoredRepresentation::ExternalExact {
                    ..
                } => {
                    return Err(StoreError::InvalidOutbound(format!(
                        "prepared outbound object {} has no locally stored representation",
                        remote.object_id()
                    )));
                }
            }
            if prepared_state {
                database.mark_remote_object_uploaded(remote).await?;
            }
        }
        Ok(())
    }
}
