use super::*;

mod abandonment;
mod acknowledgements;
mod blob_lifecycle;
pub(crate) mod blob_preparation;
mod membership_mutation;
pub(crate) mod operations;
mod preparation;
mod publication;
pub(crate) mod reclaim;
pub(crate) mod snapshot;
mod store_pull;

#[derive(Clone, Copy)]
pub(super) struct SnapshotHistoryConstruction;

impl SnapshotHistoryConstruction {
    fn authorize_history(self) -> super::history::HistoryConstructionAuthority {
        super::history::HistoryConstructionAuthority::snapshot(self)
    }
}

pub(crate) use acknowledgements::StoreAckError;
pub(super) use membership_mutation::{
    validate_prepared_publication, validate_prepared_transition, PreparedMembershipPublication,
    PreparedMembershipTransition,
};
pub(super) struct LocalStoreWriter<'store> {
    identity: &'store UserKeypair,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
    device_signer: UserKeypair,
}

pub(crate) struct AuthorizedWriterOperation<'storage> {
    database: StoreDatabase,
    history: AuthorizedStoreHistory<'storage>,
    storage: &'storage Arc<dyn SyncStorage>,
    membership: crate::protocol::membership::MembershipChain,
    writer: LocalStoreWriter<'storage>,
}

pub(super) struct MergeConflictResolutionCommitPlan {
    authorship: crate::database::store::OwnStreamAuthorship,
    root: crate::protocol::store_commit::StoreRootRef,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: Box<crate::protocol::store_commit::StoreDeviceRegistration>,
    device_signer: UserKeypair,
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
        root: crate::protocol::store_commit::StoreRootRef,
        registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: crate::protocol::store_commit::StoreDeviceRegistration,
        device_signer: UserKeypair,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        order: crate::protocol::store_commit::StoreCommitOrder,
        authorization: super::history::MergeConflictResolutionAuthorization,
    ) -> Self {
        Self {
            authorship,
            root,
            registration_ref,
            registration: Box::new(registration),
            device_signer,
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

    pub(super) fn registration_ref(
        &self,
    ) -> &crate::protocol::store_commit::StoreDeviceRegistrationRef {
        &self.registration_ref
    }

    pub(super) fn registration(&self) -> &crate::protocol::store_commit::StoreDeviceRegistration {
        &self.registration
    }

    pub(super) fn device_state(&self) -> &crate::protocol::store_commit::StoreDeviceStateRef {
        &self.device_state
    }

    pub(super) fn membership(&self) -> &crate::protocol::membership::MembershipChain {
        &self.membership
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
                record.member_pubkey != self.registration.author_pubkey
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
            self.root,
            self.registration_ref,
            *self.registration,
            self.device_signer,
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
    pub(super) fn from_parts(
        database: StoreDatabase,
        history: AuthorizedStoreHistory<'storage>,
        storage: &'storage Arc<dyn SyncStorage>,
        membership: crate::protocol::membership::MembershipChain,
        writer: LocalStoreWriter<'storage>,
    ) -> Self {
        Self {
            database,
            history,
            storage,
            membership,
            writer,
        }
    }

    fn store_root(&self) -> &crate::protocol::store_commit::StoreRootRef {
        self.history.root()
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
        self.history
            .open_keyring(self.writer.identity, &self.membership)
            .await
    }

    pub(super) async fn open_keyring_for_membership(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> Result<crate::encryption::EncryptionService, super::membership::InviteError> {
        self.history
            .open_keyring(self.writer.identity, membership)
            .await
    }

    pub(super) async fn open_keyring_or_for_membership(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
        initial: &crate::encryption::EncryptionService,
    ) -> Result<crate::encryption::EncryptionService, super::membership::InviteError> {
        self.history
            .open_keyring_or(self.writer.identity, membership, initial)
            .await
    }

    pub(super) async fn prepare_wrapped_key(
        &self,
        recipient: &str,
        value: crate::protocol::wrapped_store_key::WrappedStoreKey,
    ) -> Result<
        crate::protocol::wrapped_store_key::PreparedWrappedStoreKey,
        crate::storage::StorageError,
    > {
        self.history.prepare_wrapped_key(recipient, value).await
    }

    fn require_current_owner(&self, author_pubkey: &str) -> Result<(), String> {
        if self.membership.is_owner_now(author_pubkey) {
            Ok(())
        } else {
            Err(format!("author {author_pubkey} is not a current owner"))
        }
    }

    #[cfg(test)]
    pub(super) fn sign_device_head_for_test(
        &self,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, StoreError> {
        crate::protocol::store_commit::StoreDeviceHead::signed(
            self.store_root().store_root_hash,
            self.writer.registration_ref.clone(),
            commit,
            history_summary,
            successor,
            &self.writer.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(super) fn resign_snapshot_meta_for_test(
        &self,
        meta: crate::protocol::store_commit::SnapshotMeta,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        if meta.store_root_hash != self.store_root().store_root_hash
            || meta.author_registration != self.writer.registration_ref
        {
            return Err(StoreError::InvalidOutbound(
                "snapshot test input belongs to another Store writer".to_string(),
            ));
        }
        crate::protocol::store_commit::SnapshotMeta::signed(
            meta.store_root_hash,
            self.writer.registration_ref.clone(),
            meta.generation,
            meta.predecessor,
            meta.image,
            meta.coverage,
            meta.state,
            meta.history_summary,
            meta.schema_version,
            meta.created_at,
            meta.successor,
            &self.writer.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(super) fn parse_snapshot_meta_for_test(
        &self,
        bytes: &[u8],
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<crate::protocol::store_commit::SnapshotMeta, StoreError> {
        crate::protocol::store_commit::SnapshotMeta::parse_at(
            bytes,
            self.store_root().store_root_hash,
            reference,
            &self.writer.registration,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    #[cfg(test)]
    pub(super) fn local_registration_ref_for_test(
        &self,
    ) -> crate::protocol::store_commit::StoreDeviceRegistrationRef {
        self.writer.registration_ref.clone()
    }

    fn reclaim_history(&mut self) -> super::history::ReclaimHistory<'_, 'storage> {
        self.history.reclaim()
    }

    pub(super) fn owner_promotion(
        &mut self,
    ) -> super::owner_promotion::AuthorizedOwnerPromotion<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        let identity = self.writer.identity.clone();
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        super::owner_promotion::AuthorizedOwnerPromotion::new(
            self,
            database,
            storage,
            root,
            membership,
            identity,
            registration_ref,
            registration,
        )
    }

    pub(super) fn owner_promotion_history(
        &mut self,
    ) -> super::history::OwnerPromotionHistory<'_, 'storage> {
        self.history.owner_promotion()
    }

    pub(crate) async fn refresh_authorization_state(
        &self,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &crate::storage::PendingRotation,
        security: Option<&crate::store_security::StoreSecurity>,
    ) -> Result<(), SyncCycleFailure> {
        let result = async {
            if cipher.snapshot().is_plaintext() {
                tracing::debug!("refresh: plaintext home, nothing to refresh");
                return Ok(());
            }

            let user_keypair = self.writer.identity;
            let recipient = crate::keys::public_key_hex(user_keypair);
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
        hlc: &crate::sync::hlc::Hlc,
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
        if public_key_hex == crate::keys::public_key_hex(self.writer.identity) {
            return Err(crate::sync::store::membership::MembershipOpsError::SelfInvite);
        }
        self.resolved_membership()?;
        let root = self.store_root().clone();
        let protocol_store_id = root.store_root_id.to_string();
        let invite_timestamp = hlc.now().to_string();
        let storage = self.storage.clone();
        let (join_info, wrapped_key) =
            membership_mutation::create_invitation_with_encryption_durable(
                self,
                storage.cloud_home(),
                public_key_hex,
                invitee_email,
                role,
                encryption,
                &protocol_store_id,
                &invite_timestamp,
            )
            .await?;
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
        hlc: &crate::sync::hlc::Hlc,
        public_key_hex: &str,
        current_encryption: &crate::encryption::EncryptionService,
        security: &crate::store_security::StoreSecurity,
        cipher: &dyn crate::storage::CloudCipherAccess,
        pending_rotation: &crate::storage::PendingRotation,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let timestamp = hlc.now().to_string();
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
        let database = self.database.clone();
        membership_mutation::complete_revoke_rotation_adoption(
            &database,
            pending_rotation,
            generation,
        )
        .await?;
        Ok(fingerprint)
    }

    async fn revoke_member_without_local_adoption(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &crate::encryption::EncryptionService,
        pending_rotation: &crate::storage::PendingRotation,
    ) -> Result<
        crate::encryption::EncryptionService,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let mut membership = self.resolved_membership()?.clone();
        let store_id = self.store_root().store_root_id.to_string();
        let storage = self.storage.clone();
        let new_key = membership_mutation::revoke_member_durable(
            self,
            storage.cloud_home(),
            &mut membership,
            public_key_hex,
            &store_id,
            timestamp,
            current_encryption,
            pending_rotation,
        )
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
        pending_rotation: &crate::storage::PendingRotation,
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
        pending_rotation: &crate::storage::PendingRotation,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::InviteError> {
        membership_mutation::complete_revoke_rotation_adoption(
            &self.database,
            pending_rotation,
            adopted_generation,
        )
        .await
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
        let result = membership_mutation::resolve_membership_conflict(
            self,
            &mut membership,
            choice.conflict_hash(),
            choice.selection().clone(),
            created_at,
        )
        .await?;
        self.membership = membership;
        Ok(result)
    }

    pub(crate) fn circles(&mut self) -> super::AuthorizedCircleWriter<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let root = self.store_root().clone();
        let membership = self.membership.clone();
        let identity = self.writer.identity;
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        let device_signer = self.writer.device_signer.clone();
        super::AuthorizedCircleWriter::from_parts(
            self,
            database,
            storage,
            root,
            membership,
            identity,
            registration_ref,
            registration,
            device_signer,
        )
    }

    pub(super) fn circle_history(&mut self) -> super::circles::VerifiedCircleHistory<'_, 'storage> {
        self.history.circles()
    }

    pub(super) fn device_join_history(
        &mut self,
    ) -> super::device_join::history::DeviceJoinHistory<'_, 'storage> {
        self.history.device_join()
    }

    pub(super) fn device_exclusion_history(
        &mut self,
    ) -> super::device_exclusion::DeviceExclusionHistory<'_, 'storage> {
        self.history.device_exclusion()
    }

    pub(super) fn device_exclusion(
        &mut self,
    ) -> super::device_exclusion::AuthorizedDeviceExclusion<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        super::device_exclusion::AuthorizedDeviceExclusion::new(self, database, storage)
    }

    pub(super) fn circle_bootstrap_verifier(
        &self,
    ) -> super::circle_bootstrap::CircleBootstrapVerifier {
        super::circle_bootstrap::CircleBootstrapVerifier::new(self.storage.clone())
    }

    pub(super) fn join_operation(&mut self) -> super::device_join::AuthorizedJoin<'_, 'storage> {
        let database = self.database.clone();
        let storage = Arc::clone(self.storage);
        let root = self.store_root().clone();
        let protocol_root = self.protocol_root().clone();
        let verified_root = self.history.verified_root_object().clone();
        let membership = self.membership.clone();
        let identity = self.writer.identity;
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        let device_signer = self.writer.device_signer.clone();
        super::device_join::AuthorizedJoin::from_parts(
            self,
            database,
            storage,
            root,
            protocol_root,
            verified_root,
            membership,
            identity,
            registration_ref,
            registration,
            device_signer,
        )
    }

    pub(super) fn provider_administrator_join(
        &mut self,
    ) -> Result<
        super::device_join::AuthorizedProviderAdministratorJoin<'_, 'storage>,
        super::device_join::DeviceJoinError,
    > {
        self.join_operation().into_provider_administrator()
    }

    pub(super) async fn prepare_membership_transition(
        &mut self,
        membership: &crate::protocol::membership::MembershipChain,
        entry: crate::protocol::membership::MembershipEntry,
    ) -> Result<PreparedMembershipTransition, crate::sync::store::membership::InviteError> {
        membership_mutation::AuthorizedMembershipPublication::new(self)
            .prepare_transition(membership, entry)
            .await
    }

    pub(super) async fn finish_membership_transition(
        &mut self,
        transition: PreparedMembershipTransition,
        activation: crate::protocol::membership::MembershipHeadActivation,
    ) -> Result<PreparedMembershipPublication, crate::sync::store::membership::InviteError> {
        membership_mutation::AuthorizedMembershipPublication::new(self)
            .finish_transition(transition, activation)
            .await
    }

    pub(super) async fn publish_membership_authority(
        &mut self,
        transition: &PreparedMembershipTransition,
        wraps: &[crate::protocol::wrapped_store_key::PreparedWrappedStoreKey],
    ) -> Result<(), crate::sync::store::membership::InviteError> {
        membership_mutation::AuthorizedMembershipPublication::new(self)
            .publish_authority(transition, wraps)
            .await
    }

    pub(super) async fn publish_membership_activation(
        &mut self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        candidate: Box<operations::PreparedStoreOperationCommit>,
        completion: operations::StoreMembershipJournalCompletion,
    ) -> Result<
        operations::StoreOperationPublicationOutcome,
        crate::sync::store::membership::InviteError,
    > {
        membership_mutation::AuthorizedMembershipPublication::new(self)
            .publish_activation(transition, publication, candidate, completion)
            .await
    }

    async fn reject_excluded_merge_candidate(
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
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        let device_signer = self.writer.device_signer.clone();
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
            .history
            .authorize_retained_outbound(&order, &candidate_membership_heads, &registration_ref)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let owner_grant = authorization
            .membership
            .active_owner_grant(&registration.author_pubkey);
        let predecessor = authorization
            .membership
            .write_grant_authority(&registration.author_pubkey)
            .ok_or_else(|| {
                StoreError::InvalidOutbound(format!(
                    "Merge Store operation author {} has no active write grant",
                    registration.author_pubkey
                ))
            })?;
        Ok(operations::StoreOperationCommitPlan::new(
            operations::StoreOperationPlanCommon::new(
                base.authorship,
                root,
                registration_ref,
                registration,
                device_signer,
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

    fn membership_authority(
        &self,
        membership: &crate::protocol::membership::MembershipChain,
    ) -> Result<crate::protocol::store_commit::StoreOperationMembershipAuthority, StoreError> {
        let writer = crate::keys::public_key_hex(self.writer.identity);
        let predecessor = membership.write_grant_authority(&writer).ok_or_else(|| {
            StoreError::Preparation(crate::sync::store::StorePreparationError::Gate(format!(
                "Store writer {writer} has no active membership grant"
            )))
        })?;
        Ok(crate::protocol::store_commit::StoreOperationMembershipAuthority { predecessor })
    }

    pub(super) async fn prepare_conflict_resolution_plan(
        &mut self,
        candidate_membership_heads: &[crate::protocol::membership::MembershipHeadRef],
    ) -> Result<MergeConflictResolutionCommitPlan, StoreError> {
        let root = self.store_root().clone();
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        let device_signer = self.writer.device_signer.clone();
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
            .history
            .authorize_retained_conflict_resolution(
                &order,
                candidate_membership_heads,
                &registration_ref,
                &registration.author_pubkey,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(MergeConflictResolutionCommitPlan::new(
            base.authorship,
            root,
            registration_ref,
            registration,
            device_signer,
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
        let retained_operation_objects =
            operations::retained_store_operation_objects(&candidate.commit)?;
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
        prepared_head: crate::storage::PreparedExactObject,
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
        operations::upload_commit(self.storage.as_ref(), &activation.candidate).await?;
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
        let head_context = crate::storage::ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            crate::storage::ProtocolObjectDomain::StoreHead,
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
            Err(crate::storage::StorageError::SlotCollision(_)) => {
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
                return Err(crate::storage::StoreObjectError::from(error).into());
            }
        }
        let opened_head = self
            .storage
            .as_ref()
            .read_protocol_object(&head_context, prepared_head.reference(), &head_prefix)
            .await
            .map_err(crate::storage::StoreObjectError::from)?;
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
            .map(|activation| (activation.registration, activation.authority))
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
        let database = self.database.clone();
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
            } => vec![
                crate::protocol::store_commit::RetainedVerifiedRegistration {
                    reference: registration.reference.registration.clone(),
                    value: registration.registration.clone(),
                },
            ],
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
        let (common, verified_commit) = operations::prepare_store_operation_candidate_common(
            &database,
            storage,
            plan.common(),
            batch,
        )
        .await?;
        let acknowledgement = match acknowledgement_evidence {
            Some((reference, value)) => Some(
                self.history
                    .retain_acknowledgement(
                        &common.reference,
                        &common.commit,
                        plan.registration(),
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
            .map(|activation| {
                vec![(
                    activation.registration.clone(),
                    activation.authority.clone(),
                )]
            })
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
        let head_context = crate::storage::ProtocolObjectContext::signed_plaintext(
            common.commit.store_root_hash,
            crate::storage::ProtocolObjectDomain::StoreHead,
        );
        let device_id = plan.registration_ref().device_id.to_string();
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
            .map_err(crate::storage::StoreObjectError::from)?;
        let head = crate::protocol::store_commit::StoreDeviceHead::signed(
            common.commit.store_root_hash,
            plan.registration_ref().clone(),
            common.reference.clone(),
            successor.summary.digest(),
            crate::protocol::store_commit::SuccessorLink {
                activation: plan
                    .registration()
                    .store_announcement_activation(plan.registration_ref())
                    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
                    .activation_id(),
                predecessor: successor.predecessor_head.map(|reference| reference.object),
                next_slot,
            },
            plan.device_signer(),
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head_prefix =
            crate::protocol::store_commit::head_slot_prefix(&device_id, common.commit.seq());
        let prepared_head = storage
            .prepare_protocol_object(
                &head_context,
                successor.head_slot,
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(crate::storage::StoreObjectError::from)?;
        Ok(operations::PreparedStoreOperationCommit {
            common,
            head,
            prepared_head,
            history_summary: successor.summary,
        })
    }

    async fn finish_nonactivating_acknowledgement(
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
                .map_err(crate::storage::StoreObjectError::from)?;
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
        prepared_head: crate::storage::PreparedExactObject,
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
                &registration,
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
        &self.writer.registration.device_id
    }

    pub(super) fn announcement_stream_id(&self) -> crate::protocol::membership::AuthorStreamId {
        crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
            self.store_root().store_root_hash,
            &self.writer.registration_ref,
            crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
        )
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
        publication::drain_store_writes(self).await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_pending_store_write(
        &mut self,
        store_dir: &StoreDir,
    ) -> Result<bool, StoreError> {
        preparation::prepare_store_write(self, store_dir).await
    }

    #[cfg(test)]
    pub(crate) async fn drain_store_writes(&mut self) -> Result<u64, StoreError> {
        self.drain_prepared_store_writes().await
    }

    pub(crate) async fn publish_pending_store_writes(
        &mut self,
        store_dir: &StoreDir,
    ) -> Result<u64, SyncCycleFailure> {
        let mut published = 0_u64;
        loop {
            if !preparation::prepare_store_write(self, store_dir)
                .await
                .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))?
            {
                return Ok(published);
            }
            let drained = publication::drain_store_writes(self)
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
        let identity = self.writer.identity.clone();
        reclaim::AuthorizedReclaim::new(self, database, storage, root, membership, identity)
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
}

impl<'storage> AuthorizedStore<'storage> {
    pub(super) async fn into_writer(
        self,
    ) -> Result<AuthorizedWriterOperation<'storage>, StoreRegistrationError> {
        let AuthorizedStore {
            history,
            storage,
            identity,
            local_device,
            membership,
        } = self;
        let local_device = local_device.ok_or(StoreError::MissingState {
            key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
        })?;
        if local_device.activation.is_none() {
            return Err(StoreRegistrationError::ActivationRequired);
        }
        if &local_device.registration.store_root != history.root() {
            return Err(StoreError::InvalidOutbound(
                "local Store writer belongs to another Store root".to_string(),
            )
            .into());
        }
        let live_provider = storage
            .as_ref()
            .provider_binding()
            .await
            .map_err(crate::storage::StoreObjectError::from)
            .map_err(StoreError::from)?;
        if live_provider.device != local_device.registration.provider {
            return Err(StoreRegistrationError::Invalid(
                "live provider principal differs from the local registration".to_string(),
            ));
        }
        let device_signer = local_device
            .registration
            .device_signer(identity)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let writer = LocalStoreWriter {
            identity,
            registration_ref: local_device.registration_ref,
            registration: local_device.registration,
            device_signer,
        };
        Ok(history.bind_writer(storage, membership, writer))
    }
}
