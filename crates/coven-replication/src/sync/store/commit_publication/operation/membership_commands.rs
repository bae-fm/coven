use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &mut self,
        public_key_hex: &str,
        current_encryption: &coven_keys::encryption::EncryptionService,
        master_keys: &dyn coven_keys::keys::MasterKeyCustody,
        cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let timestamp = self.database.stamp();
        let outcome = self
            .revoke_member_without_local_adoption(
                public_key_hex,
                &timestamp,
                current_encryption,
                pending_rotation,
            )
            .await?;
        let new_key = match &outcome {
            membership_mutation::MembershipRevocation::Activated(keyring)
            | membership_mutation::MembershipRevocation::AlreadyRemoved(keyring) => keyring,
        };
        let generation = new_key.current_generation();
        let adopted = cipher
            .adopt_key_rotation(new_key, master_keys)
        .map_err(|source| {
            crate::sync::store::membership::MembershipOpsError::RotationCommittedAdoptionFailed {
                source,
            }
        })?;
        match outcome {
            membership_mutation::MembershipRevocation::Activated(_) => {
                self.complete_revoke_rotation_adoption(pending_rotation, generation)
                    .await?;
            }
            membership_mutation::MembershipRevocation::AlreadyRemoved(_) => {
                let _mutation = self.database.membership_mutation_permit().await;
                if self
                    .database
                    .load_rotation_gate()
                    .await
                    .map_err(MembershipMutationError::from)?
                    .is_some()
                {
                    let gate = self
                        .database
                        .complete_peer_rotation_adoption(generation)
                        .await
                        .map_err(MembershipMutationError::from)?;
                    pending_rotation.install_durable_gate(gate);
                }
            }
        }
        Ok(adopted.fingerprint().to_string())
    }

    pub(super) async fn revoke_member_without_local_adoption(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &coven_keys::encryption::EncryptionService,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
    ) -> Result<
        membership_mutation::MembershipRevocation,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let store_id = self.store_root().store_root_id.to_string();
        let new_key = membership_mutation::AuthorizedMembershipRevocation::begin(
            self,
            public_key_hex,
            &store_id,
            timestamp,
            current_encryption,
            pending_rotation,
        )
        .await
        .execute()
        .await?;
        Ok(new_key)
    }

    pub(super) async fn complete_revoke_rotation_adoption(
        &self,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::MembershipMutationError> {
        let _mutation = self.database.membership_mutation_permit().await;
        let row = self
            .database
            .outbound_membership_mutation()
            .await?
            .ok_or_else(|| {
                crate::sync::store::membership::MembershipMutationError::InvalidDurableMutation(
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
}
