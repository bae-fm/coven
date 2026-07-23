use super::*;
use crate::sync::store::AuthorizedStore;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthorizationRefreshError {
    #[error("select this device's wrapped-key authority: {0}")]
    Membership(#[source] crate::sync::membership::MembershipError),
    #[error("read this device's wrapped key: {0}")]
    WrappedKey(#[source] InviteError),
    #[error("refresh state is invalid: {0}")]
    InvalidState(String),
    #[error("rotation gate database state: {0}")]
    Database(#[source] crate::database::DbError),
    #[error("merge this device's live and selected keyrings: {0}")]
    InvalidKeyring(#[source] crate::encryption::EncryptionError),
    #[error("adopt committed store-key rotation: {0}")]
    KeyAdoption(#[source] KeyError),
}

impl AuthorizedStore<'_> {
    /// Refresh the current member's encryption authority before the Store seals
    /// or judges any cloud object in this cycle.
    pub(crate) async fn refresh_authorization_state(
        &self,
        cipher: &dyn CloudCipherAccess,
        pending_rotation: &PendingRotation,
        user_keypair: &UserKeypair,
        custody: Option<&dyn MasterKeyCustody>,
    ) -> Result<(), AuthorizationRefreshError> {
        if cipher.snapshot().is_plaintext() {
            debug!("refresh: plaintext home, nothing to refresh");
            return Ok(());
        }

        let recipient = crate::keys::public_key_hex(user_keypair);
        let wrapped_keys = self
            .membership()
            .wrapped_key_authority_for(&recipient)
            .map_err(AuthorizationRefreshError::Membership)?;
        let live_keyring = match cipher.snapshot() {
            crate::sync::cloud_storage::CloudCipher::Encrypted(encryption) => encryption,
            crate::sync::cloud_storage::CloudCipher::Plaintext => {
                return Err(AuthorizationRefreshError::InvalidState(
                    "plaintext home cannot enter encrypted key refresh".to_string(),
                ));
            }
        };
        if wrapped_keys.is_empty() {
            debug!("refresh: no activated wrapped key for this device; keeping the live key");
            return Ok(());
        }

        let store_root = self.store_root();
        let store_id = store_root.store_root_id.to_string();
        match unwrap_store_keyring_for_refs(
            self.storage(),
            store_root.store_root_hash,
            user_keypair,
            &store_id,
            &wrapped_keys,
        )
        .await
        {
            Ok(new_encryption) => {
                let merged = live_keyring
                    .merged_with(&new_encryption)
                    .map_err(AuthorizationRefreshError::InvalidKeyring)?;
                if merged.key_count() == live_keyring.key_count() {
                    if pending_rotation.gate().is_some() {
                        let gate = self
                            .database()
                            .complete_peer_rotation_adoption(live_keyring.current_generation())
                            .await
                            .map_err(AuthorizationRefreshError::Database)?;
                        pending_rotation
                            .install_durable_gate(gate)
                            .map_err(AuthorizationRefreshError::InvalidState)?;
                    }
                    debug!("refresh: wrapped store key is already held by the live keyring");
                } else {
                    let gate = self
                        .database()
                        .record_peer_rotation(merged.current_generation())
                        .await
                        .map_err(AuthorizationRefreshError::Database)?;
                    pending_rotation
                        .install_durable_gate(Some(gate))
                        .map_err(AuthorizationRefreshError::InvalidState)?;
                    match custody {
                        None => {
                            info!(
                                committed_generation = merged.current_generation(),
                                "refresh: found a rotated store key but this cycle has no \
                                 master-key custody to adopt it; sealing is paused until a \
                                 cycle with custody adopts it"
                            );
                        }
                        Some(custody) => {
                            let fingerprint = apply_key_rotation(new_encryption, custody, cipher)
                                .map_err(AuthorizationRefreshError::KeyAdoption)?;
                            let adopted_generation = match cipher.snapshot() {
                                crate::sync::cloud_storage::CloudCipher::Encrypted(encryption) => {
                                    encryption.current_generation()
                                }
                                crate::sync::cloud_storage::CloudCipher::Plaintext => {
                                    return Err(AuthorizationRefreshError::InvalidState(
                                        "encrypted key refresh produced a plaintext cipher"
                                            .to_string(),
                                    ));
                                }
                            };
                            let gate = self
                                .database()
                                .complete_peer_rotation_adoption(adopted_generation)
                                .await
                                .map_err(AuthorizationRefreshError::Database)?;
                            pending_rotation
                                .install_durable_gate(gate)
                                .map_err(AuthorizationRefreshError::InvalidState)?;
                            info!(%fingerprint, "Adopted rotated store key");
                        }
                    }
                }
            }
            Err(error) => return Err(AuthorizationRefreshError::WrappedKey(error)),
        }

        Ok(())
    }
}
