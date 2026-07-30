use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::protocol::membership::MembershipChain;
use crate::protocol::wrapped_store_key::{
    load_wrapped_store_key, PreparedWrappedStoreKey, WrappedStoreKey, WrappedStoreKeyRef,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};

use super::verified_history::MergeHistoryVerifier;

use crate::sync::store::membership::InviteError;

/// A membership-selected Store keyring reader bound to one verified Store
/// history and the local identity that may open its activated wraps.
pub(super) struct AuthorizedMembershipKeyring<'operation, 'storage> {
    history: &'operation MergeHistoryVerifier<'storage>,
    identity: &'operation UserKeypair,
    membership: &'operation MembershipChain,
}

impl<'operation, 'storage> AuthorizedMembershipKeyring<'operation, 'storage> {
    pub(super) fn bind(
        history: &'operation MergeHistoryVerifier<'storage>,
        identity: &'operation UserKeypair,
        membership: &'operation MembershipChain,
    ) -> Self {
        Self {
            history,
            identity,
            membership,
        }
    }

    pub(super) async fn open(self) -> Result<EncryptionService, InviteError> {
        let references = self.references()?;
        self.open_references(&references).await
    }

    pub(super) async fn open_containing(
        self,
        required: &WrappedStoreKeyRef,
    ) -> Result<EncryptionService, InviteError> {
        let references = self.references()?;
        if !references.contains(required) {
            return Err(InviteError::Crypto(
                "wrapped-key ref is not activated by the verified membership".to_string(),
            ));
        }
        self.open_references(&references).await
    }

    pub(super) async fn open_or(
        self,
        initial: &EncryptionService,
    ) -> Result<EncryptionService, InviteError> {
        let references = self.references()?;
        if references.is_empty() {
            Ok(initial.clone())
        } else {
            self.open_references(&references).await
        }
    }

    pub(super) async fn prepare(
        &self,
        recipient: &str,
        value: WrappedStoreKey,
    ) -> Result<PreparedWrappedStoreKey, StorageError> {
        crate::store_dir::validate_path_token(&value.author_pubkey)?;
        crate::store_dir::validate_path_token(recipient)?;
        if value.generation == 0 {
            return Err(StorageError::InvalidContent(
                "wrapped Store-key generation must be positive".to_string(),
            ));
        }
        let bytes = serde_json::to_vec(&value).map_err(|error| {
            StorageError::Parse(format!("serialize wrapped Store key: {error}"))
        })?;
        let wrap_hash = crate::protocol::store_commit::ObjectHash::digest(&bytes);
        let semantic_prefix = format!(
            "keys/{}/{}/{}/{}",
            value.author_pubkey, recipient, value.generation, wrap_hash
        );
        let context = ProtocolObjectContext::recipient_sealed(
            self.history.root().store_root_hash,
            ProtocolObjectDomain::StoreWrappedKey,
        );
        let slot = self
            .history
            .storage()
            .allocate_protocol_slot(&context, &semantic_prefix, ".json")
            .await?;
        let object = self.history.storage().prepare_protocol_object(
            &context,
            slot,
            &semantic_prefix,
            bytes,
        )?;
        let reference = WrappedStoreKeyRef {
            owner_pubkey: value.author_pubkey,
            recipient_pubkey: recipient.to_string(),
            generation: value.generation,
            wrap_hash,
            object: object.reference().clone(),
        };
        let prepared = PreparedWrappedStoreKey { reference, object };
        prepared.validate()?;
        Ok(prepared)
    }

    fn references(&self) -> Result<Vec<WrappedStoreKeyRef>, InviteError> {
        self.membership
            .wrapped_key_authority_for(&keys::public_key_hex(self.identity))
            .map_err(InviteError::from)
    }

    async fn open_references(
        &self,
        references: &[WrappedStoreKeyRef],
    ) -> Result<EncryptionService, InviteError> {
        let recipient = keys::public_key_hex(self.identity);
        let root = self.history.root();
        let store_id = root.store_root_id.to_string();
        let mut merged: Option<EncryptionService> = None;
        for reference in references {
            if reference.recipient_pubkey != recipient {
                return Err(InviteError::Crypto(
                    "activated wrapped-key ref names another recipient".to_string(),
                ));
            }
            let wrapped =
                load_wrapped_store_key(self.history.storage(), root.store_root_hash, reference)
                    .await?;
            let keyring = wrapped
                .verify_and_open_keyring(
                    &store_id,
                    &recipient,
                    std::iter::once(reference.owner_pubkey.as_str()),
                    reference.generation,
                    self.identity,
                )
                .map_err(|error| {
                    InviteError::Crypto(format!("verify wrapped Store key: {error}"))
                })?;
            merged = Some(match merged {
                Some(existing) => existing.merged_with(&keyring).map_err(|error| {
                    InviteError::Crypto(format!("merge wrapped keyrings: {error}"))
                })?,
                None => keyring,
            });
        }
        merged.ok_or_else(|| {
            InviteError::Bucket(crate::storage::StorageError::NotFound(format!(
                "no activated wrapped Store-key ref for {recipient}"
            )))
        })
    }
}
