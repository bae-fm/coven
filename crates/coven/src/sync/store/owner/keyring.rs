use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::protocol::membership::MembershipChain;
use crate::protocol::wrapped_store_key::{
    load_wrapped_store_key, PreparedWrappedStoreKey, WrappedStoreKey, WrappedStoreKeyRef,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};

use crate::sync::store::membership::InviteError;

/// Store-key operations bound to one exact Store root and its storage.
pub(super) struct StoreKeyrings<'storage> {
    storage: &'storage dyn crate::storage::SyncStorage,
    root: crate::protocol::store_commit::StoreRootRef,
}

impl<'storage> StoreKeyrings<'storage> {
    pub(super) fn new(
        storage: &'storage dyn crate::storage::SyncStorage,
        root: crate::protocol::store_commit::StoreRootRef,
    ) -> Self {
        Self { storage, root }
    }

    pub(super) async fn open(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
    ) -> Result<EncryptionService, InviteError> {
        let references = Self::references(identity, membership)?;
        self.open_references(identity, &references).await
    }

    pub(super) async fn open_containing(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
        required: &WrappedStoreKeyRef,
    ) -> Result<EncryptionService, InviteError> {
        let references = Self::references(identity, membership)?;
        if !references.contains(required) {
            return Err(InviteError::Crypto(
                "wrapped-key ref is not activated by the verified membership".to_string(),
            ));
        }
        self.open_references(identity, &references).await
    }

    pub(super) async fn open_or(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
        initial: &EncryptionService,
    ) -> Result<EncryptionService, InviteError> {
        let references = Self::references(identity, membership)?;
        if references.is_empty() {
            Ok(initial.clone())
        } else {
            self.open_references(identity, &references).await
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
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreWrappedKey,
        );
        let slot = self
            .storage
            .allocate_protocol_slot(&context, &semantic_prefix, ".json")
            .await?;
        let object =
            self.storage
                .prepare_protocol_object(&context, slot, &semantic_prefix, bytes)?;
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

    fn references(
        identity: &UserKeypair,
        membership: &MembershipChain,
    ) -> Result<Vec<WrappedStoreKeyRef>, InviteError> {
        membership
            .wrapped_key_authority_for(&keys::public_key_hex(identity))
            .map_err(InviteError::from)
    }

    async fn open_references(
        &self,
        identity: &UserKeypair,
        references: &[WrappedStoreKeyRef],
    ) -> Result<EncryptionService, InviteError> {
        let recipient = keys::public_key_hex(identity);
        let root = &self.root;
        let store_id = root.store_root_id.to_string();
        let mut merged: Option<EncryptionService> = None;
        for reference in references {
            if reference.recipient_pubkey != recipient {
                return Err(InviteError::Crypto(
                    "activated wrapped-key ref names another recipient".to_string(),
                ));
            }
            let wrapped =
                load_wrapped_store_key(self.storage, root.store_root_hash, reference).await?;
            let keyring = wrapped
                .verify_and_open_keyring(
                    &store_id,
                    &recipient,
                    std::iter::once(reference.owner_pubkey.as_str()),
                    reference.generation,
                    identity,
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
