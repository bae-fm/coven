use crate::protocol::membership::MembershipChain;
use crate::protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use crate::protocol::wrapped_store_key::{
    PreparedWrappedStoreKey, WrappedStoreKey, WrappedStoreKeyRef,
};
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::{IdentityKeyAuthority, UserKeypair};

use crate::sync::store::membership::InviteError;

/// Store-key operations bound to one exact Store root and its storage.
pub(crate) struct StoreKeyrings<'storage> {
    storage: &'storage dyn crate::storage::SyncStorage,
    root: crate::protocol::store_commit::StoreRootRef,
}

impl<'storage> StoreKeyrings<'storage> {
    pub(crate) fn new(
        storage: &'storage dyn crate::storage::SyncStorage,
        root: crate::protocol::store_commit::StoreRootRef,
    ) -> Self {
        Self { storage, root }
    }

    pub(super) async fn open(
        &self,
        identity: &dyn IdentityKeyAuthority,
        membership: &MembershipChain,
    ) -> Result<EncryptionService, InviteError> {
        let references = wrapped_key_references(identity, membership)?;
        self.open_references(identity, &references).await
    }

    pub(crate) async fn open_containing(
        &self,
        identity: &UserKeypair,
        membership: &MembershipChain,
        required: &WrappedStoreKeyRef,
    ) -> Result<EncryptionService, InviteError> {
        let references = wrapped_key_references(identity, membership)?;
        if !references.contains(required) {
            return Err(InviteError::Crypto(
                "wrapped-key ref is not activated by the verified membership".to_string(),
            ));
        }
        self.open_references(identity, &references).await
    }

    pub(super) async fn open_or(
        &self,
        identity: &dyn IdentityKeyAuthority,
        membership: &MembershipChain,
        initial: &EncryptionService,
    ) -> Result<EncryptionService, InviteError> {
        let references = wrapped_key_references(identity, membership)?;
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
        coven_foundation::store_dir::validate_path_token(&value.author_pubkey)?;
        coven_foundation::store_dir::validate_path_token(recipient)?;
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

    async fn open_references(
        &self,
        identity: &dyn IdentityKeyAuthority,
        references: &[WrappedStoreKeyRef],
    ) -> Result<EncryptionService, InviteError> {
        let recipient = hex::encode(identity.public_key());
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
            InviteError::Bucket(crate::protocol::objects::StorageError::NotFound(format!(
                "no activated wrapped Store-key ref for {recipient}"
            )))
        })
    }
}

fn wrapped_key_references(
    identity: &dyn IdentityKeyAuthority,
    membership: &MembershipChain,
) -> Result<Vec<WrappedStoreKeyRef>, InviteError> {
    membership
        .wrapped_key_authority_for(&hex::encode(identity.public_key()))
        .map_err(InviteError::from)
}

/// Read and validate one wrapped Store key through the exact reference the
/// membership names.
pub(crate) async fn load_wrapped_store_key(
    storage: &dyn crate::storage::SyncStorage,
    store_root_hash: crate::protocol::store_commit::ObjectHash,
    reference: &WrappedStoreKeyRef,
) -> Result<WrappedStoreKey, crate::protocol::objects::StorageError> {
    let context = crate::protocol::objects::ProtocolObjectContext::recipient_sealed(
        store_root_hash,
        crate::protocol::objects::ProtocolObjectDomain::StoreWrappedKey,
    );
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &reference.semantic_prefix())
        .await?;
    let value: WrappedStoreKey = serde_json::from_slice(&bytes).map_err(|error| {
        crate::protocol::objects::StorageError::Parse(format!("parse wrapped Store key: {error}"))
    })?;
    reference.validate_value(&value, &bytes)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::wrapped_store_key::WrappedStoreKey;
    use crate::storage::SyncStorage as _;
    use crate::sync::test_helpers::{open_test_db, test_cloud_home, TestStore};
    use coven_keys::keys::UserKeypair;

    #[tokio::test]
    async fn distinct_wraps_at_one_generation_remain_distinct_exact_objects() {
        let owner = UserKeypair::generate();
        let recipient = UserKeypair::generate();
        let recipient_pubkey = hex::encode(recipient.public_key());
        let db = open_test_db();
        let store = TestStore::create(
            &db,
            "wrapped-key-exact-objects",
            owner.clone(),
            test_cloud_home(),
        )
        .await
        .expect("create exact wrapped-key Store");
        let device = store
            .bind_device(&db, &owner)
            .await
            .expect("bind exact wrapped-key Store");
        let store_id = store.root.store_root_id.to_string();
        let first = device
            .prepare_wrapped_key(
                &recipient_pubkey,
                WrappedStoreKey::signed(&store_id, &recipient_pubkey, 3, vec![1; 32], &owner),
            )
            .await
            .expect("prepare first exact wrap");
        let second = device
            .prepare_wrapped_key(
                &recipient_pubkey,
                WrappedStoreKey::signed(&store_id, &recipient_pubkey, 3, vec![2; 32], &owner),
            )
            .await
            .expect("prepare second exact wrap");

        assert_ne!(first.reference, second.reference);
        store
            .storage()
            .create_protocol_object(&first.object)
            .await
            .expect("create first exact wrap");
        store
            .storage()
            .create_protocol_object(&second.object)
            .await
            .expect("create second exact wrap");
        assert_eq!(
            load_wrapped_store_key(
                &*store.storage(),
                store.root.store_root_hash,
                &first.reference,
            )
            .await
            .expect("load first exact wrap"),
            first.validate().expect("validate first prepared wrap"),
        );
        assert_eq!(
            load_wrapped_store_key(
                &*store.storage(),
                store.root.store_root_hash,
                &second.reference,
            )
            .await
            .expect("load second exact wrap"),
            second.validate().expect("validate second prepared wrap"),
        );
    }

    #[tokio::test]
    async fn exact_wrap_ref_rejects_relocated_identity() {
        let owner = UserKeypair::generate();
        let recipient = UserKeypair::generate();
        let recipient_pubkey = hex::encode(recipient.public_key());
        let db = open_test_db();
        let store = TestStore::create(
            &db,
            "wrapped-key-relocation",
            owner.clone(),
            test_cloud_home(),
        )
        .await
        .expect("create relocation Store");
        let device = store
            .bind_device(&db, &owner)
            .await
            .expect("bind relocation Store");
        let prepared = device
            .prepare_wrapped_key(
                &recipient_pubkey,
                WrappedStoreKey::signed(
                    &store.root.store_root_id.to_string(),
                    &recipient_pubkey,
                    1,
                    vec![3; 32],
                    &owner,
                ),
            )
            .await
            .expect("prepare exact wrap");
        store
            .storage()
            .create_protocol_object(&prepared.object)
            .await
            .expect("create exact wrap");
        let mut relocated = prepared.reference;
        relocated.recipient_pubkey = hex::encode(UserKeypair::generate().public_key());

        assert!(
            load_wrapped_store_key(&*store.storage(), store.root.store_root_hash, &relocated)
                .await
                .is_err()
        );
    }
}
