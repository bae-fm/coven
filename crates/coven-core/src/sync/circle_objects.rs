//! Exact storage dispatch for Circle access objects.

use super::circle::{AccessLeafId, CircleEpochId, CircleId};
use super::circle_access::{access_leaf_semantic_prefix, CircleAccessError, RecipientSlot};
use super::storage::{
    ProtocolObjectContext, ProtocolObjectDomain, ProtocolObjectLocator, SyncStorage,
};
use super::store_commit::{ObjectHash, StoreProtocolError};
use super::store_objects::{
    append_and_verify, load_semantic_copies, StoreObjectError, VerifiedCopies,
};

pub async fn append_access_leaf_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    owner_pubkey: &str,
    epoch_id: CircleEpochId,
    recipient_slot: RecipientSlot,
    leaf_id: AccessLeafId,
    sealed_leaf: &[u8],
) -> Result<ProtocolObjectLocator, CircleObjectError> {
    let semantic_prefix =
        access_leaf_semantic_prefix(circle_id, owner_pubkey, epoch_id, recipient_slot, leaf_id)?;
    let context = ProtocolObjectContext::recipient_sealed(
        store_root_hash,
        ProtocolObjectDomain::CircleAccessLeaf,
    );
    append_and_verify(storage, &context, &semantic_prefix, "", sealed_leaf)
        .await
        .map_err(CircleObjectError::Store)
}

pub async fn load_access_leaf_object(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    owner_pubkey: &str,
    epoch_id: CircleEpochId,
    recipient_slot: RecipientSlot,
    leaf_id: AccessLeafId,
    sealed_leaf_hash: ObjectHash,
) -> Result<Option<VerifiedCopies<Vec<u8>>>, CircleObjectError> {
    let semantic_prefix =
        access_leaf_semantic_prefix(circle_id, owner_pubkey, epoch_id, recipient_slot, leaf_id)?;
    let context = ProtocolObjectContext::recipient_sealed(
        store_root_hash,
        ProtocolObjectDomain::CircleAccessLeaf,
    );
    load_semantic_copies(
        storage,
        &context,
        &semantic_prefix,
        "",
        sealed_leaf_hash,
        |bytes| {
            let actual = ObjectHash::digest(bytes);
            if actual != sealed_leaf_hash {
                return Err(StoreProtocolError::ObjectHashMismatch {
                    expected: sealed_leaf_hash,
                    actual,
                });
            }
            Ok(bytes.to_vec())
        },
    )
    .await
    .map_err(CircleObjectError::Store)
}

#[derive(Debug, thiserror::Error)]
pub enum CircleObjectError {
    #[error(transparent)]
    Access(#[from] CircleAccessError),
    #[error(transparent)]
    Store(#[from] StoreObjectError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::encryption::EncryptionService;
    use crate::keys::{public_key_hex, UserKeypair};
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::SequentialCopyIdGenerator;
    use crate::sync::circle_access::RecipientSlot;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};

    #[tokio::test]
    async fn access_leaf_uses_its_exact_recipient_sealed_storage_path() {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let recipient = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key([9; 32])),
            BlobPathScheme::Hashed,
            "access-path-store",
            owner.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("access-copy")));
        let store_root_hash = ObjectHash::digest(b"store root");
        let circle_id = CircleId::from_bytes([1; 16]);
        let epoch_id = CircleEpochId::from_bytes([2; 16]);
        let leaf_id = AccessLeafId::from_bytes([3; 16]);
        let owner_pubkey = public_key_hex(&owner);
        let slot = RecipientSlot::derive(&owner, &public_key_hex(&recipient), circle_id).unwrap();
        let sealed_leaf = b"recipient-sealed-circle-access";
        let sealed_leaf_hash = ObjectHash::digest(sealed_leaf);

        let object = append_access_leaf_object(
            &storage,
            store_root_hash,
            circle_id,
            &owner_pubkey,
            epoch_id,
            slot,
            leaf_id,
            sealed_leaf,
        )
        .await
        .unwrap();
        let expected_prefix =
            access_leaf_semantic_prefix(circle_id, &owner_pubkey, epoch_id, slot, leaf_id).unwrap();
        let copy_id = object
            .logical_key()
            .strip_prefix(&format!("{expected_prefix}/copies/"))
            .expect("access leaf uses the exact semantic copy path");
        assert_eq!(copy_id.len(), 64);
        assert!(copy_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        assert_eq!(
            object.physical().logical_key(),
            format!("{}.enc", object.logical_key()),
            "recipient-sealed leaves carry only the opaque-home suffix, not an object extension",
        );
        assert_eq!(
            home.get_appended(object.physical().logical_key()).unwrap(),
            sealed_leaf,
            "recipient-sealed bytes must not be sealed again by the Store cipher",
        );

        let loaded = load_access_leaf_object(
            &storage,
            store_root_hash,
            circle_id,
            &owner_pubkey,
            epoch_id,
            slot,
            leaf_id,
            sealed_leaf_hash,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(loaded.bytes, sealed_leaf);
        assert!(
            load_access_leaf_object(
                &storage,
                store_root_hash,
                CircleId::from_bytes([4; 16]),
                &owner_pubkey,
                epoch_id,
                slot,
                leaf_id,
                sealed_leaf_hash,
            )
            .await
            .unwrap()
            .is_none(),
            "a reader dispatches to the named circle path instead of scanning other circles",
        );
    }
}
