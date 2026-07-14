//! Durable creation and exact opening of the Store protocol root.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::storage::SyncStorage;
use super::store_commit::{ObjectHash, StoreProtocolRoot};
use super::store_objects::{
    append_and_verify, load_expected_store_protocol_root, StoreObjectError,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreProtocolRootError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("Store protocol root database state: {0}")]
    Database(String),
    #[error("Store protocol layout is nonempty but has no supported Store protocol root")]
    NonemptyWithoutStoreProtocolRoot,
    #[error("Store protocol root schema version {root_schema} is newer than local schema {local}")]
    SchemaTooNew { root_schema: u32, local: u32 },
    #[error("Store protocol root is missing at {0}")]
    Missing(ObjectHash),
}

pub async fn create_store(
    db: &Database,
    storage: &dyn SyncStorage,
    store_id: &str,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let owned = match db
        .local_store_protocol_root()
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
    {
        Some(owned) => owned,
        None => {
            let founder = super::membership::founder_entry(store_id, signer, founder_timestamp);
            let store_protocol_root = StoreProtocolRoot::signed(
                store_id.to_string(),
                founder,
                db.schema_version(),
                signer,
            )
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
            db.stage_store_protocol_root(store_protocol_root)
                .await
                .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
            db.local_store_protocol_root()
                .await
                .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
                .ok_or_else(|| {
                    StoreProtocolRootError::Database(
                        "staged store protocol root ownership row is absent".to_string(),
                    )
                })?
        }
    };
    let store_protocol_root = StoreProtocolRoot::parse_expected(
        &owned.bytes,
        owned.semantic_hash,
        store_id,
        &crate::keys::public_key_hex(signer),
    )
    .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    if store_protocol_root.schema_version > db.schema_version() {
        return Err(StoreProtocolRootError::SchemaTooNew {
            root_schema: store_protocol_root.schema_version,
            local: db.schema_version(),
        });
    }

    let existing = load_expected_store_protocol_root(
        storage,
        owned.semantic_hash,
        store_id,
        &store_protocol_root.author_pubkey,
    )
    .await?;
    if existing.is_none() && owned.published {
        return Err(StoreProtocolRootError::Missing(owned.semantic_hash));
    }
    if existing.is_none() {
        let listing = storage
            .list_protocol_objects(super::store_commit::protocol_prefix())
            .await
            .map_err(StoreObjectError::from)?;
        if !listing.objects.is_empty() {
            return Err(StoreProtocolRootError::NonemptyWithoutStoreProtocolRoot);
        }
        append_and_verify(
            storage,
            &super::store_commit::store_protocol_root_semantic_prefix(owned.semantic_hash),
            ".json",
            &owned.bytes,
        )
        .await?;
    }
    db.complete_store_protocol_root(owned.semantic_hash)
        .await
        .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    Ok(store_protocol_root)
}

pub async fn open_store(
    db: &Database,
    storage: &dyn SyncStorage,
    expected_hash: ObjectHash,
    store_id: &str,
    expected_founder: &str,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let verified =
        load_expected_store_protocol_root(storage, expected_hash, store_id, expected_founder)
            .await?
            .ok_or(StoreProtocolRootError::Missing(expected_hash))?;
    if verified.value.schema_version > db.schema_version() {
        return Err(StoreProtocolRootError::SchemaTooNew {
            root_schema: verified.value.schema_version,
            local: db.schema_version(),
        });
    }
    db.set_protocol_state(
        crate::database::STORE_ROOT_HASH_STATE_KEY,
        &expected_hash.to_string(),
    )
    .await
    .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
    Ok(verified.value)
}
