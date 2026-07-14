//! Durable creation and exact opening of the Store protocol root.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::storage::SyncStorage;
use super::store_commit::{ObjectHash, ProtocolGenesis};
use super::store_objects::{append_and_verify, load_expected_genesis, StoreObjectError};

#[derive(Debug, thiserror::Error)]
pub enum StoreGenesisError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("Store genesis database state: {0}")]
    Database(String),
    #[error("Store protocol layout is nonempty but has no supported genesis")]
    NonemptyWithoutGenesis,
    #[error("Store genesis schema version {genesis} is newer than local schema {local}")]
    SchemaTooNew { genesis: u32, local: u32 },
    #[error("Store protocol genesis is missing at {0}")]
    Missing(ObjectHash),
}

pub async fn create_store(
    db: &Database,
    storage: &dyn SyncStorage,
    store_id: &str,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<ProtocolGenesis, StoreGenesisError> {
    let owned = match db
        .local_protocol_genesis()
        .await
        .map_err(|error| StoreGenesisError::Database(error.to_string()))?
    {
        Some(owned) => owned,
        None => {
            let founder = super::membership::founder_entry(store_id, signer, founder_timestamp);
            let genesis =
                ProtocolGenesis::signed(store_id.to_string(), founder, db.schema_version(), signer)
                    .map_err(|error| StoreGenesisError::Database(error.to_string()))?;
            db.stage_protocol_genesis(genesis)
                .await
                .map_err(|error| StoreGenesisError::Database(error.to_string()))?;
            db.local_protocol_genesis()
                .await
                .map_err(|error| StoreGenesisError::Database(error.to_string()))?
                .ok_or_else(|| {
                    StoreGenesisError::Database(
                        "staged protocol genesis ownership row is absent".to_string(),
                    )
                })?
        }
    };
    let genesis = ProtocolGenesis::parse_expected(
        &owned.bytes,
        owned.semantic_hash,
        store_id,
        &crate::keys::public_key_hex(signer),
    )
    .map_err(|error| StoreGenesisError::Database(error.to_string()))?;
    if genesis.schema_version > db.schema_version() {
        return Err(StoreGenesisError::SchemaTooNew {
            genesis: genesis.schema_version,
            local: db.schema_version(),
        });
    }

    let existing = load_expected_genesis(
        storage,
        owned.semantic_hash,
        store_id,
        &genesis.author_pubkey,
    )
    .await?;
    if existing.is_none() && owned.published {
        return Err(StoreGenesisError::Missing(owned.semantic_hash));
    }
    if existing.is_none() {
        let listing = storage
            .list_protocol_objects(super::store_commit::protocol_prefix())
            .await
            .map_err(StoreObjectError::from)?;
        if !listing.objects.is_empty() {
            return Err(StoreGenesisError::NonemptyWithoutGenesis);
        }
        append_and_verify(
            storage,
            &super::store_commit::genesis_semantic_prefix(owned.semantic_hash),
            ".json",
            &owned.bytes,
        )
        .await?;
    }
    db.complete_protocol_genesis(owned.semantic_hash)
        .await
        .map_err(|error| StoreGenesisError::Database(error.to_string()))?;
    Ok(genesis)
}

pub async fn open_store(
    db: &Database,
    storage: &dyn SyncStorage,
    expected_hash: ObjectHash,
    store_id: &str,
    expected_founder: &str,
) -> Result<ProtocolGenesis, StoreGenesisError> {
    let verified = load_expected_genesis(storage, expected_hash, store_id, expected_founder)
        .await?
        .ok_or(StoreGenesisError::Missing(expected_hash))?;
    if verified.value.schema_version > db.schema_version() {
        return Err(StoreGenesisError::SchemaTooNew {
            genesis: verified.value.schema_version,
            local: db.schema_version(),
        });
    }
    db.set_protocol_state(
        crate::database::PROTOCOL_GENESIS_HASH_STATE_KEY,
        &expected_hash.to_string(),
    )
    .await
    .map_err(|error| StoreGenesisError::Database(error.to_string()))?;
    Ok(verified.value)
}
