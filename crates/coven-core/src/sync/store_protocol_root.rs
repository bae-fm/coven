//! Durable creation and exact opening of the Store protocol root.

use crate::database::Database;
use crate::keys::UserKeypair;

use super::storage::SyncStorage;
use super::store_commit::{ObjectHash, StoreProtocolRoot};
use super::store_objects::{
    append_and_verify, load_expected_store_protocol_root, StoreObjectError,
};
use crate::WritePolicy;

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
    #[error("Serial Store coordination check failed: {0}")]
    Coordination(String),
}

pub async fn create_store(
    db: &Database,
    storage: &super::cloud_storage::CloudSyncStorage,
    store_id: &str,
    founder_timestamp: &str,
    signer: &UserKeypair,
) -> Result<StoreProtocolRoot, StoreProtocolRootError> {
    let write_policy = db.write_policy();
    if write_policy == WritePolicy::Serial {
        let (first, second) = storage
            .serial_coordination_probe_clients()
            .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
        let key = storage
            .next_coordination_probe_key()
            .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
        super::storage::probe_serial_coordination(&first, &second, key)
            .await
            .map_err(|error| StoreProtocolRootError::Coordination(error.to_string()))?;
    }
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
                db.sync_routing_hash(),
                write_policy,
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
        write_policy,
        db.sync_routing_hash(),
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
        write_policy,
        db.sync_routing_hash(),
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
            &super::storage::ProtocolObjectContext::store(
                owned.semantic_hash,
                super::storage::ProtocolObjectDomain::StoreProtocolRoot,
            ),
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
    let write_policy = db.write_policy();
    let verified = load_expected_store_protocol_root(
        storage,
        expected_hash,
        store_id,
        expected_founder,
        write_policy,
        db.sync_routing_hash(),
    )
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{
        CloudHeadCreateError, CloudHeadReplaceError, CloudHeadStorage, CloudHeadVersion,
        CloudHomeError, CloudVersionedHead, SequentialCopyIdGenerator,
    };
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::test_helpers::open_serial_test_db;

    struct RecordingHeadClient {
        id: usize,
        calls: Arc<Mutex<Vec<usize>>>,
        inner: InMemoryCloudHome,
    }

    #[async_trait]
    impl CloudHeadStorage for RecordingHeadClient {
        async fn read_head(&self, key: &str) -> Result<CloudVersionedHead, CloudHomeError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.read_head(key).await
        }

        async fn create_head(
            &self,
            key: &str,
            bytes: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.create_head(key, bytes).await
        }

        async fn replace_head(
            &self,
            key: &str,
            expected: &CloudHeadVersion,
            bytes: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.replace_head(key, expected, bytes).await
        }

        async fn delete_probe_head(&self, key: &str) -> Result<(), CloudHomeError> {
            self.calls.lock().unwrap().push(self.id);
            self.inner.delete_probe_head(key).await
        }
    }

    #[tokio::test]
    async fn serial_store_creation_probes_two_independent_provider_clients() {
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "independent-probe-clients",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(
            "independent-probe-clients",
        )))
        .with_serial_coordination_clients(
            Arc::new(RecordingHeadClient {
                id: 1,
                calls: calls.clone(),
                inner: home.clone(),
            }),
            Arc::new(RecordingHeadClient {
                id: 2,
                calls: calls.clone(),
                inner: home,
            }),
        );
        let db = open_serial_test_db();

        create_store(
            &db,
            &storage,
            "independent-probe-clients",
            "0000000000001-0000-founder",
            &keypair,
        )
        .await
        .expect("create Serial Store");

        let create_clients: BTreeSet<_> = calls.lock().unwrap().iter().copied().collect();
        assert_eq!(create_clients.len(), 2);
    }

    #[tokio::test]
    async fn failed_probe_cleanup_does_not_poison_store_root_creation_retry() {
        let home = InMemoryCloudHome::new();
        home.fail_coordination_probe_cleanup();
        let keypair = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "probe-cleanup-retry",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(
            "probe-cleanup-retry",
        )))
        .with_serial_coordination_clients(Arc::new(home.clone()), Arc::new(home));
        let db = open_serial_test_db();

        let first = create_store(
            &db,
            &storage,
            "probe-cleanup-retry",
            "0000000000001-0000-founder",
            &keypair,
        )
        .await;
        assert!(matches!(
            first,
            Err(StoreProtocolRootError::Coordination(_))
        ));

        create_store(
            &db,
            &storage,
            "probe-cleanup-retry",
            "0000000000001-0000-founder",
            &keypair,
        )
        .await
        .expect("retry creates the Store root after one failed probe cleanup");
    }
}
