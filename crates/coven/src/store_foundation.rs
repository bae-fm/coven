//! The owners every handle over a store holds, however much of the store its
//! surface exposes.
//!
//! A full [`CovenHandle`](crate::CovenHandle) and a read-only
//! [`CovenReadHandle`](crate::CovenReadHandle) differ in what they let a host
//! do, not in what a store *is*: both read rows off a connection coven owns and
//! resolve a blob's locality against the same cache, cloud storage, and key
//! custody. That common graph is built here, once, so the two handles cannot
//! drift into composing the same store two different ways — the full handle
//! then adds the writer-side owners on top.

use std::sync::Arc;

use crate::store_blobs::StoreBlobAccess;
use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_security::StoreSecurity;
use crate::store_sync::ConfigProvider;
use crate::sync::store::blob::{LocalStoreBlobAccess, StoreBlobCache};
use coven_database::{Database, StoreDatabase};
use coven_foundation::clock::ClockRef;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::{DeviceIdentityCustody, MasterKeyCustody, StoreKeys};

pub(crate) struct StoreFoundation {
    pub(crate) database: StoreDatabase,
    pub(crate) security: StoreSecurity,
    pub(crate) cloud_storage: StoreCloudStorage,
    pub(crate) local_blob_access: LocalStoreBlobAccess,
    pub(crate) blob_access: StoreBlobAccess,
}

impl StoreFoundation {
    /// Compose the store's shared owners over an already-open connection.
    ///
    /// `config_provider` is read fresh by whatever needs the current config, so
    /// a host can reconnect a provider without rebuilding anything here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        db: Database,
        store_dir: &StoreDir,
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        key_custody: Arc<dyn MasterKeyCustody>,
        identity_custody: Arc<dyn DeviceIdentityCustody>,
        oauth_clients: coven_storage::oauth::OAuthClients,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
        blob_chunking: coven_storage::BlobChunking,
    ) -> Self {
        let database = StoreDatabase::from_database(db);
        let cloud_homes =
            coven_storage::cloud::CloudHomeFactory::new(key_service.clone(), oauth_clients);
        let security = StoreSecurity::new(key_service, key_custody, identity_custody);
        let cloud_storage = StoreCloudStorage::new(
            security.clone(),
            cloud_homes,
            clock,
            cloudkit_ops,
            blob_chunking,
        );
        let blob_cache = StoreBlobCache::new(database.clone(), store_dir.clone());
        let local_blob_access =
            LocalStoreBlobAccess::new(database.clone(), store_dir.clone(), blob_cache);
        let blob_access = StoreBlobAccess::new(
            database.clone(),
            config_provider,
            cloud_storage.clone(),
            local_blob_access.clone(),
        );
        Self {
            database,
            security,
            cloud_storage,
            local_blob_access,
            blob_access,
        }
    }
}
