use super::*;

impl StoreSync {
    pub(super) fn map_storage_setup_error(error: StorageSetupError) -> SyncError {
        match error {
            StorageSetupError::CloudHome(error) => SyncError::CloudHome(error),
            StorageSetupError::Key(error) => SyncError::Key(error),
            StorageSetupError::NoEncryptionKey => SyncError::MasterKeyNotEstablished,
            error => SyncError::StorageSetup(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config_provider: ConfigProvider,
        security: StoreSecurity,
        master_keys: Arc<dyn coven_keys::keys::MasterKeyCustody>,
        database: StoreDatabase,
        #[cfg(test)] store_dir: coven_foundation::store_dir::StoreDir,
        clock: ClockRef,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        cloud_storage: StoreCloudStorage,
        blob_access: crate::store_blobs::StoreBlobAccess,
        local_blob_transitions: coven_replication::blob::transition::LocalBlobTransitions,
    ) -> Self {
        Self {
            config_provider,
            security,
            master_keys,
            database,
            #[cfg(test)]
            store_dir,
            clock,
            observer,
            open_guard,
            cloud_storage,
            blob_access,
            local_blob_transitions,
            state: Arc::new(RwLock::new(SyncConnection::Disconnected)),
            lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            status_tx: tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            #[cfg(test)]
            stopped_loops: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub(super) fn install_cloud(
        &self,
        sync: Arc<SyncLoopHandle>,
        storage: Arc<dyn CloudSyncObjectStorage>,
        driver: SyncDriver,
    ) {
        self.blob_access.install_connected(storage.clone());
        *self.state.write().expect("write Store sync connection") = SyncConnection::WithCloud {
            sync,
            #[cfg(test)]
            storage,
            driver,
        };
    }

    pub(super) fn install_without_cloud(&self) {
        self.blob_access.clear_connection();
        *self.state.write().expect("write Store sync connection") = SyncConnection::WithoutCloud;
    }

    pub(super) fn stop_current(&self) -> Result<bool, SyncError> {
        let previous = std::mem::replace(
            &mut *self.state.write().expect("write Store sync connection"),
            SyncConnection::Disconnected,
        );
        let was_connected = !matches!(previous, SyncConnection::Disconnected);
        self.blob_access.clear_connection();
        if let SyncConnection::WithCloud { sync, .. } = previous {
            sync.stop().map_err(SyncError::Loop)?;
            #[cfg(test)]
            self.stopped_loops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(was_connected)
    }

    pub(crate) fn is_connected(&self) -> bool {
        !matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::Disconnected
        )
    }

    pub(crate) fn trigger(&self) {
        match self.connected() {
            Some(sync) => sync.trigger(),
            None => debug!("sync_now: no cloud connection; sync wake ignored"),
        }
    }

    pub(crate) fn is_syncing(&self) -> bool {
        self.connected().is_some_and(|sync| sync.is_running())
    }

    pub(super) fn has_cloud(&self) -> bool {
        matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::WithCloud { .. }
        )
    }

    pub(super) fn config(&self) -> Config {
        (self.config_provider)()
    }

    pub(super) async fn build_connection(
        &self,
        config: Config,
        storage: Option<Arc<CloudSyncConnection>>,
    ) -> Result<(), SyncError> {
        let Some(storage) = storage else {
            self.install_without_cloud();
            info!("start_sync: sync not configured; no loop started");
            return Ok(());
        };

        let routing_encryption = self.connected_encryption(storage.is_plaintext())?;
        let components = self
            .initialize_components(Arc::clone(&storage), routing_encryption.clone())
            .await?;
        let storage: Arc<dyn CloudSyncObjectStorage> = storage;
        let sync = self.build_sync(components, config, routing_encryption);
        if let Err(error) = sync.start() {
            self.blob_access.clear_connection();
            return Err(SyncError::Loop(error));
        }
        info!("Sync loop started");
        self.install_cloud(sync, storage, SyncDriver::Loop);
        Ok(())
    }

    pub(super) async fn replace_connection(
        &self,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<(), SyncError> {
        let config = self.config();
        let storage = if config.cloud_home.provider.is_some() {
            let admitted = self
                .cloud_storage
                .admit(&config, cloudkit_ops)
                .map_err(Self::map_storage_setup_error)?;
            self.stop_current()?;
            Some(Arc::new(
                admitted
                    .open(None)
                    .await
                    .map_err(Self::map_storage_setup_error)?,
            ))
        } else {
            self.stop_current()?;
            None
        };
        self.build_connection(config, storage).await
    }

    pub(crate) async fn connect(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_connection(None).await?;
        info!("store sync connected");
        Ok(())
    }

    pub(crate) async fn probe_cloud_home(&self, config: &Config) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.cloud_storage
            .probe(config)
            .await
            .map_err(Self::map_storage_setup_error)
    }

    pub(crate) async fn connect_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_connection(Some(cloudkit_ops)).await?;
        info!("store sync connected with CloudKit driver");
        Ok(())
    }

    /// Replace the current connection with one over an injected cloud home.
    /// This lifetime owner constructs and installs both the loop and its storage;
    /// callers select only who drives cycles.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) async fn replace_with_test_home(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        cipher: coven_storage::CloudCipher,
        driver: SyncDriver,
    ) -> Result<(), SyncError> {
        let config = self.config();
        let routing_encryption = match &cipher {
            coven_storage::CloudCipher::Encrypted(encryption) => Some(encryption.clone()),
            coven_storage::CloudCipher::Plaintext => None,
        };
        let admitted = self
            .cloud_storage
            .admit_home(&config, home)
            .map_err(Self::map_storage_setup_error)?;
        self.stop_current()?;
        let storage = Arc::new(
            admitted
                .open(Some(cipher))
                .map_err(Self::map_storage_setup_error)?,
        );
        let components = self
            .initialize_components(Arc::clone(&storage), routing_encryption.clone())
            .await?;
        let storage: Arc<dyn CloudSyncObjectStorage> = storage;
        let sync = self.build_sync(components, config, routing_encryption);
        if matches!(&driver, SyncDriver::Loop) {
            if let Err(error) = sync.start() {
                self.blob_access.clear_connection();
                return Err(SyncError::Loop(error));
            }
            info!("Sync loop started");
        }
        self.install_cloud(sync, storage, driver);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        cipher: coven_storage::CloudCipher,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_with_test_home(home, cipher, SyncDriver::Loop)
            .await?;
        info!("store sync connected over an injected test cloud home");
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home_caller_driven(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        cipher: coven_storage::CloudCipher,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_with_test_home(home, cipher, SyncDriver::Caller)
            .await?;
        info!(
            "store sync connected over an injected test cloud home; the caller drives its cycles"
        );
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home_custody(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        let config = self.config();
        let admitted = self
            .cloud_storage
            .admit_home(&config, home)
            .map_err(Self::map_storage_setup_error)?;
        self.stop_current()?;
        let storage = Arc::new(admitted.open(None).map_err(Self::map_storage_setup_error)?);
        self.build_connection(config, Some(storage)).await?;
        info!("store sync connected over an injected test cloud home");
        Ok(())
    }

    pub(crate) async fn start(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        if !self.is_connected() {
            debug!("start_sync: no provider connected; nothing to start");
            return Ok(());
        }
        self.replace_connection(None).await
    }

    pub(crate) fn stop(&self) {
        let was_connected = match self.stop_current() {
            Ok(was_connected) => was_connected,
            Err(stop_error) => {
                error!("stop_sync failed: {stop_error}");
                false
            }
        };
        if was_connected {
            self.install_without_cloud();
        } else {
            debug!("stop_sync: no provider connected; nothing to stop");
        }
    }

    pub(crate) fn disconnect(&self) {
        if let Err(stop_error) = self.stop_current() {
            error!("disconnect_sync failed to stop sync: {stop_error}");
        }
        info!("store sync disconnected");
    }

    pub(crate) fn subscribe_status(&self) -> watch::Receiver<SyncLoopStatus> {
        self.status_tx.subscribe()
    }

    pub(crate) fn is_command_configured(&self) -> bool {
        self.has_cloud() || self.config().cloud_home.provider.is_some()
    }

    pub(crate) fn command_config(&self) -> Config {
        self.connected()
            .map(|sync| sync.config().clone())
            .unwrap_or_else(|| self.config())
    }
}
