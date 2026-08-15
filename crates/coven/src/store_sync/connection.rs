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
        database: StoreDatabase,
        #[cfg(test)] store_dir: coven_foundation::store_dir::StoreDir,
        clock: ClockRef,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        cloud_storage: StoreCloudStorage,
        blob_access: crate::store_blobs::StoreBlobAccess,
        runtime_factory: Arc<dyn coven_replication::sync::sync_loop::SyncLoopRuntimeFactory>,
    ) -> Self {
        Self {
            config_provider,
            security,
            database,
            #[cfg(test)]
            store_dir,
            clock,
            observer,
            open_guard,
            cloud_storage,
            blob_access,
            state: Arc::new(RwLock::new(SyncConnection::Disconnected)),
            lifecycle: Arc::new(tokio::sync::Mutex::new(())),
            status_tx: tokio::sync::watch::channel(SyncLoopStatus::Offline).0,
            runtime_factory,
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

    pub(super) fn stop_current(&self) -> bool {
        let previous = std::mem::replace(
            &mut *self.state.write().expect("write Store sync connection"),
            SyncConnection::Disconnected,
        );
        let was_connected = !matches!(previous, SyncConnection::Disconnected);
        self.blob_access.clear_connection();
        if let SyncConnection::WithCloud { sync, .. } = previous {
            sync.stop();
            #[cfg(test)]
            self.stopped_loops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        was_connected
    }

    pub(crate) fn is_connected(&self) -> bool {
        !matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::Disconnected
        )
    }

    pub(crate) fn trigger(&self) {
        match connected_sync!(self) {
            Some(sync) => sync.trigger(),
            None => debug!("sync_now: no cloud connection; sync wake ignored"),
        }
    }

    pub(crate) fn is_syncing(&self) -> bool {
        connected_sync!(self).is_some_and(|sync| sync.is_running())
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
            self.stop_current();
            self.install_without_cloud();
            info!("start_sync: sync not configured; no loop started");
            return Ok(());
        };

        let prepared = self
            .prepare_storage_connection(config, storage, SyncDriver::Loop)
            .await?;
        self.stop_current();
        prepared.install(self);
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
            let storage = Arc::new(
                admitted
                    .open(None)
                    .await
                    .map_err(Self::map_storage_setup_error)?,
            );
            Some(storage)
        } else {
            None
        };
        self.build_connection(config, storage).await
    }

    pub(crate) async fn connect(&self) -> Result<(), SyncError> {
        self.execute_cloud_operation(
            |owner| async move { owner.connect_on_cloud_runtime().await },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    async fn connect_on_cloud_runtime(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.replace_connection(None).await?;
        info!("store sync connected");
        Ok(())
    }

    pub(crate) async fn probe_cloud_home(&self, config: &Config) -> Result<(), SyncError> {
        let config = config.clone();
        self.execute_cloud_operation(
            move |owner| async move { owner.probe_cloud_home_on_cloud_runtime(&config).await },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    async fn probe_cloud_home_on_cloud_runtime(&self, config: &Config) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.cloud_storage
            .probe(config)
            .await
            .map_err(Self::map_storage_setup_error)
    }

    pub(crate) async fn import_master_key(
        &self,
        serialized: &str,
    ) -> Result<(), coven_keys::keys::MasterKeyError> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.is_connected() {
            return Err(coven_keys::keys::MasterKeyError::CloudHomeConnected);
        }
        self.security.import_master_key(serialized)
    }

    pub(crate) async fn forget_master_key(&self) -> Result<(), SyncError> {
        self.execute_cloud_operation(
            |owner| async move { owner.forget_master_key_on_cloud_runtime().await },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    async fn forget_master_key_on_cloud_runtime(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.security.forget_master_key()?;
        self.stop_current();
        Ok(())
    }

    pub(crate) async fn disconnect_cloud_home(&self) -> Result<(), SyncError> {
        self.execute_cloud_operation(
            |owner| async move { owner.disconnect_cloud_home_on_cloud_runtime().await },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    async fn disconnect_cloud_home_on_cloud_runtime(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.cloud_storage.forget_credentials()?;
        self.stop_current();
        info!("cloud home disconnected and its credentials forgotten");
        Ok(())
    }

    pub(crate) async fn connect_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>,
    ) -> Result<(), SyncError> {
        self.execute_cloud_operation(
            move |owner| async move {
                owner
                    .connect_with_cloudkit_on_cloud_runtime(cloudkit_ops)
                    .await
            },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    async fn connect_with_cloudkit_on_cloud_runtime(
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
    ) -> Result<(), crate::CloudHomeSetupError> {
        let config = self.config();
        let cloud_home = config.cloud_home.clone();
        let key = self
            .security
            .prepare_cloud_home_key(cloud_home.storage)
            .map_err(|error| crate::CloudHomeSetupError::MasterKey(Box::new(error)))?;
        let credentials = self.cloud_storage.current_credentials();
        let admitted = self
            .cloud_storage
            .admit_home(&config, home)
            .map_err(Self::map_storage_setup_error)
            .map_err(|error| crate::CloudHomeSetupError::Connection(Box::new(error)))?;
        let storage = match admitted
            .open(Some(cipher))
            .map_err(Self::map_storage_setup_error)
        {
            Ok(storage) => Arc::new(storage),
            Err(error) => {
                let failure = crate::CloudHomeSetupError::Connection(Box::new(error));
                return Err(failure.with_rollback(super::setup::rollback(&key, &credentials)));
            }
        };
        self.install_prepared_cloud_home(config, storage, cloud_home, &key, &credentials, driver)
            .await?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn connect_with_test_home(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        cipher: coven_storage::CloudCipher,
    ) -> Result<(), crate::CloudHomeSetupError> {
        self.execute_cloud_operation(
            move |owner| async move {
                owner
                    .connect_with_test_home_on_cloud_runtime(home, cipher)
                    .await
            },
            Self::cloud_runtime_setup_error,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn connect_with_test_home_on_cloud_runtime(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        cipher: coven_storage::CloudCipher,
    ) -> Result<(), crate::CloudHomeSetupError> {
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
    ) -> Result<(), crate::CloudHomeSetupError> {
        self.execute_cloud_operation(
            move |owner| async move {
                owner
                    .connect_with_test_home_caller_driven_on_cloud_runtime(home, cipher)
                    .await
            },
            Self::cloud_runtime_setup_error,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn connect_with_test_home_caller_driven_on_cloud_runtime(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
        cipher: coven_storage::CloudCipher,
    ) -> Result<(), crate::CloudHomeSetupError> {
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
        self.execute_cloud_operation(
            move |owner| async move {
                owner
                    .connect_with_test_home_custody_on_cloud_runtime(home)
                    .await
            },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn connect_with_test_home_custody_on_cloud_runtime(
        &self,
        home: Arc<dyn coven_storage::cloud::ExactCloudHome>,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        let config = self.config();
        let admitted = self
            .cloud_storage
            .admit_home(&config, home)
            .map_err(Self::map_storage_setup_error)?;
        let storage = Arc::new(admitted.open(None).map_err(Self::map_storage_setup_error)?);
        self.build_connection(config, Some(storage)).await?;
        info!("store sync connected over an injected test cloud home");
        Ok(())
    }

    pub(crate) async fn start(&self) -> Result<(), SyncError> {
        self.execute_cloud_operation(
            |owner| async move { owner.start_on_cloud_runtime().await },
            Self::cloud_runtime_sync_error,
        )
        .await
    }

    async fn start_on_cloud_runtime(&self) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        if !self.is_connected() {
            debug!("start_sync: no provider connected; nothing to start");
            return Ok(());
        }
        self.replace_connection(None).await
    }

    pub(crate) fn stop(&self) {
        let was_connected = self.stop_current();
        if was_connected {
            self.install_without_cloud();
        } else {
            debug!("stop_sync: no provider connected; nothing to stop");
        }
    }

    pub(crate) fn disconnect(&self) {
        self.stop_current();
        info!("store sync disconnected");
    }

    pub(crate) fn subscribe_status(&self) -> watch::Receiver<SyncLoopStatus> {
        self.status_tx.subscribe()
    }

    pub(crate) fn is_command_configured(&self) -> bool {
        self.has_cloud() || self.config().cloud_home.provider.is_some()
    }

    pub(crate) fn command_config(&self) -> Config {
        connected_sync!(self)
            .map(|sync| sync.config().clone())
            .unwrap_or_else(|| self.config())
    }
}
