use super::*;

impl StoreSync {
    pub(super) fn map_storage_setup_error(error: StorageSetupError) -> SyncError {
        match error {
            StorageSetupError::Key(error) => SyncError::Key(error),
            error => SyncError::StorageSetup(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config_provider: ConfigProvider,
        security: StoreSecurity,
        database: StoreDatabase,
        store_dir: crate::store_dir::StoreDir,
        clock: ClockRef,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        cloud_storage: StoreCloudStorage,
        local_blob_access: LocalStoreBlobAccess,
        blob_access: crate::store_blobs::StoreBlobAccess,
        local_blob_transitions: crate::blob::transition::LocalBlobTransitions,
    ) -> Self {
        Self {
            config_provider,
            security,
            database,
            store_dir,
            clock,
            observer,
            open_guard,
            cloud_storage,
            local_blob_access,
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
        storage: Arc<dyn SyncStorage>,
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
        storage: Option<Arc<CloudSyncStorage>>,
    ) -> Result<(), SyncError> {
        let Some(storage) = storage else {
            self.install_without_cloud();
            info!("start_sync: sync not configured; no loop started");
            return Ok(());
        };

        let routing_encryption = self
            .security
            .routing_encryption(self.database.has_scoped_graph())?;
        let components = self
            .initialize_components(Arc::clone(&storage), routing_encryption.clone())
            .await?;
        let storage: Arc<dyn SyncStorage> = storage;
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
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    ) -> Result<(), SyncError> {
        let config = self.config();
        let storage = if config.cloud_home.provider.is_some() {
            let admitted = self
                .cloud_storage
                .admit(&config, cloudkit_ops)
                .map_err(Self::map_storage_setup_error)?;
            let cipher = self
                .security
                .resolve_cloud_cipher(config.cloud_home.storage)?;
            self.stop_current()?;
            Some(Arc::new(
                admitted
                    .open(Some(cipher))
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

    pub(crate) async fn connect_with_cloudkit(
        &self,
        cloudkit_ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
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
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
        driver: SyncDriver,
    ) -> Result<(), SyncError> {
        let config = self.config();
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
        let routing_encryption = self
            .security
            .routing_encryption(self.database.has_scoped_graph())?;
        let components = self
            .initialize_components(Arc::clone(&storage), routing_encryption.clone())
            .await?;
        let storage: Arc<dyn SyncStorage> = storage;
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
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
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
        home: Arc<dyn CloudHome>,
        cipher: crate::storage::CloudCipher,
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
        home: Arc<dyn CloudHome>,
    ) -> Result<(), SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        let cipher = self
            .security
            .resolve_cloud_cipher(self.config().cloud_home.storage)?;
        self.replace_with_test_home(home, cipher, SyncDriver::Loop)
            .await?;
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

    pub(super) async fn ensure_command_store(&self) -> Result<(), SyncError> {
        if matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::WithCloud { .. } | SyncConnection::CommandOnly { .. }
        ) {
            return Ok(());
        }
        let config = self.command_config();
        let storage = self
            .cloud_storage
            .open(&config, None, None)
            .await
            .map_err(SyncError::StorageSetup)?;
        let store = self
            .security
            .established_identity()?
            .load_store(
                self.database.clone(),
                Arc::new(storage),
                self.store_dir.clone(),
            )
            .await
            .map_err(SyncError::from)?;
        *self.state.write().expect("write Store sync connection") = SyncConnection::CommandOnly {
            store: Arc::new(store),
        };
        Ok(())
    }
}
