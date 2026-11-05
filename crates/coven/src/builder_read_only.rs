//! Open a store for reading only, beside a handle that has it open.

use super::*;

impl CovenBuilder {
    /// Opens the store for reading only, alongside a handle that has it open.
    /// Its shared lock protects both read connections and local cache metadata.
    /// It runs no migration and refuses a database whose schema is newer,
    /// whose coven tables need migrating, or whose indexes were built with
    /// other versions of the app's collations and functions.
    /// File reads connect using saved settings and custody credentials when present.
    pub async fn open_read_only(
        mut self,
        store: StoreId,
    ) -> Result<CovenReadHandle, ReadOnlyOpenError> {
        let directory = self.directories.store_dir(&store);
        let ids = self.ids.clone();
        let clock = self.clock.clone();
        // A read-only handle cannot persist a cooldown; it holds its own requests.
        let waits = Arc::new(coven_sync::RetryWaits::in_memory(clock.clone()));
        let connector = self.connector();
        let transfers = self.transfers.clone();
        let database = self.database(directory.clone())?;
        // Before the swap's marker exists, only the record shows that a
        // replacement is moving the files a reader would open (§19.3).
        refuse_unfinished_replacement(&directory, store)?;
        refuse_unfinished_deletion(&directory, store)?;
        let database = database.open_read_only().await?;
        if let Some(identity) = database.store_state().await?.store {
            check_store_settings(&identity, &directory)?;
        }
        let device = directory.settings().map_err(CovenError::from)?.device_id;
        let oauth = self.oauth_exchange();
        let in_directory = directory.clone();
        let (credentials, data) = crate::task::blocking("read the saved credentials", move || {
            let credentials = device_credentials(self.keychain()?, store, in_directory, oauth);
            let data = credentials.connection()?;
            Ok::<_, SyncError>((credentials, data))
        })
        .await
        .map_err(SyncError::from)??;
        let storage: Option<Arc<dyn Storage>> = match data {
            Some(data) => {
                let storage = connector
                    .connect(
                        data.location,
                        coven_storage::ProviderAuth::new(data.credentials, credentials),
                        device,
                        Arc::new(crate::storage::NoPublications),
                        waits.clone(),
                        transfers.clone(),
                    )
                    .await
                    .map_err(SyncError::from)?;
                storage.check_bucket(store).await.map_err(SyncError::from)?;
                Some(storage)
            }
            None => None,
        };
        let files = coven_sync::Files::read_only(
            coven_database::FileDatabase::new(database.clone()),
            directory,
            storage,
            Arc::new(coven_sync::SequenceState::new()),
            waits,
            ids,
            transfers,
            clock,
            coven_foundation::network_watch::NetworkWatch::unwatched().view(),
        );
        Ok(CovenReadHandle::new(database, files))
    }
}
