//! Application calls delegate to database, custody, operation and file owners.

#[path = "file.rs"]
mod file;

#[path = "storage_credentials.rs"]
mod credentials;

#[path = "handle_sync.rs"]
mod sync_control;

#[path = "member_keys.rs"]
mod member_keys;

#[path = "handle_delete.rs"]
mod deletion;
pub use deletion::IfUnsent;

#[path = "handle_folders.rs"]
mod folders;

#[path = "handle_file_cleanup.rs"]
mod file_cleanup;

#[path = "handle_device_only.rs"]
mod device_only;

#[path = "handle_transfers.rs"]
mod transfers;

#[path = "handle_uploads.rs"]
mod uploads;

#[path = "handle_writes.rs"]
mod writes;

use crate::*;
use coven_crypto::custody::{StoreCustody, StoreKeychain};
use coven_database::Database;
use std::sync::{Arc, Mutex};

/// A shared handle to the open store and its running work (Appendix F.4).
/// Administrative changes catch up with storage before starting and report
/// a refusal by the applied state as `SyncError::Rejected`, including its
/// typed target.
#[derive(Clone)]
pub struct CovenHandle {
    closing: Arc<coven_foundation::files::StoreClosingGuard>,
    database: Database,
    files: coven_sync::Files,
    storage: Arc<crate::storage::StorageConnections>,
    custody: Arc<Mutex<Option<StoreCustody>>>,
    directory: StoreDir,
    keychain: Arc<StoreKeychain>,
    device: DeviceId,
}

impl std::fmt::Debug for CovenHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CovenHandle").finish_non_exhaustive()
    }
}

impl CovenHandle {
    pub(crate) fn new(
        database: Database,
        custody: StoreCustody,
        files: coven_sync::Files,
        storage: Arc<crate::storage::StorageConnections>,
        directory: StoreDir,
        keychain: Arc<StoreKeychain>,
        device: DeviceId,
    ) -> Self {
        Self {
            closing: Arc::new(database.closing_guard()),
            database,
            files,
            storage,
            custody: Arc::new(Mutex::new(Some(custody))),
            directory,
            keychain,
            device,
        }
    }

    /// The store's stable id, including before storage is configured.
    pub fn store_id(&self) -> StoreId {
        self.directory.id()
    }

    /// The store's current name, from this device's committed state (E9).
    pub async fn store_name(&self) -> CovenResult<String> {
        self.database.store_name().await
    }

    /// The same name query, live (E4).
    pub fn subscribe_store_name(&self) -> LiveQuery<String> {
        self.database.subscribe_store_name()
    }

    /// Renames the store, in a write of its own (`CovenWrite::rename_store`).
    pub async fn rename_store(&self, name: &str) -> Result<Option<OperationId>, SyncError> {
        let name = name.to_owned();
        self.write(move |write| Ok(write.rename_store(&name)?)).await.map_err(SyncError::from)
    }

    /// This installation's device id, available before setup and retained after close.
    pub fn device_id(&self) -> DeviceId {
        self.device
    }

    /// The committed storage location, without credentials or provider requests.
    /// None before setup; disconnect retains it. A closed handle refuses the call.
    pub async fn storage_config(&self) -> Result<Option<StorageConfig>, SyncError> {
        self.storage.storage_config().await
    }

    /// How many file uploads and file downloads run at once. Each upload checks the
    /// upload limit as it starts; a pin captures the download limit.
    pub fn transfer_limits(&self) -> TransferLimits {
        self.files.transfer_limits()
    }
    /// Set limits for future transfers; transfers already running continue.
    pub fn set_transfer_limits(&self, limits: TransferLimits) {
        self.files.set_transfer_limits(limits);
    }

    /// Check S3 safety and provider operations, then set up S3 using this
    /// member's own access key. The returned location is the recorded one,
    /// carrying detected settings when the endpoint needed them.
    /// `on_progress` is called on this task with each step as it runs (E1).
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
        on_progress: impl Fn(Progress<StorageSetupStep>),
    ) -> Result<StorageSetup, StorageSetupError> {
        let (progress, watched) = tokio::sync::watch::channel(None);
        let setup = self.setup_s3_reporting(
            storage,
            device_name,
            access_key_id,
            secret_access_key,
            progress,
        );
        coven_foundation::progress::forward(watched, setup, on_progress).await
    }

    /// The S3 setup [`Self::setup_s3_storage`] runs, reporting its steps
    /// into `progress` as it runs them.
    pub(crate) async fn setup_s3_reporting(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
        progress: crate::storage::SetupProgress,
    ) -> Result<StorageSetup, StorageSetupError> {
        self.storage
            .setup_s3(storage, device_name, access_key_id, secret_access_key, progress)
            .await
    }
    /// Probe the S3 service at this location with this member's key (§20.7),
    /// writing only fresh objects under the store's `.coven-check` prefix and
    /// deleting them, also on failure. Commits nothing.
    pub async fn detect_s3_settings(
        &self,
        storage: StorageConfig,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<S3Detection, StorageSetupError> {
        self.storage.detect_s3(storage, access_key_id, secret_access_key).await
    }
    /// Present provider sign-in and keep tokens in coven's session custody.
    /// Call setup to attach the chosen account to storage and persist credentials.
    /// Dropping the future cancels sign-in without replacing the previous tokens.
    pub async fn authenticate(&self, access: OAuthAccess) -> Result<(), StorageSetupError> {
        self.storage.authenticate(access).await
    }
    /// Check provider operations and set up OAuth storage using coven's held sign-in.
    /// `on_progress` is called on this task with each step as it runs (E1).
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        on_progress: impl Fn(Progress<StorageSetupStep>),
    ) -> Result<StorageSetup, StorageSetupError> {
        let (progress, watched) = tokio::sync::watch::channel(None);
        let setup = self.storage.setup_oauth(storage, device_name, progress);
        coven_foundation::progress::forward(watched, setup, on_progress).await
    }
    /// Set up native iCloud storage in the entitled container. Coven obtains the
    /// owner and creates a custom zone; reconnects retain the recorded location.
    /// Unsupported platforms return `StorageFailure::ProviderUnavailable`.
    /// `on_progress` is called on this task with each step as it runs (E1).
    pub async fn setup_cloudkit_storage(
        &self,
        container: &str,
        device_name: &str,
        on_progress: impl Fn(Progress<StorageSetupStep>),
    ) -> Result<StorageSetup, StorageSetupError> {
        let (progress, watched) = tokio::sync::watch::channel(None);
        let setup = self.storage.setup_cloudkit(container, device_name, progress);
        coven_foundation::progress::forward(watched, setup, on_progress).await
    }
    /// Open this member's sealed keys and connect without starting the loop.
    pub async fn unlock_store_key(&self) -> Result<ConnectedStorage, StoreKeyUnlockError> {
        self.storage.unlock().await
    }
    /// Whether custody holds opened store keys.
    pub fn store_key_state(&self) -> Result<StoreKeyState, KeyError> {
        self.storage.key_state()
    }
    /// Finish the active pass, forget credentials, then release the provider.
    /// A custody failure preserves the current connection and loop state.
    pub async fn disconnect_storage(&self) -> Result<(), SyncError> {
        self.storage.disconnect().await
    }

    /// The latest completed replacement stored in this installation, or None.
    /// It survives reopening; unsent edits of the old copy were discarded (§19.3).
    pub async fn device_reset(&self) -> CovenResult<Option<DeviceReset>> {
        self.database.device_reset().await
    }

    /// The same pre-built query, including a replacement completed before opening.
    pub fn subscribe_device_reset(&self) -> LiveQuery<Option<DeviceReset>> {
        self.database.subscribe_device_reset()
    }

    /// Current local conditions, including while disconnected, each with
    /// the cause `coven_pending.cause` keeps beside it.
    pub async fn pending(&self) -> CovenResult<Vec<PendingRecord>> {
        self.database.pending().await
    }

    /// The members and their devices, as this device's applied slots have them (E9).
    pub async fn members(&self) -> CovenResult<Vec<MemberInfo>> {
        self.database.members().await
    }

    /// The same internal query, live (E4).
    pub fn subscribe_members(&self) -> LiveQuery<Vec<MemberInfo>> {
        self.database.subscribe_members()
    }

    /// Changes a member's role, in a write of its own
    /// (`CovenWrite::set_member_role`).
    pub async fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<Option<OperationId>, SyncError> {
        let member = *member;
        self.write(move |write| Ok(write.set_member_role(&member, role)?))
            .await
            .map_err(SyncError::from)
    }
    /// Removes a member, in a write of its own (`CovenWrite::remove_member`).
    pub async fn remove_member(&self, member: &MemberId) -> Result<Option<OperationId>, SyncError> {
        let member = *member;
        self.write(move |write| Ok(write.remove_member(&member)?)).await.map_err(SyncError::from)
    }
    /// Removes a device, in a write of its own (`CovenWrite::remove_device`).
    pub async fn remove_device(
        &self,
        device: DeviceId,
    ) -> Result<(Option<OperationId>, Option<ProviderSignOut>), SyncError> {
        self.write(move |write| Ok(write.remove_device(device)?)).await.map_err(SyncError::from)
    }
    /// Waits for an operation the app's writes journaled to end and returns
    /// its outcome, as `coven_operations` lists it (§11.4, E6); the outcome
    /// stays listed until a write acknowledges it. It waits for storage
    /// too: a pass runs the operation once storage is connected. An
    /// operation not listed fails with `OperationUnknown`.
    pub async fn operation_outcome(
        &self,
        operation: OperationId,
    ) -> Result<OperationOutcome, SyncError> {
        let mut listed = self.database.subscribe(move |sql| {
            Ok(sql
                .query(
                    "SELECT outcome FROM coven_operations WHERE operation=?1",
                    [operation.0],
                    |row| row.get::<_, Option<OperationOutcome>>(0),
                )?
                .pop())
        });
        while let Some(listed) = listed.next().await {
            match listed? {
                None => return Err(DbError::OperationUnknown(operation).into()),
                Some(Some(outcome)) => return Ok(outcome),
                Some(None) => {}
            }
        }
        Err(DbError::StoreClosed.into())
    }
    /// The bucket, store prefix and filled member policy for making a key in
    /// the provider's console (§20.10).
    pub async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError> {
        self.storage.guided_access_key().await
    }
    /// Reload this device from snapshots, retaining and replaying its waiting writes (§19.2).
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError> {
        // An operation's state runs to tens of KiB; boxed, the future the
        // app holds stays small (application_api_tests bounds it). The other
        // operations below are boxed for the same reason.
        Box::pin(self.storage.reload_from_snapshot()).await
    }
    /// Reset the store audience from this device as an admin, then reload locally (§19.4).
    pub async fn reset_store(&self) -> Result<(), SyncError> {
        self.storage.reset(Audience::Store).await
    }
    /// Make an operation's retry or undo due now, from its recorded
    /// progress, in a write of its own (`CovenWrite::retry_pending_operation`).
    pub async fn retry_pending_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.write(move |write| Ok(write.retry_pending_operation(operation)?))
            .await
            .map_err(OperationError::from)
    }
    /// Fixes a fresh member id or an existing active replacement target, as
    /// an admin, then shares access and creates an invitation. An in-person
    /// invite expires an hour after it is made; a sent one after the lifetime
    /// chosen. Replacement preserves role, circle memberships and device ids.
    pub async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        delivery: InviteDelivery,
    ) -> Result<Invite, SyncError> {
        Box::pin(self.storage.create_invite(target, access, delivery)).await
    }
    /// Follow `coven_invites` with a live query, as an app does (E10).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_invites(&self) -> coven_database::test_utils::Followed<Vec<Invite>> {
        Invite::follow(&self.database)
    }
    /// Approves a request, in a write of its own
    /// (`CovenWrite::approve_join_request`).
    pub async fn approve_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<OperationId, SyncError> {
        let request = request.clone();
        self.write(move |write| Ok(write.approve_join_request(&request)?))
            .await
            .map_err(SyncError::from)
    }
    /// Declines a request, in a write of its own
    /// (`CovenWrite::decline_join_request`).
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        let request = request.clone();
        self.write(move |write| Ok(write.decline_join_request(&request)?))
            .await
            .map_err(SyncError::from)
    }
    /// Cancels an invite, in a write of its own (`CovenWrite::cancel_invite`).
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError> {
        let invite = *invite;
        self.write(move |write| Ok(write.cancel_invite(&invite)?)).await.map_err(SyncError::from)
    }
    /// Circle calls borrowing this store's operation owner.
    pub fn circles(&self) -> Circles<'_> {
        Circles::new(self, &self.storage, &self.database)
    }

    /// A read of one consistent snapshot, run when awaited. Attach `process`
    /// to work on the result after the connection is released.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.read(read)
    }

    /// A live query. Coven records the tables, columns and keys the query
    /// reads, and reruns it only for writes that touch them.
    pub fn subscribe<F, R>(&self, query: F) -> LiveQuery<R>
    where
        F: Fn(SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        self.database.subscribe(query)
    }

    /// A live query whose request, such as a page or a search term, can be
    /// replaced without starting a new subscription.
    pub fn subscribe_reconfigurable<Q, F, R>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        self.database.subscribe_reconfigurable(initial_request, query)
    }

    /// The SQLite settings the store's connections have in effect (§4).
    pub async fn sqlite_settings(&self) -> Result<SqliteSettingsInEffect, DbError> {
        self.database.sqlite_settings().await
    }

    /// Checkpoints the store's WAL between writes, returning what SQLite
    /// reports (§4). `Restart` and `Truncate` wait up to the busy timeout for
    /// reads still using the WAL, and report `busy` if they outlast it.
    /// `NotStarted` when another connection was checkpointing or committing.
    pub async fn checkpoint(&self, mode: CheckpointMode) -> Result<WalCheckpoint, DbError> {
        self.database.checkpoint(mode).await
    }

    /// Gives back free pages of a store created with incremental auto-vacuum,
    /// between writes (§4). On any other store it does nothing.
    pub async fn incremental_vacuum(&self, pages: IncrementalVacuum) -> Result<(), DbError> {
        self.database.incremental_vacuum(pages).await
    }

    /// Gives back the memory every connection's page cache holds, as SQLite's
    /// `sqlite3_db_release_memory` does, for when the OS warns of low memory
    /// or the app goes to the background (§4). SQLite keeps every
    /// connection's unused pages in one pool for the process, so one release
    /// on an idle reader frees them all. Safe at any time, and never waits
    /// for a write. Later reads load the pages they need again.
    pub async fn release_memory(&self) -> Result<(), DbError> {
        self.database.release_memory().await
    }

    /// Every lost value and removed row, as `_coven_lost` holds them (§8.2, §8.4).
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.database.lost_values().await
    }

    /// Dismisses lost values in a write of its own
    /// (`CovenWrite::dismiss_lost_values`).
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()> {
        let values = values.to_vec();
        self.write(move |write| Ok(write.dismiss_lost_values(&values)?)).await
    }

    /// The same, as a live query.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>> {
        self.database.subscribe_lost_values()
    }

    /// The file a row references, by the id the row holds: its size,
    /// namespace, audience and place, read in one committed state. An id this
    /// device has no record of returns `DbError::FileAbsent`; a malformed
    /// record returns `DbError::DamagedDatabase`.
    pub async fn file_ref(&self, file: FileId) -> Result<FileRef, DbError> {
        self.database.file_ref(file).await
    }

    /// Reads a whole file. A stored file is checked against its content hash.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError> {
        let stream = self.open_file_stream(file).await?;
        stream.read_at(0, stream.plaintext_size()).await
    }

    /// Opens a file for reading ranges (§17.7), as its record stands when it
    /// opens; keep the stream for as long as the file is read. A file only on
    /// another device fails with `FileReadError::DeviceOnly`. Its shared store
    /// lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.files.open_file_stream(file).await
    }

    /// The path, size and modification time coven recorded for a file's
    /// user original on this device, or `None` when it has none.
    pub async fn user_file(&self, file: FileId) -> Result<Option<UserFile>, DbError> {
        self.database.user_file(file).await
    }

    /// Removes the store keys from key custody. A failed removal leaves the
    /// custody failure visible to the caller; member identity is retained.
    pub async fn forget_store_keys(&self) -> Result<(), KeyError> {
        self.storage.forget_store_keys().await
    }

    /// Keeps an app secret, such as an API token, in the same keychain and
    /// under the same access policy as coven's keys. Names are arbitrary strings.
    /// Coven records the name in a keychain list before writing the value to its
    /// own entry, retaining the platform's per-secret size limit. Deleting the
    /// store removes every listed secret even if its database is damaged.
    /// Failure may leave an extra name; retrying is safe.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.custody
            .lock()
            .expect("custody lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .set_host_secret(name, value)
    }
    /// The secret, or `None` if it was never set.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.custody
            .lock()
            .expect("custody lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .host_secret(name)
    }
    /// Deletes the secret before its recorded name; succeeds if absent.
    /// Failure may leave an extra name; retrying is safe.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.custody
            .lock()
            .expect("custody lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .delete_host_secret(name)
    }

    /// Stops operation and file work, closes connections and releases the writer
    /// lock. Open file streams retain their shared deletion guards. Later
    /// database calls on any clone fail with `DbError::StoreClosed`; custody
    /// calls fail with `KeyError::StoreClosed`. Closing reports every failure:
    /// one alone as itself, several chained as `SyncError::Cleanup`.
    pub async fn close(&self) -> Result<(), SyncError> {
        self.closing.begin_close();
        let handle = self.clone();
        let closing = tokio::spawn(async move {
            let storage = handle.storage.close().await;
            handle.files.close().await;
            let custody = handle.custody.clone();
            let custody = crate::coven::blocking("close custody", move || {
                if let Some(custody) = custody.lock().expect("custody lock poisoned").take() {
                    custody.close();
                }
            })
            .await;
            let database = handle.database.close().await;
            // Closing the connection can fail while releasing its provider.
            // Finish closing every owner and erase custody before reporting.
            let storage = match storage {
                Err(SyncError::Database(DbError::StoreClosed)) => Ok(()),
                result => result,
            };
            [storage, custody.map_err(SyncError::from), database.map_err(SyncError::from)]
                .into_iter()
                .filter_map(Result::err)
                .reduce(|operation, cleanup| SyncError::Cleanup {
                    operation: Box::new(operation),
                    cleanup: Box::new(cleanup),
                })
                .map_or(Ok(()), Err)
        });
        crate::coven::completion("close the store", closing).await?
    }
    /// The last slot this installation applied.
    #[cfg(test)]
    pub(crate) async fn applied_slot(&self) -> u64 {
        self.database.applied_slot().await.unwrap()
    }
    /// Membership, keys and audience state as this installation applied them.
    #[cfg(test)]
    pub(crate) async fn store_state(&self) -> coven_database::StoreState {
        self.database.store_state().await.unwrap()
    }
    #[cfg(test)]
    pub(crate) async fn test_connection_retry_wait(&self) -> Option<coven_database::RetryWait> {
        self.database.retry_wait(coven_database::RetrySubject::Sync).await.unwrap()
    }

    /// Count the work the store's connections run into `counter`, in
    /// application tests: its reads' SQLite steps, the app's and coven's
    /// narrowing apart. Install it between calls, before the measured one.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn test_track_work(&self, counter: crate::WorkCounter) -> Result<(), DbError> {
        self.database.test_track_work(counter).await
    }

    /// Read committed plaintext records still awaiting upload in application tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError> {
        self.database.test_queued_writes().await
    }
}

#[cfg(test)]
impl CovenHandle {
    /// Where the waiting upload of `file` sends its bytes, for tests that
    /// fault that path.
    pub(crate) async fn test_upload_location(&self, file: FileId) -> coven_database::FileLocation {
        self.database
            .upload_queue()
            .await
            .unwrap()
            .uploads
            .into_iter()
            .find(|upload| upload.files.contains(&file))
            .expect("an upload stores the file")
            .location
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
