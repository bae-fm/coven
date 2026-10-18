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

    /// Set the store's name as an admin, judged before the rename's slot (E9).
    pub async fn rename_store(&self, name: &str) -> Result<(), SyncError> {
        self.storage
            .administer(coven_format::admin::StoreChange::RenameStore { name: name.into() })
            .await
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

    /// Limits captured when each file upload drain or pin starts.
    pub fn transfer_limits(&self) -> TransferLimits {
        self.files.transfer_limits()
    }
    /// Set limits for future transfers; active batches retain their initial limits.
    pub fn set_transfer_limits(&self, limits: TransferLimits) {
        self.files.set_transfer_limits(limits);
    }

    /// Check S3 safety and provider operations, then set up S3 using this
    /// member's own access key. The returned location is the recorded one,
    /// carrying detected settings when the endpoint needed them.
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<StorageSetup, StorageSetupError> {
        self.storage
            .setup_s3(storage, device_name, access_key_id, secret_access_key)
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
        self.storage
            .detect_s3(storage, access_key_id, secret_access_key)
            .await
    }
    /// Present provider sign-in and keep tokens in coven's session custody.
    /// Call setup to attach the chosen account to storage and persist credentials.
    /// Dropping the future cancels sign-in without replacing the previous tokens.
    pub async fn authenticate(&self, access: OAuthAccess) -> Result<(), StorageSetupError> {
        self.storage.authenticate(access).await
    }
    /// Check provider operations and set up OAuth storage using coven's held sign-in.
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<StorageSetup, StorageSetupError> {
        self.storage.setup_oauth(storage, device_name).await
    }
    /// Set up native iCloud storage in the entitled container. Coven obtains the
    /// owner and creates a custom zone; reconnects retain the recorded location.
    /// Unsupported platforms return `StorageFailure::ProviderUnavailable`.
    pub async fn setup_cloudkit_storage(
        &self,
        container: &str,
        device_name: &str,
    ) -> Result<StorageSetup, StorageSetupError> {
        self.storage.setup_cloudkit(container, device_name).await
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

    /// Current local conditions, including while disconnected.
    pub async fn pending(&self) -> CovenResult<Vec<PendingRecord>> {
        self.database.pending().await
    }

    /// Observe the same committed pending query.
    pub fn subscribe_pending(&self) -> LiveQuery<Vec<PendingRecord>> {
        self.database.subscribe_pending()
    }

    /// The members and their devices, as this device's applied slots have them (E9).
    pub async fn members(&self) -> CovenResult<Vec<MemberInfo>> {
        self.database.members().await
    }

    /// The same internal query, live (E4).
    pub fn subscribe_members(&self) -> LiveQuery<Vec<MemberInfo>> {
        self.database.subscribe_members()
    }

    /// Set a member's role as an admin.
    pub async fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<(), SyncError> {
        self.storage
            .administer(coven_format::admin::StoreChange::ChangeRole {
                member: *member,
                role,
            })
            .await
    }
    /// Publish removal and return once its slot commits. Provider work is
    /// reported through pending(), independently of the removal call (§14.2).
    pub async fn remove_member(&self, member: &MemberId) -> Result<(), SyncError> {
        self.storage
            .administer(coven_format::admin::StoreChange::RemoveMember { member: *member })
            .await
    }
    /// Remove a device and return instructions for its provider sign-out.
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError> {
        self.storage.remove_device(device).await
    }
    /// Explicitly confirms that this S3 key was deleted, including through the
    /// provider's console. Clears only this id's DeleteAccessKey record and
    /// keeps the confirmation (§14.3).
    pub async fn confirm_access_key_deleted(&self, key: &str) -> Result<(), SyncError> {
        Ok(self.database.clear_access_key(key.into()).await?)
    }
    /// Obtains the storage account's admin access from the source the person
    /// chose. It is held in memory for the operations it is passed to and is
    /// never stored, logged or put in a slot or invite (§20.10).
    pub async fn admin_access(
        &self,
        source: AdminAccessSource,
    ) -> Result<Arc<dyn AccessKeyAdmin>, SyncError> {
        self.storage.admin_access(source).await
    }
    /// The bucket, store prefix and filled member policy for making a key in
    /// the provider's console (§20.10).
    pub async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError> {
        self.storage.guided_access_key().await
    }
    /// Deletes the S3 key a pending DeleteAccessKey record names, with admin
    /// access, then reads the provider for that exact key. Only its absence
    /// clears the record; every failure is returned and the record stays (§14.3).
    pub async fn delete_access_key(
        &self,
        operation: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        self.storage.delete_access_key(operation, admin).await
    }
    /// Replaces a member's S3 key with admin access (§14.3): makes the key and
    /// checks it from the member's side, delivers it sealed to the member's
    /// devices, records it for them, then deletes the old key and reads it
    /// gone. The member's own key, or another member's as an admin. The
    /// returned key is for the person to keep; the old key's deletion outcome
    /// is reported, and a key it could not delete stays pending.
    pub async fn replace_member_access_key(
        &self,
        member: &MemberId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<AccessKeyReplacement, SyncError> {
        self.storage.replace_member_access_key(*member, admin).await
    }
    /// Resumes work waiting with `AdminAccessRequired`, holding this admin
    /// access for that operation only (§20.10).
    pub async fn retry_with_admin_access(
        &self,
        operation: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), OperationError> {
        self.storage.retry_with_admin_access(operation, admin).await
    }
    /// Reload this device from snapshots, retaining and replaying its waiting writes (§19.2).
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError> {
        self.storage.reload_from_snapshot().await
    }
    /// Reset the store audience from this device as an admin, then reload locally (§19.4).
    pub async fn reset_store(&self) -> Result<(), SyncError> {
        self.storage.reset(Audience::Store).await
    }
    /// Make an operation's retry or undo due now, from its recorded progress.
    /// It cannot resurrect an invalidated action or bypass provider cooldown.
    pub async fn retry_pending_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.storage.retry_pending_operation(operation).await
    }
    /// Fixes a fresh member id or an existing active replacement target, as
    /// an admin, then shares access and creates an invitation expiring after a
    /// day. Replacement preserves role, circle memberships and device ids.
    pub async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
    ) -> Result<Invite, SyncError> {
        self.storage.create_invite(target, access).await
    }
    /// Checked join requests, starting with the current list.
    pub fn subscribe_join_requests(&self) -> tokio::sync::watch::Receiver<Vec<JoinRequest>> {
        self.storage.subscribe_join_requests()
    }
    /// Approve the exact request shown by the subscription.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.storage.approve_join_request(request).await
    }
    /// Decline a request and revoke its invitation's access.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.storage.decline_join_request(request).await
    }
    /// Cancel an unsettled invitation and revoke its access.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError> {
        self.storage.cancel_invite(*invite).await
    }
    /// Circle calls borrowing this store's operation owner.
    pub fn circles(&self) -> Circles<'_> {
        Circles::new(&self.storage, &self.database)
    }

    /// Runs one write.
    pub async fn write<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.write_with_files(|_| Ok(()), sql).await
    }

    /// Runs one write that also hands coven app-provided files. `build` adds
    /// the files, then `sql` runs the write that refers to them. Streams are
    /// read asynchronously before taking the transaction's writer; close waits
    /// for staging and its cleanup, including cancellation.
    pub async fn write_with_files<F, S, R>(&self, build: F, sql: S) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch) -> CovenResult<()> + Send + 'static,
        S: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.write_with_files(build, sql).await
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
        self.database
            .subscribe_reconfigurable(initial_request, query)
    }

    /// Every lost value and removed row, as `_coven_lost` holds them (§8.2, §8.4).
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.database.lost_values().await
    }

    /// Dismisses lost values the app has dealt with, in a write, so every
    /// device drops them from `_coven_lost`; a removed row is deleted for
    /// good, and never comes back (§8.4).
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()> {
        self.database.dismiss_lost_values(values).await
    }

    /// The same, as a live query.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>> {
        self.database.subscribe_lost_values()
    }

    /// The file a row carries, as of its current file version. Its four file
    /// columns, audience and version are read in one committed state. A row
    /// without a file returns `DbError::FileAbsent`; malformed stored
    /// file facts return `DbError::DamagedDatabase`.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError> {
        self.database.file_ref(table, key).await
    }

    /// Reads a whole file, checking it against its row.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError> {
        let stream = self.open_file_stream(file).await?;
        stream.read_at(0, stream.plaintext_size()).await
    }

    /// Opens a file for reading ranges (§17.5). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    /// Its shared store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.files.open_file_stream(file).await
    }

    /// The path, size and modification time coven recorded for a row's
    /// user-provided file, or `None` when the row has none.
    pub async fn user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
    ) -> Result<Option<UserFile>, DbError> {
        self.database.user_file(table, key).await
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
        crate::coven::completion(tokio::spawn(async move {
            let storage = handle.storage.close().await;
            handle.files.close().await;
            let custody = handle.custody.clone();
            crate::coven::blocking(move || {
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
            [storage, database.map_err(SyncError::from)]
                .into_iter()
                .filter_map(Result::err)
                .reduce(|operation, cleanup| SyncError::Cleanup {
                    operation: Box::new(operation),
                    cleanup: Box::new(cleanup),
                })
                .map_or(Ok(()), Err)
        }))
        .await
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
        self.database
            .retry_wait(coven_database::RetrySubject::Sync)
            .await
            .unwrap()
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
#[path = "handle_tests.rs"]
mod tests;
