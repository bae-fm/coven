//! Application calls delegate to database, custody, operation and file owners.

#[path = "file.rs"]
mod file;

use crate::*;
use coven_crypto::custody::StoreCustody;
use coven_database::Database;
use std::sync::{Arc, Mutex};

/// A shared handle to the open store and its running work (§21.2).
#[derive(Clone)]
pub struct CovenHandle {
    database: Database,
    operations: coven_sync::Operations,
    files: coven_sync::Files,
    custody: Arc<Mutex<Option<StoreCustody>>>,
}

impl CovenHandle {
    pub(crate) fn new(
        database: Database,
        custody: StoreCustody,
        operations: coven_sync::Operations,
        files: coven_sync::Files,
    ) -> Self {
        Self {
            database,
            operations,
            files,
            custody: Arc::new(Mutex::new(Some(custody))),
        }
    }

    /// Active members and their active devices from the local store log.
    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        self.operations.get_members().await
    }
    /// Set a member's role as an admin.
    pub async fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<(), SyncError> {
        self.operations.set_member_role(member, role).await
    }
    /// Remove a member, rotate keys and settle their storage access.
    pub async fn remove_member(&self, member: &MemberId) -> Result<MemberRemoval, SyncError> {
        self.operations.remove_member(member).await
    }
    /// Remove a device and return instructions for its provider sign-out.
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError> {
        self.operations.remove_device(device).await
    }
    /// Confirm deletion of an S3 key in the provider console.
    pub async fn confirm_access_key_deleted(&self, key: &str) -> Result<(), SyncError> {
        self.operations.confirm_access_key_deleted(key).await
    }
    /// Retry a permanently failed operation from its next unfinished step.
    pub async fn retry_blocked_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.operations.retry_blocked_operation(operation).await
    }
    /// Abandon a failed operation after publishing any reserved entry.
    pub async fn discard_blocked_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.operations.discard_blocked_operation(operation).await
    }
    /// Share access and create an invitation expiring after a day.
    pub async fn create_invite(
        &self,
        role: MemberRole,
        access: InviteAccess,
    ) -> Result<Invite, SyncError> {
        self.operations.create_invite(role, access).await
    }
    /// Checked join requests, starting with the current list.
    pub fn subscribe_join_requests(&self) -> tokio::sync::watch::Receiver<Vec<JoinRequest>> {
        self.operations.subscribe_join_requests()
    }
    /// Approve the exact request shown by the subscription.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.operations.approve_join_request(request).await
    }
    /// Decline a request and revoke its invitation's access.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.operations.decline_join_request(request).await
    }
    /// Cancel an unsettled invitation and revoke its access.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError> {
        self.operations.cancel_invite(invite).await
    }
    /// Circle calls borrowing this store's operation owner.
    pub fn circles(&self) -> Circles<'_> {
        Circles::new(&self.operations)
    }

    /// Runs one write.
    pub async fn write<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.database.write_with_files_result(|_| Ok(()), sql).await
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
        self.database.write_with_files_result(build, sql).await
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

    /// Every lost value and removed row, as `coven_lost` holds them (§8).
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.database.lost_values().await
    }

    /// Dismisses lost values the app has dealt with, in a write, so every
    /// device drops them from `coven_lost`; a removed row is deleted for
    /// good, and never comes back (§8).
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

    /// Opens a file for reading ranges (§16.3). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    /// Its shared store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError> {
        self.files.open_file_stream(file).await
    }

    /// Checks a local file, or downloads and checks an uploaded file through
    /// the cache. Missing chunks require connected storage.
    pub async fn ensure_file_on_device(&self, file: &FileRef) -> Result<(), FileReadError> {
        self.files.ensure_file_on_device(file).await
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

    /// Makes this member's two key pairs and puts them in identity custody,
    /// for the person creating a store. Fails if custody already holds keys.
    /// Joining and restoring put the keys there themselves.
    pub fn initialize_identity(&self) -> Result<MemberId, IdentityError> {
        self.custody
            .lock()
            .expect("custody lock poisoned")
            .as_mut()
            .ok_or(KeyError::StoreClosed)?
            .initialize_identity()
    }

    /// Removes the store keys from key custody. A failed removal leaves the
    /// custody failure visible to the caller; member identity is retained.
    pub async fn forget_store_keys(&self) -> Result<(), KeyError> {
        let custody = self.custody.clone();
        crate::coven::blocking(move || {
            custody
                .lock()
                .expect("custody lock poisoned")
                .as_mut()
                .ok_or(KeyError::StoreClosed)?
                .forget_store_keys()
        })
        .await
    }

    /// Keeps an app secret, such as an API token, in the same keychain and
    /// under the same access policy as coven's keys. Names can't be empty,
    /// contain `:`, or match one of coven's own entries.
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
    /// Deletes the secret; succeeds if it was never set.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.custody
            .lock()
            .expect("custody lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .delete_host_secret(name)
    }

    /// Encrypts the app's own data with the current store key, for the app to
    /// keep in its rows, since the local database is not encrypted. `aad`
    /// binds it to its place, such as the row's key. Reads the key id from the
    /// committed store log, then unlocks its bytes from custody. Without a
    /// selected key, returns `CovenError::Seal(SealError::NoCurrentStoreKey)`.
    /// Database reads and custody work run off the async executor.
    pub async fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> CovenResult<Vec<u8>> {
        let key = self
            .database
            .current_store_key()
            .await?
            .ok_or(SealError::NoCurrentStoreKey)?;
        let custody = self.custody.clone();
        let plaintext = SecretBytes::new(plaintext.to_vec());
        let aad = aad.to_vec();
        crate::coven::blocking(move || {
            Ok(custody
                .lock()
                .expect("custody lock poisoned")
                .as_ref()
                .ok_or(KeyError::StoreClosed)?
                .seal_app_data(key, plaintext.as_bytes(), &aad)?)
        })
        .await
    }

    /// Decrypts what `seal_app_data` made, with the store key it names, so it
    /// still opens after the key is replaced. Fails with a different `aad`.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        self.custody
            .lock()
            .expect("custody lock poisoned")
            .as_ref()
            .ok_or(KeyError::StoreClosed)?
            .open_app_data(sealed, aad)
    }

    /// Stops operation and file work, closes connections and releases the writer
    /// lock. Open file streams retain their shared deletion guards. Later
    /// database calls on any clone fail with `DbError::StoreClosed`; custody
    /// calls fail with `KeyError::StoreClosed`. Closing reports every failure.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        crate::coven::completion(tokio::spawn(async move {
            handle.files.close().await;
            match handle.operations.close().await {
                Ok(()) | Err(SyncError::Database(DbError::StoreClosed)) => (),
                Err(error) => return Err(DbError::OperationWorker(Box::new(error))),
            }
            let custody = handle.custody.clone();
            crate::coven::blocking(move || {
                custody.lock().expect("custody lock poisoned").take();
            })
            .await;
            handle.database.close().await
        }))
        .await
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
