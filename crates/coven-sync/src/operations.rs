//! Lifetime owner for durable operations. Dropping an app future drops only its waiter.

use crate::*;
use coven_crypto::MemberId;
use coven_database::OperationId;
use coven_format::store_log::MemberRole;
use coven_foundation::clock::ClockRef;
use coven_foundation::id_source::{CircleId, DeviceId, InviteId};
use coven_storage::{MemberRemoval, ProviderSignOut, Storage};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

/// Owns the operation worker and its lifetime. It starts retained work on opening;
/// it does not run the store's sync loop. The sync owner calls `sync` for a pass
/// and `set_storage` when a provider connects.
#[derive(Clone)]
pub struct Operations {
    inner: Arc<RunningOperations>,
}

struct RunningOperations {
    commands: mpsc::UnboundedSender<Request>,
    joins: watch::Receiver<Vec<JoinRequest>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub(crate) enum Command {
    Reload,
    Reset(coven_merge::Audience),
    RemoveMember(MemberId),
    CreateCircle(String),
    AddCircleMember(CircleId, MemberId),
    RemoveCircleMember(CircleId, MemberId),
    DeleteCircle(CircleId),
    RenameCircle(CircleId, String),
    SetRole(MemberId, MemberRole),
    SetAccess(coven_format::MemberAccess),
    RemoveDevice(DeviceId),
    Members,
    Circles,
    CircleMembers(CircleId),
    Invite(MemberRole, InviteAccess),
    Approve(JoinRequest),
    Decline(JoinRequest),
    Cancel(InviteId),
    Retry(OperationId),
    Discard(OperationId),
    ConfirmKey(String),
    Credentials {
        previous: coven_storage::ConnectionCredentials,
        next: coven_storage::ConnectionCredentials,
        require_connected: bool,
        commit: crate::StorageCommit,
    },
    #[cfg(any(test, feature = "test-utils"))]
    InspectPositions,
    CheckKeys,
    ForgetKeys,
    #[cfg(any(test, feature = "test-utils"))]
    Sync,
    SyncAll,
    Unlock(Arc<dyn Storage>),
    Setup(
        Arc<dyn Storage>,
        coven_format::MemberAccess,
        String,
        crate::StorageCommit,
    ),
    BlockedOperations,
    AccessKeysToDelete,
    Storage(Option<Arc<dyn Storage>>),
    Close,
}

pub(crate) enum Output {
    #[cfg(any(test, feature = "test-utils"))]
    Positions(Option<coven_format::objects::PostedPositions>),
    Unit,
    Removal(MemberRemoval),
    SignOut(ProviderSignOut),
    Members(Vec<MemberInfo>),
    CircleId(CircleId),
    Circles(Vec<Circle>),
    CircleMembers(Vec<CircleMemberInfo>),
    Invite(Invite),
    BlockedOperations(Vec<BlockedOperation>),
    AccessKeysToDelete(Vec<AccessKeyToDelete>),
}

type Reply = oneshot::Sender<Result<Output, SyncError>>;
struct Request {
    command: Command,
    reply: Reply,
}

pub(crate) enum Begun {
    Value(Output),
    Operation(OperationId),
}
pub(crate) enum Progress {
    Waiting,
    Advanced,
    Reply(Result<Output, SyncError>),
    Finished(Output),
}

/// One running task; all mutations of StoreLogSync happen here, in command order.
struct OperationRun {
    sync: StoreLogSync,
    files: Files,
    writes: DeviceLogSync,
    clock: ClockRef,
    commands: mpsc::UnboundedReceiver<Request>,
    joins: watch::Sender<Vec<JoinRequest>>,
    waiters: BTreeMap<OperationId, Reply>,
}

impl Operations {
    /// Compose and start the lifetime owner after the database opens. Schedule
    /// migration publication on opening; app failures await retry or discard.
    /// The store's injected clock drives retries without postponing them on commands.
    pub fn new(sync: StoreLogSync, files: Files, writes: DeviceLogSync, clock: ClockRef) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (joins, subscription) = watch::channel(Vec::new());
        let run = OperationRun {
            sync,
            files,
            writes,
            clock,
            commands: receiver,
            joins,
            waiters: BTreeMap::new(),
        };
        Self {
            inner: Arc::new(RunningOperations {
                commands,
                joins: subscription,
                task: Mutex::new(Some(tokio::spawn(run.run()))),
            }),
        }
    }

    async fn call(&self, command: Command) -> Result<Output, SyncError> {
        let (reply, result) = oneshot::channel();
        self.inner
            .commands
            .send(Request { command, reply })
            .map_err(|_| coven_database::DbError::StoreClosed)?;
        result
            .await
            .map_err(|_| coven_database::DbError::StoreClosed)?
    }

    /// Verify and publish setup before committing credentials and keys. The
    /// supplied commit returns compensation for a subsequent local failure.
    /// The worker retains the entire call even when its app waiter is dropped.
    pub async fn setup_storage(
        &self,
        storage: Arc<dyn Storage>,
        access: coven_format::MemberAccess,
        device_name: String,
        commit: crate::StorageCommit,
    ) -> Result<(), SyncError> {
        self.unit(Command::Setup(storage, access, device_name, commit))
            .await
    }

    pub(crate) async fn install_credentials(
        &self,
        previous: coven_storage::ConnectionCredentials,
        next: coven_storage::ConnectionCredentials,
        require_connected: bool,
        commit: crate::StorageCommit,
    ) -> Result<(), SyncError> {
        self.unit(Command::Credentials {
            previous,
            next,
            require_connected,
            commit,
        })
        .await
    }

    /// Acquire the configured store's current keys and check its store log without
    /// running a full pass. The previous connection survives a failed unlock.
    pub async fn unlock_storage(&self, storage: Arc<dyn Storage>) -> Result<(), SyncError> {
        self.unit(Command::Unlock(storage)).await
    }

    /// Verify the member and current store key before starting a connected loop.
    pub async fn check_sync_keys(&self) -> Result<(), SyncError> {
        self.unit(Command::CheckKeys).await
    }

    /// Forget custody after active work; a custody failure keeps the provider.
    pub(crate) async fn forget_store_keys(&self) -> Result<(), SyncError> {
        self.unit(Command::ForgetKeys).await
    }

    /// Supply or disconnect the provider capability; waiting operations continue
    /// using their persisted bytes. This does not enable a background sync loop.
    pub async fn set_storage(&self, storage: Option<Arc<dyn Storage>>) -> Result<(), SyncError> {
        self.unit(Command::Storage(storage)).await
    }
    /// Download and replay available store-log entries, then wake operation work.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn sync_store_log(&self) -> Result<(), SyncError> {
        self.unit(Command::Sync).await
    }
    /// Run one entire pass under the operation worker's serialization. Replay,
    /// reloads, writes and snapshots cannot interleave with another operation.
    pub async fn sync(&self) -> Result<(), SyncError> {
        self.unit(Command::SyncAll).await
    }
    /// Read failed app work, including while stopped. Maintenance failures go
    /// through the sync pass and retry on its next invocation.
    pub async fn blocked_operations(&self) -> Result<Vec<BlockedOperation>, SyncError> {
        match self.call(Command::BlockedOperations).await? {
            Output::BlockedOperations(operations) => Ok(operations),
            _ => unreachable!("blocked operations result"),
        }
    }
    /// Read S3 key deletions still awaiting confirmation, including while stopped.
    pub async fn access_keys_to_delete(&self) -> Result<Vec<AccessKeyToDelete>, SyncError> {
        match self.call(Command::AccessKeysToDelete).await? {
            Output::AccessKeysToDelete(keys) => Ok(keys),
            _ => unreachable!("access keys result"),
        }
    }
    /// Active members and devices from the local store log.
    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        match self.call(Command::Members).await? {
            Output::Members(value) => Ok(value),
            _ => unreachable!("member result"),
        }
    }
    /// Change a member's role as a plain store-log entry.
    pub async fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<(), SyncError> {
        self.unit(Command::SetRole(member.clone(), role)).await
    }
    /// Publish the current member's storage access before returning a replacement
    /// restore code. A failed publication retains its fixed bytes for explicit retry.
    pub(crate) async fn set_access(
        &self,
        access: coven_format::MemberAccess,
    ) -> Result<(), SyncError> {
        self.unit(Command::SetAccess(access)).await
    }
    /// Remove the member, rotate audience keys and revoke every recorded access.
    /// Returns current access's result, or retained grants from any account.
    /// `access_keys_to_delete` lists all recorded S3 keys until confirmed deleted.
    pub async fn remove_member(&self, member: &MemberId) -> Result<MemberRemoval, SyncError> {
        match self.call(Command::RemoveMember(member.clone())).await {
            Ok(Output::Removal(value)) => Ok(value),
            Err(SyncError::AccessRemains(shares)) => Ok(MemberRemoval::AccessRemains { shares }),
            Err(error) => Err(error),
            _ => unreachable!("removal result"),
        }
    }
    /// Remove a device with the provider's instructions for signing it out.
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError> {
        match self.call(Command::RemoveDevice(device)).await? {
            Output::SignOut(value) => Ok(value),
            _ => unreachable!("device result"),
        }
    }
    /// Reload this device in place, keeping waiting uploads and their identities.
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError> {
        self.unit(Command::Reload).await
    }
    /// Snapshot and reset the store audience as an admin, including local reload.
    pub async fn reset_store(&self) -> Result<(), SyncError> {
        self.unit(Command::Reset(coven_merge::Audience::Store))
            .await
    }
    /// Snapshot and reset a circle as one of its members, including local reload.
    pub async fn reset_circle(&self, circle: CircleId) -> Result<(), SyncError> {
        self.unit(Command::Reset(coven_merge::Audience::Circle(circle)))
            .await
    }
    /// Resume a failed operation from its next uncompleted step.
    pub async fn retry_blocked_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.call(Command::Retry(operation)).await.map(|_| ())
    }
    /// Discard a failed operation, publishing any already reserved entry first.
    pub async fn discard_blocked_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.unit(Command::Discard(operation)).await
    }
    /// Acknowledge a key deleted in the S3 console.
    pub async fn confirm_access_key_deleted(&self, key: &str) -> Result<(), SyncError> {
        self.unit(Command::ConfirmKey(key.into())).await
    }
    /// Create and share an invitation that expires after one day.
    pub async fn create_invite(
        &self,
        role: MemberRole,
        access: InviteAccess,
    ) -> Result<Invite, SyncError> {
        match self.call(Command::Invite(role, access)).await? {
            Output::Invite(value) => Ok(value),
            _ => unreachable!("invite result"),
        }
    }
    /// The current checked join requests; closing ends the subscription.
    pub fn subscribe_join_requests(&self) -> watch::Receiver<Vec<JoinRequest>> {
        self.inner.joins.clone()
    }
    /// Approve exactly the checked request the app was shown.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.unit(Command::Approve(request.clone())).await
    }
    /// Decline a request and take back its invitation's storage access.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.unit(Command::Decline(request.clone())).await
    }
    /// Cancel an unsettled invitation and take back its storage access.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError> {
        self.unit(Command::Cancel(*invite)).await
    }
    /// Create a circle with this member as its first member.
    pub async fn create_circle(&self, name: &str) -> Result<CircleId, SyncError> {
        match self.call(Command::CreateCircle(name.into())).await? {
            Output::CircleId(value) => Ok(value),
            _ => unreachable!("circle result"),
        }
    }
    /// Rename a circle as a plain store-log entry.
    pub async fn rename_circle(&self, circle: CircleId, name: &str) -> Result<(), SyncError> {
        self.unit(Command::RenameCircle(circle, name.into())).await
    }
    /// Delete the circle's local rows and then its store-log membership.
    pub async fn delete_circle(&self, circle: CircleId) -> Result<(), SyncError> {
        self.unit(Command::DeleteCircle(circle)).await
    }
    /// Seal every historical circle key to an active store member.
    pub async fn add_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        self.unit(Command::AddCircleMember(circle, member.clone()))
            .await
    }
    /// Remove a circle member and rotate its key.
    pub async fn remove_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        self.unit(Command::RemoveCircleMember(circle, member.clone()))
            .await
    }
    /// List circles this member currently belongs to.
    pub async fn circles(&self) -> Result<Vec<Circle>, SyncError> {
        match self.call(Command::Circles).await? {
            Output::Circles(value) => Ok(value),
            _ => unreachable!("circle list"),
        }
    }
    /// List active store members belonging to a circle.
    pub async fn circle_members(
        &self,
        circle: CircleId,
    ) -> Result<Vec<CircleMemberInfo>, SyncError> {
        match self.call(Command::CircleMembers(circle)).await? {
            Output::CircleMembers(value) => Ok(value),
            _ => unreachable!("circle members"),
        }
    }
    /// Stop the task before closing its database; journal rows remain for reopening.
    pub async fn close(&self) -> Result<(), SyncError> {
        self.unit(Command::Close).await
    }

    async fn unit(&self, command: Command) -> Result<(), SyncError> {
        match self.call(command).await? {
            Output::Unit => Ok(()),
            _ => unreachable!("unit result"),
        }
    }
    /// Inspect the same committed positions and fingerprints used for publication.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn test_positions(
        &self,
    ) -> Result<Option<coven_format::objects::PostedPositions>, SyncError> {
        match self.call(Command::InspectPositions).await? {
            Output::Positions(state) => Ok(state),
            _ => unreachable!("position result"),
        }
    }
}

impl Drop for RunningOperations {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .expect("operation task lock poisoned")
            .take()
        {
            task.abort();
        }
    }
}

impl OperationRun {
    async fn run(mut self) {
        // Opening drives retained work immediately; commands keep the same deadline.
        let mut retry = self.clock.sleep(Duration::ZERO);
        loop {
            let mut response = None;
            let mut query = None;
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(Request { command, reply }) = command else { break; };
                    if matches!(command, Command::Close) { let _ = reply.send(Ok(Output::Unit)); break; }
                    #[cfg(any(test, feature = "test-utils"))]
                    if matches!(command, Command::InspectPositions) {
                        let _ = reply.send(self.writes.current_positions().await.map(Output::Positions));
                        continue;
                    }
                    if let Command::Storage(storage) = &command {
                        if let Err(error) = self.files.set_storage(storage.clone(), std::future::ready(Ok(()))).await {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                        self.writes.set_storage(storage.clone());
                    }
                    if matches!(command, Command::ForgetKeys) {
                        let result = self.files.set_storage(None, async {
                            self.sync.begin_operation_call(Command::ForgetKeys).await?;
                            Ok(())
                        }).await;
                        if result.is_ok() { self.writes.set_storage(None); }
                        let _ = reply.send(result.map(|()| Output::Unit));
                    } else if let Command::Unlock(storage) = command {
                        let result = self.files.set_storage(Some(storage.clone()), self.sync.unlock_storage(storage.clone())).await;
                        if result.is_ok() {
                            self.writes.set_storage(Some(storage));
                        }
                        let _ = reply.send(result.map(|()| Output::Unit));
                    } else if let Command::Setup(storage, access, device_name, commit) = command {
                        let result = self.files.set_storage(Some(storage.clone()), self.sync.setup_storage(storage.clone(), access, device_name, commit)).await;
                        if result.is_ok() {
                            self.writes.set_storage(Some(storage));
                        }
                        let _ = reply.send(result.map(|()| Output::Unit));
                    } else if matches!(command, Command::BlockedOperations | Command::AccessKeysToDelete) {
                        query = Some((reply, command));
                    } else if matches!(command, Command::SyncAll) {
                        response = Some((reply, self.sync_pass().await.map(|()| Output::Unit)));
                    } else {
                        match self.sync.begin_operation_call(command).await {
                            Ok(Begun::Value(value)) => response = Some((reply, Ok(value))),
                            Ok(Begun::Operation(id)) => {
                                if let Some(previous) = self.waiters.insert(id, reply) {
                                    let _ = previous.send(Err(SyncError::InvitationChanged));
                                }
                            }
                            Err(error) => response = Some((reply, Err(error))),
                        }
                    }
                }
                _ = &mut retry => {
                    retry = self.clock.sleep(Duration::from_secs(1));
                }
            }
            if let Err(error) = self.drive().await {
                // A journal failure cannot be recorded in the journal. Stop this
                // owner and fail its waiters, retaining the typed database cause.
                tracing::error!(error = %error, "operation journal failed; worker stopped");
                let shared = SyncFailure::from(error);
                if let Some((reply, _)) = response.take() {
                    let _ = reply.send(Err(shared.clone().into()));
                }
                if let Some((reply, _)) = query.take() {
                    let _ = reply.send(Err(shared.clone().into()));
                }
                for (_, reply) in std::mem::take(&mut self.waiters) {
                    let _ = reply.send(Err(shared.clone().into()));
                }
                break;
            }
            if let Some((reply, command)) = query {
                let result = self.sync.begin_operation_call(command).await.map(|begun| {
                    let Begun::Value(value) = begun else {
                        unreachable!("read-only query")
                    };
                    value
                });
                let _ = reply.send(result);
            }
            if let Some((reply, result)) = response {
                let _ = reply.send(result);
            }
        }
    }

    async fn sync_pass(&mut self) -> Result<(), SyncError> {
        let _reads = self.sync.begin_pass(&mut self.writes);
        self.sync.sync_store_log().await?;
        self.drive_pass().await?;
        // A pending reload cannot report a successful pass or advance positions.
        if let Some(id) = self.sync.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        self.sync.reload_deleted_history().await?;
        self.writes.upload_writes().await?;
        // Deleting a circle waits for its row deletion's write to be uploaded.
        self.drive_pass().await?;
        self.writes.download_writes().await?;
        self.drive_pass().await?;
        if let Some(id) = self.sync.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        self.files.sync_files().await?;
        self.writes.upload_writes().await?;
        self.sync.write_snapshots().await?;
        self.sync.run_retention().await?;
        self.writes.post_positions().await?;
        Ok(())
    }

    async fn drive_pass(&mut self) -> Result<(), SyncError> {
        match self.drive().await? {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    // Journal failures stop the worker; maintenance failures stop only the pass.
    async fn drive(&mut self) -> Result<Option<SyncError>, SyncError> {
        let mut maintenance_failure = None;
        loop {
            match self.sync.schedule_version_changes().await {
                Ok(()) | Err(SyncError::Stopped(SyncFailure::UpdateRequired)) => (),
                Err(error) => return Err(error),
            }
            let records = self
                .sync
                .operation_records()
                .await?
                .into_iter()
                .map(|record| Ok((crate::operation_data::Data::read(&record)?, record)))
                .collect::<Result<Vec<_>, SyncError>>()?;
            let reloading = records.iter().any(|(data, _)| data.is_reload());
            let mut advanced = false;
            let raising = records.iter().any(|(data, _)| data.raises_version());
            let mut writer = None;
            for (data, record) in &records {
                if data.entry()?.is_some() {
                    writer = Some(record.id);
                    break;
                }
            }
            if writer.is_none() {
                writer = records
                    .iter()
                    .find(|(data, _)| data.writes_entry() && (!raising || data.raises_version()))
                    .map(|(_, record)| record.id);
            }
            let raising_first = records
                .iter()
                .any(|(data, record)| Some(record.id) == writer && data.raises_version());
            for (data, record) in records {
                if record.failure.is_some() && !self.sync.invite_expired(&data) {
                    continue;
                }
                if raising_first && data.is_reload() {
                    continue;
                }
                if data.writes_entry()
                    && ((reloading && !data.raises_version() && !data.completing_reset(&record))
                        || writer != Some(record.id))
                {
                    continue;
                }
                let maintenance = data.app_kind(&record.started_by).is_none();
                let step = self.sync.operation_step(&record, data).await;
                match step {
                    Ok(Progress::Waiting) => (),
                    Ok(Progress::Advanced) => advanced = true,
                    Ok(Progress::Reply(result)) => {
                        advanced = true;
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(result);
                        }
                    }
                    Ok(Progress::Finished(value)) => {
                        advanced = true;
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(Ok(value));
                        }
                    }
                    Err(SyncError::Stopped(SyncFailure::UpdateRequired)) => {
                        // The committed operation waits for the next app open;
                        // an update requirement is not a permanently blocked step.
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(Err(SyncFailure::UpdateRequired.into()));
                        }
                    }
                    Err(error) if !error.blocks_operation() => {
                        tracing::debug!(operation = record.id.0, error = %error, "operation waiting for storage or prerequisites");
                    }
                    Err(error) => {
                        self.sync
                            .block_operation(record.id, error.to_string())
                            .await?;
                        if maintenance {
                            tracing::warn!(operation = record.id.0, error = %error, "maintenance failed; retained for the next pass");
                            if maintenance_failure.is_none() {
                                maintenance_failure = Some(error);
                            }
                        } else if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(Err(error));
                        }
                    }
                }
            }
            let requests = self.sync.current_join_requests().await?;
            self.joins.send_if_modified(|current| {
                if *current == requests {
                    false
                } else {
                    *current = requests;
                    true
                }
            });
            if !advanced {
                return Ok(maintenance_failure);
            }
        }
    }
}
