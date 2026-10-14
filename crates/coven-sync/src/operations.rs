//! Lifetime owner for durable operations. Dropping an app future drops only its waiter.

use crate::*;
use coven_database::OperationId;
use coven_format::store_log::MemberRole;
use coven_foundation::clock::ClockRef;
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, MemberId};
use coven_storage::{ProviderSignOut, Storage};
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
    Invite(MemberRole, InviteAccess),
    Approve(JoinRequest),
    Decline(JoinRequest),
    Cancel(InviteId),
    Retry(OperationId),
    Discard(OperationId),
    ConfirmKey(String),
    DeleteKey(OperationId, Arc<dyn coven_storage::AccessKeyAdmin>),
    RetryWithAdmin(OperationId, Arc<dyn coven_storage::AccessKeyAdmin>),
    ReplaceAccessKey(MemberId, Arc<dyn coven_storage::AccessKeyAdmin>),
    DeliveredKey(Option<String>),
    Credentials {
        previous: Option<coven_storage::ConnectionCredentials>,
        next: coven_storage::ConnectionCredentials,
        require_connected: bool,
        commit: crate::StorageCommit,
    },
    CheckKeys,
    ForgetKeys,
    #[cfg(any(test, feature = "test-utils"))]
    Sync,
    /// Returns once every earlier command has run.
    #[cfg(any(test, feature = "test-utils"))]
    Barrier,
    SyncAll,
    Unlock(Arc<dyn Storage>),
    Setup(
        Arc<dyn Storage>,
        coven_format::MemberAccess,
        String,
        crate::StorageCommit,
    ),
    Storage(Option<Arc<dyn Storage>>),
    Close,
}

pub(crate) enum Output {
    Unit,
    SignOut(ProviderSignOut),
    CircleId(CircleId),
    Invite(Invite),
    AccessKeyReplaced(AccessKeyReplacement),
    DeliveredKey(Option<coven_format::access_key_delivery::DeliveredKey>),
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
    Waiting(PendingReason),
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
    /// The earliest expiry among open invitations, read with the journal.
    invite_expiry: Option<std::time::SystemTime>,
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
            invite_expiry: None,
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
        previous: Option<coven_storage::ConnectionCredentials>,
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
    /// Wait for the worker to finish every earlier command.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn barrier(&self) -> Result<(), SyncError> {
        self.unit(Command::Barrier).await
    }
    /// Run one entire pass under the operation worker's serialization. Replay,
    /// reloads, writes and snapshots cannot interleave with another operation.
    pub async fn sync(&self) -> Result<(), SyncError> {
        self.unit(Command::SyncAll).await
    }
    /// Change a member's role as a plain store-log entry.
    pub async fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<(), SyncError> {
        self.unit(Command::SetRole(*member, role)).await
    }
    /// Publish the current member's replacement storage access. A failed
    /// publication retains its fixed bytes for explicit retry.
    pub(crate) async fn set_access(
        &self,
        access: coven_format::MemberAccess,
    ) -> Result<(), SyncError> {
        self.unit(Command::SetAccess(access)).await
    }
    /// Publish removal; provider access work is reported through pending().
    pub async fn remove_member(&self, member: &MemberId) -> Result<(), SyncError> {
        self.unit(Command::RemoveMember(*member)).await
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
    pub async fn retry_pending_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.call(Command::Retry(operation)).await.map(|_| ())
    }
    /// Resume provider work that needs the storage account's admin access,
    /// held by this operation only until its provider work is done (§4).
    pub async fn retry_with_admin_access(
        &self,
        operation: OperationId,
        admin: Arc<dyn coven_storage::AccessKeyAdmin>,
    ) -> Result<(), OperationError> {
        self.call(Command::RetryWithAdmin(operation, admin))
            .await
            .map(|_| ())
    }
    /// Discard a failed operation, publishing any already reserved entry first.
    pub async fn discard_pending_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), OperationError> {
        self.unit(Command::Discard(operation)).await
    }
    /// Delete the S3 key a pending deletion names using the storage account's
    /// admin access, then read the provider for that exact key. Only its
    /// absence clears the notice; every failure is returned and the notice
    /// stays (§13).
    pub async fn delete_access_key(
        &self,
        operation: OperationId,
        admin: Arc<dyn coven_storage::AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        self.unit(Command::DeleteKey(operation, admin)).await
    }
    /// Replace a member's S3 key with admin access (§13): make it, deliver it
    /// sealed to the member's devices, record it for them, then delete the
    /// old key. Their own replacement or, as an admin, another member's.
    pub async fn replace_member_access_key(
        &self,
        member: &MemberId,
        admin: Arc<dyn coven_storage::AccessKeyAdmin>,
    ) -> Result<AccessKeyReplacement, SyncError> {
        match self.call(Command::ReplaceAccessKey(*member, admin)).await? {
            Output::AccessKeyReplaced(value) => Ok(value),
            _ => unreachable!("replacement result"),
        }
    }
    /// The key an admin delivered to this member that the store log now
    /// records as their access, if this device can read it and does not
    /// already hold it as `held` (§13).
    pub(crate) async fn delivered_access_key(
        &self,
        held: Option<String>,
    ) -> Result<Option<coven_format::access_key_delivery::DeliveredKey>, SyncError> {
        match self.call(Command::DeliveredKey(held)).await? {
            Output::DeliveredKey(value) => Ok(value),
            _ => unreachable!("delivered key result"),
        }
    }
    /// Confirm explicitly that this S3 key was deleted, such as in the console.
    /// Clears only this id's notice and keeps the confirmation (§13).
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
    /// Publish one entry whose deletion rule hides the circle's rows while kept.
    pub async fn delete_circle(&self, circle: CircleId) -> Result<(), SyncError> {
        self.unit(Command::DeleteCircle(circle)).await
    }
    /// Seal every historical circle key to an active store member.
    pub async fn add_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        self.unit(Command::AddCircleMember(circle, *member)).await
    }
    /// Remove a circle member.
    pub async fn remove_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), SyncError> {
        self.unit(Command::RemoveCircleMember(circle, *member))
            .await
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
        // Opening drives retained work immediately. Afterwards the worker wakes
        // for commands and the earliest due retry, never on a fixed tick.
        let mut retry = self.clock.sleep(Duration::ZERO);
        loop {
            let mut response = None;
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(Request { command, reply }) = command else { break; };
                    if matches!(command, Command::Close) { let _ = reply.send(Ok(Output::Unit)); break; }
                    self.sync.set_discovery_download_limit(self.files.transfer_limits().downloads);
                    if let Command::Storage(storage) = &command {
                        self.sync.invalidate_sends();
                        if let Err(error) = self.files.set_storage(storage.clone(), std::future::ready(Ok(()))).await {
                            let _ = reply.send(Err(error));
                            continue;
                        }
                        self.writes.set_storage(storage.clone());
                        // A connection is a prerequisite of waiting storage work.
                        self.sync.prerequisites_changed();
                    }
                    if matches!(command, Command::ForgetKeys) {
                        let result = self.files.set_storage(None, async {
                            self.sync.begin_operation_call(Command::ForgetKeys).await?;
                            Ok(())
                        }).await;
                        if result.is_ok() { self.sync.invalidate_sends(); self.writes.set_storage(None); }
                        let _ = reply.send(result.map(|()| Output::Unit));
                    } else if let Command::Unlock(storage) = command {
                        let result = self.files.set_storage(Some(storage.clone()), self.sync.unlock_storage(storage.clone())).await;
                        if result.is_ok() {
                            self.writes.set_storage(Some(storage));
                            self.sync.prerequisites_changed();
                        }
                        let _ = reply.send(result.map(|()| Output::Unit));
                    } else if let Command::Setup(storage, access, device_name, commit) = command {
                        let result = self.files.set_storage(Some(storage.clone()), self.sync.setup_storage(storage.clone(), access, device_name, commit)).await;
                        if result.is_ok() {
                            self.writes.set_storage(Some(storage));
                            self.sync.prerequisites_changed();
                        }
                        if result.is_err() { self.sync.invalidate_sends(); }
                        let _ = reply.send(result.map(|()| Output::Unit));
                    } else if matches!(command, Command::SyncAll) {
                        response = Some((reply, self.sync_pass().await.map(|()| Output::Unit)));
                    } else {
                        let removal = matches!(command, Command::RemoveMember(_) | Command::RemoveCircleMember(..));
                        match self.sync.begin_operation_call(command).await {
                            Ok(Begun::Value(value)) if removal => { let _ = reply.send(Ok(value)); }
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
                _ = &mut retry => {}
            }
            if let Err(error) = self.drive().await {
                // A journal failure cannot be recorded in the journal. Stop this
                // owner and fail every waiter with one shared typed cause.
                tracing::error!(error = %error, "operation journal failed; worker stopped");
                let (failure, source) = error.into_shared();
                let shared = || SyncError::Failed {
                    failure,
                    source: source.clone(),
                };
                if let Some((reply, _)) = response.take() {
                    let _ = reply.send(Err(shared()));
                }
                for (_, reply) in std::mem::take(&mut self.waiters) {
                    let _ = reply.send(Err(shared()));
                }
                break;
            }
            if let Some((reply, result)) = response {
                let _ = reply.send(result);
            }
            retry = self
                .next_wake()
                .unwrap_or_else(|| Box::pin(std::future::pending()));
        }
    }

    /// When the worker must next run without a command: the earliest due
    /// operation retry, or an open invitation's expiry. There is no tick.
    fn next_wake(&self) -> Option<crate::retry_waits::Wake> {
        let retry = self.sync.next_operation_wake();
        // An invitation already expired is settled by the drive that read it.
        let expiry = self
            .invite_expiry
            .and_then(|expiry| expiry.duration_since(self.clock.now()).ok())
            .map(|left| self.clock.sleep(left));
        match (retry, expiry) {
            (Some(retry), Some(expiry)) => Some(Box::pin(futures_util::FutureExt::map(
                futures_util::future::select(retry, expiry),
                |_| (),
            ))),
            (retry, expiry) => retry.or(expiry),
        }
    }

    async fn sync_pass(&mut self) -> Result<(), SyncError> {
        let result = self.pass_steps().await;
        // Any sender may have stopped this copy; decide its reset durably.
        if self.sync.send_stopped() {
            return Err(self.sync.record_device_reset().await);
        }
        result
    }

    async fn drive_pass(&mut self) -> Result<(), SyncError> {
        match self.drive().await? {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    // Journal failures stop the worker; maintenance failures stop only the pass.
    async fn drive(&mut self) -> Result<Option<SyncError>, SyncError> {
        self.sync
            .set_discovery_download_limit(self.files.transfer_limits().downloads);
        // Maintenance that failed and has not recovered within this drive.
        let mut maintenance_failures: Vec<(OperationId, SyncError)> = Vec::new();
        loop {
            match self.sync.schedule_version_changes().await {
                Ok(()) | Err(SyncError::UpdateRequired(_)) => (),
                Err(error) => return Err(error),
            }
            let records = self
                .sync
                .operation_records()
                .await?
                .into_iter()
                .map(|record| Ok((crate::operation_data::Data::read(&record)?, record)))
                .collect::<Result<Vec<_>, SyncError>>()?;
            self.sync
                .retain_operation_waits(&records.iter().map(|(_, record)| record.id).collect());
            self.invite_expiry = records
                .iter()
                .filter_map(|(data, _)| match data {
                    crate::operation_data::Data::Invite(work)
                        if matches!(
                            work.state,
                            crate::operation_data::InviteState::Grant
                                | crate::operation_data::InviteState::Waiting { .. }
                        ) =>
                    {
                        Some(work.expires_at)
                    }
                    _ => None,
                })
                .min();
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
            for (snapshot_data, snapshot) in records {
                // Replay and invite settlement can retire another account's queued work.
                let Some(record) = self.sync.operation_record(snapshot.id).await? else {
                    if !matches!(
                        snapshot_data,
                        crate::operation_data::Data::ProviderAccess { .. }
                    ) {
                        return Err(coven_database::DbError::OperationChanged(snapshot.id).into());
                    }
                    if let Some(reply) = self.waiters.remove(&snapshot.id) {
                        let _ = reply.send(Ok(Output::Unit));
                    }
                    continue;
                };
                let data = crate::operation_data::Data::read(&record)?;
                if !self.sync.operation_ready(&record).await? && !self.sync.invite_expired(&data) {
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
                let maintenance = record.started_by == "coven"
                    && !matches!(data, crate::operation_data::Data::ProviderAccess { .. });
                let step = self.sync.operation_step(&record, data).await;
                match step {
                    Ok(Progress::Waiting(reason)) => {
                        let pending = self
                            .sync
                            .record_operation_condition(&record, reason)
                            .await?;
                        if pending.retry() != Retry::Automatic {
                            if let Some(reply) = self.waiters.remove(&record.id) {
                                let _ = reply.send(Err(SyncError::Pending(Box::new(pending))));
                            }
                        }
                    }
                    Ok(Progress::Advanced) => {
                        advanced = true;
                        maintenance_failures.retain(|(id, _)| *id != record.id);
                    }
                    Ok(Progress::Reply(result)) => {
                        advanced = true;
                        maintenance_failures.retain(|(id, _)| *id != record.id);
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(result);
                        }
                    }
                    Ok(Progress::Finished(value)) => {
                        advanced = true;
                        maintenance_failures.retain(|(id, _)| *id != record.id);
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(Ok(value));
                        }
                    }
                    Err(error) => {
                        self.sync.record_operation_error(&record, &error).await?;
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(Err(error));
                        } else if maintenance {
                            maintenance_failures.retain(|(id, _)| *id != record.id);
                            maintenance_failures.push((record.id, error));
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
                // A failure its own retry resolved within this drive, after
                // other work advanced, does not fail the pass.
                return Ok(maintenance_failures
                    .into_iter()
                    .next()
                    .map(|(_, error)| error));
            }
            // Progress is committed evidence for every waiting operation.
            self.sync.prerequisites_changed();
        }
    }
}

#[path = "operation_pass.rs"]
mod pass;
