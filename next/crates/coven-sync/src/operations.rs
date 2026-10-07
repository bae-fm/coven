//! Lifetime owner for durable operations. Dropping an app future drops only its waiter.

use crate::*;
use coven_crypto::MemberId;
use coven_database::OperationId;
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::{CircleId, DeviceId, InviteId};
use coven_storage::{MemberRemoval, ProviderSignOut, Storage};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

/// Owns the operation worker and its lifetime. It starts retained work on opening;
/// it does not run the store's sync loop. The sync owner calls `sync_store_log`
/// when remote entries may have arrived, and `set_storage` when a provider connects.
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
    KeepFiles(
        Vec<coven_database::FileRef>,
        std::collections::HashMap<String, std::path::PathBuf>,
    ),
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
    Sync,
    Report,
    Storage(Option<Arc<dyn Storage>>),
    Close,
}

pub(crate) enum Output {
    Unit,
    Removal(MemberRemoval),
    SignOut(ProviderSignOut),
    Members(Vec<MemberInfo>),
    CircleId(CircleId),
    Circles(Vec<Circle>),
    CircleMembers(Vec<CircleMemberInfo>),
    Invite(Invite),
    Report(SyncReport),
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
    commands: mpsc::UnboundedReceiver<Request>,
    joins: watch::Sender<Vec<JoinRequest>>,
    waiters: BTreeMap<OperationId, Reply>,
    notices: SyncReport,
}

impl Operations {
    /// Compose and start the lifetime owner after the database opens. Schedule
    /// migration publication on opening; retained failures stay blocked.
    pub fn new(sync: StoreLogSync, files: Files) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (joins, subscription) = watch::channel(Vec::new());
        let run = OperationRun {
            sync,
            files,
            commands: receiver,
            joins,
            waiters: BTreeMap::new(),
            notices: SyncReport::default(),
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

    /// Supply or disconnect the provider capability; waiting operations continue
    /// using their persisted bytes. This does not enable a background sync loop.
    pub async fn set_storage(&self, storage: Option<Arc<dyn Storage>>) -> Result<(), SyncError> {
        self.unit(Command::Storage(storage)).await
    }
    /// Record keep-file operations without waiting for their downloads.
    /// Each operation resumes independently and reports a permanent failure in
    /// the same journal/report as membership operations (§18, §20.7).
    pub async fn keep_files_on_this_device(
        &self,
        files: &[coven_database::FileRef],
        destinations: &std::collections::HashMap<String, std::path::PathBuf>,
    ) -> Result<(), OperationError> {
        self.unit(Command::KeepFiles(files.to_vec(), destinations.clone()))
            .await
    }
    /// Download and replay available store-log entries, then wake operation work.
    pub async fn sync_store_log(&self) -> Result<SyncReport, SyncError> {
        match self.call(Command::Sync).await? {
            Output::Report(report) => Ok(report),
            _ => unreachable!("sync result"),
        }
    }
    /// Read retained failures, manual key deletions and this member's dropped entries.
    pub async fn report(&self) -> Result<SyncReport, SyncError> {
        match self.call(Command::Report).await? {
            Output::Report(report) => Ok(report),
            _ => unreachable!("report result"),
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
    /// The sync report lists all recorded S3 keys until confirmed deleted.
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
    pub async fn reset_circle(&self, circle: CircleId) -> Result<(), CircleError> {
        Ok(self
            .unit(Command::Reset(coven_merge::Audience::Circle(circle)))
            .await?)
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
    pub async fn create_circle(&self, name: &str) -> Result<CircleId, CircleError> {
        match self.call(Command::CreateCircle(name.into())).await? {
            Output::CircleId(value) => Ok(value),
            _ => unreachable!("circle result"),
        }
    }
    /// Rename a circle as a plain store-log entry.
    pub async fn rename_circle(&self, circle: CircleId, name: &str) -> Result<(), CircleError> {
        Ok(self
            .unit(Command::RenameCircle(circle, name.into()))
            .await?)
    }
    /// Delete the circle's local rows and then its store-log membership.
    pub async fn delete_circle(&self, circle: CircleId) -> Result<(), CircleError> {
        Ok(self.unit(Command::DeleteCircle(circle)).await?)
    }
    /// Seal every historical circle key to an active store member.
    pub async fn add_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), CircleError> {
        Ok(self
            .unit(Command::AddCircleMember(circle, member.clone()))
            .await?)
    }
    /// Remove a circle member and rotate its key.
    pub async fn remove_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<(), CircleError> {
        Ok(self
            .unit(Command::RemoveCircleMember(circle, member.clone()))
            .await?)
    }
    /// List circles this member currently belongs to.
    pub async fn circles(&self) -> Result<Vec<Circle>, CircleError> {
        match self.call(Command::Circles).await? {
            Output::Circles(value) => Ok(value),
            _ => unreachable!("circle list"),
        }
    }
    /// List active store members belonging to a circle.
    pub async fn circle_members(
        &self,
        circle: CircleId,
    ) -> Result<Vec<CircleMemberInfo>, CircleError> {
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
        let mut retry = tokio::time::interval(Duration::from_secs(1));
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let mut response = None;
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(Request { command, reply }) = command else { break; };
                    if matches!(command, Command::Close) { let _ = reply.send(Ok(Output::Unit)); break; }
                    if let Command::KeepFiles(files, destinations) = command {
                        let result = self.files.record_keeps(&files, &destinations).await.map(|()| Output::Unit);
                        // Acceptance means the intent committed; downloading
                        // must not hold this reply, even when storage is online.
                        let _ = reply.send(result);
                    } else {
                        if let Command::Storage(storage) = &command { self.files.set_storage(storage.clone()); }
                        if let Command::Discard(id) = command {
                            response = Some((reply, self.discard(id).await.map(|()| Output::Unit)));
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
                }
                _ = retry.tick() => {}
            }
            if let Err(error) = self.drive().await {
                // A journal failure cannot be recorded in the journal. Stop this
                // owner and fail its waiters, retaining the typed database cause.
                tracing::error!(error = %error, "operation journal failed; worker stopped");
                let shared = SyncFailure::from(error);
                if let Some((reply, _)) = response.take() {
                    let _ = reply.send(Err(shared.clone().into()));
                }
                for (_, reply) in std::mem::take(&mut self.waiters) {
                    let _ = reply.send(Err(shared.clone().into()));
                }
                break;
            }
            if let Some((reply, mut result)) = response {
                if let Ok(Output::Report(report)) = &mut result {
                    report
                        .damaged_objects
                        .append(&mut self.notices.damaged_objects);
                    match self.sync.operation_report().await {
                        Ok(current) => {
                            report.blocked_operations = current.blocked_operations;
                            report.access_keys_to_delete = current.access_keys_to_delete;
                            report.dropped_entries = current.dropped_entries;
                        }
                        Err(error) => result = Err(error),
                    }
                }
                let _ = reply.send(result);
            }
        }
    }

    async fn drive(&mut self) -> Result<(), SyncError> {
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
            let reloading = self.sync.pending_reload().await?.is_some();
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
                if raising_first
                    && matches!(
                        data,
                        crate::operation_data::Data::Snapshots(
                            crate::snapshot_data::SnapshotTask {
                                job: crate::snapshot_data::SnapshotJob::Reload { .. },
                                ..
                            }
                        )
                    )
                {
                    continue;
                }
                if data.writes_entry()
                    && ((reloading && !data.raises_version() && !data.completing_reset(&record))
                        || writer != Some(record.id))
                {
                    continue;
                }
                let step = if matches!(data, crate::operation_data::Data::KeepFile(_)) {
                    self.files.keep_step(&record, data).await
                } else {
                    self.sync
                        .operation_step(&record, data, &mut self.notices)
                        .await
                };
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
                    Err(
                        SyncError::NoStorage
                        | SyncError::KeyUnavailable(_)
                        | SyncError::ReloadPending(_),
                    ) => (),
                    Err(SyncError::Stopped(SyncFailure::UpdateRequired)) => {
                        // The committed operation waits for the next app open;
                        // an update requirement is not a permanently blocked step.
                        if let Some(reply) = self.waiters.remove(&record.id) {
                            let _ = reply.send(Err(SyncFailure::UpdateRequired.into()));
                        }
                    }
                    Err(SyncError::Storage(error)) if error.retryable() => {
                        tracing::debug!(operation = record.id.0, error = %error, "operation waiting for storage");
                    }
                    Err(SyncError::File(
                        FileReadError::Offline { .. } | FileReadError::NoStorage,
                    )) => {
                        tracing::debug!(
                            operation = record.id.0,
                            "file operation waiting for storage"
                        );
                    }
                    Err(SyncError::File(FileReadError::Storage(error))) if error.retryable() => {
                        tracing::debug!(operation = record.id.0, error = %error, "file operation waiting for storage");
                    }
                    Err(error) => {
                        self.sync
                            .block_operation(record.id, error.to_string())
                            .await?;
                        if let Some(reply) = self.waiters.remove(&record.id) {
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
                return Ok(());
            }
        }
    }

    async fn discard(&mut self, id: OperationId) -> Result<(), SyncError> {
        let Some(record) = self
            .sync
            .operation_records()
            .await?
            .into_iter()
            .find(|r| r.id == id && r.failure.is_some())
        else {
            return Err(SyncError::NotBlocked(id));
        };
        let data = crate::operation_data::Data::read(&record)?;
        match data {
            crate::operation_data::Data::KeepFile(_) => {
                if let Err(error) = self.files.discard_keep(&record, data).await {
                    self.sync.block_operation(id, error.to_string()).await?;
                    return Err(error);
                }
            }
            _ => {
                self.sync.begin_operation_call(Command::Discard(id)).await?;
            }
        }
        Ok(())
    }
}
