//! The app's write: its SQL and coven's steps in one transaction (§4, E3).

use crate::*;
use coven_database::SqlContext;
use coven_format::admin::StoreChange;

/// One write's SQL and coven's steps, in one transaction (§4, E3). A step
/// runs where it is called: the write's later SQL sees what it changed, and
/// it commits or rolls back with the app's statements. A failed step fails
/// the write, even if the closure goes on and returns success. Returning an
/// error rolls back every statement and step.
///
/// An administrative change, making a circle, and approving a join
/// request, is checked against the state this device has applied and
/// journaled as an operation, with or without storage (§11.1). Its step
/// returns the operation's id, which `coven_operations` lists from the
/// write's commit until the app acknowledges its outcome, or `None` when the
/// applied state already has what it asks for. A change the applied
/// state refuses fails with `Rejected`. Once the write commits, a pass
/// catches up, validates the change again and claims its slot; a change the
/// caught-up state refuses is undone and ends `Rejected` (§11.2).
pub struct CovenWrite<'write> {
    sql: SqlContext<'write, 'write>,
    /// The store's operation steps, which run in this write.
    steps: &'write coven_sync::OperationSteps,
    files: &'write coven_sync::Files,
}

impl<'write> CovenWrite<'write> {
    pub(crate) fn new(
        sql: SqlContext<'write, 'write>,
        steps: &'write coven_sync::OperationSteps,
        files: &'write coven_sync::Files,
    ) -> Self {
        Self { sql, steps, files }
    }

    /// Puts files `CovenHandle::stage_files` staged into this write, and
    /// returns what its batch returned, such as their ids, for this write's
    /// rows to reference (§17.2). The write makes each file's record, in the
    /// audience of the rows that name it, and queues its upload for the
    /// cloud. Files staged by another open store, or by this one before it
    /// closed, fail with `PreparedElsewhere`; a file no row names fails the
    /// write with `FileUnreferenced`.
    pub fn put_staged<T>(&self, staged: StagedFiles<T>) -> Result<T, DbError> {
        self.sql.put_staged(staged)
    }

    /// Stages a user's original, read whole once already, as a new file in
    /// a user namespace and returns its id for a row to reference: on this
    /// device only, or for the cloud, with its upload queued. Coven records
    /// the original's path, size, modification time and the hashes its read
    /// found, which its upload checks, now or when the file moves to the
    /// cloud; it never changes the original, nor reads it here (§17.1,
    /// §17.3). An undeclared namespace fails with `UndeclaredNamespace`, an
    /// app one with `NamespaceKind`.
    pub fn put_user_file(
        &self,
        namespace: &str,
        original: PreparedUserFile,
        place: Place,
    ) -> Result<FileId, DbError> {
        self.sql.put_user_file(namespace, original, place)
    }

    /// Stages a new file over an existing file's bytes, for a row in any
    /// audience, and returns its id for that row to reference. The write
    /// makes it in the audience of the rows that reference it, with
    /// `from`'s namespace, size, place and key; nothing is uploaded again. A
    /// file on this device only gives a new file on this device only, over
    /// the same original or coven's copy. The write fails with `FileAbsent`
    /// if this device has no record of `from`, and with `NotThisDevice` if
    /// `from` is on another device only (§17.2).
    pub fn existing_file(&self, from: FileId) -> FileId {
        self.sql.existing_file(from)
    }

    /// Records a move of these files to the cloud (§17.3).
    pub fn move_to_cloud(&self, files: Vec<MoveFile>) -> Result<MoveId, DbError> {
        self.sql.move_to_cloud(files)
    }

    /// Cancels a move: every file stays on this device only (§17.3).
    pub fn cancel_move(&self, id: MoveId) -> Result<(), DbError> {
        self.sql.cancel_move(id)
    }

    /// Releases the pins on these files' stored bytes, then evicts each of
    /// their namespaces down to its budget (§17.8). A file this device has
    /// no record of, or one on one device only, has no pin to release. A
    /// file kept here whose pin coven has not yet made has its pin request
    /// ended too: `coven_pin_requests` loses it, and coven never makes that
    /// pin (§17.8). The evicted bytes go once the write has committed.
    pub fn unpin(&self, files: &[FileId]) -> Result<(), DbError> {
        self.sql.unpin(files)
    }

    /// Removes a stored file's cached copies, pinned or not, without
    /// changing storage or a local original (§17.8). A file this device has
    /// no record of, or one on one device only, has none. The bytes go once
    /// the write has committed.
    pub fn evict_file(&self, file: FileId) -> Result<(), DbError> {
        self.sql.evict_file(file)
    }

    /// Sets a declared namespace's cache budget and evicts its unpinned
    /// files and chunks, least recently read first, down to it (§17.8). An
    /// undeclared namespace fails with `UndeclaredNamespace`. The evicted
    /// bytes go once the write has committed.
    pub fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        self.sql.set_cache_budget(namespace, max_bytes)
    }

    /// Pauses trailing uploads, or resumes them (§17.9). A paused upload
    /// keeps its place, including a provider upload session in progress,
    /// and stops between confirmed parts; the setting persists. Bound
    /// uploads run on, since writes wait for them (§17.4).
    pub fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError> {
        self.files.set_uploads_paused(&self.sql, paused)
    }

    /// Moves files only on unmetered networks, or on any (§17.9). While the
    /// network is metered, trailing uploads start no more and stop between
    /// confirmed parts, and pins' downloads wait before their next request
    /// to storage; bound uploads and reads run on. The setting persists, on
    /// this device only. A retry asked after the write returns is never
    /// answered from the setting it replaced.
    pub fn set_unmetered_only(&self, unmetered_only: bool) -> Result<(), DbError> {
        self.files.set_unmetered_only(&self.sql, unmetered_only)
    }

    /// Dismisses lost values the app has dealt with (§8.4): the write's
    /// record carries the acknowledgements, and the removed rows' deletes
    /// with their cascades, after the app's own changes, so every device
    /// drops them from `_coven_lost`. A removed row is deleted for good and
    /// never comes back. A value no longer lost as it was is left alone.
    /// Setting a value and dismissing its loss in one write keeps the two
    /// together; a write that also changes a row its dismissals delete fails
    /// with `DismissedRowChanged`.
    pub fn dismiss_lost_values(&self, values: &[LostValue]) -> Result<(), DbError> {
        self.sql.dismiss_lost_values(values)
    }

    /// Keeps a prepared pin's files whole on this device, regardless of the
    /// cache budget (§17.8): each whole copy becomes the file's pinned cache
    /// entry, and each copy already kept is pinned, all in this write.
    /// Files prepared in another open store, or in this one before it
    /// closed, fail with `PreparedElsewhere`; a file whose record left its
    /// stored bytes since with `NoLongerStored`; and a copy already kept
    /// that was evicted since with `PinChanged`, which preparing again
    /// downloads.
    pub fn pin(&self, prepared: PreparedPin) -> Result<(), DbError> {
        self.sql.pin(prepared)
    }

    /// Makes a prepared call's files device-only on this device, each
    /// switched to its copy, which becomes the original this device
    /// records, in this write (§17.3). Each must still be one this device
    /// may make device-only, and still stored where its copy came from
    /// (`NotInCloud`); copies prepared in another open store, or in this one
    /// before it closed, fail with `PreparedElsewhere`. A write that also
    /// removes such a file's last reference fails with
    /// `DeviceOnlyUnreferenced`. A write that fails leaves the copies
    /// unswitched, and they are removed once it has.
    pub fn make_device_only(&self, prepared: PreparedDeviceOnly) -> Result<(), DbError> {
        self.sql.make_device_only(prepared)
    }

    /// Makes an operation's retry or undo due now, from its recorded
    /// progress (E6): its pending record and retry wait end in this write.
    /// It fails with `NotPending` for an operation that has finished or
    /// never existed, and with `Pending` and the record for one whose
    /// record never retries, or only after an update not installed yet. It
    /// can't bypass a provider cooldown. Once the write commits, a pass runs
    /// the operation as soon as storage is connected.
    pub fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError> {
        self.steps.retry_pending_operation(&self.sql, operation)
    }

    /// Renames the store, as an admin (§10, E9).
    pub fn rename_store(&self, name: &str) -> Result<Option<OperationId>, SyncError> {
        self.steps.administer(&self.sql, StoreChange::RenameStore { name: name.into() })
    }

    /// Changes a member's role, as an admin (§10, E9).
    pub fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<Option<OperationId>, SyncError> {
        self.steps.administer(&self.sql, StoreChange::ChangeRole { member: *member, role })
    }

    /// Removes a member and all their devices, as an admin (§14.1, E9).
    /// Provider work appears in `coven_pending` (§14.2). An admin removing
    /// themselves leaves: once the removal applies, this device deletes
    /// their saved restore code and S3 key, and stops.
    pub fn remove_member(&self, member: &MemberId) -> Result<Option<OperationId>, SyncError> {
        self.steps.administer(&self.sql, StoreChange::RemoveMember { member: *member })
    }

    /// Removes a device: their own, or any as an admin (§14.1, E9). It also
    /// says how the device's member signs out of the provider and signs in
    /// again on the devices they keep, read from the recorded location: none
    /// on S3, where nothing at the provider changes (§14.2). A store with no
    /// recorded location fails with `NoStorage`.
    pub fn remove_device(
        &self,
        device: DeviceId,
    ) -> Result<(Option<OperationId>, Option<ProviderSignOut>), SyncError> {
        self.steps.remove_device(&self.sql, device)
    }

    /// Publishes a prepared public pair for another active member, as an
    /// admin, expecting the pair the applied state binds to them now (§10,
    /// §12.3, E9). Replacing this member's own pair fails with
    /// `Rejected(NotAllowed)`: that is `CovenHandle::replace_member_keys`.
    pub fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<Option<OperationId>, SyncError> {
        self.steps.replace_member_keys_for(&self.sql, *member, keys)
    }

    /// Makes a circle with this member as its first member, and returns its
    /// id with the operation's (§15.5, E12). Any active member of the store
    /// may make one. Its first key, the copy that hands it to this member
    /// and its introduction follow in a pass, so rows go into the circle
    /// once its slot applies. A name that isn't a valid name fails with
    /// `Format`.
    pub fn create_circle(&self, name: &str) -> Result<(CircleId, OperationId), SyncError> {
        self.steps.create_circle(&self.sql, name.into())
    }

    /// Renames a circle this member is in, without changing its key,
    /// members or rows (§15.5, E12).
    pub fn rename_circle(
        &self,
        circle: CircleId,
        name: &str,
    ) -> Result<Option<OperationId>, SyncError> {
        self.steps.administer(&self.sql, StoreChange::RenameCircle { circle, name: name.into() })
    }

    /// Deletes a circle this member is in: from its slot the deleted-circle
    /// rule hides the circle's rows (§15.9, E12).
    pub fn delete_circle(&self, circle: CircleId) -> Result<Option<OperationId>, SyncError> {
        self.steps.administer(&self.sql, StoreChange::DeleteCircle { circle })
    }

    /// Adds a store member to a circle this member is in, sealing every key
    /// the circle has had to them (§15.7, E12).
    pub fn add_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError> {
        self.steps.administer(&self.sql, StoreChange::AddCircleMember { circle, member: *member })
    }

    /// Removes a member from a circle this member is in; the remaining
    /// members replace the exposed circle key in a later pass (§15.8, E12).
    pub fn remove_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError> {
        self.steps
            .administer(&self.sql, StoreChange::RemoveCircleMember { circle, member: *member })
    }

    /// Makes an invite, as an admin, and returns its id, the member id it
    /// fixes and its operation (§13.2, E10). A new person gets a fresh
    /// member id; recovering an existing active member keeps their id, role,
    /// circles and devices. The write checks the invite against the state
    /// this device has applied and journals it, with or without storage; it
    /// is listed in `coven_invites` from the commit, without a code. Once
    /// the write commits, a pass checks it again against the history it
    /// catches up with, grants the storage access and fixes the code, which
    /// `coven_invites` then shows. A check that fails there, or a failed
    /// grant, is the invite's pending record under its operation in
    /// `coven_pending`; cancelling the invite ends it. It expires `lifetime`
    /// after it is made. Made by a device that isn't a registered admin it
    /// fails with
    /// `Rejected(NotAllowed)`; an empty name with `Format`; an App-folder
    /// Dropbox store with `DropboxAppFolderNotShareable`.
    pub fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        lifetime: InviteLifetime,
        inviter: String,
    ) -> Result<(InviteId, MemberId, OperationId), SyncError> {
        self.steps
            .create_invite(&self.sql, coven_sync::NewInvite { target, access, lifetime, inviter })
    }

    /// Approves exactly this checked request (§13.2, E10), and returns the
    /// operation that binds its pair, which `coven_operations` lists from
    /// this write's commit until the app acknowledges its outcome. The write
    /// journals it, with or without storage, without checking it against the
    /// state this device has applied: one the caught-up history refuses,
    /// such as this admin's demotion, is the operation's outcome. Once it
    /// commits, a pass catches up and claims one admission: the entry adding
    /// the new member with the invite's role, or rebinding the existing
    /// member's pair, and registering the request's device, its prefix
    /// carrying the joiner's copies of every store key and the held circle
    /// keys. It waits, claiming nothing, while this device lacks a store key
    /// it must copy. Once begun, approval finishes. A rejection, met before
    /// anything is claimed, ends the invite as a decline does, and is the
    /// operation's outcome; it is not retried with a new entry. An admission
    /// too large for one slot is refused with `AdmissionTooLarge`. A request the invite no longer shows, and an invite
    /// whose lifetime has ended, fail with `InvitationChanged`.
    pub fn approve_join_request(&self, request: &JoinRequest) -> Result<OperationId, SyncError> {
        self.steps.approve_join_request(&self.sql, request)
    }

    /// Declines an open invite's checked request (§13.2, E10). The write
    /// ends the invite, which leaves `coven_invites` with it; once it
    /// commits, a pass deletes the request, so the requester's join ends as
    /// declined, and takes back the storage access the invite granted. On
    /// S3 the key stays pending until deleted and cleared under E9. A
    /// request the invite no longer shows, an invite whose approval has
    /// begun, and one whose lifetime has ended fail with
    /// `InvitationChanged`; an ended one leaves the list when coven writes
    /// its expiry.
    pub fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError> {
        self.steps.decline_join_request(&self.sql, request)
    }

    /// Cancels an invite before anyone joins with it (§13.2, E10). The
    /// write ends the invite, which leaves `coven_invites` with it; once it
    /// commits, a pass deletes a waiting request, so the requester's join
    /// ends as declined, and takes back the storage access the invite
    /// granted. On S3 the key stays pending until deleted and cleared under
    /// E9. An invite whose approval has begun, and one whose lifetime has
    /// ended, fail with `InvitationChanged`.
    pub fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError> {
        self.steps.cancel_invite(&self.sql, *invite)
    }

    /// Acknowledges a finished operation's outcome, in this write, so it
    /// leaves `coven_operations` (§11.4, E6). An app row that names the
    /// operation changes in the same write. One still running fails with
    /// `OperationRunning`; one not listed there with `OperationUnknown`.
    pub fn acknowledge_operation(&self, operation: OperationId) -> Result<(), DbError> {
        self.sql.acknowledge_operation(operation)
    }

    /// Acknowledges the new person's name a store a join published keeps,
    /// in this write, so the store keeps it no longer: the write records the
    /// app's profile of that person, or its request for one, and the two
    /// commit together (§13.2). With no name kept, it succeeds and changes
    /// nothing.
    pub fn acknowledge_joined_invitee(&self) -> Result<(), DbError> {
        self.sql.acknowledge_joined_invitee()
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> SqliteResult<usize> {
        self.sql.execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> SqliteResult<()> {
        self.sql.execute_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        self.sql.query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        self.sql.query(sql, params, map)
    }
}

impl SqlRead for CovenWrite<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        CovenWrite::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        CovenWrite::query(self, sql, params, map)
    }
}

#[cfg(test)]
#[path = "coven_write_tests.rs"]
mod tests;
