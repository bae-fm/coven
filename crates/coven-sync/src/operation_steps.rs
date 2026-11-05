//! The steps of the app's write that journal an operation (§4, §11.1).

use crate::sequence_sync::make_retry_due;
use crate::sequence_sync::sequence_invites::{approve_request, end_invite, make_invite, NewInvite};
use crate::{RetryWaits, SequenceState, SyncError};
use coven_crypto::{custody::KeySession, MemberIdentity};
use coven_database::{OperationId, SqlContext};
use coven_format::admin::StoreChange;
use coven_foundation::{
    clock::ClockRef,
    files::StoreDir,
    id_source::{DeviceId, IdSourceRef, InviteId, MemberId},
};
use coven_storage::{ProviderSignOut, StorageFailure};
use std::sync::Arc;

#[path = "admin_steps.rs"]
mod admin;

/// The steps of the app's write that journal an operation (§4, §11.1):
/// checked against the state this device has applied, journaled with or
/// without storage, and run by a later pass. One per writer handle.
pub struct OperationSteps {
    device: DeviceId,
    identity: Arc<KeySession<MemberIdentity>>,
    ids: IdSourceRef,
    clock: ClockRef,
    directory: StoreDir,
    waits: Arc<RetryWaits>,
    state: Arc<SequenceState>,
}

impl OperationSteps {
    /// Compose where the writer handle is made.
    pub fn new(
        device: DeviceId,
        identity: Arc<KeySession<MemberIdentity>>,
        ids: IdSourceRef,
        clock: ClockRef,
        directory: StoreDir,
        waits: Arc<RetryWaits>,
        state: Arc<SequenceState>,
    ) -> Self {
        Self { device, identity, ids, clock, directory, waits, state }
    }

    /// The member pair this installation holds, which an admin step checks
    /// the applied binding against.
    fn held(&self) -> Result<MemberIdentity, SyncError> {
        Ok(self.identity.read()?.ok_or(StorageFailure::MemberKeysMissing)?)
    }

    /// Journal a device's removal as a step of an app's write, and say how
    /// its member signs out of the provider, if anything is left to do
    /// there, from the recorded location (§14.2).
    pub fn remove_device(
        &self,
        sql: &SqlContext<'_, '_>,
        device: DeviceId,
    ) -> Result<(Option<OperationId>, Option<ProviderSignOut>), SyncError> {
        let location =
            coven_storage::recorded_config(&self.directory)?.ok_or(SyncError::NoStorage)?;
        let operation = self.administer(sql, StoreChange::RemoveDevice { device })?;
        Ok((operation, location.provider().sign_out()))
    }

    /// Make an invite, as a step of a write (§13.2), and return its id, the
    /// member id it fixes and its operation. The write checks the invite
    /// against the slots this device has applied, then journals it, with or
    /// without storage; the invite is listed in `coven_invites` from its
    /// commit, without a code. Once the write commits, a pass is asked for,
    /// which checks the invite again against the history it catches up with,
    /// grants its storage access and fixes its code. A check that fails
    /// there, or a failed grant, is the invite's pending record in
    /// `coven_pending`, under its operation; the app cancels the invite to
    /// end it. The invite expires after the delivery's lifetime by this
    /// device's clock, from now. The invite is made at the store's recorded
    /// location; a store with none fails with `NoStorage`.
    ///
    /// Made by a device that isn't a registered admin, the step fails with
    /// `Rejected(NotAllowed)`; for a replacement of a member who isn't
    /// active, with `Rejected(TargetGone)`, and of this device's own member,
    /// with `Rejected(NotAllowed)`. A name that isn't one fails with
    /// `Format`, and access that doesn't fit the location with its storage
    /// failure.
    pub fn create_invite(
        &self,
        sql: &SqlContext<'_, '_>,
        invite: NewInvite,
    ) -> Result<(InviteId, MemberId, OperationId), SyncError> {
        let location =
            coven_storage::recorded_config(&self.directory)?.ok_or(SyncError::NoStorage)?;
        let made = make_invite(
            sql,
            self.device,
            &self.held()?,
            self.ids.as_ref(),
            self.clock.now(),
            &location,
            invite,
        )?;
        let state = self.state.clone();
        sql.after_commit(move || state.request_pass());
        Ok(made)
    }

    /// Approve exactly this checked request of a sent invite, as a step of a
    /// write (§13.2), and return the operation that binds its pair: the
    /// write journals it, and the invite's approval begins, with or without
    /// storage. Once the write commits, a pass is asked for, which catches
    /// up, claims copies of every store key and the held circle keys for the
    /// request's pair, then the entry adding the member or replacing their
    /// pair; once begun, approval finishes. The operation's outcome is
    /// listed in `coven_operations`: a rejection ends the invite as a
    /// decline does. The step doesn't check the approval against the applied
    /// state, so the app is never refused an approval it was shown as
    /// waiting: one the history refuses, as when this admin was demoted, is
    /// refused by the pass, as its binding's outcome. A request the invite no
    /// longer shows, an in-person invite's, whose approval began on its own,
    /// and an invite whose lifetime ended fail the step with
    /// `InvitationChanged`.
    pub fn approve_join_request(
        &self,
        sql: &SqlContext<'_, '_>,
        request: &crate::JoinRequest,
    ) -> Result<OperationId, SyncError> {
        let (invite, binding) = approve_request(sql, self.clock.now(), request)?;
        self.wake_after_commit(sql, invite);
        Ok(binding)
    }

    /// Decline an invite's checked request, as a step of a write (§13.2):
    /// the write ends the invite, which takes it out of `coven_invites`, with
    /// or without storage. Once the write commits, a pass is asked for, which
    /// deletes the request, so the requester's join ends as declined, and
    /// takes back the access the invite granted. A request the invite no
    /// longer shows, an invite whose approval has begun, and one whose
    /// lifetime ended fail the step with `InvitationChanged`.
    pub fn decline_join_request(
        &self,
        sql: &SqlContext<'_, '_>,
        request: &crate::JoinRequest,
    ) -> Result<(), SyncError> {
        let ended = end_invite(sql, self.clock.now(), request.invite, Some(request))?;
        self.wake_after_commit(sql, ended);
        Ok(())
    }

    /// Cancel an invite before anyone joins with it, as a step of a write
    /// (§13.2): the write ends the invite, which takes it out of
    /// `coven_invites`, with or without storage. Once the write commits, a
    /// pass is asked for, which deletes a waiting request, so the requester's
    /// join ends as declined, and takes back the access the invite granted.
    /// An invite whose approval has begun, including an in-person invite's
    /// own, and one whose lifetime ended, fail the step with
    /// `InvitationChanged`.
    pub fn cancel_invite(
        &self,
        sql: &SqlContext<'_, '_>,
        invite: InviteId,
    ) -> Result<(), SyncError> {
        let ended = end_invite(sql, self.clock.now(), invite, None)?;
        self.wake_after_commit(sql, ended);
        Ok(())
    }

    /// Once the write commits, the invite's wait ends in this session and a
    /// pass is asked for.
    fn wake_after_commit(&self, sql: &SqlContext<'_, '_>, invite: OperationId) {
        let (waits, state) = (self.waits.clone(), self.state.clone());
        sql.after_commit(move || {
            waits.operation_advanced(invite);
            state.request_pass();
        });
    }

    /// Make an operation due now, as a step of a write (E6): its pending
    /// record and retry wait end in the write's transaction. It cannot
    /// resurrect an invalidated action, or one that waits for an update not
    /// installed yet, and it can't bypass a provider cooldown or the
    /// stuck-device rule. Once the write commits, the operation's wait ends
    /// in this session too, and a pass is asked for to run it.
    pub fn retry_pending_operation(
        &self,
        sql: &SqlContext<'_, '_>,
        id: OperationId,
    ) -> Result<(), SyncError> {
        make_retry_due(sql, id)?;
        let (waits, state) = (self.waits.clone(), self.state.clone());
        sql.after_commit(move || {
            waits.retried(id);
            state.request_pass();
        });
        Ok(())
    }
}
