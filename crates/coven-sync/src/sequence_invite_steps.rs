//! The app's decisions about an invite as steps of its write (§4, §13.2):
//! each reads the invite in the write's transaction and decides it there,
//! with or without storage; a pass does the storage work after.

use super::*;

/// Approve exactly this checked request of a sent invite, as a step of a
/// write (§13.2), and return the operation that binds its pair: the write
/// journals it, and the invite's approval begins, with or without storage.
/// Once the write commits, a pass is asked for, which catches up, claims
/// copies of every store key and the held circle keys for the request's
/// pair, then the entry adding the member or replacing their pair; once
/// begun, approval finishes. The operation's outcome is listed in
/// `coven_operations`: a rejection ends the invite as a decline does. An
/// approval the applied state refuses fails the step with `Rejected`. A
/// request the invite no longer shows, an in-person invite's, whose approval
/// began on its own, and an invite whose lifetime ended by `now` fail the
/// step with `InvitationChanged`.
pub fn approve_join_request(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    held: &MemberIdentity,
    now: SystemTime,
    waits: Arc<RetryWaits>,
    state: Arc<SyncState>,
    request: &JoinRequest,
) -> Result<OperationId, SyncError> {
    let (invite, binding) = sql.journal_step(|journal| {
        let (record, mut work) = invite_record(journal, request.invite)?;
        if work.expired(now) {
            return Err(SyncError::InvitationChanged);
        }
        let InviteState::Waiting { code, request: Some(stored) } = &work.state else {
            return Err(SyncError::InvitationChanged);
        };
        let (code, stored) = (code.clone(), stored.clone());
        if work.present_request(&stored)? != *request {
            return Err(SyncError::InvitationChanged);
        }
        let change = work.binding_change(&stored)?;
        let applied = journal.store_state()?;
        let author = crate::admin_steps::registered(&applied, device, held)?;
        // A binding the applied state already has is journaled all the
        // same: the pass finds it satisfied, and the invite ends approved.
        crate::admin_steps::changes(journal, &applied, device, author, &change)?;
        let binding =
            crate::admin_steps::start(journal, SequenceOperation::change(change), INVITE)?;
        work.state = InviteState::Approving { code, request: stored, binding };
        journal.advance_operation(&work.update(&record)?)?;
        Ok((record.id, binding))
    })?;
    sql.after_commit(move || {
        waits.operation_advanced(invite);
        state.request_pass();
    });
    Ok(binding)
}

/// Decline an invite's checked request, as a step of a write (§13.2): the
/// write ends the invite, which takes it out of `coven_invites`, with or
/// without storage. Once the write commits, a pass is asked for, which
/// deletes the request, so the requester's join ends as declined, and takes
/// back the access the invite granted. A request the invite no longer
/// shows, an invite whose approval has begun, and one whose lifetime ended
/// by `now` fail the step with `InvitationChanged`.
pub fn decline_join_request(
    sql: &SqlContext<'_, '_>,
    now: SystemTime,
    waits: Arc<RetryWaits>,
    state: Arc<SyncState>,
    request: &JoinRequest,
) -> Result<(), SyncError> {
    end_invite(sql, now, waits, state, request.invite, Some(request))
}

/// Cancel an invite before anyone joins with it, as a step of a write
/// (§13.2): the write ends the invite, which takes it out of
/// `coven_invites`, with or without storage. Once the write commits, a pass
/// is asked for, which deletes a waiting request, so the requester's join
/// ends as declined, and takes back the access the invite granted. An
/// invite whose approval has begun, including an in-person invite's own,
/// and one whose lifetime ended by `now`, fail the step with
/// `InvitationChanged`.
pub fn cancel_invite(
    sql: &SqlContext<'_, '_>,
    now: SystemTime,
    waits: Arc<RetryWaits>,
    state: Arc<SyncState>,
    invite: InviteId,
) -> Result<(), SyncError> {
    end_invite(sql, now, waits, state, invite, None)
}

/// End the invite in the write, for a decline of `request` or a cancel.
/// Reading the invite and ending it share the write's transaction, so no
/// expiry comes between. An invite whose lifetime ended is left to its
/// expiry, a write of its own that the handle makes at that time.
fn end_invite(
    sql: &SqlContext<'_, '_>,
    now: SystemTime,
    waits: Arc<RetryWaits>,
    state: Arc<SyncState>,
    invite: InviteId,
    request: Option<&JoinRequest>,
) -> Result<(), SyncError> {
    let ended = sql.journal_step(|journal| {
        let (record, mut work) = invite_record(journal, invite)?;
        if work.expired(now) {
            return Err(SyncError::InvitationChanged);
        }
        match (&work.state, request) {
            (InviteState::Waiting { request: Some(stored), .. }, Some(request)) => {
                if work.present_request(stored)? != *request {
                    return Err(SyncError::InvitationChanged);
                }
            }
            (InviteState::Grant | InviteState::Waiting { .. }, None) => (),
            _ => return Err(SyncError::InvitationChanged),
        }
        work.state = InviteState::Settled;
        journal.advance_operation(&work.update(&record)?)?;
        Ok(record.id)
    })?;
    sql.after_commit(move || {
        waits.operation_advanced(ended);
        state.request_pass();
    });
    Ok(())
}

/// The invite's journal row and its state, as the write sees them; a
/// finished or unknown invite fails with `InvitationChanged`.
fn invite_record(
    journal: &Journal<'_>,
    invite: InviteId,
) -> Result<(OperationRecord, InviteWork), SyncError> {
    for record in journal.operations(INVITE)? {
        let work = InviteWork::read(&record)?;
        if work.id == invite {
            return Ok((record, work));
        }
    }
    Err(SyncError::InvitationChanged)
}
