//! The app's decisions about an invite as steps of its write (§4, §13.2):
//! each reads the invite in the write's transaction and decides it there,
//! with or without storage; a pass does the storage work after.

use super::*;

/// What the app asks an invite for: whom it adds or recovers, how they
/// reach storage, and how the invite reaches them (§13.2).
pub struct NewInvite {
    /// A new person, or an existing member whose pair the invite replaces.
    pub target: InviteTarget,
    /// The provider account to share with, or the S3 key made for them.
    pub access: InviteAccess,
    /// In person or sent, which fixes its lifetime and its approval.
    pub delivery: InviteDelivery,
}

/// Make an invite, as a step of a write (§13.2), and return its id, the
/// member id it fixes and its operation. The write checks the invite against
/// the slots this device has applied, then journals it, with or without
/// storage; the invite is listed in `coven_invites` from its commit, without
/// a code. Once the write commits, a pass is asked for, which checks the
/// invite again against the history it catches up with, grants its storage
/// access and fixes its code. A check that fails there, or a failed grant,
/// is the invite's pending record in `coven_pending`, under its operation;
/// the app cancels the invite to end it. The invite expires after the
/// delivery's lifetime by this device's clock, from `now`.
///
/// Made by a device that isn't a registered admin, the step fails with
/// `Rejected(NotAllowed)`; for a replacement of a member who isn't active,
/// with `Rejected(TargetGone)`, and of this device's own member, with
/// `Rejected(NotAllowed)`. A name that isn't one fails with `Format`, and
/// access that doesn't fit `location` with its storage failure.
#[expect(
    clippy::too_many_arguments,
    reason = "a step takes the device's identity, its id source, clock and \
              location, and the sync state its commit wakes, beside the invite"
)]
pub fn create_invite(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    held: &MemberIdentity,
    ids: &dyn IdSource,
    now: SystemTime,
    location: &coven_storage::StorageConfig,
    state: Arc<SyncState>,
    invite: NewInvite,
) -> Result<(InviteId, MemberId, OperationId), SyncError> {
    let NewInvite { target, access, delivery } = invite;
    if let InviteTarget::NewMember { name, .. } = &target {
        coven_format::value::name(name)?;
    }
    if matches!(
        location,
        coven_storage::StorageConfig::Dropbox {
            location: coven_storage::DropboxLocation::AppFolder,
            ..
        }
    ) {
        return Err(StorageFailure::DropboxAppFolderNotShareable.into());
    }
    let grant = invite_grant(access, location.provider())?;
    let secret = InviteSecret::generate()?;
    let secret = SecretText::new(serde_json::to_string(secret.to_secret_bytes().as_bytes())?);
    let expires_at = now.checked_add(delivery.lifetime()).ok_or(
        StorageFailure::InvalidConfiguration
            .with_source("the clock cannot represent the invite's expiry"),
    )?;
    let id = InviteId(ids.new_id());
    let made = sql.journal_step(|journal| {
        let applied = journal.store_state()?;
        let me = crate::admin_steps::registered(&applied, device, held)?;
        if !applied
            .members
            .get(&me)
            .is_some_and(|value| !value.removed && value.role == MemberRole::Admin)
        {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        let (member, binding, invitee) = match target {
            InviteTarget::NewMember { role, name } => {
                (MemberId(ids.new_id()), InviteBinding::Add { role }, Some(name))
            }
            InviteTarget::ReplaceMemberKeys { member: target } => {
                let Some(existing) = applied.members.get(&target).filter(|value| !value.removed)
                else {
                    return Err(SyncError::Rejected(OperationRejection::TargetGone(
                        crate::EntryTarget::Member(target),
                    )));
                };
                // Replacing this installation's own pair stops the device that
                // must finish the invite; members replace their own keys directly.
                if target == me {
                    return Err(SyncError::Rejected(OperationRejection::NotAllowed));
                }
                if let InviteGrant::Account { email } = &grant {
                    if !existing
                        .access
                        .same_credential(&MemberAccess::ProviderAccount(email.clone()))
                    {
                        return Err(StorageFailure::InvalidConfiguration
                            .with_source(
                                "a replacement invite shares the member's recorded account",
                            )
                            .into());
                    }
                }
                (target, InviteBinding::Replace { expected: existing.keys }, None)
            }
        };
        let work = InviteWork {
            id,
            member,
            invitee,
            delivery: delivery.kind(),
            binding,
            access: grant,
            secret,
            expires_at,
            state: InviteState::Requested,
        };
        let operation = journal.start_operation(&NewOperation {
            kind: INVITE.into(),
            data: serde_json::to_vec(&work)?,
            started_by: "create_invite".into(),
            pauses_writes: false,
        })?;
        Ok((id, member, operation))
    })?;
    sql.after_commit(move || state.request_pass());
    Ok(made)
}

/// The storage access an invite grants, as the location allows it: a
/// provider account to share with, or an S3 key made for the person.
fn invite_grant(access: InviteAccess, provider: CloudProvider) -> Result<InviteGrant, SyncError> {
    match access {
        InviteAccess::ProviderAccount { email } => {
            let email = email.trim().to_ascii_lowercase();
            if email.is_empty() {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("invite account is empty")
                    .into());
            }
            if provider == CloudProvider::S3 {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("an S3 invitation needs its own access key")
                    .into());
            }
            Ok(InviteGrant::Account { email })
        }
        InviteAccess::S3AccessKey { access_key_id, secret_access_key } => {
            if access_key_id.is_empty() || secret_access_key.as_str().is_empty() {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("invite access key is empty")
                    .into());
            }
            if provider != CloudProvider::S3 {
                return Err(StorageFailure::InvalidConfiguration
                    .with_source("an access-key invitation requires S3")
                    .into());
            }
            Ok(InviteGrant::S3Key(S3Credentials { access_key_id, secret_access_key }))
        }
    }
}

/// Approve exactly this checked request of a sent invite, as a step of a
/// write (§13.2), and return the operation that binds its pair: the write
/// journals it, and the invite's approval begins, with or without storage.
/// Once the write commits, a pass is asked for, which catches up, claims
/// copies of every store key and the held circle keys for the request's
/// pair, then the entry adding the member or replacing their pair; once
/// begun, approval finishes. The operation's outcome is listed in
/// `coven_operations`: a rejection ends the invite as a decline does. The
/// step doesn't check the approval against the applied state, so the app is
/// never refused an approval it was shown as waiting: one the history
/// refuses, as when this admin was demoted, is refused by the pass, as its
/// binding's outcome. A request the invite no longer shows, an in-person
/// invite's, whose approval began on its own, and an invite whose lifetime
/// ended by `now` fail the step with `InvitationChanged`.
pub fn approve_join_request(
    sql: &SqlContext<'_, '_>,
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
            (InviteState::Requested | InviteState::Grant | InviteState::Waiting { .. }, None) => {}
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

#[cfg(test)]
#[path = "sequence_invite_steps_tests.rs"]
mod tests;
