//! The app's decisions about an invite as steps of its write (§4, §13.2):
//! each reads the invite in the write's transaction and decides it there,
//! with or without storage; a pass does the storage work after. The store's
//! `OperationSteps` run them and wake that pass.

use super::*;

/// What the app asks an invite for: whom it adds or recovers, how they
/// reach storage, and how long it stays open (§13.2).
pub struct NewInvite {
    /// A new person, or an existing member whose pair the invite replaces.
    pub target: InviteTarget,
    /// The provider account to share with, or the S3 key made for them.
    pub access: InviteAccess,
    /// How long after it is made the invite expires.
    pub lifetime: InviteLifetime,
    /// The inviting admin's name, a format `name`, which the invite carries
    /// for the joining person's screens (§13.2).
    pub inviter: String,
}

/// Make an invite, as a step of a write (§13.2), and return its id, the
/// member id it fixes and its operation. The write checks the invite against
/// the slots this device has applied, then journals it, with or without
/// storage; the invite is listed in `coven_invites` from its commit, without
/// a code. Once the write commits, a pass is asked for, which checks the
/// invite again against the history it catches up with, grants its storage
/// access and fixes its code. A check that fails there, or a failed grant,
/// is the invite's pending record in `coven_pending`, under its operation;
/// the app cancels the invite to end it. The invite expires after its
/// lifetime by this device's clock, from `now`.
///
/// Made by a device that isn't a registered admin, the step fails with
/// `Rejected(NotAllowed)`; for a replacement of a member who isn't active,
/// with `Rejected(TargetGone)`, and of this device's own member, with
/// `Rejected(NotAllowed)`. Access that doesn't fit `location` fails with its
/// storage failure.
pub(crate) fn make_invite(
    sql: &SqlContext<'_, '_>,
    device: DeviceId,
    held: &MemberIdentity,
    ids: &dyn IdSource,
    now: SystemTime,
    location: &coven_storage::StorageConfig,
    custody: &crate::DeviceCredentials,
    invite: NewInvite,
) -> Result<(InviteId, MemberId, OperationId), SyncError> {
    let NewInvite { target, access, lifetime, inviter } = invite;
    coven_format::value::name(&inviter)?;
    if matches!(
        location,
        coven_storage::StorageConfig::Dropbox {
            location: coven_storage::DropboxLocation::AppFolder,
            ..
        }
    ) {
        return Err(StorageFailure::DropboxAppFolderNotShareable.into());
    }
    let grant = invite_grant(access, location.provider(), custody)?;
    let secret = InviteSecret::generate()?;
    let secret = SecretText::new(serde_json::to_string(secret.to_secret_bytes().as_bytes())?);
    let expires_at = now.checked_add(lifetime.duration()).ok_or(
        StorageFailure::InvalidConfiguration
            .with_source("the clock cannot represent the invite's expiry"),
    )?;
    let id = InviteId(ids.new_id());
    let made = sql.journal_step(|journal| {
        let applied = journal.store_state()?;
        let me = crate::sequence_sync::registered(&applied, device, held)?;
        if !applied.active_admin(&me) {
            return Err(SyncError::Rejected(OperationRejection::NotAllowed));
        }
        let (member, binding) = match target {
            InviteTarget::NewMember { role } => {
                (MemberId(ids.new_id()), InviteBinding::Add { role })
            }
            InviteTarget::ReplaceMemberKeys { member: target } => {
                let Some(existing) = applied.active_member(&target) else {
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
                    let account = MemberAccess::ProviderAccount(email.clone());
                    if !existing.access.as_ref().is_some_and(|held| held.same_credential(&account))
                    {
                        return Err(StorageFailure::InvalidConfiguration
                            .with_source(
                                "a replacement invite shares the member's recorded account",
                            )
                            .into());
                    }
                }
                (target, InviteBinding::Replace { expected: existing.keys })
            }
        };
        let work = InviteWork {
            id,
            device,
            member,
            inviter,
            binding,
            access: grant,
            secret,
            expires_at,
            state: InviteState::Requested,
        };
        let operation = journal.start_operation(&NewOperation {
            kind: operation_kind::INVITE.into(),
            data: serde_json::to_vec(&work)?,
            started_by: "create_invite".into(),
            pauses_writes: false,
        })?;
        Ok((id, member, operation))
    })?;
    Ok(made)
}

/// The storage access an invite grants, as the location allows it: a
/// provider account to share with, or on S3 a key the person made for the
/// invitee or the one this device connects with, read from `custody`.
fn invite_grant(
    access: InviteAccess,
    provider: CloudProvider,
    custody: &crate::DeviceCredentials,
) -> Result<InviteGrant, SyncError> {
    match access {
        InviteAccess::ProviderAccount { email } => {
            let email = MemberAccess::normalized_account(&email);
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
        InviteAccess::S3AccessKey { .. } | InviteAccess::HeldS3AccessKey
            if provider != CloudProvider::S3 =>
        {
            Err(StorageFailure::InvalidConfiguration
                .with_source("an access-key invitation requires S3")
                .into())
        }
        InviteAccess::S3AccessKey { access_key_id, secret_access_key } => {
            let credentials = S3Credentials { access_key_id, secret_access_key };
            credentials.validate()?;
            Ok(InviteGrant::S3Key(credentials))
        }
        InviteAccess::HeldS3AccessKey => match custody.connection()? {
            Some(coven_storage::ConnectionCredentials {
                credentials: coven_storage::StorageCredentials::S3(key),
                ..
            }) => Ok(InviteGrant::S3Key(key)),
            Some(_) | None => Err(StorageFailure::InvalidConfiguration
                .with_source("this device holds no S3 key to give")
                .into()),
        },
    }
}

/// Approve exactly this checked request, as a step of a write (§13.2), and
/// return the operation that binds its pair: the write journals it, and the
/// invite's approval begins, with or without storage. Once the write commits, a
/// pass is asked for, which catches up and claims one admission: the entry
/// admitting the member with the request's pair, or rebinding their pair, and
/// registering the request's device, carrying copies of every store key and
/// the held circle keys for that pair; once begun, approval finishes. The
/// operation's outcome is listed in `coven_operations`: a rejection ends the
/// invite as a decline does. The step doesn't check the approval against the
/// applied state, so the app is never refused an approval it was shown as
/// waiting: one the history refuses, as when this admin was demoted, is refused
/// by the pass, as its binding's outcome. A request the invite no longer shows,
/// and an invite whose lifetime ended by `now`, fail the step with
/// `InvitationChanged`.
pub(crate) fn approve_request(
    sql: &SqlContext<'_, '_>,
    now: SystemTime,
    request: &JoinRequest,
) -> Result<(OperationId, OperationId), SyncError> {
    sql.journal_step(|journal| {
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
        let binding = SequenceOperation::change(change).start(journal, operation_kind::INVITE)?;
        work.state = InviteState::Approving { code, request: stored, binding };
        journal.advance_operation(&work.update(&record)?)?;
        Ok((record.id, binding))
    })
}

/// End the invite in the write, for a decline of `request` or a cancel.
/// Reading the invite and ending it share the write's transaction, so no
/// expiry comes between. An invite whose lifetime ended is left to its
/// expiry, a write of its own that the handle makes at that time.
pub(crate) fn end_invite(
    sql: &SqlContext<'_, '_>,
    now: SystemTime,
    invite: InviteId,
    request: Option<&JoinRequest>,
) -> Result<OperationId, SyncError> {
    sql.journal_step(|journal| {
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
            (
                InviteState::Requested
                | InviteState::Sharing { .. }
                | InviteState::Grant
                | InviteState::Waiting { .. },
                None,
            ) => {}
            _ => return Err(SyncError::InvitationChanged),
        }
        work.end();
        journal.advance_operation(&work.update(&record)?)?;
        Ok(record.id)
    })
}

/// The invite's journal row and its state, as the write sees them; a
/// finished or unknown invite fails with `InvitationChanged`.
fn invite_record(
    journal: &Journal<'_>,
    invite: InviteId,
) -> Result<(OperationRecord, InviteWork), SyncError> {
    for record in journal.operations(operation_kind::INVITE)? {
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
