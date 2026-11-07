//! Account intentions, confirmed provider state and their durable request (§14.2).

use crate::{sqlite::DatabaseConnection, DbError, NewOperation, OperationId, PendingValue};
use coven_crypto::SecretBytes;

/// A provider-access operation's steps (§14.2): queued; its request's
/// target fixed, before storage is called; and an inconclusive request
/// reported, to be fixed again on retry.
pub(crate) const REQUEST_QUEUED_STEP: u32 = 0;
/// The step a provider-access operation reaches once its request's target
/// is fixed, before storage is called.
pub const REQUEST_FIXED_STEP: u32 = 1;
pub(crate) const REQUEST_FAILED_STEP: u32 = 2;

/// One account's intention derived from membership and this device's invitations.
pub struct ProviderAccessChange {
    /// Provider account identity.
    pub account: String,
    /// Whether a current member or open invite needs the account.
    pub intended: bool,
    /// Received access entries describe grants already delivered.
    pub initially_granted: bool,
    /// An open invite also needs the provider's acceptance information.
    pub needs_invitation: bool,
    /// The journal payload identifying this account.
    pub operation: NewOperation,
}

/// A request remains associated with its account until confirmation matches intention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAccessRequest {
    /// Its stable pending subject.
    pub operation: OperationId,
    /// Fixed before sending; absent while ready.
    pub target: Option<bool>,
}

/// A request makes confirmation unknown until the provider supplies evidence.
#[derive(Debug)]
pub struct ProviderAccess {
    /// Latest membership and invitation intention.
    pub intended: bool,
    /// Confirmed grant, or unknown when a request has no conclusive reply.
    pub granted: Option<bool>,
    /// Encoded provider acceptance information from a confirmed grant.
    pub invitation: Option<SecretBytes>,
    /// Unfinished work, including a failed request.
    pub request: Option<ProviderAccessRequest>,
}

pub(crate) fn read(
    db: &DatabaseConnection,
    account: &str,
) -> Result<Option<ProviderAccess>, DbError> {
    db.query("SELECT intended,granted,invitation,operation,target FROM _coven_provider_access WHERE account=?1", [account], |row| {
        let operation: Option<i64> = row.get(3)?;
        let target: Option<bool> = row.get(4)?;
        if operation.is_none() && target.is_some() { return Err(crate::write_encoding::damaged(4, rusqlite::types::Type::Integer)); }
        Ok(ProviderAccess { intended: row.get(0)?, granted: row.get(1)?, invitation: row.get::<_,Option<Vec<u8>>>(2)?.map(SecretBytes::new),
            request: operation.map(|id| ProviderAccessRequest { operation: OperationId(id), target }) })
    }).map(|mut rows| rows.pop())
}

pub(crate) fn finish(
    db: &DatabaseConnection,
    account: &str,
    id: OperationId,
) -> Result<(), DbError> {
    db.internal_execute("UPDATE _coven_provider_access SET operation=NULL,target=NULL WHERE account=?1 AND operation=?2", (account,id.0))?;
    crate::operation::finish(db, id)?;
    Ok(())
}

/// Queue the account's request `id` again with no target fixed, ending the
/// wait its last attempt left.
pub(crate) fn requeue(
    db: &DatabaseConnection,
    account: &str,
    id: OperationId,
) -> Result<(), DbError> {
    crate::operation::set_step(db, id, REQUEST_QUEUED_STEP)?;
    crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
    db.internal_execute(
        "UPDATE _coven_provider_access SET target=NULL WHERE account=?1 AND target IS NOT NULL",
        [account],
    )?;
    Ok(())
}

/// Fix a request's target to the account's intention before storage is
/// called. A revoke may remove the direct share supplying acceptance
/// tokens even when other permissions remain, so neither confirmation is
/// kept.
pub(crate) fn fix_target(db: &DatabaseConnection, account: &str) -> Result<(), DbError> {
    db.internal_execute("UPDATE _coven_provider_access SET granted=NULL,invitation=NULL,target=intended WHERE account=?1", [account])?;
    Ok(())
}

/// The provider confirmed `target`, with its acceptance information when
/// it granted access; the request has no target fixed any more.
pub(crate) fn confirm(
    db: &DatabaseConnection,
    account: &str,
    target: bool,
    invitation: Option<&SecretBytes>,
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_provider_access SET granted=?1,invitation=?2,target=NULL WHERE account=?3",
        (target, invitation.map(SecretBytes::as_bytes), account),
    )?;
    Ok(())
}

/// Called inside the same transaction as membership or an invitation change.
pub(crate) fn adopt<P: PendingValue>(
    db: &DatabaseConnection,
    changes: &[ProviderAccessChange],
) -> Result<(), DbError> {
    for change in changes {
        let before = match read(db, &change.account)? {
            Some(before) => before,
            None => {
                db.internal_execute(
                    "INSERT INTO _coven_provider_access(account,intended,granted) VALUES(?1,?2,?3)",
                    (&change.account, change.intended, change.initially_granted),
                )?;
                ProviderAccess {
                    intended: change.intended,
                    granted: Some(change.initially_granted),
                    invitation: None,
                    request: None,
                }
            }
        };
        db.internal_execute(
            "UPDATE _coven_provider_access SET intended=?1 WHERE account=?2 AND intended!=?1",
            (change.intended, &change.account),
        )?;
        if let Some(request) = &before.request {
            let record =
                crate::operation::find(db, request.operation)?.ok_or(DbError::DamagedDatabase)?;
            // An in-flight completion must first account for its fixed target.
            // Unchanged membership also preserves a previously reported failure.
            if record.last_step == REQUEST_FIXED_STEP
                || (record.last_step == REQUEST_FAILED_STEP && before.intended == change.intended)
            {
                continue;
            }
        }
        let needed = before.granted != Some(change.intended)
            || (change.intended && change.needs_invitation && before.invitation.is_none());
        if !needed {
            if let Some(request) = before.request {
                finish(db, &change.account, request.operation)?;
            }
            continue;
        }
        let id = match before.request {
            Some(request) => {
                requeue(db, &change.account, request.operation)?;
                request.operation
            }
            None => {
                let id = crate::operation::insert(db, &change.operation)?;
                db.internal_execute(
                    "UPDATE _coven_provider_access SET operation=?1 WHERE account=?2",
                    (id.0, &change.account),
                )?;
                id
            }
        };
        crate::pending::save(db, &P::provider_access(id, change.account.clone(), change.intended))?;
    }
    Ok(())
}

/// Take back accounts whose confirmed grant can no longer be trusted: the
/// person confirmed that a lost owner device, whose grants may have landed
/// after this device's revocations, can land nothing more (§20.9). Each
/// account forgets its confirmed grant and queues a request for its
/// intention, so a revocation follows any grant that landed before the
/// confirmation. An account with a request between its fixed target and its
/// reply fails the call with `ProviderRequestRunning`, and nothing changes:
/// that request's reply may precede the confirmation.
pub(crate) fn retake<P: PendingValue>(
    db: &DatabaseConnection,
    changes: &[ProviderAccessChange],
) -> Result<(), DbError> {
    for change in changes {
        if let Some(request) = read(db, &change.account)?.and_then(|state| state.request) {
            let record =
                crate::operation::find(db, request.operation)?.ok_or(DbError::DamagedDatabase)?;
            if record.last_step == REQUEST_FIXED_STEP {
                return Err(DbError::ProviderRequestRunning);
            }
            if record.last_step == REQUEST_FAILED_STEP {
                // A reported failure waits for the app's retry; this
                // request is that retry.
                requeue(db, &change.account, request.operation)?;
            }
        }
        db.internal_execute(
            "INSERT INTO _coven_provider_access(account,intended) VALUES(?1,?2) \
             ON CONFLICT(account) DO NOTHING",
            (&change.account, change.intended),
        )?;
        db.internal_execute(
            "UPDATE _coven_provider_access SET granted=NULL,invitation=NULL WHERE account=?1",
            [&change.account],
        )?;
    }
    adopt::<P>(db, changes)
}

#[cfg(test)]
#[path = "provider_access_tests.rs"]
pub(crate) mod tests;
