//! Account intentions, confirmed provider state and their durable request (§13).

use crate::{sqlite::DatabaseConnection, DbError, NewOperation, OperationId, PendingValue};
use coven_crypto::SecretBytes;
use coven_foundation::id_source::DeviceId;

/// One account's intention derived from replay and this device's invitations.
pub struct ProviderAccessChange {
    /// Provider account identity.
    pub account: String,
    /// Whether a current member or open invite needs the account.
    pub intended: bool,
    /// Received access entries describe grants already delivered.
    pub initially_granted: bool,
    /// An open invite also needs the provider's acceptance information.
    pub needs_invitation: bool,
    /// Whether this installation holds the owner's account.
    pub owner: bool,
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
    /// Latest replay/invitation intention.
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
        if operation.is_none() && target.is_some() { return Err(rusqlite::Error::InvalidQuery); }
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
    crate::pending::clear_operation(db, id)?;
    db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id.0])?;
    Ok(())
}

/// Called inside the same transaction as replay or an invitation change.
pub(crate) fn adopt<P: PendingValue>(
    db: &DatabaseConnection,
    reporter: DeviceId,
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
            // Unchanged replay also preserves a previously reported failure.
            if record.last_step == 1
                || (record.last_step == 2 && before.intended == change.intended)
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
                db.internal_execute(
                    "UPDATE _coven_operations SET last_step=0 WHERE id=?1 AND last_step!=0",
                    [request.operation.0],
                )?;
                crate::retry::clear(db, crate::RetrySubject::Operation(request.operation))?;
                db.internal_execute("UPDATE _coven_provider_access SET target=NULL WHERE account=?1 AND target IS NOT NULL", [&change.account])?;
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
        crate::pending::save(
            db,
            &P::provider_access(
                id,
                reporter,
                change.account.clone(),
                change.intended,
                change.owner,
            ),
        )?;
    }
    Ok(())
}
