//! Atomic provider intentions and request confirmations.
use crate::provider_access::{adopt, finish, read};
use crate::{
    DbError, NewOperation, OperationId, PendingValue, ProviderAccess, ProviderAccessChange,
    ProviderAccessRequest,
};
use coven_crypto::SecretBytes;

impl crate::Database {
    /// Read confirmed sharing and its unfinished request from one committed row.
    pub async fn provider_access(
        &self,
        account: String,
    ) -> Result<Option<ProviderAccess>, DbError> {
        self.call(move |inner| inner.access.with_writer(|db| read(db, &account))).await
    }

    /// Record a new invitation and its account intention atomically.
    pub async fn start_operation_with_access<P: PendingValue>(
        &self,
        operation: NewOperation,
        changes: Vec<ProviderAccessChange>,
    ) -> Result<OperationId, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    let id = crate::operation::insert(db, &operation)?;
                    adopt::<P>(db, &changes)?;
                    Ok(id)
                })
            })
        })
        .await
    }

    /// Advance an invitation and its account intention atomically, ending
    /// the operation it started for the step it leaves, `child`, with its
    /// outcome, in the same transaction (§13.2).
    pub async fn advance_operation_with_access<P: PendingValue>(
        &self,
        update: crate::OperationUpdate,
        changes: Vec<ProviderAccessChange>,
        child: Option<(OperationId, crate::OperationOutcome)>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    crate::operation::advance(db, &update)?;
                    if let Some((child, outcome)) = &child {
                        crate::operation::finish_with_outcome(db, *child, outcome)?;
                    }
                    adopt::<P>(db, &changes)
                })
            })
        })
        .await
    }

    /// Fix the request's target before calling storage. A failed or interrupted
    /// request has unknown provider state, so retry must confirm the target again.
    pub async fn start_provider_request<P: PendingValue>(
        &self,
        account: String,
        id: OperationId,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    let state = read(db, &account)?.ok_or(DbError::OperationChanged(id))?;
                    if state.request.as_ref().map(|r| r.operation) != Some(id) {
                        return Err(DbError::OperationChanged(id));
                    }
                    let record =
                        crate::operation::find(db, id)?.ok_or(DbError::OperationChanged(id))?;
                    if record.last_step == crate::provider_access::REQUEST_FIXED_STEP {
                        return Err(DbError::OperationChanged(id));
                    }
                    crate::provider_access::fix_target(db, &account)?;
                    crate::operation::set_step(db, id, crate::provider_access::REQUEST_FIXED_STEP)?;
                    crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
                    crate::pending::save(db, &P::provider_access(id, account, state.intended))?;
                    Ok(state.intended)
                })
            })
        })
        .await
    }

    /// Record a revocation's reply that listed grants left in place: the
    /// operation `id` moves from its fixed request to a reported failure,
    /// recording `data`, with `pending` as its record. An entry that made
    /// the account intended again while the request ran makes the reply
    /// obsolete: the opposite request is queued on the same pending subject
    /// instead, as a confirmation's is, and this returns false (§20.9).
    pub async fn fail_provider_request<P: PendingValue>(
        &self,
        account: String,
        id: OperationId,
        data: Vec<u8>,
        pending: P,
    ) -> Result<bool, DbError> {
        let update = crate::OperationUpdate {
            id,
            previous: crate::provider_access::REQUEST_FIXED_STEP,
            last_step: crate::provider_access::REQUEST_FAILED_STEP,
            data,
        };
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    let state = read(db, &account)?.ok_or(DbError::OperationChanged(update.id))?;
                    let request = state.request.ok_or(DbError::OperationChanged(update.id))?;
                    if request.operation != update.id || pending.operation() != Some(update.id) {
                        return Err(DbError::Invariant {
                            what: "a provider request's condition names its own operation",
                        });
                    }
                    let target = request.target.ok_or(DbError::OperationChanged(update.id))?;
                    if state.intended != target {
                        crate::provider_access::requeue(db, &account, update.id)?;
                        crate::pending::save(
                            db,
                            &P::provider_access(update.id, account, state.intended),
                        )?;
                        return Ok(false);
                    }
                    crate::operation::advance(db, &update)?;
                    crate::pending::save(db, &pending)?;
                    Ok(true)
                })
            })
        })
        .await
    }

    /// Confirm a fixed request and either finish it or queue the opposite target
    /// on the same pending subject, in one commit.
    pub async fn complete_provider_request<P: PendingValue>(
        &self,
        account: String,
        id: OperationId,
        target: bool,
        invitation: Option<SecretBytes>,
    ) -> Result<bool, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    let state = read(db, &account)?.ok_or(DbError::OperationChanged(id))?;
                    if state.request
                        != Some(ProviderAccessRequest { operation: id, target: Some(target) })
                    {
                        return Err(DbError::OperationChanged(id));
                    }
                    crate::provider_access::confirm(db, &account, target, invitation.as_ref())?;
                    if state.intended == target {
                        finish(db, &account, id)?;
                        Ok(true)
                    } else {
                        crate::provider_access::requeue(db, &account, id)?;
                        crate::pending::save(db, &P::provider_access(id, account, state.intended))?;
                        Ok(false)
                    }
                })
            })
        })
        .await
    }
}

#[cfg(test)]
#[path = "database_provider_access_tests.rs"]
mod tests;
