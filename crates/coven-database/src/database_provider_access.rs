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
        self.call(move |inner| inner.access.with_writer(|db| read(db, &account)))
            .await
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
                    adopt::<P>(db, inner.access.device, &changes)?;
                    Ok(id)
                })
            })
        })
        .await
    }

    /// Advance an invitation and its account intention atomically.
    pub async fn advance_operation_with_access<P: PendingValue>(
        &self,
        update: crate::OperationUpdate,
        changes: Vec<ProviderAccessChange>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    crate::operation::advance(db, &update)?;
                    adopt::<P>(db, inner.access.device, &changes)
                })
            })
        })
        .await
    }

    /// Retire an invitation with its final account intention in one commit.
    pub async fn finish_operation_with_access<P: PendingValue>(
        &self,
        id: OperationId,
        changes: Vec<ProviderAccessChange>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    if crate::operation::find(db, id)?.is_none() {
                        return Err(DbError::OperationChanged(id));
                    }
                    crate::pending::clear_operation(db, id)?;
                    db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id.0])?;
                    adopt::<P>(db, inner.access.device, &changes)
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
        self.call(move |inner| inner.access.with_writer(|db| db.transaction(|db| {
            let state = read(db,&account)?.ok_or(DbError::OperationChanged(id))?;
            if state.request.as_ref().map(|r|r.operation) != Some(id) { return Err(DbError::OperationChanged(id)); }
            let record = crate::operation::find(db,id)?.ok_or(DbError::OperationChanged(id))?;
            if record.last_step == 1 { return Err(DbError::OperationChanged(id)); }
            // A revoke may remove the direct share supplying acceptance tokens
            // even when other permissions remain. Retain neither confirmation.
            db.internal_execute("UPDATE _coven_provider_access SET granted=NULL,invitation=NULL,target=intended WHERE account=?1", [&account])?;
            db.internal_execute("UPDATE _coven_operations SET last_step=1 WHERE id=?1", [id.0])?;
            crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
            crate::pending::save(db,&P::provider_access(id,inner.access.device,account,state.intended,true))?;
            Ok(state.intended)
        }))).await
    }

    /// Keep an inconclusive request with its notice and unknown confirmation.
    pub async fn fail_provider_request<P: PendingValue>(
        &self,
        account: String,
        update: crate::OperationUpdate,
        pending: P,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    let state = read(db, &account)?.ok_or(DbError::OperationChanged(update.id))?;
                    if state.request.as_ref().map(|r| r.operation) != Some(update.id)
                        || pending.operation() != Some(update.id)
                        || pending.reported_by() != inner.access.device
                    {
                        return Err(DbError::DamagedDatabase);
                    }
                    crate::operation::advance(db, &update)?;
                    crate::pending::save(db, &pending)
                })
            })
        })
        .await
    }

    /// Rearm a failed request without hiding its pending subject. A request
    /// interrupted in flight retains its fixed target for idempotent repetition.
    pub async fn retry_provider_request<P: PendingValue>(
        &self,
        account: String,
        id: OperationId,
        owner: bool,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|db| {
                db.transaction(|db| {
                    let state = read(db, &account)?.ok_or(DbError::OperationChanged(id))?;
                    let request = state.request.ok_or(DbError::OperationChanged(id))?;
                    if request.operation != id {
                        return Err(DbError::OperationChanged(id));
                    }
                    let record =
                        crate::operation::find(db, id)?.ok_or(DbError::OperationChanged(id))?;
                    let target = if record.last_step == 1 {
                        request.target.ok_or(DbError::DamagedDatabase)?
                    } else {
                        db.internal_execute(
                            "UPDATE _coven_operations SET last_step=0 WHERE id=?1",
                            [id.0],
                        )?;
                        crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
                        db.internal_execute(
                            "UPDATE _coven_provider_access SET target=NULL WHERE account=?1",
                            [&account],
                        )?;
                        state.intended
                    };
                    // An app retry overrides the ordinary backoff (§16.5).
                    crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
                    crate::pending::save(
                        db,
                        &P::provider_access(id, inner.access.device, account, target, owner),
                    )
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
        self.call(move |inner| inner.access.with_writer(|db| db.transaction(|db| {
            let state = read(db,&account)?.ok_or(DbError::OperationChanged(id))?;
            if state.request != Some(ProviderAccessRequest {operation:id,target:Some(target)}) { return Err(DbError::OperationChanged(id)); }
            db.internal_execute("UPDATE _coven_provider_access SET granted=?1,invitation=?2,target=NULL WHERE account=?3", (target,invitation.as_ref().map(SecretBytes::as_bytes),&account))?;
            if state.intended == target {
                finish(db,&account,id)?;
                Ok(true)
            } else {
                db.internal_execute("UPDATE _coven_operations SET last_step=0 WHERE id=?1", [id.0])?;
                crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
                crate::pending::save(db,&P::provider_access(id,inner.access.device,account,state.intended,true))?;
                Ok(false)
            }
        }))).await
    }
}

#[cfg(test)]
#[path = "database_provider_access_tests.rs"]
mod tests;
