//! Owned attempts and durable retries for S3 replacements (§11.4, §14.3).
use super::*;
use crate::sequence_sync::sequence_invites::INVITE;

impl SequenceSync {
    /// Resume work waiting with `AdminAccessRequired`, holding this admin
    /// access for that operation only (§20.10).
    pub async fn retry_with_admin_access(
        self: &Arc<Self>,
        id: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        let owner = self.clone();
        tokio::spawn(async move { owner.resume_with_admin_access(id, admin).await })
            .await
            .map_err(|error| SyncError::disk(std::io::Error::other(error)))?
    }

    async fn resume_with_admin_access(
        &self,
        id: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let record = self
            .database
            .operation(id)
            .await?
            .ok_or(SyncError::NotPending(id))?;
        if !matches!(record.kind.as_str(), REPLACE_ACCESS_KEY | INVITE) {
            return Err(SyncError::NotPending(id));
        }
        self.admin_access
            .lock()
            .expect("admin access poisoned")
            .insert(id, admin);
        let identity = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &identity).await?;
        self.read_forward(&mut session, &mut ring, &identity)
            .await?;
        self.settle_open(&mut session, &mut ring, &identity).await?;
        match record.kind.as_str() {
            REPLACE_ACCESS_KEY => self
                .run_key_replacement(&mut session, &mut ring, &identity, id)
                .await?
                .map(drop),
            INVITE => {
                self.retry_invite(&mut session, &mut ring, &identity, id)
                    .await
            }
            _ => Err(SyncError::NotPending(id)),
        }
    }

    /// The parent owns its delivery, claim and undo under one retry wait (§11.4).
    pub(crate) async fn resume_key_replacements(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        identity: &MemberIdentity,
    ) -> Result<(), SyncError> {
        for record in self.database.operations().await? {
            if record.kind != REPLACE_ACCESS_KEY || !self.operation_ready(&record, ring).await? {
                continue;
            }
            if self
                .run_key_replacement(session, ring, identity, record.id)
                .await?
                .is_err()
            {
                // The pending list reports an unfinished failure. A completed
                // rejection has undone its work and ends only this operation.
                self.state.check()?;
            }
        }
        Ok(())
    }

    /// The outer error means recording the outcome failed; passes must surface
    /// it. The inner result is the operation's reported, durable outcome (§11.3).
    pub(super) async fn run_key_replacement(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        identity: &MemberIdentity,
        id: OperationId,
    ) -> Result<Result<AccessKeyReplacement, SyncError>, SyncError> {
        let scope = self.waits.begin_attempt(id);
        let result = scope
            .run(self.advance_key_replacement(session, ring, identity, id))
            .await;
        self.finish_key_replacement(id, result, ring).await
    }

    async fn finish_key_replacement(
        &self,
        id: OperationId,
        result: Result<KeyProgress, SyncError>,
        ring: &StoreKeyring,
    ) -> Result<Result<AccessKeyReplacement, SyncError>, SyncError> {
        let error = match result {
            Ok(KeyProgress::Done(replacement)) => {
                self.admin_access
                    .lock()
                    .expect("admin access poisoned")
                    .remove(&id);
                self.waits.clear_evidence(id);
                self.waits
                    .forget(coven_database::RetrySubject::Operation(id));
                return Ok(Ok(replacement));
            }
            Ok(KeyProgress::Waiting(reason)) => SyncError::Pending(Box::new(PendingRecord {
                subject: PendingSubject::Operation {
                    id,
                    kind: OperationKind::ReplaceAccessKey,
                },
                reason,
            })),
            Err(error) => error,
        };
        let recorded = async {
            if self.database.operation(id).await?.is_some() {
                self.record_operation_error(id, OperationKind::ReplaceAccessKey, &error, ring)
                    .await?;
            } else {
                self.admin_access
                    .lock()
                    .expect("admin access poisoned")
                    .remove(&id);
                self.waits.clear_evidence(id);
                self.waits
                    .forget(coven_database::RetrySubject::Operation(id));
            }
            Ok::<_, SyncError>(())
        }
        .await;
        if let Err(cleanup) = recorded {
            return Err(SyncError::Cleanup {
                operation: Box::new(error),
                cleanup: Box::new(cleanup),
            });
        }
        Ok(Err(error))
    }
}

#[cfg(test)]
#[path = "sequence_access_keys_retry_tests.rs"]
mod tests;
