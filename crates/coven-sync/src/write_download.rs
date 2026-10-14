//! Stream the shared authenticated object reader into one database transaction.

use super::DeviceLogSync;
use crate::stream_input::{ChannelParts, WithReferences};
use crate::write_object::{ReadyWrite, Unready};
use crate::{replay_cache::ReplayCache, PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_crypto::{MemberId, StoreKeyring};
use coven_database::{ApplyOutcome, DbError, StoreLog};
use coven_foundation::id_source::DeviceId;
use coven_storage::StoredObject;
use tokio::sync::oneshot;

impl DeviceLogSync {
    /// Download, check and apply one write, routing its outcome to the write
    /// itself (§19.1): a wait or required update becomes its pending record, a
    /// failed check of its complete bytes its permanent refusal. Neither fails
    /// the caller; transport and local failures do, leaving no judgment.
    pub(super) async fn receive_write(
        &self,
        object: &StoredObject,
        ring: &StoreKeyring,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        member: &MemberId,
        reporter: DeviceId,
    ) -> Result<ApplyOutcome, SyncError> {
        let retained = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let write = object.path.write_id().expect("write path");
        let waits = |reason| PendingRecord {
            subject: PendingSubject::Write(write),
            reason,
            reported_by: reporter,
        };
        let unready = match self
            .apply_write(&retained, ring, log, replays, member)
            .await
        {
            Ok(Ok(applied)) => return Ok(applied),
            Ok(Err(unready)) => unready,
            Err(SyncError::UpdateRequired(update)) => {
                let reason = PendingReason::UpdateRequired(update);
                self.database.record_pending(waits(reason)).await?;
                return Err(SyncError::UpdateRequired(update));
            }
            Err(SyncError::Damaged(damage)) => {
                crate::write_object::refuse(&self.database, storage, retained, &damage.failure)
                    .await?;
                return Err(SyncError::Damaged(damage));
            }
            Err(error) => return Err(error),
        };
        match unready {
            Unready::History(wait) => {
                let reason = crate::pending_logs::write_wait(object.path.store(), &wait)?;
                self.database.record_pending(waits(reason)).await?;
                Ok(ApplyOutcome::Waiting(wait))
            }
            Unready::Key { audience, key } => {
                let reason = PendingReason::KeyUnavailable { audience, key };
                self.database.record_pending(waits(reason)).await?;
                Err(SyncError::KeyUnavailable(key))
            }
        }
    }

    /// The write's own outcome: applied, or its first unmet condition, before
    /// that is routed to its subject. A database wait is the same condition.
    async fn apply_write(
        &self,
        retained: &crate::object_input::ObjectInput,
        ring: &StoreKeyring,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        member: &MemberId,
    ) -> Result<Result<ApplyOutcome, Unready>, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let object = retained.object();
        let mut stream = retained.stream(storage).await;
        let ReadyWrite {
            opened,
            author,
            parts: opens,
        } = match crate::write_object::download(&mut stream, Some(ring), log, replays, || {
            Ok(member.clone())
        })
        .await?
        {
            Ok(ready) => ready,
            Err(unready) => return Ok(Err(unready)),
        };
        // The apply below holds the database writer; it reads only local input.
        stream.retain_rest().await?;
        let (primary, download) = ChannelParts::new(opened.header.clone(), &opens);
        let (references, input) = ChannelParts::new(opened.header.clone(), &opens);
        let (validation, validated) = oneshot::channel();
        let apply = self
            .database
            .apply_downloaded_stream(download, log.positions(), move || {
                validated
                    .blocking_recv()
                    .map_err(|e| DbError::SyncStream(Box::new(e)))
            });
        let download = async {
            let mut sink = WithReferences {
                primary,
                references,
            };
            let result =
                crate::write_object::finish(&mut stream, ring, &author, opened, &mut sink).await;
            drop(sink);
            result
        };
        let transfer = async {
            let (downloaded, references) =
                tokio::join!(download, self.database.write_file_references(input));
            let result = downloaded.and_then(|()| references.map_err(SyncError::from));
            // Both authentication and reference validation finish before rows
            // can commit. A failed transfer drops the sender, so the apply rolls
            // back and its error is set aside below for the transfer's own
            // typed failure. An early database refusal needs no final check.
            if result.is_ok() {
                let _ = validation.send(());
            }
            result
        };
        let (applied, transferred) = tokio::join!(apply, transfer);
        drop(stream);
        let applied = match transferred {
            Ok(references) => {
                retained.keep_files(references).await?;
                applied
            }
            Err(SyncError::Database(error)) => Err(error),
            Err(error) => return Err(error),
        };
        let applied =
            applied.map_err(|error| crate::write_object::database_failure(&object.path, error))?;
        match applied {
            ApplyOutcome::Waiting(wait) => Ok(Err(Unready::History(wait))),
            applied => {
                retained.clone().release_ranges().await?;
                Ok(Ok(applied))
            }
        }
    }
}
