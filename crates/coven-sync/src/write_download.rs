//! Stream the shared authenticated object reader into one database transaction.

use super::DeviceLogSync;
use crate::stream_input::{ChannelParts, WithReferences};
use crate::{replay_cache::ReplayCache, write_object::ReadyWrite, SyncError};
use coven_crypto::{MemberId, StoreKeyring};
use coven_database::{ApplyOutcome, DbError, StoreLog};
use coven_storage::StoredObject;
use tokio::sync::oneshot;

impl DeviceLogSync {
    pub(super) async fn receive_write(
        &self,
        object: &StoredObject,
        ring: &StoreKeyring,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        member: &MemberId,
    ) -> Result<ApplyOutcome, SyncError> {
        let retained = crate::object_input::ObjectInput::observe(&self.database, object).await?;
        let ReadyWrite {
            opened,
            author,
            parts: opens,
        } = match crate::write_object::download(
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            object,
            Some(ring),
            &retained,
            log,
            replays,
            || Ok(member.clone()),
        )
        .await?
        {
            Ok(ready) => ready,
            Err(reason) => return Ok(ApplyOutcome::Waiting(reason)),
        };
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
            let result = crate::write_object::finish(
                self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                object,
                &retained,
                ring,
                &author,
                opened,
                &mut sink,
            )
            .await;
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
        if matches!(
            applied,
            ApplyOutcome::Applied | ApplyOutcome::AlreadyApplied
        ) {
            retained.release_ranges().await?;
        }
        Ok(applied)
    }
}
