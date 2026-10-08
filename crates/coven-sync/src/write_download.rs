//! Stream the shared authenticated object reader into one database transaction.

use super::DeviceLogSync;
use crate::stream_input::{ChannelParts, WithReferences};
use crate::{
    replay_cache::ReplayCache,
    write_object::{damaged, ReadyWrite},
    ObjectCheckFailure, SyncError,
};
use coven_crypto::{MemberId, StoreKeyring};
use coven_database::{ApplyOutcome, DbError, StoreLog};
use coven_storage::StoredObject;
use std::{io, sync::Arc};
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
        let ReadyWrite {
            opened,
            author,
            parts: opens,
        } = match crate::write_object::download(
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            object,
            Some(ring),
            &self.reads,
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
                    .map_err(|e| DbError::SyncStream(Box::new(e)))?
            });
        let download = async {
            let mut sink = WithReferences {
                primary,
                references,
            };
            let result = crate::write_object::finish(
                self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                object,
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
            let checked = match &result {
                Ok(_) => Ok(()),
                Err(error) => Err(DbError::SyncStream(Box::new(io::Error::other(
                    error.to_string(),
                )))),
            };
            // Both authentication and reference validation finish before rows
            // can commit. An early database refusal needs no final check.
            let _ = validation.send(checked);
            result
        };
        let (applied, transferred) = tokio::join!(apply, transfer);
        let applied = match transferred {
            Ok(references) => {
                self.reads.keep_files(object, references);
                applied
            }
            Err(SyncError::Database(error)) => Err(error),
            Err(error) => return Err(error),
        };
        match applied {
            Ok(result) => Ok(result),
            Err(DbError::WriteFormat(coven_format::Error::UnsupportedVersion(version)))
                if version > coven_format::FORMAT_VERSION =>
            {
                Err(crate::SyncFailure::UpdateRequired.into())
            }
            Err(
                error @ (DbError::InvalidWrite { .. }
                | DbError::WriteFormat(_)
                | DbError::TooLarge { .. }
                | DbError::Snapshot(_)
                | DbError::Schema(_)),
            ) => Err(damaged(
                &object.path,
                ObjectCheckFailure::Parse(Arc::new(error)),
            )),
            Err(error) => Err(error.into()),
        }
    }
}
