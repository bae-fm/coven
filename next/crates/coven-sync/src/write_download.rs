//! Stream the shared authenticated object reader into one database transaction.

use super::DeviceLogSync;
use crate::{
    replay_cache::ReplayCache,
    write_object::{authority, damaged},
    ObjectCheckFailure, SyncError,
};
use coven_crypto::{MemberId, StoreKeyring};
use coven_database::{
    ApplyOutcome, DbError, DownloadedPartStream, DownloadedWriteStream, StoreLog, WriteWait,
};
use coven_storage::StoredObject;
use std::{
    io::{self, Read},
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};

struct PartInput {
    receiver: mpsc::Receiver<Vec<u8>>,
    chunk: io::Cursor<Vec<u8>>,
}

impl Read for PartInput {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.chunk.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            match self.receiver.blocking_recv() {
                Some(bytes) => self.chunk = io::Cursor::new(bytes),
                None => return Ok(0),
            }
        }
    }
}

impl DeviceLogSync {
    pub(super) async fn receive_write(
        &self,
        object: &StoredObject,
        ring: &StoreKeyring,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        member: &MemberId,
    ) -> Result<ApplyOutcome, SyncError> {
        let opened = crate::write_object::open(self.storage.as_ref(), object, Some(ring)).await?;
        let header = &opened.header.header;
        let missing: Vec<_> = header
            .store_log_read
            .0
            .iter()
            .copied()
            .filter(|id| !log.replay.entries.contains_key(id))
            .collect();
        if !missing.is_empty() {
            return Ok(ApplyOutcome::Waiting(WriteWait::StoreLog(missing)));
        }
        let author = authority(log, replays, header).map_err(|e| damaged(&object.path, e))?;
        let mut senders = Vec::new();
        let mut parts = Vec::new();
        for opened in crate::write_object::parts(&opened, ring, log, replays, member)? {
            if opened {
                let (sender, receiver) = mpsc::channel(1);
                senders.push(Some(sender));
                parts.push(DownloadedPartStream::Opened(PartInput {
                    receiver,
                    chunk: io::Cursor::new(Vec::new()),
                }));
            } else {
                senders.push(None);
                parts.push(DownloadedPartStream::Skipped);
            }
        }
        let download = DownloadedWriteStream {
            header: opened.header.clone(),
            parts,
        };
        let (validation, validated) = oneshot::channel();
        let apply = self
            .database
            .apply_downloaded_stream(download, log.positions(), move || {
                validated
                    .blocking_recv()
                    .map_err(|e| DbError::SyncStream(Box::new(e)))?
            });
        let transfer = async {
            let result = crate::write_object::finish(
                self.storage.as_ref(),
                object,
                ring,
                &author,
                opened,
                &mut ChannelParts(senders),
            )
            .await;
            let checked = match &result {
                Ok(()) => Ok(()),
                Err(error) => Err(DbError::SyncStream(Box::new(io::Error::other(
                    error.to_string(),
                )))),
            };
            let _ = validation.send(checked); // An early database refusal needs no final check.
            result
        };
        let (applied, transferred) = tokio::join!(apply, transfer);
        transferred?;
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

struct ChannelParts(Vec<Option<mpsc::Sender<Vec<u8>>>>);
impl crate::write_object::PartSink for ChannelParts {
    fn opens(&self, part: usize) -> bool {
        self.0[part].is_some()
    }
    async fn chunk(&mut self, part: usize, bytes: Vec<u8>) -> Result<(), SyncError> {
        // A waiting or refused transaction closes its receiver. Its awaited
        // result retains that outcome; the reader still authenticates the body.
        let _ = self.0[part]
            .as_ref()
            .expect("opened part")
            .send(bytes)
            .await;
        Ok(())
    }
    async fn end(&mut self, part: usize) -> Result<(), SyncError> {
        self.0[part] = None;
        Ok(())
    }
}
