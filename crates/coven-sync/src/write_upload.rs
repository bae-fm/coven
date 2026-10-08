//! Ordered publication, re-sealing the waiting plaintext with its recorded keys.

use super::DeviceLogSync;
use crate::SyncError;
use coven_database::{DbError, UploadReadError};
use coven_format::chunks::CHUNK_SIZE;
use coven_merge::WriteId;
use coven_storage::{StorageFailure, UploadSession};
use tokio::sync::mpsc;

impl DeviceLogSync {
    pub(super) async fn send_write(
        &self,
        write: WriteId,
        ring: coven_crypto::StoreKeyring,
        member: coven_crypto::MemberKeys,
    ) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let database = &self.database;
        let path = crate::write_seal::path(write);
        let total = database
            .read_oldest_upload(|upload| {
                coven_format::sealed_write::sealed_length(
                    upload.header_frame.len(),
                    &upload
                        .header
                        .parts
                        .iter()
                        .map(|p| p.plaintext_length)
                        .collect::<Vec<_>>(),
                )
                .map_err(SyncError::from)
            })
            .await
            .map_err(read_error)?
            .ok_or(DbError::UploadNotOldest { write })?;
        let mut session = match database.write_upload_session(write).await? {
            Some(bytes) => {
                let mut session = UploadSession::decode(&bytes)?;
                if session.path() != &path || session.total_bytes() != total {
                    return Err(StorageFailure::SessionMismatch.into());
                }
                match storage.resume_upload(&mut session).await {
                    Ok(()) => (),
                    Err(error) if error.failure() == StorageFailure::AlreadyExists => return Ok(()),
                    Err(error) if error.failure() == StorageFailure::SessionExpired => {
                        session = match storage.restart_upload(&session).await {
                            Ok(session) => session,
                            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                                return Ok(())
                            }
                            Err(error) => return Err(error.into()),
                        };
                    }
                    Err(error) => return Err(error.into()),
                }
                Some(session)
            }
            None if total <= CHUNK_SIZE as u64 && total <= storage.single_request_limit() => None,
            None => match storage.begin_upload(&path, total).await {
                Ok(session) => Some(session),
                Err(error) if error.failure() == StorageFailure::AlreadyExists => return Ok(()),
                Err(error) => return Err(error.into()),
            },
        };
        if let Some(session) = &session {
            database
                .keep_write_upload_session(write, session.encode()?.as_bytes().to_vec())
                .await?;
            if session.is_complete() {
                return Ok(());
            }
        }
        let mut skip = session.as_ref().map_or(0, UploadSession::confirmed_bytes);
        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(1);
        let producer = database.read_oldest_upload(move |upload| {
            if upload.header.header.position != write {
                return Err(DbError::UploadNotOldest { write }.into());
            }
            crate::write_seal::seal(upload, &ring, &member, &mut |piece| {
                let skipped = skip.min(piece.len() as u64) as usize;
                skip -= skipped as u64;
                if skipped < piece.len() {
                    sender
                        .blocking_send(piece[skipped..].to_vec())
                        .map_err(|error| {
                            SyncError::Database(DbError::SyncStream(Box::new(error)))
                        })?;
                }
                Ok(())
            })
        });
        let consumer = async move {
            if let Some(session) = &mut session {
                let mut part = Vec::with_capacity(session.part_size());
                while let Some(piece) = receiver.recv().await {
                    let mut remaining = piece.as_slice();
                    while !remaining.is_empty() {
                        let boundary = session.part_size()
                            - (session.confirmed_bytes() % session.part_size() as u64) as usize;
                        let length = remaining.len().min(boundary - part.len());
                        part.extend_from_slice(&remaining[..length]);
                        remaining = &remaining[length..];
                        if part.len() == boundary
                            || session.confirmed_bytes() + part.len() as u64 == total
                        {
                            let previous = session.confirmed_bytes();
                            storage.upload_part(session, &part).await?;
                            if session.confirmed_bytes() <= previous {
                                return Err(StorageFailure::Protocol
                                    .with_source("write upload did not advance")
                                    .into());
                            }
                            database
                                .keep_write_upload_session(
                                    write,
                                    session.encode()?.as_bytes().to_vec(),
                                )
                                .await?;
                            part.clear();
                        }
                    }
                }
                if session.confirmed_bytes() != total || !part.is_empty() {
                    return Err(StorageFailure::InvalidPart.into());
                }
                storage.finish_upload(session).await?;
            } else {
                let mut bytes = Vec::with_capacity(total as usize);
                while let Some(piece) = receiver.recv().await {
                    bytes.extend(piece);
                }
                if bytes.len() as u64 != total {
                    return Err(StorageFailure::InvalidPart.into());
                }
                storage.create_once(&path, &bytes).await?;
            }
            Ok::<_, SyncError>(())
        };
        let (produced, sent) = tokio::join!(producer, consumer);
        match produced {
            // A dropped receiver means transport failed; return that cause below.
            Err(UploadReadError::Consumer(SyncError::Database(DbError::SyncStream(error))))
                if error.is::<mpsc::error::SendError<Vec<u8>>>() => {}
            result => {
                result.map_err(read_error)?;
            }
        }
        match sent {
            Err(SyncError::Storage(error)) if error.failure() == StorageFailure::AlreadyExists => {
                Ok(())
            }
            result => result,
        }
    }
}

fn read_error(error: UploadReadError<SyncError>) -> SyncError {
    match error {
        UploadReadError::Database(e) => e.into(),
        UploadReadError::Consumer(e) => e,
    }
}
