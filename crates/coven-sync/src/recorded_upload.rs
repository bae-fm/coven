//! Resumable publication of repeatable file, snapshot, claim, and floor bytes.

use coven_storage::{Storage, StorageError, StorageFailure, UploadSession, UploadTarget};

/// Repeatable byte reads, durable session progress, and a caller's pause state.
pub(crate) trait UploadSource: Send {
    type Error: From<StorageError> + std::error::Error + Send + Sync + 'static;
    fn read(
        &mut self,
        offset: u64,
        length: usize,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send;
    fn save(
        &mut self,
        session: UploadSession,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    fn keep_going(&self) -> bool;
}

/// How a recorded upload ended for this invocation.
#[derive(Debug)]
pub(crate) enum Uploaded {
    /// The provider confirmed these bytes at the path.
    Stored,
    /// A paused session remains intact for the next invocation.
    Paused,
    /// The path was already occupied. The caller compares the occupant with
    /// this length and digest of the source bytes before counting it (§19.3).
    Occupied((u64, coven_crypto::ObjectDigest)),
}

/// The digest of the complete repeatable source, read one part at a time.
async fn source_digest<S: UploadSource>(source: &mut S, total: u64) -> Result<Uploaded, S::Error> {
    let mut hasher = coven_crypto::ObjectHasher::new();
    let mut offset = 0;
    while offset < total {
        let length = (total - offset).min(1024 * 1024) as usize;
        let bytes = source.read(offset, length).await?;
        if bytes.len() != length {
            return Err(StorageError::from(StorageFailure::InvalidPart).into());
        }
        hasher.update(&bytes);
        offset += length as u64;
    }
    Ok(Uploaded::Occupied((total, hasher.finish())))
}

fn occupied(target: &UploadTarget, error: &StorageError) -> bool {
    !target.path().is_replaceable()
        && error.failure() == StorageFailure::AlreadyExists
        && !matches!(error, StorageError::Cleanup { .. })
}

/// Persist every confirmed session before continuing; read only its next part.
pub(crate) async fn upload<S: UploadSource>(
    storage: &dyn Storage,
    target: &UploadTarget,
    total: u64,
    recorded: Option<UploadSession>,
    source: &mut S,
) -> Result<Uploaded, S::Error> {
    let mut session = match recorded {
        Some(mut session) => {
            if session.target() != target || session.total_bytes() != total {
                return Err(StorageError::from(StorageFailure::SessionMismatch).into());
            }
            if session.is_at(&storage.config()) {
                match storage.resume_upload(&mut session).await {
                    Ok(()) => (),
                    Err(error) => {
                        let error = save_failure(source, session.clone(), error).await?;
                        if occupied(target, &error) {
                            return source_digest(source, total).await;
                        }
                        if error.failure() != StorageFailure::SessionExpired
                            || matches!(error, StorageError::Cleanup { .. })
                        {
                            return Err(error.into());
                        }
                        session = match storage.restart_upload(&session).await {
                            Ok(session) => session,
                            Err(error) if occupied(target, &error) => {
                                return source_digest(source, total).await
                            }
                            Err(error) => return Err(error.into()),
                        };
                    }
                }
            } else {
                if matches!(target, UploadTarget::Replace(_)) {
                    return Err(StorageError::from(StorageFailure::SessionMismatch).into());
                }
                // A location move copies published objects, but provider sessions
                // belong to the old location. Send the same bytes in a new one.
                match begin(storage, target, total).await {
                    Ok(replacement) => session = replacement,
                    Err(error) if occupied(target, &error) => {
                        return source_digest(source, total).await
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            session
        }
        None => match begin(storage, target, total).await {
            Ok(session) => session,
            Err(error) if occupied(target, &error) => return source_digest(source, total).await,
            Err(error) => return Err(error.into()),
        },
    };
    source.save(session.clone()).await?;
    if session.needs_initiation() {
        if let Err(error) = storage.resume_upload(&mut session).await {
            return Err(save_failure(source, session, error).await?.into());
        }
        source.save(session.clone()).await?;
    }
    while !session.is_complete() && session.confirmed_bytes() < total {
        if !source.keep_going() {
            return Ok(Uploaded::Paused);
        }
        let offset = session.confirmed_bytes();
        let length = (session.part_size() as u64 - offset % session.part_size() as u64)
            .min(total - offset) as usize;
        if let Err(error) = storage
            .upload_part(&mut session, &source.read(offset, length).await?)
            .await
        {
            return Err(save_failure(source, session, error).await?.into());
        }
        source.save(session.clone()).await?;
    }
    if !source.keep_going() {
        return Ok(Uploaded::Paused);
    }
    if let Err(error) = storage.finish_upload(&mut session).await {
        let error = save_failure(source, session, error).await?;
        if occupied(target, &error) {
            return source_digest(source, total).await;
        }
        return Err(error.into());
    }
    source.save(session).await?;
    Ok(Uploaded::Stored)
}

async fn begin(
    storage: &dyn Storage,
    target: &UploadTarget,
    total: u64,
) -> Result<UploadSession, StorageError> {
    match target {
        UploadTarget::Create(path) => storage.begin_upload(path, total).await,
        UploadTarget::Replace(previous) => storage.begin_replace(previous, total).await,
    }
}

async fn save_failure<S: UploadSource>(
    source: &mut S,
    session: UploadSession,
    operation: StorageError,
) -> Result<StorageError, S::Error> {
    match source.save(session).await {
        Ok(()) => Ok(operation),
        Err(error) => Err(StorageFailure::File
            .with_source(StorageError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(StorageFailure::File.with_source(error)),
            })
            .into()),
    }
}

#[cfg(test)]
#[path = "recorded_upload_tests.rs"]
mod tests;
