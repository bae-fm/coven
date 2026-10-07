//! Resumable publication of fixed disk bytes shared by files and snapshots.

use coven_storage::{ObjectPath, Storage, StorageError, UploadSession};

/// Fixed byte reads, durable session progress, and a caller's pause state.
pub(crate) trait UploadSource: Send {
    type Error: From<StorageError>;
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

/// Persist every confirmed session before continuing; read only its next part.
/// Returning false leaves a paused session intact for the next invocation.
pub(crate) async fn upload<S: UploadSource>(
    storage: &dyn Storage,
    path: &ObjectPath,
    total: u64,
    recorded: Option<UploadSession>,
    source: &mut S,
) -> Result<bool, S::Error> {
    let mut session = match recorded {
        Some(mut session) => {
            if session.path() != path || session.total_bytes() != total {
                return Err(StorageError::SessionMismatch.into());
            }
            if session.is_at(&storage.config()) {
                match storage.resume_upload(&mut session).await {
                    Ok(()) => (),
                    Err(StorageError::SessionExpired) => {
                        session = storage.restart_upload(&session).await?;
                    }
                    Err(error) => return Err(error.into()),
                }
            } else {
                // A location move copies published objects, but provider sessions
                // belong to the old location. Reuse the fixed bytes in a new one.
                match storage.begin_upload(path, total).await {
                    Ok(replacement) => session = replacement,
                    Err(StorageError::AlreadyExists) => return Ok(true),
                    Err(error) => return Err(error.into()),
                }
            }
            session
        }
        None => storage.begin_upload(path, total).await?,
    };
    source.save(session.clone()).await?;
    while !session.is_complete() && session.confirmed_bytes() < total {
        if !source.keep_going() {
            return Ok(false);
        }
        let offset = session.confirmed_bytes();
        let length = (session.part_size() as u64 - offset % session.part_size() as u64)
            .min(total - offset) as usize;
        storage
            .upload_part(&mut session, &source.read(offset, length).await?)
            .await?;
        source.save(session.clone()).await?;
    }
    if !source.keep_going() {
        return Ok(false);
    }
    storage.finish_upload(&mut session).await?;
    source.save(session).await?;
    Ok(true)
}
