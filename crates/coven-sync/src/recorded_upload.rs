//! Publication of repeatable file, snapshot, claim, and floor bytes: in one
//! request or through a recorded session, as storage decides.

use coven_storage::{
    ObjectPath, Storage, StorageError, StorageFailure, StoredObject, UploadSession, UploadTarget,
};

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
    /// Who hears each part's bytes as they are handed over; no one, unless
    /// the source shows its upload as it goes.
    fn sent(&self) -> coven_storage::ByteProgress {
        coven_storage::ByteProgress::unread()
    }
}

/// How an upload ended for this invocation.
#[derive(Debug)]
pub(crate) enum Uploaded {
    /// The provider confirmed these bytes at the path, with the object's
    /// metadata when a one-request reply carried it (§21.1).
    Stored(Option<StoredObject>),
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
    !target.path().is_replaceable() && error.own_failure() == Some(StorageFailure::AlreadyExists)
}

/// Create the source's bytes at `path`. An object storage sends in one
/// request, with no session recorded for it, is read whole and created;
/// any other goes through the recorded session (§17.9, §20.6).
pub(crate) async fn create<S: UploadSource>(
    storage: &dyn Storage,
    path: &ObjectPath,
    total: u64,
    recorded: Option<UploadSession>,
    source: &mut S,
) -> Result<Uploaded, S::Error> {
    let target = UploadTarget::Create(path.clone());
    if recorded.is_some() || coven_storage::goes_in_parts(storage, total) {
        return upload(storage, &target, total, recorded, source).await;
    }
    let bytes = source.read(0, total as usize).await?;
    match storage.create_reporting(path, &bytes, &source.sent()).await {
        Ok(object) => Ok(Uploaded::Stored(object)),
        Err(error) if occupied(&target, &error) => {
            Ok(Uploaded::Occupied(crate::identity::digest(&bytes)))
        }
        Err(error) => Err(error.into()),
    }
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
            // A store never changes location, so the connection refuses a
            // session naming another one before any request.
            if let Err(error) = storage.resume_upload(&mut session).await {
                let error = save_failure(source, session.clone(), error).await?;
                if occupied(target, &error) {
                    return source_digest(source, total).await;
                }
                if error.own_failure() != Some(StorageFailure::SessionExpired) {
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
    while let Some(next) = session.parts_to_send().first().cloned() {
        if !source.keep_going() {
            return Ok(Uploaded::Paused);
        }
        let part = source.read(next.start, (next.end - next.start) as usize).await?;
        // A refusal before any request leaves the recording as it was, a
        // plan unmarked. The mark admitting a plan's first part is saved
        // before the part can start the upload (Appendix E.10).
        let permit = storage.admit_part(&mut session, &part)?;
        if permit.marked() {
            source.save(session.clone()).await?;
        }
        if let Err(error) = storage.send_part(permit, &mut session, &source.sent()).await {
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
    Ok(Uploaded::Stored(None))
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
