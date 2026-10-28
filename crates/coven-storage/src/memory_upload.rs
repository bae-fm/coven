//! Atomic conditional publication in the deterministic provider.
use super::*;

pub(super) struct Pending {
    target: UploadTarget,
    total: u64,
    pub(super) bytes: Vec<u8>,
    part_size: usize,
}
impl Pending {
    fn check(&self, session: &UploadSession) -> Result<(), StorageError> {
        if self.target != session.target
            || self.total != session.total
            || self.part_size != session.part_size
        {
            return Err(StorageFailure::SessionMismatch.into());
        }
        if (self.bytes.len() as u64) < session.confirmed || self.bytes.len() as u64 > self.total {
            return Err(
                StorageFailure::Protocol.with_source("memory provider lost confirmed progress")
            );
        }
        Ok(())
    }
}
fn confirm_published(
    state: &State,
    session: &mut UploadSession,
    id: u64,
) -> Result<(), StorageError> {
    let object = state
        .objects
        .get(session.path())
        .ok_or(StorageFailure::SessionExpired)?;
    if object.upload_id != Some(id) || object.bytes.len() as u64 != session.total {
        return Err(StorageFailure::AlreadyExists.into());
    }
    session.confirmed = session.total;
    session.state = SessionState::Memory { id, complete: true };
    Ok(())
}

fn check_target(state: &State, target: &UploadTarget) -> Result<(), StorageError> {
    match target {
        UploadTarget::Create(path) => {
            if state.objects.contains_key(path) && !state.faults.overwrite_create {
                return Err(StorageFailure::AlreadyExists.into());
            }
        }
        UploadTarget::Replace(previous) => {
            if state
                .objects
                .get(&previous.path)
                .is_none_or(|object| object.revision.to_string() != previous.replace_token)
            {
                return Err(StorageFailure::Conflict.into());
            }
        }
    }
    Ok(())
}

impl MemoryProvider {
    pub(super) async fn memory_begin_upload(
        &self,
        publication: &PublicationRequest,
        target: &UploadTarget,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        publication.check()?;
        self.before(MemoryRequest::Other("begin_upload")).await?;
        let mut state = self.state.lock().await;
        check_target(&state, target)?;
        let id = state.next_id()?;
        state.uploads.insert(
            id,
            Pending {
                target: target.clone(),
                total,
                bytes: Vec::new(),
                part_size: self.part_size,
            },
        );
        Ok(UploadSession {
            location: self.config(),
            target: target.clone(),
            total,
            confirmed: 0,
            part_size: self.part_size,
            state: SessionState::Memory {
                id,
                complete: false,
            },
        })
    }
    pub(super) async fn memory_resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        publication.check()?;
        self.before(MemoryRequest::Other("resume_upload")).await?;
        let id = self.session_id(session)?;
        let state = self.state.lock().await;
        match state.uploads.get(&id) {
            Some(pending) => {
                pending.check(session)?;
                session.confirmed = pending.bytes.len() as u64;
            }
            None => confirm_published(&state, session, id)?,
        }
        Ok(())
    }
    pub(super) async fn memory_upload_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        publication.check()?;
        // The whole part is taken as the call starts, as for a create.
        sent.moved(bytes.len() as u64);
        self.before(MemoryRequest::Other("upload_part")).await?;
        let gate = self.state.lock().await.faults.part_gate.clone();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|closed| StorageFailure::Protocol.with_source(closed))?
                .forget();
        }
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let pending = state
            .uploads
            .get(&id)
            .ok_or(StorageFailure::SessionExpired)?;
        pending.check(session)?;
        if pending.bytes.len() as u64 != session.confirmed {
            return Err(StorageFailure::InvalidPart.into());
        }
        if std::mem::replace(&mut state.faults.drop_part, false) {
            return Err(StorageFailure::Network.into());
        }
        state
            .uploads
            .get_mut(&id)
            .ok_or(StorageFailure::SessionExpired)?
            .bytes
            .extend_from_slice(bytes);
        if std::mem::replace(&mut state.faults.lose_part_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        state.sent_bytes += bytes.len() as u64;
        state.largest_part = state.largest_part.max(bytes.len());
        session.confirmed = end;
        Ok(())
    }
    pub(super) async fn memory_finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        publication.check()?;
        self.before(MemoryRequest::Other("finish_upload")).await?;
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let Some(pending) = state.uploads.get(&id) else {
            return confirm_published(&state, session, id);
        };
        pending.check(session)?;
        if pending.bytes.len() as u64 != session.total || session.confirmed != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        check_target(&state, session.target())?;
        if matches!(session.target(), UploadTarget::Replace(_))
            && state.faults.refuse_replacements > 0
        {
            state.faults.refuse_replacements -= 1;
            return Err(StorageFailure::Conflict.into());
        }
        let revision = state.next_id()?;
        let pending = state
            .uploads
            .remove(&id)
            .ok_or(StorageFailure::SessionExpired)?;
        state.objects.insert(
            pending.target.path().clone(),
            Object {
                revision,
                bytes: pending.bytes.into(),
                stored_at: self.storage_time()?,
                upload_id: Some(id),
            },
        );
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        session.state = SessionState::Memory { id, complete: true };
        Ok(())
    }
    pub(super) async fn memory_abort_upload(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        publication.check()?;
        self.before(MemoryRequest::Other("abort_upload")).await?;
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        if state.faults.abort_failures > 0 {
            state.faults.abort_failures -= 1;
            return Err(StorageFailure::Network.into());
        }
        if let Some(pending) = state.uploads.get(&id) {
            pending.check(session)?;
        }
        state.uploads.remove(&id);
        Ok(())
    }
}

#[cfg(test)]
#[path = "memory_upload_tests.rs"]
mod tests;
