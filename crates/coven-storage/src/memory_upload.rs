//! Atomic conditional publication in the deterministic provider.
use super::*;
use crate::session::{part_span, Confirmation, PlanStart, StartSent};

/// A planned session's start as it goes out: past any hold, the publication
/// gate is checked again, as S3 checks it as the request goes out, and a start
/// it lets through has gone out (Appendix E.10).
pub(super) struct MemoryStart<'a> {
    publication: &'a PublicationRequest,
    sent: &'a StartSent,
}

impl MemoryStart<'_> {
    pub(super) fn go(&self) -> Result<(), StorageError> {
        self.publication.check()?;
        self.sent.going();
        Ok(())
    }
}

pub(super) struct Pending {
    device: coven_foundation::id_source::DeviceId,
    /// When the session began, by the provider's clock.
    begun: coven_format::value::StorageTime,
    target: UploadTarget,
    total: u64,
    pub(super) bytes: Vec<u8>,
    /// An independent session's parts, by index from zero: each kept once,
    /// a part sent again taking its own place.
    parts: BTreeMap<u64, Vec<u8>>,
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
        let lost = match &session.state {
            SessionState::MemoryParts { parts, .. } => {
                parts.iter().any(|index| !self.parts.contains_key(index))
            }
            _ => {
                (self.bytes.len() as u64) < session.confirmed
                    || self.bytes.len() as u64 > self.total
            }
        };
        if lost {
            return Err(
                StorageFailure::Protocol.with_source("memory provider lost confirmed progress")
            );
        }
        Ok(())
    }
    /// The bytes publishing hands over: an independent session's parts in
    /// order, which must be every one; another's bytes as they arrived.
    fn assembled(self) -> Result<Vec<u8>, StorageError> {
        if self.parts.is_empty() {
            return Ok(self.bytes);
        }
        let count = self.total.div_ceil(self.part_size as u64);
        if self.parts.len() as u64 != count {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(self.parts.into_values().flatten().collect())
    }
}
fn confirm_published(
    state: &MemoryState,
    session: &mut UploadSession,
    id: u64,
) -> Result<(), StorageError> {
    let object = state.objects.get(session.path()).ok_or(StorageFailure::SessionExpired)?;
    if object.upload_id != Some(id) || object.bytes.len() as u64 != session.total {
        return Err(StorageFailure::AlreadyExists.into());
    }
    session.confirmed = session.total;
    session.state = SessionState::Memory { id, complete: true };
    Ok(())
}

/// Whether every session at a planned one's name is its own: a name its
/// device alone uses, a path in its own file or snapshot folders, as on S3
/// (Appendix E.10). A shared slot or floor path proves nothing.
fn owns_name(session: &UploadSession) -> bool {
    session.path().device() == Some(session.device)
}

/// End every session at a planned one's name it owns: one its first part
/// started, whose start's reply was lost or never recorded.
fn end_planned(state: &mut MemoryState, session: &UploadSession) {
    state.uploads.retain(|_, pending| pending.target.path() != session.path());
}

fn check_target(state: &MemoryState, target: &UploadTarget) -> Result<(), StorageError> {
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

impl MemoryStorage {
    /// A four-byte upload to `path` the provider holds unpublished: begun,
    /// then started by its first and only part, as an S3 multipart upload
    /// is (Appendix E.10).
    pub async fn started_upload(&self, path: &ObjectPath) -> Result<UploadSession, StorageError> {
        let mut session = self.begin_upload(path, 4).await?;
        self.upload_part(&mut session, b"part").await?;
        Ok(session)
    }
    /// Whether the provider holds the unfinished session `session` names; a
    /// plan names none until its first part starts it.
    pub async fn holds(&self, session: &UploadSession) -> bool {
        match session.state {
            SessionState::Memory { id, .. } | SessionState::MemoryParts { id, .. } => {
                self.provider.state.lock().await.uploads.contains_key(&id)
            }
            _ => false,
        }
    }
    /// Each session still unfinished, in the order it began: the device
    /// whose connection began it, and its destination.
    pub async fn unfinished_uploads(
        &self,
    ) -> Vec<(coven_foundation::id_source::DeviceId, ObjectPath)> {
        let state = self.provider.state.lock().await;
        state
            .uploads
            .values()
            .map(|pending| (pending.device, pending.target.path().clone()))
            .collect()
    }
}

impl MemoryProvider {
    /// The sessions `device`'s connections began in `store`.
    pub(super) async fn memory_device_uploads(
        &self,
        store: coven_foundation::id_source::StoreId,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        self.before(MemoryRequest::Other("device_uploads")).await?;
        let state = self.state.lock().await;
        Ok(state
            .uploads
            .iter()
            .filter(|(_, pending)| {
                pending.device == device && pending.target.path().store() == store
            })
            .map(|(id, _)| LeftUpload(crate::left_upload::Left::Memory(*id)))
            .collect())
    }
    /// The sessions at slots 1 to `through` of `store`, whoever began them:
    /// the deterministic provider holds a claim's session at its slot, as
    /// conditional completion on S3 does.
    pub(super) async fn memory_settled_claim_uploads(
        &self,
        store: coven_foundation::id_source::StoreId,
        through: u64,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        self.before(MemoryRequest::Other("settled_claim_uploads")).await?;
        let state = self.state.lock().await;
        Ok(state
            .uploads
            .iter()
            .filter(|(_, pending)| {
                let path = pending.target.path();
                path.store() == store
                    && path.slot_number().is_some_and(|slot| slot.get() <= through)
            })
            .map(|(id, _)| LeftUpload(crate::left_upload::Left::Memory(*id)))
            .collect())
    }
    /// The sessions at `floor`'s path begun in an earlier second than its
    /// publication: the deterministic provider dates each session's start,
    /// as a certified S3 profile does.
    pub(super) async fn memory_superseded_floor_uploads(
        &self,
        floor: &StoredObject,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        self.before(MemoryRequest::Other("superseded_floor_uploads")).await?;
        let state = self.state.lock().await;
        Ok(state
            .uploads
            .iter()
            .filter(|(_, pending)| {
                pending.target.path() == &floor.path
                    && pending.begun.seconds() < floor.stored_at.seconds()
            })
            .map(|(id, _)| LeftUpload(crate::left_upload::Left::Memory(*id)))
            .collect())
    }
    /// End one session another device left; one already ended succeeds.
    pub(super) async fn memory_end_left_upload(
        &self,
        upload: &LeftUpload,
    ) -> Result<(), StorageError> {
        let crate::left_upload::Left::Memory(id) = upload.0 else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        self.before(MemoryRequest::Other("end_left_upload")).await?;
        self.state.lock().await.uploads.remove(&id);
        Ok(())
    }
    pub(super) async fn memory_begin_upload(
        &self,
        publication: &PublicationRequest,
        target: &UploadTarget,
        total: u64,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<UploadSession, StorageError> {
        publication.check()?;
        self.before(MemoryRequest::Other("begin_upload")).await?;
        self.refuse_file_upload(target.path()).await?;
        check_target(&*self.state.lock().await, target)?;
        // The caller records the plan; the first part starts the session.
        Ok(UploadSession {
            device,
            location: self.config(),
            target: target.clone(),
            total,
            confirmed: 0,
            part_size: self.part_size,
            state: SessionState::MemoryPlanned { started: PlanStart::Unstarted },
        })
    }
    pub(super) async fn memory_resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        if let SessionState::MemoryPlanned { started } = session.state {
            // Nothing is at the name of a plan whose first part was never
            // sent, and nothing tells a stray at a shared name from another
            // device's session (Appendix E.10).
            if started != PlanStart::MayHaveStarted {
                return Ok(());
            }
            if !owns_name(session) {
                return Err(StorageFailure::SessionIdentityUnknown.into());
            }
            publication.check()?;
            self.before(MemoryRequest::Other("resume_upload")).await?;
            end_planned(&mut *self.state.lock().await, session);
            session.state = SessionState::MemoryPlanned { started: PlanStart::Unstarted };
            return Ok(());
        }
        publication.check()?;
        self.before(MemoryRequest::Other("resume_upload")).await?;
        let id = self.session_id(session)?;
        let state = self.state.lock().await;
        match state.uploads.get(&id) {
            Some(pending) => {
                pending.check(session)?;
                match &mut session.state {
                    // Every part the provider holds, whichever arrived first.
                    SessionState::MemoryParts { parts, .. } => {
                        *parts = pending.parts.keys().copied().collect();
                        session.confirmed = pending
                            .parts
                            .keys()
                            .filter_map(|index| part_span(pending.total, pending.part_size, *index))
                            .map(|span| span.end - span.start)
                            .sum();
                    }
                    _ => session.confirmed = pending.bytes.len() as u64,
                }
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
        if let SessionState::MemoryPlanned { started } = &mut session.state {
            // A restored marked plan resumes first, and one whose start this
            // process sent may have started a session already.
            if *started == PlanStart::MayHaveStarted {
                return Err(StorageFailure::SessionIdentityUnknown.into());
            }
            // Until it settles, the start may go out at any moment.
            *started = PlanStart::MayHaveStarted;
            let start = StartSent::default();
            if let Err(error) = self.initiate(publication, session, &start).await {
                session.start_failed(&start);
                return Err(error);
            }
        }
        if matches!(session.state, SessionState::MemoryParts { .. }) {
            let offset = end - bytes.len() as u64;
            let part = self.memory_send_part(publication, session, offset, bytes, sent).await?;
            return session.confirm(part);
        }
        publication.check()?;
        // The whole part is taken as the call starts, as for a create.
        sent.moved(bytes.len() as u64);
        self.before(MemoryRequest::Other("upload_part")).await?;
        let gate = self.state.lock().await.part_gate.clone();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|closed| StorageFailure::Protocol.with_source(closed))?
                .forget();
        }
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let pending = state.uploads.get(&id).ok_or(StorageFailure::SessionExpired)?;
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
    /// Send part `offset..` of an independent session, as S3 takes a
    /// multipart upload's part: kept by its index, replacing one sent
    /// before, and confirmed for the caller to add to its session.
    pub(super) async fn memory_send_part(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
        offset: u64,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<crate::ConfirmedPart, StorageError> {
        let SessionState::MemoryParts { id, .. } = session.state else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        publication.check()?;
        // The whole part is taken as the call starts, as for a create.
        sent.moved(bytes.len() as u64);
        self.before(MemoryRequest::Other("upload_part")).await?;
        let gate = self.state.lock().await.part_gate.clone();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|closed| StorageFailure::Protocol.with_source(closed))?
                .forget();
        }
        let index = offset / session.part_size as u64;
        let mut state = self.state.lock().await;
        let pending = state.uploads.get(&id).ok_or(StorageFailure::SessionExpired)?;
        pending.check(session)?;
        if !offset.is_multiple_of(session.part_size as u64)
            || part_span(session.total, session.part_size, index)
                .is_none_or(|span| span.end - span.start != bytes.len() as u64)
        {
            return Err(StorageFailure::InvalidPart.into());
        }
        if std::mem::replace(&mut state.faults.drop_part, false) {
            return Err(StorageFailure::Network.into());
        }
        state
            .uploads
            .get_mut(&id)
            .ok_or(StorageFailure::SessionExpired)?
            .parts
            .insert(index, bytes.to_vec());
        if std::mem::replace(&mut state.faults.lose_part_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        state.sent_bytes += bytes.len() as u64;
        state.largest_part = state.largest_part.max(bytes.len());
        Ok(crate::ConfirmedPart(Confirmation::Memory { index }))
    }
    /// Start a planned session's upload, as its first part does on S3,
    /// telling `sent` once the start goes out.
    async fn initiate(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        sent: &StartSent,
    ) -> Result<(), StorageError> {
        let start = MemoryStart { publication, sent };
        self.before_going(MemoryRequest::Other("initiate_upload"), Some(start)).await?;
        let mut state = self.state.lock().await;
        let id = state.next_id()?;
        state.uploads.insert(
            id,
            Pending {
                device: session.device,
                begun: self.storage_time()?,
                target: session.target.clone(),
                total: session.total,
                bytes: Vec::new(),
                parts: BTreeMap::new(),
                part_size: session.part_size,
            },
        );
        if std::mem::replace(&mut state.faults.lose_initiation_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        session.state = if self.independent_parts {
            SessionState::MemoryParts { id, parts: Vec::new(), complete: false }
        } else {
            SessionState::Memory { id, complete: false }
        };
        Ok(())
    }
    pub(super) async fn memory_finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        publication.check()?;
        self.before(MemoryRequest::Other("finish_upload")).await?;
        if matches!(session.state, SessionState::MemoryPlanned { .. }) {
            // Nothing is sent before the first part starts the session.
            return Err(StorageFailure::InvalidPart.into());
        }
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let Some(pending) = state.uploads.get(&id) else {
            return confirm_published(&state, session, id);
        };
        pending.check(session)?;
        let whole = match &session.state {
            SessionState::MemoryParts { .. } => {
                pending.parts.len() as u64 == session.total.div_ceil(session.part_size as u64)
            }
            _ => pending.bytes.len() as u64 == session.total,
        };
        if !whole || session.confirmed != session.total {
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
        let pending = state.uploads.remove(&id).ok_or(StorageFailure::SessionExpired)?;
        let path = pending.target.path().clone();
        state.objects.insert(
            path,
            Object {
                revision,
                bytes: pending.assembled()?.into(),
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
        if let SessionState::MemoryPlanned { started } = session.state {
            // A plan whose first part was never sent has nothing to end
            // (Appendix E.10).
            if started != PlanStart::MayHaveStarted {
                return Ok(());
            }
            if !owns_name(session) {
                return Err(StorageFailure::SessionIdentityUnknown.into());
            }
        }
        publication.check()?;
        self.before(MemoryRequest::Other("abort_upload")).await?;
        let id = match session.state {
            SessionState::MemoryPlanned { .. } => None,
            _ => Some(self.session_id(session)?),
        };
        let mut state = self.state.lock().await;
        if state.faults.abort_failures > 0 {
            state.faults.abort_failures -= 1;
            return Err(StorageFailure::Network.into());
        }
        let Some(id) = id else {
            end_planned(&mut state, session);
            return Ok(());
        };
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
