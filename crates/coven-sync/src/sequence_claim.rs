//! Exact-byte claim publication and settlement, independent of its slot kind.

use crate::{
    floor_object::FloorObject,
    recorded_upload::{self, UploadSource, Uploaded},
    sealed_stream::SealedStream,
    sequence_sync::{SequenceSync, Session},
    slot_object,
    stream_input::StreamInput,
    SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{ClaimAttempt, ClaimRead, ClaimSettlement, ClaimWork, Database};
use coven_format::{
    sealed_slot::{SlotKind, SlotObjectPrefix},
    sealed_write::WriteObjectPrefix,
    sequence::SlotHeader,
};
use coven_merge::Audience;
use coven_storage::{ObjectPath, StorageFailure, UploadSession};
use std::io::{Cursor, Read};

struct ClaimSource {
    database: Database,
}
impl UploadSource for ClaimSource {
    type Error = SyncError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, SyncError> {
        Ok(self.database.claim_range(offset, length).await?)
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), SyncError> {
        Ok(self.database.save_claim_upload(serde_json::to_vec(&session)?).await?)
    }
    fn keep_going(&self) -> bool {
        true
    }
}

impl SequenceSync {
    /// Settlement precedes every new attempt. A landed operation keeps its
    /// journal until its owner commits the corresponding operation transition.
    pub(crate) async fn settle_open(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Option<ClaimSettlement>, SyncError> {
        let Some(claim) = self.database.current_claim().await? else {
            return Ok(None);
        };
        let path = claim.attempts.last().expect("journal has an attempt");
        if claim.settlement == ClaimSettlement::DeviceIdInUse {
            return Err(self.state.stop_copy(crate::DeviceIdEvidence::UnattemptedSlot {
                slot: path.slot_number().expect("claim slot"),
            }));
        }
        self.remove_compacted_attempts(&claim, session.floor.as_ref()).await?;
        if claim.settlement == ClaimSettlement::NotLanded {
            return Ok(Some(claim.settlement));
        }
        if claim.settlement == ClaimSettlement::Landed {
            self.apply_claim(session, ring, member).await?;
            return Ok(Some(ClaimSettlement::Landed));
        }
        let input = self.fetch(path).await?;
        let floor = FloorObject::read(
            &self.database,
            self.storage.as_ref(),
            &ObjectPath::floor(self.store()),
            session.floor.as_ref(),
            ring,
            member,
        )
        .await?;
        let result = if floor
            .as_ref()
            .is_some_and(|floor| floor.header.first > path.slot_number().expect("claim slot"))
        {
            self.settle_compacted(floor.as_ref().expect("passed floor"), ring, member).await?
        } else {
            if let Some(floor) = &floor {
                self.authenticate_floor(floor, &session.history, ring, member).await?;
            }
            match input {
                Some(input) => {
                    let bytes = input.local_bytes().await?;
                    let received = slot_object::open(
                        bytes.clone(),
                        path,
                        ring,
                        &session.history,
                        |audience| {
                            crate::effects::belongs(
                                &session.history.state,
                                audience,
                                &member.member(),
                            )
                        },
                    )?;
                    self.check_copy(&received, path).await?;
                    self.database.settle_claim(ClaimRead::Kept(Some(Box::new(bytes)))).await?
                }
                None => self.database.settle_claim(ClaimRead::Kept(None)).await?,
            }
        };
        if result == ClaimSettlement::DeviceIdInUse {
            return Err(self.state.stop_copy(crate::DeviceIdEvidence::UnattemptedSlot {
                slot: path.slot_number().expect("claim slot"),
            }));
        }
        self.remove_compacted_attempts(&claim, floor.as_ref()).await?;
        if result == ClaimSettlement::Landed {
            if let Some(floor) =
                floor.filter(|floor| floor.header.first.0 > session.through.saturating_add(1))
            {
                self.reload_floor(session, floor, ring, member, None).await?;
            }
            self.apply_claim(session, ring, member).await?;
        }
        Ok(Some(result))
    }

    async fn settle_compacted(
        &self,
        floor: &FloorObject,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<ClaimSettlement, SyncError> {
        let (_, snapshot) = self.floor_history(floor, ring, member).await?;
        let (last_claim, digest) =
            match snapshot.body().claims.iter().find(|claim| claim.device == self.device) {
                Some(claim) => (claim.number, claim.digest),
                None => (0, None),
            };
        Ok(self
            .database
            .settle_claim(ClaimRead::Compacted {
                first: floor.header.first.0,
                covers: snapshot.announcement().covers,
                last_claim,
                digest,
            })
            .await?)
    }

    pub(crate) async fn publish_current(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<ClaimSettlement, SyncError> {
        self.state.check()?;
        let claim = self.database.current_claim().await?.ok_or(coven_database::DbError::NoClaim)?;
        let path = claim.attempts.last().expect("claim has an attempt");
        let upload = self.database.claim_upload().await?;
        let mut publication = None;
        let created = if upload.size <= self.storage.single_request_limit().min(32 * 1024 * 1024) {
            let bytes = self.database.claim_range(0, upload.size as usize).await?;
            match self.storage.create(path, &bytes).await {
                Ok(object) => {
                    publication = object;
                    true
                }
                Err(error) if error.failure() == StorageFailure::AlreadyExists => false,
                Err(error) => return Err(error.into()),
            }
        } else {
            let recorded =
                upload.session.map(|bytes| serde_json::from_slice(&bytes)).transpose()?;
            match recorded_upload::upload(
                self.storage.as_ref(),
                &coven_storage::UploadTarget::Create(path.clone()),
                upload.size,
                recorded,
                &mut ClaimSource { database: self.database.clone() },
            )
            .await?
            {
                Uploaded::Stored => true,
                Uploaded::Occupied(_) => false,
                Uploaded::Paused => unreachable!("claims retain their initiation until settlement"),
            }
        };
        if !created {
            return Ok(self.settle_open(session, ring, member).await?.expect("open journal"));
        }
        let floor = FloorObject::read(
            &self.database,
            self.storage.as_ref(),
            &ObjectPath::floor(self.store()),
            session.floor.as_ref(),
            ring,
            member,
        )
        .await?;
        let result = if floor
            .as_ref()
            .is_some_and(|floor| floor.header.first > path.slot_number().expect("claim slot"))
        {
            self.settle_compacted(floor.as_ref().expect("passed floor"), ring, member).await?
        } else {
            if let Some(floor) = &floor {
                self.authenticate_floor(floor, &session.history, ring, member).await?;
            }
            self.database.settle_claim(ClaimRead::Created).await?
        };
        if result == ClaimSettlement::DeviceIdInUse {
            return Err(self.state.stop_copy(crate::DeviceIdEvidence::UnattemptedSlot {
                slot: path.slot_number().expect("claim slot"),
            }));
        }
        self.remove_compacted_attempts(&claim, floor.as_ref()).await?;
        if result == ClaimSettlement::Landed {
            if floor
                .as_ref()
                .is_none_or(|floor| floor.header.first <= path.slot_number().expect("claim slot"))
            {
                let object = match publication {
                    Some(object) => object,
                    None => self.storage.status(path).await?.ok_or(StorageFailure::NotFound)?,
                };
                crate::object_input::ObjectInput::observe(&self.database, &object)
                    .await?
                    .keep_claim_bytes()
                    .await?;
            }
            if let Some(floor) =
                floor.filter(|floor| floor.header.first.0 > session.through.saturating_add(1))
            {
                self.reload_floor(session, floor, ring, member, None).await?;
            }
            self.apply_claim(session, ring, member).await?;
        }
        Ok(result)
    }

    /// A delayed create can recreate a deleted name. Its attempt remains in
    /// the journal until this deletion succeeds, including across retries.
    async fn remove_compacted_attempts(
        &self,
        claim: &coven_database::Claim,
        floor: Option<&FloorObject>,
    ) -> Result<(), SyncError> {
        let Some(floor) = floor else {
            return Ok(());
        };
        for path in &claim.attempts {
            if path.slot_number().expect("claim slot") < floor.header.first {
                self.storage.delete(path).await?;
            }
        }
        Ok(())
    }

    async fn apply_claim(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let claim = self.database.current_claim().await?.ok_or(coven_database::DbError::NoClaim)?;
        let path = claim.attempts.last().expect("claim has an attempt");
        let at = path.slot_number().expect("claim slot");
        if at.0 > session.through {
            let input =
                match crate::object_input::ObjectInput::retained(&self.database, path).await? {
                    Some(input) => input,
                    None => {
                        let object =
                            self.storage.status(path).await?.ok_or(StorageFailure::NotFound)?;
                        crate::object_input::ObjectInput::observe(&self.database, &object).await?
                    }
                };
            input.keep_claim_bytes().await?;
            let received = slot_object::open(
                input.local_bytes().await?,
                path,
                ring,
                &session.history,
                |audience| {
                    crate::effects::belongs(&session.history.state, audience, &member.member())
                },
            )?;
            let mut history = session.history.clone();
            let incoming = history.consume(received, member.member());
            history.acquire(ring, member)?;
            self.follow_person_entries(&session.history, &history, member)?;
            self.apply(session, incoming, history, ring, member).await?;
            input.release().await?;
        }
        if matches!(claim.work, ClaimWork::Writes(_)) {
            self.database.finish_claim(None).await?;
        }
        Ok(())
    }

    #[expect(
        clippy::map_err_ignore,
        reason = "a send fails only when the reader is gone, which BrokenPipe names"
    )]
    pub(crate) async fn send_writes(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        through: u64,
    ) -> Result<(), SyncError> {
        self.check_binding(session, member)?;
        if !session.history.state.devices.contains_key(&self.device) {
            return Ok(());
        }
        loop {
            let current = self.database.current_claim().await?;
            if let Some(claim) = &current {
                if !matches!(claim.work, ClaimWork::Writes(_)) {
                    return Ok(());
                }
                match claim.settlement {
                    ClaimSettlement::Unknown | ClaimSettlement::Retry => {
                        match self.publish_current(session, ring, member).await? {
                            ClaimSettlement::Landed => return Ok(()),
                            ClaimSettlement::NotLanded => {
                                self.read_forward(session, ring, member).await?;
                                continue;
                            }
                            _ => unreachable!(
                                "publication lands or loses after a successful request"
                            ),
                        }
                    }
                    ClaimSettlement::Landed => {
                        self.apply_claim(session, ring, member).await?;
                        return Ok(());
                    }
                    ClaimSettlement::DeviceIdInUse => {
                        return Err(self.state.stop_copy(
                            crate::DeviceIdEvidence::UnattemptedSlot {
                                slot: claim
                                    .attempts
                                    .last()
                                    .expect("attempt")
                                    .slot_number()
                                    .expect("slot"),
                            },
                        ))
                    }
                    ClaimSettlement::NotLanded => (),
                }
            }
            let through = match &current {
                Some(claim) => match claim.work {
                    ClaimWork::Writes(through) => through,
                    _ => unreachable!("writes selected"),
                },
                None => through,
            };
            let held = ring.clone();
            let history = session.history.clone();
            let identity = member.member();
            let headers = self
                .database
                .prepare_uploads::<SyncError, _>(through, move |header| {
                    Ok(WriteObjectPrefix {
                        format: coven_format::FormatVersion::CURRENT,
                        store_key: history.sending_key(&held, identity, &Audience::Store)?,
                        part_keys: header
                            .parts
                            .iter()
                            .map(|part| history.sending_key(&held, identity, &part.audience))
                            .collect::<Result<_, _>>()?,
                    })
                })
                .await?;
            if headers.is_empty() {
                return Ok(());
            }
            let at = session.through.checked_add(1).ok_or(SyncError::SequenceExhausted)?;
            let path = self.path(at)?;
            let prefix = SlotObjectPrefix {
                format: coven_format::FormatVersion::CURRENT,
                key: session.history.sending_key(ring, member.member(), &Audience::Store)?,
                kind: SlotKind::Writes,
            };
            let key = crate::write_seal::derive(ring, &Audience::Store, prefix.key)?;
            let (send, input) = StreamInput::channel();
            let producer_database = self.database.clone();
            let held = ring.clone();
            let author = member.clone();
            let store = self.store();
            let produce = async move {
                let output = send.clone();
                let result = producer_database
                    .read_uploads::<SyncError, _>(through, move |upload| {
                        let length = coven_format::sealed_write::sealed_length(
                            upload.header_frame.len(),
                            &upload
                                .header
                                .parts
                                .iter()
                                .map(|part| part.plaintext_length)
                                .collect::<Vec<_>>(),
                        )?;
                        let mut emit = |bytes: &[u8]| {
                            output.blocking_send(Ok(bytes.to_vec())).map_err(|_| {
                                SyncError::disk(std::io::Error::from(
                                    std::io::ErrorKind::BrokenPipe,
                                ))
                            })
                        };
                        emit(&length.to_be_bytes())?;
                        crate::write_seal::seal(store, upload, &held, &author, &mut emit)
                    })
                    .await;
                if let Err(error) = result {
                    // The reader stops only after its own failure or a
                    // cancel; the reading failure then goes to the log.
                    if let Err(tokio::sync::mpsc::error::SendError(Err(error))) =
                        send.send(Err(error)).await
                    {
                        tracing::warn!(
                            "reading writes to claim failed after the claim stopped: {error}"
                        );
                    }
                }
            };
            let author = member.clone();
            let device = self.device;
            let count = headers.len() as u64;
            let seal = move |number| -> Result<_, coven_database::DbError> {
                let header =
                    SlotHeader { device, claim: number, author: author.member(), after: at - 1 };
                let mut body = header.encode(&path)?;
                body.extend_from_slice(&count.to_be_bytes());
                let input = Cursor::new(body).chain(input);
                Ok(ClaimAttempt {
                    path: path.clone(),
                    bytes: SealedStream::new(input, path, prefix.encode()?, None, key, author),
                })
            };
            let journal = async {
                if let Some(claim) = current {
                    self.database.retry_claim(seal(claim.number)?).await?;
                } else {
                    self.database.begin_claim(ClaimWork::Writes(through), seal).await?;
                }
                Ok::<_, SyncError>(())
            };
            let ((), journaled) = tokio::join!(produce, journal);
            journaled?;
            match self.publish_current(session, ring, member).await? {
                ClaimSettlement::Landed => return Ok(()),
                ClaimSettlement::NotLanded => self.read_forward(session, ring, member).await?,
                _ => unreachable!("successful publication is settled"),
            }
        }
    }
}

#[cfg(test)]
#[path = "sequence_claim_tests.rs"]
mod tests;
