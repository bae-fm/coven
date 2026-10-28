//! Fixed floor bytes held outside the database's bounded operation record.
use super::*;
use crate::sealed_stream::SealedStream;
use coven_format::{
    floor::{FloorFrames, FloorHeader, OriginalSlot},
    sealed_floor::FloorPrefixEncoder,
};
use coven_foundation::{files::StoreDirAccess, id_source::KeyId};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct FloorCapture {
    pub(super) previous: Option<StoredObject>,
    /// The previous floor's `first`, or 1 without one. This floor's deletions
    /// start there: the slots below it are the previous floor's deletions.
    pub(super) previous_first: u64,
    pub(super) first: u64,
    pub(super) announcements: Vec<u64>,
    pub(super) at: u64,
    pub(super) key: KeyId,
    pub(super) copies: Vec<u64>,
    pub(super) name: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct FloorArtifact {
    pub(super) capture: FloorCapture,
    pub(super) size: u64,
    pub(super) session: Option<coven_storage::UploadSession>,
}

/// Feed the existing frame encoder one original at a time. Neither the complete
/// plaintext nor its ciphertext has to fit in one buffer or database value.
struct FloorPlaintext {
    frames: FloorFrames,
    originals: std::vec::IntoIter<OriginalSlot>,
    piece: Cursor<Vec<u8>>,
    ended: bool,
}
impl Read for FloorPlaintext {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let read = self.piece.read(out)?;
            if read != 0 || self.ended {
                return Ok(read);
            }
            let next = match self.originals.next() {
                Some(original) => self.frames.announcement(&original),
                None => {
                    self.ended = true;
                    self.frames.end()
                }
            }
            .map_err(std::io::Error::other)?;
            self.piece = Cursor::new(next);
        }
    }
}
impl SequenceSync {
    #[expect(
        clippy::map_err_ignore,
        reason = "a copy count past u32 cannot be framed"
    )]
    pub(super) async fn capture_floor(
        &self,
        capture: &FloorCapture,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<FloorArtifact, SyncError> {
        let file = self.artifact_file(&capture.name)?;
        // The capture is journaled before creating its file; this phase has sent
        // no provider request, so retry may replace an interrupted local capture.
        file.remove()?;
        let mut writer = file.create_writer(self.directory.lock_read_only()?)?;
        let mut originals: BTreeMap<_, _> = self
            .database
            .original_slots()
            .await?
            .into_iter()
            .map(|original| (original.slot, original))
            .collect();
        let (mut routing, mut prefix) = FloorPrefixEncoder::start(
            capture.key,
            Slot(capture.first),
            capture
                .copies
                .len()
                .try_into()
                .map_err(|_| coven_format::Error::Truncated)?,
        )?;
        for slot in &capture.copies {
            prefix.extend(
                routing.copy(
                    originals
                        .get(&Slot(*slot))
                        .ok_or(coven_database::DbError::DamagedDatabase)?,
                )?,
            );
        }
        routing.finish()?;
        let header = FloorHeader {
            author: self.device,
            at: capture.at,
            first: Slot(capture.first),
            snapshots: capture
                .announcements
                .iter()
                .map(|slot| Slot(*slot))
                .collect(),
        };
        let (frames, header) = FloorFrames::start(header)?;
        let announcements = capture
            .announcements
            .iter()
            .map(|slot| {
                originals
                    .remove(&Slot(*slot))
                    .ok_or(coven_database::DbError::DamagedDatabase)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let plaintext = FloorPlaintext {
            frames,
            originals: announcements.into_iter(),
            piece: Cursor::new(header),
            ended: false,
        };
        let mut sealed = SealedStream::new(
            plaintext,
            ObjectPath::floor(self.store()),
            prefix,
            None,
            crate::write_seal::derive(ring, &Audience::Store, capture.key)?,
            member.clone(),
        );
        let mut buffer = vec![0; coven_format::chunks::CHUNK_SIZE];
        let mut size = 0u64;
        loop {
            let length = sealed.read(&mut buffer).map_err(SyncError::disk)?;
            if length == 0 {
                break;
            }
            writer.append(&buffer[..length]).await?;
            size = size
                .checked_add(length as u64)
                .ok_or(SyncError::SequenceExhausted)?;
        }
        writer.finish().await?;
        Ok(FloorArtifact {
            capture: capture.clone(),
            size,
            session: None,
        })
    }
}
