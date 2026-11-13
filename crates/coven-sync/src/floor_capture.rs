//! Fixed floor bytes held outside the database's bounded operation record.
use super::*;
use crate::sealed_stream::StreamSealer;
use coven_format::{
    floor::{FloorFrames, FloorHeader, OriginalSlot},
    floor_pointer::{FloorPointerHeader, FloorPointerPrefix},
    sealed_floor::FloorPrefixEncoder,
};
use coven_foundation::id_source::DeviceId;
use coven_foundation::id_source::KeyId;
use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct FloorCapture {
    /// The pointer this write observed, which its replacement is conditional
    /// on; none while the store has no floor.
    pub(super) previous: Option<StoredObject>,
    /// The number of this write's body among this device's floor bodies,
    /// never used before (§16.4).
    #[serde(with = "coven_database::slot_json")]
    pub(super) number: Slot,
    /// The previous floor's `first`, or 1 without one. This floor's deletions
    /// start there: the slots below it are the previous floor's deletions.
    #[serde(with = "coven_database::slot_json")]
    pub(super) previous_first: Slot,
    #[serde(with = "coven_database::slot_json")]
    pub(super) first: Slot,
    #[serde(with = "coven_database::slot_json::list")]
    pub(super) announcements: Vec<Slot>,
    pub(super) at: u64,
    pub(super) key: KeyId,
    #[serde(with = "coven_database::slot_json::list")]
    pub(super) copies: Vec<Slot>,
    pub(super) name: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct FloorArtifact {
    pub(super) capture: FloorCapture,
    /// The body's size, and the digest of its complete bytes.
    pub(super) size: u64,
    pub(super) digest: [u8; 32],
    /// The pointer's complete sealed bytes, naming the body by digest.
    pub(super) pointer: Vec<u8>,
    /// The body's upload session, once recorded.
    pub(super) session: Option<coven_storage::UploadSession>,
}

impl FloorCapture {
    /// Where this write's body goes.
    pub(super) fn body_path(
        &self,
        store: coven_foundation::id_source::StoreId,
        device: DeviceId,
    ) -> ObjectPath {
        ObjectPath::floor_body(store, device, self.number.into())
    }
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
    /// The pointer naming the body of `digest`, sealed and signed once: a
    /// retried replacement sends these same bytes.
    fn seal_pointer(
        &self,
        capture: &FloorCapture,
        digest: [u8; 32],
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Vec<u8>, SyncError> {
        let prefix = FloorPointerPrefix {
            key: capture.key,
            first: capture.first,
            device: self.device,
            number: capture.number,
            body: digest,
        };
        let header = FloorPointerHeader { author: self.device, at: capture.at };
        let (mut sealer, mut bytes) = StreamSealer::start(
            ObjectPath::floor(self.store()),
            prefix.encode()?,
            None,
            crate::write_seal::derive(ring, &Audience::Store, capture.key)?,
        );
        bytes.extend(sealer.chunk(&header.encode()?)?);
        let (signature, _, _) = sealer.finish(member);
        bytes.extend_from_slice(signature.as_bytes());
        Ok(bytes)
    }

    #[expect(clippy::map_err_ignore, reason = "a copy count past u32 cannot be framed")]
    pub(super) async fn capture_floor(
        &self,
        capture: &FloorCapture,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<FloorArtifact, SyncError> {
        // The capture is journaled before creating its file; this phase has sent
        // no provider request, so retry may replace an interrupted local capture.
        self.artifact_file(&capture.name)?.remove()?;
        let mut writer = self.artifact_writer(&capture.name)?;
        let mut originals: BTreeMap<_, _> = self
            .database
            .original_slots()
            .await?
            .into_iter()
            .map(|original| (original.slot, original))
            .collect();
        let (mut routing, mut prefix) = FloorPrefixEncoder::start(
            capture.key,
            capture.first,
            capture.copies.len().try_into().map_err(|_| coven_format::Error::Truncated)?,
        )?;
        for slot in &capture.copies {
            prefix.extend(
                routing
                    .copy(originals.get(slot).ok_or(coven_database::DbError::DamagedDatabase)?)?,
            );
        }
        routing.finish()?;
        let header = FloorHeader {
            author: self.device,
            at: capture.at,
            first: capture.first,
            snapshots: capture.announcements.clone(),
        };
        let (frames, header) = FloorFrames::start(header)?;
        let announcements = capture
            .announcements
            .iter()
            .map(|slot| originals.remove(slot).ok_or(coven_database::DbError::DamagedDatabase))
            .collect::<Result<Vec<_>, _>>()?;
        let mut plaintext = FloorPlaintext {
            frames,
            originals: announcements.into_iter(),
            piece: Cursor::new(header),
            ended: false,
        };
        let (mut sealer, start) = StreamSealer::start(
            capture.body_path(self.store(), self.device),
            prefix,
            None,
            crate::write_seal::derive(ring, &Audience::Store, capture.key)?,
        );
        writer.append(&start).await?;
        loop {
            let mut chunk = zeroize::Zeroizing::new(Vec::new());
            plaintext
                .by_ref()
                .take(coven_format::chunks::CHUNK_SIZE as u64)
                .read_to_end(&mut chunk)
                .map_err(SyncError::disk)?;
            if chunk.is_empty() {
                break;
            }
            writer.append(&sealer.chunk(&chunk)?).await?;
        }
        let (signature, digest, size) = sealer.finish(member);
        writer.append(signature.as_bytes()).await?;
        writer.finish().await?;
        let digest = *digest.as_bytes();
        let pointer = self.seal_pointer(capture, digest, ring, member)?;
        Ok(FloorArtifact { capture: capture.clone(), size, digest, pointer, session: None })
    }
}

#[cfg(test)]
#[path = "floor_capture_tests.rs"]
mod tests;
