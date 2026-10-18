//! Journaled snapshot bytes, captured without holding the application writer.

use crate::{sequence_sync::SequenceSync, SyncError};
use coven_crypto::{DerivedKeys, MemberIdentity, ObjectHasher, StoreKeyring};
use coven_format::{
    admin::SnapshotId,
    sealed_snapshot::SnapshotObjectPrefix,
    sequence::{Purpose, SnapshotAnnouncement},
};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::{
    files::{FileArea, FileName},
    id_source::KeyId,
};
use coven_merge::Audience;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Capture {
    #[serde(with = "crate::pending_encoding::snapshot_id")]
    pub(crate) id: SnapshotId,
    #[serde(with = "purpose")]
    pub(crate) purpose: Purpose,
    pub(crate) key: KeyId,
    pub(crate) name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotArtifact {
    pub(crate) capture: Capture,
    pub(crate) covers: u64,
    pub(crate) digest: [u8; 32],
    pub(crate) size: u64,
    pub(crate) session: Option<coven_storage::UploadSession>,
}
impl SnapshotArtifact {
    pub(crate) fn announcement(&self) -> SnapshotAnnouncement {
        SnapshotAnnouncement {
            snapshot: self.capture.id.clone(),
            covers: self.covers,
            purpose: self.capture.purpose,
            digest: self.digest,
        }
    }
}

pub(crate) mod purpose {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &Purpose,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Purpose::Ordinary => (0, None),
            Purpose::Raise(version) => (1, Some(*version)),
            Purpose::Reset => (2, None),
        }
        .serialize(output)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<Purpose, D::Error> {
        match <(u8, Option<u32>)>::deserialize(input)? {
            (0, None) => Ok(Purpose::Ordinary),
            (1, Some(version)) => Ok(Purpose::Raise(version)),
            (2, None) => Ok(Purpose::Reset),
            _ => Err(serde::de::Error::custom("invalid snapshot purpose")),
        }
    }
}

impl SequenceSync {
    pub(crate) fn artifact_file(
        &self,
        name: &str,
    ) -> Result<coven_foundation::files::AtomicFile, SyncError> {
        Ok(self.directory.file(
            FileArea::AppProvided,
            &FileName::new(name).map_err(|_| coven_database::DbError::DamagedDatabase)?,
        ))
    }

    pub(crate) async fn capture_snapshot(
        &self,
        capture: &Capture,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<SnapshotArtifact, SyncError> {
        let file = self.artifact_file(&capture.name)?;
        // Capturing precedes any provider request. A partial local file from
        // this journal step has no remote identity and can be replaced.
        file.remove()?;
        let writer = file.create_writer(self.directory.lock_read_only()?)?;
        let path = crate::snapshot_object::path(self.store(), &capture.id)?;
        let key = crate::write_seal::derive(ring, &capture.id.audience, capture.key)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (header_sender, header_receiver) = tokio::sync::oneshot::channel();
        let key_id = capture.key;
        let purpose = capture.purpose;
        let begin = move |header: &coven_format::snapshot::SnapshotHeader| {
            header_sender
                .send(SnapshotObjectPrefix {
                    audience: header.id.audience.clone(),
                    key: key_id,
                    covers: header.covers,
                    purpose,
                })
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        };
        let emit = move |frame| {
            sender
                .blocking_send(frame)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        };
        let produce = async {
            match capture.purpose {
                Purpose::Raise(version) => {
                    self.database
                        .write_raise_snapshot(capture.id.clone(), version, begin, emit)
                        .await
                }
                _ => {
                    self.database
                        .write_snapshot(capture.id.clone(), begin, emit)
                        .await
                }
            }
        };
        let consume = seal_frames(writer, header_receiver, receiver, &path, key, member);
        let (produced, consumed) = tokio::join!(produce, consume);
        match produced {
            Err(coven_database::SnapshotWriteError::Database(error)) => return Err(error.into()),
            Err(coven_database::SnapshotWriteError::Format(error)) => return Err(error.into()),
            Err(coven_database::SnapshotWriteError::Output(error)) => {
                consumed?;
                return Err(SyncError::disk(error));
            }
            Ok(()) => (),
        }
        let (covers, digest, size) = consumed?;
        Ok(SnapshotArtifact {
            capture: capture.clone(),
            covers,
            digest,
            size,
            session: None,
        })
    }
}

async fn seal_frames(
    mut writer: coven_foundation::files::FileWriter,
    header: tokio::sync::oneshot::Receiver<SnapshotObjectPrefix>,
    mut receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    path: &coven_storage::ObjectPath,
    key: DerivedKeys,
    member: &MemberIdentity,
) -> Result<(u64, [u8; 32], u64), SyncError> {
    let header = header.await.map_err(|_| coven_format::Error::Truncated)?;
    let prefix = header.encode()?;
    let signature = member.keys().sign_prefix(path.as_str(), &prefix);
    let mut hash = ObjectHasher::new();
    let mut size = (prefix.len() + 64) as u64;
    for bytes in [prefix.as_slice(), signature.as_bytes().as_slice()] {
        writer.append(bytes).await?;
        hash.update(bytes);
    }
    let mut layout = coven_format::sealed_stream::StreamObjectLayout::new();
    let mut chunk = zeroize::Zeroizing::new(Vec::with_capacity(coven_format::chunks::CHUNK_SIZE));
    while let Some(frame) = receiver.recv().await {
        let mut remaining = frame.as_slice();
        while !remaining.is_empty() {
            let length = remaining
                .len()
                .min(coven_format::chunks::CHUNK_SIZE - chunk.len());
            chunk.extend_from_slice(&remaining[..length]);
            remaining = &remaining[length..];
            if chunk.len() == coven_format::chunks::CHUNK_SIZE {
                let sealed =
                    key.seal_object_chunk(path.as_str(), &prefix, 0, layout.index(), &chunk)?;
                let piece = layout.encode_chunk(&sealed)?;
                writer.append(&piece).await?;
                hash.update(&piece);
                size += piece.len() as u64;
                chunk.clear();
            }
        }
    }
    if !chunk.is_empty() {
        let sealed = key.seal_object_chunk(path.as_str(), &prefix, 0, layout.index(), &chunk)?;
        let piece = layout.encode_chunk(&sealed)?;
        writer.append(&piece).await?;
        hash.update(&piece);
        size += piece.len() as u64;
    }
    let signature = member
        .keys()
        .sign_object(path.as_str(), &hash.clone().finish());
    writer.append(signature.as_bytes()).await?;
    hash.update(signature.as_bytes());
    size += 64;
    writer.finish().await?;
    Ok((header.covers, *hash.finish().as_bytes(), size))
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotRequest {
    #[serde(with = "crate::pending_encoding::audience")]
    pub(crate) audience: Audience,
    #[serde(with = "purpose")]
    pub(crate) purpose: Purpose,
}
