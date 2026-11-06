//! Journaled snapshot bytes, captured without holding the application writer.

use crate::{sequence_sync::SequenceSync, SyncError};
use coven_crypto::{DerivedKeys, MemberIdentity, StoreKeyring};
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
    /// The newest breaking migration the body went through (§16.3).
    pub(crate) breaking_version: u32,
    pub(crate) digest: [u8; 32],
    pub(crate) size: u64,
    pub(crate) session: Option<coven_storage::UploadSession>,
}
impl SnapshotArtifact {
    pub(crate) fn announcement(&self) -> SnapshotAnnouncement {
        SnapshotAnnouncement {
            snapshot: self.capture.id.clone(),
            covers: self.covers,
            breaking_version: self.breaking_version,
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

/// A capture artifact's file name: a recorded name that is not one is damage.
#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn artifact_name(name: &str) -> Result<FileName, SyncError> {
    Ok(FileName::new(name).map_err(|_| coven_database::DbError::DamagedDatabase)?)
}

impl SequenceSync {
    pub(crate) fn artifact_file(
        &self,
        name: &str,
    ) -> Result<coven_foundation::files::AtomicFile, SyncError> {
        Ok(self.directory.file(FileArea::AppProvided, &artifact_name(name)?))
    }

    /// Create a capture's artifact for writing, holding the store until the
    /// writer ends.
    pub(crate) fn artifact_writer(
        &self,
        name: &str,
    ) -> Result<coven_foundation::files::FileWriter, SyncError> {
        Ok(self.directory.create_writer(FileArea::AppProvided, &artifact_name(name)?)?)
    }

    /// Take `capture`'s snapshot into its artifact. A raise takes its
    /// capture again from the state just applied, which a pass may have moved
    /// on since its operation prepared, and gets `None` once that capture
    /// moved to a later version, as an addition or a later breaking
    /// migration moves it (§16.3, §18.5).
    #[expect(
        clippy::map_err_ignore,
        reason = "a send fails only when the reader is gone, which BrokenPipe names"
    )]
    pub(crate) async fn capture_snapshot(
        &self,
        capture: &Capture,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Option<SnapshotArtifact>, SyncError> {
        // Capturing precedes any provider request. A partial local file from
        // this journal step has no remote identity and can be replaced.
        self.artifact_file(&capture.name)?.remove()?;
        if let Purpose::Raise(version) = capture.purpose {
            let audience = capture.id.audience.clone();
            if self.database.prepare_raise_capture(audience, version).await?.is_none() {
                return Ok(None);
            }
        }
        let writer = self.artifact_writer(&capture.name)?;
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
                _ => self.database.write_snapshot(capture.id.clone(), begin, emit).await,
            }
        };
        let consume = seal_frames(writer, header_receiver, receiver, &path, key, member);
        let (produced, consumed) = tokio::join!(produce, consume);
        let breaking_version = match produced {
            Err(coven_database::SnapshotWriteError::Database(error)) => return Err(error.into()),
            Err(coven_database::SnapshotWriteError::Format(error)) => return Err(error.into()),
            Err(coven_database::SnapshotWriteError::Output(error)) => {
                consumed?;
                return Err(SyncError::disk(error));
            }
            Ok(breaking_version) => breaking_version,
        };
        let (covers, digest, size) = consumed?;
        Ok(Some(SnapshotArtifact {
            capture: capture.clone(),
            covers,
            breaking_version,
            digest,
            size,
            session: None,
        }))
    }
}

#[expect(
    clippy::map_err_ignore,
    reason = "a header channel closed before its header leaves the snapshot truncated"
)]
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
    let (mut sealer, start) =
        crate::sealed_stream::StreamSealer::start(path.clone(), prefix, Some(signature), key);
    writer.append(&start).await?;
    let mut chunk = zeroize::Zeroizing::new(Vec::with_capacity(coven_format::chunks::CHUNK_SIZE));
    while let Some(frame) = receiver.recv().await {
        let mut remaining = frame.as_slice();
        while !remaining.is_empty() {
            let length = remaining.len().min(coven_format::chunks::CHUNK_SIZE - chunk.len());
            chunk.extend_from_slice(&remaining[..length]);
            remaining = &remaining[length..];
            if chunk.len() == coven_format::chunks::CHUNK_SIZE {
                let piece = sealer.chunk(&chunk)?;
                writer.append(&piece).await?;
                chunk.clear();
            }
        }
    }
    if !chunk.is_empty() {
        let piece = sealer.chunk(&chunk)?;
        writer.append(&piece).await?;
    }
    let (signature, digest, size) = sealer.finish(member);
    writer.append(signature.as_bytes()).await?;
    writer.finish().await?;
    Ok((header.covers, *digest.as_bytes(), size))
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotRequest {
    #[serde(with = "crate::pending_encoding::audience")]
    pub(crate) audience: Audience,
    #[serde(with = "purpose")]
    pub(crate) purpose: Purpose,
}
