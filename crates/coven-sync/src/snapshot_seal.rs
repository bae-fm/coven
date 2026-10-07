//! Snapshot frames cross a bounded channel before their sealed bytes reach disk.

use super::{io, StoreLogSync};
use crate::SyncError;
use coven_crypto::{DerivedKeys, MemberKeys, ObjectHasher};
use coven_format::{
    sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix},
    store_log::SnapshotId,
};
use coven_foundation::{files::FileWriter, id_source::KeyId};
use coven_storage::ObjectPath;
use tokio::sync::{mpsc, oneshot};

impl StoreLogSync {
    pub(super) async fn seal_snapshot(
        &self,
        id: SnapshotId,
        key_id: KeyId,
        key: DerivedKeys,
        member: &MemberKeys,
        path: &ObjectPath,
        name: &str,
    ) -> Result<(), SyncError> {
        let writer = self
            .directory
            .file(
                coven_foundation::files::FileArea::AppProvided,
                &io::name(name)?,
            )
            .create_writer(self.directory.lock_read_only()?)?;
        let (send, receive) = mpsc::channel(1);
        let (send_prefix, receive_prefix) = oneshot::channel();
        let produce = self.database.write_snapshot(
            id,
            move |header| {
                send_prefix
                    .send(SnapshotObjectPrefix {
                        audience: header.id.audience.clone(),
                        key: key_id,
                        writes: header.writes.clone(),
                        store_log: header.store_log.clone(),
                    })
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            },
            move |frame| {
                send.blocking_send(frame)
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            },
        );
        let consume = seal_frames(writer, receive_prefix, receive, path, key, member);
        let (produced, consumed) = tokio::join!(produce, consume);
        // A failed producer can close before the first frame. A failed consumer
        // instead makes the producer's next send fail with BrokenPipe.
        match produced {
            Err(coven_database::SnapshotWriteError::Database(error)) => Err(error.into()),
            Err(coven_database::SnapshotWriteError::Format(error)) => Err(error.into()),
            Err(coven_database::SnapshotWriteError::Output(error)) => {
                consumed?;
                Err(error.into())
            }
            Ok(()) => consumed,
        }
    }
}

async fn seal_frames(
    mut writer: FileWriter,
    prefix: oneshot::Receiver<SnapshotObjectPrefix>,
    mut receive: mpsc::Receiver<Vec<u8>>,
    path: &ObjectPath,
    key: DerivedKeys,
    member: &MemberKeys,
) -> Result<(), SyncError> {
    let prefix = prefix
        .await
        .map_err(|_| coven_format::Error::Truncated)?
        .encode()?;
    let first = receive.recv().await.ok_or(coven_format::Error::Truncated)?;
    writer.append(&prefix).await?;
    let signature = member.sign_prefix(path.as_str(), &prefix);
    writer.append(signature.as_bytes()).await?;
    let mut hash = ObjectHasher::new();
    hash.update(&prefix);
    hash.update(signature.as_bytes());
    let mut layout = SnapshotObjectLayout::new();
    let mut chunk = Vec::with_capacity(io::BUFFER);
    let mut frame = Some(first);
    while let Some(bytes) = frame {
        let mut remaining = bytes.as_slice();
        while !remaining.is_empty() {
            let length = remaining.len().min(io::BUFFER - chunk.len());
            chunk.extend_from_slice(&remaining[..length]);
            remaining = &remaining[length..];
            if chunk.len() == io::BUFFER {
                let sealed =
                    key.seal_object_chunk(path.as_str(), &prefix, 0, layout.index(), &chunk)?;
                let piece = layout.encode_chunk(&sealed)?;
                hash.update(&piece);
                writer.append(&piece).await?;
                chunk.clear();
            }
        }
        frame = receive.recv().await;
    }
    if !chunk.is_empty() {
        let sealed = key.seal_object_chunk(path.as_str(), &prefix, 0, layout.index(), &chunk)?;
        let piece = layout.encode_chunk(&sealed)?;
        hash.update(&piece);
        writer.append(&piece).await?;
    }
    writer
        .append(member.sign_object(path.as_str(), &hash.finish()).as_bytes())
        .await?;
    writer.finish().await?;
    Ok(())
}
