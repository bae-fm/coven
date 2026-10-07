//! Snapshot frames cross a bounded channel before their sealed bytes reach disk.

use super::{io, StoreLogSync};
use crate::SyncError;
use coven_crypto::DerivedKeys;
use coven_format::{
    sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix},
    snapshot::SnapshotDecoder,
    store_log::SnapshotId,
};
use coven_foundation::{files::FileWriter, id_source::KeyId};
use coven_storage::ObjectPath;
use tokio::sync::mpsc;

impl StoreLogSync {
    pub(super) async fn seal_snapshot(
        &self,
        id: SnapshotId,
        key_id: KeyId,
        key: DerivedKeys,
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
        let produce = self.database.write_snapshot(id, move |frame| {
            send.blocking_send(frame)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        });
        let consume = seal_frames(writer, receive, path, key_id, key);
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
    mut receive: mpsc::Receiver<Vec<u8>>,
    path: &ObjectPath,
    key_id: KeyId,
    key: DerivedKeys,
) -> Result<(), SyncError> {
    let first = receive.recv().await.ok_or(coven_format::Error::Truncated)?;
    let decoder = SnapshotDecoder::start(&first)?;
    let header = decoder.header();
    let prefix = SnapshotObjectPrefix {
        audience: header.id.audience.clone(),
        key: key_id,
        writes: header.writes.clone(),
        store_log: header.store_log.clone(),
    }
    .encode()?;
    writer.append(&prefix).await?;
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
                writer.append(&layout.encode_chunk(&sealed)?).await?;
                chunk.clear();
            }
        }
        frame = receive.recv().await;
    }
    if !chunk.is_empty() {
        let sealed = key.seal_object_chunk(path.as_str(), &prefix, 0, layout.index(), &chunk)?;
        writer.append(&layout.encode_chunk(&sealed)?).await?;
    }
    writer.finish().await?;
    Ok(())
}
