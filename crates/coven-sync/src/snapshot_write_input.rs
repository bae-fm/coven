//! Keep shared device-log downloads on disk until an atomic snapshot reload.

use super::{io, StoreLogSync};
use crate::{
    replay_cache::ReplayCache,
    snapshot_data::{SavedWrite, SnapshotTask},
    SyncError,
};
use coven_database::{OperationRecord, StoreLog};
use coven_format::{
    write::WriteHeader,
    write_stream::{PartDecoder, WriteHeaderFrame},
};
use coven_foundation::files::FileWriter;
use coven_merge::Audience;
use coven_storage::StoredObject;
use std::collections::BTreeSet;

impl StoreLogSync {
    pub(super) async fn open_write_header(
        &self,
        object: &StoredObject,
    ) -> Result<WriteHeaderFrame, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.unlock()?;
        Ok(crate::write_object::open(storage, object, ring.as_ref())
            .await?
            .header)
    }

    pub(super) async fn open_snapshot_write(
        &self,
        object: &StoredObject,
        log: &StoreLog,
        replays: &mut ReplayCache<'_>,
        readable: &BTreeSet<Audience>,
        record: &OperationRecord,
        task: &mut SnapshotTask,
    ) -> Result<SavedWrite, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let ring = self.store_keys.unlock()?;
        let opened = crate::write_object::open(storage, object, ring.as_ref()).await?;
        let ring = ring.as_ref().expect("opened header has its store key");
        let missing: Vec<_> = opened
            .header
            .header
            .store_log_read
            .0
            .iter()
            .copied()
            .filter(|id| !log.replay.entries.contains_key(id))
            .collect();
        if !missing.is_empty() {
            return Err(coven_database::DbError::Snapshot(
                coven_database::SnapshotError::WriteWaiting(coven_database::WriteWait::StoreLog(
                    missing,
                )),
            )
            .into());
        }
        let author = crate::write_object::authority(log, replays, &opened.header.header)
            .map_err(|failure| crate::write_object::damaged(&object.path, failure))?;
        let eligible = crate::write_object::parts(
            &opened,
            ring,
            log,
            replays,
            &self.operation_member()?.member_id(),
        )?;
        let mut files = Vec::new();
        let mut targets = Vec::new();
        for (part, eligible) in opened.header.parts.iter().zip(eligible) {
            if eligible && readable.contains(&part.audience) {
                let name = self.reserve_snapshot_file(record, task).await?;
                let writer = self
                    .directory
                    .file(
                        coven_foundation::files::FileArea::AppProvided,
                        &io::name(&name)?,
                    )
                    .create_writer(self.directory.lock_read_only()?)?;
                files.push(Some(name));
                targets.push(Some((writer, PartDecoder::new(part.clone())?)));
            } else {
                files.push(None);
                targets.push(None);
            }
        }
        let header = opened.header.encode()?;
        let mut sink = SnapshotWriteParts {
            header: opened.header.header.clone(),
            targets,
        };
        crate::write_object::finish(storage, object, ring, &author, opened, &mut sink).await?;
        Ok(SavedWrite {
            header,
            parts: files,
        })
    }
}

struct SnapshotWriteParts {
    header: WriteHeader,
    targets: Vec<Option<(FileWriter, PartDecoder)>>,
}
impl crate::write_object::PartSink for SnapshotWriteParts {
    fn opens(&self, part: usize) -> bool {
        self.targets[part].is_some()
    }
    async fn chunk(&mut self, part: usize, bytes: Vec<u8>) -> Result<(), SyncError> {
        let (writer, decoder) = self.targets[part].as_mut().expect("opened part");
        for frame in decoder.chunk(&bytes)? {
            if let coven_format::dismissal::WriteFrame::Dismissal(dismissal) = frame {
                dismissal.validate_past(&self.header)?;
            }
        }
        writer.append(&bytes).await?;
        Ok(())
    }
    async fn end(&mut self, part: usize) -> Result<(), SyncError> {
        if let Some((writer, decoder)) = self.targets[part].take() {
            decoder.finish()?;
            writer.finish().await?;
        }
        Ok(())
    }
}
