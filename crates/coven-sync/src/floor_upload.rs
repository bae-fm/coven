//! A floor's bytes and original publication condition survive every interrupted part.
use super::*;
use crate::recorded_upload::{self, UploadSource, Uploaded};
use coven_foundation::files::StoreDirAccess;
use coven_storage::{UploadSession, UploadTarget};

struct FloorUploadSource<'a> {
    sync: &'a SequenceSync,
    record: &'a mut OperationRecord,
    artifact: &'a mut FloorArtifact,
    reader: coven_foundation::files::FileReader,
    _lock: coven_foundation::files::StoreReadLock,
}
impl UploadSource for FloorUploadSource<'_> {
    type Error = SyncError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, SyncError> {
        Ok(self.reader.read_at(offset, length)?)
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), SyncError> {
        self.artifact.session = Some(session);
        let update = Retention::Floor(self.artifact.clone()).update(self.record)?;
        let next = update.last_step;
        self.sync.database.advance_operation(update).await?;
        self.record.last_step = next;
        Ok(())
    }
    fn keep_going(&self) -> bool {
        true
    }
}
impl SequenceSync {
    pub(super) async fn upload_floor(
        &self,
        record: &mut OperationRecord,
        artifact: &mut FloorArtifact,
    ) -> Result<(), SyncError> {
        let target = match &artifact.capture.previous {
            Some(previous) => UploadTarget::Replace(previous.clone()),
            None => UploadTarget::Create(ObjectPath::floor(self.store())),
        };
        let lock = self.directory.lock_read_only()?;
        let reader = self.artifact_file(&artifact.capture.name)?.open_reader()?;
        let total = artifact.size;
        let recorded = artifact.session.clone();
        let mut source = FloorUploadSource { sync: self, record, artifact, reader, _lock: lock };
        match recorded_upload::upload(self.storage.as_ref(), &target, total, recorded, &mut source)
            .await?
        {
            Uploaded::Stored => Ok(()),
            Uploaded::Paused => unreachable!("retention owns its active request"),
            Uploaded::Occupied(_) => {
                unreachable!("a mutable floor is settled by authenticated reading")
            }
        }
    }
    pub(super) async fn cleanup_floor(&self, artifact: &FloorArtifact) -> Result<(), SyncError> {
        if let Some(session) = &artifact.session {
            self.storage.abort_upload(session).await?;
        }
        self.artifact_file(&artifact.capture.name)?.remove()?;
        Ok(())
    }
}
