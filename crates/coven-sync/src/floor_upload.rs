//! A floor's body and pointer bytes survive every interrupted part.
use super::*;
use crate::floor_object::FloorObject;
use crate::recorded_upload::{self, UploadSource, Uploaded};
use coven_foundation::files::StoreDirAccess;
use coven_storage::UploadSession;

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
    /// Upload the body at the device's own numbered name, then replace the
    /// pointer once, conditional on the pointer observed (§16.6). A body that
    /// is already stored with these bytes, after a lost reply, counts as
    /// uploaded.
    pub(super) async fn upload_floor(
        &self,
        record: &mut OperationRecord,
        artifact: &mut FloorArtifact,
    ) -> Result<(), SyncError> {
        let body = artifact.capture.body_path(self.store(), self.device);
        let lock = self.directory.lock_read_only()?;
        let reader = self.artifact_file(&artifact.capture.name)?.open_reader()?;
        let total = artifact.size;
        let recorded = artifact.session.clone();
        let mut source = FloorUploadSource { sync: self, record, artifact, reader, _lock: lock };
        match recorded_upload::upload(self.storage.as_ref(), &body, total, recorded, &mut source)
            .await?
        {
            Uploaded::Stored(_) => (),
            Uploaded::Paused => unreachable!("retention owns its active request"),
            Uploaded::Occupied(_) => {
                // Only this device writes this name, and never twice, so what
                // is there is this write's body, stored before a reply was
                // lost. A reader checks its digest against the pointer's.
                let stored = self.storage.status(&body).await?;
                if stored.is_none_or(|stored| stored.size != total) {
                    return Err(crate::write_object::wrong_identity(&body));
                }
            }
        }
        let pointer = &artifact.pointer;
        match &artifact.capture.previous {
            Some(previous) => self.storage.replace(previous, pointer).await?,
            None => {
                self.storage.create(&ObjectPath::floor(self.store()), pointer).await?;
            }
        }
        Ok(())
    }

    /// Drop this write's local bytes and its upload session, and delete its
    /// body unless the floor now names it: a pointer moves forward only, so a
    /// body it doesn't name never becomes the floor again (§16.6).
    pub(super) async fn cleanup_floor(
        &self,
        artifact: &FloorArtifact,
        floor: Option<&FloorObject>,
    ) -> Result<(), SyncError> {
        if let Some(session) = &artifact.session {
            self.storage.abort_upload(session).await?;
        }
        let named =
            floor.is_some_and(|floor| floor.body() == (self.device, artifact.capture.number));
        if !named {
            self.storage.delete(&artifact.capture.body_path(self.store(), self.device)).await?;
        }
        self.artifact_file(&artifact.capture.name)?.remove()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "floor_upload_tests.rs"]
mod tests;
