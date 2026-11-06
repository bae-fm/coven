//! Fixed snapshot bytes and each provider-confirmed upload session share a journal.

use crate::sequence_sync::OperationState;
use crate::{
    recorded_upload::{self, UploadSource, Uploaded},
    sequence_sync::sequence_operations::{
        OperationProgress, OperationStep, Request, SequenceOperation,
    },
    sequence_sync::snapshot_capture::SnapshotArtifact,
    sequence_sync::SequenceSync,
    SyncError,
};
use coven_database::OperationRecord;
use coven_foundation::files::StoreDirAccess;
use coven_storage::UploadSession;

struct SnapshotUploadSource<'a> {
    sync: &'a SequenceSync,
    record: OperationRecord,
    request: Request,
    artifact: SnapshotArtifact,
    reader: coven_foundation::files::FileReader,
    _lock: coven_foundation::files::StoreReadLock,
}
impl UploadSource for SnapshotUploadSource<'_> {
    type Error = SyncError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, SyncError> {
        Ok(self.reader.read_at(offset, length)?)
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), SyncError> {
        self.artifact.session = Some(session);
        let work = SequenceOperation {
            request: self.request.clone(),
            progress: OperationProgress::Running(OperationStep::Uploading(self.artifact.clone())),
        };
        let update = work.update(&self.record)?;
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
    pub(crate) async fn upload_snapshot(
        &self,
        record: OperationRecord,
        request: Request,
        artifact: SnapshotArtifact,
    ) -> Result<(), SyncError> {
        let path = crate::snapshot_object::path(self.store(), &artifact.capture.id)?;
        let lock = self.directory.lock_read_only()?;
        let reader = self.artifact_file(&artifact.capture.name)?.open_reader()?;
        let mut source =
            SnapshotUploadSource { sync: self, record, request, artifact, reader, _lock: lock };
        let total = source.artifact.size;
        let recorded = source.artifact.session.clone();
        let publication = match recorded_upload::create(
            self.storage.as_ref(),
            &path,
            total,
            recorded,
            &mut source,
        )
        .await?
        {
            Uploaded::Stored(object) => object,
            Uploaded::Occupied(expected) => {
                crate::identity::settle_occupied(self.storage.as_ref(), &path, expected).await?;
                None
            }
            Uploaded::Paused => unreachable!("an operation owns its active request"),
        };
        let object = match publication {
            Some(object) => object,
            None => self.immutable_metadata(&path).await?,
        };
        crate::object_input::ObjectInput::observe(&self.database, &object).await?;
        self.database
            .advance_operation(
                SequenceOperation {
                    request: source.request,
                    progress: OperationProgress::Running(OperationStep::Announcing(
                        source.artifact,
                    )),
                }
                .update(&source.record)?,
            )
            .await?;
        Ok(())
    }
}
