//! The host-owned staging batch every exact CloudKit create commits through.
//!
//! Creating an exact object is one atomic zone modification: the parts and the
//! manifest are staged locally, then committed together. The guard below owns
//! that batch so a cancelled or failed create discards its staging instead of
//! leaving it behind, and names the discard failure when the discard itself
//! fails.

use super::*;

pub(crate) struct CloudKitStagingCleanup {
    ops: Arc<dyn CloudKitOps>,
    scope: CloudKitScope,
    batch: CloudKitAtomicCreateBatch,
    armed: std::sync::atomic::AtomicBool,
}

impl CloudKitStagingCleanup {
    pub(crate) fn new(
        ops: Arc<dyn CloudKitOps>,
        scope: CloudKitScope,
        batch: CloudKitAtomicCreateBatch,
    ) -> Self {
        Self {
            ops,
            scope,
            batch,
            armed: std::sync::atomic::AtomicBool::new(true),
        }
    }

    pub(crate) fn disarm(&self) {
        self.armed.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn cleanup_failure(&self, operation: CloudHomeError) -> CloudHomeError {
        self.disarm();
        match self.ops.discard_atomic_create(&self.scope, &self.batch) {
            Ok(()) => operation,
            Err(cleanup) => CloudHomeError::CleanupFailed {
                operation: Box::new(operation),
                cleanup: Box::new(CloudHomeError::Transport(format!(
                    "discard CloudKit atomic-create batch {:?}: {cleanup}",
                    self.batch.as_provider()
                ))),
            },
        }
    }

    pub(crate) async fn stage_record(
        self: Arc<Self>,
        record: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError> {
        if record.data.len() > CHUNK_SIZE {
            return Err(CloudHomeError::Configuration(format!(
                "CloudKit staged record {:?} has {} bytes, above the {CHUNK_SIZE}-byte bound",
                record.key,
                record.data.len()
            )));
        }
        tokio::task::spawn_blocking(move || {
            self.ops
                .stage_atomic_create_record(&self.scope, &self.batch, record)
        })
        .await
        .map_err(|error| {
            CloudHomeError::transport("run CloudKit atomic-create staging task", error)
        })?
    }

    pub(crate) async fn commit(
        self: Arc<Self>,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        tokio::task::spawn_blocking(move || {
            let created = self.ops.commit_atomic_create(&self.scope, &self.batch)?;
            self.disarm();
            Ok(created)
        })
        .await
        .map_err(|error| {
            CloudHomeError::transport("run CloudKit atomic-create commit task", error)
        })?
    }
}

impl Drop for CloudKitStagingCleanup {
    fn drop(&mut self) {
        if !*self.armed.get_mut() {
            return;
        }
        if let Err(error) = self.ops.discard_atomic_create(&self.scope, &self.batch) {
            tracing::error!(
                batch = self.batch.as_provider(),
                %error,
                "CloudKit cancellation failed to discard atomic-create batch"
            );
        }
    }
}

pub(crate) enum AtomicCreateReadback {
    Created,
    Absent,
}
