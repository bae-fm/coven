//! One sync pass in its three phases (§6, SP).

use super::OperationRun;
use crate::SyncError;

impl OperationRun {
    /// One pass in SP order: catch up on membership, make keys and access
    /// match it, then sync data. Each step runs once; operations reuse the
    /// first phase's observations rather than reading storage again.
    pub(super) async fn pass_steps(&mut self) -> Result<(), SyncError> {
        let _pass = self.sync.begin_pass(&mut self.writes);
        // Phase 1 (SP steps 1–4): read-only.
        let caught_up = self.sync.catch_up_membership().await?;
        self.writes.observe_positions().await?;
        // Phase 2: replace exposed keys and share historical ones before any
        // first attempt (§11), then resume due operations (SP step 5): those
        // whose retry delay elapsed, and all waiting ones if this pass
        // received or published anything they may need (SP waiting).
        if caught_up {
            self.sync.match_keys().await?;
            self.sync.settle_queued_entry().await?;
            self.sync.sweep_stray_access_keys().await?;
        }
        self.drive_key_phase().await?;
        // Phase 3. A required reload holds data application and first
        // attempts; work independent of it still runs and reports travel.
        if let Some(id) = self.sync.pending_reload().await? {
            self.files.sync_files().await?;
            self.writes.post_positions().await?;
            return Err(SyncError::ReloadPending(id));
        }
        self.sync.prepare_return().await?;
        let checkpoint = self.sync.return_sample();
        // An incomplete first phase permits only retries of attempted writes.
        // Operations waiting on own uploads resume once, without another pass.
        let uploaded = if caught_up {
            self.writes.upload_writes().await?
        } else {
            self.writes.retry_attempted_writes().await?
        };
        if !uploaded.is_empty() {
            self.drive_pass().await?;
        }
        let discovery = self.sync.catch_up_writes(&mut self.writes).await?;
        self.files.sync_files().await?;
        // A reset's snapshot is prepared here, after the catch-up; its entry
        // follows in step 5's request order without another pass.
        if self.sync.write_snapshots().await? {
            self.drive_pass().await?;
        }
        // A failed retention deletion is its own record and does not keep the
        // pass from step 9 (§15); it is reported once positions are posted.
        let retained = self.sync.run_retention().await;
        self.sync
            .finish_pass(&mut self.writes, discovery.complete, checkpoint)
            .await?;
        retained
    }

    /// Outside a pass, run the catch-up a waiting snapshot preparation
    /// follows, keeping its scope for the drive (§15). A failed catch-up is
    /// each waiting preparation's own condition, and an app call waiting on
    /// one receives it.
    pub(super) async fn catch_up_for_preparation(
        &mut self,
    ) -> Result<Option<crate::pass_reads::ReadScope>, SyncError> {
        match self.sync.catch_up_outside_pass(&mut self.writes).await? {
            Ok(scope) => Ok(scope),
            Err((error, waiting)) => {
                let mut error = Some(error);
                for (id, pending) in waiting {
                    if let Some(reply) = self.waiters.remove(&id) {
                        let _ = reply.send(Err(error
                            .take()
                            .unwrap_or_else(|| SyncError::Pending(Box::new(pending)))));
                    }
                }
                Ok(None)
            }
        }
    }

    /// SP step 5's due operations. A local entry that changes membership
    /// re-evaluates key eligibility before data.
    async fn drive_key_phase(&mut self) -> Result<(), SyncError> {
        self.sync.rotate_exposed_keys().await?;
        loop {
            self.drive_pass().await?;
            if !self.sync.rotate_exposed_keys().await? {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
#[path = "operation_pass_tests.rs"]
mod tests;
