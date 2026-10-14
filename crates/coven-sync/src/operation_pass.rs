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
            self.sync.prerequisites_changed();
            self.drive_pass().await?;
        }
        let mut discovery = self.writes.download_writes().await?;
        if discovery.needs_snapshots {
            self.sync.discover_snapshots().await?;
            discovery = self.writes.download_writes().await?;
        }
        self.files.sync_files().await?;
        self.sync.write_snapshots().await?;
        self.sync.run_retention().await?;
        self.sync
            .finish_pass(&mut self.writes, discovery.complete, checkpoint)
            .await
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
