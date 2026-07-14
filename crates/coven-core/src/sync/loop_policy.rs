//! Shared sync-loop policy.
//!
//! Native and wasm loops have different wait primitives, but the decision after a
//! cycle is the same: reset or increment the failure count, surface integrity /
//! schema / asset alerts, and choose immediate, idle, or backoff wait.

use crate::changeset::RowChange;

use super::cloud_storage::RotationPending;
use super::cycle::SyncCycleResult;
use super::pull::HeldChangeset;
use super::status::DeviceActivity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopWait {
    Immediate,
    Idle,
    BackoffSecs(u64),
}

impl LoopWait {
    pub fn as_millis(self, idle_interval_ms: u32) -> u32 {
        match self {
            LoopWait::Immediate => 0,
            LoopWait::Idle => idle_interval_ms,
            LoopWait::BackoffSecs(secs) => secs.saturating_mul(1_000).min(u32::MAX as u64) as u32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLoopAlerts {
    /// This device has not adopted a store-key rotation the cloud has already
    /// committed. While set, this cycle sealed nothing new for the cloud — a
    /// confidentiality invariant, so it takes priority over every other alert
    /// below.
    pub rotation_pending: Option<RotationPending>,
    pub skipped_schema: u64,
    pub rejected_unauthorized: u64,
    pub invalid_signatures: u64,
    /// Changesets held after a validation/apply failure, with per-changeset
    /// detail (device, seq, reason) — so a host can name which are stalled.
    pub held_changesets: Vec<HeldChangeset>,
    pub constraint_conflicts: u64,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
}

impl SyncLoopAlerts {
    pub fn primary_message(&self) -> Option<String> {
        if let Some(pending) = &self.rotation_pending {
            Some(format!(
                "Sync is paused: the store key rotated to generation {}, which this device \
                 has not adopted yet (still on generation {}). Remove the member again, or \
                 wait for the next sync cycle to adopt the key.",
                pending.committed_generation, pending.live_generation,
            ))
        } else if self.skipped_schema > 0 {
            Some(format!(
                "{} changes from a newer app version were skipped. Update the app to apply them.",
                self.skipped_schema,
            ))
        } else if self.rejected_unauthorized > 0 {
            Some(format!(
                "{} changes from an unauthorized device were rejected.",
                self.rejected_unauthorized,
            ))
        } else if self.invalid_signatures > 0 {
            Some(format!(
                "{} changes with an invalid signature are stalled.",
                self.invalid_signatures,
            ))
        } else if !self.held_changesets.is_empty() {
            Some(format!(
                "{} invalid changes are stalled.",
                self.held_changesets.len(),
            ))
        } else if self.constraint_conflicts > 0 {
            let noun = if self.constraint_conflicts == 1 {
                "change"
            } else {
                "changes"
            };
            Some(format!(
                "{} {} hit a local uniqueness or constraint conflict.",
                self.constraint_conflicts, noun,
            ))
        } else if self.asset_downloads_failed {
            Some("Some files failed to download; their changes remain pending.".to_string())
        } else if self.local_blob_cleanup_pending {
            Some("Some obsolete local file copies are still pending cleanup.".to_string())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncLoopSuccess {
    pub last_sync_time: String,
    pub device_count: u32,
    /// Per-device activity of the other devices — id, member key, latest seq,
    /// last-sync time — for a host to render which devices synced and when.
    pub device_activity: Vec<DeviceActivity>,
    pub data_changed: bool,
    /// Row changes from applied changesets, for the host to map to domain events.
    /// `Some` when `data_changed`. A refresh *hint*, not a complete stream: a
    /// lagged subscriber can miss it entirely, and several accepted changesets
    /// can touch the same row, so a host re-reads affected rows by primary key
    /// rather than trusting it as exhaustive.
    ///
    /// [`PullResult::row_changes`]: crate::sync::pull::PullResult::row_changes
    pub row_changes: Option<Vec<RowChange>>,
    pub alerts: SyncLoopAlerts,
}

#[derive(Debug, Clone)]
pub enum SyncLoopReport {
    Success(SyncLoopSuccess),
    Failure(String),
}

#[derive(Debug, Clone)]
pub struct SyncLoopDecision {
    pub consecutive_failures: u32,
    pub wait: LoopWait,
    pub report: SyncLoopReport,
}

pub fn after_success(result: SyncCycleResult) -> SyncLoopDecision {
    let data_changed = result.changesets_applied > 0;
    let row_changes = if data_changed && !result.row_changes.is_empty() {
        Some(result.row_changes)
    } else {
        None
    };

    SyncLoopDecision {
        consecutive_failures: 0,
        wait: if result.resume_drain_promptly {
            LoopWait::Immediate
        } else {
            LoopWait::Idle
        },
        report: SyncLoopReport::Success(SyncLoopSuccess {
            last_sync_time: result.sync_time,
            // This device plus the others its heads reported.
            device_count: (result.device_activity.len() + 1) as u32,
            device_activity: result.device_activity,
            data_changed,
            row_changes,
            alerts: SyncLoopAlerts {
                rotation_pending: result.rotation_pending,
                skipped_schema: result.skipped_schema,
                rejected_unauthorized: result.rejected_unauthorized,
                invalid_signatures: result.invalid_signatures,
                held_changesets: result.held_changesets,
                constraint_conflicts: result.constraint_conflicts,
                asset_downloads_failed: result.asset_downloads_failed,
                local_blob_cleanup_pending: result.local_blob_cleanup_pending,
            },
        }),
    }
}

pub fn after_failure(
    error: String,
    previous_failures: u32,
    backoff_cap_secs: u64,
) -> SyncLoopDecision {
    let consecutive_failures = previous_failures.saturating_add(1);
    SyncLoopDecision {
        consecutive_failures,
        wait: LoopWait::BackoffSecs(super::backoff::backoff_secs(
            consecutive_failures,
            backoff_cap_secs,
        )),
        report: SyncLoopReport::Failure(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sync::pull::HeldChangesetReason;

    fn held(n: usize) -> Vec<HeldChangeset> {
        (0..n)
            .map(|i| HeldChangeset {
                device_id: format!("dev-{i}"),
                seq: i as u64,
                reason: HeldChangesetReason::ApplyFailed {
                    error: "boom".to_string(),
                },
            })
            .collect()
    }

    fn device_activity(n: usize) -> Vec<DeviceActivity> {
        (0..n)
            .map(|i| DeviceActivity {
                device_id: format!("dev-{i}"),
                author: format!("author-{i}"),
                last_seq: i as u64,
                last_sync: "2026-07-03T00:00:00Z".to_string(),
            })
            .collect()
    }

    fn cycle_result() -> SyncCycleResult {
        SyncCycleResult {
            changesets_applied: 0,
            skipped_schema: 0,
            rejected_unauthorized: 0,
            invalid_signatures: 0,
            held_changesets: Vec::new(),
            constraint_conflicts: 0,
            device_activity: device_activity(2),
            sync_time: "2026-07-03T00:00:00Z".to_string(),
            asset_downloads_failed: false,
            local_blob_cleanup_pending: false,
            row_changes: vec![],
            resume_drain_promptly: false,
            rotation_pending: None,
        }
    }

    #[test]
    fn success_resets_failures_and_waits_idle() {
        let decision = after_success(cycle_result());

        assert_eq!(decision.consecutive_failures, 0);
        assert_eq!(decision.wait, LoopWait::Idle);
        match decision.report {
            SyncLoopReport::Success(success) => {
                assert_eq!(success.device_count, 3);
                assert!(!success.data_changed);
                assert!(success.row_changes.is_none());
            }
            SyncLoopReport::Failure(error) => panic!("expected success, got {error}"),
        }
    }

    #[test]
    fn success_carries_device_activity_and_all_alert_categories() {
        let mut result = cycle_result();
        result.device_activity = device_activity(2);
        result.held_changesets = held(3);
        result.skipped_schema = 1;
        result.rejected_unauthorized = 2;
        result.invalid_signatures = 4;
        result.constraint_conflicts = 5;
        result.asset_downloads_failed = true;

        let decision = after_success(result);

        match decision.report {
            SyncLoopReport::Success(success) => {
                // The per-device detail reaches the report, not just a count.
                assert_eq!(success.device_activity.len(), 2);
                assert_eq!(success.device_activity[0].author, "author-0");
                assert_eq!(success.device_count, 3);
                // Every warning category reaches the report's alerts.
                assert_eq!(success.alerts.skipped_schema, 1);
                assert_eq!(success.alerts.rejected_unauthorized, 2);
                assert_eq!(success.alerts.invalid_signatures, 4);
                assert_eq!(success.alerts.constraint_conflicts, 5);
                assert!(success.alerts.asset_downloads_failed);
                // Held changesets travel with device/seq/reason, not a bare count.
                assert_eq!(success.alerts.held_changesets.len(), 3);
                assert_eq!(success.alerts.held_changesets[0].device_id, "dev-0");
            }
            SyncLoopReport::Failure(error) => panic!("expected success, got {error}"),
        }
    }

    #[test]
    fn drain_success_waits_immediately() {
        let mut result = cycle_result();
        result.resume_drain_promptly = true;

        let decision = after_success(result);

        assert_eq!(decision.wait, LoopWait::Immediate);
    }

    #[test]
    fn failure_increments_and_backs_off() {
        let decision = after_failure("network".to_string(), 1, 300);

        assert_eq!(decision.consecutive_failures, 2);
        assert_eq!(decision.wait, LoopWait::BackoffSecs(120));
        match decision.report {
            SyncLoopReport::Failure(error) => assert_eq!(error, "network"),
            SyncLoopReport::Success(_) => panic!("expected failure"),
        }
    }

    #[test]
    fn alert_message_priority_matches_native_status() {
        let alerts = SyncLoopAlerts {
            rotation_pending: None,
            skipped_schema: 1,
            rejected_unauthorized: 2,
            invalid_signatures: 3,
            held_changesets: held(4),
            constraint_conflicts: 5,
            asset_downloads_failed: true,
            local_blob_cleanup_pending: true,
        };

        assert_eq!(
            alerts.primary_message().as_deref(),
            Some("1 changes from a newer app version were skipped. Update the app to apply them."),
        );
    }

    #[test]
    fn rotation_pending_alert_takes_priority_over_every_other_alert() {
        let alerts = SyncLoopAlerts {
            rotation_pending: Some(RotationPending {
                committed_generation: 2,
                live_generation: 1,
            }),
            skipped_schema: 1,
            rejected_unauthorized: 2,
            invalid_signatures: 3,
            held_changesets: held(4),
            constraint_conflicts: 5,
            asset_downloads_failed: true,
            local_blob_cleanup_pending: true,
        };

        let message = alerts.primary_message().expect("rotation pending alert");
        assert!(
            message.contains("generation 2") && message.contains("generation 1"),
            "message names both generations: {message}",
        );
    }

    #[test]
    fn constraint_conflict_alert_is_reported() {
        let alerts = SyncLoopAlerts {
            rotation_pending: None,
            skipped_schema: 0,
            rejected_unauthorized: 0,
            invalid_signatures: 0,
            held_changesets: Vec::new(),
            constraint_conflicts: 1,
            asset_downloads_failed: false,
            local_blob_cleanup_pending: false,
        };

        assert_eq!(
            alerts.primary_message().as_deref(),
            Some("1 change hit a local uniqueness or constraint conflict."),
        );
    }

    #[test]
    fn post_commit_cleanup_has_its_own_alert() {
        let mut result = cycle_result();
        result.local_blob_cleanup_pending = true;

        let decision = after_success(result);

        let SyncLoopReport::Success(success) = decision.report else {
            panic!("expected success report");
        };
        assert_eq!(
            success.alerts.primary_message().as_deref(),
            Some("Some obsolete local file copies are still pending cleanup."),
        );
        assert!(!success.alerts.asset_downloads_failed);
        assert!(success.alerts.local_blob_cleanup_pending);
    }
}
