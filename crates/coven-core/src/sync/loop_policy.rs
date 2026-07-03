//! Shared sync-loop policy.
//!
//! Native and wasm loops have different wait primitives, but the decision after a
//! cycle is the same: reset or increment the failure count, surface integrity /
//! schema / asset alerts, and choose immediate, idle, or backoff wait.

use crate::changeset::RowChange;

use super::cycle::SyncCycleResult;

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
    pub skipped_schema: u64,
    pub rejected_unauthorized: u64,
    pub invalid_signatures: u64,
    pub asset_downloads_failed: bool,
}

impl SyncLoopAlerts {
    pub fn primary_message(&self) -> Option<String> {
        if self.skipped_schema > 0 {
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
                "{} changes with an invalid signature were skipped.",
                self.invalid_signatures,
            ))
        } else if self.asset_downloads_failed {
            Some("Some files failed to download, will retry".to_string())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncLoopSuccess {
    pub last_sync_time: String,
    pub device_count: u32,
    pub data_changed: bool,
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
            device_count: (result.other_device_count + 1) as u32,
            data_changed,
            row_changes,
            alerts: SyncLoopAlerts {
                skipped_schema: result.skipped_schema,
                rejected_unauthorized: result.rejected_unauthorized,
                invalid_signatures: result.invalid_signatures,
                asset_downloads_failed: result.asset_downloads_failed,
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

    fn cycle_result() -> SyncCycleResult {
        SyncCycleResult {
            changesets_applied: 0,
            skipped_schema: 0,
            rejected_unauthorized: 0,
            invalid_signatures: 0,
            other_device_count: 2,
            sync_time: "2026-07-03T00:00:00Z".to_string(),
            asset_downloads_failed: false,
            row_changes: vec![],
            resume_drain_promptly: false,
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
            skipped_schema: 1,
            rejected_unauthorized: 2,
            invalid_signatures: 3,
            asset_downloads_failed: true,
        };

        assert_eq!(
            alerts.primary_message().as_deref(),
            Some("1 changes from a newer app version were skipped. Update the app to apply them."),
        );
    }
}
