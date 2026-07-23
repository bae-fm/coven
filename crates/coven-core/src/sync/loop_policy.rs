//! Shared sync-loop policy.
//!
//! A cycle resets or increments the failure count, surfaces integrity / schema /
//! asset alerts, and chooses an immediate, idle, or backoff wait.

use crate::changeset::RowChange;

use super::cloud_storage::RotationPending;
use super::cycle::SyncCycleResult;
use super::status::DeviceActivity;
use super::store::HeldStorePosition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopWait {
    Immediate,
    Idle,
    BackoffSecs(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLoopAlerts {
    /// This device has not adopted a store-key rotation the cloud has already
    /// committed. While set, this cycle sealed nothing new for the cloud — a
    /// confidentiality invariant, so it takes priority over every other alert
    /// below.
    pub rotation_pending: Option<RotationPending>,
    /// Changesets held after a validation/apply failure, with per-changeset
    /// detail (device, seq, reason) — so a host can name which are stalled.
    pub held_positions: Vec<HeldStorePosition>,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
}

impl SyncLoopAlerts {
    pub fn primary_message(&self) -> Option<String> {
        if let Some(pending) = &self.rotation_pending {
            Some(format!(
                "Sync is paused: store-key rotation work is incomplete ({:?}) while this device \
                 is on generation {}. Retry the membership operation or reconnect with key custody.",
                pending.state, pending.live_generation,
            ))
        } else if !self.held_positions.is_empty() {
            Some(format!(
                "Store object {}/{} is held: {:?}",
                self.held_positions[0].coordinate.device_id(),
                self.held_positions[0].coordinate.seq(),
                self.held_positions[0].reason,
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
    /// [`StorePullResult::row_changes`]: crate::sync::store::StorePullResult::row_changes
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
                held_positions: result.held_positions,
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

    use crate::sync::causal_grants::AuthorStreamId;
    use crate::sync::storage::ExactObjectRef;
    use crate::sync::store::{HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason};
    use crate::sync::store_commit::{ObjectHash, StoreBatchCommitRef, StoreCommitCoord};

    fn held(n: usize) -> Vec<HeldStorePosition> {
        (0..n)
            .map(|i| HeldStorePosition {
                coordinate: HeldStoreCoordinate::Commit {
                    device_id: format!("dev-{i}"),
                    commit: StoreBatchCommitRef {
                        coord: StoreCommitCoord {
                            stream_id: AuthorStreamId::from_digest(ObjectHash::digest(
                                format!("stream-{i}").as_bytes(),
                            )),
                            sequence: i as u64 + 1,
                        },
                        commit_hash: ObjectHash::digest(format!("commit-{i}").as_bytes()),
                        object: ExactObjectRef::new(
                            crate::storage::cloud::ObjectSlot::logical(format!("test-commit-{i}"))
                                .expect("test commit slot"),
                            0,
                            ObjectHash::digest(&[]),
                        ),
                    },
                },
                reason: HeldStorePositionReason::InvalidChangeset("boom".to_string()),
            })
            .collect()
    }

    fn device_activity(n: usize) -> Vec<DeviceActivity> {
        (0..n)
            .map(|i| DeviceActivity {
                device_id: format!("dev-{i}"),
                author: format!("author-{i}"),
                last_seq: i as u64,
                last_sync: Some("2026-07-03T00:00:00Z".to_string()),
            })
            .collect()
    }

    fn cycle_result() -> SyncCycleResult {
        SyncCycleResult {
            changesets_applied: 0,
            held_positions: Vec::new(),
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
        result.held_positions = held(3);
        result.asset_downloads_failed = true;

        let decision = after_success(result);

        match decision.report {
            SyncLoopReport::Success(success) => {
                // The per-device detail reaches the report, not just a count.
                assert_eq!(success.device_activity.len(), 2);
                assert_eq!(success.device_activity[0].author, "author-0");
                assert_eq!(success.device_count, 3);
                // Every warning category reaches the report's alerts.
                assert!(success.alerts.asset_downloads_failed);
                // Held changesets travel with device/seq/reason, not a bare count.
                assert_eq!(success.alerts.held_positions.len(), 3);
                assert_eq!(
                    success.alerts.held_positions[0].coordinate.device_id(),
                    "dev-0"
                );
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
    fn alert_message_priority_matches_sync_status() {
        let alerts = SyncLoopAlerts {
            rotation_pending: None,
            held_positions: held(4),
            asset_downloads_failed: true,
            local_blob_cleanup_pending: true,
        };

        assert_eq!(
            alerts.primary_message().as_deref(),
            Some("Store object dev-0/1 is held: InvalidChangeset(\"boom\")"),
        );
    }

    #[test]
    fn rotation_pending_alert_takes_priority_over_every_other_alert() {
        let alerts = SyncLoopAlerts {
            rotation_pending: Some(RotationPending {
                state: crate::sync::cloud_storage::RotationPendingState::LocalCommitted {
                    generation: 2,
                },
                live_generation: 1,
            }),
            held_positions: held(4),
            asset_downloads_failed: true,
            local_blob_cleanup_pending: true,
        };

        let message = alerts.primary_message().expect("rotation pending alert");
        assert!(
            message.contains("generation: 2") && message.contains("generation 1"),
            "message names both generations: {message}",
        );
    }

    #[test]
    fn constraint_conflict_alert_is_reported() {
        let alerts = SyncLoopAlerts {
            rotation_pending: None,
            held_positions: Vec::new(),
            asset_downloads_failed: false,
            local_blob_cleanup_pending: false,
        };

        assert_eq!(alerts.primary_message(), None);
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
