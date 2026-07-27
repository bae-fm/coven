//! When a queued `cloud_outbox` entry is due for another attempt. One policy
//! serves both drains — uploads ([`crate::blob::upload`]) and delete tombstones
//! ([`crate::blob::delete`]) — so a row's retry schedule does not depend on which
//! operation it carries.

use tracing::warn;

use crate::db::OutboxEntry;

/// Minimum delay before a failed outbox entry is retried, keyed on its prior
/// `attempt_count`. Exponential (`30s · 2^(n-1)`) capped at one hour: the base
/// equals the sync-loop interval so the first retry rides the next natural
/// cycle, and the cap keeps a persistently-failing entry retrying hourly rather
/// than every cycle. A freshly-queued entry (`attempt_count == 0`) is eligible
/// immediately.
pub(super) fn backoff_window(attempt_count: i64) -> chrono::Duration {
    if attempt_count <= 0 {
        return chrono::Duration::zero();
    }
    let n = (attempt_count - 1) as u32;
    chrono::Duration::seconds(crate::sync::backoff::backoff_secs(n, 3600) as i64)
}

/// Whether `entry` is still inside its retry backoff window and must be skipped
/// this pass.
///
/// An unparseable `last_attempt_at` is logged and the entry treated as due: the
/// timestamp is local retry bookkeeping, not the deletion or upload intent, so a
/// corrupt one must not decide anything. Treating it as due rewrites it on the
/// next attempt, which clears the corruption through the ordinary path — and,
/// because a drain walks its queue in order, refusing the row instead would
/// strand every entry queued behind it for as long as the corruption lasts.
pub(super) fn entry_in_backoff(entry: &OutboxEntry, now: chrono::DateTime<chrono::Utc>) -> bool {
    let Some(last) = entry.last_attempt_at.as_deref() else {
        return false;
    };
    match chrono::DateTime::parse_from_rfc3339(last) {
        Ok(last_dt) => {
            let elapsed = now.signed_duration_since(last_dt.with_timezone(&chrono::Utc));
            elapsed < backoff_window(entry.attempt_count)
        }
        Err(e) => {
            warn!(
                "Outbox entry {} has unparseable last_attempt_at {last:?}: {e}; retrying",
                entry.id
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn backoff_window_is_exponential_and_capped() {
        assert_eq!(backoff_window(0), Duration::zero());
        assert_eq!(backoff_window(1), Duration::seconds(30));
        assert_eq!(backoff_window(2), Duration::seconds(60));
        assert_eq!(backoff_window(3), Duration::seconds(120));
        assert_eq!(backoff_window(8), Duration::seconds(3600));
        assert_eq!(backoff_window(50), Duration::seconds(3600));
    }
}
