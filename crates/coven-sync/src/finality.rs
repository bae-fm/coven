//! The strict, monotonic store-log finality horizon (§9, Appendix C10).
//!
//! Publication times establish finality only because store-log entries are
//! bounded single-chunk objects published in one request. Multipart initiation
//! times cannot supply this evidence.

use crate::replay::ENTRY_WINDOW;
use coven_database::StoreLog;
use coven_foundation::clock::Clock;
use futures_util::FutureExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The shared provider listing guarantee (§4); independent of timestamp precision.
pub(crate) const LISTING_MARGIN: Duration = Duration::from_secs(60 * 60);

/// Observations survive passes in the sync owner. A timer only schedules a
/// provider check; its completion never certifies storage age.
#[derive(Default)]
pub(crate) struct Observation {
    sample: Option<(SystemTime, Duration)>,
    /// The sample known before the reads of the latest complete catch-up.
    caught_up: Option<SystemTime>,
    retry: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
}

impl Observation {
    /// The newest provider time known now, at its full precision.
    pub(crate) fn sample_time(&self) -> Option<SystemTime> {
        self.sample.map(|(time, _)| time)
    }

    /// Record the sample taken before a catch-up that has now completed. An
    /// older sample is conservative for snapshot admission, so a catch-up
    /// that started without one keeps the preceding value.
    pub(crate) fn complete_catch_up(&mut self, sample: Option<SystemTime>) {
        if sample.is_some() {
            self.caught_up = sample;
        }
    }

    /// The `T_c` a snapshot prepared now records (§15): never device time.
    pub(crate) fn caught_up(&self) -> Option<SystemTime> {
        self.caught_up
    }

    pub(crate) fn through(&self) -> Option<(Duration, Duration)> {
        self.sample
            .and_then(|(time, unit)| observation_time(time, unit).map(|through| (through, unit)))
    }

    pub(crate) fn observe(&mut self, object: &coven_storage::StoredObject) {
        if self.sample.is_none_or(|(time, _)| object.stored_at > time) {
            self.sample = Some((object.stored_at, object.time_resolution));
        }
    }

    pub(crate) fn probe_due(&mut self, log: &StoreLog, clock: &dyn Clock) -> bool {
        if self
            .retry
            .as_mut()
            .is_some_and(|wait| wait.as_mut().now_or_never().is_none())
        {
            return false;
        }
        self.retry = None;
        let now = clock.now();
        let pending = log
            .entries
            .iter()
            .filter(|entry| !log.is_final(entry.entry.position))
            .map(|entry| entry.stored_at)
            .min();
        let last_late = late_times(log).max();
        // Before the first sample, a probe must learn the provider's unit.
        // Once known, its extra tick also belongs to the scheduling threshold.
        let wait = match self.sample {
            Some((_, unit)) => ENTRY_WINDOW + LISTING_MARGIN + unit,
            None => ENTRY_WINDOW + LISTING_MARGIN,
        };
        let due = pending.is_some_and(|pending| {
            let threshold = last_late.map_or(pending, |late| late.max(pending));
            now.duration_since(threshold).is_ok_and(|age| age > wait)
        });
        if due {
            self.retry = Some(clock.sleep(Duration::from_secs(60)));
        }
        due
    }
}

/// The inclusive complete endpoint precedes the listing guarantee by one tick:
/// T′ = T − one hour − δ, for every provider.
/// Underflow supplies no observation; it must never certify the clock's own tick.
pub(crate) fn observation_time(time: SystemTime, resolution: Duration) -> Option<Duration> {
    assert!(
        !resolution.is_zero(),
        "provider timestamp resolution must be positive"
    );
    time.duration_since(UNIX_EPOCH)
        .ok()?
        .checked_sub(LISTING_MARGIN)?
        .checked_sub(resolution)
}

/// Recover the greatest qualifying quiet window through a completed endpoint.
/// `log` must contain and have judged every publication through `through`.
/// Publications after that endpoint do not belong to its recent window.
pub(crate) fn horizon(log: &StoreLog, through: Duration, resolution: Duration) -> Option<Duration> {
    let Some(endpoint) = quiet_endpoint(log, through, resolution) else {
        return log.horizon;
    };
    let cutoff = endpoint.saturating_sub(ENTRY_WINDOW);
    Some(log.horizon.map_or(cutoff, |previous| previous.max(cutoff)))
}

/// Greatest quiet endpoint through the completed prefix, on provider ticks.
pub(crate) fn quiet_endpoint(
    log: &StoreLog,
    through: Duration,
    resolution: Duration,
) -> Option<Duration> {
    assert!(
        !resolution.is_zero(),
        "provider timestamp resolution must be positive"
    );
    let mut late: Vec<_> = late_times(log)
        .map(|time| {
            time.duration_since(UNIX_EPOCH)
                .expect("nonnegative provider time")
        })
        .filter(|time| *time <= through)
        .collect();
    late.sort_unstable();
    late.dedup();
    let mut endpoint = through;
    for time in late.into_iter().rev() {
        if time > endpoint {
            continue;
        }
        if endpoint - time > ENTRY_WINDOW {
            break;
        }
        // Both ends belong to the recent interval. Recover only provider ticks,
        // using the same resolution as the scan's preceding clock sample.
        endpoint = time.checked_sub(resolution)?;
    }
    Some(endpoint)
}

fn late_times(log: &StoreLog) -> impl Iterator<Item = SystemTime> + '_ {
    log.entries.iter().filter_map(|entry| {
        log.entries
            .iter()
            .any(|prior| {
                prior.stored_at < entry.stored_at
                    && !crate::replay::had_read(&entry.entry, &prior.entry)
            })
            .then_some(entry.stored_at)
    })
}

#[cfg(test)]
#[path = "finality_tests.rs"]
mod tests;
