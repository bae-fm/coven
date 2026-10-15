//! The strict, monotonic store-log finality horizon (§9, Appendix C10).
//!
//! Publication times establish finality only because store-log entries are
//! bounded single-chunk objects published in one request. Multipart initiation
//! times cannot supply this evidence.

use crate::replay::ENTRY_WINDOW;
use coven_database::StoreLog;
use coven_format::value::StorageTime;
use coven_foundation::clock::Clock;
use futures_util::FutureExt;
use std::time::{Duration, UNIX_EPOCH};

/// The shared provider listing guarantee (§4); independent of timestamp precision.
pub(crate) const LISTING_MARGIN: Duration = Duration::from_secs(60 * 60);

/// Observations survive passes in the sync owner. A timer only schedules a
/// provider check; its completion never certifies storage age.
#[derive(Default)]
pub(crate) struct Observation {
    sample: Option<(StorageTime, Duration)>,
    /// The sample known before the reads of the latest complete catch-up.
    caught_up: Option<StorageTime>,
    /// This session's last clock-object time, paired with the sleep-inclusive
    /// monotonic reading taken before its replacement request (§15). Only this
    /// pairing can certify that a write miss is recent.
    clock: Option<(StorageTime, Duration)>,
    /// The sample before this session's last completed snapshot discovery;
    /// later misses are judged against it as against a saved checkpoint.
    discovered: Option<StorageTime>,
    retry: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
}

impl Observation {
    /// The newest provider time known now, at its full precision.
    pub(crate) fn sample_time(&self) -> Option<StorageTime> {
        self.sample.map(|(time, _)| time)
    }

    /// Record the sample taken before a catch-up that has now completed. An
    /// older sample is conservative for snapshot admission, so a catch-up
    /// that started without one keeps the preceding value.
    pub(crate) fn complete_catch_up(&mut self, sample: Option<StorageTime>) {
        if sample.is_some() {
            self.caught_up = sample;
        }
    }

    /// The `T_c` a snapshot prepared now records (§15): never device time.
    pub(crate) fn caught_up(&self) -> Option<StorageTime> {
        self.caught_up
    }

    /// The latest publication time of a write whose every earlier entry by
    /// thirty days that catch-up received: §4's listing guarantee made each
    /// entry stored by `observation_time(T_c)` visible to it (§15).
    pub(crate) fn caught_up_window(&self) -> Option<StorageTime> {
        let (_, unit) = self.sample?;
        observation_time(self.caught_up? + coven_database::WRITE_WINDOW, unit)
    }

    /// The storage time through which the latest complete store-log catch-up
    /// received every stored entry: §4's listing guarantee made each entry
    /// stored by `observation_time(T_c)` visible to it (§9, §15).
    pub(crate) fn entries_received_through(&self) -> Option<StorageTime> {
        let (_, unit) = self.sample?;
        observation_time(self.caught_up?, unit)
    }

    /// Record a clock-object time with the monotonic reading from before its
    /// replacement. A retained sample keeps its original starting point.
    pub(crate) fn clock_observed(&mut self, stored: StorageTime, started: Duration) {
        self.clock = Some((stored, started));
    }

    pub(crate) fn clock(&self) -> Option<(StorageTime, Duration)> {
        self.clock
    }

    /// A completed snapshot discovery certifies misses from its sample on.
    pub(crate) fn snapshots_discovered(&mut self, sample: StorageTime) {
        self.discovered = Some(self.discovered.map_or(sample, |known| known.max(sample)));
    }

    pub(crate) fn discovered(&self) -> Option<StorageTime> {
        self.discovered
    }

    pub(crate) fn through(&self) -> Option<(StorageTime, Duration)> {
        self.sample
            .and_then(|(time, unit)| observation_time(time, unit).map(|through| (through, unit)))
    }

    pub(crate) fn observe(&mut self, object: &coven_storage::StoredObject) {
        if self.sample.is_none_or(|(time, _)| object.stored_at > time) {
            self.sample = Some((object.stored_at, object.time_resolution));
        }
    }

    /// Whether a monotonic schedule says a finality or drain threshold may
    /// have been reached that the known sample cannot decide (SP step 1).
    pub(crate) fn probe_due(
        &mut self,
        log: &StoreLog,
        drains: &[coven_database::RetirementFinality],
        clock: &dyn Clock,
    ) -> bool {
        if self
            .retry
            .as_mut()
            .is_some_and(|wait| wait.as_mut().now_or_never().is_none())
        {
            return false;
        }
        self.retry = None;
        // The device clock only schedules a provider check; a reading before
        // the epoch schedules nothing.
        let now = StorageTime::since_epoch(
            clock
                .now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
        );
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
        let finality_due = pending.is_some_and(|pending| {
            let threshold = last_late.map_or(pending, |late| late.max(pending));
            now.duration_since(threshold).is_some_and(|age| age > wait)
        });
        // §6's drain needs a sample strictly past F + one day + five minutes.
        let drain_due = drains.iter().any(|drain| {
            drain.drain_due().is_some_and(|deadline| {
                now > deadline && self.sample.is_none_or(|(time, _)| time <= deadline)
            })
        });
        let due = finality_due || drain_due;
        if due {
            self.retry = Some(clock.sleep(Duration::from_secs(60)));
        }
        due
    }
}

/// The inclusive complete endpoint precedes the listing guarantee by one tick:
/// T′ = T − one hour − δ, for every provider.
/// Underflow supplies no observation; it must never certify the clock's own tick.
pub(crate) fn observation_time(time: StorageTime, resolution: Duration) -> Option<StorageTime> {
    assert!(
        !resolution.is_zero(),
        "provider timestamp resolution must be positive"
    );
    time.checked_sub(LISTING_MARGIN)?.checked_sub(resolution)
}

/// Recover the greatest qualifying quiet window through a completed endpoint.
/// `log` must contain and have judged every publication through `through`.
/// Publications after that endpoint do not belong to its recent window.
pub(crate) fn horizon(
    log: &StoreLog,
    through: StorageTime,
    resolution: Duration,
) -> Option<StorageTime> {
    let Some(endpoint) = quiet_endpoint(log, through, resolution) else {
        return log.horizon;
    };
    let cutoff = endpoint
        .checked_sub(ENTRY_WINDOW)
        .unwrap_or(StorageTime::UNIX_EPOCH);
    Some(log.horizon.map_or(cutoff, |previous| previous.max(cutoff)))
}

/// Greatest quiet endpoint through the completed prefix, on provider ticks.
pub(crate) fn quiet_endpoint(
    log: &StoreLog,
    through: StorageTime,
    resolution: Duration,
) -> Option<StorageTime> {
    assert!(
        !resolution.is_zero(),
        "provider timestamp resolution must be positive"
    );
    let mut late: Vec<_> = late_times(log).filter(|time| *time <= through).collect();
    late.sort_unstable();
    late.dedup();
    let mut endpoint = through;
    for time in late.into_iter().rev() {
        if time > endpoint {
            continue;
        }
        if endpoint
            .duration_since(time)
            .is_some_and(|gap| gap > ENTRY_WINDOW)
        {
            break;
        }
        // Both ends belong to the recent interval. Recover only provider ticks,
        // using the same resolution as the scan's preceding clock sample.
        endpoint = time.checked_sub(resolution)?;
    }
    Some(endpoint)
}

fn late_times(log: &StoreLog) -> impl Iterator<Item = StorageTime> + '_ {
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
