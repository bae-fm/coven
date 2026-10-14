//! When a write miss means nothing new (§15's recent-return rule).

use crate::finality::Observation;
use coven_format::value::StorageTime;
use coven_foundation::clock::Clock;
use std::sync::Mutex;
use std::time::Duration;

/// Misses are trusted for 29 storage days after the checkpoint: one day of
/// margin below the 30-day age after which retention may delete a write.
pub(crate) const RECENT: Duration = Duration::from_secs(29 * 24 * 60 * 60);

/// Whether a write miss must first wait for snapshot discovery: there is no
/// checkpoint, or the sample plus elapsed time has reached `S + 29 days`.
/// Equality takes discovery (IO `needsSnapshots`).
pub(crate) fn needs_snapshots(
    saved: Option<StorageTime>,
    observed: StorageTime,
    elapsed: Duration,
) -> bool {
    let Some(saved) = saved else {
        return true;
    };
    nanos(saved) + RECENT.as_nanos() <= nanos(observed) + elapsed.as_nanos()
}

/// Refresh storage time a day before the window would close, so a quiet
/// session's next completed pass can save a later checkpoint.
pub(crate) fn refresh_due(
    saved: Option<StorageTime>,
    observed: StorageTime,
    elapsed: Duration,
) -> bool {
    needs_snapshots(saved, observed, elapsed + Duration::from_secs(24 * 60 * 60))
}

fn nanos(time: StorageTime) -> u128 {
    u128::from(time.seconds()) * 1_000_000_000 + u128::from(time.nanos())
}

/// Judge a miss now, when it is used, not at the start of its pass: the
/// session's clock-object sample plus monotonic time since its request began,
/// against the saved checkpoint or this session's snapshot discovery. A
/// session without a clock-object sample cannot trust any miss.
pub(crate) fn miss_is_recent(
    observation: &Mutex<Observation>,
    clock: &dyn Clock,
    saved: Option<StorageTime>,
) -> bool {
    let (sample, discovered) = {
        let observation = observation.lock().expect("finality lock poisoned");
        (observation.clock(), observation.discovered())
    };
    let Some((stored, started)) = sample else {
        return false;
    };
    let elapsed = clock.elapsed().saturating_sub(started);
    !needs_snapshots(saved.max(discovered), stored, elapsed)
}

#[cfg(test)]
#[path = "recent_return_tests.rs"]
mod tests;
