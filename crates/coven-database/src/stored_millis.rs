//! Waits and times as coven's tables store them: whole milliseconds, a time
//! counted from the Unix epoch.

use crate::DbError;
use std::time::{Duration, SystemTime};

/// Which way a stored value rounds a part of a millisecond.
#[derive(Clone, Copy)]
pub(crate) enum Round {
    /// Up, so a stored wait or end never comes before the one it stores.
    Up,
    /// Down, so a stored moment never comes after the one it stores.
    Down,
}

/// `duration` in whole milliseconds, rounded `round`; none past SQLite's
/// integers.
pub(crate) fn millis(duration: Duration, round: Round) -> Option<i64> {
    let millis = match round {
        Round::Up => duration.as_nanos().div_ceil(1_000_000),
        Round::Down => duration.as_millis(),
    };
    i64::try_from(millis).ok()
}

/// `time` in milliseconds since the Unix epoch, rounded `round`; none
/// before the epoch or past SQLite's integers.
pub(crate) fn since_epoch(time: SystemTime, round: Round) -> Option<i64> {
    millis(time.duration_since(SystemTime::UNIX_EPOCH).ok()?, round)
}

/// The wait a stored count of milliseconds names. A negative one is a
/// stored fact that doesn't decode: `DamagedDatabase` (§19.3).
pub(crate) fn stored_wait(millis: i64) -> Result<Duration, DbError> {
    Ok(Duration::from_millis(u64::try_from(millis).map_err(|_negative| DbError::DamagedDatabase)?))
}

/// The time a stored count of milliseconds since the Unix epoch names, as
/// `coven_retry_effect.cooling_until` and `coven_sync_status.last_synced`
/// hold it. One the clock can't name is `DamagedDatabase` (§19.3).
pub fn stored_time(millis: i64) -> Result<SystemTime, DbError> {
    SystemTime::UNIX_EPOCH.checked_add(stored_wait(millis)?).ok_or(DbError::DamagedDatabase)
}
