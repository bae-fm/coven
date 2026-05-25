/// Hybrid Logical Clock (HLC) for causal ordering of writes across devices.
///
/// Used as the `_updated_at` column value on all synced tables. Provides
/// monotonically increasing timestamps that handle clock skew between devices.
///
/// Format: `{millis:013}-{counter:04}-{device_id}`
/// Lexicographic string comparison gives correct causal ordering.
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum allowed clock skew from a remote timestamp (24 hours in ms).
/// If an incoming timestamp is more than this far ahead of local wall time,
/// we accept it but don't advance our local clock past wall time.
const MAX_CLOCK_DRIFT_MS: u64 = 24 * 60 * 60 * 1000;

/// A parsed HLC timestamp.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp {
    pub millis: u64,
    pub counter: u16,
    pub device_id: String,
}

impl Timestamp {
    pub fn new(millis: u64, counter: u16, device_id: String) -> Self {
        Self {
            millis,
            counter,
            device_id,
        }
    }

    /// Parse from the string format.
    pub fn parse(s: &str) -> Option<Self> {
        let mut parts = s.splitn(3, '-');
        let millis = parts.next()?.parse::<u64>().ok()?;
        let counter = parts.next()?.parse::<u16>().ok()?;
        let device_id = parts.next()?;
        if device_id.is_empty() {
            return None;
        }
        Some(Self {
            millis,
            counter,
            device_id: device_id.to_string(),
        })
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:013}-{:04}-{}",
            self.millis, self.counter, self.device_id
        )
    }
}

struct HlcState {
    millis: u64,
    counter: u16,
}

/// Hybrid Logical Clock.
///
/// Thread-safe via interior `Mutex`. Create one per application lifetime,
/// pass by reference to write methods.
pub struct Hlc {
    device_id: String,
    state: Mutex<HlcState>,
    /// Injected wall clock for testing. Returns milliseconds since epoch.
    wall_clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl Hlc {
    /// Create a new HLC with the given device ID.
    pub fn new(device_id: String) -> Self {
        Self {
            device_id,
            state: Mutex::new(HlcState {
                millis: 0,
                counter: 0,
            }),
            wall_clock: Box::new(wall_clock_ms),
        }
    }

    /// Generate a new timestamp. Guaranteed to be greater than any previous
    /// timestamp returned by this clock.
    pub fn now(&self) -> Timestamp {
        let wall = (self.wall_clock)();
        let mut state = self.state.lock().unwrap();

        if wall > state.millis {
            state.millis = wall;
            state.counter = 0;
        } else {
            state.counter += 1;
        }

        Timestamp::new(state.millis, state.counter, self.device_id.clone())
    }

    /// Merge with a remote timestamp. Advances the local clock to maintain
    /// the "happened after" relationship. Returns the new local timestamp.
    ///
    /// Implements a clock skew guard: if the remote's wall time is more than
    /// 24 hours ahead of local wall time, we accept the remote but don't
    /// advance our physical clock past local wall time.
    pub fn update(&self, remote: &Timestamp) -> Timestamp {
        let wall = (self.wall_clock)();
        let mut state = self.state.lock().unwrap();

        let remote_millis = if remote.millis > wall + MAX_CLOCK_DRIFT_MS {
            // Remote clock is unreasonably far ahead. Don't let it pull us
            // into the future -- use our wall clock instead.
            wall
        } else {
            remote.millis
        };

        if wall > state.millis && wall > remote_millis {
            // Wall clock is ahead of both local and remote: reset counter.
            state.millis = wall;
            state.counter = 0;
        } else if remote_millis > state.millis {
            // Remote is ahead of local: adopt remote's time, increment counter.
            state.millis = remote_millis;
            state.counter = remote.counter + 1;
        } else if state.millis > remote_millis {
            // Local is ahead: keep local time, increment counter.
            state.counter += 1;
        } else {
            // Same millis: take the higher counter + 1.
            state.counter = state.counter.max(remote.counter) + 1;
        }

        Timestamp::new(state.millis, state.counter, self.device_id.clone())
    }

    #[cfg(test)]
    fn with_wall_clock(device_id: String, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self {
            device_id,
            state: Mutex::new(HlcState {
                millis: 0,
                counter: 0,
            }),
            wall_clock: Box::new(clock),
        }
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    fn fixed_clock(ms: u64) -> impl Fn() -> u64 + Send + Sync + 'static {
        move || ms
    }

    fn advancing_clock(start: u64) -> (Arc<AtomicU64>, impl Fn() -> u64 + Send + Sync + 'static) {
        let time = Arc::new(AtomicU64::new(start));
        let time_clone = time.clone();
        (time, move || time_clone.load(Ordering::SeqCst))
    }

    #[test]
    fn basic_monotonicity() {
        let hlc = Hlc::new("dev-1".into());
        let t1 = hlc.now();
        let t2 = hlc.now();
        let t3 = hlc.now();

        assert!(t2 > t1, "t2={t2} should be > t1={t1}");
        assert!(t3 > t2, "t3={t3} should be > t2={t2}");
    }

    #[test]
    fn counter_increments_when_clock_stalls() {
        let hlc = Hlc::with_wall_clock("dev-1".into(), fixed_clock(1000));

        let t1 = hlc.now();
        assert_eq!(t1.millis, 1000);
        assert_eq!(t1.counter, 0);

        let t2 = hlc.now();
        assert_eq!(t2.millis, 1000);
        assert_eq!(t2.counter, 1);

        let t3 = hlc.now();
        assert_eq!(t3.millis, 1000);
        assert_eq!(t3.counter, 2);

        assert!(t3 > t2);
        assert!(t2 > t1);
    }

    #[test]
    fn wall_clock_advance_resets_counter() {
        let (time, clock) = advancing_clock(1000);
        let hlc = Hlc::with_wall_clock("dev-1".into(), clock);

        let t1 = hlc.now();
        assert_eq!(t1.millis, 1000);
        assert_eq!(t1.counter, 0);

        // Stall the clock -- counter increments.
        let t2 = hlc.now();
        assert_eq!(t2.counter, 1);

        // Advance the clock -- counter resets.
        time.store(2000, Ordering::SeqCst);
        let t3 = hlc.now();
        assert_eq!(t3.millis, 2000);
        assert_eq!(t3.counter, 0);

        assert!(t3 > t2);
    }

    #[test]
    fn merge_with_remote_ahead() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(1000));

        // Local clock is at 1000. Remote is at 5000.
        let remote = Timestamp::new(5000, 3, "dev-remote".into());
        let t = hlc.update(&remote);

        assert_eq!(t.millis, 5000);
        assert_eq!(t.counter, 4); // remote counter + 1
        assert_eq!(t.device_id, "dev-local");

        // Subsequent now() should still be >= the merged timestamp.
        let t2 = hlc.now();
        assert!(t2 > t, "t2={t2} should be > t={t}");
    }

    #[test]
    fn merge_with_remote_behind() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(5000));

        // Prime the local clock.
        let _ = hlc.now();

        // Remote is behind.
        let remote = Timestamp::new(1000, 10, "dev-remote".into());
        let t = hlc.update(&remote);

        // Local millis should stay at 5000 (ahead), counter increments.
        assert_eq!(t.millis, 5000);
        assert_eq!(t.counter, 1); // was 0, now incremented
        assert_eq!(t.device_id, "dev-local");
    }

    #[test]
    fn merge_with_same_millis() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(3000));

        // Prime: millis=3000, counter=0.
        let _ = hlc.now();

        // Remote also at 3000 but with counter=5.
        let remote = Timestamp::new(3000, 5, "dev-remote".into());
        let t = hlc.update(&remote);

        assert_eq!(t.millis, 3000);
        // max(local_counter=0, remote_counter=5) + 1 = 6
        assert_eq!(t.counter, 6);
    }

    #[test]
    fn clock_skew_guard_rejects_far_future() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(1000));

        // Remote claims a time 48 hours in the future -- beyond the 24h guard.
        let far_future = 1000 + MAX_CLOCK_DRIFT_MS + 1;
        let remote = Timestamp::new(far_future, 0, "dev-remote".into());
        let t = hlc.update(&remote);

        // Should NOT adopt the far-future millis. Should use wall clock instead.
        assert_eq!(t.millis, 1000);
    }

    #[test]
    fn clock_skew_guard_accepts_near_future() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(1000));

        // Remote is 1 hour ahead -- within the 24h guard.
        let near_future = 1000 + 60 * 60 * 1000;
        let remote = Timestamp::new(near_future, 0, "dev-remote".into());
        let t = hlc.update(&remote);

        // Should adopt the near-future millis.
        assert_eq!(t.millis, near_future);
    }

    #[test]
    fn string_roundtrip() {
        let ts = Timestamp::new(1707580800000, 42, "dev-abc123".into());
        let s = ts.to_string();
        let parsed = Timestamp::parse(&s).expect("parse should succeed");

        assert_eq!(parsed, ts);
        assert_eq!(s, "1707580800000-0042-dev-abc123");
    }

    #[test]
    fn string_format_is_zero_padded() {
        let ts = Timestamp::new(1000, 0, "d".into());
        assert_eq!(ts.to_string(), "0000000001000-0000-d");

        let ts2 = Timestamp::new(9999999999999, 9999, "d".into());
        assert_eq!(ts2.to_string(), "9999999999999-9999-d");
    }

    #[test]
    fn lexicographic_ordering_matches_causal_ordering() {
        let timestamps = [
            Timestamp::new(1000, 0, "dev-a".into()),
            Timestamp::new(1000, 1, "dev-a".into()),
            Timestamp::new(1000, 1, "dev-b".into()),
            Timestamp::new(2000, 0, "dev-a".into()),
            Timestamp::new(2000, 0, "dev-b".into()),
        ];

        let strings: Vec<String> = timestamps.iter().map(|t| t.to_string()).collect();

        // Verify the string list is sorted.
        for i in 1..strings.len() {
            assert!(
                strings[i] > strings[i - 1],
                "Expected {:?} > {:?}",
                strings[i],
                strings[i - 1]
            );
        }
    }

    #[test]
    fn device_id_breaks_ties() {
        let ts_a = Timestamp::new(5000, 3, "aaa".into());
        let ts_b = Timestamp::new(5000, 3, "bbb".into());

        // Derived ordering: same millis, same counter, device_id decides.
        assert!(ts_b > ts_a);

        // String comparison should agree.
        assert!(ts_b.to_string() > ts_a.to_string());
    }

    #[test]
    fn parse_rejects_invalid_input() {
        assert!(Timestamp::parse("").is_none());
        assert!(Timestamp::parse("not-a-timestamp").is_none());
        assert!(Timestamp::parse("1000-0000").is_none()); // missing device_id
        assert!(Timestamp::parse("1000-0000-").is_none()); // empty device_id
        assert!(Timestamp::parse("abc-0000-dev").is_none()); // non-numeric millis
        assert!(Timestamp::parse("1000-xyz-dev").is_none()); // non-numeric counter
    }

    #[test]
    fn parse_handles_device_id_with_dashes() {
        // Device IDs are UUIDs, which contain dashes. splitn(3, '-') must
        // correctly capture the remainder as the device_id.
        let ts = Timestamp::new(1000, 0, "550e8400-e29b-41d4-a716-446655440000".into());
        let s = ts.to_string();
        let parsed = Timestamp::parse(&s).expect("parse should handle UUID device_id");
        assert_eq!(parsed.device_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(parsed, ts);
    }
}
