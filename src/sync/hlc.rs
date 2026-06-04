/// Hybrid Logical Clock (HLC) for causal ordering of writes across devices.
///
/// This clock is coven's `_updated_at` register: the host stamps every synced
/// row's `_updated_at` with [`Hlc::now`] (via
/// `SyncManager::stamp_updated_at`), and pull advances the clock past every
/// applied row's `_updated_at` so a subsequent local write sorts causally
/// after anything just pulled. Row-level last-writer-wins (`conflict.rs`)
/// compares these strings lexicographically. Because the clock never mints a
/// stamp behind a value it has already seen — even under wall-clock skew or a
/// same-millisecond restart — a device that edits a row right after pulling a
/// peer's edit always wins, which a plain wall clock cannot guarantee.
///
/// `_updated_at` is opaque to the host: it binds the string coven hands it and
/// never parses it. Format (coven-internal): `{millis:013}-{counter:04}-{device_id}`.
///
/// The in-memory monotonic state is seeded on construction ([`Hlc::seed`]) so
/// it cannot regress across restarts. The seed floor is the max of two sources:
/// the persisted high-water mark ([`Hlc::high_water`], flushed at cycle end) and
/// the max `_updated_at` coven scans across its registered synced tables in
/// [`crate::sync::sync_manager::SyncManager::new`]. The on-disk row scan is the
/// authoritative floor — the high-water flush lags any local row stamp minted
/// between cycles, so seeding from it alone could let the first post-restart
/// stamp sort below the device's own un-flushed rows.
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// `sync_state` key under which the clock's high-water mark is persisted, so it
/// cannot regress across restarts (see [`Hlc::seed`]). Written whenever the
/// clock advances (host stamp flushed at cycle end, and on apply-merge).
pub const HIGHWATER_STATE_KEY: &str = "hlc_highwater";

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

    /// Seed the clock's monotonic state from a persisted high-water mark so it
    /// cannot mint a stamp behind a value it minted (or saw) before a restart.
    ///
    /// Idempotent and monotonic: a seed below the current state is ignored, so
    /// re-seeding can only push the clock forward. The seeded `device_id` is
    /// irrelevant — only `millis`/`counter` gate future stamps.
    pub fn seed(&self, high_water: &Timestamp) {
        let mut state = self.state.lock().unwrap();
        if high_water.millis > state.millis
            || (high_water.millis == state.millis && high_water.counter > state.counter)
        {
            state.millis = high_water.millis;
            state.counter = high_water.counter;
        }
    }

    /// The clock's current high-water mark: a [`Timestamp`] at the latest
    /// `millis`/`counter` this clock has reached. Persist this whenever the
    /// clock advances (on stamp and on apply-merge) and feed it back to
    /// [`Hlc::seed`] on the next construction.
    pub fn high_water(&self) -> Timestamp {
        let state = self.state.lock().unwrap();
        Timestamp::new(state.millis, state.counter, self.device_id.clone())
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

    /// Advance the clock past an applied row's `_updated_at`, so the next local
    /// stamp sorts causally after it. `remote` is an authoritative register
    /// value the LWW layer already accepted and wrote to disk — never an
    /// untrusted peer wall clock — so the advance is **unconditional**: no skew
    /// cap. Capping here would let the next local edit mint a stamp below an
    /// already-stored applied row and lose LWW to it.
    ///
    /// Monotonic: a `remote` at or behind the current state only bumps the
    /// counter; a `remote` ahead adopts its time. Either way the next [`now`]
    /// outranks `remote`.
    pub fn advance_past(&self, remote: &Timestamp) {
        let wall = (self.wall_clock)();
        let mut state = self.state.lock().unwrap();

        if wall > state.millis && wall > remote.millis {
            // Wall clock is ahead of both: adopt it, reset counter.
            state.millis = wall;
            state.counter = 0;
        } else if remote.millis > state.millis {
            // Remote is ahead of local: adopt remote's time, increment counter.
            state.millis = remote.millis;
            state.counter = remote.counter + 1;
        } else if state.millis > remote.millis {
            // Local is ahead: keep local time, increment counter.
            state.counter += 1;
        } else {
            // Same millis: take the higher counter + 1.
            state.counter = state.counter.max(remote.counter) + 1;
        }
    }

    #[cfg(test)]
    pub(crate) fn with_wall_clock(
        device_id: String,
        clock: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
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

/// A cloneable handle that mints `_updated_at` register values from a shared
/// [`Hlc`] — the register-stamping capability, sliced off the whole
/// [`crate::sync::sync_manager::SyncManager`].
///
/// The host's database is built before the manager and then moved into
/// `SyncManager::new`, but its write path must stamp `_updated_at` with the
/// *same* clock the manager drives: coven advances that clock past every pulled
/// row, and if the db held a separate clock that advance would never reach the
/// db's stamper, letting a later local write sort behind a pulled row and
/// silently lose last-writer-wins. So the host obtains this handle from the
/// constructed manager ([`crate::sync::sync_manager::SyncManager::updated_at_stamper`])
/// and injects it into the write path; every clone shares one `Arc<Hlc>`, so
/// coven's seeding and advance-on-pull are reflected in every stamp the host
/// mints.
///
/// It exposes only [`UpdatedAtStamper::stamp`] — never `seed`/`advance_past`/
/// `high_water`. Those drive the clock and are coven's alone; the host write
/// path is a pure consumer of stamps and must not poke clock state.
#[derive(Clone)]
pub struct UpdatedAtStamper {
    hlc: Arc<Hlc>,
}

impl UpdatedAtStamper {
    pub(crate) fn new(hlc: Arc<Hlc>) -> Self {
        Self { hlc }
    }

    /// Mint the next `_updated_at` register value for a synced-row write. The
    /// returned string is an opaque HLC stamp; the host binds it into the write
    /// and must not parse or compare it as a wall-clock time.
    pub fn stamp(&self) -> String {
        self.hlc.now().to_string()
    }

    /// A standalone stamper over a fresh in-memory HLC, for host tests that
    /// inject a real stamper through their production injection path without
    /// constructing a [`SyncManager`](crate::sync::sync_manager::SyncManager).
    /// Not for production — production stampers come from
    /// `SyncManager::updated_at_stamper` so they share the manager's seeded,
    /// pull-advanced clock.
    #[cfg(feature = "test-utils")]
    pub fn for_test() -> Self {
        Self::new(Arc::new(Hlc::new("test-device".to_string())))
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
    fn advance_past_remote_ahead() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(1000));

        // Local clock is at 1000. Applied row stamp is at 5000.
        let remote = Timestamp::new(5000, 3, "dev-remote".into());
        hlc.advance_past(&remote);

        // The next stamp must sort strictly after the applied row.
        let t = hlc.now();
        assert!(
            t.to_string() > remote.to_string(),
            "t={t} must beat {remote}"
        );
        assert_eq!(t.millis, 5000);
        assert_eq!(t.device_id, "dev-local");
    }

    #[test]
    fn advance_past_remote_behind() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(5000));

        // Prime the local clock to 5000.
        let primed = hlc.now();

        // An applied row stamp that's behind must not regress the clock.
        let remote = Timestamp::new(1000, 10, "dev-remote".into());
        hlc.advance_past(&remote);

        let t = hlc.now();
        assert!(
            t > primed,
            "t={t} must stay above the primed clock {primed}"
        );
        assert_eq!(t.millis, 5000);
    }

    /// The register-floor guarantee: an applied row's `_updated_at` is an
    /// authoritative value the LWW layer already wrote to disk, not an untrusted
    /// peer wall clock. The clock must advance past it *unconditionally* — even
    /// when it sits far beyond local wall time — or the next local stamp sorts
    /// below an already-stored row and loses LWW to it. Any skew cap that bounded
    /// the advance to wall time would reintroduce exactly that loss.
    #[test]
    fn advance_past_far_future_applied_row_is_not_capped() {
        let hlc = Hlc::with_wall_clock("dev-local".into(), fixed_clock(1000));

        // An applied row stamped 48 hours beyond local wall — well past the old
        // 24h cap.
        let far_future = 1000 + 48 * 60 * 60 * 1000;
        let applied = Timestamp::new(far_future, 7, "dev-remote".into());
        hlc.advance_past(&applied);

        // The next local stamp must sort *after* the applied row, not be capped
        // back to wall time (1000) where it would sort below it.
        let next = hlc.now();
        assert!(
            next.to_string() > applied.to_string(),
            "next stamp {next} regressed below applied row {applied}: the clock \
             refused to advance past an authoritative register value",
        );
        assert_eq!(next.millis, far_future);
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
