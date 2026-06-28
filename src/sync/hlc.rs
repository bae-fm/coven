/// Hybrid Logical Clock (HLC) for causal ordering of writes across devices.
///
/// This clock is coven's `_updated_at` register: the host stamps every synced
/// row's `_updated_at` with [`Hlc::now`] (via the [`UpdatedAtStamper`]
/// [`crate::database::Database::open`] returns), and pull advances the
/// clock past every
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
/// the max `_updated_at` coven scans across the synced tables in
/// [`crate::database::Database::open`]. The on-disk row scan is the
/// authoritative floor — the high-water flush lags any local row stamp minted
/// between cycles, so seeding from it alone could let the first post-restart
/// stamp sort below the device's own un-flushed rows.
use std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

/// `sync_state` key under which the clock's high-water mark is persisted, so it
/// cannot regress across restarts (see [`Hlc::seed`]). Written whenever the
/// clock advances (host stamp flushed at cycle end, and on apply-merge).
pub const HIGHWATER_STATE_KEY: &str = "hlc_highwater";

/// How far ahead of the receiver's wall clock an incoming `_updated_at`'s
/// physical (millis) component may sit and still be treated as honest. A device
/// can legitimately be offline for a long stretch and cross-device wall clocks
/// drift, so the window is generous — 30 days. A stamp beyond `receiver wall + this`
/// has no honest explanation (a broken clock or buggy client), so the receiver
/// refuses to let it win last-writer-wins or ratchet the local clock. The bound is
/// one-sided: only grossly-*future* stamps are rejected; a stamp in the past is
/// always honest (an offline device's older edits) and is never bounded.
pub const MAX_FUTURE_SKEW_MS: u64 = 30 * 24 * 60 * 60 * 1000;

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

    /// Whether this stamp's physical (millis) component is within the honest
    /// future bound relative to `receiver_wall_ms` (the receiver's current wall
    /// clock when it observed the stamp). A stamp at or behind wall time is always
    /// honest; one ahead is honest only within [`MAX_FUTURE_SKEW_MS`]. Beyond that
    /// it is grossly-future — a broken clock or buggy client — and the receiver
    /// must not let it win last-writer-wins or ratchet the local clock.
    pub fn is_within_future_bound(&self, receiver_wall_ms: u64) -> bool {
        self.millis <= receiver_wall_ms.saturating_add(MAX_FUTURE_SKEW_MS)
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

    /// The receiver's current wall-clock millis, read from the same injected
    /// source the clock stamps from. This is the reference the pull bounds an
    /// incoming `_updated_at` against (see [`Timestamp::is_within_future_bound`]):
    /// it is the receiver's view of "now", in the same millis unit as a stamp's
    /// physical component — never an author-supplied value. Read once per pull and
    /// passed down, not sampled in a loop.
    pub fn wall_now_ms(&self) -> u64 {
        (self.wall_clock)()
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
/// The host obtains this handle from [`crate::database::Database::open`] (seeded,
/// non-optional) and injects it into the write path — before any
/// [`crate::sync::sync_manager::SyncManager`]
/// exists. The manager, built later, borrows the same `Arc<Hlc>`: coven advances
/// that clock past every pulled row, and because the stamper shares the
/// instance, that advance reaches every stamp the host mints, so a later local
/// write never sorts behind a pulled row and loses last-writer-wins. Every clone
/// shares one `Arc<Hlc>`, so coven's seeding and advance-on-pull are reflected in
/// every stamp.
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
    /// opening a [`Database`](crate::database::Database). Not for production —
    /// production stampers come from
    /// [`Database::open`](crate::database::Database::open) so they share the
    /// seeded, pull-advanced clock.
    #[cfg(feature = "test-utils")]
    pub fn for_test() -> Self {
        Self::new(Arc::new(Hlc::new("test-device".to_string())))
    }
}

/// The OS wall clock in epoch milliseconds — the same physical source [`Hlc`]
/// stamps from. Production reads "now" through an injected clock
/// ([`Hlc::wall_now_ms`]); this is for callers that apply a *trusted*, already-
/// captured changeset against a raw connection with no injected clock (snapshot
/// round-trips, gate/FK mechanics tests), where the honest receiver-now is real
/// wall time and the future-skew bound is incidental, not under test.
#[cfg(test)]
pub fn now_wall_ms() -> u64 {
    wall_clock_ms()
}

/// Epoch milliseconds for the HLC's physical component. Native reads the OS clock
/// via `std::time`; wasm reads the browser clock via `js_sys::Date::now()`, since
/// `std::time` has no wasm backend (`SystemTime::now()` panics there). The HLC's
/// monotonic counter still guarantees ordering when this source repeats or jumps.
#[cfg(not(target_arch = "wasm32"))]
fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

#[cfg(target_arch = "wasm32")]
fn wall_clock_ms() -> u64 {
    js_sys::Date::now() as u64
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
    fn future_bound_admits_honest_and_rejects_grossly_future() {
        let wall: u64 = 1_700_000_000_000;

        // At or behind wall time: always honest, regardless of how far behind.
        assert!(Timestamp::new(wall, 0, "d".into()).is_within_future_bound(wall));
        assert!(Timestamp::new(0, 0, "d".into()).is_within_future_bound(wall));

        // Inside the allowance (offline device, plausible drift): honest.
        let just_inside = wall + MAX_FUTURE_SKEW_MS - 1;
        assert!(Timestamp::new(just_inside, 0, "d".into()).is_within_future_bound(wall));
        // Exactly at the allowance boundary: still admitted (inclusive).
        let at_bound = wall + MAX_FUTURE_SKEW_MS;
        assert!(Timestamp::new(at_bound, 0, "d".into()).is_within_future_bound(wall));

        // One past the allowance: grossly-future, rejected.
        let just_beyond = wall + MAX_FUTURE_SKEW_MS + 1;
        assert!(!Timestamp::new(just_beyond, 0, "d".into()).is_within_future_bound(wall));
        // Absurd far-future (broken clock): rejected.
        assert!(!Timestamp::new(u64::MAX, 0, "d".into()).is_within_future_bound(wall));

        // The wall + allowance sum saturates rather than overflowing, so a near-max
        // wall clock still admits an at-or-behind stamp.
        assert!(Timestamp::new(u64::MAX, 0, "d".into()).is_within_future_bound(u64::MAX));
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
