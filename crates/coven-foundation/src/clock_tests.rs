use super::*;
use crate::clock::HANG_GUARD;

#[test]
fn system_clock_is_a_shared_clock() {
    let clock: ClockRef = Arc::new(SystemClock);
    let before = SystemClock.now();
    let now = clock.now();
    let after = SystemClock.now();
    assert!((before..=after).contains(&now));
}

#[cfg(feature = "test-utils")]
#[test]
fn setting_a_fixed_clock_changes_every_shared_read_and_allows_backward_time() {
    use std::time::{Duration, UNIX_EPOCH};
    let fixed = Arc::new(FixedClock::new(UNIX_EPOCH));
    let clock: ClockRef = fixed.clone();
    assert_eq!(clock.now(), UNIX_EPOCH);
    fixed.set(UNIX_EPOCH + Duration::from_millis(13));
    assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_millis(13));
    fixed.set(UNIX_EPOCH - Duration::from_secs(1));
    assert_eq!(clock.now(), UNIX_EPOCH - Duration::from_secs(1));
}

#[cfg(feature = "test-utils")]
#[test]
fn closure_clock_calls_the_function_on_every_read() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, UNIX_EPOCH};
    let calls = AtomicU64::new(10);
    let clock =
        ClosureClock(|| UNIX_EPOCH + Duration::from_millis(calls.fetch_add(1, Ordering::Relaxed)));
    assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_millis(10));
    assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_millis(11));
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn controlled_sleep_counts_forward_time_and_captures_its_start_before_polling() {
    use std::{future::poll_fn, task::Poll, time::UNIX_EPOCH};
    let clock = FixedClock::new(UNIX_EPOCH + Duration::from_secs(100));
    let mut sleep = clock.sleep(Duration::from_secs(30));
    clock.set(UNIX_EPOCH + Duration::from_secs(110));
    clock.set(UNIX_EPOCH + Duration::from_secs(50));
    poll_fn(|cx| {
        assert!(sleep.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    clock.set(UNIX_EPOCH + Duration::from_secs(70));
    sleep.await;
    let sleep = clock.sleep(Duration::from_secs(30));
    clock.set(UNIX_EPOCH + Duration::from_secs(100));
    sleep.await;
}

#[test]
fn system_elapsed_time_never_decreases() {
    let first = SystemClock.elapsed();
    let second = SystemClock.elapsed();
    assert!(second >= first);
}

#[cfg(feature = "test-utils")]
#[test]
fn fixed_elapsed_time_counts_forward_settings_as_sleep_and_ignores_backward_ones() {
    use std::time::{Duration, UNIX_EPOCH};
    let fixed = FixedClock::new(UNIX_EPOCH + Duration::from_secs(100));
    assert_eq!(fixed.elapsed(), Duration::ZERO);
    fixed.set(UNIX_EPOCH + Duration::from_secs(130));
    assert_eq!(fixed.elapsed(), Duration::from_secs(30));
    fixed.set(UNIX_EPOCH);
    assert_eq!(fixed.elapsed(), Duration::from_secs(30));
    fixed.set(UNIX_EPOCH + Duration::from_secs(5));
    assert_eq!(fixed.elapsed(), Duration::from_secs(35));
}

/// The system's timer for a wall-clock time already past fires as it is
/// armed, and a time at the epoch needs no timer.
#[tokio::test]
async fn a_system_wall_wait_for_a_time_already_past_ends_at_once() {
    use std::time::UNIX_EPOCH;
    for at in [SystemClock.now() - Duration::from_secs(3600), UNIX_EPOCH] {
        let waited = tokio::time::timeout(HANG_GUARD, SystemClock.sleep_until(at)).await;
        waited.expect("the wait ended at once").unwrap();
    }
}

/// The system's timer for a time a few hundred milliseconds ahead fires
/// once the wall clock reads it, not before.
#[tokio::test]
async fn a_system_wall_wait_ends_when_the_clock_reaches_its_time() {
    let at = SystemClock.now() + Duration::from_millis(300);
    let waited = tokio::time::timeout(HANG_GUARD, SystemClock.sleep_until(at)).await;
    waited.expect("the timer fired").unwrap();
    let woke = SystemClock.now();
    assert!(woke >= at, "the timer fired {:?} early", at.duration_since(woke));
}

/// A wait dropped while its timer is armed releases the timer: on macOS
/// and iOS the source's cancel handler runs, after which its event handler
/// never does; elsewhere the timer's handle closes as the wait drops.
#[tokio::test]
async fn a_dropped_system_wall_wait_releases_its_timer() {
    use std::{future::poll_fn, task::Poll};
    let at = SystemClock.now() + Duration::from_secs(60);
    let released = super::when_released(at);
    let mut wait = SystemClock.sleep_until(at);
    poll_fn(|cx| {
        assert!(wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(wait);
    tokio::time::timeout(HANG_GUARD, released)
        .await
        .expect("the timer is released")
        .expect("the release is told");
}

/// A fixed clock's wall wait ends when a setting reaches its time; settings
/// before it, forward or back, leave it waiting.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn a_fixed_wall_wait_ends_when_a_setting_reaches_its_time() {
    use std::{future::poll_fn, task::Poll, time::UNIX_EPOCH};
    let clock = FixedClock::new(UNIX_EPOCH + Duration::from_secs(100));
    let at = UNIX_EPOCH + Duration::from_secs(200);
    let mut wait = clock.sleep_until(at);
    for set in [150, 50, 199] {
        clock.set(UNIX_EPOCH + Duration::from_secs(set));
        poll_fn(|cx| {
            assert!(wait.as_mut().poll(cx).is_pending(), "woke at {set}");
            Poll::Ready(())
        })
        .await;
    }
    clock.set(at);
    wait.await.unwrap();
    clock.sleep_until(at - Duration::from_secs(1)).await.unwrap();
}

/// A closure clock has no timer for its readings, so its wall wait fails.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn a_closure_clock_refuses_a_wall_wait() {
    use std::time::UNIX_EPOCH;
    let clock = ClosureClock(|| UNIX_EPOCH);
    assert!(clock.sleep_until(UNIX_EPOCH + Duration::from_secs(1)).await.is_err());
}
