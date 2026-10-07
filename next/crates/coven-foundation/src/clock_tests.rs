use super::*;

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
