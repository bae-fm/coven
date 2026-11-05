//! A wall-clock deadline on Linux and Android: a timerfd on `CLOCK_REALTIME`
//! armed with an absolute expiry (`TFD_TIMER_ABSTIME`), which the kernel
//! moves when the clock is set.
//!
//! POSIX, for `clock_settime`: "If the value of the CLOCK_REALTIME clock is
//! set via clock_settime(), the new value of the clock shall be used to
//! determine the time of expiration for absolute time services based upon
//! the CLOCK_REALTIME clock. This applies to the time at which armed
//! absolute timers expire." timerfd_create(2): with `TFD_TIMER_ABSTIME`
//! "the timer will expire when the value of the timer's clock reaches the
//! value specified in new_value.it_value". Linux compares an absolute
//! `CLOCK_REALTIME` timer's expiry with the monotonic clock plus the
//! realtime clock's offset, and setting the clock updates that offset and
//! reprograms the timer hardware: kernel/time/hrtimer.c, `clock_was_set`, "This requires to
//! update the offsets for these clocks vs. CLOCK_MONOTONIC. When high
//! resolution timers are enabled, then this also requires to eventually
//! reprogram the per CPU clock event devices when the change moves an
//! affected timer ahead of the first expiring timer on that CPU."
//!
//! The same page: a clock set back after the timer fired but before its
//! read "may cause read(2) to unblock, but return a value of 0". The wait
//! still ends: the clock reached its time, and the caller reads the clock
//! again for what to do.

use super::WallTimerError;
use rustix::time::{
    timerfd_create, timerfd_settime, Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags,
    Timespec,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::unix::AsyncFd;

/// Wait until the system's wall clock reads `at`, a time after the epoch:
/// at a zero expiry the timer would disarm instead.
pub(super) async fn sleep_until(at: SystemTime) -> Result<(), WallTimerError> {
    let since = at.duration_since(UNIX_EPOCH).expect("the deadline is after the epoch");
    // Made before the timer, so it drops after the timer's handle closes.
    #[cfg(test)]
    let _released = super::ReleasedOnDrop(at);
    let timer =
        timerfd_create(TimerfdClockId::Realtime, TimerfdFlags::CLOEXEC | TimerfdFlags::NONBLOCK)
            .map_err(WallTimerError::new)?;
    let expiry = Itimerspec {
        // No interval: the timer fires once.
        it_interval: Timespec { tv_sec: 0, tv_nsec: 0 },
        it_value: Timespec::try_from(since).map_err(WallTimerError::new)?,
    };
    timerfd_settime(&timer, TimerfdTimerFlags::ABSTIME, &expiry).map_err(WallTimerError::new)?;
    let timer = AsyncFd::new(timer).map_err(WallTimerError::new)?;
    loop {
        let mut ready = timer.readable().await.map_err(WallTimerError::new)?;
        // The timer is readable once it has fired: it reads its expiry
        // count. A read it would block on was a readiness the runtime
        // reported early, which `try_io` clears before waiting again.
        let mut count = [0u8; 8];
        match ready.try_io(|timer| Ok(rustix::io::read(timer, &mut count)?)) {
            Ok(read) => return read.map(drop).map_err(WallTimerError::new),
            Err(_would_block) => continue,
        }
    }
}
