//! A wall-clock deadline on Windows: a waitable timer set to an absolute
//! due time, which Windows moves when the clock is set, and a thread-pool
//! wait on it.
//!
//! Microsoft's documentation of `SetWaitableTimer`: "Positive values
//! indicate absolute time. Be sure to use a UTC-based absolute time, as the
//! system uses UTC-based time internally", and, in its remarks, "If the
//! system time is adjusted, the due time of any outstanding absolute timers
//! is adjusted." The kernel's `KeSetTimer` says the same of the timers under
//! it: "Absolute expiration times track any changes in the system time".

use super::WallTimerError;
use std::ffi::c_void;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Threading::{
    CreateWaitableTimerExW, RegisterWaitForSingleObject, SetWaitableTimer, UnregisterWaitEx,
    CREATE_WAITABLE_TIMER_MANUAL_RESET, INFINITE, TIMER_ALL_ACCESS, WT_EXECUTEONLYONCE,
};

/// Seconds from 1601-01-01, where a FILETIME counts from, to the epoch.
const FILETIME_EPOCH_SECONDS: u64 = 11_644_473_600;

/// The timer, the thread pool's wait on it, and what the wait's callback
/// notifies; all three end as the wait drops.
struct WaitableTimer {
    timer: HANDLE,
    wait: HANDLE,
    /// A `Notify` keeps the callback's one notice until the wait asks for
    /// it, whichever comes first.
    fired: *const Notify,
}

// Safety: the handles are kernel objects any thread may use, and `fired` is
// a `Notify`, which any thread may notify; this owner releases each once.
unsafe impl Send for WaitableTimer {}

impl Drop for WaitableTimer {
    fn drop(&mut self) {
        // Safety: with `INVALID_HANDLE_VALUE`, `UnregisterWaitEx` returns
        // once a callback under way has finished, so none runs after it and
        // `fired` can be freed. Teardown failures leave nothing to undo.
        unsafe {
            UnregisterWaitEx(self.wait, INVALID_HANDLE_VALUE);
            CloseHandle(self.timer);
            drop(Box::from_raw(self.fired.cast_mut()));
        }
    }
}

/// Wait until the system's wall clock reads `at`, a time after the epoch.
pub(super) async fn sleep_until(at: SystemTime) -> Result<(), WallTimerError> {
    let since = at.duration_since(UNIX_EPOCH).expect("the deadline is after the epoch");
    // A due time is a positive count of 100 ns from 1601, UTC; a time past
    // what it holds fails the wait.
    let due = ((u128::from(since.as_secs()) + u128::from(FILETIME_EPOCH_SECONDS)) * 10_000_000
        + u128::from(since.subsec_nanos() / 100))
    .try_into()
    .map_err(|error: std::num::TryFromIntError| {
        WallTimerError::new(format!("the time is past what a Windows timer holds: {error}"))
    })?;
    let timer = WaitableTimer::arm(due)?;
    timer.fired().notified().await;
    drop(timer);
    Ok(())
}

impl WaitableTimer {
    /// A waitable timer due at `due` and a thread-pool wait whose callback
    /// notifies `fired` when it is.
    fn arm(due: i64) -> Result<Self, WallTimerError> {
        let failed = |call: &str| {
            WallTimerError::new(format!("{call}: {}", std::io::Error::last_os_error()))
        };
        // Safety: each call gets the objects its signature names, and each
        // object made is released on every path after it: by `WaitableTimer`
        // once it holds them, here before.
        unsafe {
            let timer = CreateWaitableTimerExW(
                std::ptr::null(),
                std::ptr::null(),
                CREATE_WAITABLE_TIMER_MANUAL_RESET,
                TIMER_ALL_ACCESS,
            );
            if timer.is_null() {
                return Err(failed("CreateWaitableTimerExW"));
            }
            if SetWaitableTimer(timer, &due, 0, None, std::ptr::null(), 0) == 0 {
                let error = failed("SetWaitableTimer");
                CloseHandle(timer);
                return Err(error);
            }
            let fired: *const Notify = Box::into_raw(Box::new(Notify::new()));
            let mut wait: HANDLE = std::ptr::null_mut();
            if RegisterWaitForSingleObject(
                &mut wait,
                timer,
                Some(on_fired),
                fired.cast::<c_void>(),
                INFINITE,
                WT_EXECUTEONLYONCE,
            ) == 0
            {
                let error = failed("RegisterWaitForSingleObject");
                CloseHandle(timer);
                drop(Box::from_raw(fired.cast_mut()));
                return Err(error);
            }
            Ok(Self { timer, wait, fired })
        }
    }

    /// What the callback notifies once the timer is signalled.
    fn fired(&self) -> &Notify {
        // Safety: `fired` lives until this owner drops.
        unsafe { &*self.fired }
    }
}

/// The thread pool's callback once the timer is signalled.
unsafe extern "system" fn on_fired(context: *mut c_void, _timed_out: bool) {
    // Safety: `context` is the `Notify` the timer's owner frees only after
    // unregistering this wait, which waits for this callback to return.
    unsafe { &*context.cast::<Notify>() }.notify_one();
}
