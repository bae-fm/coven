//! A wall-clock deadline on macOS and iOS: a libdispatch timer source whose
//! start is a `dispatch_walltime`, which libdispatch moves when the clock is
//! set.
//!
//! libdispatch (apple-oss-distributions/libdispatch, src/event/event_kevent.c)
//! asks the kernel for a calendar-change notification the first time a
//! wall-clock timer is armed: `_dispatch_event_loop_timer_arm` calls
//! `_dispatch_mach_host_calendar_change_register` when the timer's clock is
//! `DISPATCH_CLOCK_WALL`, which registers `HOST_NOTIFY_CALENDAR_SET` through
//! `host_request_notification`. Each notification calls
//! `_dispatch_timers_calendar_change`, "calendar change may have gone past
//! the wallclock deadline", which marks every wall-clock timer heap dirty so
//! the timers are programmed again against the clock as it now reads: a
//! setting past the deadline fires the timer, and one before it moves the
//! firing later.

use super::WallTimerError;
use block2::RcBlock;
use std::ffi::c_void;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

/// `DISPATCH_TIME_FOREVER`, as a timer's interval: it fires once.
const FOREVER: u64 = !0;
/// `DISPATCH_TIME_MAX_VALUE` (src/shims/time.h): a wall time is the
/// negated count of nanoseconds since the epoch with the top two bits set,
/// so it holds fewer than 2^62 nanoseconds.
const MAX_NANOS: u128 = (1 << 62) - 1;

/// libdispatch's `struct dispatch_source_type_s`, which only its address
/// names.
#[repr(C)]
struct SourceType {
    _opaque: [u8; 0],
}

unsafe extern "C" {
    /// `DISPATCH_SOURCE_TYPE_TIMER` is this object's address.
    static _dispatch_source_type_timer: SourceType;
    fn dispatch_walltime(when: *const libc::timespec, delta: i64) -> u64;
    fn dispatch_get_global_queue(identifier: isize, flags: usize) -> *mut c_void;
    fn dispatch_source_create(
        kind: *const SourceType,
        handle: usize,
        mask: usize,
        queue: *mut c_void,
    ) -> *mut c_void;
    fn dispatch_source_set_timer(source: *mut c_void, start: u64, interval: u64, leeway: u64);
    fn dispatch_source_set_event_handler(source: *mut c_void, handler: &block2::Block<dyn Fn()>);
    #[cfg(test)]
    fn dispatch_source_set_cancel_handler(source: *mut c_void, handler: &block2::Block<dyn Fn()>);
    fn dispatch_activate(object: *mut c_void);
    fn dispatch_source_cancel(source: *mut c_void);
    fn dispatch_release(object: *mut c_void);
}

/// An armed timer source, cancelled and released as the wait drops.
struct DispatchTimer(*mut c_void);

// Safety: libdispatch's sources are thread-safe objects; this owner only
// cancels and releases its source, once.
unsafe impl Send for DispatchTimer {}

impl Drop for DispatchTimer {
    fn drop(&mut self) {
        // Safety: the source is live and this owner's only reference;
        // cancelling stops its handler, which owns its own state.
        unsafe {
            dispatch_source_cancel(self.0);
            dispatch_release(self.0);
        }
    }
}

/// Wait until the system's wall clock reads `at`, a time after the epoch.
pub(super) async fn sleep_until(at: SystemTime) -> Result<(), WallTimerError> {
    let nanos = at.duration_since(UNIX_EPOCH).expect("the deadline is after the epoch").as_nanos();
    if nanos > MAX_NANOS {
        return Err(WallTimerError::new("the time is past what libdispatch's wall timer holds"));
    }
    // `dispatch_walltime` takes a count of one nanosecond or less as
    // `DISPATCH_TIME_FOREVER` (src/time.c); two nanoseconds after the epoch
    // has passed as surely.
    let nanos = u64::try_from(nanos.max(2)).expect("below 2^62");
    let when = libc::timespec {
        tv_sec: libc::time_t::try_from(nanos / 1_000_000_000).expect("below 2^62 nanoseconds"),
        tv_nsec: libc::c_long::try_from(nanos % 1_000_000_000).expect("below one second"),
    };
    let (fired, firing) = oneshot::channel::<()>();
    let timer = DispatchTimer::arm(
        &when,
        fired,
        #[cfg(test)]
        at,
    )?;
    firing.await.expect("the armed timer keeps its handler");
    drop(timer);
    Ok(())
}

impl DispatchTimer {
    /// A timer source that fires at `when` and sends on `fired`, on the global
    /// queue. The handler block isn't `Send`, so it stays out of the wait.
    /// In a test, its cancel handler tells the test the source for `at` is
    /// released: libdispatch runs it once the source is cancelled and no
    /// event handler can run any more.
    fn arm(
        when: &libc::timespec,
        fired: oneshot::Sender<()>,
        #[cfg(test)] at: SystemTime,
    ) -> Result<Self, WallTimerError> {
        let fired = Mutex::new(Some(fired));
        let handler = RcBlock::new(move || {
            // A panic here would abort the process; nothing panics holding the
            // lock.
            if let Some(fired) =
                fired.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
            {
                // The wait may have dropped as the timer fired; nothing waits.
                #[expect(clippy::let_underscore_must_use, reason = "the wait may have gone")]
                let _ = fired.send(());
            }
        });
        // Safety: each call gets the objects its signature names. The global
        // queue is never released; the source copies the handler block, so
        // dropping ours is safe; `DispatchTimer` releases the source.
        unsafe {
            let start = dispatch_walltime(when, 0);
            let queue = dispatch_get_global_queue(0, 0);
            let source =
                dispatch_source_create(&raw const _dispatch_source_type_timer, 0, 0, queue);
            if source.is_null() {
                return Err(WallTimerError::new("libdispatch could not create a timer source"));
            }
            dispatch_source_set_event_handler(source, &handler);
            #[cfg(test)]
            dispatch_source_set_cancel_handler(source, &RcBlock::new(move || super::released(at)));
            dispatch_source_set_timer(source, start, FOREVER, 0);
            dispatch_activate(source);
            Ok(Self(source))
        }
    }
}
