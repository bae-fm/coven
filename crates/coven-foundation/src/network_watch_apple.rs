//! The device's network on macOS and iOS: Network.framework's path monitor
//! (api.md E5). The path is usable when satisfied, and metered when it is
//! expensive, as cellular and a phone's hotspot are, or constrained, as Low
//! Data Mode makes it. Another set of interfaces is another network.
//!
//! No crate binds Network.framework (`objc2-network` is a reserved name with
//! no bindings), so coven declares the few C functions it calls here.

use super::{heard, NetworkSink, NetworkWatchError, UsableNetwork};
use crate::locks::lock_unpoisoned;
use crate::task::BugSinkRef;
use block2::RcBlock;
use std::ffi::{c_char, c_void, CStr};
use std::sync::{mpsc, Arc, Mutex};

/// `nw_path_status_satisfied`.
const SATISFIED: i32 = 1;

#[link(name = "Network", kind = "framework")]
unsafe extern "C" {
    fn nw_path_monitor_create() -> *mut c_void;
    fn nw_path_monitor_set_queue(monitor: *mut c_void, queue: *mut c_void);
    fn nw_path_monitor_set_update_handler(
        monitor: *mut c_void,
        handler: &block2::Block<dyn Fn(*mut c_void)>,
    );
    fn nw_path_monitor_start(monitor: *mut c_void);
    fn nw_path_monitor_cancel(monitor: *mut c_void);
    fn nw_path_get_status(path: *mut c_void) -> i32;
    fn nw_path_is_expensive(path: *mut c_void) -> bool;
    fn nw_path_is_constrained(path: *mut c_void) -> bool;
    fn nw_path_enumerate_interfaces(
        path: *mut c_void,
        // The block returns C's `bool`, one byte, whether to go on;
        // objc2 encodes no Rust `bool`, since Objective-C's `BOOL` differs.
        enumerate: &block2::Block<dyn Fn(*mut c_void) -> u8 + '_>,
    );
    fn nw_interface_get_name(interface: *mut c_void) -> *const c_char;
    fn nw_release(object: *mut c_void);
}

unsafe extern "C" {
    fn dispatch_queue_create(label: *const c_char, attributes: *mut c_void) -> *mut c_void;
    fn dispatch_release(object: *mut c_void);
}

/// The path monitor and the serial queue it reports on, cancelled and
/// released as the watch drops.
pub(super) struct PathMonitor {
    monitor: *mut c_void,
    queue: *mut c_void,
}

// Safety: Network.framework's monitor and libdispatch's queue are
// thread-safe objects; this owner only cancels and releases them, once.
unsafe impl Send for PathMonitor {}
// Safety: as above; nothing reads them through a shared reference.
unsafe impl Sync for PathMonitor {}

impl PathMonitor {
    /// Start the monitor, and return once it has described the path as it
    /// is now, which it does as it starts. A panic reading a path goes to
    /// `bugs`.
    pub(super) fn start(
        sink: Arc<Mutex<NetworkSink>>,
        bugs: BugSinkRef,
    ) -> Result<Self, NetworkWatchError> {
        let (first, described) = mpsc::channel::<()>();
        let first = Mutex::new(Some(first));
        let handler = RcBlock::new(move |path: *mut c_void| {
            heard(&sink, bugs.as_ref(), "read the network's path", || {
                lock_unpoisoned(&sink).observe(network(path));
            });
            if let Some(first) = lock_unpoisoned(&first).take() {
                // The opener waiting for it may have gone; nothing is lost.
                #[expect(clippy::let_underscore_must_use, reason = "the opener may have gone")]
                let _ = first.send(());
            }
        });
        // Safety: each call gets the objects its signature names; the
        // monitor copies the handler block, so dropping ours is safe.
        let watch = unsafe {
            let monitor = nw_path_monitor_create();
            if monitor.is_null() {
                return Err(NetworkWatchError::new("the path monitor could not be created"));
            }
            let queue = dispatch_queue_create(c"fm.coven.network".as_ptr(), std::ptr::null_mut());
            nw_path_monitor_set_queue(monitor, queue);
            nw_path_monitor_set_update_handler(monitor, &handler);
            nw_path_monitor_start(monitor);
            Self { monitor, queue }
        };
        described.recv().map_err(|_gone| {
            NetworkWatchError::new("the path monitor ended before describing the path")
        })?;
        Ok(watch)
    }
}

impl Drop for PathMonitor {
    fn drop(&mut self) {
        // Safety: both were created in `start` and are released once, here.
        unsafe {
            nw_path_monitor_cancel(self.monitor);
            nw_release(self.monitor);
            dispatch_release(self.queue);
        }
    }
}

/// The usable network a path describes, named by its interfaces.
fn network(path: *mut c_void) -> Option<UsableNetwork> {
    // Safety: the monitor hands its handler a valid path for the call.
    unsafe {
        if nw_path_get_status(path) != SATISFIED {
            return None;
        }
        let names = Mutex::new(Vec::new());
        let enumerate = block2::StackBlock::new(|interface: *mut c_void| -> u8 {
            let name = nw_interface_get_name(interface);
            if !name.is_null() {
                lock_unpoisoned(&names).push(CStr::from_ptr(name).to_string_lossy().into_owned());
            }
            1
        });
        nw_path_enumerate_interfaces(path, &enumerate);
        let mut names = crate::locks::into_unpoisoned(names);
        names.sort();
        Some(UsableNetwork {
            id: names.join(","),
            metered: nw_path_is_expensive(path) || nw_path_is_constrained(path),
        })
    }
}
