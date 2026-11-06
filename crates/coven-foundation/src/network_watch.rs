//! The device's network, as the platform describes it (§6.7, api.md E5).
//!
//! Each open store watches it from opening to closing. The platform says
//! which usable network the device is on, if any, and whether it is metered;
//! the watch keeps that, and counts each time a usable network appears or
//! the device moves to another one. Each such count is a network report,
//! which ends the waits that followed an attempt with no answer from
//! storage. Losing the network, or a network turning metered or unmetered,
//! counts nothing. Where the platform can't tell, the watch says so: no
//! network counts and none is metered. A platform that fails to answer
//! after the watch started can't tell either, but its network may be
//! metered, so the watch calls it metered until the platform answers.

// The platforms coven watches; elsewhere it can't tell.
#[cfg(target_os = "android")]
#[path = "network_watch_android.rs"]
mod android;
#[cfg(target_vendor = "apple")]
#[path = "network_watch_apple.rs"]
mod apple;
#[cfg(any(test, target_os = "android"))]
#[path = "network_watch_default.rs"]
mod default_network;
#[cfg(target_os = "linux")]
#[path = "network_watch_linux.rs"]
mod linux;
#[cfg(windows)]
#[path = "network_watch_windows.rs"]
mod winrt;

#[cfg(any(
    test,
    feature = "test-utils",
    target_vendor = "apple",
    target_os = "android",
    target_os = "linux",
    windows
))]
use crate::locks::lock_unpoisoned;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// What coven knows of the device's network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkState {
    /// Whether the device has a usable network; `None` where the platform
    /// can't tell.
    pub usable: Option<bool>,
    /// Whether coven treats that network as metered: the platform says it
    /// is, or failed to answer since the watch started, so it may be. False
    /// with no network, and where the platform never tells.
    pub metered: bool,
    /// How many times a usable network has appeared, or the device moved to
    /// another one, since the watch started, the network it found at the
    /// start included: each is a network report (§6.7).
    pub joined: u64,
}

impl NetworkState {
    /// The state where the platform can't tell.
    const UNKNOWN: Self = Self { usable: None, metered: false, joined: 0 };
}

/// One usable network, as the platform names it.
// Only the platforms coven watches, and a test's network, say anything.
#[cfg(any(
    test,
    feature = "test-utils",
    target_vendor = "apple",
    target_os = "android",
    target_os = "linux",
    windows
))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UsableNetwork {
    /// The platform's own name for the network: another name is another
    /// network.
    pub(crate) id: String,
    /// Whether the platform says it is metered.
    pub(crate) metered: bool,
}

/// Turns what the platform says into the watch's state, counting each
/// network that appears.
#[cfg(any(
    test,
    feature = "test-utils",
    target_vendor = "apple",
    target_os = "android",
    target_os = "linux",
    windows
))]
pub(crate) struct NetworkSink {
    /// The usable network the device was last on.
    current: Option<String>,
    state: watch::Sender<NetworkState>,
}

#[cfg(any(
    test,
    feature = "test-utils",
    target_vendor = "apple",
    target_os = "android",
    target_os = "linux",
    windows
))]
impl NetworkSink {
    fn new() -> Self {
        Self { current: None, state: watch::channel(NetworkState::UNKNOWN).0 }
    }

    /// The platform says the device is on `network`, or on none.
    pub(crate) fn observe(&mut self, network: Option<UsableNetwork>) {
        let mut next = *self.state.borrow();
        match network {
            Some(network) => {
                if self.current.as_ref() != Some(&network.id) {
                    next.joined += 1;
                }
                next.usable = Some(true);
                next.metered = network.metered;
                self.current = Some(network.id);
            }
            None => {
                next.usable = Some(false);
                next.metered = false;
                self.current = None;
            }
        }
        self.state.send_if_modified(|state| {
            let changed = *state != next;
            *state = next;
            changed
        });
    }

    /// The platform failed to say: a read it answered with an error after
    /// the watch started. Coven can't tell until a read succeeds, and calls
    /// the network metered meanwhile: it may be, and the setting to move
    /// files only on unmetered networks moves them only on one known to be
    /// unmetered. The network the device was last on stays its last, so
    /// that network read again is no report: nothing said it changed.
    pub(crate) fn cannot_tell(&mut self) {
        self.state.send_if_modified(|state| {
            let next = NetworkState { usable: None, metered: true, joined: state.joined };
            let changed = *state != next;
            *state = next;
            changed
        });
    }

    fn subscribe(&self) -> watch::Receiver<NetworkState> {
        self.state.subscribe()
    }

    /// The platform says the network the device is on may work again
    /// though it never changed, as Android before 10 says when Doze ends:
    /// a network report, if the device is on a usable one.
    #[cfg(any(test, target_os = "android"))]
    pub(crate) fn network_returned(&mut self) {
        if self.current.is_some() {
            self.state.send_modify(|state| state.joined += 1);
        }
    }
}

/// Run `read`, what a platform's callback does with what the platform says
/// of the network. A panic unwinding out of a callback the platform calls
/// would end the process; here it goes to `bugs` (api.md E5, E15).
#[cfg(any(test, target_vendor = "apple", target_os = "android", windows))]
pub(crate) fn heard(
    sink: &Mutex<NetworkSink>,
    bugs: &dyn crate::task::BugSink,
    operation: &'static str,
    read: impl FnOnce(),
) {
    if let Err(bug) = crate::task::caught_sync(operation, read) {
        panicked(sink, bugs, &bug);
    }
}

/// The watch's reading of what the platform says panicked with `bug`: it
/// goes to `bugs`, and the watch can't tell until the platform says again,
/// calling the network metered meanwhile, as after a failed read.
#[cfg(any(test, target_vendor = "apple", target_os = "android", target_os = "linux", windows))]
pub(crate) fn panicked(
    sink: &Mutex<NetworkSink>,
    bugs: &dyn crate::task::BugSink,
    bug: &crate::task::Bug,
) {
    lock_unpoisoned(sink).cannot_tell();
    crate::task::report(bugs, bug);
}

/// Where the store's watch hears about the network: the platform's own, or
/// a test's.
#[derive(Clone)]
pub enum NetworkSource {
    /// What the platform this runs on says (api.md E5).
    Platform,
    /// A network a test drives (api.md E1 `network`).
    #[cfg(any(test, feature = "test-utils"))]
    Test(TestNetwork),
}

/// The platform refused to let coven watch the device's network (api.md
/// E1, E5), such as an Android app without coven's Android library.
#[derive(Clone, Debug, thiserror::Error)]
pub enum NetworkWatchError {
    /// The platform's path monitor couldn't be created.
    #[error("the platform's path monitor could not be created")]
    MonitorUnavailable,
    /// The platform's path monitor ended before it described the network.
    #[error("the platform's path monitor ended before describing the network")]
    MonitorEnded,
    /// A call to the platform's network interface failed.
    #[error("coven can't watch the device's network: `{call}` failed")]
    Platform {
        /// The call that failed.
        call: &'static str,
        /// What the platform answered.
        #[source]
        source: std::sync::Arc<dyn std::error::Error + Send + Sync>,
    },
}

#[cfg(any(target_os = "android", target_os = "linux", windows))]
impl NetworkWatchError {
    /// The platform's answer to a failed `call`.
    pub(crate) fn platform(
        call: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Platform { call, source: std::sync::Arc::new(source) }
    }
}

/// One open store's watch of the device's network (§6.7): it starts as the
/// store opens, already holding what the platform says, and stops as it
/// closes. Each owner that acts on the network holds a view of it.
pub struct NetworkWatch {
    state: watch::Receiver<NetworkState>,
    /// The platform's registration, which ends as the watch drops.
    #[cfg(target_vendor = "apple")]
    _platform: Option<apple::PathMonitor>,
    #[cfg(target_os = "android")]
    _platform: Option<android::JavaWatch>,
    #[cfg(target_os = "linux")]
    _platform: Option<linux::ManagerWatch>,
    #[cfg(windows)]
    _platform: Option<winrt::ProfileWatch>,
}

impl NetworkWatch {
    /// Start watching, as `source` says. The state already holds what the
    /// platform says now. A panic in what the platform's callbacks run goes
    /// to `bugs`.
    pub fn start(
        source: &NetworkSource,
        #[cfg_attr(
            not(any(
                target_vendor = "apple",
                target_os = "android",
                target_os = "linux",
                windows
            )),
            expect(unused_variables, reason = "where coven asks nothing, nothing calls back")
        )]
        bugs: &crate::task::BugSinkRef,
    ) -> Result<Self, NetworkWatchError> {
        match source {
            #[cfg(any(
                target_vendor = "apple",
                target_os = "android",
                target_os = "linux",
                windows
            ))]
            NetworkSource::Platform => {
                let sink = Arc::new(Mutex::new(NetworkSink::new()));
                let state = lock_unpoisoned(&sink).subscribe();
                #[cfg(target_vendor = "apple")]
                let platform = Some(apple::PathMonitor::start(sink, bugs.clone())?);
                #[cfg(target_os = "android")]
                let platform = Some(android::JavaWatch::start(sink, bugs.clone())?);
                #[cfg(windows)]
                let platform = Some(winrt::ProfileWatch::start(sink, bugs.clone())?);
                // None without NetworkManager: the state stays can't tell.
                #[cfg(target_os = "linux")]
                let platform = linux::ManagerWatch::start(sink, bugs.clone())?;
                Ok(Self { state, _platform: platform })
            }
            // Elsewhere, as on the BSDs, coven asks nothing and can't tell
            // (api.md E5).
            #[cfg(not(any(
                target_vendor = "apple",
                target_os = "android",
                target_os = "linux",
                windows
            )))]
            NetworkSource::Platform => Ok(Self::unwatched()),
            #[cfg(any(test, feature = "test-utils"))]
            NetworkSource::Test(network) => Ok(Self::without_platform(network.subscribe())),
        }
    }

    /// A watch that hears nothing, for work that transfers no file the
    /// network holds: a read-only handle's, and loading's (api.md E4).
    pub fn unwatched() -> Self {
        Self::without_platform(watch::channel(NetworkState::UNKNOWN).1)
    }

    /// A watch of `state` with no platform registration.
    fn without_platform(state: watch::Receiver<NetworkState>) -> Self {
        Self {
            state,
            #[cfg(any(
                target_vendor = "apple",
                target_os = "android",
                target_os = "linux",
                windows
            ))]
            _platform: None,
        }
    }

    /// A view of the network, for an owner that acts on it.
    pub fn view(&self) -> NetworkView {
        NetworkView(self.state.clone())
    }
}

/// Hears what the store's watch knows of the network (§6.7).
#[derive(Clone)]
pub struct NetworkView(watch::Receiver<NetworkState>);

impl NetworkView {
    /// What the watch knows now.
    pub fn current(&self) -> NetworkState {
        *self.0.borrow()
    }

    /// Resolves once the state changes after the one last read through this
    /// view; never once the watch is gone.
    pub async fn changed(&mut self) {
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// The state, marking it read: `changed` then waits for a later one.
    pub fn read(&mut self) -> NetworkState {
        *self.0.borrow_and_update()
    }
}

/// A network a test drives in place of the platform's (api.md E1
/// `network`): clones drive the same network.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct TestNetwork {
    state: Arc<Mutex<TestNetworkState>>,
}

/// What a test's network says, and how many networks it has named.
#[cfg(any(test, feature = "test-utils"))]
struct TestNetworkState {
    sink: NetworkSink,
    named: u64,
}

#[cfg(any(test, feature = "test-utils"))]
impl TestNetwork {
    /// The network as the platform would describe it as the store opens.
    pub fn new(usable: bool, metered: bool) -> Self {
        let network = Self {
            state: Arc::new(Mutex::new(TestNetworkState { sink: NetworkSink::new(), named: 0 })),
        };
        if usable {
            network.join(metered);
        } else {
            network.lose();
        }
        network
    }

    fn subscribe(&self) -> watch::Receiver<NetworkState> {
        lock_unpoisoned(&self.state).sink.subscribe()
    }

    /// The device moves to another usable network: a network report.
    pub fn join(&self, metered: bool) {
        let mut state = lock_unpoisoned(&self.state);
        state.named += 1;
        let id = format!("test network {}", state.named);
        state.sink.observe(Some(UsableNetwork { id, metered }));
    }

    /// The device loses its network: no report.
    pub fn lose(&self) {
        lock_unpoisoned(&self.state).sink.observe(None);
    }

    /// The platform fails to say what the network is: coven can't tell, and
    /// calls the network metered, until the test says again.
    pub fn cannot_tell(&self) {
        lock_unpoisoned(&self.state).sink.cannot_tell();
    }

    /// The same network turns metered or unmetered: no report.
    pub fn set_metered(&self, metered: bool) {
        let mut state = lock_unpoisoned(&self.state);
        let Some(id) = state.sink.current.clone() else {
            panic!("a test network turns metered only while the device is on one");
        };
        state.sink.observe(Some(UsableNetwork { id, metered }));
    }
}

#[cfg(test)]
#[path = "network_watch_tests.rs"]
mod tests;
