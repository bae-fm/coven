//! The device's network, as the platform describes it (§6.7, api.md E5).
//!
//! Each open store watches it from opening to closing. The platform says
//! which usable network the device is on, if any, and whether it is metered;
//! the watch keeps that, and counts each time a usable network appears or
//! the device moves to another one. Each such count is a network report,
//! which ends the waits that followed an attempt with no answer from
//! storage. Losing the network, or a network turning metered or unmetered,
//! counts nothing. Where the platform can't tell, the watch says so: no
//! network counts and none is metered.

// The platforms coven watches; elsewhere it can't tell.
#[cfg(target_vendor = "apple")]
#[path = "network_watch_apple.rs"]
mod platform;

#[cfg(any(test, feature = "test-utils", target_vendor = "apple"))]
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// What coven knows of the device's network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkState {
    /// Whether the device has a usable network; `None` where the platform
    /// can't tell.
    pub usable: Option<bool>,
    /// Whether that network is metered: false with none, and where the
    /// platform can't tell.
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
// Sentinel: the platforms whose watches the rest of this chain adds widen
// these cfgs; until then only Apple's and a test's network say anything.
#[cfg(any(test, feature = "test-utils", target_vendor = "apple"))]
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
#[cfg(any(test, feature = "test-utils", target_vendor = "apple"))]
pub(crate) struct NetworkSink {
    /// The usable network the device was last on.
    current: Option<String>,
    state: watch::Sender<NetworkState>,
}

#[cfg(any(test, feature = "test-utils", target_vendor = "apple"))]
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

    fn subscribe(&self) -> watch::Receiver<NetworkState> {
        self.state.subscribe()
    }
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
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("coven can't watch the device's network: {0}")]
pub struct NetworkWatchError(String);

#[cfg(target_vendor = "apple")]
impl NetworkWatchError {
    /// The platform's failure, as its text.
    pub(crate) fn new(failure: impl std::fmt::Display) -> Self {
        Self(failure.to_string())
    }
}

/// One open store's watch of the device's network (§6.7): it starts as the
/// store opens, already holding what the platform says, and stops as it
/// closes. Each owner that acts on the network holds a view of it.
pub struct NetworkWatch {
    state: watch::Receiver<NetworkState>,
    /// The platform's registration, which ends as the watch drops.
    #[cfg(target_vendor = "apple")]
    _platform: Option<platform::PlatformWatch>,
}

impl NetworkWatch {
    /// Start watching, as `source` says. The state already holds what the
    /// platform says now.
    pub fn start(source: &NetworkSource) -> Result<Self, NetworkWatchError> {
        match source {
            #[cfg(target_vendor = "apple")]
            NetworkSource::Platform => {
                let sink = Arc::new(Mutex::new(NetworkSink::new()));
                let state = sink.lock().expect("network sink poisoned").subscribe();
                let platform = platform::PlatformWatch::start(sink)?;
                Ok(Self { state, _platform: Some(platform) })
            }
            // Sentinel: the platforms whose watches the rest of this chain
            // adds leave this arm; until then they are ones where coven
            // can't tell.
            #[cfg(not(target_vendor = "apple"))]
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
            #[cfg(target_vendor = "apple")]
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
        self.state.lock().expect("test network poisoned").sink.subscribe()
    }

    /// The device moves to another usable network: a network report.
    pub fn join(&self, metered: bool) {
        let mut state = self.state.lock().expect("test network poisoned");
        state.named += 1;
        let id = format!("test network {}", state.named);
        state.sink.observe(Some(UsableNetwork { id, metered }));
    }

    /// The device loses its network: no report.
    pub fn lose(&self) {
        self.state.lock().expect("test network poisoned").sink.observe(None);
    }

    /// The same network turns metered or unmetered: no report.
    pub fn set_metered(&self, metered: bool) {
        let mut state = self.state.lock().expect("test network poisoned");
        let Some(id) = state.sink.current.clone() else {
            panic!("a test network turns metered only while the device is on one");
        };
        state.sink.observe(Some(UsableNetwork { id, metered }));
    }
}

#[cfg(test)]
#[path = "network_watch_tests.rs"]
mod tests;
