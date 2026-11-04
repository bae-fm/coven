//! The device's default network as Android reports it, piece by piece
//! (api.md E5): which network is the default, whether it is validated,
//! whether Android blocks it for the app, as Doze and App Standby do, and
//! whether it is metered. The default network is usable once validated and
//! not blocked; another default network is another network. Each report
//! settles what the watch's sink hears.

use super::{NetworkSink, UsableNetwork};
use std::sync::{Arc, Mutex};

/// What Android last said of the default network.
pub(crate) struct DefaultNetwork {
    sink: Arc<Mutex<NetworkSink>>,
    /// The default network, by Android's handle for it.
    network: Option<i64>,
    validated: bool,
    blocked: bool,
    metered: bool,
}

impl DefaultNetwork {
    /// No default network yet.
    pub(crate) fn new(sink: Arc<Mutex<NetworkSink>>) -> Self {
        Self { sink, network: None, validated: false, blocked: false, metered: false }
    }

    /// `network` became the default; its capabilities and blocked state
    /// follow. Android says so again of the default network as the watch
    /// registers, after the watch read it itself: the same network again
    /// changes nothing.
    pub(crate) fn available(&mut self, network: i64) {
        if self.network == Some(network) {
            return;
        }
        self.network = Some(network);
        self.validated = false;
        self.blocked = false;
        self.metered = false;
        self.settle();
    }

    /// The default network's capabilities; another network's are ignored.
    pub(crate) fn capabilities(&mut self, network: i64, validated: bool, metered: bool) {
        if self.network == Some(network) {
            self.validated = validated;
            self.metered = metered;
            self.settle();
        }
    }

    /// Whether Android blocks the default network for the app.
    pub(crate) fn blocked(&mut self, network: i64, blocked: bool) {
        if self.network == Some(network) {
            self.blocked = blocked;
            self.settle();
        }
    }

    /// `network` is gone; `None` when Android has no default network at all.
    pub(crate) fn lost(&mut self, network: Option<i64>) {
        if network.is_none() || self.network == network {
            self.network = None;
            self.settle();
        }
    }

    /// Doze ended, which Android before 10 reports no other way: a usable
    /// network may work again.
    pub(crate) fn awake(&mut self) {
        self.sink.lock().expect("network sink poisoned").network_returned();
    }

    fn settle(&self) {
        let usable = self.network.filter(|_| self.validated && !self.blocked);
        self.sink.lock().expect("network sink poisoned").observe(
            usable.map(|network| UsableNetwork { id: network.to_string(), metered: self.metered }),
        );
    }
}

#[cfg(test)]
#[path = "network_watch_default_tests.rs"]
mod tests;
