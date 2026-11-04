use super::*;
use crate::network_watch::NetworkState;

fn network() -> (DefaultNetwork, tokio::sync::watch::Receiver<NetworkState>) {
    let sink = Arc::new(Mutex::new(NetworkSink::new()));
    let state = sink.lock().unwrap().subscribe();
    (DefaultNetwork::new(sink), state)
}

fn now(state: &tokio::sync::watch::Receiver<NetworkState>) -> NetworkState {
    *state.borrow()
}

/// A default network is usable only once Android has validated it and
/// while it doesn't block it for the app; each new default network is a
/// report, and losing it is none.
#[test]
fn a_default_network_is_usable_once_validated_and_while_unblocked() {
    let (mut network, state) = network();
    network.available(100);
    assert_eq!(now(&state), NetworkState { usable: Some(false), metered: false, joined: 0 });
    network.capabilities(100, true, true);
    assert_eq!(now(&state), NetworkState { usable: Some(true), metered: true, joined: 1 });
    // Doze blocks it, then lets it go: the same network again is a report.
    network.blocked(100, true);
    assert_eq!(now(&state).usable, Some(false));
    network.blocked(100, false);
    assert_eq!(now(&state), NetworkState { usable: Some(true), metered: true, joined: 2 });
    // Another network's news changes nothing.
    network.capabilities(200, true, false);
    network.blocked(200, true);
    network.lost(Some(200));
    assert_eq!(now(&state), NetworkState { usable: Some(true), metered: true, joined: 2 });
    // Wi-Fi becomes the default, unmetered: another network.
    network.available(200);
    network.capabilities(200, true, false);
    assert_eq!(now(&state), NetworkState { usable: Some(true), metered: false, joined: 3 });
    network.lost(Some(200));
    assert_eq!(now(&state), NetworkState { usable: Some(false), metered: false, joined: 3 });
}

/// Before Android 10, Doze ending is the only sign the network works again:
/// a report while a usable network is the default, none without one.
#[test]
fn doze_ending_is_a_report_only_on_a_usable_network() {
    let (mut network, state) = network();
    network.awake();
    assert_eq!(now(&state).joined, 0);
    network.available(100);
    network.capabilities(100, true, false);
    network.awake();
    assert_eq!(now(&state).joined, 2);
    network.lost(None);
    network.awake();
    assert_eq!(now(&state), NetworkState { usable: Some(false), metered: false, joined: 2 });
}

/// The watch reads the default network, then registers its callback, and
/// Android tells the callback that same network again: one report.
#[test]
fn the_default_network_told_again_is_no_second_report() {
    let (mut network, state) = network();
    network.available(100);
    network.capabilities(100, true, false);
    network.available(100);
    network.capabilities(100, true, false);
    network.blocked(100, false);
    assert_eq!(now(&state), NetworkState { usable: Some(true), metered: false, joined: 1 });
}
