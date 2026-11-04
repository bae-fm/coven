use super::*;

fn state(network: &TestNetwork) -> NetworkState {
    NetworkWatch::start(&NetworkSource::Test(network.clone())).unwrap().view().current()
}

#[test]
fn a_network_found_as_the_watch_starts_counts_as_one_report() {
    let network = TestNetwork::new(true, false);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: false, joined: 1 });
    let offline = TestNetwork::new(false, false);
    assert_eq!(state(&offline), NetworkState { usable: Some(false), metered: false, joined: 0 });
}

#[test]
fn each_network_the_device_moves_to_is_a_report_and_losing_one_is_none() {
    let network = TestNetwork::new(true, false);
    network.lose();
    assert_eq!(state(&network), NetworkState { usable: Some(false), metered: false, joined: 1 });
    network.join(true);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: true, joined: 2 });
    network.join(false);
    assert_eq!(state(&network).joined, 3);
}

#[test]
fn a_network_turning_metered_is_no_report() {
    let network = TestNetwork::new(true, false);
    network.set_metered(true);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: true, joined: 1 });
}

#[test]
fn the_same_network_reported_again_counts_once() {
    let mut sink = NetworkSink::new();
    let wifi = UsableNetwork { id: "wifi".into(), metered: false };
    sink.observe(Some(wifi.clone()));
    sink.observe(Some(wifi.clone()));
    assert_eq!(sink.subscribe().borrow().joined, 1);
    sink.observe(None);
    sink.observe(Some(wifi));
    assert_eq!(sink.subscribe().borrow().joined, 2, "the network coming back is a report");
}

/// A read the platform fails says can't tell, and the next one that
/// succeeds says the network again: the same network is no report, another
/// is one.
#[test]
fn a_failed_read_cannot_tell_until_the_next_one() {
    let network = TestNetwork::new(true, true);
    network.cannot_tell();
    assert_eq!(state(&network), NetworkState { usable: None, metered: false, joined: 1 });
    network.set_metered(true);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: true, joined: 1 });
    network.cannot_tell();
    network.join(false);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: false, joined: 2 });
}

#[tokio::test]
async fn a_view_hears_each_change_and_an_unwatched_one_never_does() {
    let network = TestNetwork::new(true, false);
    let watch = NetworkWatch::start(&NetworkSource::Test(network.clone())).unwrap();
    let mut view = watch.view();
    view.read();
    network.set_metered(true);
    view.changed().await;
    assert!(view.read().metered);

    let unwatched = NetworkWatch::unwatched();
    let mut view = unwatched.view();
    assert_eq!(view.current(), NetworkState { usable: None, metered: false, joined: 0 });
    let waited = tokio::time::timeout(std::time::Duration::from_millis(10), view.changed()).await;
    assert!(waited.is_err(), "an unwatched network never changes");
}

/// Where coven watches, the watch holds what the platform says as it
/// starts, usable or not, and lets the platform's registration go as it
/// drops: Network.framework's path monitor on macOS and iOS, the internet
/// connection profile's event handler on Windows.
#[cfg(any(target_vendor = "apple", windows))]
#[test]
fn the_platform_watch_knows_the_network_as_it_starts() {
    for _ in 0..3 {
        let watch = NetworkWatch::start(&NetworkSource::Platform).unwrap();
        let state = watch.view().current();
        assert!(state.usable.is_some(), "{state:?}");
        assert_eq!(state.joined, u64::from(state.usable == Some(true)));
        drop(watch);
    }
}
