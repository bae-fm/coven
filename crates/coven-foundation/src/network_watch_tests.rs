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

/// On macOS and iOS the watch holds what Network.framework says of the path
/// as it starts, usable or not, and lets the monitor go as it drops.
#[cfg(target_vendor = "apple")]
#[test]
fn the_apple_watch_knows_the_path_as_it_starts() {
    for _ in 0..3 {
        let watch = NetworkWatch::start(&NetworkSource::Platform).unwrap();
        let state = watch.view().current();
        assert!(state.usable.is_some(), "{state:?}");
        assert_eq!(state.joined, u64::from(state.usable == Some(true)));
        drop(watch);
    }
}
