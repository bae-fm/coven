use super::*;

/// Where a test watch's bugs go: none of its callbacks panic.
fn logged() -> crate::task::BugSinkRef {
    Arc::new(crate::task::LoggedBugs)
}

fn state(network: &TestNetwork) -> NetworkState {
    NetworkWatch::start(&NetworkSource::Test(network.clone()), &logged()).unwrap().view().current()
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

/// A read the platform fails says can't tell, and calls the network
/// metered, since it may be; the next read that succeeds says the network
/// again: the same network is no report, another is one.
#[test]
fn a_failed_read_cannot_tell_until_the_next_one() {
    let network = TestNetwork::new(true, false);
    network.cannot_tell();
    assert_eq!(state(&network), NetworkState { usable: None, metered: true, joined: 1 });
    network.set_metered(false);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: false, joined: 1 });
    network.cannot_tell();
    network.join(false);
    assert_eq!(state(&network), NetworkState { usable: Some(true), metered: false, joined: 2 });
}

#[tokio::test]
async fn a_view_hears_each_change_and_an_unwatched_one_never_does() {
    let network = TestNetwork::new(true, false);
    let watch = NetworkWatch::start(&NetworkSource::Test(network.clone()), &logged()).unwrap();
    let mut view = watch.view();
    view.read();
    network.set_metered(true);
    view.changed().await;
    assert!(view.read().metered);

    let unwatched = NetworkWatch::unwatched();
    let view = unwatched.view();
    assert_eq!(view.current(), NetworkState { usable: None, metered: false, joined: 0 });
    // Nothing can ever change it: its sender is gone as it is made, and
    // `changed` then waits forever.
    assert!(view.0.has_changed().is_err(), "an unwatched network never changes");
}

/// Where coven watches, the watch holds what the platform says as it
/// starts, and lets the platform's registration go as it drops:
/// Network.framework's path monitor on macOS and iOS, the internet
/// connection profile's event handler on Windows, the thread following
/// NetworkManager on Linux. Linux without NetworkManager can't tell.
#[cfg(any(target_vendor = "apple", target_os = "linux", windows))]
#[test]
fn the_platform_watch_knows_the_network_as_it_starts() {
    for _ in 0..3 {
        let watch = NetworkWatch::start(&NetworkSource::Platform, &logged()).unwrap();
        let state = watch.view().current();
        #[cfg(not(target_os = "linux"))]
        assert!(state.usable.is_some(), "{state:?}");
        if state.usable.is_none() {
            assert_eq!(state, NetworkState::UNKNOWN);
        }
        assert_eq!(state.joined, u64::from(state.usable == Some(true)));
        drop(watch);
    }
}

/// A panic in what a platform callback runs can't unwind into the
/// platform, which would end the process: it goes to the bug sink, and the
/// watch can't tell, calling the network metered, until the platform says
/// again (api.md E5, E15).
#[test]
fn a_panic_in_a_platform_callback_goes_to_the_bug_sink_and_cannot_tell() {
    let sink = Mutex::new(NetworkSink::new());
    let state = lock_unpoisoned(&sink).subscribe();
    lock_unpoisoned(&sink).observe(Some(UsableNetwork { id: "wifi".into(), metered: false }));
    let bugs = crate::task::HeardBugs::default();
    heard(&sink, &bugs, "read the network's path", || panic!("the path has no interfaces"));
    assert_eq!(*state.borrow(), NetworkState { usable: None, metered: true, joined: 1 });
    assert_eq!(
        bugs.heard(),
        [crate::task::Bug {
            operation: "read the network's path",
            message: "the path has no interfaces".into()
        }]
    );
    // The platform saying again tells the network: the same one is no report.
    heard(&sink, &bugs, "read the network's path", || {
        lock_unpoisoned(&sink).observe(Some(UsableNetwork { id: "wifi".into(), metered: false }));
    });
    assert_eq!(*state.borrow(), NetworkState { usable: Some(true), metered: false, joined: 1 });
    assert_eq!(bugs.heard().len(), 1);
}
