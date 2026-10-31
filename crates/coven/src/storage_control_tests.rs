use super::*;

#[tokio::test]
async fn repeated_start_preserves_the_connector_waiting_in_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { .. } | SyncStatus::Offline { .. })
    })
    .await;
    f.handle.start_sync().await.unwrap();
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn start_after_stop_during_backoff_owns_a_replacement_connector() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| {
        matches!(s, SyncStatus::Failed { .. } | SyncStatus::Offline { .. })
    })
    .await;
    f.handle.stop_sync();
    f.handle.start_sync().await.unwrap();
    f.advance(Duration::from_secs(30));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stopping_a_failing_connector_does_not_wait_for_its_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.stop_sync();
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), f.handle.close())
        .await
        .expect("stopping must wake a connector even while its request is in flight")
        .unwrap();
}

async fn stopped_fixture() -> Fixture {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f
}

fn failed(status: &SyncStatus) -> bool {
    matches!(
        status,
        SyncStatus::Failed { .. } | SyncStatus::Offline { .. }
    )
}

#[tokio::test]
async fn connection_backoff_keeps_its_deadline_and_exponent_across_reopening() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    for failures in 1..=4 {
        let wait = f
            .handle
            .test_connection_retry_wait()
            .await
            .expect("persisted connection wait");
        assert_eq!(wait.failures, failures);
        assert_eq!(wait.wait, Duration::from_secs(1 << (failures - 1)));
        if failures < 4 {
            *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
            let mut shown = f.handle.subscribe_sync_status();
            shown.borrow_and_update();
            f.advance(wait.wait);
            shown.changed().await.unwrap();
            status(&f.handle, failed).await;
        }
    }
    let persisted = f.handle.test_connection_retry_wait().await.unwrap();
    f.advance(Duration::from_secs(4));
    f.handle.close().await.unwrap();
    let reopened = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id(), |_| ())
    .await
    .unwrap();
    assert_eq!(reopened.test_connection_retry_wait().await, Some(persisted));
    let attempts = *f.connector.attempts.borrow();
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    reopened.start_sync().await.unwrap();
    f.advance(Duration::from_secs(3));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        *f.connector.attempts.borrow(),
        attempts,
        "four seconds remain after reopening"
    );
    f.advance(Duration::from_secs(1));
    status(&reopened, failed).await;
    let next = reopened.test_connection_retry_wait().await.unwrap();
    assert_eq!(next.failures, 5);
    assert_eq!(next.wait, Duration::from_secs(16));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_connector_failure_is_offline() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.with_source(
        std::io::Error::new(std::io::ErrorKind::NotConnected, "offline"),
    ));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    assert!(
        matches!(&*f.handle.subscribe_sync_status().borrow(), SyncStatus::Offline { error }
        if std::error::Error::source(error.as_ref()).unwrap().is::<std::io::Error>())
    );
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn connector_failures_wait_for_new_credentials_without_timers() {
    for failure in [StorageFailure::Authentication, StorageFailure::Protocol] {
        let f = stopped_fixture().await;
        *f.connector.failure.lock().unwrap() = Some(failure.into());
        f.handle.start_sync().await.unwrap();
        status(&f.handle, failed).await;
        assert!(f.handle.test_connection_retry_wait().await.is_none());
        let attempts = *f.connector.attempts.borrow();
        f.advance(Duration::from_secs(600));
        f.handle.sync_now();
        f.handle.start_sync().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *f.connector.attempts.borrow(),
            attempts,
            "{failure:?} must not retry by time or repeated start"
        );
        f.handle
            .update_credentials("replacement".into(), SecretText::new("secret".into()))
            .await
            .unwrap();
        status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
        f.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_connected_pass_waits_for_new_credentials_after_authentication_failure() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage
        .set_faults(Faults {
            read_failures: [(
                ObjectPath::floor(f.directory.id()),
                StorageFailure::Authentication,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, failed).await;
    assert!(f.handle.test_connection_retry_wait().await.is_none());
    let requests = f.storage.request_count();
    f.advance(Duration::from_secs(600));
    f.handle.sync_now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(f.storage.request_count(), requests);
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stopping_an_inflight_failure_preserves_its_delay_for_the_next_start() {
    let f = stopped_fixture().await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    f.handle.stop_sync();
    resume.send(()).unwrap();
    f.handle.start_sync().await.unwrap();
    assert_eq!(
        f.handle.test_connection_retry_wait().await.unwrap().wait,
        Duration::from_secs(1)
    );
    f.advance(Duration::from_secs(1));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn credentials_changed_during_a_failed_pass_are_evidence_for_its_retry() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let floor = ObjectPath::floor(f.directory.id());
    f.storage
        .set_faults(Faults {
            read_failures: [(floor.clone(), StorageFailure::Authentication)].into(),
            ..Faults::none()
        })
        .await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(floor, entered, resumed).await;
    f.handle.sync_now();
    entering.await.unwrap();
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    f.storage.set_faults(Faults::none()).await;
    resume.send(()).unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

/// What `retry_now` came to, within the test's patience.
async fn answer(answer: RetryAnswer) -> RetryOutcome {
    tokio::time::timeout(Duration::from_secs(10), answer)
        .await
        .expect("the retry was answered")
}

/// Ana pulls to refresh: the pass her retry runs answers her (§6.7).
#[tokio::test]
async fn a_retry_is_answered_by_the_pass_it_runs() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Synced { finished_at } if finished_at == f.clock.now()),
        "{outcome:?}"
    );
    f.handle.close().await.unwrap();
}

/// Ben pulls to refresh while a pass is already running. That pass read the
/// store before he asked, so it is not his answer: it ends synced, and the
/// next pass, which the provider refuses, answers him (IO
/// `running_pass_skips`).
#[tokio::test]
async fn a_retry_during_a_pass_is_answered_by_the_next_pass() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let floor = ObjectPath::floor(f.directory.id());
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(floor, entered, resumed).await;
    f.handle.sync_now();
    entering.await.unwrap();
    let retried = f.handle.retry_now();
    // The held read's response is fixed; the next request is refused.
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::PermissionDenied,
            ..Faults::none()
        })
        .await;
    resume.send(()).unwrap();
    let outcome = answer(retried).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::PermissionDenied),
        "{outcome:?}"
    );
    f.handle.close().await.unwrap();
}

/// A connection attempt already running when Ana and Ben pull to refresh
/// fails, but it started before they asked: the next attempt connects, and
/// the loop's first pass answers both of them (IO
/// `concurrent_requests_share`).
#[tokio::test]
async fn retries_during_a_connection_attempt_share_the_next_pass() {
    let f = stopped_fixture().await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    let attempts = *f.connector.attempts.borrow();
    let ana = f.handle.retry_now();
    let ben = f.handle.retry_now();
    resume.send(()).unwrap();
    for outcome in [answer(ana).await, answer(ben).await] {
        assert!(
            matches!(outcome, RetryOutcome::Synced { .. }),
            "{outcome:?}"
        );
    }
    // The failed attempt and one more, for both requests.
    assert_eq!(*f.connector.attempts.borrow(), attempts + 1);
    f.handle.close().await.unwrap();
}

/// The provider asked for a cooldown: a pull to refresh says when it ends,
/// at once, and runs no pass (IO `held_answers`).
#[tokio::test]
async fn a_retry_during_a_cooldown_says_when_it_ends() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, failed).await;
    let requests = f.storage.request_count();
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::CoolingDown { until }
            if until == f.clock.now() + Duration::from_secs(200)),
        "{outcome:?}"
    );
    assert_eq!(
        f.storage.request_count(),
        requests,
        "a pass ran in the cooldown"
    );
    f.handle.close().await.unwrap();
}

/// A failed attempt answers with its cause; a failure only new credentials
/// can clear answers with itself at once, making no attempt.
#[tokio::test]
async fn failed_and_held_attempts_answer_failed_with_their_cause() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.with_source(
        std::io::Error::new(std::io::ErrorKind::NotConnected, "offline"),
    ));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.with_source(
        std::io::Error::new(std::io::ErrorKind::NotConnected, "offline"),
    ));
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Failed(PassFailure::Offline { .. })),
        "{outcome:?}"
    );
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Authentication.into());
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    let attempts = *f.connector.attempts.borrow();
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(*f.connector.attempts.borrow(), attempts);
    f.handle.close().await.unwrap();
}

/// Stopping answers a retry still waiting `NotSyncing`, and leaves the
/// attempt already running to answer its own; so does asking while
/// stopped, or on a closed handle (IO `stop_lets_pass_answer`,
/// `stopped_answers_at_once`).
#[tokio::test]
async fn stopping_answers_waiting_retries_not_syncing() {
    let f = stopped_fixture().await;
    let outcome = answer(f.handle.retry_now()).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    // This retry ends the backoff; the attempt it starts takes it.
    let taken = f.handle.retry_now();
    entering.await.unwrap();
    let waiting = f.handle.retry_now();
    f.handle.stop_sync();
    let outcome = answer(waiting).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
    resume.send(()).unwrap();
    let outcome = answer(taken).await;
    assert!(matches!(outcome, RetryOutcome::Failed(_)), "{outcome:?}");
    f.handle.close().await.unwrap();
    let outcome = answer(f.handle.retry_now()).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
}

/// What the handle publishes a retry would do, as an app reads it now.
fn effect(f: &Fixture) -> RetryEffect {
    *f.handle.subscribe_retry_effect().borrow()
}

/// Wait, as an app watching the effect would, until it is `expected`.
async fn effect_becomes(f: &Fixture, expected: RetryEffect) {
    let mut effect = f.handle.subscribe_retry_effect();
    let waited =
        tokio::time::timeout(Duration::from_secs(10), effect.wait_for(|e| *e == expected)).await;
    assert!(
        waited.is_ok_and(|changed| changed.is_ok()),
        "the effect stayed {:?}",
        *effect.borrow()
    );
}

/// Ana's app offers Retry Now only while the published effect is a pass.
/// The provider answers 429 with Retry-After: before the status shows the
/// failure, the effect says when the cooldown ends, and her retry is
/// answered with that end at once. When the cooldown ends, the effect is a
/// pass again with no one asking, and her retry runs one.
#[tokio::test]
async fn a_cooldown_is_published_as_the_retry_answers_it() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    assert_eq!(effect(&f), RetryEffect::Pass);
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Synced { .. }),
        "{outcome:?}"
    );
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, failed).await;
    let until = f.clock.now() + Duration::from_secs(200);
    assert_eq!(effect(&f), RetryEffect::CoolingDown { until });
    let requests = f.storage.request_count();
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::CoolingDown { until: at } if at == until),
        "{outcome:?}"
    );
    assert_eq!(
        f.storage.request_count(),
        requests,
        "a pass ran in the cooldown"
    );
    f.advance(Duration::from_secs(200));
    effect_becomes(&f, RetryEffect::Pass).await;
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Synced { .. }),
        "{outcome:?}"
    );
    f.handle.close().await.unwrap();
}

/// A connected pass fails authentication, which only new credentials can
/// clear: before the status shows it, the effect says a retry would fail,
/// and Ben's retry is answered with that failure at once, reaching no
/// storage. New credentials publish a pass again, and his retry runs one.
#[tokio::test]
async fn a_failure_only_new_credentials_clear_is_published_as_the_retry_answers_it() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage
        .set_faults(Faults {
            read_failures: [(
                ObjectPath::floor(f.directory.id()),
                StorageFailure::Authentication,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, failed).await;
    assert_eq!(effect(&f), RetryEffect::Failed);
    let requests = f.storage.request_count();
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(f.storage.request_count(), requests, "the held pass ran");
    assert_eq!(effect(&f), RetryEffect::Failed);
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    effect_becomes(&f, RetryEffect::Pass).await;
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Synced { .. }),
        "{outcome:?}"
    );
    f.handle.close().await.unwrap();
}

/// The connection's first attempt publishes what a retry would do too. An
/// offline backoff leaves a pass: Carol's retry ends it and the attempt it
/// runs answers her. An authentication failure is published before she
/// hears of it, and her next retry is answered with it, making no attempt.
/// Stopping and closing publish that sync isn't running, as the retry
/// answers then.
#[tokio::test]
async fn the_first_attempt_publishes_what_its_retries_get() {
    let f = stopped_fixture().await;
    assert_eq!(effect(&f), RetryEffect::NotSyncing);
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.with_source(
        std::io::Error::new(std::io::ErrorKind::NotConnected, "offline"),
    ));
    f.handle.start_sync().await.unwrap();
    status(&f.handle, failed).await;
    assert_eq!(effect(&f), RetryEffect::Pass);
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Authentication.into());
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(effect(&f), RetryEffect::Failed);
    let attempts = *f.connector.attempts.borrow();
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(*f.connector.attempts.borrow(), attempts);
    f.handle.stop_sync();
    assert_eq!(effect(&f), RetryEffect::NotSyncing);
    let outcome = answer(f.handle.retry_now()).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
    f.handle.close().await.unwrap();
    assert_eq!(effect(&f), RetryEffect::NotSyncing);
}

/// Two passes answered 429 with a one-second Retry-After leave a two-second
/// connection backoff that outlasts the cooldown. Dan stops and starts sync,
/// so the connection's first attempt waits them out. When the cooldown ends,
/// the effect is a pass again with no one asking, though the backoff still
/// holds the attempt: his retry would end it, and it does.
#[tokio::test]
async fn the_first_attempt_publishes_a_cooldowns_end_within_its_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.storage
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::RateLimited,
            retry_after: Some(1),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    status(&f.handle, failed).await;
    let mut shown = f.handle.subscribe_sync_status();
    shown.borrow_and_update();
    f.advance(Duration::from_secs(1));
    shown.changed().await.unwrap();
    status(&f.handle, failed).await;
    let backoff = f.handle.test_connection_retry_wait().await.unwrap();
    assert_eq!(
        (backoff.failures, backoff.wait),
        (2, Duration::from_secs(2))
    );
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    assert_eq!(effect(&f), RetryEffect::NotSyncing);
    f.handle.start_sync().await.unwrap();
    let until = f.clock.now() + Duration::from_secs(1);
    effect_becomes(&f, RetryEffect::CoolingDown { until }).await;
    // Starting publishes the cooldown before the attempt plans; let it plan
    // and wait, so the cooldown ends while it waits.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let attempts = *f.connector.attempts.borrow();
    f.advance(Duration::from_secs(1));
    effect_becomes(&f, RetryEffect::Pass).await;
    assert_eq!(
        *f.connector.attempts.borrow(),
        attempts,
        "the backoff still holds the attempt"
    );
    let outcome = answer(f.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Synced { .. }),
        "{outcome:?}"
    );
    f.handle.close().await.unwrap();
}

/// An upload's Retry-After begins a cooldown while the loop is idle: what
/// the person's retry would do says when it ends, and returns to a pass
/// when it ends, before any idle pass and with no pass requested, so an app
/// offers Retry Now again without polling.
#[tokio::test]
async fn an_uploads_cooldown_is_published_until_it_ends() {
    use crate::tests::{attach, file_network, wait_for_empty_queue};
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    let mut effect = device.handle.subscribe_retry_effect();
    assert_eq!(*effect.borrow_and_update(), RetryEffect::Pass);
    device
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            // Two hundred seconds pass in two hundred milliseconds here.
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    device.handle.set_uploads_paused(false).await.unwrap();
    let until = network.clock.now() + Duration::from_secs(200);
    tokio::time::timeout(Duration::from_secs(5), async {
        effect
            .wait_for(|effect| *effect == RetryEffect::CoolingDown { until })
            .await
            .unwrap();
    })
    .await
    .expect("the cooldown was published");
    // The upload waits, so its retry after the cooldown wakes nothing. Idle
    // passes come every thirty seconds here; the cooldown's end comes first.
    device.handle.set_uploads_paused(true).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        effect
            .wait_for(|effect| *effect == RetryEffect::Pass)
            .await
            .unwrap();
    })
    .await
    .expect("the cooldown's end was published");
    let outcome = answer(device.handle.retry_now()).await;
    assert!(
        matches!(outcome, RetryOutcome::Synced { .. }),
        "{outcome:?}"
    );
    device.handle.set_uploads_paused(false).await.unwrap();
    wait_for_empty_queue(&device.handle).await;
    network.close().await;
}

/// The effect is published before the status a failed pass leaves: an app
/// reading it the moment the failure shows already sees that a retry would
/// fail. A network report during the pass makes the owner read the database
/// before it next plans, so only the publication the pass makes as it ends
/// can be in place by then.
#[tokio::test]
async fn a_failed_pass_publishes_its_effect_before_its_status() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let floor = ObjectPath::floor(f.directory.id());
    f.storage
        .set_faults(Faults {
            read_failures: [(floor.clone(), StorageFailure::Authentication)].into(),
            ..Faults::none()
        })
        .await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(floor, entered, resumed).await;
    f.handle.sync_now();
    entering.await.unwrap();
    f.handle.network_changed();
    let mut shown = f.handle.subscribe_sync_status();
    shown.borrow_and_update();
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !failed(&shown.borrow_and_update()) {
            shown.changed().await.unwrap();
        }
    })
    .await
    .expect("the pass failed");
    assert_eq!(effect(&f), RetryEffect::Failed);
    f.handle.close().await.unwrap();
}
