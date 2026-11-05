use super::*;
use coven_foundation::clock::HANG_GUARD;

#[tokio::test]
async fn repeated_start_preserves_the_connector_waiting_in_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, failed).await;
    f.handle.start_sync().await.unwrap();
    f.advance(Duration::from_secs(30));
    synced(&f.handle).await;
    assert_eq!(f.connector.clients.lock().unwrap().len(), 2);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn start_after_stop_during_backoff_owns_a_replacement_connector() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, failed).await;
    f.handle.stop_sync().await.unwrap();
    f.handle.start_sync().await.unwrap();
    f.advance(Duration::from_secs(30));
    synced(&f.handle).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn stopping_a_failing_connector_does_not_wait_for_its_backoff() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    entering.await.unwrap();
    let stopping = crate::tests::stopping(&f.handle);
    resume.send(()).unwrap();
    tokio::time::timeout(HANG_GUARD, stopping)
        .await
        .expect("stopping must wake a connector even while its request is in flight")
        .unwrap()
        .unwrap();
    f.handle.close().await.unwrap();
}

async fn stopped_fixture() -> Fixture {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    f
}

fn failed(last: &Option<LastPass>) -> bool {
    matches!(last, Some(LastPass::Failed { .. } | LastPass::Offline { .. }))
}

/// A connector failure whose text names its attempt, so the last pass each
/// attempt leaves differs from the one before.
fn attempt_failure(attempt: u32) -> StorageError {
    StorageFailure::Network.with_source(format!("attempt {attempt}"))
}

/// Wait until the last pass is attempt `attempt`'s failure.
async fn attempt_failed(handle: &CovenHandle, attempt: u32) {
    let text = format!("attempt {attempt}");
    last_pass(handle, |last| {
        matches!(last, Some(LastPass::Offline { error, .. } | LastPass::Failed { error, .. }) if error.contains(&text))
    })
    .await;
}

#[tokio::test]
async fn connection_backoff_keeps_its_deadline_and_exponent_across_reopening() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(attempt_failure(1));
    f.handle.start_sync().await.unwrap();
    attempt_failed(&f.handle, 1).await;
    for failures in 1..=4 {
        let wait = f.handle.test_connection_retry_wait().await.expect("persisted connection wait");
        assert_eq!(wait.failures, failures);
        assert_eq!(wait.wait, Duration::from_secs(1 << (failures - 1)));
        if failures < 4 {
            // Each attempt's failure names it, so the one shown is its own;
            // the attempt records its wait before it.
            *f.connector.failure.lock().unwrap() = Some(attempt_failure(failures + 1));
            f.advance(wait.wait);
            attempt_failed(&f.handle, failures + 1).await;
        }
    }
    let persisted = f.handle.test_connection_retry_wait().await.unwrap();
    f.advance(Duration::from_secs(4));
    f.handle.close().await.unwrap();
    // The device still has no network as it reopens: opening on one would
    // end this wait for the network (§6.7).
    f.app.lose_network();
    let reopened = builder(&f.app, f.clock.clone(), f.connector.clone())
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    assert_eq!(reopened.test_connection_retry_wait().await, Some(persisted));
    let attempts = *f.connector.attempts.borrow();
    *f.connector.failure.lock().unwrap() = Some(attempt_failure(5));
    reopened.start_sync().await.unwrap();
    f.advance(Duration::from_secs(3));
    reopened.test_parked().await;
    assert_eq!(*f.connector.attempts.borrow(), attempts, "four seconds remain after reopening");
    f.advance(Duration::from_secs(1));
    attempt_failed(&reopened, 5).await;
    let next = reopened.test_connection_retry_wait().await.unwrap();
    assert_eq!(next.failures, 5);
    assert_eq!(next.wait, Duration::from_secs(16));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn an_unanswered_connector_failure_is_offline() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(
        StorageFailure::Network
            .with_source(std::io::Error::new(std::io::ErrorKind::NotConnected, "offline")),
    );
    f.handle.start_sync().await.unwrap();
    let shown = last_pass(&f.handle, failed).await;
    assert!(
        matches!(shown, Some(LastPass::Offline { failure: StorageFailure::Network, .. })),
        "{shown:?}"
    );
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn connector_failures_wait_for_new_credentials_without_timers() {
    for failure in [StorageFailure::Authentication, StorageFailure::Protocol] {
        let f = stopped_fixture().await;
        *f.connector.failure.lock().unwrap() = Some(failure.into());
        f.handle.start_sync().await.unwrap();
        last_pass(&f.handle, failed).await;
        assert!(f.handle.test_connection_retry_wait().await.is_none());
        let attempts = *f.connector.attempts.borrow();
        f.advance(Duration::from_secs(600));
        f.handle.sync_now();
        f.handle.start_sync().await.unwrap();
        f.handle.test_parked().await;
        assert_eq!(
            *f.connector.attempts.borrow(),
            attempts,
            "{failure:?} must not retry by time or repeated start"
        );
        f.handle
            .update_credentials("replacement".into(), SecretText::new("secret".into()))
            .await
            .unwrap();
        synced(&f.handle).await;
        f.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_connected_pass_waits_for_new_credentials_after_authentication_failure() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.storage
        .set_faults(Faults {
            read_failures: [(ObjectPath::floor(f.directory.id()), StorageFailure::Authentication)]
                .into(),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    last_pass(&f.handle, failed).await;
    assert!(f.handle.test_connection_retry_wait().await.is_none());
    let requests = f.storage.request_count();
    f.advance(Duration::from_secs(600));
    f.handle.sync_now();
    f.handle.test_parked().await;
    assert_eq!(f.storage.request_count(), requests);
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    synced(&f.handle).await;
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
    let stopping = crate::tests::stopping(&f.handle);
    resume.send(()).unwrap();
    stopping.await.unwrap().unwrap();
    f.handle.start_sync().await.unwrap();
    assert_eq!(f.handle.test_connection_retry_wait().await.unwrap().wait, Duration::from_secs(1));
    f.advance(Duration::from_secs(1));
    synced_since(&f.handle, f.clock.now()).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn credentials_changed_during_a_failed_pass_are_evidence_for_its_retry() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
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
    // The clock moves, so the end of the pass after the failed one moves the
    // last synced time; an authentication failure waits for no timer.
    f.advance(Duration::from_secs(1));
    resume.send(()).unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    f.handle.close().await.unwrap();
}

/// What `retry_now` came to, within the test's patience.
async fn answer(answer: RetryAnswer) -> RetryOutcome {
    tokio::time::timeout(HANG_GUARD, answer).await.expect("the retry was answered")
}

/// Ana pulls to refresh: the pass her retry runs answers her (§6.7).
#[tokio::test]
async fn a_retry_is_answered_by_the_pass_it_runs() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
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
    synced(&f.handle).await;
    let floor = ObjectPath::floor(f.directory.id());
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(floor, entered, resumed).await;
    f.handle.sync_now();
    entering.await.unwrap();
    let retried = f.handle.retry_now().await.unwrap();
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
    let ana = f.handle.retry_now().await.unwrap();
    let ben = f.handle.retry_now().await.unwrap();
    resume.send(()).unwrap();
    for outcome in [answer(ana).await, answer(ben).await] {
        assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
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
    synced(&f.handle).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    last_pass(&f.handle, failed).await;
    let requests = f.storage.request_count();
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(
        matches!(outcome, RetryOutcome::CoolingDown { until }
            if until == f.clock.now() + Duration::from_secs(200)),
        "{outcome:?}"
    );
    assert_eq!(f.storage.request_count(), requests, "a pass ran in the cooldown");
    f.handle.close().await.unwrap();
}

/// An administrative call on a device with no storage fails with `NoStorage`
/// before it journals anything or sends a request (§11.1).
#[tokio::test]
async fn an_operation_on_a_device_without_storage_fails_before_journaling() {
    let f = Fixture::new().await;
    let circle = CircleId(uuid::Uuid::from_u128(1));
    let requests = f.storage.request_count();
    assert!(matches!(f.handle.reset_store().await, Err(SyncError::NoStorage)));
    assert!(matches!(f.handle.circles().reset(circle).await, Err(SyncError::NoStorage)));
    assert!(matches!(f.handle.reload_from_snapshot().await, Err(SyncError::NoStorage)));
    let journal: i64 = f
        .handle
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM coven_operations", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(journal, 0);
    assert_eq!(f.storage.request_count(), requests);
    f.handle.close().await.unwrap();
}

/// An operation's pass meets a provider cooldown. Retrying the operation
/// makes it due, but the next attempt still starts no request before the
/// cooldown ends (E6).
#[tokio::test]
async fn a_retried_operation_still_waits_out_a_provider_cooldown() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let (_, operation) = f.handle.circles().create("Gifts").await.unwrap();
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, failed).await;
    f.handle.retry_pending_operation(operation).await.unwrap();
    let requests = f.storage.request_count();
    let outcome = tokio::time::timeout(HANG_GUARD, answer(f.handle.retry_now().await.unwrap()))
        .await
        .expect("the retry is answered at once");
    assert!(matches!(outcome, RetryOutcome::CoolingDown { .. }), "{outcome:?}");
    assert_eq!(f.storage.request_count(), requests, "the operation started a request");

    f.advance(Duration::from_secs(201));
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
    let finished: Option<OperationOutcome> = f
        .handle
        .read(move |sql| {
            Ok(sql.query_row(
                "SELECT outcome FROM coven_operations WHERE operation=?1",
                [operation.0],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert!(finished.is_some());
    f.handle.close().await.unwrap();
}

/// A failed attempt answers with its cause; a failure only new credentials
/// can clear answers with itself at once, making no attempt.
#[tokio::test]
async fn failed_and_held_attempts_answer_failed_with_their_cause() {
    let f = stopped_fixture().await;
    *f.connector.failure.lock().unwrap() = Some(
        StorageFailure::Network
            .with_source(std::io::Error::new(std::io::ErrorKind::NotConnected, "offline")),
    );
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, failed).await;
    *f.connector.failure.lock().unwrap() = Some(
        StorageFailure::Network
            .with_source(std::io::Error::new(std::io::ErrorKind::NotConnected, "offline")),
    );
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Failed(PassFailure::Offline { .. })), "{outcome:?}");
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Authentication.into());
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    let attempts = *f.connector.attempts.borrow();
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
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
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, failed).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *f.connector.held.lock().await = Some((entered, resumed));
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.into());
    // This retry ends the backoff; the attempt it starts takes it.
    let taken = f.handle.retry_now().await.unwrap();
    entering.await.unwrap();
    let waiting = f.handle.retry_now().await.unwrap();
    // Stopping answers the waiting retry before the attempt it waits for
    // ends.
    let stopping = crate::tests::stopping(&f.handle);
    let outcome = answer(waiting).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
    resume.send(()).unwrap();
    let outcome = answer(taken).await;
    assert!(matches!(outcome, RetryOutcome::Failed(_)), "{outcome:?}");
    stopping.await.unwrap().unwrap();
    f.handle.close().await.unwrap();
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
}

/// What `coven_retry_effect` says a retry would do, as an app reads it now.
async fn effect(f: &Fixture) -> RetryEffect {
    f.handle.read(RetryEffect::read).await.unwrap()
}

/// Wait, as an app's live query on `coven_retry_effect` would, until it is
/// `expected`.
async fn effect_becomes(f: &Fixture, expected: RetryEffect) {
    f.handle.test_retry_effect().wait_for(|effect| *effect == expected).await;
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
    synced(&f.handle).await;
    assert_eq!(effect(&f).await, RetryEffect::Pass);
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    last_pass(&f.handle, failed).await;
    let until = f.clock.now() + Duration::from_secs(200);
    assert_eq!(effect(&f).await, RetryEffect::CoolingDown { until });
    let requests = f.storage.request_count();
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(
        matches!(outcome, RetryOutcome::CoolingDown { until: at } if at == until),
        "{outcome:?}"
    );
    assert_eq!(f.storage.request_count(), requests, "a pass ran in the cooldown");
    f.advance(Duration::from_secs(200));
    effect_becomes(&f, RetryEffect::Pass).await;
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
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
    synced(&f.handle).await;
    f.storage
        .set_faults(Faults {
            read_failures: [(ObjectPath::floor(f.directory.id()), StorageFailure::Authentication)]
                .into(),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    last_pass(&f.handle, failed).await;
    assert_eq!(effect(&f).await, RetryEffect::Failed);
    let requests = f.storage.request_count();
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(f.storage.request_count(), requests, "the held pass ran");
    assert_eq!(effect(&f).await, RetryEffect::Failed);
    f.storage.set_faults(Faults::none()).await;
    f.handle
        .update_credentials("replacement".into(), SecretText::new("secret".into()))
        .await
        .unwrap();
    effect_becomes(&f, RetryEffect::Pass).await;
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
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
    assert_eq!(effect(&f).await, RetryEffect::NotSyncing);
    *f.connector.failure.lock().unwrap() = Some(
        StorageFailure::Network
            .with_source(std::io::Error::new(std::io::ErrorKind::NotConnected, "offline")),
    );
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, failed).await;
    assert_eq!(effect(&f).await, RetryEffect::Pass);
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Authentication.into());
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(effect(&f).await, RetryEffect::Failed);
    let attempts = *f.connector.attempts.borrow();
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(
        matches!(&outcome, RetryOutcome::Failed(PassFailure::Failed {
            error: SyncFailure::Storage(error)
        }) if error.failure() == StorageFailure::Authentication),
        "{outcome:?}"
    );
    assert_eq!(*f.connector.attempts.borrow(), attempts);
    f.handle.stop_sync().await.unwrap();
    assert_eq!(effect(&f).await, RetryEffect::NotSyncing);
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::NotSyncing), "{outcome:?}");
    f.handle.close().await.unwrap();
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
    synced(&f.handle).await;
    f.storage
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::RateLimited,
            retry_after: Some(1),
            ..Faults::none()
        })
        .await;
    f.handle.sync_now();
    last_pass(&f.handle, failed).await;
    // The second pass's 429 begins a cooldown from the moved clock. Its
    // pass then records the backoff, and stopping waits for that pass.
    f.advance(Duration::from_secs(1));
    let cooling = RetryEffect::CoolingDown { until: f.clock.now() + Duration::from_secs(1) };
    effect_becomes(&f, cooling).await;
    stop(&f).await;
    let backoff = f.handle.test_connection_retry_wait().await.unwrap();
    assert_eq!((backoff.failures, backoff.wait), (2, Duration::from_secs(2)));
    assert_eq!(effect(&f).await, RetryEffect::NotSyncing);
    f.handle.start_sync().await.unwrap();
    let until = f.clock.now() + Duration::from_secs(1);
    effect_becomes(&f, RetryEffect::CoolingDown { until }).await;
    // Starting publishes the cooldown before the attempt plans; it plans
    // and waits, so the cooldown ends while it waits.
    f.handle.test_parked().await;
    let attempts = *f.connector.attempts.borrow();
    f.advance(Duration::from_secs(1));
    effect_becomes(&f, RetryEffect::Pass).await;
    assert_eq!(*f.connector.attempts.borrow(), attempts, "the backoff still holds the attempt");
    let outcome = answer(f.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
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
    let mut effect = device.handle.test_retry_effect();
    assert_eq!(effect.latest().await, RetryEffect::Pass);
    // The cooldown ends only as the test moves the clock.
    network.polling.hold_waits();
    device
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    device.handle.set_uploads_paused(false).await.unwrap();
    let until = network.clock.now() + Duration::from_secs(200);
    effect.wait_for(|effect| *effect == RetryEffect::CoolingDown { until }).await;
    // The upload waits, so its retry after the cooldown wakes nothing. Idle
    // passes come every thirty seconds here; the cooldown's end comes first.
    device.handle.set_uploads_paused(true).await.unwrap();
    network.clock.set(until);
    effect.wait_for(|effect| *effect == RetryEffect::Pass).await;
    let outcome = answer(device.handle.retry_now().await.unwrap()).await;
    assert!(matches!(outcome, RetryOutcome::Synced { .. }), "{outcome:?}");
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
    synced(&f.handle).await;
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
    f.app.join_network(false);
    // The pass before the held one ended synced, so the failure is the held
    // pass's own.
    let mut shown = f.handle.test_last_pass();
    resume.send(()).unwrap();
    shown.wait_for(failed).await;
    assert_eq!(effect(&f).await, RetryEffect::Failed);
    f.handle.close().await.unwrap();
}

/// Stopping returns once the pass running as it was asked has ended and the
/// provider's client is released; it finishes without its caller (E5).
#[tokio::test]
async fn stopping_returns_after_the_running_pass_and_the_release() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(ObjectPath::floor(f.directory.id()), entered, resumed).await;
    f.handle.sync_now();
    entering.await.unwrap();
    let stopping = crate::tests::stopping(&f.handle);
    // Stopping's first write says sync isn't running; then it waits.
    f.handle.test_retry_effect().wait_for(|effect| *effect == RetryEffect::NotSyncing).await;
    assert!(!stopping.is_finished(), "stopping returned while its pass ran");
    // A caller that stops waiting leaves stopping to finish.
    stopping.abort();
    resume.send(()).unwrap();
    f.connector.counted.released().await;
    // Stopping has recorded the location too, under the call lock.
    f.handle.storage_config().await.unwrap();
    f.handle.close().await.unwrap();
}

/// A start right after a stop connects again: the stop it follows has
/// released the client, whatever the order the two calls ran in.
#[tokio::test]
async fn a_start_right_after_a_stop_connects_a_new_client() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.handle.stop_sync().await.unwrap();
    f.advance(Duration::from_secs(1));
    f.handle.start_sync().await.unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    let clients: Vec<bool> =
        f.connector.clients.lock().unwrap().iter().map(|c| c.upgrade().is_some()).collect();
    assert_eq!(clients, [false, true]);
    f.handle.close().await.unwrap();
}

/// `coven_sync_status.starts` and `last_pass`, as an app reads them now.
async fn starts_and_last_pass(f: &Fixture) -> (i64, Option<Vec<u8>>) {
    f.handle
        .read(|sql| {
            Ok(sql.query_row("SELECT starts,last_pass FROM coven_sync_status", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?)
        })
        .await
        .unwrap()
}

/// Dan's key is refused, so his start holds on a failure only new
/// credentials clear. He stops sync and starts it again: the second start
/// holds for the same reason and ends the way the first did, yet it still
/// writes the row, which counts it, so an app's trigger that clears its
/// record of a failed start hears it (E5).
#[tokio::test]
async fn every_start_counts_even_one_held_for_the_reason_the_last_was() {
    let f = stopped_fixture().await;
    let (before, _) = starts_and_last_pass(&f).await;
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Authentication.into());
    f.handle.start_sync().await.unwrap();
    assert_eq!(starts_and_last_pass(&f).await.0, before + 1);
    last_pass(&f.handle, failed).await;
    effect_becomes(&f, RetryEffect::Failed).await;
    let (_, held) = starts_and_last_pass(&f).await;
    f.handle.stop_sync().await.unwrap();
    f.handle.start_sync().await.unwrap();
    effect_becomes(&f, RetryEffect::Failed).await;
    assert_eq!(starts_and_last_pass(&f).await, (before + 2, held));
    f.handle.close().await.unwrap();
}

/// Each `(last_pass, effect)` an app's live query over `coven_sync_status`
/// and `coven_retry_effect` reports, as one row.
fn status_and_effect(
    f: &Fixture,
) -> coven_database::test_utils::Followed<(Option<Vec<u8>>, String)> {
    coven_database::test_utils::Followed::new(f.handle.subscribe(|sql| {
        Ok(sql.query_row(
            "SELECT s.last_pass,e.effect FROM coven_sync_status AS s,coven_retry_effect AS e",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?)
    }))
}

/// Eve's connection last failed for an update of her app, which no retry
/// clears, and her app stopped after the failure was saved but before the
/// status showed it, so the status still says synced. When she starts sync
/// again, the connection's first attempt shows the failure, and publishes
/// that a retry would fail before it does: no moment of the start shows the
/// failure while a retry still says sync isn't running.
#[tokio::test]
async fn a_held_start_publishes_its_effect_before_its_status() {
    let f = stopped_fixture().await;
    let update = RequiredUpdate::CovenFormat { version: u16::MAX };
    f.handle
        .test_record_pending(coven_sync::PendingRecord::new(
            coven_sync::PendingSubject::Connection,
            coven_sync::PendingReason::UpdateRequired(update),
        ))
        .await;
    let mut followed = status_and_effect(&f);
    let (before, effect) = followed.next().await;
    assert_eq!(effect, "not_syncing");
    f.handle.start_sync().await.unwrap();
    loop {
        let (last_pass, effect) = followed.next().await;
        if last_pass != before {
            assert_eq!(effect, "failed", "{last_pass:?}");
            break;
        }
    }
    f.handle.close().await.unwrap();
}

/// A failure releasing the providers is recorded as the last pass's failure
/// and returned, and leaves no pending connection record (E5).
#[tokio::test]
async fn a_failed_release_is_the_last_passs_failure_with_no_connection_record() {
    let f = stopped_fixture().await;
    // Stopping records the location by reading custody; this read fails.
    f.app.fail_next_keychain_operation();
    let error = f.handle.stop_sync().await.expect_err("the failed release is returned");
    assert!(error.to_string().contains("NoStorageAccess"), "{error}");
    let shown = f.handle.test_last_pass().latest().await;
    assert!(
        matches!(&shown, Some(LastPass::Failed { error: text, .. }) if text.contains("NoStorageAccess")),
        "{shown:?}"
    );
    let pending = f.handle.pending().await.unwrap();
    assert!(
        pending.iter().all(|record| record.subject != coven_sync::PendingSubject::Connection),
        "{pending:?}"
    );
    f.handle.close().await.unwrap();
}
