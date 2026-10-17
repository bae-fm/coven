use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

/// Order interval observations independently of the host clock's resolution.
/// Backoff still uses the runtime's paused timers.
struct RaceClock(AtomicU64);

impl Clock for RaceClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH
    }

    fn elapsed(&self) -> Duration {
        Duration::from_micros(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

#[test]
fn judge_rejects_double_success_wrong_bytes_unexpected_failures_and_disjoint_requests() {
    let payloads = vec![b"first".to_vec(), b"other".to_vec()];
    let replies = || {
        vec![
            Reply {
                writer: 0,
                start: Duration::ZERO,
                end: Duration::from_millis(4),
                result: Ok(()),
            },
            Reply {
                writer: 1,
                start: Duration::from_millis(1),
                end: Duration::from_millis(5),
                result: Err(StorageFailure::AlreadyExists.into()),
            },
        ]
    };
    assert!(judge(replies(), &payloads, Some(b"first"), None).is_ok());
    assert!(judge(replies(), &payloads, Some(b"other"), None).is_err());
    let mut both = replies();
    both[1].result = Ok(());
    assert!(judge(both, &payloads, Some(b"other"), None).is_err());
    let mut neither = replies();
    neither[0].result = Err(StorageFailure::AlreadyExists.into());
    assert!(judge(neither, &payloads, Some(b"first"), None).is_err());
    let mut disjoint = replies();
    disjoint[1].start = disjoint[0].end;
    assert!(judge(disjoint, &payloads, Some(b"first"), None).is_err());
    let mut network = replies();
    network[1].result = Err(StorageFailure::Network.into());
    assert!(judge(network, &payloads, Some(b"first"), None).is_err());
    let mut cleanup = replies();
    cleanup[1].result = Err(StorageError::Cleanup {
        operation: Box::new(StorageFailure::AlreadyExists.into()),
        cleanup: Box::new(StorageFailure::Network.into()),
    });
    assert!(judge(cleanup, &payloads, Some(b"first"), None).is_err());
}

#[tokio::test]
async fn lost_replies_are_settled_through_the_storage_connection() {
    let storage = crate::test_utils::MemoryStorage::builder().build().unwrap();
    lost_replies(
        &coven_foundation::clock::SystemClock,
        &storage,
        &storage,
        crate::test_utils::STORE_ID,
    )
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn throttled_slot_round_is_retried_without_counting_it() {
    for (width, trials) in STAGES {
        let clock = Arc::new(RaceClock(AtomicU64::new(0)));
        let storage = Arc::new(
            crate::test_utils::MemoryStorage::builder()
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let mut faults = crate::test_utils::Faults::none();
        faults.fail_next = 1;
        faults.failure = StorageFailure::RateLimited;
        faults.retry_after = Some(7);
        faults.creation_barrier = Some(Arc::new(Barrier::new(width)));
        storage.set_faults(faults).await;
        let writers: Vec<Arc<dyn Storage>> = (0..width)
            .map(|_| storage.clone() as Arc<dyn Storage>)
            .collect();
        let started = tokio::time::Instant::now();
        let throttled = rounds(clock.as_ref(), "create", trials, async |trial, attempt| {
            run(
                clock.as_ref(),
                &writers,
                &ObjectPath::slot(crate::test_utils::STORE_ID, attempt.try_into().unwrap()),
                None,
                trial,
                attempt,
            )
            .await
        })
        .await
        .unwrap();
        assert!(started.elapsed() >= Duration::from_secs(7));
        assert_eq!(throttled, 1);
        assert_eq!(storage.peak_requests(), width);
        assert_eq!(
            storage
                .list(&crate::ObjectPrefix::all(crate::test_utils::STORE_ID))
                .await
                .unwrap()
                .len() as u64,
            trials + throttled
        );
    }
}

fn throttled(seconds: u64) -> StorageError {
    crate::providers::ProviderResponse::injected(
        crate::CloudProvider::S3,
        StorageFailure::RateLimited,
        429,
        Some(seconds),
    )
}

fn replies_with(results: Vec<Result<(), StorageError>>) -> Vec<Reply> {
    results
        .into_iter()
        .enumerate()
        .map(|(writer, result)| Reply {
            writer,
            start: Duration::ZERO,
            end: Duration::from_secs(1),
            result,
        })
        .collect()
}

#[test]
fn throttling_preserves_the_longest_delay_and_cannot_hide_a_violation() {
    let payloads = vec![b"winner".to_vec(), b"other".to_vec(), b"third".to_vec()];
    let result = judge(
        replies_with(vec![Ok(()), Err(throttled(3)), Err(throttled(9))]),
        &payloads,
        Some(b"winner"),
        None,
    )
    .unwrap_err();
    assert_eq!(result.failure(), StorageFailure::RateLimited);
    assert_eq!(
        result.retry_after(),
        Some(crate::RetryAfter::Wait(Duration::from_secs(9)))
    );
    for (results, stored, before) in [
        (
            vec![Ok(()), Ok(()), Err(throttled(9))],
            Some(b"winner".as_slice()),
            None,
        ),
        (
            vec![
                Ok(()),
                Err(StorageFailure::PermissionDenied.into()),
                Err(throttled(9)),
            ],
            Some(b"winner".as_slice()),
            None,
        ),
        (
            vec![
                Ok(()),
                Err(StorageError::Cleanup {
                    operation: Box::new(throttled(9)),
                    cleanup: Box::new(StorageFailure::Network.into()),
                }),
            ],
            Some(b"winner".as_slice()),
            None,
        ),
        (
            vec![Ok(()), Err(throttled(9))],
            Some(b"other".as_slice()),
            None,
        ),
        (vec![Ok(()), Err(throttled(9))], None, None),
        (
            vec![Err(throttled(9)), Err(throttled(3))],
            Some(b"winner".as_slice()),
            None,
        ),
        (
            vec![Err(throttled(9)), Err(throttled(3))],
            Some(b"winner".as_slice()),
            Some(b"old floor".as_slice()),
        ),
    ] {
        let error = judge(replies_with(results), &payloads, stored, before).unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Protocol);
    }
    for before in [None, Some(b"old floor".as_slice())] {
        let error = judge(
            replies_with(vec![Err(throttled(9)), Err(throttled(3))]),
            &payloads,
            before,
            before,
        )
        .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::RateLimited);
    }
}

#[tokio::test(start_paused = true)]
async fn rounds_retry_the_same_trial_with_new_attempts_and_wait_before_releasing_it() {
    let clock = coven_foundation::clock::SystemClock;
    let started = tokio::time::Instant::now();
    let mut attempts = Vec::new();
    let discarded = rounds(&clock, "fixture", 2, async |trial, attempt| {
        attempts.push((trial, attempt, started.elapsed()));
        if attempt <= 2 {
            Err(throttled(7))
        } else {
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(discarded, 2);
    assert_eq!(
        attempts,
        vec![
            (1, 1, Duration::from_secs(1)),
            (1, 2, Duration::from_secs(9)),
            (1, 3, Duration::from_secs(17)),
            (2, 4, Duration::from_secs(18)),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn exhausted_throttling_waits_before_returning_control_to_cleanup() {
    let clock = coven_foundation::clock::SystemClock;
    let mut requests = 0;
    let mut last_request = tokio::time::Instant::now();
    let error = retry(&clock, async || {
        requests += 1;
        last_request = tokio::time::Instant::now();
        Err::<(), _>(throttled(37))
    })
    .await
    .unwrap_err();
    assert_eq!(requests, 20);
    assert_eq!(
        error.retry_after(),
        Some(crate::RetryAfter::Wait(Duration::from_secs(37)))
    );
    assert!(last_request.elapsed() >= Duration::from_secs(37));

    let mut attempts = 0;
    let error = rounds(&clock, "persistent throttle", 1, async |trial, attempt| {
        assert_eq!(trial, 1);
        attempts = attempt;
        last_request = tokio::time::Instant::now();
        Err(throttled(37))
    })
    .await
    .unwrap_err();
    assert_eq!(attempts, 100);
    assert_eq!(error.failure(), StorageFailure::RateLimited);
    assert!(last_request.elapsed() >= Duration::from_secs(37));
}

#[tokio::test(start_paused = true)]
async fn failed_round_waits_for_throttled_writers_before_cleanup() {
    let clock = coven_foundation::clock::SystemClock;
    for results in [
        vec![Ok(()), Ok(()), Err(throttled(9))],
        vec![
            Err(StorageFailure::PermissionDenied.into()),
            Err(throttled(9)),
        ],
        vec![Err(StorageError::Cleanup {
            operation: Box::new(throttled(9)),
            cleanup: Box::new(StorageFailure::Network.into()),
        })],
    ] {
        let started = tokio::time::Instant::now();
        let error = wait_for_writers(&clock, &replies_with(results))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Protocol);
        assert!(started.elapsed() >= Duration::from_secs(9));
    }
}
