use super::*;
use coven_foundation::clock::Clock;
use coven_foundation::{
    clock::FixedClock,
    files::{StoreDir, StoreLayout},
    id_source::{SequentialIds, StoreId},
};
use coven_storage::Cooldown;
use serde_json::json;
use std::{sync::Arc, time::UNIX_EPOCH};

struct Fixture {
    _temporary: tempfile::TempDir,
    directory: StoreDir,
    clock: Arc<FixedClock>,
}

impl Fixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let directory = StoreLayout::new(temporary.path().to_owned())
            .create_store_dir(
                StoreId(uuid::Uuid::from_u128(1)),
                "Home",
                &SequentialIds::new(),
            )
            .unwrap();
        Self {
            _temporary: temporary,
            directory,
            clock: Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1_000_000))),
        }
    }

    async fn open(&self) -> (Database, RetryWaits) {
        let database = coven_database::DatabaseBuilder::new(self.directory.clone())
            .synced_tables(vec![])
            .migrations(vec![coven_database::Migration::addition(
                1, "local", "SELECT 1",
            )])
            .open()
            .await
            .unwrap();
        let waits = RetryWaits::open(database.clone(), self.clock.clone())
            .await
            .unwrap();
        (database, waits)
    }

    fn advance(&self, seconds: u64) {
        self.clock
            .set(self.clock.now() + Duration::from_secs(seconds));
    }
}

const JOIN: RetrySubject = RetrySubject::Join;

async fn due(database: &Database, waits: &RetryWaits, subject: RetrySubject) -> bool {
    let persisted = database.retry_wait(subject).await.unwrap();
    waits.due(subject, persisted.as_ref())
}

#[tokio::test]
async fn unsuccessful_attempts_double_their_persisted_wait() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let mut delays = Vec::new();
    for _ in 0..11 {
        let previous = database.retry_wait(JOIN).await.unwrap();
        waits.fail(JOIN, previous.as_ref()).await.unwrap();
        let persisted = database.retry_wait(JOIN).await.unwrap().unwrap();
        // T is persisted with W before waiting.
        assert_eq!(persisted.not_before, fixture.clock.now() + persisted.wait);
        assert!(!due(&database, &waits, JOIN).await);
        fixture.advance(persisted.wait.as_secs() - 1);
        assert!(!due(&database, &waits, JOIN).await);
        fixture.advance(1);
        assert!(due(&database, &waits, JOIN).await);
        delays.push(persisted.wait.as_secs());
    }
    assert_eq!(delays, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300]);
    database.close().await.unwrap();
}

#[tokio::test]
async fn reopening_arms_the_remaining_wait_bounded_by_the_full_wait() {
    // Ana restarts with four seconds left in an eight-second delay (§6.7).
    for (jump, expected) in [(0i64, 4u64), (-1_000_000, 8), (1_000, 0)] {
        let fixture = Fixture::new();
        let (database, waits) = fixture.open().await;
        for _ in 0..4 {
            let previous = database.retry_wait(JOIN).await.unwrap();
            waits.fail(JOIN, previous.as_ref()).await.unwrap();
        }
        assert_eq!(
            database.retry_wait(JOIN).await.unwrap().unwrap().wait,
            Duration::from_secs(8)
        );
        fixture.advance(4);
        database.close().await.unwrap();
        // The device clock moved backward, not at all, or forward.
        let now = fixture.clock.now();
        fixture.clock.set(if jump < 0 {
            now - Duration::from_secs(jump.unsigned_abs())
        } else {
            now + Duration::from_secs(jump as u64)
        });
        let (database, waits) = fixture.open().await;
        let left = waits
            .next_due(|subject| *subject == JOIN)
            .unwrap_or_default();
        assert_eq!(left, Duration::from_secs(expected), "clock jump {jump}");
        // Reopening keeps the exponent: the next failure waits sixteen seconds.
        let previous = database.retry_wait(JOIN).await.unwrap();
        assert_eq!(previous.unwrap().failures, 4);
        waits.fail(JOIN, previous.as_ref()).await.unwrap();
        assert_eq!(
            database.retry_wait(JOIN).await.unwrap().unwrap().wait,
            Duration::from_secs(16)
        );
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_provider_cooldown_holds_every_consumer_across_reopening() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.fail(JOIN, None).await.unwrap();
    waits.record(Duration::from_secs(600)).await.unwrap();
    // A shorter request cannot shorten a live cooldown.
    waits.record(Duration::from_secs(5)).await.unwrap();
    let upload = RetrySubject::Upload(7);
    fixture.advance(1);
    // The join poll's own second elapsed; the provider's 600 did not.
    assert!(!due(&database, &waits, JOIN).await);
    assert!(!due(&database, &waits, upload).await);
    assert_eq!(waits.cooldown(), Some(Duration::from_secs(599)));
    assert_eq!(
        waits.next_due(|subject| *subject == JOIN),
        Some(Duration::from_secs(599))
    );
    database.close().await.unwrap();
    // W stays 600, beyond the ordinary cap, and survives reopening.
    fixture.advance(99);
    let (database, waits) = fixture.open().await;
    assert_eq!(waits.cooldown(), Some(Duration::from_secs(500)));
    // An app retry overrides only ordinary backoff.
    waits.forget(JOIN);
    database.clear_retry(JOIN).await.unwrap();
    assert!(!due(&database, &waits, JOIN).await);
    fixture.advance(500);
    assert!(due(&database, &waits, JOIN).await);
    assert!(due(&database, &waits, upload).await);
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_wait_that_cannot_be_persisted_holds_until_the_app_retries() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    database.close().await.unwrap();
    assert!(waits.fail(JOIN, None).await.is_err());
    fixture.advance(3600);
    assert!(
        !waits.due(JOIN, None),
        "a failed save is no permission to retry"
    );
    assert_eq!(waits.next_due(|subject| *subject == JOIN), None);
    waits.forget(JOIN);
    assert!(waits.due(JOIN, None));
}

/// IO `Waiting`: the ordinary delay, the restart arm and whether a timer is
/// due before and after an app retry, from the Rust owner's decisions.
/// `WaitingEvidence` runs through the operation owner
/// (`io_differential_waiting_evidence`).
#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_waiting() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut pick = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    for case in 0..96 {
        let failures = (case % 16) as u32;
        let deadline = pick(2_000) as i64 - 500;
        let now = pick(2_000) as i64 - 500;
        let wait = pick(900);
        let ordinary = pick(700);
        let provider = pick(700);
        let at = pick(800);
        cases.push(
            json!({"check": "waiting", "failures": failures, "deadline": deadline,
            "now": now, "wait": wait, "ordinary": ordinary, "provider": provider, "at": at}),
        );
        let base = UNIX_EPOCH + Duration::from_secs(10_000);
        let wall = |seconds: i64| {
            if seconds < 0 {
                base - Duration::from_secs(seconds.unsigned_abs())
            } else {
                base + Duration::from_secs(seconds as u64)
            }
        };
        let restart = restart_wait(wall(deadline), wall(now), Duration::from_secs(wait));
        // A timer armed at monotonic zero for `ordinary` seconds, under a
        // provider cooldown of `provider` seconds, observed at `at`.
        let clock = Arc::new(FixedClock::new(base));
        let waits = RetryWaits::in_memory(clock.clone());
        let ordinary_wait = RetryWait {
            failures: 1,
            not_before: base + Duration::from_secs(ordinary),
            wait: Duration::from_secs(ordinary),
        };
        waits.arm(JOIN, &ordinary_wait);
        if provider > 0 {
            waits.record(Duration::from_secs(provider)).await.unwrap();
        }
        clock.set(base + Duration::from_secs(at));
        let timer_due = waits.due(JOIN, Some(&ordinary_wait));
        waits.forget(JOIN);
        let retried = waits.due(JOIN, None);
        actual.push(json!({
            "delay": coven_storage::retry_delay(failures).as_secs(),
            "restart": restart.as_secs(),
            "due": timer_due,
            "retried": retried,
        }));
    }
    assert_eq!(actual, crate::tests::io_runner(&cases));
}

#[tokio::test]
async fn the_worker_wakes_for_the_earliest_due_retry() {
    use futures_util::FutureExt;
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let operation = |id| RetrySubject::Operation(coven_database::OperationId(id));
    let later = RetryWait {
        failures: 3,
        not_before: fixture.clock.now() + Duration::from_secs(4),
        wait: Duration::from_secs(4),
    };
    let sooner = RetryWait {
        failures: 1,
        not_before: fixture.clock.now() + Duration::from_secs(1),
        wait: Duration::from_secs(1),
    };
    waits.arm(operation(1), &later);
    waits.arm(operation(2), &sooner);
    let operations = |subject: &RetrySubject| matches!(subject, RetrySubject::Operation(_));
    let mut wake = waits.next_wake(operations).unwrap();
    assert!((&mut wake).now_or_never().is_none());
    fixture.advance(1);
    tokio::time::timeout(Duration::from_secs(5), wake)
        .await
        .unwrap();
    // Once the sooner wait is evaluated, the next wake is the later one.
    assert!(waits.due(operation(2), Some(&sooner)));
    waits.forget(operation(2));
    let mut wake = waits.next_wake(operations).unwrap();
    fixture.advance(2);
    assert!((&mut wake).now_or_never().is_none());
    fixture.advance(1);
    tokio::time::timeout(Duration::from_secs(5), wake)
        .await
        .unwrap();
    database.close().await.unwrap();
}
