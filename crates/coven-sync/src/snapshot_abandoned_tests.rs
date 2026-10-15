//! An attempt abandoned before its landing was confirmed is recorded with
//! the abandonment: a landing that does not count is deleted, one that
//! counts stays, and an absent path is settled once storage time is past the
//! listing margin; the operation finishes only after (§15, T224; io
//! `abandoned_landing_unseen`, `counting_abandoned_landed_before`).
use super::*;
use crate::snapshot_data::AbandonedAttempt;
use coven_storage::test_utils::Faults;

/// The abandoned attempts the operation has recorded, by number and what
/// it knew of their landing.
pub(super) async fn recorded(
    device: &Device,
    id: crate::OperationId,
) -> Vec<(u64, AbandonedLanding)> {
    let record = device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == id)
        .unwrap();
    let Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write { abandoned, .. },
        ..
    }) = Data::read(&record).unwrap()
    else {
        panic!("a snapshot write")
    };
    abandoned
        .into_iter()
        .map(
            |AbandonedAttempt {
                 number, landing, ..
             }| (number.get(), landing),
        )
        .collect()
}

/// Run one step, keeping its error.
async fn try_step(device: &mut Device, id: crate::OperationId) -> Result<Progress, SyncError> {
    let record = device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == id)
        .unwrap();
    device
        .sync
        .operation_step(&record, Data::read(&record).unwrap())
        .await
}

async fn observe(device: &Device) {
    device
        .sync
        .observe_clock(&device.db.local_store_log().await.unwrap())
        .await
        .unwrap();
}

/// Ana prepares at 10,000 and sends at `sent`, losing the reply; she then
/// observes a storage time past the day and abandons the attempt unconfirmed.
async fn lost_reply(
    sent: u64,
) -> (
    Arc<MemoryStorage>,
    Arc<FixedClock>,
    Device,
    crate::OperationId,
    SnapshotId,
) {
    let (storage, clock, mut a, _b) = pair().await;
    set_time(&clock, 10_000);
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(attempt)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    set_time(&clock, sent);
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(try_step(&mut a, id).await.is_err());
    assert!(storage.status(&path_of(&attempt)).await.unwrap().is_some());
    set_time(&clock, 10_000 + DAY + 5);
    observe(&a).await;
    assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    assert_eq!(task(&a, id).await, Some((0, None)));
    assert_eq!(
        recorded(&a, id).await,
        vec![(
            attempt.number,
            AbandonedLanding::Unconfirmed {
                abandoned_at: StorageTime::new(10_000 + DAY + 5, 0).unwrap()
            }
        )]
    );
    (storage, clock, a, id, attempt)
}

/// The request landed after the day, unconfirmed: the next run deletes it,
/// and readers never selected it.
#[tokio::test]
async fn an_unconfirmed_late_landing_is_deleted() {
    let (storage, _clock, mut a, id, attempt) = lost_reply(10_000 + DAY + 1).await;
    assert_ne!(current(&storage).await, Some(path_of(&attempt)));
    let _ = step(&mut a, id).await;
    assert!(recorded(&a, id).await.is_empty());
    assert!(storage.status(&path_of(&attempt)).await.unwrap().is_none());
}

/// The request landed within the day, unconfirmed: it counts, so it stays
/// an ordinary own snapshot that readers select.
#[tokio::test]
async fn an_unconfirmed_counting_landing_stays() {
    let (storage, _clock, mut a, id, attempt) = lost_reply(10_000 + 10).await;
    assert_eq!(current(&storage).await, Some(path_of(&attempt)));
    let _ = step(&mut a, id).await;
    assert!(recorded(&a, id).await.is_empty());
    assert_eq!(current(&storage).await, Some(path_of(&attempt)));
}

/// Its request failed before reaching storage, so its path stays absent.
/// The operation prepares and publishes afresh, but finishes only once
/// storage time is more than the listing margin past the abandonment.
#[tokio::test]
async fn an_absent_abandoned_attempt_settles_after_the_listing_margin() {
    let (storage, clock, mut a, _b) = pair().await;
    set_time(&clock, 10_000);
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(expired)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(try_step(&mut a, id).await.is_err());
    assert!(storage.status(&path_of(&expired)).await.unwrap().is_none());
    let abandoned_at = 10_000 + DAY + 5;
    set_time(&clock, abandoned_at);
    observe(&a).await;
    assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    assert_eq!(recorded(&a, id).await.len(), 1);
    // A fresh attempt is prepared and lands counting meanwhile.
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (2, Some(fresh)) = task(&a, id).await.unwrap() else {
        panic!("a confirmed fresh attempt")
    };
    assert_ne!(fresh.number, expired.number);
    assert_eq!(current(&storage).await, Some(path_of(&fresh)));
    // Exactly the margin past the abandonment does not settle it.
    set_time(&clock, abandoned_at + 3600);
    observe(&a).await;
    let Some(Progress::Waiting(crate::PendingReason::Waits(crate::Prerequisite::Object(path)))) =
        step(&mut a, id).await
    else {
        panic!("waiting on the abandoned path")
    };
    assert_eq!(path, path_of(&expired));
    assert_eq!(recorded(&a, id).await.len(), 1);
    // Past it, the absent path is settled and the operation finishes.
    set_time(&clock, abandoned_at + 3601);
    observe(&a).await;
    assert!(matches!(
        step(&mut a, id).await,
        Some(Progress::Finished(_))
    ));
    assert!(task(&a, id).await.is_none());
    assert!(storage.status(&path_of(&expired)).await.unwrap().is_none());
}
