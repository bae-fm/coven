use super::LoadStep;
use crate::snapshot_write::tests::{frames, id, stream};
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use coven_foundation::clock::Clock;
use coven_foundation::id_source::SequentialIds;
use coven_foundation::progress::{Progress, ProgressReport, RecordingClock, REPORT_INTERVAL};
use coven_merge::Audience;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

/// A clock each reading of which moves a report's interval on, so every
/// update is reported.
struct Stepping(Mutex<Duration>);

impl Clock for Stepping {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + *self.0.lock().unwrap()
    }
    fn elapsed(&self) -> Duration {
        let mut elapsed = self.0.lock().unwrap();
        *elapsed += REPORT_INTERVAL;
        *elapsed
    }
}

fn at(step: LoadStep, done: u64, total: u64) -> Progress<LoadStep> {
    Progress { step, done, total }
}

/// Ben's laptop loads Ana's store snapshot and the slot after it in one
/// transaction. It reports installing the snapshot's rows, one table at a
/// time in each of installing's six passes, then the slot it applies in
/// that transaction: each step from its start to its end, one unit at a
/// time, `done` never passing its total.
#[tokio::test]
async fn loading_reports_its_tables_then_its_slots() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','first','')").await.unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let prefix = snapshot.prefix.clone();
    sql(&a, "UPDATE notes SET title='second'").await.unwrap();
    let after = records(&a).pop().unwrap();
    let (sender, watch) = watch::channel(None);
    let clock = Arc::new(RecordingClock::new(Arc::new(Stepping(Mutex::new(Duration::ZERO)))));
    clock.watch(watch);
    let reload = crate::tests::reload(
        &b,
        vec![(
            id(Audience::Store),
            prefix,
            Cursor::new(snapshot.concat()),
            crate::snapshot_write::tests::author(),
        )],
        vec![stream(&after)],
        Arc::new(crate::test_utils::TestAuthorship),
    )
    .await;
    b.load_snapshots::<crate::pending::tests::Record, _>(
        reload,
        Some(ProgressReport::new(sender, clock.clone())),
    )
    .await
    .unwrap();

    let seen = clock.seen();
    // Each of the six passes counts every table once.
    let passes = seen.first().map_or(0, |first| first.total);
    assert!(passes > 0 && passes.is_multiple_of(crate::snapshot_install::TABLE_PASSES), "{seen:?}");
    let expected: Vec<_> = (0..=passes)
        .map(|done| at(LoadStep::Installing, done, passes))
        .chain((0..=1).map(|done| at(LoadStep::InstallingSlots, done, 1)))
        .collect();
    assert_eq!(seen, expected);
}
