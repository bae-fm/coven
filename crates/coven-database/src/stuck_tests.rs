use super::*;
use crate::tests::TestStore;
use coven_format::pending::RefusalCode;
use coven_foundation::clock::FixedClock;
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

fn refused() -> LogRefusal {
    LogRefusal {
        object: LogObject::Write(coven_merge::WriteId {
            device: DeviceId(2),
            number: 1,
        }),
        failure: RefusalCode::InvalidWrite,
    }
}

#[tokio::test]
async fn judgments_are_recorded_once_observed_and_retried_after_a_version_change() {
    let store = TestStore::new();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let db = store
        .builder(vec![], vec![])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let mut live = db.subscribe_stuck_logs();
    assert!(live.next().await.unwrap().is_empty());
    db.record_stuck_log(refused()).await.unwrap();
    let expected = vec![StuckLog {
        record: refused(),
        reported_by: None,
    }];
    assert_eq!(live.next().await.unwrap(), expected);
    clock.set(UNIX_EPOCH + Duration::from_secs(2));
    db.record_stuck_log(refused()).await.unwrap();
    db.inspect_writer(|sql| {
        let (when, version): (Vec<u8>, String) = sql
            .query_row(
                "SELECT judged_at,coven_version FROM _coven_stuck_logs",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            crate::user_file::decode_time(&when).unwrap(),
            UNIX_EPOCH + Duration::from_secs(1)
        );
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
    });
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.stuck_logs().await.unwrap(), expected);
    db.inspect_writer(|sql| {
        sql.transaction(|sql| {
            sql.internal_execute(
                "UPDATE _coven_stuck_logs SET coven_version='previous-version'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    });
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert!(db.stuck_logs().await.unwrap().is_empty());
    db.record_stuck_log(refused()).await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.stuck_logs().await.unwrap(), expected);
}

#[tokio::test]
async fn peer_reports_are_replaced_atomically_and_never_published_as_local_judgments() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let own = db.sync_state(Vec::new()).await.unwrap().device;
    let report = LogRefusal {
        object: LogObject::Entry(crate::EntryId {
            device: own,
            number: 1,
        }),
        failure: RefusalCode::Parse,
    };
    db.record_stuck_log(refused()).await.unwrap();
    db.replace_stuck_reports(vec![(DeviceId(3), report)])
        .await
        .unwrap();
    assert_eq!(db.stuck_logs().await.unwrap().len(), 2);
    assert_eq!(
        db.sync_state(Vec::new()).await.unwrap().stuck,
        vec![refused()]
    );
    assert!(db.replace_stuck_reports(vec![(own, report)]).await.is_err());
    assert_eq!(db.stuck_logs().await.unwrap().len(), 2);
    db.replace_stuck_reports(Vec::new()).await.unwrap();
    assert_eq!(
        db.stuck_logs().await.unwrap(),
        vec![StuckLog {
            record: refused(),
            reported_by: None
        }]
    );
}

#[tokio::test]
async fn judgments_clear_only_with_a_committed_changed_reset() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let boundary = || crate::WriteBoundary::Reset {
        entry: crate::EntryId {
            device: DeviceId(1),
            number: 1,
        },
        audience: coven_merge::Audience::Store,
        included: coven_format::value::WritePositions(Vec::new()),
    };
    let reload = |snapshots| crate::SnapshotReload::<std::io::Empty, std::io::Empty> {
        snapshots,
        writes: Vec::new(),
        absent: Vec::new(),
        boundaries: Some(vec![boundary()]),
        expected_entries: None,
        operation: None,
    };
    db.record_stuck_log(refused()).await.unwrap();
    assert!(db.load_snapshots(reload(Vec::new())).await.is_err());
    assert_eq!(db.stuck_logs().await.unwrap().len(), 1);
    db.load_snapshots(reload(vec![crate::SnapshotSource::Empty(
        coven_merge::Audience::Store,
    )]))
    .await
    .unwrap();
    assert!(db.stuck_logs().await.unwrap().is_empty());
    db.record_stuck_log(refused()).await.unwrap();
    db.load_snapshots(reload(vec![crate::SnapshotSource::Empty(
        coven_merge::Audience::Store,
    )]))
    .await
    .unwrap();
    assert_eq!(db.stuck_logs().await.unwrap().len(), 1);
    let mut without_reset = reload(vec![crate::SnapshotSource::Empty(
        coven_merge::Audience::Store,
    )]);
    without_reset.boundaries = Some(Vec::new());
    db.load_snapshots(without_reset).await.unwrap();
    assert_eq!(db.stuck_logs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_reload_that_rewinds_a_log_can_record_an_earlier_refusal() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    for number in [3, 2, 4] {
        db.record_stuck_log(LogRefusal {
            object: LogObject::Write(coven_merge::WriteId {
                device: DeviceId(2),
                number,
            }),
            failure: RefusalCode::InvalidWrite,
        })
        .await
        .unwrap();
    }
    assert_eq!(
        db.stuck_logs().await.unwrap()[0].record.object,
        LogObject::Write(coven_merge::WriteId {
            device: DeviceId(2),
            number: 2
        })
    );
}
