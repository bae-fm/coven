use super::*;
use crate::{tests::TestStore, CovenError, Database, NewOperation, OperationUpdate};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    subject: Subject,
    reported_by: DeviceId,
    reason: Reason,
    operation: Option<OperationId>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum Subject {
    Number(u64),
    Audience(String),
    Write(u64, u64),
}
impl From<u64> for Subject {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Reason {
    AccessKey(String),
    Missing,
    Refused,
    Update(u32),
    Dropped,
}
impl Record {
    pub(crate) fn access_key(&self) -> Option<&str> {
        match &self.reason {
            Reason::AccessKey(key) => Some(key),
            _ => None,
        }
    }
}
impl PendingValue for Record {
    fn reload(audience: coven_merge::Audience, reported_by: DeviceId) -> Self {
        Self {
            subject: Subject::Audience(crate::write_encoding::audience_text(&audience)),
            reported_by,
            reason: Reason::Missing,
            operation: None,
        }
    }
    fn access_key(id: OperationId, reported_by: DeviceId, key: crate::AccessKeyToDelete) -> Self {
        Self {
            subject: Subject::Number(id.0 as u64),
            reported_by,
            reason: Reason::AccessKey(key.access_key_id),
            operation: Some(id),
        }
    }
    fn dropped_entry(entry: crate::EntryId, _reason: crate::DropReason) -> Self {
        Self {
            subject: entry.number.into(),
            reported_by: entry.device,
            reason: Reason::Dropped,
            operation: None,
        }
    }
    type Subject = Subject;
    fn subject(&self) -> &Subject {
        &self.subject
    }
    fn reported_by(&self) -> DeviceId {
        self.reported_by
    }
    fn operation(&self) -> Option<OperationId> {
        self.operation
    }
    fn log_object(&self) -> Option<crate::LogObject> {
        match self.subject {
            Subject::Write(device, number) => Some(crate::LogObject::Write(coven_merge::WriteId {
                device: DeviceId(device),
                number,
            })),
            Subject::Number(number) => matches!(self.reason, Reason::Dropped).then_some(
                crate::LogObject::Entry(crate::EntryId {
                    device: self.reported_by,
                    number,
                }),
            ),
            Subject::Audience(_) => None,
        }
    }
    fn ends_when_covered(&self) -> bool {
        !matches!(self.reason, Reason::Refused | Reason::Dropped)
    }
    fn can_retry(&self, version: &str, schema: u32) -> bool {
        match self.reason {
            Reason::Missing => true,
            Reason::AccessKey(_) => false,
            Reason::Dropped => false,
            Reason::Refused => version != env!("CARGO_PKG_VERSION"),
            Reason::Update(required) => schema >= required,
        }
    }
}
pub(crate) async fn operation_record(db: &Database, id: OperationId) -> Record {
    reported_operation(own(db).await, id)
}
pub(crate) fn reported_operation(reported_by: DeviceId, id: OperationId) -> Record {
    Record {
        subject: Subject::Number(id.0.try_into().unwrap()),
        reported_by,
        reason: Reason::Missing,
        operation: Some(id),
    }
}
async fn own(db: &Database) -> DeviceId {
    db.sync_state(Vec::new()).await.unwrap().device
}
async fn record(db: &Database, subject: u64) -> Record {
    Record {
        subject: subject.into(),
        reported_by: own(db).await,
        reason: Reason::Refused,
        operation: None,
    }
}

#[tokio::test]
async fn subjects_and_reporters_are_independent_and_reasons_replace_atomically() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut live = db.subscribe_pending::<Record>();
    assert!(live.next().await.unwrap().is_empty());
    let local = record(&db, 2).await;
    db.record_pending(local.clone()).await.unwrap();
    assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&local));
    let peer = Record {
        reported_by: DeviceId(local.reported_by.0.wrapping_add(1)),
        ..local.clone()
    };
    db.replace_pending_reports(vec![peer.clone()])
        .await
        .unwrap();
    let mut expected = vec![local.clone(), peer.clone()];
    expected.sort_by_key(|r| (r.subject.clone(), r.reported_by));
    assert_eq!(live.next().await.unwrap(), expected);
    let next = Record {
        reason: Reason::Missing,
        ..local.clone()
    };
    db.record_pending(next.clone()).await.unwrap();
    let observed = live.next().await.unwrap();
    assert_eq!(observed.len(), 2);
    assert!(observed.contains(&next));
    assert!(observed.contains(&peer));
    assert!(db
        .replace_pending_reports(vec![local.clone()])
        .await
        .is_err());
    assert_eq!(db.pending::<Record>().await.unwrap(), observed);
    db.clear_pending(local.subject).await.unwrap();
    assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&peer));
    db.record_pending(record(&db, 3).await).await.unwrap();
    db.record_pending(record(&db, 4).await).await.unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap().len(), 3);
    db.replace_pending_reports::<Record>(Vec::new())
        .await
        .unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap().len(), 2);
}

#[tokio::test]
async fn reopening_preserves_conditions_and_a_version_change_rearms_without_erasing_them() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let pending = record(&db, 1).await;
    db.record_pending(pending.clone()).await.unwrap();
    assert!(!db.pending_can_retry::<Record>(1.into()).await.unwrap());
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        std::slice::from_ref(&pending)
    );
    assert!(!db.pending_can_retry::<Record>(1.into()).await.unwrap());
    db.test_pending_version("previous-version").await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        std::slice::from_ref(&pending)
    );
    assert!(db.pending_can_retry::<Record>(1.into()).await.unwrap());
    db.record_pending(pending).await.unwrap();
    assert!(!db.pending_can_retry::<Record>(1.into()).await.unwrap());
}

#[tokio::test]
async fn completed_steps_replace_or_clear_the_condition_in_the_same_commit() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let id = db
        .start_operation(NewOperation {
            kind: "fixture".into(),
            data: vec![0],
            started_by: "test".into(),
        })
        .await
        .unwrap();
    let first = operation_record(&db, id).await;
    db.record_pending(first.clone()).await.unwrap();
    let mut live = db.subscribe_pending::<Record>();
    assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&first));
    assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 0);
    let next = Record {
        reason: Reason::Update(2),
        ..first.clone()
    };
    db.advance_operation_pending(
        OperationUpdate {
            id,
            previous: 0,
            last_step: 1,
            data: vec![1],
        },
        next.clone(),
    )
    .await
    .unwrap();
    assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&next));
    assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 1);
    assert!(db
        .advance_operation_pending(
            OperationUpdate {
                id,
                previous: 0,
                last_step: 2,
                data: vec![2]
            },
            first
        )
        .await
        .is_err());
    assert_eq!(db.pending::<Record>().await.unwrap(), [next]);
    db.advance_operation(OperationUpdate {
        id,
        previous: 1,
        last_step: 2,
        data: vec![2],
    })
    .await
    .unwrap();
    assert!(live.next().await.unwrap().is_empty());
    assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 2);
    db.record_pending(operation_record(&db, id).await)
        .await
        .unwrap();
    db.finish_operation(id).await.unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    assert!(db.operation(id).await.unwrap().is_none());
}

fn cause(error: DbError) -> std::sync::Arc<DbError> {
    let DbError::PendingPersistence(cause) = error else {
        panic!("{error:?}")
    };
    assert!(matches!(*cause, DbError::Sqlite(_)));
    cause
}
fn db_error(error: CovenError) -> DbError {
    let CovenError::Database(error) = error else {
        panic!("{error:?}")
    };
    error
}

#[tokio::test]
async fn losing_a_pending_update_fails_all_calls_and_subscriptions_until_reopen() {
    for statement in ["INSERT", "DELETE", "COMMIT"] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let pending = record(&db, 1).await;
        db.record_pending(pending.clone()).await.unwrap();
        let mut live = db.subscribe_pending::<Record>();
        assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&pending));
        let mut ordinary =
            db.subscribe(|sql| Ok(sql.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?));
        assert_eq!(ordinary.next().await.unwrap(), 1);
        db.inspect_writer(|sql| {
            if statement == "COMMIT" {
                sql.batch("CREATE TEMP TABLE parent(id INTEGER PRIMARY KEY);
                    CREATE TEMP TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
                    CREATE TEMP TRIGGER fail_pending AFTER UPDATE ON _coven_pending BEGIN INSERT INTO child VALUES(1); END;").unwrap();
            } else {
                sql.batch(&format!("CREATE TEMP TRIGGER fail_pending BEFORE {statement} ON _coven_pending BEGIN SELECT RAISE(ABORT,'injected pending failure'); END;")).unwrap();
            }
        });
        let failed = if statement != "DELETE" {
            db.record_pending(Record {
                reason: Reason::Missing,
                ..pending.clone()
            })
            .await
        } else {
            db.clear_pending(pending.subject.clone()).await
        };
        let original = cause(failed.unwrap_err());
        let failures = [
            cause(db_error(live.next().await.unwrap_err())),
            cause(db_error(ordinary.next().await.unwrap_err())),
            cause(db_error(db.pending::<Record>().await.unwrap_err())),
            cause(db_error(db.read(|_| Ok(())).await.unwrap_err())),
            cause(db.write(|_| Ok(())).await.unwrap_err()),
            cause(db.applied_migrations().unwrap_err()),
            cause(db.finish_recovery().await.unwrap_err()),
            cause(db.schema_version().await.unwrap_err()),
            cause(db_error(
                db.subscribe_pending::<Record>().next().await.unwrap_err(),
            )),
        ];
        assert!(failures
            .iter()
            .all(|error| std::sync::Arc::ptr_eq(&original, error)));
        db.close().await.unwrap();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        assert_eq!(
            db.pending::<Record>().await.unwrap(),
            std::slice::from_ref(&pending)
        );
        db.clear_pending(pending.subject).await.unwrap();
        assert!(db.pending::<Record>().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn losing_a_pending_update_fails_an_already_running_subscription() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let pending = record(&db, 1).await;
    db.inspect_writer(|sql| {
        sql.batch("CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON _coven_pending BEGIN SELECT RAISE(ABORT,'injected pending failure'); END;").unwrap();
    });
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = std::sync::Mutex::new(released);
    let mut live = db.subscribe(move |sql| {
        let records = sql.pending::<Record>()?;
        entered.lock().unwrap().take().unwrap().send(()).unwrap();
        released
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        Ok(records)
    });
    tokio::select! {
        result = live.next() => panic!("held query returned: {result:?}"),
        result = entry => result.unwrap(),
    }
    let failed = db.record_pending(pending).await;
    release.send(()).unwrap();
    let original = cause(failed.unwrap_err());
    let observed = cause(db_error(live.next().await.unwrap_err()));
    assert!(std::sync::Arc::ptr_eq(&original, &observed));
    db.close().await.unwrap();
}

#[tokio::test]
async fn judgments_clear_only_with_a_committed_changed_reset() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = crate::EntryId {
        device: DeviceId(1),
        number: 1,
    };
    db.check_boundary_fixture(crate::Boundary::Reset {
        entry,
        snapshot: coven_format::store_log::SnapshotId {
            audience: coven_merge::Audience::Store,
            device: entry.device,
            number: entry.number,
        },
    })
    .await
    .unwrap();
    let boundary = || crate::BoundaryCoverage {
        entry: crate::EntryId {
            device: DeviceId(1),
            number: 1,
        },
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
    db.record_pending(record(&db, 1).await).await.unwrap();
    assert!(db.load_snapshots(reload(Vec::new())).await.is_err());
    assert_eq!(db.pending::<Record>().await.unwrap().len(), 2);
    db.load_snapshots(reload(vec![crate::SnapshotSource::Empty(
        coven_merge::Audience::Store,
    )]))
    .await
    .unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    db.record_pending(record(&db, 1).await).await.unwrap();
    db.load_snapshots(reload(vec![crate::SnapshotSource::Empty(
        coven_merge::Audience::Store,
    )]))
    .await
    .unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_failed_condition_change_rolls_back_its_completed_step() {
    for statement in ["INSERT", "DELETE"] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let id = db
            .start_operation(NewOperation {
                kind: "fixture".into(),
                data: vec![0],
                started_by: "test".into(),
            })
            .await
            .unwrap();
        let original = operation_record(&db, id).await;
        db.record_pending(original.clone()).await.unwrap();
        db.inspect_writer(|sql| sql.batch(&format!("CREATE TEMP TRIGGER fail_pending BEFORE {statement} ON _coven_pending BEGIN SELECT RAISE(ABORT,'injected pending failure'); END;")).unwrap());
        let update = OperationUpdate {
            id,
            previous: 0,
            last_step: 1,
            data: vec![1],
        };
        let error = if statement == "INSERT" {
            db.advance_operation_pending(
                update,
                Record {
                    reason: Reason::Update(2),
                    ..original.clone()
                },
            )
            .await
            .unwrap_err()
        } else {
            db.advance_operation(update).await.unwrap_err()
        };
        cause(error);
        db.close().await.unwrap();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let operation = db.operation(id).await.unwrap().unwrap();
        assert_eq!(operation.last_step, 0);
        assert_eq!(operation.data, [0]);
        assert_eq!(db.pending::<Record>().await.unwrap(), [original]);
    }
}

#[tokio::test]
async fn a_writes_wait_ends_when_positions_cover_it_but_a_verdict_on_its_bytes_remains() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let reported_by = own(&db).await;
    let observed = |number, reason| Record {
        subject: Subject::Write(7, number),
        reported_by,
        reason,
        operation: None,
    };
    let waiting = observed(1, Reason::Missing);
    let refused = observed(2, Reason::Refused);
    let later = observed(3, Reason::Missing);
    for record in [&waiting, &refused, &later] {
        db.record_pending(record.clone()).await.unwrap();
    }
    // A loaded snapshot's positions pass writes 1 and 2 without applying
    // them: the wait ends, the refusal stays, the uncovered wait stays.
    db.test_cover_positions(vec![(7, 2)]).await.unwrap();
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        vec![refused.clone(), later.clone()]
    );
    // Applying a write itself ends any record for it, verdicts included.
    for number in [2, 3] {
        db.test_clear_log(crate::LogObject::Write(coven_merge::WriteId {
            device: DeviceId(7),
            number,
        }))
        .await
        .unwrap();
    }
    assert!(db.pending::<Record>().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_completed_scan_replaces_exactly_its_own_class_of_records() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let reported_by = own(&db).await;
    let audience = |name: &str, reason| Record {
        subject: Subject::Audience(name.into()),
        reported_by,
        reason,
        operation: None,
    };
    let scanned = |record: &Record| matches!(record.subject, Subject::Audience(_));
    let other = record(&db, 1).await;
    db.record_pending(other.clone()).await.unwrap();
    let mut live = db.subscribe_pending::<Record>();
    assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&other));
    let first = vec![
        audience("a", Reason::Missing),
        audience("b", Reason::Missing),
    ];
    db.replace_local_pending(scanned, first.clone())
        .await
        .unwrap();
    let mut expected = first.clone();
    expected.push(other.clone());
    expected.sort_by_key(|r| (r.subject.clone(), r.reported_by));
    assert_eq!(live.next().await.unwrap(), expected);
    // An unchanged scan commits nothing.
    db.replace_local_pending(scanned, first).await.unwrap();
    // A subject that now checks loses its record; a changed reason replaces it.
    let next = vec![audience("b", Reason::Refused)];
    db.replace_local_pending(scanned, next.clone())
        .await
        .unwrap();
    let mut expected = next.clone();
    expected.push(other.clone());
    expected.sort_by_key(|r| (r.subject.clone(), r.reported_by));
    assert_eq!(live.next().await.unwrap(), expected);
    // Records outside the scanned class, or reported by peers, are refused.
    assert!(db
        .replace_local_pending(scanned, vec![other.clone()])
        .await
        .is_err());
    let peer = Record {
        reported_by: DeviceId(reported_by.0.wrapping_add(1)),
        ..audience("c", Reason::Missing)
    };
    assert!(db.replace_local_pending(scanned, vec![peer]).await.is_err());
    db.replace_local_pending(scanned, Vec::new()).await.unwrap();
    assert_eq!(live.next().await.unwrap(), std::slice::from_ref(&other));
}
