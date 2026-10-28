use super::*;
use crate::{tests::TestStore, CovenError, NewOperation, OperationUpdate};
use coven_foundation::id_source::DeviceId;
use serde::Deserialize;

/// The pending records as the app's SQL reads them from `coven_pending`,
/// as a live query.
fn records(db: &crate::Database) -> crate::LiveQuery<Vec<Record>> {
    db.subscribe(|sql| {
        sql.query(
            "SELECT record FROM coven_pending ORDER BY position",
            [],
            |r| r.get::<_, Vec<u8>>(0),
        )?
        .iter()
        .map(|record| Ok(serde_json::from_slice(record).map_err(DbError::PendingEncoding)?))
        .collect()
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    subject: Subject,
    reason: Reason,
    operation: Option<OperationId>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum Subject {
    Number(u64),
    Audience(String),
    Slot(u64),
}
impl From<u64> for Subject {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Reason {
    Missing,
    Refused,
    Update(u32),
    WritesRefused,
}
impl PendingValue for Record {
    fn provider_access(id: OperationId, _account: String, _grant: bool, _owner: bool) -> Self {
        operation_record(id)
    }
    fn reload(audience: coven_merge::Audience) -> Self {
        Self {
            subject: Subject::Audience(crate::write_encoding::audience_text(&audience)),
            reason: Reason::Missing,
            operation: None,
        }
    }
    fn writes_refused(slot: crate::Slot, _cutoff: crate::Slot, _cause: crate::WriteCutoff) -> Self {
        Self {
            subject: Subject::Slot(slot.0),
            reason: Reason::WritesRefused,
            operation: None,
        }
    }
    type Subject = Subject;
    fn subject(&self) -> &Subject {
        &self.subject
    }
    fn file(
        &self,
    ) -> Option<(
        coven_foundation::id_source::DeviceId,
        coven_foundation::id_source::FileId,
    )> {
        None
    }
    fn operation(&self) -> Option<OperationId> {
        self.operation
    }
    fn slot(&self) -> Option<crate::Slot> {
        match self.subject {
            Subject::Slot(number) => Some(crate::Slot(number)),
            _ => None,
        }
    }
    fn ends_when_covered(&self) -> bool {
        !matches!(self.reason, Reason::Refused | Reason::WritesRefused)
    }
    fn can_retry(&self, version: &str, schema: u32) -> bool {
        match self.reason {
            Reason::Missing => true,
            Reason::WritesRefused => false,
            Reason::Refused => version != env!("CARGO_PKG_VERSION"),
            Reason::Update(required) => schema >= required,
        }
    }
}
/// A missing-object record about a numbered subject with no operation.
pub(crate) fn missing(subject: u64) -> Record {
    Record {
        subject: subject.into(),
        reason: Reason::Missing,
        operation: None,
    }
}
pub(crate) fn operation_record(id: OperationId) -> Record {
    Record {
        subject: Subject::Number(id.0.try_into().unwrap()),
        reason: Reason::Missing,
        operation: Some(id),
    }
}
fn record(subject: u64) -> Record {
    Record {
        subject: subject.into(),
        reason: Reason::Refused,
        operation: None,
    }
}

#[tokio::test]
async fn reopening_preserves_conditions_and_a_version_change_rearms_without_erasing_them() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let pending = record(1);
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
            pauses_writes: false,
        })
        .await
        .unwrap();
    let first = operation_record(id);
    db.record_pending(first.clone()).await.unwrap();
    let mut live = records(&db);
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        std::slice::from_ref(&first)
    );
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
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        std::slice::from_ref(&next)
    );
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
    assert!(live
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 2);
    db.record_pending(operation_record(id)).await.unwrap();
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
        let pending = record(1);
        db.record_pending(pending.clone()).await.unwrap();
        let mut live = records(&db);
        assert_eq!(
            live.next().await.expect("the live query answers").unwrap(),
            std::slice::from_ref(&pending)
        );
        let mut ordinary =
            db.subscribe(|sql| Ok(sql.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?));
        assert_eq!(
            ordinary
                .next()
                .await
                .expect("the live query answers")
                .unwrap(),
            1
        );
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
            cause(db_error(
                live.next()
                    .await
                    .expect("the live query answers")
                    .unwrap_err(),
            )),
            cause(db_error(
                ordinary
                    .next()
                    .await
                    .expect("the live query answers")
                    .unwrap_err(),
            )),
            cause(db_error(db.pending::<Record>().await.unwrap_err())),
            cause(db_error(db.read(|_| Ok(())).await.unwrap_err())),
            cause(db.write(|_| Ok(())).await.unwrap_err()),
            cause(db.applied_migrations().unwrap_err()),
            cause(db.schema_version().await.unwrap_err()),
            cause(db_error(
                records(&db)
                    .next()
                    .await
                    .expect("the live query answers")
                    .unwrap_err(),
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
    let pending = record(1);
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
    let observed = cause(db_error(
        live.next()
            .await
            .expect("the live query answers")
            .unwrap_err(),
    ));
    assert!(std::sync::Arc::ptr_eq(&original, &observed));
    db.close().await.unwrap();
}

#[tokio::test]
async fn judgments_clear_only_with_a_committed_changed_reset() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = crate::Slot(1);
    db.check_boundary_fixture(crate::Boundary::Reset {
        entry,
        snapshot: coven_format::admin::SnapshotId {
            audience: coven_merge::Audience::Store,
            device: DeviceId(99),
            number: 1,
        },
    })
    .await
    .unwrap();
    let state = db.store_state().await.unwrap();
    let reload = |snapshots| crate::SnapshotReload::<
        crate::StagedRecords,
        crate::DownloadedWriteStream<std::io::Empty>,
    > {
        provider: Default::default(),
        snapshots,
        slots: vec![crate::IncomingSlot {
            digest: [0x31; 32],
            slot: entry,
            header: coven_format::sequence::SlotHeader {
                device: DeviceId(99),
                claim: 1,
                author: crate::tests::member(1),
                after: 0,
            },
            content: crate::SlotContent::Admin {
                original: coven_format::floor::OriginalSlot {
                    slot: entry,
                    object: vec![1],
                },
                effect: None,
            },
        }],
        target: 1,
        state: state.clone(),
        cutoffs: vec![],
        boundaries: vec![crate::BoundaryCoverage {
            boundary: crate::Boundary::Reset {
                entry,
                snapshot: state.resets[&coven_merge::Audience::Store].clone(),
            },
            included: 0,
        }],
        operation: None,
        authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
    };
    db.record_pending(record(1)).await.unwrap();
    assert!(db
        .load_snapshots::<crate::pending::tests::Record, std::io::Empty>(reload(Vec::new()))
        .await
        .is_err());
    assert_eq!(db.pending::<Record>().await.unwrap().len(), 1);
    db.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(reload(vec![
        crate::SnapshotSource::Empty(coven_merge::Audience::Store),
    ]))
    .await
    .unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    db.record_pending(record(1)).await.unwrap();
    db.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(reload(vec![
        crate::SnapshotSource::Empty(coven_merge::Audience::Store),
    ]))
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
                pauses_writes: false,
            })
            .await
            .unwrap();
        let original = operation_record(id);
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
async fn a_writes_wait_ends_when_slots_cover_it_but_a_verdict_on_its_bytes_remains() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let observed = |number, reason| Record {
        subject: Subject::Slot(number),
        reason,
        operation: None,
    };
    let waiting = observed(1, Reason::Missing);
    let refused = observed(2, Reason::Refused);
    let later = observed(3, Reason::Missing);
    for record in [&waiting, &refused, &later] {
        db.record_pending(record.clone()).await.unwrap();
    }
    // A loaded snapshot covers slots 1 and 2 without applying
    // them: the wait ends, the refusal stays, the uncovered wait stays.
    db.commit_writer(|sql| {
        crate::sequence::set_applied(sql, 2).unwrap();
        crate::pending::clear_covered_slots(sql).unwrap();
    });
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        vec![refused.clone(), later.clone()]
    );
    // Applying a write itself ends any record for it, verdicts included.
    for number in [2, 3] {
        db.commit_writer(|sql| crate::pending::clear_slot(sql, crate::Slot(number)).unwrap());
    }
    assert!(db.pending::<Record>().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_completed_scan_replaces_exactly_its_own_class_of_records() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let audience = |name: &str, reason| Record {
        subject: Subject::Audience(name.into()),
        reason,
        operation: None,
    };
    let scanned = |record: &Record| matches!(record.subject, Subject::Audience(_));
    let other = record(1);
    db.record_pending(other.clone()).await.unwrap();
    let mut live = records(&db);
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        std::slice::from_ref(&other)
    );
    let first = vec![
        audience("a", Reason::Missing),
        audience("b", Reason::Missing),
    ];
    db.replace_local_pending(scanned, first.clone())
        .await
        .unwrap();
    let mut expected = first.clone();
    expected.push(other.clone());
    expected.sort_by_key(|r| r.subject.clone());
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        expected
    );
    // An unchanged scan commits nothing.
    db.replace_local_pending(scanned, first).await.unwrap();
    // A subject that now checks loses its record; a changed reason replaces it.
    let next = vec![audience("b", Reason::Refused)];
    db.replace_local_pending(scanned, next.clone())
        .await
        .unwrap();
    let mut expected = next.clone();
    expected.push(other.clone());
    expected.sort_by_key(|r| r.subject.clone());
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        expected
    );
    // Records outside the scanned class and duplicate subjects are refused.
    assert!(db
        .replace_local_pending(scanned, vec![other.clone()])
        .await
        .is_err());
    let duplicate = audience("c", Reason::Missing);
    assert!(db
        .replace_local_pending(scanned, vec![duplicate.clone(), duplicate])
        .await
        .is_err());
    db.replace_local_pending(scanned, Vec::new()).await.unwrap();
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        std::slice::from_ref(&other)
    );
}

#[tokio::test]
async fn a_download_wait_lives_with_its_pending_record() {
    use crate::{RetrySubject, RetryWait};
    use std::time::{Duration, UNIX_EPOCH};
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let local = record(7);
    let subject = RetrySubject::Download {
        uploader: DeviceId(2),
        file: coven_foundation::id_source::FileId(uuid::Uuid::from_u128(9)),
    };
    let wait = RetryWait {
        failures: 1,
        not_before: UNIX_EPOCH + Duration::from_secs(1),
        wait: Duration::from_secs(1),
    };
    db.record_pending_retry(local.clone(), subject, Some(wait))
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.download_retry_waits().await.unwrap(), [(subject, wait)]);
    assert_eq!(db.retry_wait(subject).await.unwrap(), Some(wait));
    db.record_pending_retry(local.clone(), subject, None)
        .await
        .unwrap();
    assert!(db.download_retry_waits().await.unwrap().is_empty());
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        std::slice::from_ref(&local)
    );
    db.record_pending_retry(local.clone(), subject, Some(wait))
        .await
        .unwrap();
    db.clear_pending(local.subject.clone()).await.unwrap();
    assert!(db.download_retry_waits().await.unwrap().is_empty());
    assert!(db.record_retry(subject, wait).await.is_err());
    assert!(db.retry_waits().await.unwrap().is_empty());
    db.close().await.unwrap();
}

/// §19.1 and E5: a pending subject alone identifies its local record.
#[tokio::test]
async fn a_pending_subject_accepts_one_local_record_without_an_observer_identity() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let pending = record(7);
    db.commit_writer(|sql| {
        let subject = serde_json::to_vec(pending.subject()).unwrap();
        let value = serde_json::to_vec(&pending).unwrap();
        for _ in 0..2 {
            sql.internal_execute(
                "INSERT INTO _coven_pending(subject,value,coven_version) VALUES(?1,?2,?3)
                 ON CONFLICT(subject) DO UPDATE SET value=excluded.value",
                (&subject, &value, env!("CARGO_PKG_VERSION")),
            )
            .unwrap();
        }
    });
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        std::slice::from_ref(&pending)
    );
    db.clear_pending(pending.subject).await.unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    db.close().await.unwrap();
}

/// `coven_pending` gives each record its place in the order the records
/// arose, and `pending()` lists them in that order. A record whose condition changes keeps its place, and one that
/// arises after a later one is cleared still comes last (E4).
#[tokio::test]
async fn coven_pending_lists_records_in_the_order_they_arose() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let listed = || {
        db.read(|sql| {
            Ok(sql.query(
                "SELECT record FROM coven_pending ORDER BY position",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )?)
        })
    };
    let subjects = |records: Vec<Vec<u8>>| -> Vec<Subject> {
        records
            .iter()
            .map(|bytes| serde_json::from_slice::<Record>(bytes).unwrap().subject)
            .collect()
    };
    for subject in [5, 2, 9] {
        db.record_pending(record(subject)).await.unwrap();
    }
    assert_eq!(
        subjects(listed().await.unwrap()),
        [5.into(), 2.into(), 9.into()]
    );
    // Subject 2's condition changes; it keeps its place.
    db.record_pending(missing(2)).await.unwrap();
    db.clear_pending(Subject::from(9)).await.unwrap();
    db.record_pending(record(1)).await.unwrap();
    assert_eq!(
        subjects(listed().await.unwrap()),
        [5.into(), 2.into(), 1.into()]
    );
    // The call answers in the same order.
    let read: Vec<Subject> = db
        .pending::<Record>()
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.subject)
        .collect();
    assert_eq!(read, [5.into(), 2.into(), 1.into()]);
    db.close().await.unwrap();
}
