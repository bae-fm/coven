use super::*;
use crate::{tests::TestStore, DbError, NewOperation, OperationUpdate};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::id_source::DeviceId;
use serde::Deserialize;

/// The pending records as the app's SQL reads them from `coven_pending`,
/// as a live query.
fn records(db: &crate::Database) -> crate::LiveQuery<Vec<Record>, DbError> {
    db.subscribe(|sql| {
        sql.query("SELECT record FROM coven_pending ORDER BY position", [], |r| {
            r.get::<_, Vec<u8>>(0)
        })?
        .iter()
        .map(|record| serde_json::from_slice(record).map_err(DbError::StateEncoding))
        .collect()
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    subject: Subject,
    reason: Reason,
    operation: Option<OperationId>,
    #[serde(skip)]
    cause: Option<String>,
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
    /// A move's original is missing: the record names the move.
    Original(i64),
}
impl PendingValue for Record {
    fn provider_access(id: OperationId, _account: String, _grant: bool) -> Self {
        operation_record(id)
    }
    fn reload(audience: coven_merge::Audience) -> Self {
        Self {
            subject: Subject::Audience(crate::write_encoding::audience_text(&audience)),
            reason: Reason::Missing,
            operation: None,
            cause: None,
        }
    }
    fn raise_wait(write: coven_merge::WriteId, _: coven_merge::Audience, _: u32) -> Self {
        Self {
            subject: Subject::Slot(write.number),
            reason: Reason::Missing,
            operation: None,
            cause: None,
        }
    }
    fn raise_awaited(&self) -> Option<(coven_merge::WriteId, &coven_merge::Audience, u32)> {
        None
    }
    fn writes_refused(slot: crate::Slot, _cutoff: crate::Slot, _cause: crate::WriteCutoff) -> Self {
        Self {
            subject: Subject::Slot(slot.0),
            reason: Reason::WritesRefused,
            operation: None,
            cause: None,
        }
    }
    type Subject = Subject;
    fn subject(&self) -> &Subject {
        &self.subject
    }
    fn file(
        &self,
    ) -> Option<(
        crate::FileWork,
        coven_foundation::id_source::DeviceId,
        coven_foundation::id_source::FileId,
    )> {
        None
    }
    fn operation(&self) -> Option<OperationId> {
        self.operation
    }
    fn move_of(&self) -> Option<crate::MoveId> {
        match self.reason {
            Reason::Original(of) => Some(crate::MoveId(of)),
            _ => None,
        }
    }
    fn pin_of(&self) -> Option<coven_foundation::id_source::FileId> {
        None
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
    const BUILD: u128 = 1;
    fn can_retry(&self, build: u128, schema: u32) -> bool {
        match self.reason {
            Reason::Missing | Reason::Original(_) => true,
            Reason::WritesRefused => false,
            Reason::Refused => build != Self::BUILD,
            Reason::Update(required) => schema >= required,
        }
    }
    fn cause(&self) -> Option<&str> {
        self.cause.as_deref()
    }
    fn with_cause(self, cause: Option<String>) -> Self {
        Self { cause, ..self }
    }
}
/// A missing-object record about a numbered subject with no operation.
pub(crate) fn missing(subject: u64) -> Record {
    Record { subject: subject.into(), reason: Reason::Missing, operation: None, cause: None }
}
/// A record that a move's original is missing, about a numbered subject.
pub(crate) fn moving(subject: u64, of: crate::MoveId) -> Record {
    Record { subject: subject.into(), reason: Reason::Original(of.0), operation: None, cause: None }
}
pub(crate) fn operation_record(id: OperationId) -> Record {
    Record {
        subject: Subject::Number(id.0.try_into().unwrap()),
        reason: Reason::Missing,
        operation: Some(id),
        cause: None,
    }
}
fn record(subject: u64) -> Record {
    Record { subject: subject.into(), reason: Reason::Refused, operation: None, cause: None }
}

#[tokio::test]
async fn reopening_preserves_conditions_and_a_build_change_rearms_without_erasing_them() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let pending = record(1);
    db.record_pending(pending.clone()).await.unwrap();
    assert!(!db.pending_can_retry::<Record>(1.into()).await.unwrap());
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap(), std::slice::from_ref(&pending));
    assert!(!db.pending_can_retry::<Record>(1.into()).await.unwrap());
    // A database whose records another build of coven recorded.
    db.set_pending_build(2).await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap(), std::slice::from_ref(&pending));
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
    let next = Record { reason: Reason::Update(2), ..first.clone() };
    db.advance_operation_pending(
        OperationUpdate { id, previous: 0, last_step: 1, data: vec![1] },
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
            OperationUpdate { id, previous: 0, last_step: 2, data: vec![2] },
            first
        )
        .await
        .is_err());
    assert_eq!(db.pending::<Record>().await.unwrap(), [next]);
    db.advance_operation(OperationUpdate { id, previous: 1, last_step: 2, data: vec![2] })
        .await
        .unwrap();
    assert!(live.next().await.expect("the live query answers").unwrap().is_empty());
    assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 2);
    db.record_pending(operation_record(id)).await.unwrap();
    db.finish_operation(id).await.unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    assert!(db.operation(id).await.unwrap().is_none());
}

fn cause(error: DbError) -> std::sync::Arc<DbError> {
    let DbError::StatePersistence(cause) = error else { panic!("{error:?}") };
    assert!(matches!(*cause, DbError::Sqlite(_)));
    cause
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
        assert_eq!(ordinary.next().await.expect("the live query answers").unwrap(), 1);
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
            db.record_pending(Record { reason: Reason::Missing, ..pending.clone() }).await
        } else {
            db.clear_pending(pending.subject.clone()).await
        };
        let original = cause(failed.unwrap_err());
        let failures = [
            cause(live.next().await.expect("the live query answers").unwrap_err()),
            cause(ordinary.next().await.expect("the live query answers").unwrap_err()),
            cause(db.pending::<Record>().await.unwrap_err()),
            cause(db.read(|_| Ok(())).await.unwrap_err()),
            cause(db.write(|_| Ok(())).await.unwrap_err()),
            cause(db.applied_migrations().unwrap_err()),
            cause(db.schema_version().await.unwrap_err()),
            cause(records(&db).next().await.expect("the live query answers").unwrap_err()),
        ];
        assert!(failures.iter().all(|error| std::sync::Arc::ptr_eq(&original, error)));
        db.close().await.unwrap();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        assert_eq!(db.pending::<Record>().await.unwrap(), std::slice::from_ref(&pending));
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
        released.lock().unwrap().recv_timeout(HANG_GUARD).unwrap();
        Ok(records)
    });
    tokio::select! {
        result = live.next() => panic!("held query returned: {result:?}"),
        result = entry => result.unwrap(),
    }
    let failed = db.record_pending(pending).await;
    release.send(()).unwrap();
    let original = cause(failed.unwrap_err());
    let observed = cause(live.next().await.expect("the live query answers").unwrap_err());
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
        crate::SnapshotStaging,
        crate::DownloadedWriteStream<std::io::Empty>,
    > {
        provider_access: Vec::new(),
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
                original: coven_format::floor::OriginalSlot { slot: entry, object: vec![1] },
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
        .load_snapshots::<crate::pending::tests::Record, std::io::Empty>(reload(Vec::new()), None)
        .await
        .is_err());
    assert_eq!(db.pending::<Record>().await.unwrap().len(), 1);
    db.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        reload(vec![crate::SnapshotSource::Empty(coven_merge::Audience::Store)]),
        None,
    )
    .await
    .unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    db.record_pending(record(1)).await.unwrap();
    db.load_snapshots::<crate::pending::tests::Record, std::io::Empty>(
        reload(vec![crate::SnapshotSource::Empty(coven_merge::Audience::Store)]),
        None,
    )
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
        let update = OperationUpdate { id, previous: 0, last_step: 1, data: vec![1] };
        let error = if statement == "INSERT" {
            db.advance_operation_pending(
                update,
                Record { reason: Reason::Update(2), ..original.clone() },
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
        cause: None,
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
    assert_eq!(db.pending::<Record>().await.unwrap(), vec![refused.clone(), later.clone()]);
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
        cause: None,
    };
    let scanned = |record: &Record| matches!(record.subject, Subject::Audience(_));
    let other = record(1);
    db.record_pending(other.clone()).await.unwrap();
    let mut live = records(&db);
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        std::slice::from_ref(&other)
    );
    let first = vec![audience("a", Reason::Missing), audience("b", Reason::Missing)];
    db.replace_local_pending(scanned, first.clone()).await.unwrap();
    let mut expected = first.clone();
    expected.push(other.clone());
    expected.sort_by_key(|r| r.subject.clone());
    assert_eq!(live.next().await.expect("the live query answers").unwrap(), expected);
    // An unchanged scan commits nothing.
    db.replace_local_pending(scanned, first).await.unwrap();
    // A subject that now checks loses its record; a changed reason replaces it.
    let next = vec![audience("b", Reason::Refused)];
    db.replace_local_pending(scanned, next.clone()).await.unwrap();
    let mut expected = next.clone();
    expected.push(other.clone());
    expected.sort_by_key(|r| r.subject.clone());
    assert_eq!(live.next().await.expect("the live query answers").unwrap(), expected);
    // Records outside the scanned class and duplicate subjects are refused.
    assert!(db.replace_local_pending(scanned, vec![other.clone()]).await.is_err());
    let duplicate = audience("c", Reason::Missing);
    assert!(db.replace_local_pending(scanned, vec![duplicate.clone(), duplicate]).await.is_err());
    db.replace_local_pending(scanned, Vec::new()).await.unwrap();
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        std::slice::from_ref(&other)
    );
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
                "INSERT INTO _coven_pending(subject,value,build) VALUES(?1,?2,?3)
                 ON CONFLICT(subject) DO UPDATE SET value=excluded.value",
                (&subject, &value, Record::BUILD.to_be_bytes()),
            )
            .unwrap();
        }
    });
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap(), std::slice::from_ref(&pending));
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
            Ok::<_, DbError>(sql.query(
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
    assert_eq!(subjects(listed().await.unwrap()), [5.into(), 2.into(), 9.into()]);
    // Subject 2's condition changes; it keeps its place.
    db.record_pending(missing(2)).await.unwrap();
    db.clear_pending(Subject::from(9)).await.unwrap();
    db.record_pending(record(1)).await.unwrap();
    assert_eq!(subjects(listed().await.unwrap()), [5.into(), 2.into(), 1.into()]);
    // The call answers in the same order.
    let read: Vec<Subject> =
        db.pending::<Record>().await.unwrap().into_iter().map(|record| record.subject).collect();
    assert_eq!(read, [5.into(), 2.into(), 1.into()]);
    db.close().await.unwrap();
}

/// A record's cause is written with it, in the same commit, so a live
/// query on `coven_pending` sees the record and its cause arrive together
/// and change together. Opening clears every cause: the run that wrote it
/// met that error, and the next run has met none of its own.
#[tokio::test]
async fn a_records_cause_is_written_with_it_and_cleared_at_open() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let follow = |db: &crate::Database| {
        db.subscribe(|sql| {
            Ok(sql.query("SELECT record,cause FROM coven_pending ORDER BY position", [], |r| {
                let record: Vec<u8> = r.get(0)?;
                Ok((serde_json::from_slice::<Record>(&record).unwrap(), r.get(1)?))
            })?)
        })
    };
    let mut live: crate::LiveQuery<_, DbError> = follow(&db);
    let mut next = async || live.next().await.expect("the live query answers").unwrap();
    assert_eq!(next().await, vec![]);
    let failed = Record { cause: Some("disk full".into()), ..record(1) };
    db.record_pending(failed.clone()).await.unwrap();
    assert_eq!(next().await, vec![(record(1), Some("disk full".to_owned()))]);
    assert_eq!(db.pending::<Record>().await.unwrap(), vec![failed]);
    // The same reason met again with another error keeps the newer one;
    // one an app call received keeps none.
    db.record_pending(Record { cause: Some("still full".into()), ..record(1) }).await.unwrap();
    assert_eq!(next().await, vec![(record(1), Some("still full".to_owned()))]);
    db.record_pending(record(1)).await.unwrap();
    assert_eq!(next().await, vec![(record(1), None)]);
    db.record_pending(Record { cause: Some("full again".into()), ..record(1) }).await.unwrap();
    assert_eq!(next().await, vec![(record(1), Some("full again".to_owned()))]);
    drop(live);
    db.close().await.unwrap();

    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap(), vec![record(1)]);
    let mut live = follow(&db);
    assert_eq!(live.next().await.unwrap().unwrap(), vec![(record(1), None)]);
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_pending(subject,value,operation,object,work,device,file,build)
           SELECT key,x'00',CASE WHEN i%2=0 THEN i END,
                  CAST(x'00' || n AS BLOB),CASE WHEN i%4=1 THEN 'upload' WHEN i%4=3 THEN 'download' END,
                  CASE WHEN i%2=1 THEN device END,
                  CASE WHEN i%2=1 THEN substr(key,17,16) END,zeroblob(16) FROM fixture;",
    )
    .unwrap();
}

/// The scans this module's statements may make, with their reasons, for the
/// plan audit (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    _sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
("pending.rs", "{SELECT} ORDER BY id", "_coven_pending") =>
    Some("pending returns every retained condition"),
("pending.rs", "SELECT subject,value,operation,object,work,device,file,build,move,pin,cause,id FROM _coven_pending", "_coven_pending") =>
    Some("pending returns every retained condition"),
("pending.rs", "UPDATE _coven_pending SET build=?1", "_coven_pending") =>
    Some("test support: every record becomes another build's"),
("pending.rs", "UPDATE _coven_pending SET cause=NULL WHERE cause IS NOT NULL", "_coven_pending") =>
    Some("once per open: every record's cause names an error an earlier run met"),
("pending.rs", "DELETE FROM _coven_pending AS p WHERE NOT EXISTS(SELECT 1 FROM _coven_provider_access a WHERE a.operation=p.operation)", "p") =>
    Some("reset visits every condition; provider-obligation lookups remain indexed"),
        _ => None,
    }
}

/// §19.3: a stored record naming only part of a file is damage, not a
/// query error.
#[tokio::test]
async fn a_stored_record_with_part_of_a_file_is_damaged_database() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.record_pending(record(1)).await.unwrap();
    db.inspect_writer(|sql| {
        sql.batch(
            "PRAGMA ignore_check_constraints=ON; UPDATE _coven_pending SET work='upload';
             PRAGMA ignore_check_constraints=OFF;",
        )
        .unwrap()
    });
    let read = db.pending::<Record>().await;
    assert!(matches!(read, Err(DbError::DamagedDatabase)), "{read:?}");
    db.close().await.unwrap();
}

/// A stored pending record that doesn't decode as the requested record type
/// is a state encoding failure (E5).
#[tokio::test]
async fn a_pending_record_read_as_another_type_fails_as_state_encoding() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.record_pending(record(1)).await.unwrap();
    db.inspect_writer(|sql| {
        sql.batch("UPDATE _coven_pending SET value=CAST('{\"shape\":\"another type\"}' AS BLOB)")
            .unwrap()
    });
    let read = db.pending::<Record>().await;
    assert!(matches!(read, Err(DbError::StateEncoding(_))), "{read:?}");
    db.close().await.unwrap();
}
