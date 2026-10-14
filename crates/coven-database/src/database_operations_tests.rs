//! Test transport support using the actual local-write queue.

use super::Database;
use crate::DbError;

impl Database {
    /// Count executed work on every connection without including counter reads.
    /// Install while the database is quiescent, before starting the measured pass.
    pub async fn test_track_work(
        &self,
        counter: crate::test_utils::WorkCounter,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                *writer.work.borrow_mut() = Some(counter.clone());
                Ok::<_, DbError>(())
            })?;
            inner.access.readers.test_track_work(&counter);
            Ok(())
        })
        .await
    }

    /// Observe transaction completion and file cleanup on the writer thread.
    /// Install after setup; the callback may terminate a subprocess to simulate
    /// a crash without Rust destructors or SQLite connection shutdown.
    pub async fn test_on_write_checkpoint(
        &self,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.on_write_checkpoint(callback);
                Ok(())
            })
        })
        .await
    }

    /// Model a database last judged by another library release before reopening it.
    pub async fn test_pending_version(&self, version: &'static str) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute(
                        "UPDATE _coven_pending SET coven_version=?1 WHERE reporter=?2",
                        (version, inner.access.device.0.to_be_bytes().as_slice()),
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Inject an agreement fault without changing positions or application rows.
    /// Recovery tests use the actual incremental sum and production reload path.
    pub async fn test_damage_fingerprint(
        &self,
        audience: coven_merge::Audience,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute("INSERT INTO _coven_fingerprint_sums(audience,sum) VALUES(?1,?2) ON CONFLICT(audience) DO UPDATE SET sum=excluded.sum",
                        (crate::write_encoding::audience_text(&audience), [37u8;32].as_slice()))?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Test transport: read actual queued records without reauthoring their writes.
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer
                    .query(
                        "SELECT record FROM _coven_uploads ORDER BY device,number",
                        [],
                        |r| r.get::<_, Vec<u8>>(0),
                    )?
                    .iter()
                    .map(|bytes| {
                        coven_format::write_stream::decode_plaintext(bytes).map_err(Into::into)
                    })
                    .collect()
            })
        })
        .await
    }
}

#[cfg(test)]
#[tokio::test]
async fn a_ready_batch_rolls_back_entries_cached_state_and_operation_progress_together() {
    use crate::pending::tests::Record;
    use crate::store_log::tests::{checked, circle_history};
    use crate::{NewOperation, OperationUpdate};
    let store = crate::tests::TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let history = circle_history(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(2),
    ));
    db.apply_store_log::<Record>(
        checked(history[0].0.clone()),
        history[0].1.clone(),
        Vec::new(),
    )
    .await
    .unwrap();
    let before = db.local_store_log().await.unwrap().log;
    let operation = db
        .start_operation(NewOperation {
            kind: "entry".into(),
            data: b"reserved".to_vec(),
            started_by: "test".into(),
        })
        .await
        .unwrap();
    let mut observed = db.subscribe(|sql| sql.store_log());
    assert_eq!(observed.next().await.unwrap(), before);
    let incoming = history[1..4]
        .iter()
        .map(|(entry, _)| checked(entry.clone()))
        .collect::<Vec<_>>();
    let replay = history[3].1.clone();
    let update = || OperationUpdate {
        id: operation,
        previous: 0,
        last_step: 4,
        data: b"applied".to_vec(),
    };
    for failure in [
        "BEFORE INSERT ON _coven_store_log WHEN NEW.number=X'0000000000000004'",
        "BEFORE UPDATE ON _coven_operations",
    ] {
        db.inspect_writer(|sql| sql.fail_at("refuse_batch", failure, "batch failed"));
        assert!(db
            .apply_store_log_batch::<Record>(
                before.clone(),
                incoming.clone(),
                replay.clone(),
                vec![],
                vec![update()],
                Vec::new()
            )
            .await
            .is_err());
        assert_eq!(db.store_log().await.unwrap(), before);
        assert_eq!(db.local_store_log().await.unwrap().log, before);
        assert_eq!(db.operation(operation).await.unwrap().unwrap().last_step, 0);
        assert!(!observed.is_marked_for_rerun());
        db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse_batch").unwrap());
    }
    db.apply_store_log_batch::<Record>(
        before.clone(),
        incoming.clone(),
        replay.clone(),
        vec![],
        vec![update()],
        Vec::new(),
    )
    .await
    .unwrap();
    let after = observed.next().await.unwrap();
    assert_eq!(after.entries.len(), 4);
    assert_eq!(after.replay, replay);
    assert_eq!(db.local_store_log().await.unwrap().log, after);
    assert_eq!(db.operation(operation).await.unwrap().unwrap().last_step, 4);
    assert!(matches!(
        db.apply_store_log_batch::<Record>(before, incoming, replay, vec![], vec![], Vec::new())
            .await,
        Err(DbError::StoreLogEntriesChanged)
    ));
    db.close().await.unwrap();
    let reopened = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(reopened.local_store_log().await.unwrap().log, after);
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_batch_caller_cannot_leave_the_decoded_log_behind_its_commit() {
    use crate::pending::tests::Record;
    use crate::store_log::tests::{checked, circle_history};
    use crate::test_utils::WriteCheckpoint;
    use std::time::Duration;
    let store = crate::tests::TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let history = circle_history(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(2),
    ));
    let before = db.local_store_log().await.unwrap().log;
    let entries = history[..4]
        .iter()
        .map(|(entry, _)| checked(entry.clone()))
        .collect();
    let replay = history[3].1.clone();
    let (committed, wait_committed) = tokio::sync::oneshot::channel();
    let committed = std::sync::Mutex::new(Some(committed));
    let (resume, wait_resume) = std::sync::mpsc::channel();
    db.test_on_write_checkpoint(move |checkpoint| {
        if checkpoint == WriteCheckpoint::Committed {
            if let Some(sender) = committed.lock().unwrap().take() {
                sender.send(()).unwrap();
                wait_resume.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
    })
    .await
    .unwrap();
    let writer = db.clone();
    let expected = replay.clone();
    let task = tokio::spawn(async move {
        writer
            .apply_store_log_batch::<Record>(before, entries, replay, vec![], vec![], Vec::new())
            .await
    });
    wait_committed.await.unwrap();
    let visible = db.store_log().await.unwrap();
    assert_eq!(
        visible.entries.len(),
        4,
        "the first committed view contains the whole batch"
    );
    assert_eq!(visible.replay, expected);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    let current = db.local_store_log().await.unwrap().log;
    assert_eq!(current.entries.len(), 4);
    assert_eq!(current.replay, expected);
    assert_eq!(db.store_log().await.unwrap(), current);
}

#[cfg(test)]
mod conformance {
    use crate::{
        tests::TestStore, AccessKeyDeletion, AccessKeyToDelete, DbError, NewOperation,
        OperationUpdate,
    };

    fn deletion(key: &str) -> AccessKeyDeletion {
        AccessKeyDeletion {
            key: AccessKeyToDelete {
                access_key_id: key.into(),
                principal: coven_format::S3Principal::Console,
                member: None,
            },
            operation: NewOperation {
                kind: "delete-access-key".into(),
                data: key.as_bytes().to_vec(),
                started_by: "coven".into(),
            },
        }
    }

    async fn pending(db: &crate::Database) -> Vec<String> {
        let mut keys: Vec<_> = db
            .pending::<crate::pending::tests::Record>()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|record| record.access_key().map(str::to_owned))
            .collect();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn access_key_confirmation_survives_repeated_notices_and_reopening() {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.clear_access_key("confirmed-first".into()).await.unwrap();
        db.close().await.unwrap();
        for replayed in [false, true] {
            let db = store.builder(vec![], vec![]).open().await.unwrap();
            let id = db
                .start_operation(NewOperation {
                    kind: "provider-access".into(),
                    data: vec![1],
                    started_by: "cancel_invite".into(),
                })
                .await
                .unwrap();
            let keys = ["confirmed-first", "confirmed-after"].map(deletion);
            // A failed journal update must not leave a deletion notice behind.
            let error = db
                .record_access_key_deletions::<crate::pending::tests::Record>(
                    Some(OperationUpdate {
                        id,
                        previous: 1,
                        last_step: 2,
                        data: vec![2],
                    }),
                    keys.into_iter().collect(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, DbError::OperationChanged(changed) if changed == id));
            assert!(pending(&db).await.is_empty());
            assert_eq!(db.operations().await.unwrap().len(), 1);
            assert_eq!(db.operation(id).await.unwrap().unwrap().last_step, 0);
            db.record_access_key_deletions::<crate::pending::tests::Record>(
                Some(OperationUpdate {
                    id,
                    previous: 0,
                    last_step: 1,
                    data: vec![2],
                }),
                ["confirmed-first", "confirmed-after"].map(deletion).into(),
            )
            .await
            .unwrap();
            // Recording an open key again keeps its one operation and notice.
            db.record_access_key_deletions::<crate::pending::tests::Record>(
                None,
                vec![deletion("confirmed-after")],
            )
            .await
            .unwrap();
            let expected: Vec<String> = if replayed {
                vec![]
            } else {
                vec!["confirmed-after".into()]
            };
            assert_eq!(pending(&db).await, expected);
            assert_eq!(db.operations().await.unwrap().len(), 1 + expected.len());
            db.clear_access_key("confirmed-after".into()).await.unwrap();
            db.clear_access_key("confirmed-after".into()).await.unwrap();
            assert!(pending(&db).await.is_empty());
            assert_eq!(db.operations().await.unwrap().len(), 1);
            db.finish_operation(id).await.unwrap();
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn finishing_with_work_commits_each_follow_up_with_its_condition() {
        use crate::pending::tests::{reported_operation, Record};
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let device = db.sync_state(Vec::new()).await.unwrap().device;
        let new = |n: u8| NewOperation {
            kind: "follow-up".into(),
            data: vec![n],
            started_by: "coven".into(),
        };
        let id = db.start_operation(new(0)).await.unwrap();
        // A condition naming another row fails the whole commit.
        let error = db
            .finish_operation_with_work(
                id,
                vec![
                    (
                        new(1),
                        Box::new(move |started| reported_operation(device, started))
                            as Box<dyn FnOnce(crate::OperationId) -> Record + Send>,
                    ),
                    (new(2), Box::new(move |_| reported_operation(device, id))),
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::DamagedDatabase));
        assert_eq!(
            db.operations()
                .await
                .unwrap()
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [id]
        );
        db.finish_operation_with_work(
            id,
            [new(1), new(2)]
                .into_iter()
                .map(|operation| {
                    (operation, move |started| {
                        reported_operation(device, started)
                    })
                })
                .collect(),
        )
        .await
        .unwrap();
        let started = db.operations().await.unwrap();
        assert_eq!(
            started
                .iter()
                .map(|record| record.data.clone())
                .collect::<Vec<_>>(),
            [vec![1], vec![2]]
        );
        for record in &started {
            assert_eq!(
                db.operation_pending::<Record>(record.id).await.unwrap(),
                Some(reported_operation(device, record.id))
            );
        }
        // A row that already finished cannot leave follow-ups behind.
        let error = db
            .finish_operation_with_work(
                id,
                vec![(new(3), move |started| reported_operation(device, started))],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::OperationChanged(changed) if changed == id));
        assert_eq!(db.operations().await.unwrap().len(), 2);
        db.close().await.unwrap();
    }
}
