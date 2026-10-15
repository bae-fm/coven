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
            inner.inputs.test_track_work(&counter);
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

    /// Record a device's replayed state as a kept store-log entry would, under
    /// a placeholder member when the device is not yet registered.
    pub async fn test_device_state(
        &self,
        device: coven_foundation::id_source::DeviceId,
        state: crate::DeviceState,
    ) -> Result<(), DbError> {
        let state = match state {
            crate::DeviceState::Active => "active",
            crate::DeviceState::Removed => "removed",
            crate::DeviceState::Replaced => "replaced",
        };
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let member = [7u8; 16];
                    let keys = coven_crypto::MemberKeys::from_secret_bytes(
                        &[b"CVMK\x01".as_slice(), &[7; 64]].concat(),
                    )
                    .expect("fixed placeholder pair")
                    .public_keys();
                    db.internal_execute(
                        "INSERT INTO _coven_members(member,signing,sealing,access,role,removed)
                         VALUES(?1,?2,?3,x'00','member',0) ON CONFLICT DO NOTHING",
                        (
                            member.as_slice(),
                            keys.signing.as_bytes().as_slice(),
                            keys.sealing.as_bytes().as_slice(),
                        ),
                    )?;
                    db.internal_execute(
                        "INSERT INTO _coven_devices(device,member,name,state) VALUES(?1,?2,'test',?3)
                         ON CONFLICT(device) DO UPDATE SET state=excluded.state",
                        (device.0.to_be_bytes().as_slice(), member.as_slice(), state),
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Model a snapshot load's position jump: install consumed positions
    /// without applying their writes, clearing covered waits in that commit.
    pub async fn test_cover_positions(&self, positions: Vec<(u64, u64)>) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    for (device, number) in &positions {
                        db.internal_execute(
                            "INSERT INTO _coven_positions(device,number) VALUES(?1,?2)",
                            (
                                device.to_be_bytes().as_slice(),
                                number.to_be_bytes().as_slice(),
                            ),
                        )?;
                    }
                    crate::pending::clear_covered_writes(db)
                })
            })
        })
        .await
    }

    /// The progress hook an applied log object runs in its transaction.
    pub async fn test_clear_log(&self, object: crate::LogObject) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::pending::clear_log(db, object))
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

    /// Each queued write in number order, with whether its first attempt fixed
    /// its keys. Queue differentials compare this with the model's tried flags.
    pub async fn test_queued_attempts(&self) -> Result<Vec<(coven_merge::WriteId, bool)>, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.query(
                    "SELECT device,number,sealing_keys IS NOT NULL FROM _coven_uploads ORDER BY device,number",
                    [],
                    |r| {
                        Ok((
                            coven_merge::WriteId {
                                device: coven_foundation::id_source::DeviceId(
                                    crate::write_encoding::counter(r.get(0)?),
                                ),
                                number: crate::write_encoding::counter(r.get(1)?),
                            },
                            r.get(2)?,
                        ))
                    },
                )
            })
        })
        .await
    }

    /// The production window verdict for a received write against the
    /// applied store log, without applying it: whether it is late and whether
    /// that verdict is final (§10). Differentials compare it with the model.
    pub async fn test_write_landing(
        &self,
        header: coven_format::write::WriteHeader,
        published: crate::Published,
    ) -> Result<(bool, bool), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                let landing = crate::write_landing::judge(
                    writer,
                    &header,
                    &published.signer,
                    published.stored_at,
                )?;
                Ok((landing.late, landing.fixed))
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
            pauses_writes: false,
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
                Vec::new(),
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
        db.apply_store_log_batch::<Record>(
            before,
            incoming,
            replay,
            vec![],
            vec![],
            Vec::new(),
            Vec::new()
        )
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
            .apply_store_log_batch::<Record>(
                before,
                entries,
                replay,
                vec![],
                vec![],
                Vec::new(),
                Vec::new(),
            )
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
                pauses_writes: false,
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
                    pauses_writes: false,
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
}
