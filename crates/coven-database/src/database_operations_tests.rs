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
                writer.test_track_work(counter.clone());
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
                    db.internal_execute("UPDATE _coven_pending SET coven_version=?1", [version])?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// Record a device's state as a counted administrative slot would, under
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
mod conformance {
    use crate::{
        tests::TestStore, AccessKeyDeletion, AccessKeyToDelete, DbError, NewOperation,
        OperationUpdate,
    };

    fn deletion(key: &str) -> AccessKeyDeletion {
        AccessKeyDeletion {
            key: AccessKeyToDelete {
                access_key_id: key.into(),
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
