use super::*;
use serde_json::{json, Value};

#[tokio::test]
async fn maintenance_failures_are_visible_and_wait_for_a_changed_connection() {
    for job in [
        SnapshotJob::Write {
            audience: Audience::Store,
            device: DeviceId(1),
            trigger: SnapshotTrigger::Requested,
            session: None,
        },
        SnapshotJob::Reload {
            scope: ReloadScope::All,
            files: None,
        },
        SnapshotJob::Retain,
    ] {
        let storage = Arc::new(
            MemoryStorage::builder()
                .provider(coven_storage::CloudProvider::GoogleDrive)
                .transfer_limits(65536, 65536)
                .build()
                .unwrap(),
        );
        let [mut a, _b, _c] = accounts(storage.clone()).await;
        let id =
            a.db.start_operation(
                Data::Snapshots(SnapshotTask {
                    job,
                    temporary: Vec::new(),
                })
                .new_operation("coven")
                .unwrap(),
            )
            .await
            .unwrap();
        a.sync.storage = Some(Arc::new(
            MemoryStorage::for_recipient(&storage, "uninvited@example.com").unwrap(),
        ));
        let files = file_owner(&a);
        let operations = operation_owner(a.sync, files);
        assert_eq!(pending(&operations, &a.db).await.len(), 1);
        let failed = a.db.operations().await.unwrap().remove(0);
        assert_eq!(failed.id, id);
        assert!(a
            .db
            .operation_pending::<PendingRecord>(id)
            .await
            .unwrap()
            .is_some());
        assert!(
            matches!(operations.retry_pending_operation(id).await, Err(SyncError::NotPending(found)) if found == id)
        );
        assert!(
            matches!(operations.discard_pending_operation(id).await, Err(SyncError::NotPending(found)) if found == id)
        );
        let error = operations.sync().await.unwrap_err();
        assert!(
            matches!(&error, SyncError::Storage(error) if error.failure() == StorageFailure::PermissionDenied),
            "{error:?}"
        );
        operations.set_storage(Some(storage)).await.unwrap();

        operations.sync().await.unwrap();
        assert!(!a.db.operations().await.unwrap().iter().any(|r| r.id == id));
        operations.close().await.unwrap();
    }
}

#[tokio::test]
async fn app_reload_and_reset_stay_pending_until_explicit_retry() {
    for (command, kind) in [
        (Command::Reload, OperationKind::ReloadFromSnapshot),
        (Command::Reset(Audience::Store), OperationKind::Reset),
    ] {
        let storage = Arc::new(
            MemoryStorage::builder()
                .provider(coven_storage::CloudProvider::GoogleDrive)
                .transfer_limits(65536, 65536)
                .build()
                .unwrap(),
        );
        let [mut a, _b, _c] = accounts(storage.clone()).await;
        let id = begin(&mut a, command).await;
        a.sync.storage = Some(Arc::new(
            MemoryStorage::for_recipient(&storage, "uninvited@example.com").unwrap(),
        ));
        let files = file_owner(&a);
        let operations = operation_owner(a.sync, files);
        let conditions = pending(&operations, &a.db).await;
        assert_eq!(conditions.len(), 1);
        assert_eq!(pending_id(&conditions[0]), id);
        assert_eq!(pending_kind(&conditions[0]), kind);
        assert!(matches!(
            conditions[0].reason,
            PendingReason::Storage {
                failure: StorageFailure::PermissionDenied,
                ..
            }
        ));
        operations.set_storage(Some(storage.clone())).await.unwrap();
        if kind == OperationKind::ReloadFromSnapshot {
            assert!(
                matches!(operations.sync().await, Err(SyncError::ReloadPending(found)) if found == id)
            );
        } else {
            operations.sync().await.unwrap();
        }
        assert_eq!(pending(&operations, &a.db).await, conditions);
        operations.close().await.unwrap();
        a.sync = StoreLogSync::new(
            storage.clone(),
            a.db.clone(),
            a.custody.clone(),
            a.identity.clone(),
            a.clock.clone(),
            Arc::new(coven_foundation::id_source::UuidIds),
            a.directory.clone(),
        );
        let files = file_owner(&a);
        let operations = operation_owner(a.sync, files);
        assert_eq!(pending(&operations, &a.db).await, conditions);
        operations.retry_pending_operation(id).await.unwrap();
        assert!(pending(&operations, &a.db).await.is_empty());
        operations.close().await.unwrap();
    }
}

#[tokio::test]
async fn migration_and_audience_publication_share_the_schema_change_purpose() {
    let storage = storage();
    let mut a = device(storage, 1, member(1), store(1)).await;
    a.create(key(1)).await;
    for operation in [
        StoreLogSync::migration_operation(2).unwrap(),
        Data::Snapshots(SnapshotTask {
            job: SnapshotJob::Write {
                audience: Audience::Store,
                device: a.device().await,
                trigger: SnapshotTrigger::Raise {
                    version: 2,
                    entry: None,
                },
                session: None,
            },
            temporary: Vec::new(),
        })
        .new_operation("coven")
        .unwrap(),
    ] {
        let id = a.db.start_operation(operation).await.unwrap();
        let record = a.db.operation(id).await.unwrap().unwrap();
        a.sync
            .record_operation_condition(&record, PendingReason::Failed(LocalFailure::Operation))
            .await
            .unwrap();
        assert_eq!(
            a.db.pending::<PendingRecord>().await.unwrap(),
            [PendingRecord {
                subject: PendingSubject::Operation {
                    id,
                    kind: OperationKind::SchemaChange
                },
                reason: PendingReason::Failed(LocalFailure::Operation),
                reported_by: a.device().await,
            }]
        );
        a.sync
            .begin_operation_call(Command::Discard(id))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_reload_call_receives_its_native_failure_and_automatic_retry_replaces_the_reason() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let (sleeps, mut sleeping) = tokio::sync::watch::channel(0);
    a.sync.clock = Arc::new(invites::RetryClock {
        time: a.clock.clone(),
        sleeps,
    });
    let files = file_owner(&a);
    let operations = operation_owner(a.sync, files);
    // Consume the initial retry before reconnecting storage; only the sync call
    // below may encounter the missing snapshot.
    tokio::time::timeout(
        Duration::from_secs(5),
        sleeping.wait_for(|count| *count == 2),
    )
    .await
    .expect("operation worker did not schedule its injected retry")
    .unwrap();
    operations.reset_store().await.unwrap();
    let snapshot = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .remove(0)
        .path;
    storage.set_online(false);
    let mut waiting = Box::pin(operations.reload_from_snapshot());
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(waiting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    operations.get_members().await.unwrap();
    let retained = a.db.operations().await.unwrap();
    assert_eq!(retained.len(), 1);
    assert!(matches!(
        a.db.operation_pending::<PendingRecord>(retained[0].id)
            .await
            .unwrap()
            .unwrap()
            .reason,
        PendingReason::Storage {
            failure: StorageFailure::Network,
            ..
        }
    ));
    assert!(
        matches!(waiting.await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network)
    );
    storage.set_online(true);
    storage.delete(&snapshot).await.unwrap();
    let error = operations.sync().await.unwrap_err();
    assert!(
        matches!(&error, SyncError::Storage(error) if error.failure() == StorageFailure::NotFound),
        "{error:?}"
    );
    let conditions = pending(&operations, &a.db).await;
    assert_eq!(conditions.len(), 1);
    assert_eq!(pending_id(&conditions[0]), retained[0].id);
    assert_eq!(
        pending_kind(&conditions[0]),
        OperationKind::ReloadFromSnapshot
    );
    operations.close().await.unwrap();
}

#[tokio::test]
async fn update_gates_survive_reopen_and_explicit_retry_cannot_bypass_them() {
    for reason in [
        PendingReason::Refused(Refusal::Signature { cause: None }),
        PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 1 }),
        PendingReason::UpdateRequired(RequiredUpdate::CovenFormat {
            version: coven_format::FORMAT_VERSION + 1,
        }),
    ] {
        let storage = storage();
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        a.create(key(1)).await;
        let id = begin(&mut a, Command::CreateCircle("waiting".into())).await;
        let record = a.operation(id).await;
        a.sync
            .record_operation_condition(&record, reason.clone())
            .await
            .unwrap();
        let expected = a.db.pending::<PendingRecord>().await.unwrap();
        let reads = storage.reads().await;
        for _ in 0..2 {
            assert!(!a.sync.operation_can_retry(&record).await.unwrap());
            assert!(
                matches!(a.sync.begin_operation_call(Command::Retry(id)).await,
                Err(SyncError::Pending(pending)) if pending.reason == reason)
            );
            assert_eq!(storage.reads().await, reads);
            a.restart(storage.clone()).await;
            assert_eq!(a.db.pending::<PendingRecord>().await.unwrap(), expected);
        }
        match reason {
            PendingReason::Refused(_) => {
                a.db.test_pending_version("previous-version").await.unwrap();
                a.restart(storage.clone()).await;
                assert!(a.sync.operation_can_retry(&record).await.unwrap());
                // A failed retry under the installed version closes the gate again.
                a.sync
                    .record_operation_condition(&record, reason)
                    .await
                    .unwrap();
                assert!(!a.sync.operation_can_retry(&record).await.unwrap());
            }
            PendingReason::UpdateRequired(RequiredUpdate::AppSchema { .. }) => {
                a.reopen(
                    storage.clone(),
                    vec![],
                    vec![coven_database::Migration::sql(
                        1,
                        "local",
                        "CREATE TABLE local(value TEXT)",
                    )],
                )
                .await;
                assert!(a.sync.operation_can_retry(&record).await.unwrap());
                finish(&mut a, id).await;
                assert!(a.db.pending::<PendingRecord>().await.unwrap().is_empty());
            }
            PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { .. }) => {
                a.db.test_pending_version("previous-version").await.unwrap();
                assert!(!a.sync.operation_can_retry(&record).await.unwrap());
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn persisted_refusals_keep_the_category_without_serializing_the_native_cause() {
    use std::error::Error;
    let storage = storage();
    let a = device(storage, 1, member(1), store(1)).await;
    let native = Arc::new(coven_crypto::CryptoError::Signature);
    let refusal = Refusal::Signature {
        cause: Some(native.clone()),
    };
    let record = PendingRecord {
        subject: PendingSubject::Connection,
        reason: PendingReason::Refused(refusal.clone()),
        reported_by: a.device().await,
    };
    a.db.record_pending(record.clone()).await.unwrap();
    assert!(std::ptr::eq(
        refusal
            .source()
            .unwrap()
            .downcast_ref::<coven_crypto::CryptoError>()
            .unwrap(),
        native.as_ref()
    ));
    let saved = a.db.pending::<PendingRecord>().await.unwrap().remove(0);
    assert_eq!(record, saved);
    let PendingReason::Refused(saved) = saved.reason else {
        panic!()
    };
    assert!(saved.source().is_none());
}

#[tokio::test]
async fn pending_queries_round_trip_every_subject_and_reason() {
    use coven_foundation::id_source::{FileId, InviteId};
    use coven_storage::{CloudProvider, RetainedAccess, RetainedAccessReason};
    let storage = storage();
    let mut a = device(storage, 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let id = begin(&mut a, Command::CreateCircle("pending".into())).await;
    let own = a.device().await;
    let entry = coven_format::value::EntryId {
        device: own,
        number: 2,
    };
    let path = crate::store_log_object::path(store(1), entry);
    let subjects = vec![
        PendingSubject::Write(coven_merge::WriteId {
            device: own,
            number: 1,
        }),
        PendingSubject::Entry(entry),
        PendingSubject::KeyCopy {
            audience: coven_merge::Audience::Store,
            key: key(1),
            member: a.member.member_id(),
        },
        PendingSubject::Snapshot(coven_format::store_log::SnapshotId {
            audience: coven_merge::Audience::Store,
            device: own,
            number: 1,
        }),
        PendingSubject::Positions(own),
        PendingSubject::File {
            device: own,
            file: FileId(key(2).0),
        },
        PendingSubject::Operation {
            id,
            kind: OperationKind::CreateCircle,
        },
        PendingSubject::Retention { path: path.clone() },
        PendingSubject::Audience(coven_merge::Audience::Store),
        PendingSubject::Agreement {
            audience: coven_merge::Audience::Store,
            peer: DeviceId(2),
        },
        PendingSubject::JoinRequest(InviteId(key(3).0)),
        PendingSubject::Connection,
    ];
    for subject in subjects.iter().rev() {
        a.db.record_pending(PendingRecord {
            subject: subject.clone(),
            reason: PendingReason::Missing { path: path.clone() },
            reported_by: own,
        })
        .await
        .unwrap();
    }
    assert_eq!(
        a.db.pending::<PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.subject)
            .collect::<Vec<_>>(),
        subjects
    );
    let mut reasons = vec![
        PendingReason::KeyUnavailable {
            audience: coven_merge::Audience::Store,
            key: key(1),
        },
        PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: 2 }),
        PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { version: 2 }),
        PendingReason::NoStorage,
        PendingReason::Storage {
            provider: CloudProvider::GoogleDrive,
            failure: StorageFailure::Network,
        },
        PendingReason::Refused(Refusal::Signature { cause: None }),
        PendingReason::InvalidPositions(Refusal::Parse { cause: None }),
        PendingReason::Dropped(coven_database::DropReason::BeatenBy(entry)),
        PendingReason::ProviderPending {
            action: ProviderAccessAction::Grant,
            account: "member@example.com".into(),
        },
        PendingReason::PendingOwner {
            account: "owner@example.com".into(),
        },
        PendingReason::AccountInUse {
            account: "shared@example.com".into(),
        },
        PendingReason::AccessRemains {
            account: "member@example.com".into(),
            shares: vec![RetainedAccess {
                provider_id: "grant-id".into(),
                reason: RetainedAccessReason::Inherited,
            }],
        },
        PendingReason::DeleteAccessKey(AccessKeyToDelete {
            access_key_id: "public-key-id".into(),
            principal: coven_format::S3Principal::GcsServiceAccount {
                project: "project".into(),
                email: "member@project.iam.gserviceaccount.com".into(),
                managed_folder: "base/store/".into(),
            },
            member: Some(a.member.member_id()),
        }),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Changed)),
        PendingReason::Disagrees,
        PendingReason::Failed(LocalFailure::Database),
        PendingReason::Paused,
        PendingReason::Removed,
    ];
    reasons.extend(
        [
            Prerequisite::Object(path),
            Prerequisite::DeviceRegistration(own),
            Prerequisite::SchemaPublication {
                audience: coven_merge::Audience::Store,
                version: 2,
            },
            Prerequisite::Reload(coven_merge::Audience::Store),
            Prerequisite::Positions(own),
            Prerequisite::EntryFinality(entry),
        ]
        .into_iter()
        .map(PendingReason::Waits),
    );
    for reason in reasons {
        let record = PendingRecord {
            subject: PendingSubject::Connection,
            reason,
            reported_by: own,
        };
        a.db.record_pending(record.clone()).await.unwrap();
        assert_eq!(
            a.db.pending::<PendingRecord>().await.unwrap().last(),
            Some(&record)
        );
    }
}

// Operation ids are fresh UUIDs in Rust and naturals in Lean. Each history
// maps its one real operation to its case index; reporter ids remain exact.
async fn observed(
    operations: &Operations,
    db: &coven_database::Database,
    storage: &MemoryStorage,
    account: &str,
    operation: Option<OperationId>,
    index: usize,
) -> Value {
    let mut records = Vec::new();
    for record in pending(operations, db).await {
        assert_eq!(Some(pending_id(&record)), operation);
        let PendingReason::AccessRemains { account, .. } = record.reason else {
            panic!("unexpected pending record: {record:?}");
        };
        records.push(json!({
            "operation": index,
            "reporter": record.reported_by.0,
            "account": account,
            "reason": "accessRemains",
        }));
    }
    let recipient = MemoryStorage::for_recipient(storage, account).unwrap();
    let actual = match recipient.list(&ObjectPrefix::all(store(1))).await {
        Ok(_) => true,
        Err(error) if error.failure() == StorageFailure::PermissionDenied => false,
        Err(error) => panic!("unexpected provider result: {error}"),
    };
    json!({"actual": actual, "pending": records})
}

#[tokio::test]
#[ignore = "requires COVEN_ACCESS_LEAN; scripts/check.sh supplies it"]
async fn lean_differential_access_pending() {
    let mut inputs = Vec::new();
    let mut rust = Vec::new();
    for remote in [false, true] {
        for failures in 0..=3 {
            let storage = google();
            let [mut a, mut b, c] = accounts(storage.clone()).await;
            let account = "cat@example.com";
            let reporter = a.device().await;
            let shares = vec![coven_storage::RetainedAccess {
                provider_id: "parent-permission".into(),
                reason: coven_storage::RetainedAccessReason::Inherited,
            }];
            if failures > 0 {
                storage.set_retained_access(account, shares.clone()).await;
            }
            if remote {
                let id = begin(&mut b, Command::RemoveMember(c.member.member_id())).await;
                finish(&mut b, id).await;
                a.sync().await;
            }
            let files = file_owner(&a);
            let operations = operation_owner(a.sync, files);
            if !remote {
                let result = operations
                    .remove_member(&c.member.member_id())
                    .await
                    .unwrap();
                assert_eq!(
                    result,
                    if failures > 0 {
                        MemberRemoval::AccessRemains {
                            shares: shares.clone(),
                        }
                    } else {
                        MemberRemoval::Revoked
                    }
                );
            }
            let conditions = pending(&operations, &a.db).await;
            assert_eq!(conditions.len(), usize::from(failures > 0));
            let operation = conditions.first().map(pending_id);
            let index = inputs.len();
            let mut results =
                vec![observed(&operations, &a.db, &storage, account, operation, index).await];
            for _ in 1..failures {
                assert!(matches!(
                    operations.retry_pending_operation(operation.unwrap()).await,
                    Err(SyncError::Pending(found)) if matches!(&found.reason, PendingReason::AccessRemains { shares: found, .. } if *found == shares)
                ));
                results
                    .push(observed(&operations, &a.db, &storage, account, operation, index).await);
            }
            if let Some(id) = operation {
                storage.set_retained_access(account, Vec::new()).await;
                operations.retry_pending_operation(id).await.unwrap();
                results
                    .push(observed(&operations, &a.db, &storage, account, operation, index).await);
            }
            assert!(a.db.operations().await.unwrap().is_empty());
            let retained: Vec<_> = std::iter::repeat_n(true, failures)
                .chain(std::iter::once(false))
                .collect();
            inputs.push(json!({
                "account": account, "reporter": reporter.0,
                "operation": index, "retained": retained,
            }));
            rust.push(results);
            operations.close().await.unwrap();
        }
    }
    let output = crate::tests::run_lean("COVEN_ACCESS_LEAN", &json!(inputs));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Vec<Value>> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(lean, rust);
}

#[test]
#[ignore = "requires COVEN_ACCESS_LEAN; scripts/check.sh supplies it"]
fn lean_differential_access_rejects_invalid_inputs() {
    for retained in [json!([]), json!([false, true]), json!(["retained"])] {
        let output = crate::tests::run_lean(
            "COVEN_ACCESS_LEAN",
            &json!([{
                "account": "cat@example.com", "reporter": 1,
                "operation": 0, "retained": retained,
            }]),
        );
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
    }
}

#[tokio::test]
async fn missing_original_source_is_reported_as_a_write_object_prerequisite() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    let id =
        a.db.start_operation(StoreLogSync::migration_operation(2).unwrap())
            .await
            .unwrap();
    let record = a.db.operation(id).await.unwrap().unwrap();
    let source = coven_format::write_sources::ChangeId {
        write: coven_merge::WriteId {
            device: DeviceId(77),
            number: 3,
        },
        row: coven_merge::RowId {
            table: "notes".into(),
            key: coven_format::key::encode_key(&[coven_format::value::Value::Text("n".into())])
                .unwrap(),
            audience: Audience::Store,
        },
    };
    a.sync
        .record_operation_error(
            &record,
            &SyncError::Database(coven_database::DbError::Snapshot(
                coven_database::SnapshotError::WriteWaiting(coven_database::WriteWait::Sources(
                    vec![source.clone()],
                )),
            )),
        )
        .await
        .unwrap();
    let expected = PendingReason::Waits(Prerequisite::Object(crate::write_seal::path(
        store(1),
        source.write,
    )));
    assert_eq!(
        a.db.operation_pending::<PendingRecord>(id)
            .await
            .unwrap()
            .unwrap()
            .reason,
        expected
    );
    a.restart(storage).await;
    assert_eq!(
        a.db.operation_pending::<PendingRecord>(id)
            .await
            .unwrap()
            .unwrap()
            .reason,
        expected
    );
}
