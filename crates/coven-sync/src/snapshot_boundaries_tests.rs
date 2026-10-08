use super::*;

#[tokio::test]
async fn unusable_snapshots_do_not_turn_deleted_history_into_an_empty_audience() {
    for (deleted, damaged_prefix) in [(false, false), (true, false), (false, true), (true, true)] {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        write_rows(&a, 0, 1, 17, Audience::Store).await;
        upload(&a, &storage).await;
        if deleted {
            a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
        }
        a.sync.write_snapshot(Audience::Store).await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs())
                .await
                .unwrap()
                .is_empty(),
            deleted
        );
        let object = storage
            .list(&ObjectPrefix::snapshots())
            .await
            .unwrap()
            .remove(0);
        let mut bytes = storage.read(&object.path).await.unwrap();
        if damaged_prefix {
            bytes[0] = 255;
        } else {
            *bytes.last_mut().unwrap() ^= 1;
        }
        storage.delete(&object.path).await.unwrap();
        storage.create(&object.path, &bytes).await.unwrap();
        let mut b = notes_device(storage.clone(), 2).await;
        add_device(&mut b).await;
        write_rows(&b, 99, 1, 17, Audience::Store).await;
        let before = tables(&b).await;
        let result = b.sync.reload_from_snapshots().await;
        if deleted {
            if damaged_prefix {
                assert!(matches!(
                    result,
                    Err(SyncError::Database(coven_database::DbError::Snapshot(
                        coven_database::SnapshotError::Inconsistent(_)
                    )))
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(SyncError::Database(coven_database::DbError::Snapshot(
                        coven_database::SnapshotError::MissingWrites { .. }
                    )))
                ));
            }
            assert_eq!(tables(&b).await, before);
        } else {
            assert_eq!(result.unwrap().len(), 1);
            let mut expected = tables(&a).await;
            expected.extend(before);
            assert_eq!(tables(&b).await, expected);
        }
    }
}

#[tokio::test]
async fn a_removed_device_cannot_start_or_resume_snapshot_publication() {
    for recorded_step in [None, Some(0), Some(1)] {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = notes_device(storage.clone(), 2).await;
        add_device(&mut b).await;
        a.sync().await;
        write_rows(&a, 0, 1, 17, Audience::Store).await;
        if let Some(step) = recorded_step {
            let data = Data::Snapshots(SnapshotTask {
                job: SnapshotJob::Write {
                    audience: Audience::Store,
                    device: a.device().await,
                    trigger: crate::snapshot_data::SnapshotTrigger::Requested,
                    session: None,
                },
                temporary: Vec::new(),
            });
            let id =
                a.db.start_operation(data.new_operation("coven").unwrap())
                    .await
                    .unwrap();
            for _ in 0..step {
                let record =
                    a.db.operations()
                        .await
                        .unwrap()
                        .into_iter()
                        .find(|r| r.id == id)
                        .unwrap();
                a.sync
                    .operation_step(&record, Data::read(&record).unwrap())
                    .await
                    .unwrap();
            }
        }
        b.sync
            .make_and_upload_entry(StoreChange::RemoveDevice {
                device: a.device().await,
            })
            .await
            .unwrap();
        assert!(matches!(
            a.sync.sync_store_log().await,
            Err(SyncFailure::Removed)
        ));
        let result = if recorded_step.is_some() {
            a.sync.write_snapshots().await.map(|_| ())
        } else {
            a.sync.write_snapshot(Audience::Store).await.map(|_| ())
        };
        assert!(matches!(
            result,
            Err(SyncError::Stopped(SyncFailure::Removed))
        ));
        assert!(
            storage
                .list(&ObjectPrefix::snapshots())
                .await
                .unwrap()
                .is_empty(),
            "{recorded_step:?}"
        );
    }
}

#[tokio::test]
async fn reload_retries_with_the_stored_copy_when_a_waiting_write_finishes_uploading() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    let expected = tables(&a).await;
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Reload {
            scope: crate::snapshot_data::ReloadScope::All,
            files: None,
        },
        temporary: Vec::new(),
    });
    let id =
        a.db.start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    let record =
        a.db.operations()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
    a.sync.operation_step(&record, data).await.unwrap();
    upload(&a, &storage).await;
    let record =
        a.db.operations()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
    assert!(matches!(
        a.sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await,
        Err(SyncError::Database(coven_database::DbError::Snapshot(
            coven_database::SnapshotError::MissingWrites { .. }
        )))
    ));
    assert_eq!(tables(&a).await, expected);
    let record =
        a.db.operations()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
    assert_eq!(record.last_step, 0);
    a.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(tables(&a).await, expected);
    assert!(a.db.operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn queued_entries_wait_for_reload_and_finishing_a_queued_reset_blocks_new_entries() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let reset = a.sync.write_snapshot(Audience::Store).await.unwrap();
    storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        a.sync
            .make_and_upload_entry(StoreChange::Reset { snapshot: reset })
            .await,
        Err(SyncError::Storage(_))
    ));
    let change = StoreChange::AddDevice {
        device: DeviceId(77),
        name: "waiting".into(),
    };
    assert!(matches!(
        a.sync.make_and_upload_entry(change.clone()).await,
        Err(SyncError::ReloadPending(_))
    ));
    assert!(!a
        .log()
        .await
        .replay
        .state
        .devices
        .contains_key(&DeviceId(77)));
    a.sync.sync_store_log().await.unwrap();
    storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        a.sync.make_and_upload_entry(change).await,
        Err(SyncError::Storage(_))
    ));
    let queued = a.db.local_store_log().await.unwrap().upload.unwrap();
    let expected = a.reseal(&queued);
    let reload = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Reload {
            scope: crate::snapshot_data::ReloadScope::All,
            files: None,
        },
        temporary: Vec::new(),
    });
    let pending =
        a.db.start_operation(reload.new_operation("coven").unwrap())
            .await
            .unwrap();
    a.db.operation_failure(pending, Some("reload failed".into()))
        .await
        .unwrap();
    a.sync.sync_store_log().await.unwrap();
    assert_eq!(
        storage
            .read(&object::path(queued.entry.position))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::NotFound
    );
    assert!(!a
        .db
        .operations()
        .await
        .unwrap()
        .iter()
        .any(|r| r.id == pending));
    a.sync.sync_store_log().await.unwrap();
    assert_eq!(
        storage
            .read(&object::path(queued.entry.position))
            .await
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn a_failed_internal_reload_prevents_snapshot_publication_until_it_can_resume() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let reset = a.sync.write_snapshot(Audience::Store).await.unwrap();
    a.sync
        .make_and_upload_entry(StoreChange::Reset { snapshot: reset })
        .await
        .unwrap();
    let pending = a.db.operations().await.unwrap().remove(0).id;
    let snapshot = storage
        .list(&ObjectPrefix::snapshots())
        .await
        .unwrap()
        .remove(0)
        .path;
    let bytes = storage.read(&snapshot).await.unwrap();
    storage.delete(&snapshot).await.unwrap();
    for _ in 0..2 {
        let error = a.sync.write_snapshot(Audience::Store).await.unwrap_err();
        assert!(
            matches!(&error, SyncError::Storage(error) if error.failure() == StorageFailure::NotFound),
            "{error:?}"
        );
        assert!(storage
            .list(&ObjectPrefix::snapshots())
            .await
            .unwrap()
            .is_empty());
        assert!(a
            .db
            .operations()
            .await
            .unwrap()
            .iter()
            .any(|r| r.id == pending && r.failure.is_some()));
    }
    storage.create(&snapshot, &bytes).await.unwrap();
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    assert!(a.db.operations().await.unwrap().is_empty());
}

#[tokio::test]
async fn repeating_an_applied_reset_does_not_reject_snapshots_written_since_that_reset() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let reset = a.sync.write_snapshot(Audience::Store).await.unwrap();
    a.sync
        .make_and_upload_entry(StoreChange::Reset {
            snapshot: reset.clone(),
        })
        .await
        .unwrap();
    a.sync.sync_store_log().await.unwrap();
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let latest = a.sync.write_snapshot(Audience::Store).await.unwrap();
    a.sync
        .make_and_upload_entry(StoreChange::Reset { snapshot: reset })
        .await
        .unwrap();
    assert_eq!(selected_snapshot(&mut a).await.snapshot_id(), Some(latest));
}

mod reset {
    use super::*;
    use crate::operations::{Begun, Command, Output};
    use crate::OperationId;

    async fn begin_reset(d: &mut Device, audience: Audience) -> OperationId {
        let Begun::Operation(id) = d
            .sync
            .begin_operation_call(Command::Reset(audience))
            .await
            .unwrap()
        else {
            panic!("reset must be journaled")
        };
        id
    }

    async fn reset_step(d: &mut Device, id: OperationId) -> Progress {
        let record =
            d.db.operations()
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.id == id)
                .unwrap();
        d.sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await
            .unwrap()
    }

    async fn finish_reset(d: &mut Device, id: OperationId) {
        for _ in 0..20 {
            match reset_step(d, id).await {
                Progress::Finished(Output::Unit) => return,
                Progress::Advanced => (),
                Progress::Waiting => {
                    d.sync.sync_store_log().await.unwrap();
                }
                _ => panic!("unexpected reset result"),
            }
        }
        panic!("reset did not settle")
    }

    #[tokio::test]
    async fn reset_reloads_author_and_peers_loses_dependent_branch_and_merges_independent_late_write(
    ) {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = notes_device(storage.clone(), 2).await;
        let mut c = notes_device(storage.clone(), 3).await;
        add_device(&mut b).await;
        add_device(&mut c).await;
        a.sync().await;
        b.sync().await;
        // The reset keeps A's branch. B and C have not read it.
        write_rows(&a, 1, 1, 7, Audience::Store).await;
        write_rows(&b, 2, 1, 7, Audience::Store).await;
        let omitted = b.db.test_queued_writes().await.unwrap().remove(0);
        c.db.apply_downloaded(omitted.into()).await.unwrap();
        write_rows(&c, 3, 1, 7, Audience::Store).await;
        upload(&a, &storage).await;
        let reset = begin_reset(&mut a, Audience::Store).await;
        // Seal the reset snapshot, then let the resetting device read the rejected branch.
        reset_step(&mut a, reset).await;
        for d in [&b, &c] {
            for write in d.db.test_queued_writes().await.unwrap() {
                a.db.apply_downloaded(write.into()).await.unwrap();
            }
            upload(d, &storage).await;
        }
        finish_reset(&mut a, reset).await;
        b.sync().await;
        c.sync().await;
        // B's independent first write is late; C's write depended on B but did not
        // read A, so it is lost. This is §7.1's causal boundary, not a time cutoff.
        let ids: Vec<_> = tables(&a).await.into_iter().map(|r| r.0).collect();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(tables(&a).await, tables(&b).await);
        assert_eq!(tables(&a).await, tables(&c).await);
        assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
        assert_eq!(fingerprint(&a).await, fingerprint(&c).await);
        assert!(a.db.operations().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn concurrent_reset_operations_keep_smaller_timestamp_without_reauthoring_loser() {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = notes_device(storage.clone(), 2).await;
        add_device(&mut b).await;
        a.sync().await;
        write_rows(&a, 1, 1, 7, Audience::Store).await;
        write_rows(&b, 2, 1, 7, Audience::Store).await;
        upload(&a, &storage).await;
        upload(&b, &storage).await;
        a.clock.set(UNIX_EPOCH + Duration::from_secs(10));
        b.clock.set(UNIX_EPOCH + Duration::from_secs(20));
        let first = begin_reset(&mut a, Audience::Store).await;
        let second = begin_reset(&mut b, Audience::Store).await;
        // Snapshot publication and entry preparation happen before either entry uploads.
        for (d, id) in [(&mut a, first), (&mut b, second)] {
            for _ in 0..4 {
                assert!(matches!(reset_step(d, id).await, Progress::Advanced));
            }
        }
        let winner = a.db.local_store_log().await.unwrap().upload.unwrap().entry;
        let loser = b.db.local_store_log().await.unwrap().upload.unwrap().entry;
        finish_reset(&mut b, second).await;
        finish_reset(&mut a, first).await;
        b.sync().await;
        assert_eq!(
            a.log().await.replay.state.resets,
            b.log().await.replay.state.resets
        );
        assert!(matches!(
            b.log().await.replay.entries[&winner.position],
            coven_database::EntryOutcome::Kept
        ));
        assert!(matches!(
            b.log().await.replay.entries[&loser.position],
            coven_database::EntryOutcome::Dropped(_)
        ));
        assert_eq!(
            a.log()
                .await
                .entries
                .iter()
                .filter(|e| matches!(e.entry.change, StoreChange::Reset { .. }))
                .count(),
            2
        );
        assert_eq!(tables(&a).await, tables(&b).await);
        assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
    }

    #[tokio::test]
    async fn reset_resumes_every_durable_step_without_replacing_its_snapshot_or_entry() {
        for steps in 0..=7 {
            let storage = snapshot_storage();
            let mut a = notes_device(storage.clone(), 1).await;
            a.create(key(1)).await;
            write_rows(&a, 1, 1, 7, Audience::Store).await;
            upload(&a, &storage).await;
            let id = begin_reset(&mut a, Audience::Store).await;
            for _ in 0..steps {
                assert!(matches!(reset_step(&mut a, id).await, Progress::Advanced));
            }
            let mut published = Vec::new();
            for object in storage.list(&ObjectPrefix::all()).await.unwrap() {
                if object.path.snapshot_id().is_some() || object.path.store_log_position().is_some()
                {
                    published.push((
                        object.path.clone(),
                        storage.read(&object.path).await.unwrap(),
                    ));
                }
            }
            a.db.close().await.unwrap();
            a.db = open_notes(a.directory.clone(), a.clock.clone()).await;
            a.sync.database = a.db.clone();
            finish_reset(&mut a, id).await;
            for (path, bytes) in published {
                assert_eq!(storage.read(&path).await.unwrap(), bytes);
            }
            assert_eq!(
                a.log()
                    .await
                    .entries
                    .iter()
                    .filter(|e| matches!(e.entry.change, StoreChange::Reset { .. }))
                    .count(),
                1
            );
            let mut b = notes_device(storage.clone(), 2).await;
            add_device(&mut b).await;
            assert_eq!(tables(&a).await, tables(&b).await);
            assert_eq!(fingerprint(&a).await, fingerprint(&b).await);
        }
    }
}
