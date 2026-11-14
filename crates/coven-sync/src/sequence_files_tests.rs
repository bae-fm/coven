use crate::sequence_sync::tests::*;
use coven_database::DbError;
use coven_format::admin::StoreChange;
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::files::FileLease;
use coven_storage::{ObjectPath, Storage};

/// Ana's store on S3 with Ben admitted, then removed: his key still works.
async fn removed_fixture() -> (Group, Device, Device) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    (group, ana, ben)
}

/// One listing of `uploader`'s folder the way a pass or the app's cleanup
/// takes it, completing or failing.
async fn scan(
    ana: &Device,
    uploader: coven_foundation::id_source::DeviceId,
    listing: super::Listing,
    completes: bool,
) {
    use coven_storage::{test_utils::Faults, StorageFailure};
    let history = ana.sync.session.lock().await.history.clone();
    let scans = ana.sync.uploader_scans(&history, &[uploader].into(), listing).await.unwrap();
    let attempted = !scans.is_empty();
    ana.storage
        .set_faults(Faults {
            fail_next: usize::from(!completes),
            failure: StorageFailure::Network,
            ..Faults::none()
        })
        .await;
    let result = ana.sync.scan_uploaders(scans).await;
    assert_eq!(result.is_ok(), !attempted || completes, "{result:?}");
    ana.storage.set_faults(Faults::none()).await;
}

async fn scan_paths(
    ana: &Device,
    uploader: coven_foundation::id_source::DeviceId,
) -> serde_json::Value {
    match ana.database().file_scan(uploader).await.unwrap() {
        None => serde_json::Value::Null,
        Some(_) => serde_json::json!(ana
            .database()
            .file_candidates([uploader].into())
            .await
            .unwrap()
            .into_iter()
            .map(|candidate| u64::try_from(candidate.object.0.as_u128()).unwrap())
            .collect::<Vec<_>>()),
    }
}

/// A removed S3 member's folder is listed once when the removal applies,
/// with no confirmation that their key was deleted, and a later pass doesn't
/// list it again (§17.10).
#[tokio::test]
async fn a_removed_uploader_is_listed_once_at_its_removal() {
    let (group, ana, ben) = removed_fixture().await;
    let prefix = coven_storage::ObjectPrefix::device_files(group.store, ben.sync.device);
    let listings = || {
        ana.storage
            .request_trace()
            .iter()
            .filter(|request| {
                matches!(request,
                coven_storage::test_utils::MemoryRequest::List(listed) if *listed == prefix)
            })
            .count()
    };
    ana.sync.pass().await.unwrap();
    assert!(ana.database().file_scan(ben.sync.device).await.unwrap().is_some());
    assert_eq!(listings(), 1);
    ana.sync.pass().await.unwrap();
    assert_eq!(listings(), 1);
    ana.sync.clean_up_gone_devices_files().await.unwrap();
    assert_eq!(listings(), 2);
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_file_scan() {
    use serde_json::json;
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for events in [
        vec![
            json!({"pass":false}),
            json!({"pass":true}),
            json!({"upload":11}),
            json!({"pass":true}),
            json!({"clean_up":false}),
            json!({"clean_up":true}),
        ],
        vec![
            json!({"upload":11}),
            json!({"upload":24}),
            json!({"clean_up":true}),
            json!({"pass":true}),
            json!({"upload":39}),
            json!({"pass":false}),
            json!({"clean_up":true}),
        ],
    ] {
        let (group, ana, ben) = removed_fixture().await;
        let mut observed = Vec::new();
        for event in &events {
            if let Some(file) = event.get("upload") {
                group
                    .storage
                    .create(
                        &ObjectPath::file(
                            group.store,
                            ben.sync.device,
                            coven_foundation::id_source::ObjectId(uuid::Uuid::from_u128(
                                file.as_u64().unwrap().into(),
                            )),
                        ),
                        b"file",
                    )
                    .await
                    .unwrap();
            } else if let Some(completes) = event.get("pass") {
                let completes = completes.as_bool().unwrap();
                scan(&ana, ben.sync.device, super::Listing::AtStop, completes).await;
            } else {
                let completes = event["clean_up"].as_bool().unwrap();
                scan(&ana, ben.sync.device, super::Listing::Every, completes).await;
            }
            observed.push(scan_paths(&ana, ben.sync.device).await);
        }
        inputs.push(json!({"check":"file_scan","events":events}));
        actual.push(json!(observed));
        ana.database().close().await.unwrap();
        ben.database().close().await.unwrap();
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}

/// One device for each of `seeds`, whose app keeps notes and a table of
/// recordings that reference user files.
async fn file_devices(group: &Group, seeds: &[u8]) -> Vec<Device> {
    use coven_database::{
        DatabaseBuilder, FileColumn, Migration, Namespace, Provenance, RowIdentity, SyncedTable,
    };
    use coven_foundation::id_source::{IdSource, MemberId};
    let mut devices = Vec::new();
    for seed in seeds {
        let (temp, directory) = group.directory();
        let database = DatabaseBuilder::new(directory)
            .synced_tables(vec![
                SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("files", RowIdentity::SharedKey)
                    .file_column(FileColumn::new("file")),
            ])
            .namespaces(vec![Namespace::new("files", Provenance::UserProvided, 1 << 20)])
            .migrations(vec![Migration::addition(1, "files", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT)")])
            .open().await.unwrap();
        let identity = coven_crypto::MemberIdentity::new(
            MemberId(group.ids.new_id()),
            coven_format::test_utils::keys_from_seed(*seed),
        );
        devices.push(
            group
                .attach(
                    temp,
                    identity,
                    Some(group.ring.clone()),
                    group.storage.for_device(),
                    database,
                )
                .await,
        );
    }
    devices
}

/// Ana's original finished uploading, and she pins the stored file, which
/// keeps it whole from storage (§17.8). A held download of it takes her one
/// file download, and never holds her sequence passes: their reads don't
/// count against the download limit (§21.1), so the download goes on
/// untouched while passes apply Ben's writes.
#[tokio::test]
async fn stalled_pin_downloads_leave_sequence_passes_free() {
    use coven_database::{FilePlace, PinProgress};
    let group = Group::new();
    let devices = file_devices(&group, &[1, 2]).await;
    let ana = &devices[0];
    let ben = &devices[1];
    group.create(ana).await;
    group.admit(ana, ben).await;
    let originals = tempfile::tempdir().unwrap();
    let path = originals.path().join("recording");
    std::fs::write(&path, b"sound").unwrap();
    let prepared = coven_database::prepare_user_file(&path, &FileLease::for_test().await, |_| {})
        .await
        .unwrap();
    let staged = ana
        .database()
        .write_as::<_, _, coven_database::DbError>(move |sql| {
            let file = sql.put_user_file("files", prepared, coven_database::Place::Cloud)?;
            sql.execute("INSERT INTO files(id,file) VALUES('recording',?1)", [file])?;
            Ok(file)
        })
        .await
        .unwrap();
    ana.files().retry_uploads_now().await.unwrap();
    let file = ana.database().file_ref(staged).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the file");
    };
    // Whether every byte of the file is kept on Ana's device.
    let kept = || async {
        let PinProgress { bytes_cached, bytes_total, .. } =
            ana.database().pin_progress(vec![file.clone()]).await.unwrap();
        bytes_cached == bytes_total
    };
    let object = location.path(group.store);
    // Hold the next response reaching the file's only chunk, whether or not
    // it carries the header; the receiver reports when it starts, the sender
    // releases it.
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group.storage.hold_body_at(object.clone(), 12, started, held).await;
    // Ana's pin starts keeping the stored file whole, and its response is
    // held.
    let pinning = tokio::spawn({
        let files = ana.files().clone();
        let file = file.clone();
        async move { files.pin(std::slice::from_ref(&file)).await }
    });
    tokio::time::timeout(HANG_GUARD, observed).await.unwrap().unwrap();
    // The pass completes while the file's one download waits.
    let pass = tokio::time::timeout(HANG_GUARD, ana.sync.pass())
        .await
        .expect("a held file must not hold a sequence pass");
    assert!(matches!(pass.unwrap(), crate::SequenceStatus::Synced { .. }));
    assert!(!resume.is_closed(), "the pass left the download in flight");
    assert!(!kept().await);
    let applied = ana.database().applied_slot().await.unwrap();
    insert(ben.database(), "later", "arrived while the file waited").await;
    ben.sync.pass().await.unwrap();
    let pass = tokio::time::timeout(HANG_GUARD, ana.sync.pass())
        .await
        .expect("later slots must apply while the file waits")
        .unwrap();
    assert!(matches!(pass, crate::SequenceStatus::Synced { .. }));
    assert!(ana.database().applied_slot().await.unwrap() > applied);
    assert_eq!(
        rows(ana.database()).await,
        [("later".into(), "arrived while the file waited".into())]
    );
    assert!(!resume.is_closed(), "the passes left the download in flight");
    assert!(!kept().await);
    resume.send(()).unwrap();
    tokio::time::timeout(HANG_GUARD, pinning).await.unwrap().unwrap().unwrap();
    assert!(kept().await);
    assert_eq!(ana.files().read_file(&file, &FileLease::for_test().await).await.unwrap(), b"sound");
    for device in devices {
        device.files().close().await;
        device.database().close().await.unwrap();
    }
}

/// A pass never waits for a file: with a trailing file's upload held, the
/// pass still sends the row's writes. Once the upload is stored, a second
/// local write switches the record to the stored location, a later pass
/// sends it, and the app's row is unchanged (§6.6, §17.3).
#[tokio::test]
async fn a_pass_sends_its_writes_while_their_file_upload_is_held() {
    use coven_database::{
        DatabaseBuilder, FileColumn, FilePlace, Migration, Namespace, Place, Provenance,
        RowIdentity, SyncedTable,
    };
    use coven_foundation::id_source::{IdSource, MemberId};
    let group = Group::new();
    let (temp, directory) = group.directory();
    let database = DatabaseBuilder::new(directory)
        .synced_tables(vec![
            SyncedTable::new("photos", RowIdentity::SharedKey).file_column(FileColumn::new("file"))
        ])
        .namespaces(vec![Namespace::new("photos", Provenance::AppProvided, 1 << 20)])
        .migrations(vec![Migration::addition(
            1,
            "photos",
            "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
        )])
        .open()
        .await
        .unwrap();
    let identity = coven_crypto::MemberIdentity::new(
        MemberId(group.ids.new_id()),
        coven_format::test_utils::keys_from_seed(1),
    );
    let ana = group
        .attach(temp, identity, Some(group.ring.clone()), group.storage.for_device(), database)
        .await;
    group.create(&ana).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    ana.storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::device_files(group.store, ana.sync.device),
            started,
            held,
        )
        .await;
    let photo = ana
        .database()
        .staged_write::<_, _, _, _, coven_database::DbError>(
            |batch| Ok(batch.put_file("photos", b"photo".to_vec(), Place::Cloud)),
            |sql, file| {
                sql.execute("INSERT INTO photos(id,file) VALUES('1',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    observed.await.unwrap();
    let row = |ana: &Device| {
        let database = ana.database().clone();
        async move {
            database
                .read(|sql| {
                    Ok::<_, DbError>(sql.query_row("SELECT id,file FROM photos", [], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                    })?)
                })
                .await
                .unwrap()
        }
    };
    let before = row(&ana).await;
    let slot = ana.database().applied_slot().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.database().applied_slot().await.unwrap(),
        slot + 1,
        "the writes slot is claimed"
    );
    assert!(matches!(
        ana.database().file_ref(photo).await.unwrap().place(),
        FilePlace::DeviceOnly { .. }
    ));

    resume.send(()).unwrap();
    ana.files().retry_uploads_now().await.unwrap();
    assert!(matches!(
        ana.database().file_ref(photo).await.unwrap().place(),
        FilePlace::Cloud { .. }
    ));
    assert_eq!(row(&ana).await, before, "the completion leaves the app's row as it was");
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.database().applied_slot().await.unwrap(), slot + 2, "the switch is sent");
    assert!(ana.database().test_queued_writes().await.unwrap().is_empty());
    ana.files().close().await;
    ana.database().close().await.unwrap();
}

/// Ana stored a recording's file and then deleted its row; returns the
/// file's object path.
async fn stored_file_whose_row_was_deleted(group: &Group, ana: &Device) -> ObjectPath {
    use coven_database::FilePlace;
    let originals = tempfile::tempdir().unwrap();
    let path = originals.path().join("recording");
    std::fs::write(&path, b"sound").unwrap();
    let prepared = coven_database::prepare_user_file(&path, &FileLease::for_test().await, |_| {})
        .await
        .unwrap();
    let staged = ana
        .database()
        .write_as::<_, _, coven_database::DbError>(move |sql| {
            let file = sql.put_user_file("files", prepared, coven_database::Place::Cloud)?;
            sql.execute("INSERT INTO files(id,file) VALUES('recording',?1)", [file])?;
            Ok(file)
        })
        .await
        .unwrap();
    ana.files().retry_uploads_now().await.unwrap();
    let file = ana.database().file_ref(staged).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the file");
    };
    let object = location.path(group.store);
    ana.sync.pass().await.unwrap();
    ana.database()
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    object
}

/// Ana stored a recording's file and then deleted its row. Once a snapshot
/// that leaves the file out has passed grace and the floor has deleted the
/// slots that named it, nothing refers to the stored file: the next passes
/// would delete it. Returns the group, Ana and the file's object path.
async fn ana_with_an_unreferenced_stored_file() -> (Group, Device, ObjectPath) {
    let group = Group::new();
    let mut devices = file_devices(&group, &[1]).await;
    let ana = devices.remove(0);
    group.create(&ana).await;
    let object = stored_file_whose_row_was_deleted(&group, &ana).await;
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    (group, ana, object)
}

/// A provider that refuses the deletion keeps the file stored and the
/// refusal visible in the pending records; a later attempt with the
/// permission back deletes it and ends the record (§17.10).
#[tokio::test]
async fn a_refused_file_deletion_keeps_the_file_and_stays_visible() {
    use coven_storage::{test_utils::Faults, StorageFailure};
    let (group, ana, object) = ana_with_an_unreferenced_stored_file().await;
    ana.storage
        .set_faults(Faults {
            delete_failures: [(object.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    assert!(group.storage.status(&object).await.unwrap().is_some(), "the refused file stays");
    let operation = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "sequence-files")
        .expect("the refused deletion stays journaled")
        .id;
    let pending = ana
        .database()
        .operation_pending::<crate::PendingRecord>(operation)
        .await
        .unwrap()
        .expect("the refusal is reported");
    assert!(matches!(
        pending.reason,
        crate::PendingReason::Storage { failure: StorageFailure::PermissionDenied, .. }
    ));
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(group.storage.status(&object).await.unwrap().is_none(), "the file is deleted");
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    ana.files().close().await;
    ana.database().close().await.unwrap();
}

/// The snapshot a deletion decision reads changes on Ana's disk while she
/// holds it staged, before its check or as its staging checks its writes.
/// It is what she wrote, so it is not refused: the deletion fails here as
/// the local database's failure, and its retry deletes the file (§19.1).
#[tokio::test]
async fn a_snapshot_staged_for_file_deletion_damaged_here_is_not_refused() {
    for finishing in [false, true] {
        let (group, ana, object) = ana_with_an_unreferenced_stored_file().await;
        if finishing {
            ana.database()
                .damage_staged_finishes(coven_database::StagedDamage::Writes, 1)
                .await
                .unwrap();
        } else {
            ana.database()
                .damage_next_staged_read(coven_database::StagedDamage::Record)
                .await
                .unwrap();
        }
        ana.sync.pass().await.unwrap();
        assert!(group.storage.status(&object).await.unwrap().is_some(), "the file stays");
        let operation = ana
            .database()
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.kind == "sequence-files")
            .expect("the failed deletion stays journaled")
            .id;
        let pending = ana
            .database()
            .operation_pending::<crate::PendingRecord>(operation)
            .await
            .unwrap()
            .expect("the failure is reported");
        assert_eq!(
            pending.reason,
            crate::PendingReason::Failed(crate::LocalFailure::Database),
            "{finishing}: {pending:?}"
        );
        ana.sync.retry_pending_operation(operation).await.unwrap();
        assert!(group.storage.status(&object).await.unwrap().is_none(), "the file is deleted");
        assert!(ana.database().operation(operation).await.unwrap().is_none());
        ana.files().close().await;
        ana.database().close().await.unwrap();
    }
}

/// The same damage, and removing the staging the failed check ends fails
/// too. That removal says nothing about the snapshot's bytes, so the check
/// is judged as it would be alone: the snapshot is not refused on its one
/// staging, and the record keeps both local failures (§19.1).
#[tokio::test]
async fn a_snapshot_damaged_here_is_not_refused_when_removing_its_staging_fails() {
    let (group, ana, object) = ana_with_an_unreferenced_stored_file().await;
    ana.database().damage_next_staged_read(coven_database::StagedDamage::Record).await.unwrap();
    ana.database().fail_next_staging_removal().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&object).await.unwrap().is_some(), "the file stays");
    let operation = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "sequence-files")
        .expect("the failed deletion stays journaled")
        .id;
    let pending = ana
        .database()
        .operation_pending::<crate::PendingRecord>(operation)
        .await
        .unwrap()
        .expect("the failure is reported");
    let local = crate::PendingReason::Failed(crate::LocalFailure::Database);
    assert_eq!(
        pending.reason,
        crate::PendingReason::Cleanup {
            operation: Box::new(local.clone()),
            cleanup: Box::new(local)
        },
        "{pending:?}"
    );
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(group.storage.status(&object).await.unwrap().is_none(), "the file is deleted");
    ana.files().close().await;
    ana.database().close().await.unwrap();
}

/// The snapshots a deletion decision reads are the ones announcements and
/// the floor name, read by their paths: no pass lists a snapshot folder to
/// find them (§17.10).
#[tokio::test]
async fn file_deletion_reads_the_snapshots_the_floor_names_and_lists_no_snapshot_folder() {
    use coven_storage::test_utils::MemoryRequest;
    let (group, ana, object) = ana_with_an_unreferenced_stored_file().await;
    let snapshots = coven_storage::ObjectPrefix::snapshots(group.store);
    let announced = {
        let session = ana.sync.session.lock().await;
        let (_, announcement) = session.history.announcements.last_key_value().unwrap();
        crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap()
    };
    let before = ana.storage.request_trace().len();
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    assert!(group.storage.status(&object).await.unwrap().is_none(), "the file is deleted");
    let trace = ana.storage.request_trace().split_off(before);
    assert!(
        trace
            .iter()
            .any(|request| matches!(request, MemoryRequest::Read(path, _) if *path == announced)),
        "the announced snapshot is read: {trace:?}"
    );
    assert!(
        !trace.iter().any(|request| matches!(request, MemoryRequest::List(prefix)
            if prefix.as_str().starts_with(snapshots.as_str()))),
        "no snapshot folder is listed: {trace:?}"
    );
    ana.files().close().await;
    ana.database().close().await.unwrap();
}

/// Removing a member lists the files folder of each of their devices, not
/// only the first: every device's unreferenced object goes (§17.10).
#[tokio::test]
async fn removing_a_member_lists_the_folder_of_each_of_their_devices() {
    use coven_foundation::id_source::ObjectId;
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let laptop = group.laptop(&ben).await;
    ana.sync.pass().await.unwrap();
    let mut orphans = Vec::new();
    for (n, device) in [&ben, &laptop].into_iter().enumerate() {
        let orphan = ObjectPath::file(
            group.store,
            device.sync.device,
            ObjectId(uuid::Uuid::from_u128(n as u128 + 100)),
        );
        group.storage.create(&orphan, b"never named").await.unwrap();
        orphans.push(orphan);
    }
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    ana.sync.pass().await.unwrap();
    for orphan in &orphans {
        assert!(group.storage.status(orphan).await.unwrap().is_none(), "{orphan:?} stays");
    }
    for device in [&ana, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// Ben, who cannot see Gifts' key yet, has announced a Gifts snapshot, so
/// Ana cannot prove nothing in it names her unreferenced file: the file
/// stays, her second pass makes no read of the snapshot, and once Ben adds
/// her to Gifts the next pass reads it and deletes the file (§17.10).
#[tokio::test]
async fn unreadable_evidence_keeps_a_file_and_an_unchanged_pass_reads_it_no_more() {
    use coven_storage::test_utils::MemoryRequest;
    let group = Group::new();
    let devices = file_devices(&group, &[1, 2]).await;
    let (ana, ben) = (&devices[0], &devices[1]);
    group.create(ana).await;
    group.admit(ana, ben).await;
    let object = stored_file_whose_row_was_deleted(&group, ana).await;
    // Ben reads Ana's deletion and publishes a write that read through it.
    ben.sync.pass().await.unwrap();
    insert(ben.database(), "note", "written after the deletion").await;
    ben.sync.pass().await.unwrap();
    let gifts = ben.sync.create_circle("Gifts".into()).await.unwrap();
    let announced = ben.sync.snapshot(coven_merge::Audience::Circle(gifts)).await.unwrap();
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    let gifts_body = {
        let session = ana.sync.session.lock().await;
        crate::snapshot_object::path(
            group.store,
            &session.history.announcements[&announced].snapshot,
        )
        .unwrap()
    };
    let reads = |ana: &Device| {
        ana.storage
            .request_trace()
            .iter()
            .filter(
                |request| matches!(request, MemoryRequest::Read(path, _) if *path == gifts_body),
            )
            .count()
    };
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    assert!(group.storage.status(&object).await.unwrap().is_some(), "unreadable evidence keeps it");
    let operation = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "sequence-files")
        .expect("the waiting deletion stays journaled")
        .id;
    let pending = ana
        .database()
        .operation_pending::<crate::PendingRecord>(operation)
        .await
        .unwrap()
        .expect("the wait is reported");
    assert!(matches!(pending.reason, crate::PendingReason::KeyUnavailable { .. }), "{pending:?}");
    let before = reads(ana);
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(reads(ana), before, "an unchanged pass reads the snapshot no more");

    // Ben adds Ana to Gifts with no pass since the clock moved: his next
    // pass announces a newer Gifts snapshot and writes a floor naming it,
    // past the addition Ana's reload of Gifts targets.
    ben.sync
        .administer(StoreChange::AddCircleMember { circle: gifts, member: ana.identity.member() })
        .await
        .unwrap();
    for _ in 0..8 {
        ben.sync.pass().await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    // Ana's reload of Gifts restarts what every other device must have
    // seen, so Ben writes again before the deletion is settled.
    insert(ben.database(), "later", "written after Ana joined Gifts").await;
    for _ in 0..3 {
        ben.sync.pass().await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    assert!(group.storage.status(&object).await.unwrap().is_none(), "the file is deleted");
    assert!(ana.database().pending::<crate::PendingRecord>().await.unwrap().is_empty());
    for device in &devices {
        device.files().close().await;
        device.database().close().await.unwrap();
    }
}

/// The provider sees random object names and ciphertext only: a file's key,
/// its content and chunk hashes and the path of the user's original appear
/// in no stored object, and the file's object is named by its random id
/// (§17.5).
#[tokio::test]
async fn stored_objects_never_hold_a_files_key_hashes_or_path() {
    use coven_crypto::ContentHasher;
    use coven_database::FilePlace;
    let group = Group::new();
    let devices = file_devices(&group, &[1, 2]).await;
    let (ana, ben) = (&devices[0], &devices[1]);
    group.create(ana).await;
    group.admit(ana, ben).await;
    let originals = tempfile::tempdir().unwrap();
    let path = originals.path().join("private-recording-title.flac");
    let bytes: Vec<u8> =
        (0..70_000u32).map(|n| (n.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
    std::fs::write(&path, &bytes).unwrap();
    let prepared = coven_database::prepare_user_file(&path, &FileLease::for_test().await, |_| {})
        .await
        .unwrap();
    let staged = ana
        .database()
        .write_as::<_, _, coven_database::DbError>(move |sql| {
            let file = sql.put_user_file("files", prepared, coven_database::Place::Cloud)?;
            sql.execute("INSERT INTO files(id,file) VALUES('recording',?1)", [file])?;
            Ok(file)
        })
        .await
        .unwrap();
    ana.files().retry_uploads_now().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let file = ana.database().file_ref(staged).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the file");
    };
    let (coordinates, content_hash) = ana
        .database()
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row(
                "SELECT coordinates,hash FROM coven_files",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )?)
        })
        .await
        .unwrap();
    let key_text = coordinates.split_whitespace().last().unwrap().to_owned();
    let mut secrets: Vec<(&str, Vec<u8>)> = vec![
        ("the file key", key_text.clone().into_bytes()),
        ("the content hash", content_hash.clone()),
        (
            "the content hash in hex",
            content_hash.iter().map(|b| format!("{b:02x}")).collect::<String>().into_bytes(),
        ),
        ("the original's path", path.to_str().unwrap().as_bytes().to_vec()),
        ("the original's name", b"private-recording-title".to_vec()),
    ];
    if key_text.len() == 64 {
        if let Ok(raw) = (0..32)
            .map(|i| u8::from_str_radix(&key_text[2 * i..2 * i + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
        {
            secrets.push(("the raw file key", raw));
        }
    }
    for chunk in bytes.chunks(64 * 1024) {
        let mut hasher = ContentHasher::new();
        hasher.update(chunk);
        secrets.push(("a chunk hash", hasher.finish().as_bytes().to_vec()));
    }
    let objects = group.storage.list(&coven_storage::ObjectPrefix::all(group.store)).await.unwrap();
    assert!(objects.len() > 3, "slots, a snapshot and the file are stored");
    let mut found_file = false;
    for object in objects {
        let stored = group.storage.read(&object.path).await.unwrap();
        for (what, secret) in &secrets {
            assert!(
                !stored.windows(secret.len()).any(|window| window == secret.as_slice()),
                "{:?} holds {what}",
                object.path
            );
        }
        if object.path == location.path(group.store) {
            found_file = true;
            assert_eq!(
                object.path.as_str().rsplit('/').next().unwrap(),
                location.object().0.to_string()
            );
        }
    }
    assert!(found_file);
    for device in &devices {
        device.files().close().await;
        device.database().close().await.unwrap();
    }
}

/// Deleting stored files waits for a snapshot that has not finished (§17.10):
/// its journal still owns references no row holds. Ana's snapshot fails on
/// its announcement, so the cleanup of gone devices' files is refused with
/// the snapshot's id, and once the snapshot finishes it runs.
#[tokio::test]
async fn deleting_stored_files_waits_for_an_unfinished_snapshot() {
    use coven_storage::{test_utils::Faults, StorageFailure};
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let next = ana.database().applied_slot().await.unwrap() + 1;
    let announcement = ObjectPath::slot(group.store, next.try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(announcement, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.snapshot(coven_merge::Audience::Store).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let snapshot = ana.database().operations().await.unwrap()[0].id;
    let refused = ana.sync.clean_up_gone_devices_files().await;
    assert!(
        matches!(&refused, Err(crate::SyncError::SnapshotPending(id)) if *id == snapshot),
        "{refused:?}"
    );
    ana.sync.retry_pending_operation(snapshot).await.unwrap();
    ana.sync.clean_up_gone_devices_files().await.unwrap();
    ana.database().close().await.unwrap();
}

/// Ana's first store snapshot names her recording's file. She deletes the
/// row and takes a second snapshot without it. Once her floor passes the
/// first, she forgets its announcement, and deleting its body fails, so the
/// body stays in storage. File deletion reads the snapshots of the
/// announcements she keeps: the second, not the first, so the file goes
/// (§16.8, §17.10).
#[tokio::test]
async fn file_deletion_reads_no_snapshot_whose_announcement_was_forgotten() {
    use coven_database::FilePlace;
    use coven_storage::{
        test_utils::{Faults, MemoryRequest},
        StorageFailure,
    };
    let group = Group::new();
    let mut devices = file_devices(&group, &[1]).await;
    let ana = devices.remove(0);
    group.create(&ana).await;
    let originals = tempfile::tempdir().unwrap();
    let recording = originals.path().join("recording");
    std::fs::write(&recording, b"sound").unwrap();
    let prepared =
        coven_database::prepare_user_file(&recording, &FileLease::for_test().await, |_| {})
            .await
            .unwrap();
    let staged = ana
        .database()
        .write_as::<_, _, coven_database::DbError>(move |sql| {
            let file = sql.put_user_file("files", prepared, coven_database::Place::Cloud)?;
            sql.execute("INSERT INTO files(id,file) VALUES('recording',?1)", [file])?;
            Ok(file)
        })
        .await
        .unwrap();
    ana.files().retry_uploads_now().await.unwrap();
    let file = ana.database().file_ref(staged).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the file");
    };
    let object = location.path(group.store);
    ana.sync.pass().await.unwrap();
    let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let forgotten = announced_body(&group, &ana, announced).await;
    ana.database()
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    let kept = announced_body(&group, &ana, announced).await;
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.storage
        .set_faults(Faults {
            delete_failures: [(forgotten.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    let before = ana.storage.request_trace().len();
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    assert!(group.storage.status(&forgotten).await.unwrap().is_some(), "its deletion failed");
    assert!(group.storage.status(&object).await.unwrap().is_none(), "the file is deleted");
    let trace = ana.storage.request_trace().split_off(before);
    let read = |path: &ObjectPath| {
        trace.iter().any(|request| matches!(request, MemoryRequest::Read(read, _) if read == path))
    };
    assert!(read(&kept), "the kept announcement's snapshot is read: {trace:?}");
    assert!(!read(&forgotten), "the forgotten one's is not: {trace:?}");
    ana.files().close().await;
    ana.database().close().await.unwrap();
}

/// The path of the body the announcement at `slot` names.
async fn announced_body(group: &Group, device: &Device, slot: coven_merge::Slot) -> ObjectPath {
    let id = device.sync.session.lock().await.history.announcements[&slot].snapshot.clone();
    crate::snapshot_object::path(group.store, &id).unwrap()
}
