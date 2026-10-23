use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_storage::{ObjectPath, Storage};

/// Ana's store on S3 with Ben admitted, then removed: his key still works.
async fn removed_fixture() -> (Group, Device, Device) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
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
    let scans = ana
        .sync
        .uploader_scans(&history, &[uploader].into(), listing)
        .await
        .unwrap();
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
            .map(|candidate| u64::try_from(candidate.file.0.as_u128()).unwrap())
            .collect::<Vec<_>>()),
    }
}

/// A removed S3 member's folder is listed once when the removal applies,
/// with no confirmation that their key was deleted, and a later pass doesn't
/// list it again (§17.8).
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
    assert!(ana
        .database()
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_some());
    assert_eq!(listings(), 1);
    ana.sync.pass().await.unwrap();
    assert_eq!(listings(), 1);
    ana.sync.clean_up_removed_files().await.unwrap();
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
                            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(
                                file.as_u64().unwrap().into(),
                            )),
                        ),
                        b"file",
                    )
                    .await
                    .unwrap();
            } else if let Some(completes) = event.get("pass") {
                let completes = completes.as_bool().unwrap();
                scan(&ana, ben.sync.device, super::Listing::AtRemoval, completes).await;
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

/// Ana's pinned original finished uploading, so her pin worker keeps the
/// stored file whole from storage (§17.6). A held download of it never
/// holds her sequence passes: each pass interrupts it, releasing the single
/// download slot, and it resumes after (§6.6).
#[tokio::test]
async fn stalled_pin_downloads_leave_sequence_passes_free() {
    use coven_database::{
        DatabaseBuilder, FileDecl, Migration, PinProgress, Provenance, RowIdentity, SyncedTable,
    };
    use coven_foundation::id_source::{IdSource, MemberId};
    use std::time::Duration;
    let group = Group::new();
    let mut devices = Vec::new();
    for seed in [1, 2] {
        let (temp, directory) = group.directory();
        let database = DatabaseBuilder::new(directory)
            .synced_tables(vec![
                SyncedTable::new("notes", RowIdentity::SharedKey),
                SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                    "files",
                    1 << 20,
                    Provenance::UserProvided,
                )),
            ])
            .migrations(vec![Migration::addition(1, "files", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")])
            .open().await.unwrap();
        let identity = coven_crypto::MemberIdentity::new(
            MemberId(group.ids.new_id()),
            coven_format::test_utils::keys_from_seed(seed),
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
    let ana = &devices[0];
    let ben = &devices[1];
    group.create(ana).await;
    group.admit(ana, ben).await;
    let originals = tempfile::tempdir().unwrap();
    let path = originals.path().join("recording");
    std::fs::write(&path, b"sound").unwrap();
    let prepared = coven_database::prepare_user_file(&path, |_| {})
        .await
        .unwrap();
    ana.database()
        .write(move |sql| {
            sql.execute("INSERT INTO files(id,size) VALUES('recording',5)", [])?;
            sql.register_user_file("files", "recording", prepared)?;
            Ok(())
        })
        .await
        .unwrap();
    let file = ana.database().file_ref("files", "recording").await.unwrap();
    ana.files().pin(std::slice::from_ref(&file)).await.unwrap();
    // Whether every byte of the file is kept on Ana's device.
    let kept = || async {
        let PinProgress {
            bytes_cached,
            bytes_total,
            ..
        } = ana
            .database()
            .pin_progress(vec![file.clone()])
            .await
            .unwrap();
        bytes_cached == bytes_total
    };
    let object = ObjectPath::file(group.store, file.uploader(), file.id());
    // Hold the next response reaching the file's only chunk, whether or not
    // it carries the header; the receiver reports when it starts, the sender
    // releases it.
    let hold = || async {
        let (started, observed) = tokio::sync::oneshot::channel();
        let (resume, held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_body_at(object.clone(), 12, started, held)
            .await;
        (observed, resume)
    };
    // Ana's first pass publishes her write and lets her upload start.
    ana.files().set_uploads_paused(true).await.unwrap();
    ana.sync.pass().await.unwrap();
    let (observed, resume) = hold().await;
    // Completing the upload carries the pin; the pin worker starts keeping
    // the stored file whole, and its response is held.
    ana.files().set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), observed)
        .await
        .unwrap()
        .unwrap();
    // The pass completes while the file waits: it interrupts the download,
    // which yields the single download slot and starts again after it.
    let (retry_observed, retry_resume) = hold().await;
    let pass = tokio::time::timeout(Duration::from_secs(20), ana.sync.pass())
        .await
        .expect("a held file must not hold a sequence pass");
    assert!(matches!(
        pass.unwrap(),
        crate::SequenceStatus::Synced { .. }
    ));
    assert!(
        resume.send(()).is_err(),
        "the first response released its lease"
    );
    tokio::time::timeout(Duration::from_secs(20), retry_observed)
        .await
        .unwrap()
        .unwrap();
    assert!(!kept().await);
    let applied = ana.database().applied_slot().await.unwrap();
    insert(ben.database(), "later", "arrived while the file waited").await;
    ben.sync.pass().await.unwrap();
    // A cancelled pass must release the pause as well as its session lock.
    let (observed, resume) = hold().await;
    let (reading, read_observed) = tokio::sync::oneshot::channel();
    let (read_resume, read_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_read(
            ObjectPath::slot(group.store, std::num::NonZeroU64::new(applied + 1).unwrap()),
            reading,
            read_held,
        )
        .await;
    {
        let pass = ana.sync.pass();
        tokio::pin!(pass);
        tokio::time::timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = &mut pass => panic!("held sequence read returned: {result:?}"),
                result = read_observed => result.unwrap(),
            }
        })
        .await
        .unwrap();
    }
    assert!(read_resume.send(()).is_err());
    assert!(retry_resume.send(()).is_err());
    tokio::time::timeout(Duration::from_secs(20), observed)
        .await
        .unwrap()
        .unwrap();
    assert!(!kept().await);
    let (last_observed, last_resume) = hold().await;
    let pass = tokio::time::timeout(Duration::from_secs(20), ana.sync.pass())
        .await
        .expect("later slots must apply while the file waits")
        .unwrap();
    assert!(matches!(pass, crate::SequenceStatus::Synced { .. }));
    assert!(ana.database().applied_slot().await.unwrap() > applied);
    assert_eq!(
        rows(ana.database()).await,
        [("later".into(), "arrived while the file waited".into())]
    );
    assert!(resume.send(()).is_err());
    tokio::time::timeout(Duration::from_secs(20), last_observed)
        .await
        .unwrap()
        .unwrap();
    assert!(!kept().await);
    let mut progress = ana.database().subscribe_pin_progress(vec![file.clone()]);
    last_resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let progress = progress.next().await.unwrap();
            if progress.bytes_cached == progress.bytes_total {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(ana.files().read_file(&file).await.unwrap(), b"sound");
    for device in devices {
        device.files().close().await;
        device.database().close().await.unwrap();
    }
}
