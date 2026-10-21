use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_foundation::id_source::IdSource;
use coven_storage::{ObjectPath, Storage};

#[tokio::test]
async fn a_removed_uploader_is_scanned_once_only_after_provider_cutoff() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let orphan = ObjectPath::file(
        group.store,
        ben.sync.device,
        coven_foundation::id_source::FileId(group.ids.new_id()),
    );
    group
        .storage
        .create(&orphan, b"orphan upload")
        .await
        .unwrap();
    ana.sync
        .administer(StoreChange::RemoveDevice {
            device: ben.sync.device,
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_none());
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_none());
    ana.sync
        .confirm_access_key_deleted("ben".into())
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .database
        .file_scan(ben.sync.device)
        .await
        .unwrap()
        .is_some());
    assert!(group.storage.status(&orphan).await.unwrap().is_none());
    let listings = || {
        ana.storage.request_trace().iter().filter(|request| matches!(request,
        coven_storage::test_utils::MemoryRequest::List(prefix) if *prefix == coven_storage::ObjectPrefix::device_files(group.store, ben.sync.device))).count()
    };
    assert_eq!(listings(), 1);
    let count = ana.storage.request_count();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.storage.request_count() - count, 2);
    assert_eq!(listings(), 1);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

const SCAN_ACCOUNT: &str = "ben@example.com";

async fn scan_fixture() -> (Group, Device, Device) {
    let group = Group::shared();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group
        .create_with_access(
            &ana,
            coven_format::MemberAccess::ProviderAccount("ana@example.com".into()),
        )
        .await;
    ana.sync
        .administer(StoreChange::AddMember {
            member: ben.identity.member(),
            keys: ben.identity.keys().public_keys(),
            role: coven_format::admin::MemberRole::Admin,
            access: coven_format::MemberAccess::ProviderAccount(SCAN_ACCOUNT.into()),
        })
        .await
        .unwrap();
    ben.sync
        .administer(StoreChange::AddDevice {
            name: "Ben".into(),
            replaces: None,
        })
        .await
        .unwrap();
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    set_scan_access(&ana, true).await;
    (group, ana, ben)
}

/// An invitation can restore a removed member's provider account without
/// changing the removed device. Exercise the real intention and confirmation
/// transactions, including the trigger which invalidates its old scan.
async fn set_scan_access(ana: &Device, granted: bool) {
    use coven_database::{NewOperation, ProviderAccessChange};
    let operation = || NewOperation {
        kind: "provider-access".into(),
        data: serde_json::to_vec(SCAN_ACCOUNT).unwrap(),
        started_by: "coven".into(),
        pauses_writes: false,
    };
    let invitation = ana
        .database()
        .start_operation_with_access::<crate::PendingRecord>(
            NewOperation {
                kind: "scan-invitation".into(),
                ..operation()
            },
            vec![ProviderAccessChange {
                account: SCAN_ACCOUNT.into(),
                intended: granted,
                initially_granted: true,
                needs_invitation: false,
                owner: true,
                operation: operation(),
            }],
        )
        .await
        .unwrap();
    if let Some(request) = ana
        .database()
        .provider_access(SCAN_ACCOUNT.into())
        .await
        .unwrap()
        .unwrap()
        .request
    {
        let target = ana
            .database()
            .start_provider_request::<crate::PendingRecord>(SCAN_ACCOUNT.into(), request.operation)
            .await
            .unwrap();
        if target {
            ana.storage.grant_access(SCAN_ACCOUNT).await.unwrap();
        } else {
            assert!(ana
                .storage
                .revoke_access(SCAN_ACCOUNT)
                .await
                .unwrap()
                .is_empty());
        }
        assert!(ana
            .database()
            .complete_provider_request::<crate::PendingRecord>(
                SCAN_ACCOUNT.into(),
                request.operation,
                target,
                None
            )
            .await
            .unwrap());
    }
    ana.database().finish_operation(invitation).await.unwrap();
}

async fn scan(ana: &Device, uploader: coven_foundation::id_source::DeviceId, completes: bool) {
    use coven_storage::{test_utils::Faults, StorageFailure};
    let history = ana.sync.session.lock().await.history.clone();
    let scans = ana
        .sync
        .uploader_scans(&history, &[uploader].into())
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

#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_file_scan() {
    use serde_json::json;
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for files in [vec![], vec![11], vec![11, 24, 39]] {
        let (group, ana, ben) = scan_fixture().await;
        for file in &files {
            group
                .storage
                .create(
                    &ObjectPath::file(
                        group.store,
                        ben.sync.device,
                        coven_foundation::id_source::FileId(uuid::Uuid::from_u128(*file)),
                    ),
                    b"file",
                )
                .await
                .unwrap();
        }
        set_scan_access(&ana, false).await;
        let mut listings = Vec::new();
        let mut observed = Vec::new();
        for completes in [false, false, true, false, true] {
            listings.push(if completes {
                json!({"found":files})
            } else {
                json!({"failed":true})
            });
            scan(&ana, ben.sync.device, completes).await;
            observed.push(scan_paths(&ana, ben.sync.device).await);
        }
        inputs.push(json!({"check":"file_scan","writer":ben.sync.device.0,"listings":listings}));
        actual.push(json!(observed));
        ana.database().close().await.unwrap();
        ben.database().close().await.unwrap();
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}

#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_folder() {
    use serde_json::json;
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for first in [false, true] {
        let (group, ana, ben) = scan_fixture().await;
        let events = vec![
            json!({"retain":true}),
            json!({"upload":11}),
            json!({"revoke":true}),
            json!({"retain":first}),
            json!({"retain":true}),
            json!({"restore":true}),
            json!({"upload":24}),
            json!({"revoke":true}),
            json!({"retain":false}),
            json!({"retain":true}),
            json!({"restore":true}),
            json!({"revoke":true}),
            json!({"retain":true}),
        ];
        let mut published = Vec::new();
        let mut observed = Vec::new();
        for event in &events {
            if let Some(file) = event.get("upload") {
                let file = file.as_u64().unwrap();
                group
                    .storage
                    .create(
                        &ObjectPath::file(
                            group.store,
                            ben.sync.device,
                            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(file.into())),
                        ),
                        b"file",
                    )
                    .await
                    .unwrap();
                published.push(json!(file));
            } else if let Some(completes) = event.get("retain") {
                scan(&ana, ben.sync.device, completes.as_bool().unwrap()).await;
            } else if event.get("restore").is_some() {
                set_scan_access(&ana, true).await;
            } else {
                assert!(event.get("revoke").is_some());
                set_scan_access(&ana, false).await;
            }
            let paths = scan_paths(&ana, ben.sync.device).await;
            observed.push(match paths {
                serde_json::Value::Null => serde_json::Value::Null,
                serde_json::Value::Array(paths) => {
                    json!(published.iter().all(|file| paths.contains(file)))
                }
                _ => unreachable!("scan paths are an array or pending"),
            });
        }
        inputs.push(json!({"check":"folder","writer":ben.sync.device.0,"events":events}));
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
