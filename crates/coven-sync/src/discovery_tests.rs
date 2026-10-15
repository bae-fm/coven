use crate::store_log_sync::tests::{device, key, member, store};
use coven_storage::test_utils::{MemoryRequest, MemoryStorage};
use std::sync::Arc;

#[tokio::test]
async fn membership_discovery_keeps_received_input_across_reopen() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut author = device(storage.clone(), 1, member(1), store(1)).await;
    author.create(key(1)).await;
    let connection = Arc::new(storage.for_device());
    let mut receiver = device(connection.clone(), 2, member(1), store(1)).await;
    assert!(receiver
        .sync
        .bootstrap_member(crate::BindingSource::Restore)
        .await
        .unwrap());
    assert_eq!(receiver.log().await, author.log().await);
    let first = connection.request_trace();
    assert!(first.iter().all(|request| !matches!(request,
        MemoryRequest::List(prefix) if prefix.as_str().contains("/store-log/") || prefix.as_str().contains("/keys/")
    )), "history must be discovered by exact reads: {first:?}");
    receiver.reopen(connection.clone(), vec![], vec![]).await;
    assert!(receiver
        .sync
        .bootstrap_member(crate::BindingSource::Restore)
        .await
        .unwrap());
    let later = connection.request_trace();
    assert!(later[first.len()..].iter().all(|request| !matches!(request,
        MemoryRequest::Read(path, _) if path.store_log_position().or(path.key_copy_position()).is_some_and(|(writer, number)| writer.0 == 1 && number.get() == 1)
    )), "received entry and copy bytes survive reopening");
}

use crate::{discovery, pass_reads::PassReads};
use coven_database::LogKind;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, Storage};
use std::{collections::BTreeMap, num::NonZeroUsize};

#[tokio::test]
async fn fetch_ahead_retains_a_hit_beyond_the_first_miss_without_crossing_it() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = |n: u64| ObjectPath::key_copy(store(1), DeviceId(1), n.try_into().unwrap());
    for n in [1u64, 3] {
        storage.create(&path(n), &[n as u8]).await.unwrap();
    }
    let writers = BTreeMap::from([(DeviceId(1), 0)]);
    let read = discovery::scan(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::KeyCopy,
        &writers,
        NonZeroUsize::new(3).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(read.len(), 2);
    assert_eq!(
        receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap(),
        [(DeviceId(1), 1)]
    );
    receiver.reopen(storage.clone(), vec![], vec![]).await;
    storage.create(&path(2), &[2]).await.unwrap();
    let before = storage.request_trace().len();
    discovery::scan(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::KeyCopy,
        &writers,
        NonZeroUsize::MIN,
    )
    .await
    .unwrap();
    assert_eq!(
        receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap(),
        [(DeviceId(1), 3)]
    );
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|request| matches!(request, MemoryRequest::Read(p, _) if p == &path(3))));
}

#[tokio::test]
async fn invitation_tail_never_substitutes_for_earlier_copy_history() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
    for n in 1u64..=4 {
        storage
            .create(
                &ObjectPath::key_copy(store(1), DeviceId(1), n.try_into().unwrap()),
                &[n as u8],
            )
            .await
            .unwrap();
    }
    let mut received = discovery::retained(
        &receiver.db,
        store(1),
        LogKind::KeyCopy,
        &BTreeMap::from([(DeviceId(1), 0)]),
    )
    .await
    .unwrap();
    discovery::scan_tail(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::KeyCopy,
        DeviceId(1),
        2,
        NonZeroUsize::MIN,
        &mut received,
    )
    .await
    .unwrap();
    assert_eq!(
        receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap(),
        [(DeviceId(1), 0)]
    );
    receiver.reopen(storage.clone(), vec![], vec![]).await;
    let before = storage.request_trace().len();
    let mut received = discovery::retained(
        &receiver.db,
        store(1),
        LogKind::KeyCopy,
        &BTreeMap::from([(DeviceId(1), 0)]),
    )
    .await
    .unwrap();
    discovery::scan_tail(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::KeyCopy,
        DeviceId(1),
        2,
        NonZeroUsize::MIN,
        &mut received,
    )
    .await
    .unwrap();
    let tail = &storage.request_trace()[before..];
    assert_eq!(tail.len(), 1);
    assert!(
        matches!(&tail[0], MemoryRequest::Read(path, None) if path.key_copy_position().unwrap().1.get() == 5)
    );
    discovery::scan(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::KeyCopy,
        &BTreeMap::from([(DeviceId(1), 0)]),
        NonZeroUsize::MIN,
    )
    .await
    .unwrap();
    assert_eq!(
        receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap(),
        [(DeviceId(1), 4)]
    );
}

#[tokio::test]
async fn malformed_copy_does_not_hide_a_later_valid_copy() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 1, member(1), store(1)).await;
    let material = coven_crypto::StoreKey::generate(key(1)).unwrap();
    let first = ObjectPath::key_copy(store(1), DeviceId(1), 1.try_into().unwrap());
    let bad_box = ObjectPath::key_copy(store(1), DeviceId(1), 2.try_into().unwrap());
    let second = ObjectPath::key_copy(store(1), DeviceId(1), 3.try_into().unwrap());
    storage.create(&first, b"invalid").await.unwrap();
    let bytes = crate::store_log_keys::store_copy(
        &second,
        &material,
        &receiver.member.member(),
        &receiver.member.keys().public_keys().sealing,
    )
    .unwrap();
    storage.create(&bad_box, &bytes).await.unwrap();
    storage.create(&second, &bytes).await.unwrap();
    let opened = crate::store_log_keys::read_store_key(
        &receiver.db,
        &PassReads::default(),
        NonZeroUsize::MIN,
        storage.as_ref(),
        store(1),
        key(1),
        Some(&material.hash()),
        &receiver.member,
    )
    .await
    .unwrap();
    assert_eq!(opened.hash(), material.hash());
    assert_eq!(
        receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap(),
        [(DeviceId(1), 3)]
    );
}

#[tokio::test]
async fn registration_learned_during_a_scan_discovers_an_unlisted_writer() {
    use coven_format::store_log::StoreChange;
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut author = device(storage.clone(), 1, member(1), store(1)).await;
    author.create(key(1)).await;
    let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
    receiver
        .sync
        .bootstrap_member(crate::BindingSource::Restore)
        .await
        .unwrap();
    author
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "second".into(),
            replaces: None,
        })
        .await
        .unwrap();
    let mut second = device(storage.clone(), 2, member(1), store(1)).await;
    second
        .sync
        .bootstrap_member(crate::BindingSource::Restore)
        .await
        .unwrap();
    let entry = second
        .sync
        .make_and_upload_entry(StoreChange::AddDevice {
            device: DeviceId(3),
            name: "third".into(),
            replaces: None,
        })
        .await
        .unwrap();
    storage
        .set_faults(coven_storage::test_utils::Faults {
            hide_listings: true,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    receiver
        .sync
        .bootstrap_member(crate::BindingSource::Restore)
        .await
        .unwrap();
    assert!(receiver.log().await.replay.entries.contains_key(&entry));
}

#[tokio::test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
async fn io_differential_discovery() {
    use coven_database::LogInput;
    use coven_storage::test_utils::Faults;
    use serde_json::json;
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for count in 0u64..=5 {
        for start in 0..=count {
            for (lost, failed) in [(false, false), (true, false), (false, true)] {
                let storage = Arc::new(MemoryStorage::builder().build().unwrap());
                let mut receiver = device(storage.clone(), 1, member(1), store(1)).await;
                for n in 1..=count {
                    receiver
                        .db
                        .prepare_key_upload(move |_| {
                            Ok::<_, coven_database::DbError>(vec![n as u8])
                        })
                        .await
                        .unwrap();
                }
                if lost {
                    storage
                        .set_faults(Faults {
                            lose_completion_reply: true,
                            ..Faults::none()
                        })
                        .await;
                }
                let first = crate::store_log_object::upload_keys(
                    store(1),
                    Some(storage.as_ref()),
                    &receiver.db,
                    &receiver.send,
                )
                .await;
                assert_eq!(first.is_err(), lost && count > 0);
                let before_confirmed = receiver.db.published_key_copy_number().await.unwrap();
                let mut before_published = 0;
                for n in 1..=count {
                    if storage
                        .status(&ObjectPath::key_copy(
                            store(1),
                            DeviceId(1),
                            n.try_into().unwrap(),
                        ))
                        .await
                        .unwrap()
                        .is_some()
                    {
                        before_published += 1;
                    }
                }
                storage.set_faults(Faults::none()).await;
                receiver.reopen(storage.clone(), vec![], vec![]).await;
                crate::store_log_object::upload_keys(
                    store(1),
                    Some(storage.as_ref()),
                    &receiver.db,
                    &receiver.send,
                )
                .await
                .unwrap();
                let confirmed = receiver.db.published_key_copy_number().await.unwrap();
                let mut published = 0;
                for n in 1..=count {
                    let path = ObjectPath::key_copy(store(1), DeviceId(1), n.try_into().unwrap());
                    let read = storage.read_object(&path, None).await.unwrap();
                    assert_eq!(read.bytes, [n as u8]);
                    published += 1;
                    if n <= start {
                        receiver
                            .db
                            .keep_log_inputs(
                                LogKind::KeyCopy,
                                vec![LogInput {
                                    writer: DeviceId(1),
                                    number: n.try_into().unwrap(),
                                    observation: serde_json::to_vec(&read.metadata).unwrap(),
                                    bytes: read.bytes.clone(),
                                }],
                            )
                            .await
                            .unwrap();
                    }
                }
                if failed {
                    storage
                        .set_faults(Faults {
                            fail_next: 1,
                            ..Faults::none()
                        })
                        .await;
                }
                let begin = storage.request_trace().len();
                let result = discovery::scan(
                    &receiver.db,
                    &PassReads::default(),
                    storage.as_ref(),
                    store(1),
                    LogKind::KeyCopy,
                    &BTreeMap::from([(DeviceId(1), 0)]),
                    NonZeroUsize::MIN,
                )
                .await;
                let positions = receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap();
                let position = positions.first().map_or(0, |(_, n)| *n);
                let requests: Vec<_> = storage.request_trace()[begin..]
                    .iter()
                    .map(|request| {
                        let MemoryRequest::Read(path, None) = request else {
                            panic!("unexpected request {request:?}");
                        };
                        path.key_copy_position().unwrap().1.get()
                    })
                    .collect();
                cases.push(json!({"check":"discovery","count":count,"start":start,"lost":lost,"failed":failed}));
                actual.push(json!({"before_published":before_published,"before_confirmed":before_confirmed,
                    "published":published,"confirmed":confirmed,"position":position,"requests":requests,"failed":result.is_err()}));
            }
        }
    }
    assert_eq!(actual, io_runner(&cases));
}

/// Evaluate cases with the Lean IO runner that scripts/check.sh supplies.
pub(crate) fn io_runner(cases: &[serde_json::Value]) -> Vec<serde_json::Value> {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let runner = std::env::var("COVEN_IO_RUNNER").expect("Lean IO runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn a_failed_read_keeps_completed_fetch_ahead_bodies_and_blocks_its_cursor() {
    use coven_storage::{test_utils::Faults, StorageFailure};
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = |n: u64| ObjectPath::key_copy(store(1), DeviceId(1), n.try_into().unwrap());
    for n in 1..=3 {
        storage.create(&path(n), &[n as u8]).await.unwrap();
    }
    storage
        .set_faults(Faults {
            read_failures: [(path(2), StorageFailure::Network)].into(),
            ..Faults::none()
        })
        .await;
    assert!(discovery::scan(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::KeyCopy,
        &BTreeMap::from([(DeviceId(1), 0)]),
        NonZeroUsize::new(3).unwrap()
    )
    .await
    .is_err());
    assert_eq!(
        receiver.db.log_downloads(LogKind::KeyCopy).await.unwrap(),
        [(DeviceId(1), 1)]
    );
    let retained = receiver
        .db
        .log_inputs(LogKind::KeyCopy, vec![(DeviceId(1), 0)])
        .await
        .unwrap();
    assert_eq!(
        retained
            .iter()
            .map(|input| input.number.get())
            .collect::<Vec<_>>(),
        [1, 3]
    );
}

#[tokio::test]
async fn an_unknown_folder_survives_its_first_miss_and_a_later_listing_omission() {
    use coven_storage::test_utils::Faults;
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = |n: u64| ObjectPath::store_log(store(1), DeviceId(42), n.try_into().unwrap());
    storage.create(&path(2), &[2]).await.unwrap();
    let writers = discovery::writers(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
    )
    .await
    .unwrap();
    discovery::scan(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::Entry,
        &writers.iter().map(|writer| (*writer, 0)).collect(),
        NonZeroUsize::MIN,
    )
    .await
    .unwrap();
    receiver.reopen(storage.clone(), vec![], vec![]).await;
    storage
        .set_faults(Faults {
            hide_listings: true,
            ..Faults::none()
        })
        .await;
    storage.create(&path(1), &[1]).await.unwrap();
    let writers = discovery::writers(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
    )
    .await
    .unwrap();
    assert!(writers.contains(&DeviceId(42)));
    let input = discovery::scan(
        &receiver.db,
        &PassReads::default(),
        storage.as_ref(),
        store(1),
        LogKind::Entry,
        &writers.iter().map(|writer| (*writer, 0)).collect(),
        NonZeroUsize::MIN,
    )
    .await
    .unwrap();
    assert_eq!(input.len(), 2);
}

/// Applied entries live in the decoded log: a catch-up reads retained entries
/// only past each writer's applied ones, keeping the newest applied entry,
/// whose observation keeps the storage-time sample (SP step 1, §3.1).
#[tokio::test]
async fn applied_entries_are_not_read_again() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut author = device(storage.clone(), 1, member(1), store(1)).await;
    author.create(key(1)).await;
    for device in [10, 11, 12] {
        author
            .sync
            .make_and_upload_entry(coven_format::store_log::StoreChange::AddDevice {
                device: DeviceId(device),
                name: "phone".into(),
                replaces: None,
            })
            .await
            .unwrap();
    }
    author.catch_up().await;
    let log = author.log().await;
    let writers = author.db.known_log_writers().await.unwrap();
    let retained = discovery::retained(
        &author.db,
        store(1),
        LogKind::Entry,
        &discovery::past_applied(&log, writers),
    )
    .await
    .unwrap();
    assert_eq!(
        retained.keys().copied().collect::<Vec<_>>(),
        [(DeviceId(1), 4.try_into().unwrap())]
    );
}

/// An entry or key copy longer than its format allows fails the read before
/// any of its body is received or retained; one at the bound is read (§6).
#[tokio::test]
async fn a_log_object_past_its_format_bound_fails_its_read_unreceived() {
    use coven_storage::StorageFailure;
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    for (kind, bound) in [
        (
            LogKind::Entry,
            coven_format::store_log::MAX_STORE_LOG_OBJECT,
        ),
        (
            LogKind::KeyCopy,
            coven_format::sealed_key::MAX_KEY_COPY_OBJECT,
        ),
    ] {
        let path = |n: u64| discovery::path(store(1), kind, DeviceId(1), n.try_into().unwrap());
        storage.create(&path(1), &vec![1; bound]).await.unwrap();
        storage.plant(&path(2), &vec![2; bound + 1]).await.unwrap();
        let error = discovery::scan(
            &receiver.db,
            &PassReads::default(),
            storage.as_ref(),
            store(1),
            kind,
            &BTreeMap::from([(DeviceId(1), 0)]),
            NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, crate::SyncError::Storage(error) if error.failure() == StorageFailure::Protocol),
            "{error:?}"
        );
        let retained = receiver
            .db
            .log_inputs(kind, vec![(DeviceId(1), 0)])
            .await
            .unwrap();
        assert_eq!(
            retained
                .iter()
                .map(|input| (input.number.get(), input.bytes.len()))
                .collect::<Vec<_>>(),
            [(1, bound)]
        );
    }
}
