use super::*;
use coven_storage::StorageError;
use tokio::sync::Barrier;

#[tokio::test]
async fn far_future_entry_waits_for_missing_or_damaged_sealed_keys() {
    let storage = storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(365 * 86400));
    a.create(key(1)).await;
    let path = copy_path(
        storage.as_ref(),
        Audience::Store,
        key(1),
        &a.member.member(),
    )
    .await;
    let original = storage.read(&path).await.unwrap();
    storage.delete(&path).await.unwrap();
    let mut b = device(storage.clone(), 2, member(1), store(1)).await;
    b.sync().await;
    assert!(b.log().await.entries.is_empty());
    // Copies are immutable; each variant arrives at the writer's next number.
    let (writer, first) = path.key_copy_position().unwrap();
    let next =
        |offset: u64| ObjectPath::key_copy(store(1), writer, first.checked_add(offset).unwrap());
    let mut damaged = original.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    storage.create(&path, &damaged).await.unwrap();
    // A refused copy is the key copy's own pending record, not a pass failure.
    let store_copy = crate::PendingSubject::KeyCopy {
        audience: Audience::Store,
        key: key(1),
        member: b.member.member(),
        sealing: b.member.keys().public_keys().sealing,
    };
    let copy_reason = |records: Vec<crate::PendingRecord>| {
        records
            .into_iter()
            .find(|record| record.subject == store_copy)
            .map(|record| record.reason)
    };
    b.sync.step().await.unwrap();
    assert!(matches!(
        copy_reason(b.db.pending().await.unwrap()),
        Some(crate::PendingReason::Refused(Refusal::Decryption { .. }))
    ));
    assert!(b.log().await.entries.is_empty());
    for (offset, claimed_key) in [(1, key(1)), (2, key(2))] {
        let path = next(offset);
        let prefix = coven_format::sealed_key::KeyCopyPrefix {
            audience: Audience::Store,
            key: claimed_key,
            member: a.member.member(),
            sealing: a.member.keys().public_keys().sealing,
        };
        let wrong = coven_crypto::seal_store_key(
            &StoreKey::generate(key(2)).unwrap(),
            &prefix.sealing,
            path.as_str(),
            &prefix.encode().unwrap(),
        )
        .unwrap();
        let wrong = prefix.encode_box(&wrong).unwrap();
        storage.create(&path, &wrong).await.unwrap();
        b.sync.step().await.unwrap();
        let reason = copy_reason(b.db.pending().await.unwrap());
        // The first copy remains refused; later failures do not hide it.
        assert!(matches!(
            reason,
            Some(crate::PendingReason::Refused(Refusal::Decryption { .. }))
        ));
        assert!(b.custody.read().unwrap().is_none());
    }
    // A later valid copy supplies the key without rereading the refused ones.
    let ring = a.custody.read().unwrap().unwrap();
    let valid = crate::store_log_keys::store_copy(
        &next(3),
        ring.store_key(key(1)).unwrap(),
        &a.member.member(),
        &a.member.keys().public_keys().sealing,
    )
    .unwrap();
    storage.create(&next(3), &valid).await.unwrap();
    let before = storage.request_trace().len();
    b.sync().await;
    assert_eq!(a.log().await, b.log().await);
    assert_eq!(copy_reason(b.db.pending().await.unwrap()), None);
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|request| matches!(
            request,
            MemoryRequest::Read(read, _) if read == &path || read == &next(1) || read == &next(2)
        )));
}

#[tokio::test]
async fn copies_use_one_contiguous_writer_sequence_across_audiences() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Shared".into(),
            key: key(2),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    let paths: Vec<_> = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.as_str().to_owned())
        .collect();
    assert_eq!(
        paths,
        (1..=3)
            .map(|number| format!("{}/keys/1/{number}", store(1)))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn introductions_refuse_material_that_does_not_match_the_committed_hash() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut wrong_hash = ana.sync.test_key(Audience::Store, key(1));
    wrong_hash[0] ^= 1;
    let result = ana
        .sync
        .make_and_upload_entry(StoreChange::CreateStore {
            store: store(1),
            name: ana.directory.settings().unwrap().name,
            admin: public(&ana.member),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "owner".into(),
                principal: coven_format::S3Principal::Console,
            },
            device_name: "Ana".into(),
            key: key(1),
            key_hash: wrong_hash,
        })
        .await;
    assert!(matches!(
        result,
        Err(SyncError::Key(
            coven_crypto::MaterialError::StoreKeyConflict(_)
        ))
    ));
    assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
    assert!(ana.log().await.entries.is_empty());
    ana.create(key(1)).await;
    let mut wrong_hash = ana.sync.test_key(Audience::Circle(circle(1)), key(2));
    wrong_hash[31] ^= 1;
    let result = ana
        .sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Home".into(),
            key: key(2),
            key_hash: wrong_hash,
        })
        .await;
    assert!(matches!(
        result,
        Err(SyncError::Key(
            coven_crypto::MaterialError::CircleKeyConflict { .. }
        ))
    ));
    assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Home".into(),
            key: key(2),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    for audience in [Audience::Store, Audience::Circle(circle(1))] {
        let before = ana.log().await;
        let mut key_hash = ana.sync.test_key(audience.clone(), key(3));
        key_hash[15] ^= 1;
        let result = ana
            .sync
            .make_and_upload_entry(StoreChange::RotateKey {
                audience,
                key: key(3),
                key_hash,
            })
            .await;
        assert!(matches!(result, Err(SyncError::Key(_))));
        assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
        assert_eq!(ana.log().await, before);
    }
}

#[path = "../tests/fixtures/copy_storage.rs"]
mod copy_storage;
pub(crate) use copy_storage::failing_creation;
use copy_storage::{CopyAttempt, CopyStorage};

async fn household(storage: &Arc<MemoryStorage>) -> [Device; 4] {
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let dan = device(storage.clone(), 3, member(3), store(1)).await;
    let mut erin = device(storage.clone(), 4, member(4), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Admin).await;
    ana.add(&dan.member, MemberRole::Member).await;
    ana.add(&erin.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    ana.sync().await;
    erin.sync().await;
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    [ana, ben, dan, erin]
}

/// Ben rotates to K3 while Ana concurrently adds Erin, so Ben's rotation
/// copies omit Erin and Ana's addition carries only K1. Both admins then
/// need to share K3 with Erin. Copies are never deleted to create a need.
/// `holder` treats Ben's K3 copy for Ana as still in flight, or as damaged
/// by the provider. Returns that copy's path.
async fn concurrent_addition(
    storage: &Arc<MemoryStorage>,
    holder: Option<bool>,
) -> ([Device; 4], ObjectPath) {
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let dan = device(storage.clone(), 3, member(3), store(1)).await;
    let mut erin = device(storage.clone(), 4, member(4), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Admin).await;
    ana.add(&dan.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    ana.sync().await;
    ana.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    ben.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    let ben_device = ben.device().await;
    let first = next_copy(storage.as_ref(), ben_device)
        .await
        .key_copy_position()
        .unwrap()
        .1;
    if holder == Some(true) {
        ben.sync.storage = Some(Arc::new(CopyStorage::new(
            storage.clone(),
            Vec::new(),
            CopyAttempt::CorruptFor(ana.member.member()),
        )));
    }
    let rotation = ben.sync.test_rotate(Audience::Store, key(3)).await;
    ben.sync.storage = Some(storage.clone());
    let holder_path = copy_path(
        storage.as_ref(),
        Audience::Store,
        key(3),
        &ana.member.member(),
    )
    .await;
    // Ana's addition is concurrent: Ben's rotation and its copies have not
    // landed for her yet.
    let mut unseen = vec![crate::store_log_object::path(store(1), rotation)];
    let mut number = first;
    loop {
        let path = ObjectPath::key_copy(store(1), ben_device, number);
        if storage.status(&path).await.unwrap().is_none() {
            break;
        }
        unseen.push(path);
        number = number.checked_add(1).unwrap();
    }
    ana.sync.storage = Some(Arc::new(CopyStorage::new(
        storage.clone(),
        unseen,
        CopyAttempt::Hidden,
    )));
    ana.add(&erin.member, MemberRole::Member).await;
    ana.sync.storage = Some(if holder == Some(false) {
        Arc::new(CopyStorage::new(
            storage.clone(),
            vec![holder_path.clone()],
            CopyAttempt::Hidden,
        ))
    } else {
        storage.clone()
    });
    erin.sync().await;
    assert!(find_copy(
        storage.as_ref(),
        Audience::Store,
        key(3),
        &erin.member.member()
    )
    .await
    .is_none());
    ([ana, ben, dan, erin], holder_path)
}

async fn next_copy(storage: &dyn Storage, writer: DeviceId) -> ObjectPath {
    let last = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|object| object.path.key_copy_position())
        .filter(|(device, _)| *device == writer)
        .map(|(_, number)| number.get())
        .max()
        .unwrap_or(0);
    ObjectPath::key_copy(store(1), writer, (last + 1).try_into().unwrap())
}

#[tokio::test]
async fn a_missing_or_damaged_holder_copy_waits_and_a_later_copy_enables_sharing() {
    for damaged in [false, true] {
        let storage = storage();
        let ([mut ana, ben, _dan, erin], _) = concurrent_addition(&storage, Some(damaged)).await;
        ana.sync.step().await.unwrap();
        let subject = crate::PendingSubject::KeyCopy {
            audience: Audience::Store,
            key: key(3),
            member: ana.member.member(),
            sealing: ana.member.keys().public_keys().sealing,
        };
        let reason = ana
            .db
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.subject == subject)
            .unwrap()
            .reason;
        if damaged {
            assert!(matches!(
                reason,
                crate::PendingReason::Refused(Refusal::Decryption { .. })
            ));
        } else {
            assert_eq!(
                reason,
                crate::PendingReason::KeyUnavailable {
                    audience: Audience::Store,
                    key: key(3),
                }
            );
        }
        assert!(ana
            .custody
            .read()
            .unwrap()
            .unwrap()
            .store_key(key(3))
            .is_err());
        assert!(find_copy(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member()
        )
        .await
        .is_none());
        // The in-flight copy lands, or Ben's next copy supplies a valid one.
        ana.sync.storage = Some(storage.clone());
        if damaged {
            let ring = ben.custody.read().unwrap().unwrap();
            let later = next_copy(storage.as_ref(), ben.device().await).await;
            let valid = crate::store_log_keys::store_copy(
                &later,
                ring.store_key(key(3)).unwrap(),
                &ana.member.member(),
                &ana.member.keys().public_keys().sealing,
            )
            .unwrap();
            storage.create(&later, &valid).await.unwrap();
        }
        ana.sync().await;
        let copy = find_copy(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member(),
        )
        .await
        .unwrap();
        assert_eq!(copy.path.device(), Some(ana.device().await));
        assert_eq!(
            coven_format::sealed_key::SealedKey::decode(&copy.bytes)
                .unwrap()
                .open_store_key(&erin.member, copy.path.as_str())
                .unwrap()
                .id(),
            key(3)
        );
    }
}

#[tokio::test]
async fn two_suppliers_publish_distinct_copies_for_the_same_member() {
    let storage = storage();
    let ([mut ana, mut ben, _dan, mut erin], _) = concurrent_addition(&storage, None).await;
    let paths = vec![
        next_copy(storage.as_ref(), ana.device().await).await,
        next_copy(storage.as_ref(), ben.device().await).await,
    ];
    let racing = Arc::new(CopyStorage::new(
        storage.clone(),
        paths.clone(),
        CopyAttempt::Race(Barrier::new(2)),
    ));
    ana.sync.storage = Some(racing.clone());
    ben.sync.storage = Some(racing.clone());
    let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(ana.sync.sync_store_log(), ben.sync.sync_store_log())
    })
    .await
    .unwrap();
    a.unwrap();
    b.unwrap();
    let attempts = racing.attempts();
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0].0, attempts[1].0);
    assert_ne!(attempts[0].1, attempts[1].1);
    let expected = ben
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(key(3))
        .unwrap()
        .hash();
    for (path, bytes) in attempts {
        assert!(paths.contains(&path));
        assert_eq!(storage.read(&path).await.unwrap(), bytes);
        assert_eq!(
            coven_format::sealed_key::SealedKey::decode(&bytes)
                .unwrap()
                .open_store_key(&erin.member, path.as_str())
                .unwrap()
                .hash(),
            expected
        );
    }
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
    assert!(ben.db.key_uploads().await.unwrap().is_empty());
    erin.sync().await;
    assert_eq!(ana.log().await, erin.log().await);
}

#[tokio::test]
async fn interrupted_and_lost_reply_copies_retry_identical_bytes_after_restart() {
    for lost_reply in [false, true] {
        let storage = storage();
        let ([mut ana, _ben, _dan, mut erin], _) = concurrent_addition(&storage, None).await;
        let path = next_copy(storage.as_ref(), ana.device().await).await;
        let fault = Arc::new(CopyStorage::new(
            storage.clone(),
            vec![path.clone()],
            if lost_reply {
                CopyAttempt::LoseReplyOnce
            } else {
                CopyAttempt::InterruptOnce
            },
        ));
        ana.sync.storage = Some(fault.clone());
        assert!(
            matches!(ana.sync.sync_store_log().await.unwrap_err(), SyncError::Storage(error) if error.failure() == StorageFailure::Network)
        );
        let fixed = ana.db.key_uploads().await.unwrap().remove(0);
        assert_eq!(fixed.path(store(1)), path);
        assert_eq!(fault.attempts()[0].1, fixed.bytes);
        ana.restart(storage.clone()).await;
        assert_eq!(
            ana.db.key_uploads().await.unwrap(),
            std::slice::from_ref(&fixed)
        );
        ana.sync.storage = Some(fault.clone());
        ana.catch_up().await;
        ana.sync
            .make_and_upload_entry(StoreChange::AddDevice {
                device: DeviceId(20),
                name: "Ana phone".into(),
                replaces: None,
            })
            .await
            .unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), fixed.bytes);
        assert_eq!(fault.attempts(), vec![(path, fixed.bytes); 2]);
        assert!(ana.db.key_uploads().await.unwrap().is_empty());
        erin.sync().await;
        assert!(erin
            .custody
            .read()
            .unwrap()
            .unwrap()
            .store_key(key(3))
            .is_ok());
    }
}

#[tokio::test]
async fn reserved_copy_settles_even_after_another_supplier_or_recipient_removal() {
    for removed in [false, true] {
        let storage = storage();
        let ([mut ana, mut ben, _dan, erin], _) = concurrent_addition(&storage, None).await;
        let path = next_copy(storage.as_ref(), ana.device().await).await;
        let fault = Arc::new(CopyStorage::new(
            storage.clone(),
            vec![path.clone()],
            CopyAttempt::InterruptOnce,
        ));
        ana.sync.storage = Some(fault);
        assert!(ana.sync.sync_store_log().await.is_err());
        let fixed = ana.db.key_uploads().await.unwrap().remove(0);
        // The other writer supplies the same need at its own path.
        ben.sync().await;
        let supplied = copy_path(
            storage.as_ref(),
            Audience::Store,
            key(3),
            &erin.member.member(),
        )
        .await;
        assert_ne!(supplied, path);
        if removed {
            let id = ben
                .sync
                .make_and_upload_entry(StoreChange::RemoveMember {
                    member: erin.member.member(),
                })
                .await
                .unwrap();
            let entry = ben
                .log()
                .await
                .entries
                .into_iter()
                .find(|entry| entry.entry.position == id)
                .unwrap()
                .entry;
            let (entry, replay) = crate::replay::tests::replay_entry(&ana.log().await, entry);
            ana.db
                .apply_store_log::<crate::PendingRecord>(entry, replay, Vec::new())
                .await
                .unwrap();
        }
        ana.restart(storage.clone()).await;
        ana.sync().await;
        assert_eq!(storage.read(&path).await.unwrap(), fixed.bytes);
        let expected_next = fixed.number.get() + 1;
        ana.sync
            .make_and_upload_entry(StoreChange::CreateCircle {
                circle: circle(9),
                name: "Next".into(),
                key: key(9),
                key_hash: ana.sync.test_key(Audience::Circle(circle(9)), key(9)),
            })
            .await
            .unwrap();
        let next = copy_path(
            storage.as_ref(),
            Audience::Circle(circle(9)),
            key(9),
            &ana.member.member(),
        )
        .await;
        assert_eq!(next.key_copy_position().unwrap().1.get(), expected_next);
    }
}

#[tokio::test]
async fn cancelling_publication_retains_prerequisites_and_leaves_no_number_gap() {
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(4096, 4096)
            .build()
            .unwrap(),
    );
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (_resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_creation(ObjectPrefix::keys(store(1)), started, held)
        .await;
    let change = StoreChange::CreateCircle {
        circle: circle(1),
        name: "Retained".into(),
        key: key(2),
        key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
    };
    let mut call = Box::pin(ana.sync.make_and_upload_entry(change));
    tokio::select! { result = &mut call => panic!("publication returned {result:?}"), result = waiting => result.unwrap() }
    drop(call);
    let queued = ana.db.key_uploads().await.unwrap();
    let entry = ana
        .db
        .local_store_log()
        .await
        .unwrap()
        .upload
        .unwrap()
        .entry
        .position;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].before_entry, Some(entry));
    assert_eq!(queued[0].number.get(), 2);
    assert!(
        matches!(storage.read(&object::path(store(1), entry)).await, Err(error) if error.failure() == StorageFailure::NotFound)
    );
    ana.restart(storage.clone()).await;
    ana.sync().await;
    assert_eq!(
        storage.read(&queued[0].path(store(1))).await.unwrap(),
        queued[0].bytes
    );
    ana.add(&member(2), MemberRole::Member).await;
    let mut numbers: Vec<_> = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.key_copy_position().unwrap().1.get())
        .collect();
    numbers.sort();
    assert_eq!(numbers, [1, 2, 3]);
}

/// Another live copy of this database sealed its own copy at the same
/// number: both decode and the bytes differ, so §10's comparison stops this
/// copy and keeps the copy queued. Undecodable bytes there would be damage,
/// never another copy's (`OwnCopyDamage.unreadable_is_damage`).
#[tokio::test]
async fn unequal_occupant_of_an_own_copy_path_stops_the_copy_and_keeps_it_queued() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    let ring = ana.custody.read().unwrap().unwrap();
    let material = ring.store_key(key(1)).unwrap().clone();
    let sealed = material.clone();
    let (id, sealing) = (ana.member.member(), ana.member.keys().public_keys().sealing);
    let copy = ana
        .db
        .prepare_key_upload(move |path| keys::store_copy(path, &sealed, &id, &sealing))
        .await
        .unwrap();
    // The other copy seals the same key with its own randomness.
    let other = keys::store_copy(&copy.path(store(1)), &material, &id, &sealing).unwrap();
    assert_ne!(other, copy.bytes);
    storage.create(&copy.path(store(1)), &other).await.unwrap();
    assert!(matches!(
        ana.sync.step().await,
        Err(SyncError::DeviceReset(
            coven_foundation::files::DeviceResetReason::SlotMismatch
        ))
    ));
    assert_eq!(ana.db.key_uploads().await.unwrap(), [copy]);
}

#[tokio::test]
async fn failed_occupied_comparison_keeps_the_copy_pending() {
    let storage = storage();
    let ana = device(storage.clone(), 1, member(1), store(1)).await;
    let copy = ana
        .db
        .prepare_key_upload(|_| Ok::<_, coven_database::DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    storage
        .create(&copy.path(store(1)), &copy.bytes)
        .await
        .unwrap();
    let fault = CopyStorage::new(
        storage.clone(),
        vec![copy.path(store(1))],
        CopyAttempt::FailComparison,
    );
    assert!(
        matches!(object::upload_keys(store(1), Some(&fault), &ana.db, &ana.send).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Network)
    );
    assert_eq!(ana.db.key_uploads().await.unwrap(), [copy]);
    object::upload_keys(store(1), Some(storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_cleanup_failure_is_reported_without_confirming_the_copy() {
    let storage = storage();
    let ana = device(storage.clone(), 1, member(1), store(1)).await;
    let copy = ana
        .db
        .prepare_key_upload(|_| Ok::<_, coven_database::DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    storage
        .create(&copy.path(store(1)), &copy.bytes)
        .await
        .unwrap();
    storage
        .set_faults(Faults {
            fail_duplicate_cleanup: true,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        object::upload_keys(store(1), Some(storage.as_ref()), &ana.db, &ana.send).await,
        Err(SyncError::Storage(StorageError::Cleanup { .. }))
    ));
    assert_eq!(
        ana.db.key_uploads().await.unwrap(),
        std::slice::from_ref(&copy)
    );
    storage.set_faults(Faults::none()).await;
    object::upload_keys(store(1), Some(storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
}

#[tokio::test]
async fn partially_confirmed_prerequisites_resume_before_their_entry() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    ana.add(&member(3), MemberRole::Member).await;
    let second = ObjectPath::key_copy(store(1), DeviceId(1), 5.try_into().unwrap());
    ana.sync.storage = Some(failing_creation(storage.clone(), second, false));
    let key_hash = ana.sync.test_key(Audience::Store, key(2));
    assert!(matches!(
        ana.sync
            .make_and_upload_entry(StoreChange::RotateKey {
                audience: Audience::Store,
                key: key(2),
                key_hash,
            })
            .await,
        Err(SyncError::Storage(_))
    ));
    let upload = ana.db.local_store_log().await.unwrap().upload.unwrap();
    let pending = ana.db.key_uploads().await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|copy| copy.number.get())
            .collect::<Vec<_>>(),
        [5, 6]
    );
    assert!(pending
        .iter()
        .all(|copy| copy.before_entry == Some(upload.entry.position)));
    let confirmed = ObjectPath::key_copy(store(1), DeviceId(1), 4.try_into().unwrap());
    let confirmed_bytes = storage.read(&confirmed).await.unwrap();
    let copies = ana.send.observation().unwrap();
    assert!(
        copies.iter().any(|copy| copy.path == confirmed),
        "a later failed copy cannot hide a confirmed local exposure"
    );
    let entry_path = object::path(store(1), upload.entry.position);
    assert_eq!(
        storage.read(&entry_path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    ana.restart(storage.clone()).await;
    assert_eq!(ana.db.key_uploads().await.unwrap(), pending);
    ana.sync().await;
    assert_eq!(storage.read(&confirmed).await.unwrap(), confirmed_bytes);
    for copy in pending {
        assert_eq!(
            storage.read(&copy.path(store(1))).await.unwrap(),
            copy.bytes
        );
    }
    assert_eq!(
        storage.read(&entry_path).await.unwrap(),
        ana.reseal(&upload)
    );
    assert!(ana.db.key_uploads().await.unwrap().is_empty());
    assert!(ana.db.local_store_log().await.unwrap().upload.is_none());
    ana.add(&member(4), MemberRole::Member).await;
    let mut numbers: Vec<_> = storage
        .list(&ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path.key_copy_position().unwrap().1.get())
        .collect();
    numbers.sort();
    assert_eq!(numbers, (1..=8).collect::<Vec<_>>());
}

#[tokio::test]
async fn creation_recovers_after_copy_confirmation_without_resealing_the_copy() {
    for lost_reply in [false, true] {
        let storage = storage();
        let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
        let entry_path = ObjectPath::store_log(store(1), DeviceId(1), 1.try_into().unwrap());
        ana.sync.storage = Some(failing_creation(
            storage.clone(),
            entry_path.clone(),
            lost_reply,
        ));
        let key_hash = ana.sync.test_key(Audience::Store, key(1));
        assert!(matches!(
            ana.sync
                .make_and_upload_entry(StoreChange::CreateStore {
                    store: store(1),
                    name: ana.directory.settings().unwrap().name,
                    admin: public(&ana.member),
                    access: coven_format::MemberAccess::S3AccessKey {
                        access_key_id: "owner".into(),
                        principal: coven_format::S3Principal::Console,
                    },
                    device_name: "Ana".into(),
                    key: key(1),
                    key_hash,
                })
                .await,
            Err(SyncError::Storage(_))
        ));
        let upload = ana.db.local_store_log().await.unwrap().upload.unwrap();
        let expected = ana.reseal(&upload);
        assert!(upload.sealing.keys.is_empty());
        assert!(ana.db.key_uploads().await.unwrap().is_empty());
        let copy_path = ObjectPath::key_copy(store(1), DeviceId(1), 1.try_into().unwrap());
        let copy = storage.read(&copy_path).await.unwrap();
        ana.custody.forget().unwrap();
        ana.restart(storage.clone()).await;
        ana.sync().await;
        assert_eq!(storage.read(&copy_path).await.unwrap(), copy);
        assert_eq!(storage.read(&entry_path).await.unwrap(), expected);
        assert_eq!(
            ana.custody
                .read()
                .unwrap()
                .unwrap()
                .store_key(key(1))
                .unwrap()
                .hash(),
            key_hash
        );
        ana.add(&member(2), MemberRole::Member).await;
        assert!(storage
            .read(&ObjectPath::key_copy(
                store(1),
                DeviceId(1),
                2.try_into().unwrap()
            ))
            .await
            .is_ok());
        assert_eq!(
            storage
                .list(&ObjectPrefix::keys(store(1)))
                .await
                .unwrap()
                .len(),
            2
        );
    }
}

/// A removed member returns only with a fresh pair (§9). The refused return
/// with the old pair seals nothing, so its historical copy reserves no number:
/// the fresh pair's copy takes the next number, 3.
#[tokio::test]
async fn a_member_returns_with_a_fresh_pair_and_historical_copies_reserve_no_numbers() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&member(2), MemberRole::Member).await;
    let before = storage.list(&ObjectPrefix::keys(store(1))).await.unwrap();
    // `replay::tests` numbers member(2)'s id 1.
    use crate::{effects::tests::add_with, replay::tests::remove};
    use coven_database::DropReason;
    ana.sync.make_and_upload_entry(remove(1)).await.unwrap();
    let old = add_with(1, public(&member(2)));
    assert!(matches!(
        ana.sync.make_and_upload_entry(old).await,
        Err(SyncError::Rejected(DropReason::KeysAlreadyUsed))
    ));
    let keys = storage.list(&ObjectPrefix::keys(store(1))).await.unwrap();
    assert_eq!(keys, before);
    let fresh = crate::effects::tests::addition_keys(&ana.log().await, &member(2));
    assert_ne!(fresh, public(&member(2)));
    ana.sync
        .make_and_upload_entry(add_with(1, fresh))
        .await
        .unwrap();
    let copy = ObjectPath::key_copy(store(1), DeviceId(1), 3.try_into().unwrap());
    assert!(storage.read(&copy).await.is_ok());
}

#[tokio::test]
async fn circle_rotations_share_only_with_current_circle_members() {
    let storage = storage();
    let [mut ana, mut ben, dan, mut erin] = household(&storage).await;
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Shared".into(),
            key: key(10),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(10)),
        })
        .await
        .unwrap();
    for who in [&ben.member, &dan.member, &erin.member] {
        ana.sync
            .make_and_upload_entry(StoreChange::AddCircleMember {
                circle: circle(1),
                member: who.member(),
            })
            .await
            .unwrap();
    }
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveCircleMember {
            circle: circle(1),
            member: dan.member.member(),
        })
        .await
        .unwrap();
    ben.sync().await;
    let rotation = ben
        .sync
        .test_rotate(Audience::Circle(circle(1)), key(11))
        .await;
    let missing = copy_path(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &erin.member.member(),
    )
    .await;
    storage.delete(&missing).await.unwrap();
    ana.sync().await;
    erin.sync().await;
    assert_eq!(
        erin.custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle(1), key(11))
            .unwrap()
            .hash(),
        ben.custody
            .read()
            .unwrap()
            .unwrap()
            .circle_key(circle(1), key(11))
            .unwrap()
            .hash()
    );
    assert_eq!(
        erin.log().await.replay.entries[&rotation],
        EntryOutcome::Kept
    );
    assert!(find_copy(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &dan.member.member()
    )
    .await
    .is_none());
    ana.sync
        .make_and_upload_entry(StoreChange::DeleteCircle { circle: circle(1) })
        .await
        .unwrap();
    let copy = copy_path(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &erin.member.member(),
    )
    .await;
    storage.delete(&copy).await.unwrap();
    ana.sync().await;
    assert!(find_copy(
        storage.as_ref(),
        Audience::Circle(circle(1)),
        key(11),
        &erin.member.member()
    )
    .await
    .is_none());
}
