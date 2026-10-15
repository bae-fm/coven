//! One bad key copy is refused by itself; unlock needs only a usable store key (§11, E9).

use super::*;
use crate::store_log_sync::tests::{
    circle, device, find_copy, key, member, storage, store, Device,
};
use coven_format::store_log::MemberRole;
use coven_storage::test_utils::MemoryStorage;

/// Ana's store with the Gifts circle; Ben is a member outside Gifts.
async fn household(storage: &Arc<MemoryStorage>) -> (Device, Device) {
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let ben = device(storage.clone(), 2, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ana.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: circle(1),
            name: "Gifts".into(),
            key: key(2),
            key_hash: ana.sync.test_key(Audience::Circle(circle(1)), key(2)),
        })
        .await
        .unwrap();
    (ana, ben)
}

/// Ana publishes a copy for Ben of material that is not the committed key.
async fn forge_copy(ana: &Device, ben: &Device, audience: Audience, id: KeyId) {
    let recipient = ben.member.member();
    let sealing = ben.member.keys().public_keys().sealing;
    ana.db
        .prepare_key_upload(move |path| match audience {
            Audience::Store => keys::store_copy(
                path,
                &coven_crypto::StoreKey::generate(id).unwrap(),
                &recipient,
                &sealing,
            ),
            Audience::Circle(c) => keys::circle_copy(
                path,
                &coven_crypto::CircleKey::generate(c, id).unwrap(),
                &recipient,
                &sealing,
            ),
        })
        .await
        .unwrap();
    object::upload_keys(store(1), Some(ana.storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
}

async fn key_copy_record(device: &Device, audience: Audience, id: KeyId) -> Option<PendingReason> {
    device
        .db
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .find(|record| {
            record.subject
                == PendingSubject::KeyCopy {
                    audience: audience.clone(),
                    key: id,
                    member: device.member.member(),
                    sealing: device.member.keys().public_keys().sealing,
                }
        })
        .map(|record| record.reason)
}

#[tokio::test]
async fn a_usable_store_key_unlocks_despite_a_bad_unrelated_copy() {
    let storage = storage();
    let (ana, mut ben) = household(&storage).await;
    let gifts = Audience::Circle(circle(1));
    forge_copy(&ana, &ben, gifts.clone(), key(2)).await;
    ben.sync.unlock_storage(storage.clone()).await.unwrap();
    let ring = ben.custody.read().unwrap();
    assert!(keys::holds(ring.as_ref(), &Audience::Store, key(1)));
    assert!(!keys::holds(ring.as_ref(), &gifts, key(2)));
    assert!(matches!(
        key_copy_record(&ben, gifts, key(2)).await,
        Some(PendingReason::Refused(Refusal::WrongIdentity { .. }))
    ));
    // The circle Ben is not in leaves no wait for a copy he will never receive.
    assert!(key_copy_record(&ben, Audience::Store, key(1))
        .await
        .is_none());
}

#[tokio::test]
async fn a_later_valid_copy_supplies_the_key_and_clears_the_refusal() {
    let storage = storage();
    let (mut ana, mut ben) = household(&storage).await;
    let gifts = Audience::Circle(circle(1));
    forge_copy(&ana, &ben, gifts.clone(), key(2)).await;
    ben.sync.unlock_storage(storage.clone()).await.unwrap();
    assert!(key_copy_record(&ben, gifts.clone(), key(2)).await.is_some());
    // Ben joins Gifts. The forged copy already names him, so Ana publishes the
    // genuine key as another supplier would, after it.
    ana.sync
        .make_and_upload_entry(StoreChange::AddCircleMember {
            circle: circle(1),
            member: ben.member.member(),
        })
        .await
        .unwrap();
    let genuine = ana
        .custody
        .read()
        .unwrap()
        .unwrap()
        .circle_key(circle(1), key(2))
        .unwrap()
        .clone();
    let (recipient, sealing) = (ben.member.member(), ben.member.keys().public_keys().sealing);
    ana.db
        .prepare_key_upload(move |path| keys::circle_copy(path, &genuine, &recipient, &sealing))
        .await
        .unwrap();
    object::upload_keys(store(1), Some(ana.storage.as_ref()), &ana.db, &ana.send)
        .await
        .unwrap();
    ben.sync().await;
    let ring = ben.custody.read().unwrap();
    assert!(keys::holds(ring.as_ref(), &gifts, key(2)));
    assert!(key_copy_record(&ben, gifts, key(2)).await.is_none());
}

#[tokio::test]
async fn an_absent_usable_store_key_stays_pending() {
    for forged in [false, true] {
        let storage = storage();
        let (_ana, mut ben) = household(&storage).await;
        let genuine = find_copy(
            storage.as_ref(),
            Audience::Store,
            key(1),
            &ben.member.member(),
        )
        .await
        .unwrap();
        storage.delete(&genuine.path).await.unwrap();
        if forged {
            // The first arriving copy is forged; no missing earlier number
            // hides it from next-number discovery.
            let bytes = keys::store_copy(
                &genuine.path,
                &coven_crypto::StoreKey::generate(key(1)).unwrap(),
                &ben.member.member(),
                &ben.member.keys().public_keys().sealing,
            )
            .unwrap();
            storage.create(&genuine.path, &bytes).await.unwrap();
        }
        let error = ben.sync.unlock_storage(storage.clone()).await.unwrap_err();
        let SyncError::Pending(record) = error else {
            panic!("unlock must name the pending store key: {error:?}");
        };
        assert_eq!(
            record.subject,
            PendingSubject::KeyCopy {
                audience: Audience::Store,
                key: key(1),
                member: ben.member.member(),
                sealing: ben.member.keys().public_keys().sealing,
            }
        );
        if forged {
            assert!(matches!(record.reason, PendingReason::Refused(_)));
        } else {
            assert_eq!(
                record.reason,
                PendingReason::KeyUnavailable {
                    audience: Audience::Store,
                    key: key(1),
                }
            );
        }
        assert_eq!(
            key_copy_record(&ben, Audience::Store, key(1)).await,
            Some(record.reason)
        );
        assert!(ben.custody.read().unwrap().is_none());
    }
}

#[tokio::test]
async fn a_copy_to_another_sealing_key_of_this_member_is_skipped_unopened() {
    let storage = storage();
    let (ana, mut ben) = household(&storage).await;
    let genuine = find_copy(
        storage.as_ref(),
        Audience::Store,
        key(1),
        &ben.member.member(),
    )
    .await
    .unwrap();
    storage.delete(&genuine.path).await.unwrap();
    // Ben's id at a sealing key he does not hold, carrying the genuine key.
    let mut seeds = ben.member.keys().to_secret_bytes().as_bytes().to_vec();
    seeds[37..69].fill(0x77);
    let other = MemberIdentity::new(
        ben.member.member(),
        coven_crypto::MemberKeys::from_secret_bytes(&seeds).unwrap(),
    );
    assert_eq!(other.member(), ben.member.member());
    let ring = ana.custody.read().unwrap().unwrap();
    let bytes = keys::store_copy(
        &genuine.path,
        ring.store_key(key(1)).unwrap(),
        &ben.member.member(),
        &other.keys().public_keys().sealing,
    )
    .unwrap();
    storage.create(&genuine.path, &bytes).await.unwrap();
    let error = ben.sync.unlock_storage(storage.clone()).await.unwrap_err();
    let SyncError::Pending(record) = error else {
        panic!("unlock must name the pending store key: {error:?}");
    };
    // Not a refusal: the copy names another credential, so Ben never opens it.
    assert_eq!(
        record.reason,
        PendingReason::KeyUnavailable {
            audience: Audience::Store,
            key: key(1),
        }
    );
    assert!(ben.custody.read().unwrap().is_none());
}

#[tokio::test]
async fn recovering_a_creation_from_a_refused_copy_is_that_copys_record() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let entry = ObjectPath::store_log(
        store(1),
        coven_foundation::id_source::DeviceId(1),
        1.try_into().unwrap(),
    );
    ana.sync.storage = Some(
        crate::store_log_sync::tests::key_distribution::failing_creation(
            storage.clone(),
            entry,
            false,
        ),
    );
    let key_hash = ana.sync.test_key(Audience::Store, key(1));
    assert!(matches!(
        ana.sync
            .make_and_upload_entry(StoreChange::CreateStore {
                store: store(1),
                name: "Store".into(),
                admin: crate::store_log_sync::tests::public(&ana.member),
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
    // The only copy of the creation's key fails to open after a restart that
    // lost the key from custody.
    let copy = ObjectPath::key_copy(
        store(1),
        coven_foundation::id_source::DeviceId(1),
        1.try_into().unwrap(),
    );
    let size = storage.read(&copy).await.unwrap().len();
    storage.corrupt_byte(&copy, size - 1).await.unwrap();
    ana.custody.forget().unwrap();
    ana.restart(storage.clone()).await;
    let error = ana.sync.sync_store_log().await.unwrap_err();
    let SyncError::Pending(record) = error else {
        panic!("recovery must name the refused key copy: {error:?}");
    };
    assert_eq!(
        record.subject,
        PendingSubject::KeyCopy {
            audience: Audience::Store,
            key: key(1),
            member: ana.member.member(),
            sealing: ana.member.keys().public_keys().sealing,
        }
    );
    assert!(matches!(
        record.reason,
        PendingReason::Refused(Refusal::Decryption { .. })
    ));
    assert_eq!(
        key_copy_record(&ana, Audience::Store, key(1)).await,
        Some(record.reason)
    );
    assert!(ana.db.local_store_log().await.unwrap().upload.is_some());
}

/// D-11-1: someone with storage access plants an entry claiming the id of
/// Ana's next store key, sealed with chosen bytes, and a copy of those bytes
/// for Ben. The entry fails its checks, so its bytes never become that key in
/// Ben's custody, and Ana's genuine rotation and the entry after it, sealed
/// with the new key, still apply for Ben in one scan (§11).
#[tokio::test]
async fn a_forged_introduction_cannot_plant_its_key() {
    let storage = storage();
    let (mut ana, mut ben) = household(&storage).await;
    ben.sync().await;
    // Ana's rotation to key 3: its copies are stored, its entry is not yet.
    let own = ana.device().await;
    let next = ana
        .log()
        .await
        .entries
        .iter()
        .filter(|applied| applied.entry.position.device == own)
        .count() as u64
        + 1;
    ana.interrupt_entry(next).await;
    let key_hash = ana.sync.test_key(Audience::Store, key(3));
    assert!(ana
        .sync
        .make_and_upload_entry(StoreChange::RotateKey {
            audience: Audience::Store,
            key: key(3),
            key_hash,
        })
        .await
        .is_err());
    ana.restore_storage();
    let thief = coven_foundation::id_source::DeviceId(9);
    let chosen = coven_crypto::StoreKey::generate(key(3)).unwrap();
    let copy = ObjectPath::key_copy(store(1), thief, 1.try_into().unwrap());
    let sealed = keys::store_copy(
        &copy,
        &chosen,
        &ben.member.member(),
        &ben.member.keys().public_keys().sealing,
    )
    .unwrap();
    storage.create(&copy, &sealed).await.unwrap();
    let forged = StoreLogEntry {
        position: EntryId {
            device: thief,
            number: 1,
        },
        timestamp: coven_merge::Timestamp::new(5000, 0, thief).unwrap(),
        author: member(9).member(),
        had_read: ben.log().await.positions(),
        change: StoreChange::RotateKey {
            audience: Audience::Store,
            key: key(3),
            key_hash: chosen.hash(),
        },
    };
    let path = object::path(store(1), forged.position);
    let bytes = object::seal(
        store(1),
        &forged,
        coven_format::FormatVersion::CURRENT,
        &chosen,
        &member(9),
    )
    .unwrap();
    storage.create(&path, &bytes).await.unwrap();
    ben.sync.scan_store_log_once(false).await.unwrap();
    assert!(!keys::holds(
        ben.custody.read().unwrap().as_ref(),
        &Audience::Store,
        key(3)
    ));
    // The refused entry would also stop Ana's own catch-up; that is another
    // matter, so storage loses it before she publishes.
    storage.delete(&path).await.unwrap();
    ana.sync().await;
    let after = ana.add(&member(3), MemberRole::Member).await;
    ben.sync.scan_store_log_once(false).await.unwrap();
    let ring = ben.custody.read().unwrap().unwrap();
    assert_eq!(ring.store_key(key(3)).unwrap().hash(), key_hash);
    assert!(ben.log().await.replay.entries.contains_key(&after));
}

/// An entry sealed with a candidate whose introduction fails its checks does
/// not apply, even when its own author and signature check out: an unchecked
/// candidate authorizes no other data (§11, `KeyPhases.admitsDependent`).
#[tokio::test]
async fn an_unchecked_candidate_opens_no_applied_entry() {
    let storage = storage();
    let (mut ana, mut ben) = household(&storage).await;
    let mut carol = device(storage.clone(), 9, member(3), store(1)).await;
    ana.add(&carol.member, MemberRole::Member).await;
    carol.sync().await;
    carol.sync.load_new_device("Carol", None).await.unwrap();
    ben.sync().await;
    // Writer 5, listed before Carol's 9, introduces chosen bytes as key 3 and
    // seals Ben a copy. Carol's next entry is sealed with the same bytes.
    let thief = coven_foundation::id_source::DeviceId(5);
    let chosen = coven_crypto::StoreKey::generate(key(3)).unwrap();
    let copy = ObjectPath::key_copy(store(1), thief, 1.try_into().unwrap());
    let sealed = keys::store_copy(
        &copy,
        &chosen,
        &ben.member.member(),
        &ben.member.keys().public_keys().sealing,
    )
    .unwrap();
    storage.create(&copy, &sealed).await.unwrap();
    let positions = ben.log().await.positions();
    let introduction = StoreLogEntry {
        position: EntryId {
            device: thief,
            number: 1,
        },
        timestamp: coven_merge::Timestamp::new(5000, 0, thief).unwrap(),
        author: member(5).member(),
        had_read: positions.clone(),
        change: StoreChange::RotateKey {
            audience: Audience::Store,
            key: key(3),
            key_hash: chosen.hash(),
        },
    };
    let own = carol.device().await;
    let dependent = StoreLogEntry {
        position: EntryId {
            device: own,
            number: 2,
        },
        timestamp: coven_merge::Timestamp::new(6000, 0, own).unwrap(),
        author: carol.member.member(),
        // Its own earlier entries are implicit.
        had_read: coven_format::value::EntryPositions(
            positions
                .0
                .into_iter()
                .filter(|id| id.device != own)
                .collect(),
        ),
        change: StoreChange::SetAccess {
            member: carol.member.member(),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "carol".into(),
                principal: coven_format::S3Principal::Console,
            },
        },
    };
    for (entry, signer) in [(&introduction, &member(5)), (&dependent, &carol.member)] {
        let bytes = object::seal(
            store(1),
            entry,
            coven_format::FormatVersion::CURRENT,
            &chosen,
            signer,
        )
        .unwrap();
        storage
            .create(&object::path(store(1), entry.position), &bytes)
            .await
            .unwrap();
    }
    ben.sync.scan_store_log_once(false).await.unwrap();
    assert!(!ben
        .log()
        .await
        .replay
        .entries
        .contains_key(&dependent.position));
    assert!(!keys::holds(
        ben.custody.read().unwrap().as_ref(),
        &Audience::Store,
        key(3)
    ));
    let pending = ben.db.pending::<PendingRecord>().await.unwrap();
    assert!(pending.iter().any(|record| {
        record.subject == PendingSubject::Entry(dependent.position)
            && record.reason
                == PendingReason::KeyUnavailable {
                    audience: Audience::Store,
                    key: key(3),
                }
    }));
}
