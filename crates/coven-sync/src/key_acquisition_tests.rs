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
    let recipient = ben.member.member_id();
    let sealing = ben.member.sealing_public_key();
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
    object::upload_keys(store(1), Some(ana.storage.as_ref()), &ana.db)
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
                    member: device.member.member_id(),
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
            member: ben.member.member_id(),
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
    let (recipient, sealing) = (ben.member.member_id(), ben.member.sealing_public_key());
    ana.db
        .prepare_key_upload(move |path| keys::circle_copy(path, &genuine, &recipient, &sealing))
        .await
        .unwrap();
    object::upload_keys(store(1), Some(ana.storage.as_ref()), &ana.db)
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
            &ben.member.member_id(),
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
                &ben.member.member_id(),
                &ben.member.sealing_public_key(),
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
                member: ben.member.member_id(),
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
        &ben.member.member_id(),
    )
    .await
    .unwrap();
    storage.delete(&genuine.path).await.unwrap();
    // Ben's id at a sealing key he does not hold, carrying the genuine key.
    let mut seeds = ben.member.to_secret_bytes().as_bytes().to_vec();
    seeds[37..69].fill(0x77);
    let other = MemberKeys::from_secret_bytes(&seeds).unwrap();
    assert_eq!(other.member_id(), ben.member.member_id());
    let ring = ana.custody.read().unwrap().unwrap();
    let bytes = keys::store_copy(
        &genuine.path,
        ring.store_key(key(1)).unwrap(),
        &ben.member.member_id(),
        &other.sealing_public_key(),
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
