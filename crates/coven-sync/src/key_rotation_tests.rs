//! First attempts use only unexposed keys; an exposed key is replaced (§11, C12).

use super::*;
use crate::store_log_sync::tests::Device;
use crate::store_log_sync::tests::{device, find_copy, key, member, storage, store};
use coven_database::EntryOutcome;
use coven_format::store_log::MemberRole;

/// A fresh complete observation, as a new pass would make.
async fn observed(device: &Device) -> crate::key_introductions::CopyObservation {
    keys::observe(
        &device.db,
        &crate::pass_reads::PassReads::default(),
        std::num::NonZeroUsize::MIN,
        device.storage.as_ref(),
        store(1),
    )
    .await
    .unwrap()
}

async fn selected(device: &Device) -> Result<KeyId, SyncError> {
    let ring = device.custody.read().unwrap().unwrap();
    crate::write_seal::first_attempt(
        &device.log().await,
        &observed(device).await,
        &ring,
        &device.member.member_id(),
        &Audience::Store,
    )
}

#[tokio::test]
async fn a_forged_copy_to_an_outsider_exposes_the_key_and_costs_one_rotation() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    let dan = member(4);
    ana.create(key(1)).await;
    ana.add(&ben.member, MemberRole::Member).await;
    ben.sync().await;
    ben.sync.load_new_device("Ben").await.unwrap();
    ana.sync().await;
    assert_eq!(selected(&ana).await.unwrap(), key(1));
    // Ben publishes a box naming the store key for Dan, who is not a member.
    // Nobody but Dan can open it, so it counts whatever it holds.
    let (dan_id, dan_sealing) = (dan.member_id(), dan.sealing_public_key());
    ben.db
        .prepare_key_upload(move |path| {
            keys::store_copy(
                path,
                &coven_crypto::StoreKey::generate(key(1)).unwrap(),
                &dan_id,
                &dan_sealing,
            )
        })
        .await
        .unwrap();
    object::upload_keys(store(1), Some(ben.storage.as_ref()), &ben.db)
        .await
        .unwrap();
    assert!(matches!(
        selected(&ana).await,
        Err(SyncError::KeyUnavailable(id)) if id == key(1)
    ));
    // Phase 2 publishes one rotation; the fresh key reached only members.
    ana.sync.sync_store_log().await.unwrap();
    ana.sync.rotate_exposed_keys().await.unwrap();
    let fresh = selected(&ana).await.unwrap();
    assert_ne!(fresh, key(1));
    let log = ana.log().await;
    let rotation = log.entries.last().unwrap();
    assert!(matches!(
        &rotation.entry.change,
        StoreChange::RotateKey { audience: Audience::Store, key, .. } if *key == fresh
    ));
    assert_eq!(
        log.replay.entries[&rotation.entry.position],
        EntryOutcome::Kept
    );
    for (who, has) in [(&ana.member, true), (&ben.member, true), (&dan, false)] {
        assert_eq!(
            find_copy(storage.as_ref(), Audience::Store, fresh, &who.member_id())
                .await
                .is_some(),
            has
        );
    }
    // Another pass keeps the replacement: the old copies stay listed but no
    // longer require a rotation.
    ana.sync.rotate_exposed_keys().await.unwrap();
    assert_eq!(ana.log().await.entries.len(), log.entries.len());
    // Old keys still decrypt: Ben reads entries sealed with either key.
    ben.sync().await;
    assert_eq!(ben.log().await, ana.log().await);
    let ring = ben.custody.read().unwrap().unwrap();
    assert!(ring.store_key(key(1)).is_ok() && ring.store_key(fresh).is_ok());
    assert_eq!(selected(&ben).await.unwrap(), fresh);
    let mut restored = device(storage.clone(), 3, member(2), store(1)).await;
    restored.sync().await;
    assert_eq!(restored.log().await, ana.log().await);
}

#[tokio::test]
async fn an_incomplete_copy_observation_blocks_first_attempts() {
    let storage = storage();
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    ana.create(key(1)).await;
    let ring = ana.custody.read().unwrap().unwrap();
    let damage = crate::DamagedObject {
        path: "keys/1/9".into(),
        failure: Refusal::Parse { cause: None },
    };
    let blocked = crate::write_seal::first_attempt(
        &ana.log().await,
        &crate::key_introductions::CopyObservation::Incomplete(damage),
        &ring,
        &ana.member.member_id(),
        &Audience::Store,
    );
    assert!(matches!(blocked, Err(SyncError::Damaged(d)) if d.path == "keys/1/9"));
    // An undecodable copy in a known writer's log leaves an observation
    // incomplete.
    let writer = ana.db.local_store_log().await.unwrap().device;
    let next = storage
        .list(&coven_storage::ObjectPrefix::keys(store(1)))
        .await
        .unwrap()
        .len() as u64
        + 1;
    storage
        .create(
            &coven_storage::ObjectPath::key_copy(store(1), writer, next.try_into().unwrap()),
            b"not a sealed key",
        )
        .await
        .unwrap();
    assert!(matches!(
        observed(&ana).await,
        crate::key_introductions::CopyObservation::Incomplete(_)
    ));
    ana.sync.rotate_exposed_keys().await.unwrap();
    assert_eq!(
        ana.log().await.entries.len(),
        1,
        "no rotation without an observation"
    );
}
