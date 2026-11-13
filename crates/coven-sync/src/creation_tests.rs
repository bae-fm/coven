use crate::sequence_sync::tests::{Device, Group};
use crate::SyncError;

/// Decide Ana's slot 1 in her database, as creating the store does, and let
/// her owner see it.
pub(crate) async fn decide_for(group: &Group, ana: &Device) -> Result<(), SyncError> {
    ana.sync.decide_creation("sequence tests", "Ana", &group.ring).await
}

use crate::sequence_sync::tests::insert;
use coven_storage::{test_utils::Faults, ObjectPath, Storage};

/// Slot 1's bytes in storage.
async fn stored_slot_1(group: &Group) -> Option<Vec<u8>> {
    let path = ObjectPath::slot(group.store, std::num::NonZeroU64::MIN);
    match group.storage.read(&path).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.failure() == coven_storage::StorageFailure::NotFound => None,
        Err(error) => panic!("{error:?}"),
    }
}

/// The exact bytes the device kept as its attempt at slot 1.
async fn kept_attempt(ana: &Device) -> Vec<u8> {
    ana.database()
        .read_current_attempt::<_, SyncError, _>(|chunks| {
            let mut kept = Vec::new();
            for chunk in chunks {
                kept.extend(chunk?);
            }
            Ok(kept)
        })
        .await
        .unwrap()
}

/// Setup sends slot 1 exactly as creation decided it, and its history is the
/// creation alone until Set up storage claims the next slot (§10.3).
#[tokio::test]
async fn publishing_a_creation_sends_the_kept_bytes() {
    let group = Group::new();
    let ana = group.device(1).await;
    decide_for(&group, &ana).await.unwrap();
    assert_eq!(ana.database().applied_slot().await.unwrap(), 1);
    assert_eq!(stored_slot_1(&group).await, None);
    let kept = kept_attempt(&ana).await;

    group.storage.claim_location(group.store).await.unwrap();
    ana.sync.publish_creation().await.unwrap();
    assert_eq!(stored_slot_1(&group).await, Some(kept));
    assert!(ana.database().current_claim().await.unwrap().is_none());
    // Publishing again, with the operation ended, sends nothing more.
    ana.sync.publish_creation().await.unwrap();
    assert_eq!(ana.database().applied_slot().await.unwrap(), 1);
    ana.database().close().await.unwrap();
}

/// Slot 1 is stored but its reply is lost: the retry reads the slot, finds
/// the kept bytes and settles the claim as won, with no second slot.
#[tokio::test]
async fn a_lost_reply_on_slot_1_settles_as_won_on_retry() {
    let group = Group::new();
    let ana = group.device(1).await;
    decide_for(&group, &ana).await.unwrap();
    let kept = kept_attempt(&ana).await;
    group.storage.claim_location(group.store).await.unwrap();
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    assert!(ana.sync.publish_creation().await.is_err());
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.publish_creation().await.unwrap();
    assert_eq!(stored_slot_1(&group).await, Some(kept));
    assert_eq!(ana.database().applied_slot().await.unwrap(), 1);
    ana.database().close().await.unwrap();
}

/// A copy of Ana's installation decides its own slot 1 and sends it first:
/// the original finds its device id on other bytes and stops (§19.3).
#[tokio::test]
async fn a_copy_of_the_device_sending_its_own_slot_1_first_stops_the_original() {
    let group = Group::new();
    let ana = group.device(1).await;
    let (ana, copy) = group.copy(ana).await;
    decide_for(&group, &ana).await.unwrap();
    decide_for(&group, &copy).await.unwrap();
    group.storage.claim_location(group.store).await.unwrap();
    copy.sync.publish_creation().await.unwrap();

    let result = ana.sync.publish_creation().await;
    assert!(matches!(result, Err(SyncError::DeviceIdInUse(_))), "{result:?}");
    for device in [&ana, &copy] {
        device.database().close().await.unwrap();
    }
}

/// Slot 1's clear prefix carries the creator's copy of the first store key,
/// and a creation whose copy names another member or key is refused when it
/// is read.
#[tokio::test]
async fn a_creation_slot_carries_only_its_creators_copy_of_the_first_key() {
    let group = Group::new();
    let ana = group.device(1).await;
    decide_for(&group, &ana).await.unwrap();
    insert(ana.database(), "a", "kept").await;
    let kept = kept_attempt(&ana).await;
    let received = coven_format::sealed_slot::BoundedSlot::decode(&kept).unwrap();
    let coven_format::sealed_slot::SlotKind::Admission(copies) = &received.prefix.kind else {
        panic!("slot 1 is an admission");
    };
    assert_eq!(copies.len(), 1);
    assert_eq!(copies[0].routing.member, ana.identity.member());
    assert_eq!(copies[0].routing.sealing, ana.identity.keys().public_keys().sealing);
    assert_eq!(copies[0].routing.audience, coven_merge::Audience::Store);
    assert_eq!(copies[0].routing.key, group.ring.store_key_ids().next().unwrap());

    // Another member's copy in the same position is refused by validation.
    let history = crate::history::History::default();
    let change = coven_format::admin::StoreChange::CreateStore {
        store: group.store,
        name: "n".into(),
        admin: ana.identity.keys().public_keys(),
        key: copies[0].routing.key,
        key_hash: group.ring.store_key(copies[0].routing.key).unwrap().hash(),
        device_name: "d".into(),
    };
    let header = coven_format::sequence::SlotHeader {
        device: ana.device_id(),
        claim: 1,
        author: ana.identity.member(),
        after: 0,
    };
    let mut wrong = copies.clone();
    wrong[0].routing.member = coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(77));
    assert!(history
        .validate_admission(&header, coven_merge::Slot::FIRST, &change, &wrong)
        .is_err());
    assert!(history.validate_admission(&header, coven_merge::Slot::FIRST, &change, copies).is_ok());
    ana.database().close().await.unwrap();
}
