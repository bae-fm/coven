use super::*;
use crate::{history::tests::Projection, history::History, sequence_sync::tests::*};
use coven_crypto::StoreKeyring;
use coven_format::sealed_slot::{BoundedSlot, SlotKind, SlotObjectPrefix};
use coven_merge::Audience;
use serde_json::json;

async fn compare_rotation(
    group: &Group,
    ana: &Device,
    recipients: &[&MemberIdentity],
    audience: Audience,
) {
    let (before, through) = {
        let session = ana.sync.session.lock().await;
        (session.history.clone(), session.through)
    };
    let old_ring = ana.sync.keys.read().unwrap().unwrap();
    let outer = before.metadata_key(&old_ring).unwrap();
    ana.sync.rotate_key(audience.clone()).await.unwrap();
    let ring = ana.sync.keys.read().unwrap().unwrap();
    let session = ana.sync.session.lock().await;
    let after = &session.history;
    let introduced = after.introductions.last().unwrap();
    assert_eq!(introduced.audience, audience);
    assert!(!after.exposed(&audience, introduced.key));
    assert_eq!(
        after
            .sending_key(&ring, ana.identity.member(), &audience)
            .unwrap(),
        introduced.key
    );
    for i in &before.introductions {
        assert_eq!(
            after.accepted(&ring, &i.audience, i.key),
            before.accepted(&old_ring, &i.audience, i.key)
        );
    }

    // The operation has installed generated material before claiming copies.
    // Supplying that custody to the preceding prefix still cannot accept it.
    let projection = Projection::new(after, &ring);
    let mut input = projection.input(&before, &ring, ana.identity.member(), through);
    input["kind"] = json!("rotation");
    let mut rotation = projection.introduction(introduced);
    rotation["members"] = json!(before
        .state
        .members
        .keys()
        .map(|m| projection.member(*m))
        .collect::<Vec<_>>());
    input["rotation"] = rotation;
    let actual_slots = projection.slots(after, session.through);
    let expected = json!({"slots":actual_slots[usize::try_from(through).unwrap()..],
        "through":session.through, "after":projection.result(after, &ring, ana.identity.member())});
    assert_eq!(
        crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
        expected
    );

    let originals = ana.sync.database.original_slots().await.unwrap();
    for original in originals.iter().filter(|o| o.slot.0 > through) {
        let bounded = BoundedSlot::decode(&original.object).unwrap();
        assert_eq!(
            bounded.prefix.key, outer,
            "rotation never needs an unheld outer key"
        );
    }
    for saved in after
        .copies
        .iter()
        .filter(|c| c.path.slot_number().unwrap().0 > through)
    {
        let SlotKind::KeyCopy(copy) = &saved.prefix.kind else {
            panic!("copy");
        };
        let recipient = recipients
            .iter()
            .find(|m| m.member() == copy.routing.member)
            .unwrap();
        let context =
            SlotObjectPrefix::copy_context(saved.prefix.format, saved.prefix.key, &copy.routing)
                .unwrap();
        let hash = match audience {
            Audience::Store => copy
                .open_store_key(recipient, saved.path.as_str(), &context)
                .unwrap()
                .hash(),
            Audience::Circle(_) => copy
                .open_circle_key(recipient, saved.path.as_str(), &context)
                .unwrap()
                .hash(),
        };
        assert_eq!(hash, introduced.hash);
    }
    // Reconstruct from the database's retained encrypted originals, including
    // historical key copies and introductions, rather than an in-memory clone.
    let mut restored_ring = ring.clone();
    let restored =
        History::restore(&originals, group.store, &mut restored_ring, &ana.identity).unwrap();
    assert_eq!(
        projection.result(&restored, &restored_ring, ana.identity.member()),
        expected["after"]
    );
    assert_eq!(projection.slots(&restored, session.through), actual_slots);
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN"]
async fn sequence_rotation_matches_lean() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let circle = ana.sync.create_circle("Circle".into()).await.unwrap();
    for recipient in [&ben, &carol] {
        ana.sync
            .administer(StoreChange::AddCircleMember {
                circle,
                member: recipient.identity.member(),
            })
            .await
            .unwrap();
    }
    ana.sync
        .administer(StoreChange::RemoveMember {
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    {
        let session = ana.sync.session.lock().await;
        let ring = ana.sync.keys.read().unwrap().unwrap();
        let key = session.history.metadata_key(&ring).unwrap();
        assert!(session.history.exposed(&Audience::Store, key));
        assert!(matches!(
            session
                .history
                .sending_key(&ring, ana.identity.member(), &Audience::Store),
            Err(SyncError::KeyUnavailable(_))
        ));
    }
    // Another removal can be published while every store key is exposed.
    // Device removal leaves Carol's shared sealing credential current.
    ana.sync
        .administer(StoreChange::RemoveDevice {
            device: carol.sync.device,
        })
        .await
        .unwrap();
    for audience in [
        Audience::Circle(circle),
        Audience::Store,
        Audience::Circle(circle),
    ] {
        compare_rotation(&group, &ana, &[&ana.identity, &carol.identity], audience).await;
    }
    let replacement = coven_format::test_utils::keys_from_seed(4);
    ana.sync
        .replace_member_keys_for(carol.identity.member(), replacement.public_keys())
        .await
        .unwrap();
    let carol_current = MemberIdentity::new(carol.identity.member(), replacement);
    for audience in [Audience::Circle(circle), Audience::Store] {
        compare_rotation(&group, &ana, &[&ana.identity, &carol_current], audience).await;
    }
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn rotation_does_not_need_preceding_circle_material() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = ana.sync.create_circle("Circle".into()).await.unwrap();
    let mut session = ana.sync.session.lock().await;
    let held = ana.sync.keys.read().unwrap().unwrap();
    let outer = session.history.metadata_key(&held).unwrap();
    let mut ring = StoreKeyring::new(held.store_key(outer).unwrap().clone());
    assert_eq!(ring.circle_key_ids(circle).count(), 0);
    ana.sync
        .install_key(
            &mut session,
            &mut ring,
            &ana.identity,
            Audience::Circle(circle),
            None,
        )
        .await
        .unwrap();
    let intro = session.history.introductions.last().unwrap();
    assert_eq!(intro.audience, Audience::Circle(circle));
    assert!(session.history.accepted(&ring, &intro.audience, intro.key));
    drop(session);
    ana.sync.database.close().await.unwrap();
}
