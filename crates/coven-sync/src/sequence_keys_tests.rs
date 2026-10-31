use super::*;
use crate::{history::tests::Projection, history::History, sequence_sync::tests::*};
use coven_crypto::StoreKeyring;
use coven_format::sealed_slot::{BoundedSlot, SlotKind, SlotObjectPrefix};
use coven_foundation::files::StoreDirAccess;
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
    assert_eq!(after.sending_key(&ring, ana.identity.member(), &audience).unwrap(), introduced.key);
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
    rotation["members"] =
        json!(before.state.members.keys().map(|m| projection.member(*m)).collect::<Vec<_>>());
    input["rotation"] = rotation;
    let actual_slots = projection.slots(after, session.through);
    let expected = json!({"slots":actual_slots[usize::try_from(through).unwrap()..],
        "through":session.through, "after":projection.result(after, &ring, ana.identity.member())});
    assert_eq!(crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input), expected);

    let originals = ana.sync.database.original_slots().await.unwrap();
    for original in originals.iter().filter(|o| o.slot.0 > through) {
        let bounded = BoundedSlot::decode(&original.object).unwrap();
        assert_eq!(bounded.prefix.key, outer, "rotation never needs an unheld outer key");
    }
    for saved in after.copies.iter().filter(|c| c.path.slot_number().unwrap().0 > through) {
        let SlotKind::KeyCopy(copy) = &saved.prefix.kind else {
            panic!("copy");
        };
        let recipient = recipients.iter().find(|m| m.member() == copy.routing.member).unwrap();
        let context =
            SlotObjectPrefix::copy_context(saved.prefix.format, saved.prefix.key, &copy.routing)
                .unwrap();
        let hash = match audience {
            Audience::Store => {
                copy.open_store_key(recipient, saved.path.as_str(), &context).unwrap().hash()
            }
            Audience::Circle(_) => {
                copy.open_circle_key(recipient, saved.path.as_str(), &context).unwrap().hash()
            }
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
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    {
        let session = ana.sync.session.lock().await;
        let ring = ana.sync.keys.read().unwrap().unwrap();
        let key = session.history.metadata_key(&ring).unwrap();
        assert!(session.history.exposed(&Audience::Store, key));
        assert!(matches!(
            session.history.sending_key(&ring, ana.identity.member(), &Audience::Store),
            Err(SyncError::KeyUnavailable(_))
        ));
    }
    // Another removal can be published while every store key is exposed.
    // Device removal leaves Carol's shared sealing credential current.
    ana.sync.administer(StoreChange::RemoveDevice { device: carol.sync.device }).await.unwrap();
    for audience in [Audience::Circle(circle), Audience::Store, Audience::Circle(circle)] {
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
        .install_key(&mut session, &mut ring, &ana.identity, Audience::Circle(circle), None)
        .await
        .unwrap();
    let intro = session.history.introductions.last().unwrap();
    assert_eq!(intro.audience, Audience::Circle(circle));
    assert!(session.history.accepted(&ring, &intro.audience, intro.key));
    drop(session);
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn audience_keys_never_enter_database_files() {
    let mut group = Group::new();
    group.custody = Custody::Passphrase;
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    for audience in [Audience::Circle(circle), Audience::Store] {
        ana.sync.rotate_key(audience).await.unwrap();
    }
    let held = ana.sync.keys.read().unwrap().unwrap();
    assert_database_has_no_keys(&ana, &held, circle);
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.keys.read().unwrap().unwrap().to_secret_bytes().as_bytes(),
        held.to_secret_bytes().as_bytes()
    );
    assert_database_has_no_keys(&ana, &held, circle);
    ana.database().close().await.unwrap();
    assert_database_has_no_keys(&ana, &held, circle);
}

fn assert_database_has_no_keys(ana: &Device, ring: &StoreKeyring, circle: CircleId) {
    let encoded = ring.to_secret_bytes();
    let secrets: Vec<_> = ring
        .store_key_ids()
        .chain(ring.circle_key_ids(circle))
        .map(|id| {
            let offset =
                encoded.as_bytes().windows(16).position(|bytes| bytes == id.0.as_bytes()).unwrap()
                    + 16;
            encoded.as_bytes()[offset..offset + 32].to_vec()
        })
        .collect();
    let path = ana.sync.directory.database_path();
    let mut scanned = Vec::new();
    for entry in std::fs::read_dir(path.parent().unwrap()).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_name().to_str().unwrap().starts_with("store.db") {
            continue;
        }
        let bytes = std::fs::read(entry.path()).unwrap();
        for secret in &secrets {
            let json = serde_json::to_vec(secret).unwrap();
            for needle in [secret.as_slice(), &json[1..json.len() - 1]] {
                assert!(
                    !bytes.windows(needle.len()).any(|bytes| bytes == needle),
                    "audience key reached {:?}",
                    entry.file_name()
                );
            }
        }
        scanned.push(entry.file_name());
    }
    assert!(scanned.iter().any(|name| name == "store.db"));
}

async fn resume_key_generation(saved: bool) -> serde_json::Value {
    use coven_foundation::id_source::IdSource;
    let mut group = Group::new();
    group.custody = Custody::Passphrase;
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = CircleId(group.ids.new_id());
    let key = KeyId(group.ids.new_id());
    let record = ana
        .database()
        .start_operation(NewOperation {
            kind: "sequence".into(),
            data: serde_json::to_vec(&SequenceOperation {
                request: Request::Key(KeyPreparation::Circle { circle, key, name: "Gifts".into() }),
                progress: Progress::Running(Step::Preparing),
            })
            .unwrap(),
            started_by: "key".into(),
            pauses_writes: false,
        })
        .await
        .unwrap();
    let kept = coven_crypto::CircleKey::from_bytes(circle, key, [29; 32]);
    if saved {
        let mut ring = ana.sync.keys.read().unwrap().unwrap();
        ring.insert_circle_key(kept.clone()).unwrap();
        ana.sync.keys.persist(&ring).unwrap();
    }
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    assert!(ana.database().operation(record).await.unwrap().is_none());
    let ring = ana.sync.keys.read().unwrap().unwrap();
    let hash = ring.circle_key(circle, key).unwrap().hash();
    assert_eq!(hash == kept.hash(), saved, "a restart must reuse saved material");
    let session = ana.sync.session.lock().await;
    let introduction = session.history.introductions.last().unwrap();
    assert_eq!(introduction.key, key);
    assert_eq!(introduction.audience, Audience::Circle(circle));
    assert_eq!(introduction.hash, hash);
    assert_database_has_no_keys(&ana, &ring, circle);
    let output = json!({"audience":1,"key":1,"hash":if hash == kept.hash() {2} else {1}});
    drop(session);
    ana.database().close().await.unwrap();
    output
}

#[tokio::test]
async fn key_generation_resumes_on_either_side_of_the_custody_commit() {
    for saved in [false, true] {
        resume_key_generation(saved).await;
    }
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN"]
async fn key_custody_preparation_matches_lean() {
    for saved in [false, true] {
        let input = json!({"kind":"keyCustody","audience":1,"key":1,
            "held":saved.then_some(2),"fresh":1});
        assert_eq!(
            resume_key_generation(saved).await,
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input)
        );
    }
}
