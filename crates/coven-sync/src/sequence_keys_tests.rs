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
        .install_key(&mut session, &mut ring, &ana.identity, Audience::Circle(circle))
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
        .sync
        .start_request_row(
            Request::Key(KeyPreparation::Circle { circle, key, name: "Gifts".into() }),
            "key",
        )
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

/// Ana removes Ben, then writes. Every store key was exposed to Ben, so
/// Ana's device rotates first, and the write is sealed with a key introduced
/// after the removal that no copy names Ben for (§12.2).
#[tokio::test]
async fn a_device_with_no_usable_key_rotates_before_sending() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let removal = ana.database().applied_slot().await.unwrap() + 1;
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    insert(ana.database(), "after", "the removal").await;
    // The first pass rotates; the write waits for the replacement key.
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();

    let last = ana.database().applied_slot().await.unwrap();
    let mut written = None;
    for number in (removal..=last).rev() {
        let path = coven_storage::ObjectPath::slot(group.store, number.try_into().unwrap());
        let bytes = coven_storage::Storage::read(&group.storage, &path).await.unwrap();
        let prefix =
            SlotObjectPrefix::decode(&bytes[..SlotObjectPrefix::length(&bytes).unwrap()]).unwrap();
        if matches!(prefix.kind, SlotKind::Writes) {
            written = Some(prefix);
            break;
        }
    }
    let sealed_with = written.expect("the write was sent").key;
    let session = ana.sync.session.lock().await;
    let introduced = session
        .history
        .introductions
        .iter()
        .find(|introduction| introduction.key == sealed_with)
        .expect("the write's key was introduced");
    assert_eq!(introduced.audience, Audience::Store);
    assert!(
        introduced.slot.0 > removal,
        "introduced at {:?}, removal at {removal}",
        introduced.slot
    );
    assert!(session.history.copies.iter().all(|copy| {
        let SlotKind::KeyCopy(copy) = &copy.prefix.kind else { panic!("copy") };
        copy.routing.key != sealed_with || copy.routing.member != ben.identity.member()
    }));
    assert!(!session.history.exposed(&Audience::Store, sealed_with));
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana removes Ben, then writes and publishes a snapshot. The removal
/// replaced the store key, so the write's slot and the snapshot open with
/// the key ring Ana holds now and are refused with the one Ben held before
/// the removal (§12.2).
#[tokio::test]
async fn after_a_removal_the_next_write_and_snapshot_open_only_with_the_replacement_keys() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let removed_ring = ben.sync.keys.read().unwrap().unwrap();
    let removal = ana.database().applied_slot().await.unwrap() + 1;
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    insert(ana.database(), "after", "the removal").await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let announcement = ana.sync.snapshot(Audience::Store).await.unwrap();

    let ring = ana.sync.keys.read().unwrap().unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    let last = ana.database().applied_slot().await.unwrap();
    let mut write_slot = None;
    for number in (removal..=last).rev() {
        let path = coven_storage::ObjectPath::slot(group.store, number.try_into().unwrap());
        let bytes = coven_storage::Storage::read(&group.storage, &path).await.unwrap();
        let prefix =
            SlotObjectPrefix::decode(&bytes[..SlotObjectPrefix::length(&bytes).unwrap()]).unwrap();
        if matches!(prefix.kind, SlotKind::Writes) {
            write_slot = Some(path);
            break;
        }
    }
    let path = write_slot.expect("the write was sent");
    let open = |ring: StoreKeyring| {
        let (sync, path, history) = (ana.sync.clone(), path.clone(), history.clone());
        async move {
            let bytes = sync.fetch(&path).await.unwrap().unwrap().local_bytes().await.unwrap();
            crate::slot_object::open(bytes, &path, &ring, &history, |_| true).map(drop)
        }
    };
    open(ring.clone()).await.expect("the replacement keys open the write");
    let refused = open(removed_ring.clone()).await;
    assert!(matches!(refused, Err(SyncError::KeyUnavailable(_))), "{refused:?}");

    let announced = history.announcements[&announcement].clone();
    let snapshot_path = crate::snapshot_object::path(group.store, &announced.snapshot).unwrap();
    let snapshot = crate::snapshot_object::SnapshotObject::read_prefix(
        ana.database(),
        ana.sync.storage.as_ref(),
        &snapshot_path,
    )
    .await
    .unwrap();
    snapshot
        .read_body(ana.sync.storage.as_ref(), &ring, &announced, None, None)
        .await
        .map(drop)
        .expect("the replacement keys open the snapshot");
    let refused = snapshot
        .read_body(ana.sync.storage.as_ref(), &removed_ring, &announced, None, None)
        .await
        .map(drop);
    assert!(matches!(refused, Err(SyncError::KeyUnavailable(_))), "{refused:?}");
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Carol joins after the store key was rotated: she is sent a copy of the
/// older key too, so she reads what was written under it. A retried
/// operation sends no copy that is already in the sequence (§12.1).
#[tokio::test]
async fn a_member_lacking_an_older_key_gets_a_copy_once() {
    let group = Group::new();
    let ana = group.device(1).await;
    let carol = MemberIdentity::new(
        coven_foundation::id_source::MemberId(coven_foundation::id_source::IdSource::new_id(
            group.ids.as_ref(),
        )),
        coven_format::test_utils::keys_from_seed(9),
    );
    group.create(&ana).await;
    insert(ana.database(), "old", "under the first key").await;
    ana.sync.pass().await.unwrap();
    ana.sync.rotate_key(Audience::Store).await.unwrap();
    let keys: Vec<_> = ana
        .sync
        .session
        .lock()
        .await
        .history
        .introductions
        .iter()
        .filter(|introduction| introduction.audience == Audience::Store)
        .map(|introduction| introduction.key)
        .collect();
    assert_eq!(keys.len(), 2);

    // The admission's claim fails once, after its copies landed.
    let first = ana.database().applied_slot().await.unwrap() + 1;
    let claim = coven_storage::ObjectPath::slot(group.store, (first + 2).try_into().unwrap());
    ana.storage
        .set_faults(coven_storage::test_utils::Faults {
            create_failures: [(claim, (coven_storage::StorageFailure::Network, 500))].into(),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let admission = StoreChange::AddMember {
        member: carol.member(),
        keys: carol.keys().public_keys(),
        role: coven_format::admin::MemberRole::Member,
        access: group.access("carol"),
    };
    assert!(ana.sync.administer(admission).await.is_err());
    ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(120));
    ana.sync.pass().await.unwrap();

    {
        let session = ana.sync.session.lock().await;
        assert!(session.history.state.members.contains_key(&carol.member()));
        let mut sent: Vec<_> = session
            .history
            .copies
            .iter()
            .filter_map(|copy| match &copy.prefix.kind {
                SlotKind::KeyCopy(copy) if copy.routing.member == carol.member() => {
                    Some(copy.routing.key)
                }
                _ => None,
            })
            .collect();
        sent.sort();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(sent, expected, "one copy of each store key");
    }
    // Carol's phone holds no key but what the copies sent to her give it.
    let phone = group.newcomer(carol, group.storage.for_device()).await;
    phone.sync.pass().await.unwrap();
    phone
        .sync
        .administer(StoreChange::AddDevice { name: "Carol".into(), replaces: None })
        .await
        .unwrap();
    phone.sync.pass().await.unwrap();
    assert_eq!(
        rows(phone.database()).await,
        vec![("old".to_owned(), "under the first key".to_owned())]
    );
    for device in [&ana, &phone] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana adds Ben to Gifts after its key was replaced. Ben receives its
/// earlier key as well as its current one, before the entry adding him, so
/// his pass shows the pins written under either (§15.7).
#[tokio::test]
async fn a_member_added_after_a_rotation_reads_the_circles_history_under_its_earlier_key() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    pin(ana.database(), PIN_1, circle, "under the first key").await;
    ana.sync.pass().await.unwrap();
    ana.sync.rotate_key(Audience::Circle(circle)).await.unwrap();
    pin(ana.database(), PIN_2, circle, "under the second key").await;
    ana.sync.pass().await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(ben.sync.keys.read().unwrap().unwrap().circle_key_ids(circle).count(), 2);
    assert_eq!(
        pins(ben.database()).await,
        [
            (PIN_1.to_owned(), "under the first key".to_owned()),
            (PIN_2.to_owned(), "under the second key".to_owned())
        ]
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

const PIN_1: &str = "00000000-0000-4000-8000-000000000001";
const PIN_2: &str = "00000000-0000-4000-8000-000000000002";

/// Ben left Gifts, and Ana's next write replaced the key he had been sent.
/// Adding him back sends him the key made while he was out, and his own next
/// Gifts write uses that one, never the key exposed to him before he left
/// (§15.7, §12.2).
#[tokio::test]
async fn a_member_added_back_gets_keys_made_while_out_and_never_sends_with_an_exposed_one() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let exposed = *ben
        .sync
        .keys
        .read()
        .unwrap()
        .unwrap()
        .circle_key_ids(circle)
        .collect::<Vec<_>>()
        .first()
        .unwrap();
    ana.sync
        .administer(StoreChange::RemoveCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    pin(ana.database(), PIN_1, circle, "while Ben was out").await;
    // The first pass replaces the exposed key; the write follows under the new one.
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let ring = ben.sync.keys.read().unwrap().unwrap();
    let audience = Audience::Circle(circle);
    let session = ben.sync.session.lock().await;
    let sending = session.history.sending_key(&ring, ben.identity.member(), &audience).unwrap();
    assert_ne!(sending, exposed, "the key Ben had been sent stays exposed");
    assert!(ring.circle_key_ids(circle).any(|key| key == sending), "Ben holds the newer key");
    assert_eq!(
        session.history.introductions.iter().filter(|i| i.audience == audience).count(),
        2,
        "Ana replaced the key while Ben was out"
    );
    drop(session);
    assert_eq!(pins(ben.database()).await.len(), 1);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Removing a member from a circle is one entry with no key copy. The next
/// pass replaces the key Ben was sent and copies the new one to the members
/// who remain, before Ana's next Gifts write; that write goes out under it
/// (§15.8, §12.2).
#[tokio::test]
async fn a_circle_removal_is_one_entry_and_the_next_circle_write_waits_for_a_new_key() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    let audience = Audience::Circle(circle);
    let keys_before = ana
        .sync
        .session
        .lock()
        .await
        .history
        .introductions
        .iter()
        .filter(|i| i.audience == audience)
        .count();
    let applied = ana.sync.database.applied_slot().await.unwrap();
    ana.sync
        .administer(StoreChange::RemoveCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    assert_eq!(ana.sync.database.applied_slot().await.unwrap(), applied + 1, "one entry, no copy");
    assert_eq!(
        ana.sync
            .session
            .lock()
            .await
            .history
            .introductions
            .iter()
            .filter(|i| i.audience == audience)
            .count(),
        keys_before
    );
    pin(ana.database(), PIN_1, circle, "after Ben left").await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let ring = ana.sync.keys.read().unwrap().unwrap();
    let session = ana.sync.session.lock().await;
    let introduced: Vec<_> =
        session.history.introductions.iter().filter(|i| i.audience == audience).collect();
    assert_eq!(introduced.len(), keys_before + 1, "a new key replaced the exposed one");
    let replacement = introduced.last().unwrap();
    assert!(!session.history.exposed(&audience, replacement.key));
    assert_eq!(
        session.history.sending_key(&ring, ana.identity.member(), &audience).unwrap(),
        replacement.key
    );
    drop(session);
    ben.sync.pass().await.unwrap();
    assert!(pins(ben.database()).await.is_empty());
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Ana removes Ben from Gifts, and Ben reads the removal. He keeps the row
/// he had read there and the key that opened it (§15.8).
#[tokio::test]
async fn a_member_removed_from_a_circle_keeps_its_rows_and_keys() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    pin(ana.database(), "00000000-0000-4000-8000-000000000001", circle, "gift").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(pins(ben.database()).await.len(), 1);
    let held = ben.sync.keys.read().unwrap().unwrap().circle_key_ids(circle).collect::<Vec<_>>();
    ana.sync
        .administer(StoreChange::RemoveCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert!(!ben
        .database()
        .store_state()
        .await
        .unwrap()
        .reads_circle(circle, &ben.identity.member()));
    assert_eq!(pins(ben.database()).await.len(), 1, "Ben keeps the row");
    assert_eq!(
        ben.sync.keys.read().unwrap().unwrap().circle_key_ids(circle).collect::<Vec<_>>(),
        held
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Ana deletes Gifts, then asks for its key to be replaced. The circle is
/// gone, so the rotation is refused as a gone target, not as a right she
/// lacks, and nothing is claimed (§10.4, §15.9).
#[tokio::test]
async fn rotating_a_deleted_circles_key_finds_its_target_gone() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync.administer(StoreChange::DeleteCircle { circle }).await.unwrap();
    let applied = ana.database().applied_slot().await.unwrap();
    let result = ana.sync.rotate_key(Audience::Circle(circle)).await;
    assert!(
        matches!(
            result,
            Err(SyncError::Rejected(crate::OperationRejection::TargetGone(
                crate::EntryTarget::Circle(gone)
            ))) if gone == circle
        ),
        "{result:?}"
    );
    assert_eq!(ana.database().applied_slot().await.unwrap(), applied);
    assert!(ana.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
}

/// A store key custody holds under the store key's id with other material
/// conflicts with this member's copy in history: opening the sequence fails
/// with that key's material error, and custody keeps what it held (E5).
#[tokio::test]
async fn a_held_key_whose_material_conflicts_fails_with_its_material_error() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let key = group.ring.store_key_ids().next().unwrap();
    let conflicting = StoreKeyring::new(coven_crypto::StoreKey::from_bytes(key, [9; 32]));
    ana.sync.keys.persist(&conflicting).unwrap();
    let held = &ana.sync;
    let result = SequenceSync::open(
        held.database.clone(),
        held.storage.clone(),
        held.directory.clone(),
        held.device,
        held.keys.clone(),
        held.member.clone(),
        held.person.clone(),
        Arc::new(crate::SequenceState::new()),
        held.clock.clone(),
        held.ids.clone(),
        held.waits.clone(),
        held.files.clone(),
    )
    .await;
    assert!(
        matches!(
            &result,
            Err(SyncError::Key(coven_crypto::MaterialError::StoreKeyConflict(id))) if *id == key
        ),
        "{:?}",
        result.err()
    );
    assert_eq!(
        ana.sync.keys.read().unwrap().unwrap().to_secret_bytes().as_bytes(),
        conflicting.to_secret_bytes().as_bytes()
    );
    ana.database().close().await.unwrap();
}
