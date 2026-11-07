use super::*;
use crate::sequence_sync::tests::*;
use coven_merge::slot;
use coven_storage::Storage;
use std::time::UNIX_EPOCH;

/// A snapshot's signed prefix states the coverage and purpose its announcement
/// names; one that states others is refused as the wrong identity before its
/// body loads (§16.2, format.md D9).
#[tokio::test]
async fn a_snapshot_whose_prefix_differs_from_its_announcement_is_refused() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let slot = ana.sync.snapshot(Audience::Store).await.unwrap();
    let announced = ana.sync.session.lock().await.history.announcements[&slot].clone();
    ana.sync.snapshot_prefix(&announced).await.unwrap();

    let altered = [
        SnapshotAnnouncement { covers: announced.covers - 1, ..announced.clone() },
        SnapshotAnnouncement { purpose: Purpose::Reset, ..announced.clone() },
    ];
    for altered in altered {
        let result = ana.sync.snapshot_prefix(&altered).await.map(drop);
        assert!(
            matches!(
                &result,
                Err(SyncError::Damaged(damaged))
                    if matches!(damaged.failure, Refusal::WrongIdentity { .. })
            ),
            "{result:?}"
        );
    }
    ana.sync.database.close().await.unwrap();
}

/// A floor names the store's newest snapshot first and then each circle's in
/// audience order, and every snapshot it names covers at least the slot before
/// its first kept one (format.md D8).
#[tokio::test]
async fn a_floor_naming_snapshots_out_of_order_or_below_its_first_kept_slot_is_damaged() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    insert(&ana.sync.database, "a", "in the store").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    group.clock.set(
        UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert_eq!(floor.announcements.len(), 2, "the store's snapshot and Gifts'");

    let history = ana.sync.session.lock().await.history.clone();
    let authenticate = |floor: FloorObject| {
        let (ana, history) = (&ana, &history);
        async move {
            let mut ring = ana.sync.keys.read().unwrap().unwrap();
            ana.sync.authenticate_floor(&floor, history, &mut ring, &ana.identity).await
        }
    };
    authenticate(floor.clone()).await.unwrap();

    let mut reordered = floor.clone();
    reordered.announcements.reverse();
    let mut below = floor;
    below.header.first = slot(below.header.at + 2);
    for damaged in [reordered, below] {
        let result = authenticate(damaged).await;
        assert!(
            matches!(
                &result,
                Err(SyncError::Damaged(damaged))
                    if matches!(damaged.failure, Refusal::WrongIdentity { .. })
            ),
            "{result:?}"
        );
    }
    ana.sync.database.close().await.unwrap();
}

/// A snapshot's author is resolved at its coverage from a history that holds
/// every admin slot through it, so an author that history doesn't hold there
/// can't appear later: the snapshot is damaged, not waiting (format.md D9).
/// Ben's store snapshot authenticates against Ana's history, and is refused
/// against her history cut before the slot that added Ben's device, which
/// still holds Ben's member.
#[tokio::test]
async fn a_snapshot_whose_author_its_history_lacks_at_its_coverage_is_damaged() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let slot = ben.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    let announced = history.announcements[&slot].clone();
    let ring = ana.sync.keys.read().unwrap().unwrap();
    let object = ana.sync.snapshot_prefix(&announced).await.unwrap();
    let read = ana
        .sync
        .read_snapshot(announced, object, &ring, None, None)
        .await
        .map_err(|failure| failure.error)
        .unwrap();
    assert_eq!(read.authenticate(&history).unwrap(), ben.identity.member());

    let added = history.added(ben.sync.device).unwrap();
    let before: Vec<_> = ana
        .sync
        .database
        .original_slots()
        .await
        .unwrap()
        .into_iter()
        .filter(|original| original.slot.get() < added)
        .collect();
    let mut short_ring = ring.clone();
    let short = History::restore(&before, group.store, &mut short_ring, &ana.identity).unwrap();
    assert!(short.belonged(&Audience::Store, &ben.identity.member(), added - 1));
    let result = read.authenticate(&short);
    assert!(
        matches!(
            &result,
            Err(SyncError::Damaged(damaged)) if matches!(damaged.failure, Refusal::NotAuthorized)
        ),
        "{result:?}"
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// The plaintext of the stream object `bytes` at `path`, whose chunks start
/// at `start` and are sealed under `prefix`.
fn opened_stream(
    bytes: &[u8],
    start: usize,
    path: &ObjectPath,
    prefix: &[u8],
    key: &coven_crypto::DerivedKeys,
) -> Vec<u8> {
    let mut layout = coven_format::sealed_stream::StreamObjectLayout::new();
    let mut plain = Vec::new();
    let (mut at, end) = (start, bytes.len() - coven_crypto::SIGNATURE_LEN);
    while at < end {
        let length = layout.chunk_length(&bytes[at..at + 4]).unwrap();
        let index = layout.index();
        let chunk = layout.decode_chunk(&bytes[at..at + length]).unwrap();
        plain.extend(key.open_object_chunk(path.as_str(), prefix, 0, index, chunk).unwrap());
        at += length;
    }
    plain
}

/// `plain` sealed as a stream object at `path` and signed by `signer`, as
/// a snapshot's or the floor's writer seals it, with its complete digest.
fn sealed_stream(
    path: &ObjectPath,
    prefix: &[u8],
    prefix_signature: Option<coven_crypto::Signature>,
    key: coven_crypto::DerivedKeys,
    plain: &[u8],
    signer: &MemberIdentity,
) -> (Vec<u8>, [u8; 32]) {
    let (mut sealer, mut bytes) = crate::sealed_stream::StreamSealer::start(
        path.clone(),
        prefix.to_vec(),
        prefix_signature,
        key,
    );
    for chunk in plain.chunks(coven_format::chunks::CHUNK_SIZE) {
        bytes.extend(sealer.chunk(chunk).unwrap());
    }
    let (signature, digest, size) = sealer.finish(signer);
    bytes.extend_from_slice(signature.as_bytes());
    assert_eq!(size, bytes.len() as u64);
    (bytes, *digest.as_bytes())
}

/// A floor with `header`, `copies` and `announcements`, sealed with the
/// store key `device` holds and signed by `signer`.
fn sealed_floor(
    device: &Device,
    header: coven_format::floor::FloorHeader,
    copies: &[coven_format::floor::OriginalSlot],
    announcements: &[coven_format::floor::OriginalSlot],
    signer: &MemberIdentity,
) -> Vec<u8> {
    let ring = device.sync.keys.read().unwrap().unwrap();
    let key = ring.store_key_ids().next().unwrap();
    let (mut routing, mut prefix) = coven_format::sealed_floor::FloorPrefixEncoder::start(
        key,
        header.first,
        copies.len().try_into().unwrap(),
    )
    .unwrap();
    for copy in copies {
        prefix.extend(routing.copy(copy).unwrap());
    }
    routing.finish().unwrap();
    let (mut frames, mut plain) = coven_format::floor::FloorFrames::start(header).unwrap();
    for announcement in announcements {
        plain.extend(frames.announcement(announcement).unwrap());
    }
    plain.extend(frames.end().unwrap());
    let path = ObjectPath::floor(device.sync.store());
    let key = crate::write_seal::derive(&ring, &Audience::Store, key).unwrap();
    sealed_stream(&path, &prefix, None, key, &plain, signer).0
}

/// Ana's store with a note of 64 KiB, her laptop and `earlier`, compacted:
/// past the grace period her pass announces a store snapshot and retention
/// writes a floor naming it, deleting the oldest slots. `later` is then
/// admitted, in slots too young and too small to ask for a newer snapshot
/// (§16.2), so the floor still names that one. Its announcement slot and
/// coverage.
async fn compacted(
    group: &Group,
    ana: &Device,
    earlier: Option<&Device>,
    later: Option<&Device>,
) -> (Slot, u64) {
    group.create(ana).await;
    ana.sync
        .database
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('a',?1)", ["a".repeat(64 * 1024)])?;
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let laptop = group.laptop(ana).await;
    ana.sync.pass().await.unwrap();
    laptop.sync.database.close().await.unwrap();
    if let Some(earlier) = earlier {
        group.admit(ana, earlier).await;
    }
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(
        UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(ana).await;
    let [slot] = floor.header.snapshots[..] else { panic!("one store snapshot") };
    let covers = ana.sync.session.lock().await.history.announcements[&slot].covers;
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    if let Some(later) = later {
        group.admit(ana, later).await;
        let now = crate::sequence_sync::sequence_reload::tests::stored_floor(ana).await;
        assert_eq!(now.header, floor.header);
    }
    (slot, covers)
}

/// A newcomer of Ana's member restores nothing from storage that `refused`
/// accepts: its pass fails, and it applied, staged and kept nothing.
async fn restore_refused(
    group: &Group,
    ana: &Device,
    refused: impl Fn(&crate::DamagedObject) -> bool,
) {
    let laptop = group.newcomer(ana.identity.clone(), group.storage.for_device()).await;
    let result = laptop.sync.pass().await;
    assert!(matches!(&result, Err(SyncError::Damaged(damaged)) if refused(damaged)), "{result:?}");
    assert_eq!(laptop.sync.database.staged_snapshot_records().await.unwrap(), 0);
    assert_eq!(laptop.sync.database.applied_slot().await.unwrap(), 0);
    assert!(laptop.sync.database.original_slots().await.unwrap().is_empty());
    assert!(rows(&laptop.sync.database).await.is_empty());
    laptop.sync.database.close().await.unwrap();
}

/// A store snapshot's original slots give a restored device the admin state
/// in order, and the kept slots after its coverage give newer bindings: Ben,
/// admitted after the snapshot, is in the restored laptop's state as in
/// Ana's. Each original is opened at its own slot's path: the same snapshot
/// with an admin original moved to another slot number, sealed, signed and
/// announced again so that only the path can tell, is refused at that path
/// and commits nothing (format.md D7.6).
#[tokio::test]
async fn a_store_snapshots_original_slots_are_checked_at_their_paths_and_give_the_admin_state() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let (slot, covers) = compacted(&group, &ana, None, Some(&ben)).await;
    let history = ana.sync.session.lock().await.history.clone();
    assert!(history.added(ben.sync.device).unwrap() > covers);

    let laptop = group.newcomer(ana.identity.clone(), group.storage.for_device()).await;
    laptop.sync.pass().await.unwrap();
    let restored = laptop.sync.session.lock().await.history.clone();
    assert_eq!(restored.state, history.state);
    assert!(restored.belonged(&Audience::Store, &ben.identity.member(), u64::MAX));
    laptop.sync.database.close().await.unwrap();

    // An admin original after the first whose slot follows a writes slot,
    // moved back to that writes slot's number: the originals keep their order.
    let originals: Vec<_> = ana
        .sync
        .database
        .original_slots()
        .await
        .unwrap()
        .into_iter()
        .filter(|original| original.slot.get() <= covers)
        .collect();
    let moved = originals
        .iter()
        .skip(1)
        .find(|original| {
            let object = coven_format::sealed_slot::BoundedSlot::decode(&original.object).unwrap();
            matches!(object.prefix.kind, coven_format::sealed_slot::SlotKind::Admin)
                && originals.iter().all(|other| other.slot.get() != original.slot.get() - 1)
        })
        .expect("the laptop's registration follows the note's writes slot");
    let to = moved.slot.get() - 1;

    let ring = ana.sync.keys.read().unwrap().unwrap();
    let announced = history.announcements[&slot].clone();
    let path = crate::snapshot_object::path(group.store, &announced.snapshot).unwrap();
    let bytes = group.storage.read_object(&path, None).await.unwrap().bytes.to_vec();
    let signed =
        coven_format::sealed_snapshot::SnapshotObjectPrefix::signed_length(&bytes).unwrap();
    let prefix = &bytes[..signed - coven_crypto::SIGNATURE_LEN];
    let decoded = coven_format::sealed_snapshot::SnapshotObjectPrefix::decode(prefix).unwrap();
    let prefix_signature = coven_crypto::Signature::from_bytes(
        bytes[signed - coven_crypto::SIGNATURE_LEN..signed].try_into().unwrap(),
    );
    let key = || crate::write_seal::derive(&ring, &decoded.audience, decoded.key).unwrap();
    let mut plain = opened_stream(&bytes, signed, &path, prefix, &key());
    // The record's wire form: its slot, then its object's length and bytes.
    let found = [
        &moved.slot.get().to_be_bytes()[..],
        &(moved.object.len() as u32).to_be_bytes(),
        &moved.object,
    ]
    .concat();
    let places: Vec<_> = plain
        .windows(found.len())
        .enumerate()
        .filter_map(|(at, window)| (window == found).then_some(at))
        .collect();
    let [at] = places[..] else { panic!("the record occurs once: {places:?}") };
    plain[at..at + 8].copy_from_slice(&to.to_be_bytes());
    let (snapshot, digest) =
        sealed_stream(&path, prefix, Some(prefix_signature), key(), &plain, &ana.identity);

    let slot_path = ObjectPath::slot(group.store, slot.get().try_into().unwrap());
    let original = group.storage.read_object(&slot_path, None).await.unwrap().bytes.to_vec();
    let (header, SlotFrame::Announcement(announcement)) =
        slot_object::bounded_plaintext(&original, &slot_path, &ring).unwrap()
    else {
        panic!("an announcement slot");
    };
    let announcement = slot_object::seal_bounded(
        &slot_path,
        &header,
        &SlotFrame::Announcement(SnapshotAnnouncement { digest, ..announcement }),
        coven_format::sealed_slot::BoundedSlot::decode(&original).unwrap().prefix,
        &ring,
        &ana.identity,
    )
    .unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    let named = [coven_format::floor::OriginalSlot { slot, object: announcement.clone() }];
    let floor = sealed_floor(&ana, floor.header, &floor.copies, &named, &ana.identity);
    group.storage.plant(&path, &snapshot).await.unwrap();
    group.storage.plant(&slot_path, &announcement).await.unwrap();
    group.storage.plant(&ObjectPath::floor(group.store), &floor).await.unwrap();

    let moved_path = ObjectPath::slot(group.store, to.try_into().unwrap());
    restore_refused(&group, &ana, |damaged| damaged.path == moved_path.as_str()).await;
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A floor binds its signer and the copies it carries only once verified
/// against the history through its `at`: its encrypted `author` names the
/// device whose key must have signed it, that device must belong at `at`,
/// and each copy opens only at its original slot's path. A restore refuses
/// a floor signed by Ben, admitted after `at`, as its author or as Ana; one
/// whose copy for the restoring member is moved to another slot number,
/// when it routes the copy; and one whose copy for Carol is moved, when it
/// checks the floor against history. Each such floor is sealed and signed
/// like the honest one, which a restore loads (format.md D8, D9).
#[tokio::test]
async fn a_floor_whose_signer_or_copies_fail_verification_commits_no_reload() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    compacted(&group, &ana, Some(&carol), Some(&ben)).await;
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    let history = ana.sync.session.lock().await.history.clone();
    assert!(history.added(ben.sync.device).unwrap() > floor.header.at);
    let path = ObjectPath::floor(group.store);
    let plant = |header: coven_format::floor::FloorHeader,
                 copies: Vec<coven_format::floor::OriginalSlot>,
                 signer: &MemberIdentity| {
        let bytes = sealed_floor(&ana, header, &copies, &floor.announcements, signer);
        let (storage, path) = (group.storage.clone(), path.clone());
        async move { storage.plant(&path, &bytes).await.unwrap() }
    };

    plant(floor.header.clone(), floor.copies.clone(), &ana.identity).await;
    let laptop = group.newcomer(ana.identity.clone(), group.storage.for_device()).await;
    laptop.sync.pass().await.unwrap();
    assert_eq!(rows(&laptop.sync.database).await, rows(&ana.sync.database).await);
    laptop.sync.database.close().await.unwrap();

    let by_ben =
        coven_format::floor::FloorHeader { author: ben.sync.device, ..floor.header.clone() };
    plant(by_ben, floor.copies.clone(), &ben.identity).await;
    restore_refused(&group, &ana, |damaged| {
        damaged.path == path.as_str() && matches!(damaged.failure, Refusal::NotAuthorized)
    })
    .await;

    plant(floor.header.clone(), floor.copies.clone(), &ben.identity).await;
    restore_refused(&group, &ana, |damaged| {
        damaged.path == path.as_str() && matches!(damaged.failure, Refusal::Signature { .. })
    })
    .await;

    // Each copy moved to a slot number between its neighbours' that holds no
    // original, so the copies keep their order.
    let originals: Vec<_> = ana
        .sync
        .database
        .original_slots()
        .await
        .unwrap()
        .into_iter()
        .map(|original| original.slot.get())
        .collect();
    let routes = |copy: &coven_format::floor::OriginalSlot| {
        let object = coven_format::sealed_slot::BoundedSlot::decode(&copy.object).unwrap();
        let coven_format::sealed_slot::SlotKind::KeyCopy(copy) = object.prefix.kind else {
            panic!("a key copy");
        };
        copy.routing.member
    };
    let members: Vec<_> = floor.copies.iter().map(routes).collect();
    assert_eq!(members, [ana.identity.member(), carol.identity.member()]);
    for index in 0..floor.copies.len() {
        let below = index.checked_sub(1).map_or(0, |before| floor.copies[before].slot.get());
        let above = floor.copies.get(index + 1).map_or(floor.header.at + 1, |next| next.slot.get());
        let to = (below + 1..above).find(|slot| !originals.contains(slot)).unwrap();
        let mut copies = floor.copies.clone();
        copies[index].slot = slot(to);
        plant(floor.header.clone(), copies, &ana.identity).await;
        let moved = ObjectPath::slot(group.store, to.try_into().unwrap());
        restore_refused(&group, &ana, |damaged| {
            damaged.path == moved.as_str() && matches!(damaged.failure, Refusal::Decryption { .. })
        })
        .await;
    }
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana snapshots the store twice, and storage damages the newer body. Ben's
/// reload refuses it and waits: it never loads the older snapshot, whose body
/// is intact, to get around the damage (§16.2).
#[tokio::test]
async fn a_reload_waits_on_a_damaged_newest_snapshot_and_loads_no_older_one() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(ana.database(), "a", "in both snapshots").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    insert(ana.database(), "b", "in the newer snapshot").await;
    ana.sync.pass().await.unwrap();
    let newer = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let snapshot = ben.sync.session.lock().await.history.announcements[&newer].snapshot.clone();
    let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
    let size = group.storage.status(&path).await.unwrap().unwrap().size;
    group.storage.corrupt_byte(&path, size as usize - 1).await.unwrap();
    let result = ben.sync.reload_from_snapshot().await;
    assert!(matches!(result, Err(crate::SyncError::Damaged(_))), "{result:?}");
    let result = ben.sync.reload_from_snapshot().await;
    assert!(matches!(result, Err(crate::SyncError::Pending(_))), "{result:?}");
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}
