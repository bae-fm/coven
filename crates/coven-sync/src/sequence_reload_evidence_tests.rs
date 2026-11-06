use super::*;
use crate::sequence_sync::tests::*;
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
    below.header.first = Slot(below.header.at + 2);
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
        .filter(|original| original.slot.0 < added)
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
    let (signature, digest) = sealer.finish(signer);
    bytes.extend_from_slice(signature.as_bytes());
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

/// Ana's store with a note of 64 KiB and her laptop, compacted: past the
/// grace period her pass announces a store snapshot and retention writes a
/// floor naming it, deleting the oldest slots. `later` is then admitted, in
/// slots too young and too small to ask for a newer snapshot (§16.2), so the
/// floor still names that one. Its announcement slot and coverage.
async fn compacted(group: &Group, ana: &Device, later: Option<&Device>) -> (Slot, u64) {
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
    let (slot, covers) = compacted(&group, &ana, Some(&ben)).await;
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
        .filter(|original| original.slot.0 <= covers)
        .collect();
    let moved = originals
        .iter()
        .skip(1)
        .find(|original| {
            let object = coven_format::sealed_slot::BoundedSlot::decode(&original.object).unwrap();
            matches!(object.prefix.kind, coven_format::sealed_slot::SlotKind::Admin)
                && originals.iter().all(|other| other.slot.0 != original.slot.0 - 1)
        })
        .expect("the laptop's registration follows the note's writes slot");
    let to = moved.slot.0 - 1;

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
        &moved.slot.0.to_be_bytes()[..],
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

    let slot_path = ObjectPath::slot(group.store, slot.0.try_into().unwrap());
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
