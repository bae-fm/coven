use super::*;
use crate::test_utils as f;

#[test]
fn headers_bind_claims_to_exact_slot_paths() {
    for slot in [1, 8, u64::MAX] {
        let path = f::slot_path(slot);
        let header = f::slot_header(slot, u64::MAX);
        let bytes = header.encode(&path).unwrap();
        assert_eq!(bytes.len(), 47);
        assert_eq!(SlotHeader::decode(&bytes, &path).unwrap(), header);
        assert!(SlotHeader::decode(&bytes, &f::slot_path(if slot == 1 { 2 } else { 1 })).is_err());
        for end in 0..bytes.len() {
            assert!(SlotHeader::decode(&bytes[..end], &path).is_err());
        }
        let mut invalid = header;
        invalid.claim = 0;
        assert!(invalid.encode(&path).is_err());
    }
}

#[test]
fn bounded_frames_round_trip_and_refuse_wrong_placement() {
    for (slot, frame) in [
        (1, SlotFrame::Admin(f::creation_change())),
        (2, SlotFrame::KeyCopy),
        (7, SlotFrame::Announcement(f::announcement())),
    ] {
        let path = f::slot_path(slot);
        let header = f::slot_header(slot, slot);
        let bytes = frame.encode(&header, &path).unwrap();
        assert_eq!(SlotFrame::decode(&bytes, &path).unwrap(), (header, frame));
        crate::tests::mutations(&bytes, |bytes| {
            if let Ok((header, frame)) = SlotFrame::decode(bytes, &path) {
                assert_eq!(frame.encode(&header, &path).unwrap(), bytes);
            }
        });
    }
    assert!(SlotFrame::Admin(f::creation_change())
        .encode(&f::slot_header(2, 2), &f::slot_path(2))
        .is_err());
    let self_replacement = SlotFrame::Admin(StoreChange::AddDevice {
        name: "Phone".into(),
        replaces: Some(f::slot_header(2, 2).device),
    });
    assert!(self_replacement.encode(&f::slot_header(2, 2), &f::slot_path(2)).is_err());
    let future = SlotFrame::Admin(StoreChange::Reset { announcement: Slot(3) });
    assert!(future.encode(&f::slot_header(2, 2), &f::slot_path(2)).is_err());
}

#[test]
fn announcements_name_prior_coverage_and_explicit_purpose() {
    for purpose in [Purpose::Ordinary, Purpose::Raise(1), Purpose::Raise(u32::MAX), Purpose::Reset]
    {
        let announcement = SnapshotAnnouncement { purpose, ..f::announcement() };
        let bytes = announcement.encode(Slot(7)).unwrap();
        assert_eq!(SnapshotAnnouncement::decode(&bytes, Slot(7)).unwrap(), announcement);
        assert!(SnapshotAnnouncement::decode(&bytes, Slot(6)).is_err());
    }
    assert!(SnapshotAnnouncement { purpose: Purpose::Raise(0), ..f::announcement() }
        .encode(Slot(7))
        .is_err());
}
