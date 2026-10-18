use super::*;
use crate::{
    sealed_floor::{FloorPrefixDecoder, FloorPrefixEncoder},
    sealed_slot::SlotKind,
    sequence::SlotFrame,
    test_utils as f,
};

fn announcement(slot: u64) -> OriginalSlot {
    OriginalSlot {
        slot: Slot(slot),
        object: f::bounded_slot(
            slot,
            slot,
            SlotKind::Announcement,
            SlotFrame::Announcement(f::announcement()),
        ),
    }
}
fn header() -> FloorHeader {
    FloorHeader {
        author: DeviceId(1),
        at: 10,
        first: Slot(6),
        snapshots: vec![Slot(9), Slot(7)],
    }
}

#[test]
fn floor_round_trips_original_announcements_in_header_order() {
    let (mut writer, first) = FloorFrames::start(header()).unwrap();
    let mut reader = FloorFrames::decode_header(&first, header().first).unwrap();
    assert_eq!(reader.header(), &header());
    assert!(reader.finish().is_err());
    assert!(writer.end().is_err());
    for slot in [9, 7] {
        let original = announcement(slot);
        let frame = writer.announcement(&original).unwrap();
        assert_eq!(reader.frame(&frame).unwrap(), Some(original));
    }
    assert_eq!(reader.frame(&writer.end().unwrap()).unwrap(), None);
    reader.finish().unwrap();
    assert!(writer.end().is_err());
    assert!(reader
        .frame(&crate::encode_frame_with(7, |_| Ok(())).unwrap())
        .is_err());
}

#[test]
fn floor_refuses_a_header_that_disagrees_with_the_clear_first() {
    let bytes = header().encode().unwrap();
    for first in [5, 7] {
        assert!(FloorFrames::decode_header(&bytes, Slot(first)).is_err());
    }
}

#[test]
fn floor_refuses_duplicate_future_missing_wrong_kind_and_reordered_evidence() {
    for h in [
        FloorHeader {
            first: Slot(12),
            ..header()
        },
        FloorHeader {
            snapshots: vec![Slot(11)],
            ..header()
        },
        FloorHeader {
            snapshots: vec![Slot(7), Slot(7)],
            ..header()
        },
        FloorHeader {
            snapshots: vec![],
            ..header()
        },
    ] {
        assert!(FloorFrames::start(h).is_err());
    }
    let (mut writer, _) = FloorFrames::start(header()).unwrap();
    assert!(writer.announcement(&announcement(7)).is_err());
    let wrong = OriginalSlot {
        slot: Slot(9),
        object: f::bounded_slot(
            1,
            1,
            SlotKind::Admin,
            SlotFrame::Admin(f::creation_change()),
        ),
    };
    assert!(writer.announcement(&wrong).is_err());
    writer.announcement(&announcement(9)).unwrap();
    assert!(writer.announcement(&announcement(9)).is_err());
}

#[test]
fn floor_prefix_preserves_original_key_copy_objects_and_rejects_lengths_before_allocation() {
    let copy = OriginalSlot {
        slot: Slot(2),
        object: crate::tests::hex(include_str!("../fixtures/sealed-store-key.hex")),
    };
    let (mut writer, first) = FloorPrefixEncoder::start(f::store_key().id(), Slot(6), 1).unwrap();
    let mut reader = FloorPrefixDecoder::start(&first).unwrap();
    assert_eq!(reader.key, f::store_key().id());
    assert_eq!(reader.first, Slot(6));
    assert_eq!(first.len(), FloorPrefixDecoder::HEADER_LENGTH);
    let bytes = writer.copy(&copy).unwrap();
    assert_eq!(reader.copy_length(&bytes[..12]).unwrap(), bytes.len());
    let mut invalid = bytes.clone();
    invalid[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(reader.copy_length(&invalid[..12]).is_err());
    assert_eq!(reader.copy(&bytes).unwrap(), copy);
    assert_eq!(reader.remaining(), 0);
    assert!(reader.copy(&bytes).is_err());
    writer.finish().unwrap();
    reader.finish().unwrap();
    let (writer, first) = FloorPrefixEncoder::start(f::store_key().id(), Slot(6), 1).unwrap();
    assert!(writer.finish().is_err());
    assert!(FloorPrefixDecoder::start(&first).unwrap().finish().is_err());
    assert!(FloorPrefixEncoder::start(f::store_key().id(), Slot(6), 65537).is_err());
    assert!(FloorPrefixEncoder::start(f::store_key().id(), Slot(0), 0).is_err());
    let mut invalid = first;
    invalid[19..27].fill(0);
    assert!(FloorPrefixDecoder::start(&invalid).is_err());
}
