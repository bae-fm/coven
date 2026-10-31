use super::*;
use crate::{sequence::SlotFrame, test_utils as f};
use coven_crypto::ObjectHasher;

fn open(bytes: &[u8], path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let object = BoundedSlot::decode(bytes)?;
    let prefix = object.prefix.encode()?;
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    f::member().signing.verify_object(path, &hash.finish(), &object.signature)?;
    let plain = f::store_key().derive().open_object_chunk(path, &prefix, 0, 0, object.chunk)?;
    let (_, frame) = SlotFrame::decode(&plain, &crate::path::ObjectPath::parse(path)?)?;
    object.prefix.kind.check_frame(&frame)?;
    Ok(())
}

#[test]
fn slot_routing_and_encrypted_claim_header_are_authenticated() {
    for (slot, kind, frame) in [
        (1, SlotKind::Admin, SlotFrame::Admin(f::creation_change())),
        (7, SlotKind::Announcement, SlotFrame::Announcement(f::announcement())),
    ] {
        let bytes = f::bounded_slot(slot, slot, kind, frame);
        let path = f::slot_path(slot);
        open(&bytes, path.as_str()).unwrap();
        assert_eq!(BoundedSlot::decode(&bytes).unwrap().encode().unwrap(), bytes);
        assert!(open(&bytes, f::slot_path(slot + 1).as_str()).is_err());
        crate::tests::mutations(&bytes, |changed| {
            if changed != bytes {
                assert!(open(changed, path.as_str()).is_err());
            }
            if let Ok(object) = BoundedSlot::decode(changed) {
                assert_eq!(object.encode().unwrap(), changed);
            }
        });
    }
}

#[test]
fn complete_bounded_slot_limit_counts_prefix_chunk_and_signature() {
    let prefix = SlotObjectPrefix {
        format: crate::FormatVersion::CURRENT,
        key: f::store_key().id(),
        kind: SlotKind::Admin,
    };
    let chunk = vec![0; MAX_BOUNDED_SLOT - 21 - 4 - 64];
    let mut bytes = prefix.encode_chunk(&chunk).unwrap();
    bytes.extend([0; 64]);
    assert_eq!(bytes.len(), MAX_BOUNDED_SLOT);
    BoundedSlot::decode(&bytes).unwrap();
    bytes.push(0);
    assert!(BoundedSlot::decode(&bytes).is_err());
    assert!(prefix.encode_chunk(&vec![0; chunk.len() + 1]).is_err());
    let writes = SlotObjectPrefix { kind: SlotKind::Writes, ..prefix };
    assert!(writes.encode_chunk(&chunk).is_err());
}

#[test]
fn kind_copy_option_and_frame_must_agree() {
    for kind in [SlotKind::Admin, SlotKind::Writes, SlotKind::Announcement] {
        let prefix = SlotObjectPrefix {
            format: crate::FormatVersion::CURRENT,
            key: f::store_key().id(),
            kind,
        };
        let bytes = prefix.encode().unwrap();
        assert_eq!(bytes.len(), 21);
        assert_eq!(SlotObjectPrefix::decode(&bytes).unwrap(), prefix);
        for tag in [1, 2, 255] {
            let mut bad = bytes.clone();
            bad[20] = tag;
            assert!(SlotObjectPrefix::decode(&bad).is_err());
        }
    }
    assert!(SlotKind::Admin.check_frame(&SlotFrame::KeyCopy).is_err());
    assert!(SlotKind::Writes.check_frame(&SlotFrame::Admin(f::creation_change())).is_err());
}
