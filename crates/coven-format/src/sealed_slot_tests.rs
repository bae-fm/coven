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

fn admission_box(
    audience: coven_merge::Audience,
    key: u128,
    member: u8,
) -> crate::sealed_key::CopyBox {
    let routing = crate::sealed_key::KeyCopyPrefix {
        audience,
        key: KeyId(uuid::Uuid::from_u128(key)),
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_bytes([member; 16])),
        sealing: f::other_member().sealing,
    };
    let length =
        coven_crypto::KEY_BOX_HEAD_LEN + routing.material_length() + coven_crypto::KEY_BOX_TAG_LEN;
    crate::sealed_key::CopyBox { routing, sealed: vec![7; length] }
}

/// An admission's prefix carries its boxes in place of the copy flag, and
/// its length follows from them; it holds at least one, all to one recipient
/// and sealing key, in increasing `(audience, key)` order (D9).
#[test]
fn admission_prefix_holds_ordered_boxes_to_one_recipient() {
    use coven_merge::Audience;
    let circle = Audience::Circle(coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(5)));
    let prefix = |copies| SlotObjectPrefix {
        format: crate::FormatVersion::CURRENT,
        key: f::store_key().id(),
        kind: SlotKind::Admission(copies),
    };
    let good = prefix(vec![
        admission_box(Audience::Store, 1, 9),
        admission_box(Audience::Store, 2, 9),
        admission_box(circle.clone(), 1, 9),
    ]);
    let bytes = good.encode().unwrap();
    assert!(SlotObjectPrefix::admission(&bytes).unwrap());
    assert_eq!(SlotObjectPrefix::length(&bytes).unwrap(), bytes.len());
    assert_eq!(SlotObjectPrefix::decode(&bytes).unwrap(), good);
    assert_eq!(good.kind.boxes().count(), 3);
    for bad in [
        vec![],
        vec![admission_box(Audience::Store, 2, 9), admission_box(Audience::Store, 1, 9)],
        vec![admission_box(Audience::Store, 1, 9), admission_box(Audience::Store, 1, 9)],
        vec![admission_box(circle.clone(), 1, 9), admission_box(Audience::Store, 1, 9)],
        vec![admission_box(Audience::Store, 1, 9), admission_box(Audience::Store, 2, 8)],
    ] {
        let bad = prefix(bad);
        assert!(bad.encode().is_err());
        let mut encoded = good.start().unwrap();
        crate::wire::Wire::put(
            &match bad.kind {
                SlotKind::Admission(copies) => copies,
                _ => unreachable!(),
            },
            &mut encoded,
        )
        .unwrap();
        assert!(SlotObjectPrefix::decode(&encoded.bytes).is_err());
    }
    // A prefix cut inside its last box is truncated, not shorter.
    assert!(SlotObjectPrefix::decode(&bytes[..bytes.len() - 1]).is_err());
}

/// Only an admission holds an admit-member or recover-member change, and it
/// holds nothing else (D6, D9).
#[test]
fn admit_and_recover_changes_belong_to_admissions_only() {
    let admission = SlotKind::Admission(vec![admission_box(coven_merge::Audience::Store, 1, 9)]);
    for change in f::store_changes() {
        let admits = matches!(
            change,
            crate::admin::StoreChange::AdmitMember { .. }
                | crate::admin::StoreChange::RecoverMember { .. }
        );
        let frame = SlotFrame::Admin(change);
        assert_eq!(admission.check_frame(&frame).is_ok(), admits);
        assert_eq!(SlotKind::Admin.check_frame(&frame).is_ok(), !admits);
    }
    assert!(admission.check_frame(&SlotFrame::KeyCopy).is_err());
}

/// An admission carries a copy of every store key in history, so about
/// 22,000 of them fit in one; past that the slot doesn't fit, and the
/// operation that would claim it is refused before it claims (D2).
#[test]
fn an_admission_fits_about_twenty_two_thousand_store_keys() {
    let frame = SlotFrame::Admin(f::store_changes().remove(1))
        .encode(&f::slot_header(4, 4), &f::slot_path(4))
        .unwrap()
        .len();
    let admission = |count: u128| SlotObjectPrefix {
        format: crate::FormatVersion::CURRENT,
        key: f::store_key().id(),
        kind: SlotKind::Admission(
            (0..count).map(|key| admission_box(coven_merge::Audience::Store, key, 9)).collect(),
        ),
    };
    assert_eq!(admission(1).encode().unwrap().len(), 20 + 4 + 189);
    assert!(admission(22_000).fits(frame).unwrap());
    assert!(!admission(22_200).fits(frame).unwrap());
}
