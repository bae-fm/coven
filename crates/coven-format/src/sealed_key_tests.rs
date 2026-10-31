use super::*;
use crate::{
    sealed_slot::{BoundedSlot, SlotKind, SlotObjectPrefix},
    sequence::SlotFrame,
    test_utils as f,
};
use coven_crypto::{seal_circle_key, seal_store_key};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

fn routing(audience: Audience) -> KeyCopyPrefix {
    KeyCopyPrefix {
        audience,
        key: KeyId(Uuid::from_u128(3)),
        member: f::member_id(),
        sealing: f::member().sealing,
    }
}
fn context(routing: &KeyCopyPrefix) -> Vec<u8> {
    SlotObjectPrefix::copy_context(crate::FormatVersion::CURRENT, f::store_key().id(), routing)
        .unwrap()
}
fn seal(routing: KeyCopyPrefix, audience: Audience, key: KeyId) -> CopyBox {
    let path = f::slot_path(2);
    let context = context(&routing);
    let sealed = match audience {
        Audience::Store => seal_store_key(
            &StoreKey::from_bytes(key, [17; 32]),
            &routing.sealing,
            path.as_str(),
            &context,
        ),
        Audience::Circle(circle) => seal_circle_key(
            &CircleKey::from_bytes(circle, key, [18; 32]),
            &routing.sealing,
            path.as_str(),
            &context,
        ),
    }
    .unwrap();
    CopyBox { routing, sealed }
}
fn open(copy: &CopyBox, member: &MemberIdentity, path: &str) -> Result<(), CryptoError> {
    match copy.routing.audience {
        Audience::Store => copy.open_store_key(member, path, &context(&copy.routing)).map(|_| ()),
        Audience::Circle(_) => {
            copy.open_circle_key(member, path, &context(&copy.routing)).map(|_| ())
        }
    }
}

#[test]
fn key_boxes_bind_slot_store_key_and_every_recipient_routing_field() {
    for audience in [Audience::Store, Audience::Circle(CircleId(Uuid::from_u128(2)))] {
        let routing = routing(audience.clone());
        let copy = seal(routing.clone(), audience.clone(), routing.key);
        let path = f::slot_path(2);
        open(&copy, &f::identity(), path.as_str()).unwrap();
        assert!(open(&copy, &f::identity(), f::slot_path(3).as_str()).is_err());
        let other = MemberIdentity::new(f::other_member_id(), f::member_keys());
        assert!(open(&copy, &other, path.as_str()).is_err());
        let replaced = MemberIdentity::new(f::member_id(), f::replacement_keys());
        assert!(open(&copy, &replaced, path.as_str()).is_err());
        let mut variants = vec![
            KeyCopyPrefix { key: KeyId(Uuid::from_u128(4)), ..routing.clone() },
            KeyCopyPrefix { member: other.member(), ..routing.clone() },
            KeyCopyPrefix { sealing: f::replacement().sealing, ..routing.clone() },
        ];
        if matches!(audience, Audience::Circle(_)) {
            variants.push(KeyCopyPrefix {
                audience: Audience::Circle(CircleId(Uuid::from_u128(5))),
                ..routing.clone()
            });
        }
        for changed in variants {
            let copy = CopyBox { routing: changed, sealed: copy.sealed.clone() };
            assert!(open(&copy, &f::identity(), path.as_str()).is_err());
            // Also verify the box authentication itself, beyond recipient selection.
            let result = match audience {
                Audience::Store => f::member_keys()
                    .open_store_key(path.as_str(), &context(&copy.routing), &copy.sealed)
                    .map(|_| ()),
                Audience::Circle(_) => f::member_keys()
                    .open_circle_key(path.as_str(), &context(&copy.routing), &copy.sealed)
                    .map(|_| ()),
            };
            assert!(result.is_err());
        }
        let changed = SlotObjectPrefix::copy_context(
            crate::FormatVersion::CURRENT,
            KeyId(Uuid::from_u128(99)),
            &routing,
        )
        .unwrap();
        let result = match audience {
            Audience::Store => {
                copy.open_store_key(&f::identity(), path.as_str(), &changed).map(|_| ())
            }
            Audience::Circle(_) => {
                copy.open_circle_key(&f::identity(), path.as_str(), &changed).map(|_| ())
            }
        };
        assert!(result.is_err());
        let prefix = SlotObjectPrefix {
            format: crate::FormatVersion::CURRENT,
            key: f::store_key().id(),
            kind: SlotKind::KeyCopy(copy),
        };
        let bytes = prefix.encode().unwrap();
        assert_eq!(SlotObjectPrefix::decode(&bytes).unwrap(), prefix);
        assert!(bytes.len() <= SlotObjectPrefix::MAX_LENGTH);
        for end in 0..bytes.len() {
            assert!(SlotObjectPrefix::decode(&bytes[..end]).is_err());
        }
        let mut bad = bytes.clone();
        let at = context(&routing).len() + 56;
        bad[at..at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(SlotObjectPrefix::length(&bad).is_err());
        let mut bad = bytes;
        bad.push(0);
        assert!(SlotObjectPrefix::decode(&bad).is_err());
    }
}

#[test]
fn authenticated_material_must_match_routing_and_introduction() {
    let path = f::slot_path(2);
    for audience in [Audience::Store, Audience::Circle(CircleId(Uuid::from_u128(2)))] {
        let routing = routing(audience.clone());
        let wrong = seal(routing.clone(), audience.clone(), KeyId(Uuid::from_u128(4)));
        assert!(matches!(
            open(&wrong, &f::identity(), path.as_str()),
            Err(CryptoError::KeyIdentity)
        ));
        let copy = seal(routing.clone(), audience.clone(), routing.key);
        let wrong_hash = StoreKey::from_bytes(routing.key, [19; 32]).hash();
        let result = match audience {
            Audience::Store => copy
                .open_store_key(&f::identity(), path.as_str(), &context(&routing))
                .unwrap()
                .check_hash(&wrong_hash),
            Audience::Circle(_) => copy
                .open_circle_key(&f::identity(), path.as_str(), &context(&routing))
                .unwrap()
                .check_hash(&wrong_hash),
        };
        assert!(result.is_err());
    }
    let routing = routing(Audience::Circle(CircleId(Uuid::from_u128(2))));
    let copy = seal(routing.clone(), Audience::Circle(CircleId(Uuid::from_u128(5))), routing.key);
    assert!(matches!(open(&copy, &f::identity(), path.as_str()), Err(CryptoError::KeyIdentity)));
}

#[test]
fn key_copy_fixtures_are_signed_slots_with_recoverable_boxes() {
    for (slot, text, member) in [
        (2, include_str!("../fixtures/sealed-store-key.hex"), f::identity()),
        (
            8,
            include_str!("../fixtures/sealed-circle-key.hex"),
            MemberIdentity::new(f::member_id(), f::replacement_keys()),
        ),
    ] {
        let bytes = crate::tests::hex(text);
        let path = f::slot_path(slot);
        let check = |bytes: &[u8]| -> Result<(), Box<dyn std::error::Error>> {
            let envelope = BoundedSlot::decode(bytes)?;
            let SlotKind::KeyCopy(copy) = &envelope.prefix.kind else {
                return Err("expected copy".into());
            };
            let context = SlotObjectPrefix::copy_context(
                envelope.prefix.format,
                envelope.prefix.key,
                &copy.routing,
            )?;
            match copy.routing.audience {
                Audience::Store => {
                    copy.open_store_key(&member, path.as_str(), &context)?;
                }
                Audience::Circle(_) => {
                    copy.open_circle_key(&member, path.as_str(), &context)?;
                }
            }
            let mut hash = coven_crypto::ObjectHasher::new();
            hash.update(&bytes[..bytes.len() - 64]);
            f::member().signing.verify_object(
                path.as_str(),
                &hash.finish(),
                &envelope.signature,
            )?;
            let plain = f::store_key().derive().open_object_chunk(
                path.as_str(),
                &envelope.prefix.encode()?,
                0,
                0,
                envelope.chunk,
            )?;
            let (_, frame) = SlotFrame::decode(&plain, &path)?;
            assert_eq!(frame, SlotFrame::KeyCopy);
            assert_eq!(envelope.encode()?, bytes);
            Ok(())
        };
        check(&bytes).unwrap();
        crate::tests::mutations(&bytes, |changed| {
            if changed != bytes {
                assert!(check(changed).is_err());
            }
        });
    }
}
