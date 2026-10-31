use super::*;
use crate::{encode_frame, test_utils, Object};

// The admin frame begins directly with its change tag.
const HEADER: usize = 7;

#[test]
fn device_registration_decodes_optional_replacement() {
    for (replaces, payload) in [None, Some(DeviceId(3))]
        .into_iter()
        .zip([&b"\x04\0\0\0\x01D\0"[..], &b"\x04\0\0\0\x01D\x01\0\0\0\0\0\0\0\x03"[..]])
    {
        let mut bytes = Object::Admin(test_utils::creation_change()).encode().unwrap();
        bytes.truncate(HEADER);
        bytes.extend_from_slice(payload);
        set_length(&mut bytes);
        let expected = frame(StoreChange::AddDevice { name: "D".into(), replaces });
        let decoded = Object::decode(&bytes).expect("D6 registration names an optional old id");
        assert_eq!(decoded, expected);
        assert_eq!(expected.encode().unwrap(), bytes);
    }
}

#[test]
fn device_registration_preserves_full_ids_and_checks_names() {
    for replaces in [None, Some(DeviceId(0)), Some(DeviceId(u64::MAX))] {
        for (name, valid) in [
            ("Phone", true),
            ("电话", true),
            (&"x".repeat(1024), true),
            ("", false),
            ("bad\0name", false),
            (&"x".repeat(1025), false),
        ] {
            let entry = StoreChange::AddDevice { name: name.into(), replaces };
            let object = Object::Admin(entry.clone());
            let encoded = object.encode();
            let decoded = Object::decode(&encode_frame(4, &entry).unwrap());
            if valid {
                assert_eq!(decoded.unwrap(), object);
                assert_eq!(Object::decode(&encoded.unwrap()).unwrap(), object);
            } else {
                assert!(encoded.is_err());
                assert!(decoded.is_err());
            }
        }
    }
}

#[test]
fn device_registration_rejects_malformed_replacement_payloads() {
    for replaces in [None, Some(DeviceId(3))] {
        let bytes = frame(StoreChange::AddDevice { name: "D".into(), replaces }).encode().unwrap();
        // Tag and length-prefixed name precede the D2 option tag.
        let option = HEADER + 1 + 4 + 1;
        for tag in 2..=u8::MAX {
            let mut malformed = bytes.clone();
            malformed[option] = tag;
            assert_eq!(Object::decode(&malformed), Err(Error::UnknownTag { field: "option", tag }));
        }
        for end in option..bytes.len() {
            let mut truncated = bytes[..end].to_vec();
            set_length(&mut truncated);
            assert_eq!(Object::decode(&truncated), Err(Error::Truncated));
        }
        for extra in [1, 8, 16] {
            let mut extended = bytes.clone();
            extended.extend(vec![0; extra]);
            set_length(&mut extended);
            assert_eq!(Object::decode(&extended), Err(Error::TrailingBytes));
        }
    }
}

#[test]
fn removal_payloads_contain_only_their_targets() {
    let member = test_utils::other_member_id();
    for (tag, target) in [
        (2, member.0.as_bytes().to_vec()),
        (10, [circle(1).0.as_bytes().as_slice(), member.0.as_bytes()].concat()),
    ] {
        let mut bytes = Object::Admin(test_utils::creation_change()).encode().unwrap();
        bytes.truncate(HEADER);
        bytes.push(tag);
        bytes.extend(target);
        let length = (bytes.len() - crate::FRAME_PREFIX_LEN) as u32;
        bytes[3..7].copy_from_slice(&length.to_be_bytes());
        let decoded = Object::decode(&bytes).expect("D6 removal has no key payload");
        assert_eq!(decoded.encode().unwrap(), bytes);
    }
}

fn circle(number: u128) -> CircleId {
    CircleId(uuid::Uuid::from_u128(number))
}

fn frame(change: StoreChange) -> Object {
    Object::Admin(change)
}

fn set_length(bytes: &mut [u8]) {
    let length = (bytes.len() - crate::FRAME_PREFIX_LEN) as u32;
    bytes[3..7].copy_from_slice(&length.to_be_bytes());
}

#[test]
fn introduction_layouts_preserve_exact_key_ids_and_hashes() {
    for key_bytes in [[0; 16], std::array::from_fn(|i| i as u8)] {
        for key_hash in [[0; 32], std::array::from_fn(|i| i as u8), [255; 32]] {
            let key = KeyId(uuid::Uuid::from_bytes(key_bytes));
            let changes = [
                StoreChange::CreateStore {
                    store: StoreId(uuid::Uuid::from_bytes([0x10; 16])),
                    name: "S".into(),
                    admin: test_utils::member(),
                    access: crate::MemberAccess::ProviderAccount("a".into()),
                    device_name: "D".into(),
                    key,
                    key_hash,
                },
                StoreChange::CreateCircle { circle: circle(1), name: "C".into(), key, key_hash },
                StoreChange::RotateKey { audience: Audience::Store, key, key_hash },
                StoreChange::RotateKey { audience: Audience::Circle(circle(1)), key, key_hash },
            ];
            let admin = test_utils::member();
            let creation = [
                vec![0],
                vec![0x10; 16],
                b"\0\0\0\x01S".to_vec(),
                admin.signing.as_bytes().to_vec(),
                admin.sealing.as_bytes().to_vec(),
                b"\0\0\0\0\x01a\0\0\0\x01D".to_vec(),
            ]
            .concat();
            let prefixes = [
                creation,
                [vec![6], circle(1).0.as_bytes().to_vec(), b"\0\0\0\x01C".to_vec()].concat(),
                vec![15, 0],
                [vec![15, 1], circle(1).0.as_bytes().to_vec()].concat(),
            ];
            for (change, prefix) in changes.into_iter().zip(prefixes) {
                let object = frame(change);
                let bytes = object.encode().unwrap();
                let expected = [prefix, key_bytes.to_vec(), key_hash.to_vec()].concat();
                assert_eq!(&bytes[HEADER..], expected);
                assert_eq!(Object::decode(&bytes).unwrap(), object);
                for hash_length in 0..32 {
                    let mut shortened = bytes[..bytes.len() - 32 + hash_length].to_vec();
                    set_length(&mut shortened);
                    assert_eq!(Object::decode(&shortened), Err(Error::Truncated));
                }
                let mut overlong = bytes.clone();
                overlong.push(0);
                set_length(&mut overlong);
                assert_eq!(Object::decode(&overlong), Err(Error::TrailingBytes));
            }
        }
    }
}

#[test]
fn store_changes_keep_their_tags_and_reject_truncated_or_extended_payloads() {
    let tags = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 15, 16, 17];
    let changes = test_utils::store_changes();
    assert_eq!(changes.len(), tags.len());
    for (change, tag) in changes.into_iter().zip(tags) {
        let bytes = frame(change).encode().unwrap();
        assert_eq!(bytes[HEADER], tag);
        for end in HEADER + 1..bytes.len() {
            let mut truncated = bytes[..end].to_vec();
            set_length(&mut truncated);
            assert!(Object::decode(&truncated).is_err(), "tag {tag}, end {end}");
        }
        let mut extended = bytes.clone();
        extended.push(0);
        set_length(&mut extended);
        assert_eq!(Object::decode(&extended), Err(Error::TrailingBytes));
        if matches!(tag, 2 | 10) {
            let mut key_payload = bytes;
            key_payload.extend([0; 20]);
            set_length(&mut key_payload);
            assert_eq!(Object::decode(&key_payload), Err(Error::TrailingBytes));
        }
    }
}

#[test]
fn rotation_rejects_unknown_audiences_and_member_ids_need_no_key_validation() {
    let mut rotation = frame(StoreChange::RotateKey {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::nil()),
        key_hash: [0; 32],
    })
    .encode()
    .unwrap();
    rotation[HEADER + 1] = 2;
    assert!(matches!(Object::decode(&rotation), Err(Error::UnknownTag { tag: 2, .. })));
    // A member id is a UUID, not a key: the nil id decodes.
    let mut removal = frame(test_utils::member_removal()).encode().unwrap();
    removal[HEADER + 1..].fill(0);
    assert_eq!(
        Object::decode(&removal).unwrap(),
        frame(StoreChange::RemoveMember { member: MemberId(uuid::Uuid::nil()) })
    );
}

#[test]
fn creation_names_its_writing_device() {
    for device_name in ["Ana’s phone", "", "bad\0name", &"x".repeat(1025)] {
        let mut entry = test_utils::creation_change();
        let StoreChange::CreateStore { device_name: name, .. } = &mut entry else { unreachable!() };
        *name = device_name.into();
        let encoded = Object::Admin(entry.clone()).encode();
        let decoded = Object::decode(&encode_frame(4, &entry).unwrap());
        if device_name == "Ana’s phone" {
            let object = Object::Admin(entry);
            assert_eq!(Object::decode(&encoded.unwrap()).unwrap(), object);
            assert_eq!(decoded.unwrap(), object);
        } else {
            assert!(encoded.is_err());
            assert!(decoded.is_err());
        }
    }
}

#[test]
fn unused_tag_12_is_rejected() {
    let entry = StoreChange::Reset { announcement: crate::value::Slot(7) };
    let mut bytes = Object::Admin(entry).encode().unwrap();
    bytes[HEADER] = 12;
    bytes.splice(HEADER + 1..HEADER + 1, 1u16.to_be_bytes());
    let length = (bytes.len() - crate::FRAME_PREFIX_LEN) as u32;
    bytes[3..7].copy_from_slice(&length.to_be_bytes());
    assert_eq!(Object::decode(&bytes), Err(Error::UnknownTag { field: "store change", tag: 12 }));
}

#[test]
fn every_change_fits_one_request_at_its_field_bounds() {
    for mut change in test_utils::store_changes() {
        match &mut change {
            StoreChange::CreateStore { name, device_name, access, .. } => {
                *name = "n".repeat(1024);
                *device_name = "d".repeat(1024);
                *access = crate::MemberAccess::ProviderAccount("a".repeat(1024));
            }
            StoreChange::AddMember { access, .. } | StoreChange::SetAccess { access, .. } => {
                *access = crate::MemberAccess::S3AccessKey { access_key_id: "a".repeat(1024) };
            }
            StoreChange::RenameStore { name }
            | StoreChange::AddDevice { name, .. }
            | StoreChange::CreateCircle { name, .. }
            | StoreChange::RenameCircle { name, .. } => *name = "n".repeat(1024),
            _ => (),
        }
        let object = Object::Admin(change);
        let frame = object.encode().unwrap();
        let size = frame.len() + 47 + 21 + 4 + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD + 64;
        assert!(size <= crate::sequence::MAX_BOUNDED_SLOT);
        assert_eq!(Object::decode(&frame).unwrap(), object);
    }
}

#[test]
fn member_access_bound_applies_before_decoding_its_declared_bytes() {
    for tag in [0, 1] {
        let access = if tag == 0 {
            crate::MemberAccess::ProviderAccount("a".repeat(1025))
        } else {
            crate::MemberAccess::S3AccessKey { access_key_id: "a".repeat(1025) }
        };
        assert!(matches!(
            frame(StoreChange::SetAccess { member: test_utils::member_id(), access }).encode(),
            Err(Error::Limit { field: "member access", actual: 1025, maximum: 1024 })
        ));
        let mut bytes = frame(StoreChange::SetAccess {
            member: test_utils::member_id(),
            access: crate::MemberAccess::ProviderAccount(String::new()),
        })
        .encode()
        .unwrap();
        // The empty account is the frame's last field: its tag and zero length.
        let at = bytes.len() - 5;
        bytes[at] = tag;
        bytes[at + 1..].copy_from_slice(&1025u32.to_be_bytes());
        assert!(matches!(
            Object::decode(&bytes),
            Err(Error::Limit { field: "member access", actual: 1025, maximum: 1024 })
        ));
    }
}

#[test]
fn member_key_changes_pin_ids_pairs_and_tag_order() {
    let ana = test_utils::member_id();
    let ben = test_utils::other_member_id();
    let pair = |keys: MemberPublicKeys| {
        [keys.signing.as_bytes().as_slice(), keys.sealing.as_bytes().as_slice()].concat()
    };
    let add = frame(StoreChange::AddMember {
        member: ben,
        keys: test_utils::other_member(),
        role: MemberRole::Member,
        access: crate::MemberAccess::ProviderAccount("b".into()),
    })
    .encode()
    .unwrap();
    assert_eq!(
        add[HEADER..],
        [
            vec![1],
            ben.0.as_bytes().to_vec(),
            pair(test_utils::other_member()),
            vec![1, 0, 0, 0, 0, 1, b'b'],
        ]
        .concat()
    );
    let replace = frame(StoreChange::ReplaceMemberKeys {
        member: ana,
        expected: test_utils::member(),
        keys: test_utils::replacement(),
    })
    .encode()
    .unwrap();
    assert_eq!(
        replace[HEADER..],
        [
            vec![16],
            ana.0.as_bytes().to_vec(),
            pair(test_utils::member()),
            pair(test_utils::replacement())
        ]
        .concat()
    );
    assert_eq!(replace.len(), HEADER + 1 + 16 + 128);
    // Every pair field refuses invalid and weak signing points; a sealing key
    // has no point check in the frame.
    let mut weak = [0; 32];
    weak[0] = 1;
    let mut invalid = [0; 32];
    invalid[0] = 2;
    for (bytes, signing) in [(&add, HEADER + 1 + 16), (&replace, HEADER + 1 + 16)] {
        for bad in [weak, invalid] {
            let mut changed = bytes.clone();
            changed[signing..signing + 32].copy_from_slice(&bad);
            assert_eq!(Object::decode(&changed), Err(Error::InvalidSigningKey));
        }
        let mut sealing = bytes.clone();
        sealing[signing + 32..signing + 64].fill(0);
        assert!(Object::decode(&sealing).is_ok());
    }
    let creation = Object::Admin(test_utils::creation_change()).encode().unwrap();
    // Creation's admin pair follows its tag, store id and one-byte name "S".
    let signing = HEADER + 1 + 16 + 5;
    assert_eq!(creation[signing..signing + 64], pair(test_utils::member())[..]);
    let mut changed = creation.clone();
    changed[signing..signing + 32].copy_from_slice(&weak);
    assert_eq!(Object::decode(&changed), Err(Error::InvalidSigningKey));
}

#[test]
fn store_rename_uses_tag_17_and_the_name_bounds() {
    for (name, valid) in [
        ("Family", true),
        ("家", true),
        (&"x".repeat(1024), true),
        ("", false),
        ("bad\0name", false),
        (&"x".repeat(1025), false),
    ] {
        let change = StoreChange::RenameStore { name: name.into() };
        let encoded = change.encode();
        let decoded = StoreChange::decode(&encode_frame(4, &change).unwrap());
        if valid {
            let bytes = encoded.unwrap();
            assert_eq!(bytes[HEADER], 17);
            assert_eq!(&bytes[HEADER + 5..], name.as_bytes());
            assert_eq!(decoded.unwrap(), change);
            assert_eq!(StoreChange::decode(&bytes).unwrap(), change);
        } else {
            assert!(encoded.is_err());
            assert!(decoded.is_err());
        }
    }
}
