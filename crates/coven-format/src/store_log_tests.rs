use super::*;
use crate::{encode_frame, test_utils, Object};

#[test]
fn device_registration_decodes_optional_replacement() {
    for (replaces, payload) in [None, Some(DeviceId(1))].into_iter().zip([
        &b"\x04\0\0\0\0\0\0\0\x02\0\0\0\x01D\0"[..],
        &b"\x04\0\0\0\0\0\0\0\x02\0\0\0\x01D\x01\0\0\0\0\0\0\0\x01"[..],
    ]) {
        let mut bytes = Object::StoreLog(test_utils::store_log()).encode().unwrap();
        bytes.truncate(75);
        bytes.extend_from_slice(payload);
        set_length(&mut bytes);
        let expected = frame(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "D".into(),
            replaces,
        });
        let decoded = Object::decode(&bytes).expect("D6 registration names an optional old id");
        assert_eq!(decoded, expected);
        assert_eq!(expected.encode().unwrap(), bytes);
    }
}

#[test]
fn device_registration_rejects_self_replacement() {
    for device in [DeviceId(0), DeviceId(1), DeviceId(u64::MAX)] {
        let entry = StoreLogEntry {
            change: StoreChange::AddDevice {
                device,
                name: "Phone".into(),
                replaces: Some(device),
            },
            ..test_utils::store_log()
        };
        for result in [
            Object::StoreLog(entry.clone()).encode().map(|_| ()),
            Object::decode(&encode_frame(4, &entry).unwrap()).map(|_| ()),
        ] {
            assert_eq!(
                result,
                Err(Error::Invalid {
                    field: "replacement device",
                    rule: Rule::DeviceReplacement,
                })
            );
        }
    }
}

#[test]
fn device_registration_preserves_full_ids_and_checks_names() {
    for device in [DeviceId(0), DeviceId(u64::MAX)] {
        for replaces in [None, Some(DeviceId(!device.0))] {
            for (name, valid) in [
                ("Phone", true),
                ("电话", true),
                (&"x".repeat(1024), true),
                ("", false),
                ("bad\0name", false),
                (&"x".repeat(1025), false),
            ] {
                let entry = StoreLogEntry {
                    change: StoreChange::AddDevice {
                        device,
                        name: name.into(),
                        replaces,
                    },
                    ..test_utils::store_log()
                };
                let object = Object::StoreLog(entry.clone());
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
}

#[test]
fn device_registration_rejects_malformed_replacement_payloads() {
    for replaces in [None, Some(DeviceId(1))] {
        let bytes = frame(StoreChange::AddDevice {
            device: DeviceId(2),
            name: "D".into(),
            replaces,
        })
        .encode()
        .unwrap();
        // Tag, device and length-prefixed name precede the D2 option tag.
        let option = 75 + 1 + 8 + 4 + 1;
        for tag in 2..=u8::MAX {
            let mut malformed = bytes.clone();
            malformed[option] = tag;
            assert_eq!(
                Object::decode(&malformed),
                Err(Error::UnknownTag {
                    field: "option",
                    tag
                })
            );
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
    let member = test_utils::member().signing;
    for (tag, target) in [
        (2, member.to_bytes().to_vec()),
        (
            10,
            [circle(1).0.as_bytes().as_slice(), &member.to_bytes()].concat(),
        ),
    ] {
        let mut bytes = Object::StoreLog(test_utils::store_log()).encode().unwrap();
        bytes.truncate(75);
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
    Object::StoreLog(StoreLogEntry {
        change,
        ..test_utils::store_log()
    })
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
                StoreChange::CreateCircle {
                    circle: circle(1),
                    name: "C".into(),
                    key,
                    key_hash,
                },
                StoreChange::RotateKey {
                    audience: Audience::Store,
                    key,
                    key_hash,
                },
                StoreChange::RotateKey {
                    audience: Audience::Circle(circle(1)),
                    key,
                    key_hash,
                },
            ];
            let admin = test_utils::member();
            let creation = [
                vec![0],
                vec![0x10; 16],
                b"\0\0\0\x01S".to_vec(),
                admin.signing.to_bytes().to_vec(),
                admin.sealing.as_bytes().to_vec(),
                b"\0\0\0\0\x01a\0\0\0\x01D".to_vec(),
            ]
            .concat();
            let prefixes = [
                creation,
                [
                    vec![6],
                    circle(1).0.as_bytes().to_vec(),
                    b"\0\0\0\x01C".to_vec(),
                ]
                .concat(),
                vec![15, 0],
                [vec![15, 1], circle(1).0.as_bytes().to_vec()].concat(),
            ];
            for (change, prefix) in changes.into_iter().zip(prefixes) {
                let object = frame(change);
                let bytes = object.encode().unwrap();
                let expected = [prefix, key_bytes.to_vec(), key_hash.to_vec()].concat();
                assert_eq!(&bytes[75..], expected);
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
    let tags = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 15];
    let changes = test_utils::store_changes();
    assert_eq!(changes.len(), tags.len());
    for (change, tag) in changes.into_iter().zip(tags) {
        let bytes = frame(change).encode().unwrap();
        assert_eq!(bytes[75], tag);
        for end in 76..bytes.len() {
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
fn rotation_rejects_unknown_audiences_and_removal_rejects_invalid_members() {
    let mut rotation = frame(StoreChange::RotateKey {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::nil()),
        key_hash: [0; 32],
    })
    .encode()
    .unwrap();
    rotation[76] = 2;
    assert!(matches!(
        Object::decode(&rotation),
        Err(Error::UnknownTag { tag: 2, .. })
    ));
    let mut removal = frame(test_utils::member_removal().change).encode().unwrap();
    removal[76..].fill(0);
    assert!(Object::decode(&removal).is_err());
}

#[test]
fn creation_names_its_writing_device() {
    for device_name in ["Ana’s phone", "", "bad\0name", &"x".repeat(1025)] {
        let mut entry = test_utils::store_log();
        let StoreChange::CreateStore {
            device_name: name, ..
        } = &mut entry.change
        else {
            unreachable!()
        };
        *name = device_name.into();
        let encoded = Object::StoreLog(entry.clone()).encode();
        let decoded = Object::decode(&encode_frame(4, &entry).unwrap());
        if device_name == "Ana’s phone" {
            let object = Object::StoreLog(entry);
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
    let mut entry = test_utils::store_log();
    entry.change = StoreChange::Reset {
        snapshot: test_utils::snapshot_header().id,
    };
    let mut bytes = Object::StoreLog(entry).encode().unwrap();
    bytes[75] = 12;
    bytes.splice(76..76, 1u16.to_be_bytes());
    let length = (bytes.len() - crate::FRAME_PREFIX_LEN) as u32;
    bytes[3..7].copy_from_slice(&length.to_be_bytes());
    assert_eq!(
        Object::decode(&bytes),
        Err(Error::UnknownTag {
            field: "store change",
            tag: 12,
        })
    );
}

#[test]
fn had_read_refuses_every_explicit_own_device_position() {
    let mut entry = test_utils::store_log();
    entry.position.number = 3;
    entry.change = StoreChange::RemoveDevice {
        device: DeviceId(2),
    };
    entry.had_read = EntryPositions(vec![EntryId {
        device: DeviceId(1),
        number: 2,
    }]);
    for number in [1, 2, 3, 4] {
        entry.had_read.0[0].number = number;
        assert!(Object::StoreLog(entry.clone()).encode().is_err());
        assert!(matches!(
            Object::decode(&encode_frame(4, &entry).unwrap()),
            Err(Error::Invalid {
                rule: Rule::OwnPosition,
                ..
            })
        ));
    }
}

#[test]
fn first_entry_and_versions_are_checked() {
    let mut entry = test_utils::store_log();
    entry.author = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        .parse()
        .unwrap();
    assert!(matches!(
        Object::decode(&encode_frame(4, &entry).unwrap()),
        Err(Error::Invalid {
            field: "first admin",
            rule: Rule::Required
        })
    ));
    assert!(Object::StoreLog(entry).encode().is_err());
    let entry = StoreLogEntry {
        change: StoreChange::RaiseSchema {
            version: 0,
            snapshot: test_utils::snapshot_header().id,
        },
        ..test_utils::store_log()
    };
    assert!(Object::StoreLog(entry.clone()).encode().is_err());
    assert!(Object::decode(&encode_frame(4, &entry).unwrap()).is_err());
}
