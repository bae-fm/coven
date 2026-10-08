use super::*;
use crate::{encode_frame, test_utils, Object};

#[test]
fn entries_preserve_every_key_id_byte_including_zero() {
    for key in [
        KeyId(uuid::Uuid::from_bytes([0; 16])),
        KeyId(uuid::Uuid::from_bytes(std::array::from_fn(|i| i as u8))),
    ] {
        let changes = [
            StoreChange::CreateStore {
                access: crate::MemberAccess::S3AccessKey {
                    access_key_id: "fixture-access-key".into(),
                },
                store: StoreId(uuid::Uuid::from_u128(1)),
                name: "S".into(),
                admin: test_utils::member(),
                device_name: "D".into(),
                key,
            },
            StoreChange::CreateCircle {
                circle: circle(1),
                name: "C".into(),
                key,
            },
            StoreChange::RemoveMember {
                member: test_utils::member().signing,
                key,
                circle_keys: vec![CircleKeyId {
                    circle: circle(1),
                    key,
                }],
            },
            StoreChange::RemoveCircleMember {
                circle: circle(1),
                member: test_utils::member().signing,
                key,
            },
        ];
        for change in changes {
            let object = Object::StoreLog(StoreLogEntry {
                change,
                ..test_utils::store_log()
            });
            assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
        }
    }
}

fn circle(number: u128) -> CircleId {
    CircleId(uuid::Uuid::from_u128(number))
}

fn circle_key(circle_number: u128, key: u8) -> CircleKeyId {
    CircleKeyId {
        circle: circle(circle_number),
        key: KeyId(uuid::Uuid::from_bytes([key; 16])),
    }
}

#[test]
fn member_removal_round_trips_replacement_keys() {
    for circle_keys in [vec![], vec![circle_key(1, u8::MAX), circle_key(3, 1)]] {
        let mut entry = test_utils::member_removal();
        entry.change = StoreChange::RemoveMember {
            member: test_utils::member().signing,
            key: KeyId(uuid::Uuid::from_bytes([u8::MAX; 16])),
            circle_keys,
        };
        let object = Object::StoreLog(entry);
        assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    }
}

#[test]
fn member_removal_rejects_repeated_or_unordered_replacement_circles() {
    for circle_keys in [
        vec![circle_key(1, 2), circle_key(1, 3)],
        vec![circle_key(3, 2), circle_key(1, 3)],
    ] {
        let mut entry = test_utils::member_removal();
        entry.change = StoreChange::RemoveMember {
            member: test_utils::member().signing,
            key: KeyId(uuid::Uuid::from_bytes([1; 16])),
            circle_keys,
        };
        for result in [
            Object::StoreLog(entry.clone()).encode().map(|_| ()),
            Object::decode(&encode_frame(4, &entry).unwrap()).map(|_| ()),
        ] {
            assert!(
                matches!(
                    result,
                    Err(Error::Invalid {
                        rule: Rule::Order,
                        ..
                    })
                ),
                "{result:?}"
            );
        }
    }
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
