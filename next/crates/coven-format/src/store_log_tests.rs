use super::*;
use crate::{encode_frame, test_utils, Object};

#[test]
fn member_removal_encodes_both_circle_lists_when_empty() {
    let mut entry = test_utils::store_log();
    entry.change = StoreChange::RemoveMember {
        member: test_utils::member().signing,
        key_number: 2,
        circle_keys: vec![],
        deleted_circles: vec![],
    };
    let object = Object::StoreLog(entry);
    let bytes = object.encode().unwrap();
    assert_eq!(bytes.len(), 124);
    assert_eq!(bytes[75], 2);
    assert_eq!(&bytes[108..116], &2u64.to_be_bytes());
    assert_eq!(&bytes[116..], &[0; 8]);
    assert_eq!(Object::decode(&bytes).unwrap(), object);
}

fn circle(number: u128) -> CircleId {
    CircleId(uuid::Uuid::from_u128(number))
}

fn circle_key(circle_number: u128, key_number: u64) -> CircleKeyNumber {
    CircleKeyNumber {
        circle: circle(circle_number),
        key_number,
    }
}

#[test]
fn member_removal_round_trips_independent_circle_lists() {
    for (circle_keys, deleted_circles) in [
        (vec![circle_key(1, u64::MAX), circle_key(3, 1)], vec![]),
        (vec![], vec![circle(2), circle(4)]),
        (
            vec![circle_key(1, 5), circle_key(3, 2)],
            vec![circle(2), circle(4)],
        ),
    ] {
        let mut entry = test_utils::member_removal();
        entry.change = StoreChange::RemoveMember {
            member: test_utils::member().signing,
            key_number: u64::MAX,
            circle_keys,
            deleted_circles,
        };
        let object = Object::StoreLog(entry);
        assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    }
}

#[test]
fn member_removal_rejects_zero_keys_unordered_lists_and_overlap() {
    for (key_number, circle_keys, deleted_circles, expected) in [
        (0, vec![], vec![], Rule::Required),
        (1, vec![circle_key(1, 0)], vec![], Rule::Required),
        (
            1,
            vec![circle_key(1, 2), circle_key(1, 3)],
            vec![],
            Rule::Order,
        ),
        (
            1,
            vec![circle_key(3, 2), circle_key(1, 3)],
            vec![],
            Rule::Order,
        ),
        (1, vec![], vec![circle(2), circle(2)], Rule::Order),
        (1, vec![], vec![circle(4), circle(2)], Rule::Order),
        (
            1,
            vec![circle_key(1, 2)],
            vec![circle(1)],
            Rule::CircleRemoval,
        ),
        (
            1,
            vec![circle_key(1, 2), circle_key(3, 4)],
            vec![circle(2), circle(3), circle(4)],
            Rule::CircleRemoval,
        ),
    ] {
        let mut entry = test_utils::member_removal();
        entry.change = StoreChange::RemoveMember {
            member: test_utils::member().signing,
            key_number,
            circle_keys,
            deleted_circles,
        };
        for result in [
            Object::StoreLog(entry.clone()).encode().map(|_| ()),
            Object::decode(&encode_frame(2, &entry).unwrap()).map(|_| ()),
        ] {
            assert!(
                matches!(result, Err(Error::Invalid { rule, .. }) if rule == expected),
                "{result:?}"
            );
        }
    }
}

#[test]
fn every_store_log_change_round_trips_with_a_pinned_tag() {
    let c = CircleId(uuid::Uuid::from_bytes([2; 16]));
    let m = test_utils::member().signing;
    let snapshot = test_utils::snapshot_header().id;
    let changes = vec![
        test_utils::store_log().change,
        StoreChange::AddMember {
            keys: test_utils::member(),
            role: MemberRole::Member,
        },
        test_utils::member_removal().change,
        StoreChange::ChangeRole {
            member: m.clone(),
            role: MemberRole::Admin,
        },
        StoreChange::AddDevice {
            member: m.clone(),
            device: DeviceId(2),
            name: "D".into(),
        },
        StoreChange::RemoveDevice {
            device: DeviceId(2),
        },
        StoreChange::CreateCircle {
            circle: c,
            name: "C".into(),
            creator: m.clone(),
        },
        StoreChange::RenameCircle {
            circle: c,
            name: "N".into(),
        },
        StoreChange::DeleteCircle { circle: c },
        StoreChange::AddCircleMember {
            circle: c,
            member: m.clone(),
        },
        StoreChange::RemoveCircleMember {
            circle: c,
            member: m.clone(),
            key_number: 2,
        },
        StoreChange::RaiseSchema {
            version: 2,
            snapshot: snapshot.clone(),
        },
        StoreChange::RaiseFormat {
            version: 2,
            snapshot: snapshot.clone(),
        },
        StoreChange::Reset {
            snapshot: SnapshotId {
                audience: Audience::Circle(c),
                ..snapshot.clone()
            },
        },
    ];
    for (tag, change) in changes.into_iter().enumerate() {
        let mut entry = test_utils::store_log();
        entry.change = change;
        let object = Object::StoreLog(entry);
        let bytes = object.encode().unwrap();
        assert_eq!(bytes[75], tag as u8);
        assert_eq!(Object::decode(&bytes).unwrap(), object);
    }
}

#[test]
fn had_read_can_name_earlier_entries_on_the_same_device() {
    let mut entry = test_utils::store_log();
    entry.position.number = 3;
    entry.change = StoreChange::RemoveDevice {
        device: DeviceId(2),
    };
    entry.had_read = EntryPositions(vec![EntryId {
        device: DeviceId(1),
        number: 2,
    }]);
    let object = Object::StoreLog(entry.clone());
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    for number in [3, 4] {
        entry.had_read.0[0].number = number;
        assert!(matches!(
            Object::decode(&encode_frame(2, &entry).unwrap()),
            Err(Error::Invalid {
                rule: Rule::OwnPosition,
                ..
            })
        ));
    }
}

#[test]
fn first_entry_versions_and_key_numbers_are_checked() {
    let mut entry = test_utils::store_log();
    entry.author = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        .parse()
        .unwrap();
    assert!(Object::StoreLog(entry).encode().is_err());
    let snapshot = test_utils::snapshot_header().id;
    for change in [
        StoreChange::RaiseSchema {
            version: 0,
            snapshot: snapshot.clone(),
        },
        StoreChange::RaiseFormat {
            version: 0,
            snapshot: snapshot.clone(),
        },
        StoreChange::RemoveMember {
            member: test_utils::member().signing,
            key_number: 0,
            circle_keys: vec![],
            deleted_circles: vec![],
        },
    ] {
        let mut entry = test_utils::store_log();
        entry.change = change;
        assert!(Object::decode(&encode_frame(2, &entry).unwrap()).is_err());
    }
}
