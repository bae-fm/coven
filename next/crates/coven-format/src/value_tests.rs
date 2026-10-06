use super::*;
use crate::{encode_frame, test_utils, Object};

fn bytes<T: Wire>(value: &T) -> Vec<u8> {
    let mut out = Encoder::new();
    value.put(&mut out).unwrap();
    out.bytes
}
#[test]
fn uuid_identities_use_foundations_uuid_byte_order() {
    let raw = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    let uuid = Uuid::from_bytes(raw);
    let invite = InviteId(uuid);
    assert_eq!(bytes(&invite), raw);
    assert_eq!(bytes(&StoreId(uuid)), raw);
    assert_eq!(bytes(&CircleId(uuid)), raw);
    assert_eq!(
        InviteId::get(&mut Decoder::new(&raw).unwrap()).unwrap(),
        invite
    );
}

#[test]
fn member_identity_encoding_uses_cryptos_public_bytes() {
    let member = test_utils::member().signing;
    let raw = member.to_bytes();
    assert_eq!(bytes(&member), raw);
    let text = member.to_string();
    assert_eq!(bytes(&text.parse::<MemberId>().unwrap()), raw);
    assert_eq!(
        MemberId::get(&mut Decoder::new(&raw).unwrap()).unwrap(),
        member
    );
    assert_eq!(
        MemberId::get(&mut Decoder::new(&raw).unwrap())
            .unwrap()
            .to_string(),
        text
    );
}

#[test]
fn timestamps_use_merges_checked_type_and_sort_like_its_fields() {
    let stamp = Timestamp::new(0x0102_0304_0506, 0x0708, DeviceId(0x090a_0b0c_0d0e_0f10)).unwrap();
    assert_eq!(bytes(&stamp), (1..=16).collect::<Vec<_>>());
    assert_eq!(
        Timestamp::get(&mut Decoder::new(&bytes(&stamp)).unwrap()).unwrap(),
        stamp
    );
    let ordered = [
        Timestamp::new(1, 0, DeviceId(0)).unwrap(),
        Timestamp::new(1, 0, DeviceId(u64::MAX)).unwrap(),
        Timestamp::new(1, 1, DeviceId(0)).unwrap(),
        Timestamp::new(2, 0, DeviceId(0)).unwrap(),
    ];
    for pair in ordered.windows(2) {
        assert_eq!(pair[0].cmp(&pair[1]), bytes(&pair[0]).cmp(&bytes(&pair[1])));
    }
    assert!(Timestamp::new(1u64 << 48, 0, DeviceId(0)).is_err());
    let max = Timestamp::new(Timestamp::MAX_MILLISECONDS, u16::MAX, DeviceId(u64::MAX)).unwrap();
    assert_eq!(bytes(&max), [255; 16]);
    assert_eq!(
        Timestamp::get(&mut Decoder::new(&[255; 16]).unwrap()).unwrap(),
        max
    );
}
#[test]
fn positions_are_unique_by_device_and_do_not_sort_by_number() {
    let valid = WritePositions(vec![
        WriteId {
            device: DeviceId(1),
            number: 100,
        },
        WriteId {
            device: DeviceId(2),
            number: 1,
        },
    ]);
    valid.validate().unwrap();
    assert!(valid.covers(WriteId {
        device: DeviceId(1),
        number: 99
    }));
    assert!(!valid.covers(WriteId {
        device: DeviceId(3),
        number: 1
    }));
    let mut unordered = valid.clone();
    unordered.0.reverse();
    assert!(unordered.validate().is_err());
    assert!(WritePositions(vec![valid.0[0], valid.0[0]])
        .validate()
        .is_err());
    assert!(WritePositions(vec![WriteId {
        device: DeviceId(1),
        number: 0
    }])
    .validate()
    .is_err());
    let entries = EntryPositions(vec![
        EntryId {
            device: DeviceId(1),
            number: 100,
        },
        EntryId {
            device: DeviceId(2),
            number: 1,
        },
    ]);
    entries.validate().unwrap();
    assert_eq!(bytes(&entries), bytes(&valid));
}
#[test]
fn sqlite_values_keep_their_storage_classes_outside_keys() {
    for value in [
        Value::Null,
        Value::Integer(i64::MIN),
        Value::Integer(i64::MAX),
        Value::Real(0),
        Value::Real(f64::INFINITY.to_bits()),
        Value::Real(f64::NEG_INFINITY.to_bits()),
        Value::Real(1.25f64.to_bits()),
        Value::Text("日本語\0".into()),
        Value::Blob(vec![0, 255]),
    ] {
        let mut write = test_utils::write();
        write.parts[0].rows[0].change.operation = coven_merge::Operation::Update(
            std::collections::BTreeMap::from([("x".into(), test_utils::column(value))]),
        );
        assert_eq!(
            crate::write_stream::decode_plaintext(&test_utils::write_plaintext(&write).unwrap())
                .unwrap(),
            write
        );
    }
    for bits in [
        f64::NAN.to_bits(),
        (-0.0f64).to_bits(),
        0x7ff0_0000_0000_0001,
    ] {
        let mut write = test_utils::write();
        write.parts[0].rows[0].change.operation = coven_merge::Operation::Update(
            std::collections::BTreeMap::from([("x".into(), test_utils::column(Value::Real(bits)))]),
        );
        assert!(test_utils::write_plaintext(&write).is_err());
        assert!(crate::write::RowChange::decode(
            &encode_frame(13, &write.parts[0].rows[0]).unwrap()
        )
        .is_err());
    }
}
#[test]
fn member_ids_are_validated_by_crypto_in_every_wire_context() {
    for raw in [[0; 32], {
        let mut p = [0; 32];
        p[0] = 1;
        p
    }] {
        assert!(matches!(
            MemberId::get(&mut Decoder::new(&raw).unwrap()),
            Err(Error::InvalidMemberId)
        ));
        let mut join = Object::JoinRequest(crate::objects::JoinRequest {
            invite: InviteId(Uuid::from_bytes([1; 16])),
            keys: test_utils::member(),
            device_name: "D".into(),
        })
        .encode()
        .unwrap();
        join[23..55].copy_from_slice(&raw);
        assert!(matches!(Object::decode(&join), Err(Error::InvalidMemberId)));
        let mut entry = Object::StoreLog(test_utils::store_log()).encode().unwrap();
        entry[39..71].copy_from_slice(&raw);
        assert!(matches!(
            Object::decode(&entry),
            Err(Error::InvalidMemberId)
        ));
    }
}
