use super::*;
#[test]
fn file_names_are_random_ids_and_snapshots_name_their_audience() {
    for path in [
        "files/00112233-4455-6677-8899-aabbccddeeff",
        "snapshots/store/42/3",
        "snapshots/00112233-4455-6677-8899-aabbccddeeff/42/3",
    ] {
        assert!(ObjectPath::parse(path).is_ok(), "{path}");
    }
    for path in [
        format!("files/{}", "ab".repeat(32)),
        "snapshots/42/3".to_owned(),
    ] {
        assert!(ObjectPath::parse(&path).is_err(), "{path}");
    }
}

#[test]
fn prefixes_find_every_device_and_snapshots_retain_the_device() {
    let circle = CircleId(uuid::Uuid::from_u128(17));
    for device in [DeviceId(1), DeviceId(42)] {
        let write = ObjectPath::device_log(device, NonZeroU64::MIN);
        let entry = ObjectPath::store_log(device, NonZeroU64::MIN);
        assert!(ObjectPrefix::device_logs().contains(&write));
        assert!(!ObjectPrefix::device_logs().contains(&entry));
        assert!(ObjectPrefix::store_logs().contains(&entry));
        assert!(!ObjectPrefix::store_logs().contains(&write));
        for snapshot in [
            ObjectPath::snapshot(Audience::Store, device, NonZeroU64::MIN),
            ObjectPath::snapshot(Audience::Circle(circle), device, NonZeroU64::MIN),
        ] {
            assert_eq!(snapshot.device(), Some(device));
            assert_eq!(ObjectPath::parse(snapshot.as_str()).unwrap(), snapshot);
        }
    }
}

#[test]
fn every_layout_round_trips_without_aliases() {
    let device = DeviceId(42);
    let number = NonZeroU64::new(3).unwrap();
    let key = KeyId(uuid::Uuid::from_bytes([0xab; 16]));
    let id = uuid::Uuid::from_u128(17);
    let member = coven_crypto::MemberKeys::generate().unwrap().member_id();
    for path in [
        ObjectPath::device_log(device, number),
        ObjectPath::store_log(device, number),
        ObjectPath::snapshot(Audience::Store, device, number),
        ObjectPath::positions(device),
        ObjectPath::store_key(key, &member),
        ObjectPath::circle_key(CircleId(id), key, &member),
        ObjectPath::join_request(InviteId(id)),
        ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
            [0xab; 16],
        ))),
    ] {
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
        let json = serde_json::to_vec(&path).unwrap();
        assert_eq!(serde_json::from_slice::<ObjectPath>(&json).unwrap(), path);
    }
    assert_eq!(
        ObjectPath::device_log(device, number).as_str(),
        "devices/42/3"
    );
    assert_eq!(
        ObjectPath::store_log(device, number).as_str(),
        "store-log/42/3"
    );
    assert_eq!(
        ObjectPath::snapshot(Audience::Store, device, number).as_str(),
        "snapshots/store/42/3"
    );
    assert_eq!(
        ObjectPath::store_key(key, &member).as_str(),
        format!("keys/store/abababab-abab-abab-abab-abababababab/{member}")
    );
    assert_eq!(
        ObjectPath::circle_key(CircleId(id), key, &member).as_str(),
        format!("keys/circles/{id}/abababab-abab-abab-abab-abababababab/{member}")
    );
    for path in [
        "devices/42/0",
        "devices/042/1",
        "devices/42/01",
        "devices/42/1/extra",
        "devices/../1",
        "keys/store/1/../member",
        "keys/store/1/Member",
        "files/a",
        "/devices/42/1",
        "devices/42/18446744073709551616",
        "join-requests/no-id",
    ] {
        assert!(ObjectPath::parse(path).is_err(), "{path}");
    }
    assert!(
        !ObjectPrefix::device_log(DeviceId(4)).contains(&ObjectPath::device_log(device, number))
    );
    for path in [
        format!("keys/store/{key}/{}", "00".repeat(32)),
        format!("keys/circles/{id}/{key}/{}", "00".repeat(32)),
        format!("keys/store/{key}/{}", member.to_string().to_uppercase()),
        format!("files/{}", "AB".repeat(32)),
        format!("keys/store/1/{member}"),
        format!("keys/store/{}/{member}", key.0.simple()),
        format!("keys/circles/{id}/{}/{member}", key.0.simple()),
        format!("keys/store/{{{key}}}/{member}"),
        format!("keys/circles/{id}/urn:uuid:{key}/{member}"),
        format!("keys/circles/{id}/1/{member}"),
        format!("keys/store/{}/{member}", key.to_string().to_uppercase()),
        format!(
            "keys/circles/{id}/{}/{member}",
            key.to_string().to_uppercase()
        ),
        format!("keys/store/{}/{member}", "a".repeat(31)),
        format!("keys/circles/{id}/{}/{member}", "a".repeat(33)),
    ] {
        assert!(ObjectPath::parse(&path).is_err(), "{path}");
    }
}

#[test]
fn appendix_d_paths_include_snapshot_audience_and_random_file_id() {
    for path in [
        "snapshots/store/42/3",
        "snapshots/abababab-abab-abab-abab-abababababab/42/3",
        "files/abababab-abab-abab-abab-abababababab",
    ] {
        assert!(ObjectPath::parse(path).is_ok(), "{path}");
    }
}
