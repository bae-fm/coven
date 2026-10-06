use super::*;
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
