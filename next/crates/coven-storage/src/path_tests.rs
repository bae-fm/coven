use super::*;
#[test]
fn every_layout_round_trips_without_aliases() {
    let device = DeviceId(42);
    let number = NonZeroU64::new(3).unwrap();
    let id = uuid::Uuid::from_u128(17);
    let member = coven_crypto::MemberKeys::generate().unwrap().member_id();
    for path in [
        ObjectPath::device_log(device, number),
        ObjectPath::store_log(device, number),
        ObjectPath::snapshot(device, number),
        ObjectPath::positions(device),
        ObjectPath::store_key(number, &member),
        ObjectPath::circle_key(CircleId(id), number, &member),
        ObjectPath::join_request(InviteId(id)),
        ObjectPath::file(&StoredFileName::from_bytes([0xab; 32])),
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
        ObjectPath::snapshot(device, number).as_str(),
        "snapshots/42/3"
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
        format!("keys/store/1/{}", "00".repeat(32)),
        format!("keys/circles/{id}/1/{}", "00".repeat(32)),
        format!("keys/store/1/{}", member.to_string().to_uppercase()),
        format!("files/{}", "AB".repeat(32)),
    ] {
        assert!(ObjectPath::parse(&path).is_err(), "{path}");
    }
}
