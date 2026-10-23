use super::*;
use crate::test_utils::STORE_ID;

#[test]
fn sequence_slots_have_one_store_wide_identity_and_floor_is_replaceable() {
    for number in [1, u64::MAX] {
        let path = ObjectPath::slot(STORE_ID, NonZeroU64::new(number).unwrap());
        assert_eq!(path.as_str(), format!("{STORE_ID}/seq/{number}"));
        assert_eq!(path.slot_number(), Some(crate::value::Slot(number)));
        assert_eq!(path.device(), None);
        assert!(!path.is_replaceable());
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
    }
    let floor = ObjectPath::floor(STORE_ID);
    assert_eq!(floor.as_str(), format!("{STORE_ID}/floor"));
    assert!(floor.is_replaceable());
    assert_eq!(floor.device(), None);
    assert_eq!(ObjectPath::parse(floor.as_str()).unwrap(), floor);
    assert!(validate_directory(floor.as_str()).is_err());
    validate_directory(&format!("{STORE_ID}/seq")).unwrap();
    for tail in [
        "seq/0",
        "seq/01",
        "seq/+1",
        "seq/-1",
        "seq/1/extra",
        "seq/18446744073709551616",
        "seq",
        "floor/1",
        "notes.txt",
        "Photos/1/1",
    ] {
        assert!(
            ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(),
            "{tail}"
        );
    }
}

#[test]
fn all_layouts_are_canonical_and_store_scoped() {
    let id = uuid::Uuid::from_u128(17);
    let device = DeviceId(42);
    for path in [
        ObjectPath::slot(STORE_ID, NonZeroU64::MIN),
        ObjectPath::floor(STORE_ID),
        ObjectPath::snapshot(STORE_ID, Audience::Store, device, NonZeroU64::MIN),
        ObjectPath::snapshot(
            STORE_ID,
            Audience::Circle(CircleId(id)),
            device,
            NonZeroU64::MIN,
        ),
        ObjectPath::file(STORE_ID, device, FileId(id)),
        ObjectPath::join_request(STORE_ID, InviteId(id)),
    ] {
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
        assert_eq!(path.store(), STORE_ID);
        assert!(ObjectPrefix::all(STORE_ID).contains(&path));
        let parts = path.components();
        for end in 1..parts.len() {
            validate_directory(&parts[..end].join("/")).unwrap();
        }
        assert!(validate_directory(path.as_str()).is_err());
        for bad in [
            "",
            "not-a-store",
            "00112233445566778899aabbccddeeff",
            "00112233-4455-6677-8899-AABBCCDDEEFF",
        ] {
            assert!(ObjectPath::parse(&format!("{bad}/{}", parts[1..].join("/"))).is_err());
        }
    }
}

#[test]
fn file_scans_keep_device_prefixes_and_snapshot_paths_keep_their_audience() {
    let fixture = include_str!("../fixtures/file-reference.txt");
    let path = ObjectPath::parse(fixture.lines().next().unwrap()).unwrap();
    let reference =
        crate::file_reference::FileReference::decode(fixture.lines().nth(1).unwrap()).unwrap();
    assert_eq!(
        ObjectPath::file(STORE_ID, reference.device, reference.id),
        path
    );
    assert_eq!(path.device(), Some(DeviceId(1)));
    assert!(ObjectPrefix::files(STORE_ID).contains(&path));
    assert!(ObjectPrefix::device_files(STORE_ID, DeviceId(1)).contains(&path));
    assert!(!ObjectPrefix::device_files(STORE_ID, DeviceId(2)).contains(&path));
    for audience in [
        Audience::Store,
        Audience::Circle(CircleId(uuid::Uuid::from_u128(17))),
    ] {
        let path = ObjectPath::snapshot(STORE_ID, audience.clone(), DeviceId(42), NonZeroU64::MIN);
        let id = path.snapshot_id().unwrap();
        assert_eq!(id.audience, audience);
        assert_eq!(id.device, DeviceId(42));
        assert_eq!(id.number, 1);
    }
    for tail in [
        "files/01/11111111-1111-1111-1111-111111111111",
        "files/1/no-id",
        "snapshots/42/1",
        "snapshots/store/01/1",
        "snapshots/store/1/0",
        "access-keys/00/11111111-1111-1111-1111-111111111111",
    ] {
        assert!(
            ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(),
            "{tail}"
        );
    }
    for folder in [
        "Photos",
        "notes.txt",
        "seq/1",
        "snapshots/store/01",
        "files/+1",
    ] {
        assert!(
            validate_directory(&format!("{STORE_ID}/{folder}")).is_err(),
            "{folder}"
        );
    }
}

#[test]
fn only_join_request_paths_name_an_invite() {
    let invite = InviteId(uuid::Uuid::from_u128(17));
    assert_eq!(
        ObjectPath::join_request(STORE_ID, invite).join_request_invite(),
        Some(invite)
    );
    for path in [
        ObjectPath::floor(STORE_ID),
        ObjectPath::slot(STORE_ID, NonZeroU64::MIN),
    ] {
        assert_eq!(path.join_request_invite(), None);
    }
}
