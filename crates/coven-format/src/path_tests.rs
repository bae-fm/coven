use super::*;
use crate::test_utils::STORE_ID;
use coven_foundation::id_source::CircleId;

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
        assert!(ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(), "{tail}");
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
        ObjectPath::snapshot(STORE_ID, Audience::Circle(CircleId(id)), device, NonZeroU64::MIN),
        ObjectPath::file(STORE_ID, device, ObjectId(id)),
        ObjectPath::join_request(STORE_ID, device, InviteId(id)),
        ObjectPath::device_gone(STORE_ID, device),
    ] {
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
        assert_eq!(path.store(), STORE_ID);
        assert!(ObjectPrefix::all(STORE_ID).contains(&path));
        assert_eq!(ObjectPrefix::slots(STORE_ID).contains(&path), path.slot_number().is_some());
        assert_eq!(ObjectPrefix::snapshots(STORE_ID).contains(&path), path.snapshot_id().is_some());
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
    assert_eq!(ObjectPath::file(STORE_ID, reference.device, reference.object), path);
    assert_eq!(path.device(), Some(DeviceId(1)));
    assert!(ObjectPrefix::files(STORE_ID).contains(&path));
    assert!(ObjectPrefix::device_files(STORE_ID, DeviceId(1)).contains(&path));
    assert!(!ObjectPrefix::device_files(STORE_ID, DeviceId(2)).contains(&path));
    for audience in [Audience::Store, Audience::Circle(CircleId(uuid::Uuid::from_u128(17)))] {
        let path = ObjectPath::snapshot(STORE_ID, audience.clone(), DeviceId(42), NonZeroU64::MIN);
        let id = path.snapshot_id().unwrap();
        assert_eq!(id.audience, audience);
        assert_eq!(id.device, DeviceId(42));
        assert_eq!(id.number, 1);
        let folder = ObjectPrefix::device_snapshots(STORE_ID, audience.clone(), DeviceId(42));
        assert!(folder.contains(&path));
        for (audience, device) in [
            (audience.clone(), DeviceId(420)),
            (audience.clone(), DeviceId(4)),
            (Audience::Circle(CircleId(uuid::Uuid::from_u128(18))), DeviceId(42)),
        ] {
            let other = ObjectPath::snapshot(STORE_ID, audience, device, NonZeroU64::MIN);
            assert!(!folder.contains(&other));
        }
    }
    assert_eq!(
        ObjectPrefix::device_snapshots(STORE_ID, Audience::Store, DeviceId(42)).as_str(),
        format!("{STORE_ID}/snapshots/store/42/")
    );
    for tail in [
        "files/01/11111111-1111-1111-1111-111111111111",
        "files/1/no-id",
        "snapshots/42/1",
        "snapshots/store/01/1",
        "snapshots/store/1/0",
        "access-keys/00/11111111-1111-1111-1111-111111111111",
    ] {
        assert!(ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(), "{tail}");
    }
    for folder in ["Photos", "notes.txt", "seq/1", "snapshots/store/01", "files/+1"] {
        assert!(validate_directory(&format!("{STORE_ID}/{folder}")).is_err(), "{folder}");
    }
}

#[test]
fn only_join_request_paths_name_an_invite() {
    let invite = InviteId(uuid::Uuid::from_u128(17));
    let path = ObjectPath::join_request(STORE_ID, DeviceId(42), invite);
    assert_eq!(path.as_str(), format!("{STORE_ID}/join-requests/42/{}", invite.0));
    assert_eq!(path.join_request_invite(), Some(invite));
    for path in [ObjectPath::floor(STORE_ID), ObjectPath::slot(STORE_ID, NonZeroU64::MIN)] {
        assert_eq!(path.join_request_invite(), None);
    }
}

/// A join request lives under the device that made its invite, so other
/// devices can find every request of a device that is gone for good (§13.2).
#[test]
fn join_requests_live_under_their_inviting_device() {
    let invite = "11111111-1111-1111-1111-111111111111";
    for tail in [
        format!("join-requests/{invite}"),
        format!("join-requests/01/{invite}"),
        format!("join-requests/+1/{invite}"),
        "join-requests/1/not-an-invite".to_owned(),
        format!("join-requests/1/{invite}/extra"),
    ] {
        assert!(ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(), "{tail}");
    }
    validate_directory(&format!("{STORE_ID}/join-requests/1")).unwrap();
    assert!(validate_directory(&format!("{STORE_ID}/join-requests/01")).is_err());
}

/// A device gone for good has one marker in its join requests' folder. It
/// names no invite, and the folder's prefix finds the device's requests and
/// its marker, and no other device's (§13.2).
#[test]
fn a_gone_devices_marker_sits_beside_its_requests() {
    let invite = InviteId(uuid::Uuid::from_u128(17));
    let gone = ObjectPath::device_gone(STORE_ID, DeviceId(42));
    assert_eq!(gone.as_str(), format!("{STORE_ID}/join-requests/42/gone"));
    assert_eq!(ObjectPath::parse(gone.as_str()).unwrap(), gone);
    assert_eq!(gone.join_request_invite(), None);
    let folder = ObjectPrefix::device_join_requests(STORE_ID, DeviceId(42));
    assert_eq!(folder.as_str(), format!("{STORE_ID}/join-requests/42/"));
    assert!(folder.contains(&gone));
    assert!(folder.contains(&ObjectPath::join_request(STORE_ID, DeviceId(42), invite)));
    for other in [DeviceId(4), DeviceId(420)] {
        assert!(!folder.contains(&ObjectPath::join_request(STORE_ID, other, invite)));
        assert!(!folder.contains(&ObjectPath::device_gone(STORE_ID, other)));
    }
    for tail in [
        "join-requests/gone",
        "join-requests/01/gone",
        "join-requests/1/Gone",
        "join-requests/1/gone/extra",
    ] {
        assert!(ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(), "{tail}");
    }
}

/// Each device's upload temporaries and sessions sit in a reserved folder of
/// their own, which names the device and holds no object path, so whose they
/// are stays known and a listing of published objects skips them (D10).
#[test]
fn each_devices_uploads_sit_in_a_reserved_folder_naming_it() {
    let folder = device_uploads(STORE_ID, DeviceId(42));
    assert_eq!(folder, format!("{STORE_ID}/.coven-uploads/42"));
    let session = format!("{folder}/{}", uuid::Uuid::from_u128(9));
    assert!(ObjectPath::parse(&session).is_err());
    assert!(validate_directory(&folder).is_err());
    assert_ne!(device_uploads(STORE_ID, DeviceId(4)), folder);
}
