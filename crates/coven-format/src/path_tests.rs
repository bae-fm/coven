use super::*;
use crate::test_utils::STORE_ID;

#[test]
fn clock_paths_are_replaceable_device_objects() {
    let path = ObjectPath::parse(&format!("{STORE_ID}/clock/42")).unwrap();
    assert_eq!(path.device(), Some(DeviceId(42)));
    assert!(path.is_replaceable());
    validate_directory(&format!("{STORE_ID}/clock")).unwrap();
    assert!(validate_directory(path.as_str()).is_err());
    assert_eq!(path, ObjectPath::clock(STORE_ID, DeviceId(42)));
    for device in [0, u64::MAX] {
        let path = ObjectPath::clock(STORE_ID, DeviceId(device));
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
        assert_eq!(path.device(), Some(DeviceId(device)));
    }
    for tail in [
        "clock",
        "clock/",
        "clock/01",
        "clock/+1",
        "clock/-1",
        "clock/a",
        "clock/18446744073709551616",
        "clock/1/extra",
        "clock/../1",
        "clocks/1",
    ] {
        assert!(
            ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(),
            "{tail}"
        );
    }
}

#[test]
fn storage_paths_require_a_canonical_store_id() {
    for tail in [
        "devices/1/1".to_owned(),
        "store-log/1/1".to_owned(),
        "positions/1".to_owned(),
        "clock/1".to_owned(),
        "snapshots/store/1/1".to_owned(),
        "files/1/11111111-1111-1111-1111-111111111111".to_owned(),
        "join-requests/11111111-1111-1111-1111-111111111111".to_owned(),
        "keys/1/1".to_owned(),
    ] {
        let scoped = ObjectPath::parse(&format!("{STORE_ID}/{tail}")).unwrap();
        assert_eq!(scoped.store(), STORE_ID);
        assert!(ObjectPrefix::all(STORE_ID).contains(&scoped));
        assert!(ObjectPath::parse(&tail).is_err(), "{tail}");
        for bad in [
            "",
            "not-a-store",
            "00112233445566778899aabbccddeeff",
            "00112233-4455-6677-8899-AABBCCDDEEFF",
            "../",
            "/",
        ] {
            assert!(ObjectPath::parse(&format!("{bad}/{tail}")).is_err());
        }
        let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
        let other_path = ObjectPath::parse(&format!("{other}/{tail}")).unwrap();
        assert_ne!(scoped, other_path);
        assert!(!ObjectPrefix::all(STORE_ID).contains(&other_path));
    }
}

#[test]
fn uploaded_file_fixture_keeps_its_device_in_the_path() {
    let fixture = include_str!("../fixtures/uploaded-file.txt");
    let path = ObjectPath::parse(fixture.lines().next().unwrap()).unwrap();
    let reference =
        crate::file_reference::FileReference::decode(fixture.lines().nth(1).unwrap()).unwrap();
    assert_eq!(
        ObjectPath::file(STORE_ID, reference.device, reference.id),
        path
    );
    assert_eq!(path.device(), Some(DeviceId(1)));
    assert!(ObjectPrefix::files(STORE_ID).contains(&path));
    validate_directory("10101010-1010-1010-1010-101010101010/files/1").unwrap();
    for bad in [
        "10101010-1010-1010-1010-101010101010/files/11111111-1111-1111-1111-111111111111",
        "10101010-1010-1010-1010-101010101010/files/01/11111111-1111-1111-1111-111111111111",
        "10101010-1010-1010-1010-101010101010/files/+1/11111111-1111-1111-1111-111111111111",
        "10101010-1010-1010-1010-101010101010/files/18446744073709551616/11111111-1111-1111-1111-111111111111",
        "10101010-1010-1010-1010-101010101010/files/1/11111111111111111111111111111111",
        "10101010-1010-1010-1010-101010101010/files/1/11111111-1111-1111-1111-111111111111/extra",
    ] {
        assert!(ObjectPath::parse(bad).is_err(), "{bad}");
    }
    for bad in [
        "10101010-1010-1010-1010-101010101010/files/01",
        "10101010-1010-1010-1010-101010101010/files/+1",
        "10101010-1010-1010-1010-101010101010/files/18446744073709551616",
    ] {
        assert!(validate_directory(bad).is_err(), "{bad}");
    }
}

#[test]
fn file_names_are_random_ids_and_snapshots_name_their_audience() {
    for path in [
        "10101010-1010-1010-1010-101010101010/files/31/00112233-4455-6677-8899-aabbccddeeff",
        "10101010-1010-1010-1010-101010101010/snapshots/store/42/3",
        "10101010-1010-1010-1010-101010101010/snapshots/00112233-4455-6677-8899-aabbccddeeff/42/3",
    ] {
        assert!(ObjectPath::parse(path).is_ok(), "{path}");
    }
    for path in [
        format!(
            "10101010-1010-1010-1010-101010101010/files/{}",
            "ab".repeat(32)
        ),
        "10101010-1010-1010-1010-101010101010/snapshots/42/3".to_owned(),
    ] {
        assert!(ObjectPath::parse(&path).is_err(), "{path}");
    }
}

#[test]
fn prefixes_find_every_device_and_snapshots_retain_the_device() {
    let circle = CircleId(uuid::Uuid::from_u128(17));
    for device in [DeviceId(1), DeviceId(42)] {
        let write = ObjectPath::device_log(STORE_ID, device, NonZeroU64::MIN);
        let entry = ObjectPath::store_log(STORE_ID, device, NonZeroU64::MIN);
        assert_eq!(
            write.write_id(),
            Some(coven_merge::WriteId { device, number: 1 })
        );
        assert_eq!(entry.write_id(), None);
        assert_eq!(write.snapshot_id(), None);
        assert!(ObjectPrefix::device_logs(STORE_ID).contains(&write));
        assert!(!ObjectPrefix::device_logs(STORE_ID).contains(&entry));
        assert!(ObjectPrefix::store_logs(STORE_ID).contains(&entry));
        assert!(!ObjectPrefix::store_logs(STORE_ID).contains(&write));
        for snapshot in [
            ObjectPath::snapshot(STORE_ID, Audience::Store, device, NonZeroU64::MIN),
            ObjectPath::snapshot(STORE_ID, Audience::Circle(circle), device, NonZeroU64::MIN),
        ] {
            assert_eq!(snapshot.device(), Some(device));
            assert_eq!(ObjectPath::parse(snapshot.as_str()).unwrap(), snapshot);
            let id = snapshot.snapshot_id().unwrap();
            assert_eq!(id.device, device);
            assert_eq!(id.number, 1);
            assert!(ObjectPrefix::audience_snapshots(STORE_ID, &id.audience).contains(&snapshot));
            assert_eq!(snapshot.write_id(), None);
        }
        assert_eq!(
            ObjectPrefix::audience_snapshots(STORE_ID, &Audience::Store).as_str(),
            "10101010-1010-1010-1010-101010101010/snapshots/store/"
        );
        let circle_prefix = ObjectPrefix::audience_snapshots(STORE_ID, &Audience::Circle(circle));
        assert_eq!(
            circle_prefix.as_str(),
            "10101010-1010-1010-1010-101010101010/snapshots/00000000-0000-0000-0000-000000000011/"
        );
        assert!(!circle_prefix.contains(&ObjectPath::snapshot(
            STORE_ID,
            Audience::Store,
            device,
            NonZeroU64::MIN
        )));
    }
}

#[test]
fn every_layout_round_trips_without_aliases() {
    let device = DeviceId(42);
    let number = NonZeroU64::new(3).unwrap();
    let id = uuid::Uuid::from_u128(17);
    for (path, replaceable) in [
        (ObjectPath::device_log(STORE_ID, device, number), false),
        (ObjectPath::store_log(STORE_ID, device, number), false),
        (
            ObjectPath::snapshot(STORE_ID, Audience::Store, device, number),
            false,
        ),
        (ObjectPath::positions(STORE_ID, device), true),
        (ObjectPath::clock(STORE_ID, device), true),
        (ObjectPath::key_copy(STORE_ID, device, number), false),
        (ObjectPath::join_request(STORE_ID, InviteId(id)), false),
        (
            ObjectPath::file(STORE_ID, device, FileId(uuid::Uuid::from_bytes([0xab; 16]))),
            false,
        ),
    ] {
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
        assert_eq!(path.is_replaceable(), replaceable, "{}", path.as_str());
        assert!(validate_directory(path.as_str()).is_err());
        let parts = path.components();
        for end in 1..parts.len() {
            validate_directory(&parts[..end].join("/")).unwrap();
        }
    }
    assert_eq!(
        ObjectPath::device_log(STORE_ID, device, number).as_str(),
        "10101010-1010-1010-1010-101010101010/devices/42/3"
    );
    assert_eq!(
        ObjectPath::store_log(STORE_ID, device, number).as_str(),
        "10101010-1010-1010-1010-101010101010/store-log/42/3"
    );
    assert_eq!(
        ObjectPath::snapshot(STORE_ID, Audience::Store, device, number).as_str(),
        "10101010-1010-1010-1010-101010101010/snapshots/store/42/3"
    );
    let copy = ObjectPath::key_copy(STORE_ID, device, number);
    assert_eq!(
        copy.as_str(),
        "10101010-1010-1010-1010-101010101010/keys/42/3"
    );
    assert_eq!(copy.key_copy_position(), Some((device, number)));
    assert_eq!(copy.device(), Some(device));
    assert!(!copy.is_replaceable());
    for path in [
        "10101010-1010-1010-1010-101010101010/devices/42/0",
        "10101010-1010-1010-1010-101010101010/devices/042/1",
        "10101010-1010-1010-1010-101010101010/devices/42/01",
        "10101010-1010-1010-1010-101010101010/devices/42/1/extra",
        "10101010-1010-1010-1010-101010101010/devices/../1",
        "10101010-1010-1010-1010-101010101010/keys/store/1/../member",
        "10101010-1010-1010-1010-101010101010/keys/store/1/Member",
        "10101010-1010-1010-1010-101010101010/files/a",
        "/10101010-1010-1010-1010-101010101010/devices/42/1",
        "10101010-1010-1010-1010-101010101010/devices/42/18446744073709551616",
        "10101010-1010-1010-1010-101010101010/join-requests/no-id",
    ] {
        assert!(ObjectPath::parse(path).is_err(), "{path}");
    }
    assert!(!ObjectPrefix::device_log(STORE_ID, DeviceId(4))
        .contains(&ObjectPath::device_log(STORE_ID, device, number)));
    for tail in [
        "keys/42/0",
        "keys/042/1",
        "keys/42/01",
        "keys/42/1/extra",
        "keys/+42/1",
        "keys/42/18446744073709551616",
        "keys/18446744073709551616/1",
        "keys/store/11111111-1111-1111-1111-111111111111/member",
        "keys/circles/11111111-1111-1111-1111-111111111111/key/member",
        "files/ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB",
    ] {
        assert!(
            ObjectPath::parse(&format!("{STORE_ID}/{tail}")).is_err(),
            "{tail}"
        );
    }
    assert!(ObjectPath::parse(&format!(
        "{STORE_ID}/keys/18446744073709551615/18446744073709551615"
    ))
    .is_ok());
}

#[test]
fn appendix_d_paths_include_snapshot_audience_and_random_file_id() {
    for path in [
        "10101010-1010-1010-1010-101010101010/snapshots/store/42/3",
        "10101010-1010-1010-1010-101010101010/snapshots/abababab-abab-abab-abab-abababababab/42/3",
        "10101010-1010-1010-1010-101010101010/files/31/abababab-abab-abab-abab-abababababab",
    ] {
        assert!(ObjectPath::parse(path).is_ok(), "{path}");
    }
}

#[test]
fn provider_folders_cannot_introduce_another_layout_or_alias() {
    for folder in [
        "",
        "/devices",
        "10101010-1010-1010-1010-101010101010/devices/",
        "10101010-1010-1010-1010-101010101010/devices/00",
        "10101010-1010-1010-1010-101010101010/devices/../1",
        "10101010-1010-1010-1010-101010101010/positions/42",
        "10101010-1010-1010-1010-101010101010/snapshots/42",
        "10101010-1010-1010-1010-101010101010/snapshots/store/042",
        "10101010-1010-1010-1010-101010101010/keys/other",
        "10101010-1010-1010-1010-101010101010/keys/store/not-a-key",
        "10101010-1010-1010-1010-101010101010/keys/circles/not-a-circle",
        "vacation",
        "10101010-1010-1010-1010-101010101010/files/a",
    ] {
        assert!(
            matches!(validate_directory(folder), Err(PathError)),
            "{folder}"
        );
    }
}
