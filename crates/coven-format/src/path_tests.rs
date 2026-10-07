use super::*;

#[test]
fn uploaded_file_fixture_keeps_its_device_in_the_path() {
    let fixture = include_str!("../fixtures/uploaded-file.txt");
    let path = ObjectPath::parse(fixture.lines().next().unwrap()).unwrap();
    let reference =
        crate::file_reference::UploadedFileReference::decode(fixture.lines().nth(1).unwrap())
            .unwrap();
    assert_eq!(ObjectPath::file(reference.device, reference.id), path);
    assert_eq!(path.device(), Some(DeviceId(1)));
    assert!(ObjectPrefix::files().contains(&path));
    validate_directory("files/1").unwrap();
    for bad in [
        "files/11111111-1111-1111-1111-111111111111",
        "files/01/11111111-1111-1111-1111-111111111111",
        "files/+1/11111111-1111-1111-1111-111111111111",
        "files/18446744073709551616/11111111-1111-1111-1111-111111111111",
        "files/1/11111111111111111111111111111111",
        "files/1/11111111-1111-1111-1111-111111111111/extra",
    ] {
        assert!(ObjectPath::parse(bad).is_err(), "{bad}");
    }
    for bad in ["files/01", "files/+1", "files/18446744073709551616"] {
        assert!(validate_directory(bad).is_err(), "{bad}");
    }
}

#[test]
fn file_names_are_random_ids_and_snapshots_name_their_audience() {
    for path in [
        "files/31/00112233-4455-6677-8899-aabbccddeeff",
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
        assert_eq!(
            write.write_id(),
            Some(coven_merge::WriteId { device, number: 1 })
        );
        assert_eq!(entry.write_id(), None);
        assert_eq!(write.snapshot_id(), None);
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
            let id = snapshot.snapshot_id().unwrap();
            assert_eq!(id.device, device);
            assert_eq!(id.number, 1);
            assert!(ObjectPrefix::audience_snapshots(&id.audience).contains(&snapshot));
            assert_eq!(snapshot.write_id(), None);
        }
        assert_eq!(
            ObjectPrefix::audience_snapshots(&Audience::Store).as_str(),
            "snapshots/store/"
        );
        let circle_prefix = ObjectPrefix::audience_snapshots(&Audience::Circle(circle));
        assert_eq!(
            circle_prefix.as_str(),
            "snapshots/00000000-0000-0000-0000-000000000011/"
        );
        assert!(!circle_prefix.contains(&ObjectPath::snapshot(
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
        ObjectPath::file(device, FileId(uuid::Uuid::from_bytes([0xab; 16]))),
    ] {
        assert_eq!(ObjectPath::parse(path.as_str()).unwrap(), path);
        assert!(validate_directory(path.as_str()).is_err());
        let parts = path.components();
        for end in 1..parts.len() {
            validate_directory(&parts[..end].join("/")).unwrap();
        }
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
        "files/31/abababab-abab-abab-abab-abababababab",
    ] {
        assert!(ObjectPath::parse(path).is_ok(), "{path}");
    }
}

#[test]
fn provider_folders_cannot_introduce_another_layout_or_alias() {
    for folder in [
        "",
        "/devices",
        "devices/",
        "devices/00",
        "devices/../1",
        "positions/42",
        "snapshots/42",
        "snapshots/store/042",
        "keys/other",
        "keys/store/not-a-key",
        "keys/circles/not-a-circle",
        "vacation",
        "files/a",
    ] {
        assert!(
            matches!(validate_directory(folder), Err(PathError)),
            "{folder}"
        );
    }
}
