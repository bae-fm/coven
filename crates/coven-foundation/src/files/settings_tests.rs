use super::*;
use uuid::Uuid;

#[test]
fn settings_round_trip_all_fields_and_replace_as_one_file() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = StoreSettings {
        id: StoreId(Uuid::from_u128(42)),
        name: "家庭\nnotes".into(),
        device_id: DeviceId(u64::MAX),
    };
    write(directory.path(), &settings).unwrap();
    assert_eq!(read(directory.path(), settings.id).unwrap(), settings);
    settings.name = "Another name".into();
    settings.device_id = DeviceId(12);
    write(directory.path(), &settings).unwrap();
    assert_eq!(read(directory.path(), settings.id).unwrap(), settings);
}

#[test]
fn corruption_and_missing_fields_are_typed_errors() {
    let directory = tempfile::tempdir().unwrap();
    let id = StoreId(Uuid::from_u128(42));
    assert!(
        matches!(read(directory.path(), id), Err(SettingsError::Missing(found)) if found == id)
    );
    for invalid in [b"not json".as_slice(), b"{}", br#"{"id":"not-a-uuid","name":"x","device_id":3}"#, br#"{"id":"00000000-0000-0000-0000-00000000002a","name":"x","device_id":18446744073709551616}"#] {
        std::fs::write(directory.path().join(SETTINGS_FILE), invalid).unwrap();
        assert!(matches!(read(directory.path(), id), Err(SettingsError::Corrupt(_))));
    }
}

#[test]
fn settings_cannot_identify_another_store() {
    let directory = tempfile::tempdir().unwrap();
    let actual = StoreId(Uuid::from_u128(1));
    let expected = StoreId(Uuid::from_u128(2));
    write(
        directory.path(),
        &StoreSettings {
            id: actual,
            name: "one".into(),
            device_id: DeviceId(7),
        },
    )
    .unwrap();
    assert!(
        matches!(read(directory.path(), expected), Err(SettingsError::WrongStore { actual: a, expected: e }) if a == actual && e == expected)
    );
}

#[test]
fn a_directory_at_the_settings_path_is_not_valid_settings() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join(SETTINGS_FILE)).unwrap();
    assert!(matches!(
        read(directory.path(), StoreId(Uuid::nil())),
        Err(SettingsError::NotRegularFile(_))
    ));
}
