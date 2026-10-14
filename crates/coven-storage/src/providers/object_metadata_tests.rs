use super::*;
use coven_foundation::id_source::DeviceId;
use serde_json::json;

#[test]
fn onedrive_immutable_publication_uses_copy_modification_time() {
    let path = ObjectPath::device_log(
        crate::test_utils::STORE_ID,
        DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let item = json!({"id":"copied", "name":path.file_name(), "file":{}, "size":4, "createdDateTime":"2026-10-06T00:00:00Z", "lastModifiedDateTime":"2026-10-06T00:10:00.123Z", "eTag":"copied-revision"});
    let observed = onedrive(&path, &item).unwrap();
    assert_eq!(
        observed.stored_at,
        http::timestamp(&item, "lastModifiedDateTime").unwrap()
    );
    assert_eq!(observed.time_resolution, Duration::from_millis(1));
}

#[test]
fn provider_times_retain_precision_and_replaceable_paths_use_modification() {
    let positions = ObjectPath::positions(crate::test_utils::STORE_ID, DeviceId(1));
    let immutable = ObjectPath::store_log(
        crate::test_utils::STORE_ID,
        DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    let mut drive_item = json!({"id":"native", "name":positions.file_name(), "size":"4", "createdTime":"2026-10-06T00:00:00.001Z", "modifiedTime":"2026-10-06T00:00:00.1234567Z", "version":"7", "headRevisionId":"content-7", "md5Checksum":"0123456789abcdef0123456789abcdef"});
    let replaced = drive(&positions, &drive_item).unwrap();
    drive_item["version"] = json!("8");
    assert_eq!(drive(&positions, &drive_item).unwrap(), replaced);
    assert_eq!(replaced.time_resolution, Duration::from_nanos(100));
    assert_eq!(
        replaced.stored_at,
        http::timestamp(&drive_item, "modifiedTime").unwrap()
    );
    assert!(matches!(
        replaced.checksum,
        Some(ObjectChecksum::FullObject(ChecksumValue::Md5Hex(_)))
    ));
    drive_item["name"] = json!(immutable.file_name());
    let first = drive(&immutable, &drive_item).unwrap();
    assert_eq!(first.time_resolution, Duration::from_millis(1));
    assert!(first.stored_at < replaced.stored_at);
    let item = json!({"id":"native", "name":positions.file_name(), "file":{"hashes":{"quickXorHash":"AAAAAA=="}}, "size":4, "createdDateTime":"2026-10-06T00:00:00Z", "lastModifiedDateTime":"2026-10-06T00:00:00.120Z", "eTag":"new"});
    let one = onedrive(&positions, &item).unwrap();
    assert_eq!(one.time_resolution, Duration::from_millis(1));
    assert_eq!(
        one.stored_at,
        http::timestamp(&item, "lastModifiedDateTime").unwrap()
    );
    assert_eq!(one.revision, "new");
}

#[test]
fn absent_checksums_are_optional_but_malformed_metadata_is_not() {
    let path = ObjectPath::positions(crate::test_utils::STORE_ID, DeviceId(1));
    let item = json!({"id":"native", "path_lower":path.absolute(), "size":4, "server_modified":"2026-10-06T00:00:00Z", "rev":"new"});
    assert_eq!(dropbox(&path, &item).unwrap().checksum, None);
    for field in ["id", "rev", "server_modified", "size"] {
        let mut invalid = item.clone();
        invalid[field] = Value::Null;
        assert!(dropbox(&path, &invalid).is_err());
    }
    let mut malformed = item;
    malformed["content_hash"] = Value::Null;
    assert!(dropbox(&path, &malformed).is_err());
}

#[test]
fn native_hash_fields_determine_algorithm_encoding_and_whole_object_scope() {
    let path = ObjectPath::positions(crate::test_utils::STORE_ID, DeviceId(1));
    let mut item = json!({"id":"native", "path_lower":path.absolute(), "size":4,
        "server_modified":"1970-01-01T00:00:01Z", "rev":"revision",
        "content_hash":"0123456789abcdef".repeat(4)});
    assert_eq!(
        dropbox(&path, &item).unwrap().checksum,
        Some(ObjectChecksum::FullObject(
            ChecksumValue::DropboxContentHash("0123456789abcdef".repeat(4))
        ))
    );
    item = json!({"id":"native", "name":path.file_name(), "size":4,
        "file":{"hashes":{"sha1Hash":"ab".repeat(20)}},
        "lastModifiedDateTime":"1970-01-01T00:00:01Z", "eTag":"opaque"});
    assert_eq!(
        onedrive(&path, &item).unwrap().checksum,
        Some(ObjectChecksum::FullObject(ChecksumValue::Sha1Hex(
            "ab".repeat(20)
        )))
    );
    item["file"]["hashes"] = json!({"quickXorHash":"AAECAwQFBgcICQoLDA0ODxAREhM="});
    assert_eq!(
        onedrive(&path, &item).unwrap().checksum,
        Some(ObjectChecksum::FullObject(ChecksumValue::QuickXorBase64(
            "AAECAwQFBgcICQoLDA0ODxAREhM=".into()
        )))
    );
    item["file"]["hashes"] = json!({});
    let observed = onedrive(&path, &item).unwrap();
    assert_eq!(observed.revision, "opaque");
    assert_eq!(observed.checksum, None);
}

#[test]
fn account_timestamps_preserve_each_returned_fractional_digit() {
    let path = ObjectPath::positions(crate::test_utils::STORE_ID, DeviceId(1));
    for (timestamp, nanos, resolution_nanos) in [
        ("1970-01-01T00:00:01Z", 0, 1_000_000_000),
        ("1970-01-01T00:00:01.1Z", 100_000_000, 100_000_000),
        ("1970-01-01T00:00:01.120Z", 120_000_000, 1_000_000),
        ("1970-01-01T00:00:01.123456789Z", 123_456_789, 1),
    ] {
        let item = json!({"id":"native", "name":path.file_name(), "file":{}, "size":4,
            "lastModifiedDateTime":timestamp, "eTag":"revision"});
        let one = onedrive(&path, &item).unwrap();
        let item = json!({"id":"native", "name":path.file_name(), "size":"4",
            "modifiedTime":timestamp, "headRevisionId":"revision"});
        let google = drive(&path, &item).unwrap();
        // StorageTime keeps the digits a 100 ns Windows SystemTime would drop.
        for observed in [one, google] {
            assert_eq!(
                observed.stored_at,
                coven_format::value::StorageTime::new(1, nanos).unwrap()
            );
            assert_eq!(
                observed.time_resolution,
                Duration::from_nanos(resolution_nanos)
            );
        }
    }
}

#[test]
fn missing_native_identity_size_time_or_revision_is_a_protocol_failure() {
    let path = ObjectPath::positions(crate::test_utils::STORE_ID, DeviceId(1));
    for (provider, item, fields) in [
        (
            CloudProvider::GoogleDrive,
            json!({"id":"native", "name":path.file_name(), "size":"4", "modifiedTime":"1970-01-01T00:00:01Z", "headRevisionId":"revision"}),
            ["id", "size", "modifiedTime", "headRevisionId"],
        ),
        (
            CloudProvider::OneDrive,
            json!({"id":"native", "name":path.file_name(), "file":{}, "size":4, "lastModifiedDateTime":"1970-01-01T00:00:01Z", "eTag":"revision"}),
            ["id", "size", "lastModifiedDateTime", "eTag"],
        ),
        (
            CloudProvider::Dropbox,
            json!({"id":"native", "path_lower":path.absolute(), "size":4, "server_modified":"1970-01-01T00:00:01Z", "rev":"revision"}),
            ["id", "size", "server_modified", "rev"],
        ),
    ] {
        let metadata = |item: &Value| match provider {
            CloudProvider::GoogleDrive => drive(&path, item),
            CloudProvider::OneDrive => onedrive(&path, item),
            CloudProvider::Dropbox => dropbox(&path, item),
            _ => unreachable!("account metadata cases"),
        };
        metadata(&item).unwrap();
        for field in fields {
            let mut invalid = item.clone();
            invalid.as_object_mut().unwrap().remove(field);
            assert_eq!(
                metadata(&invalid).unwrap_err().failure(),
                StorageFailure::Protocol
            );
        }
    }
}
