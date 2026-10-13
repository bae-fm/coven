use super::*;
use coven_foundation::id_source::DeviceId;
use serde_json::json;

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
