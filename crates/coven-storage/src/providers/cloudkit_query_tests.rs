use super::*;
use coven_foundation::id_source::{CircleId, FileId, InviteId};
use coven_merge::Audience;
use std::num::NonZeroU64;

fn store() -> StoreId {
    StoreId(uuid::Uuid::from_u128(1))
}

fn fields(path: &ObjectPath) -> Retained<objc2_foundation::NSDictionary<NSString, NSString>> {
    let mut fields: Vec<_> = directories(path)
        .map(|(key, value)| (NSString::from_str(key), NSString::from_str(value)))
        .collect();
    if let Some((_, number)) = path.store_log_position() {
        fields.push((
            NSString::from_str("number"),
            NSString::from_str(&number.to_string()),
        ));
    }
    objc2_foundation::NSDictionary::from_slices(
        &fields.iter().map(|(key, _)| &**key).collect::<Vec<_>>(),
        &fields.iter().map(|(_, value)| &**value).collect::<Vec<_>>(),
    )
}

#[test]
fn every_prefix_selects_exactly_its_objects_by_directory_equality() {
    let store = store();
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_u128(2)));
    let uuid = uuid::Uuid::from_u128(3);
    let mut paths = Vec::new();
    for store in [store, StoreId(uuid::Uuid::from_u128(4))] {
        for device in [DeviceId(7), DeviceId(70), DeviceId(u64::MAX)] {
            paths.extend([
                ObjectPath::device_log(store, device, NonZeroU64::MIN),
                ObjectPath::store_log(store, device, NonZeroU64::MIN),
                ObjectPath::key_copy(store, device, NonZeroU64::MIN),
                ObjectPath::snapshot(store, Audience::Store, device, NonZeroU64::MIN),
                ObjectPath::snapshot(store, circle.clone(), device, NonZeroU64::MIN),
                ObjectPath::file(store, device, FileId(uuid)),
                ObjectPath::positions(store, device),
                ObjectPath::clock(store, device),
            ]);
        }
        paths.push(ObjectPath::join_request(store, InviteId(uuid)));
    }
    for prefix in [
        ObjectPrefix::all(store),
        ObjectPrefix::device_logs(store),
        ObjectPrefix::store_logs(store),
        ObjectPrefix::device_log(store, DeviceId(7)),
        ObjectPrefix::store_log(store, DeviceId(7)),
        ObjectPrefix::snapshots(store),
        ObjectPrefix::audience_snapshots(store, &Audience::Store),
        ObjectPrefix::audience_snapshots(store, &circle),
        ObjectPrefix::files(store),
        ObjectPrefix::join_requests(store),
        ObjectPrefix::keys(store),
        ObjectPrefix::positions(store),
    ] {
        let selection = Selection::Objects(prefix.clone());
        assert_eq!(selection.keys(), ["path", "size"]);
        let predicate = selection.predicate();
        let format = predicate.predicateFormat().to_string();
        assert!(!format.contains("BEGINSWITH"), "{format}");
        assert!(format.contains(" == "), "{format}");
        for path in &paths {
            assert_eq!(
                unsafe { predicate.evaluateWithObject(Some(&fields(path))) },
                prefix.contains(path),
                "{}: {}",
                prefix.as_str(),
                path.as_str(),
            );
        }
    }
}

#[test]
fn directory_fields_preserve_full_ancestors_and_omit_nonexistent_levels() {
    let snapshot = ObjectPath::snapshot(store(), Audience::Store, DeviceId(7), NonZeroU64::MIN);
    let root = store().to_string();
    assert_eq!(
        directories(&snapshot).collect::<Vec<_>>(),
        [
            ("directory1", root.as_str()),
            ("directory2", &format!("{root}/snapshots")),
            ("directory3", &format!("{root}/snapshots/store")),
            ("directory4", &format!("{root}/snapshots/store/7")),
        ]
    );
    let position = ObjectPath::positions(store(), DeviceId(7));
    assert_eq!(
        directories(&position).collect::<Vec<_>>(),
        [
            ("directory1", root.as_str()),
            ("directory2", &format!("{root}/positions")),
        ]
    );
    for (_, directory) in directories(&snapshot).chain(directories(&position)) {
        coven_format::path::validate_directory(directory).unwrap();
    }
}

#[test]
fn writer_query_selects_only_permanent_entry_one_in_its_store() {
    let selection = Selection::Writers(store());
    assert_eq!(selection.keys(), ["path"]);
    let predicate = selection.predicate();
    for (path, matches) in [
        (
            ObjectPath::store_log(store(), DeviceId(0), NonZeroU64::MIN),
            true,
        ),
        (
            ObjectPath::store_log(store(), DeviceId(u64::MAX), NonZeroU64::MIN),
            true,
        ),
        (
            ObjectPath::store_log(store(), DeviceId(7), NonZeroU64::new(2).unwrap()),
            false,
        ),
        (
            ObjectPath::store_log(
                StoreId(uuid::Uuid::from_u128(2)),
                DeviceId(7),
                NonZeroU64::MIN,
            ),
            false,
        ),
        (
            ObjectPath::device_log(store(), DeviceId(7), NonZeroU64::MIN),
            false,
        ),
        (
            ObjectPath::key_copy(store(), DeviceId(7), NonZeroU64::MIN),
            false,
        ),
        (ObjectPath::positions(store(), DeviceId(7)), false),
    ] {
        assert_eq!(
            unsafe { predicate.evaluateWithObject(Some(&fields(&path))) },
            matches
        );
        let converted = writer_id(path.as_str(), path.as_str(), store());
        if matches {
            assert_eq!(converted.unwrap(), path.device().unwrap());
        } else {
            assert_eq!(converted.unwrap_err().failure(), StorageFailure::Protocol);
        }
    }
    let first = ObjectPath::store_log(store(), DeviceId(7), NonZeroU64::MIN);
    for (name, path) in [("different", first.as_str()), ("invalid", "invalid")] {
        assert_eq!(
            writer_id(name, path, store()).unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}

#[test]
fn publication_fields_and_writer_conversion_use_only_published_records() {
    use crate::providers::cloudkit::record::Record;
    use objc2::AnyThread;
    use objc2_cloud_kit::CKRecordID;
    let first = ObjectPath::store_log(store(), DeviceId(7), NonZeroU64::MIN);
    for path in [
        first.clone(),
        ObjectPath::snapshot(store(), Audience::Store, DeviceId(7), NonZeroU64::MIN),
        ObjectPath::positions(store(), DeviceId(7)),
    ] {
        let id = unsafe {
            CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(path.as_str()))
        };
        let mut record = Record::new("CovenObject", &id);
        record.set_object_fields(&path, 42);
        assert_eq!(record.text("path").unwrap(), path.as_str());
        assert_eq!(record.number("size").unwrap(), 42);
        for (field, directory) in directories(&path) {
            assert_eq!(record.text(field).unwrap(), directory);
        }
        if path == first {
            assert_eq!(record.text("number").unwrap(), "1");
            assert_eq!(record.writer(store()).unwrap(), DeviceId(7));
        } else {
            assert_eq!(record.optional_text("number").unwrap(), None);
            assert_eq!(
                record.writer(store()).unwrap_err().failure(),
                StorageFailure::Protocol
            );
        }
    }
    for kind in ["CovenUpload", "CovenAsset"] {
        let id = unsafe {
            CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(first.as_str()))
        };
        let mut record = Record::new(kind, &id);
        record.set_text("path", first.as_str());
        assert_eq!(
            record.writer(store()).unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}
