use super::*;
use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, StoreId};
use coven_merge::Audience;
use std::num::NonZeroU64;

fn store() -> StoreId {
    StoreId(uuid::Uuid::from_u128(1))
}

fn fields(path: &ObjectPath) -> Retained<objc2_foundation::NSDictionary<NSString, NSString>> {
    let fields: Vec<_> = directories(path)
        .map(|(key, value)| (NSString::from_str(key), NSString::from_str(value)))
        .collect();
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
                ObjectPath::slot(store, NonZeroU64::MIN),
                ObjectPath::snapshot(store, Audience::Store, device, NonZeroU64::MIN),
                ObjectPath::snapshot(store, circle.clone(), device, NonZeroU64::MIN),
                ObjectPath::file(store, device, FileId(uuid)),
                ObjectPath::floor(store),
            ]);
        }
        paths.push(ObjectPath::join_request(store, InviteId(uuid)));
    }
    for prefix in [
        ObjectPrefix::all(store),
        ObjectPrefix::files(store),
        ObjectPrefix::device_files(store, DeviceId(7)),
        ObjectPrefix::join_requests(store),
    ] {
        let selection = Selection(prefix.clone());
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
    let floor = ObjectPath::floor(store());
    assert_eq!(directories(&floor).collect::<Vec<_>>(), [("directory1", root.as_str()),]);
    for (_, directory) in directories(&snapshot).chain(directories(&floor)) {
        coven_format::path::validate_directory(directory).unwrap();
    }
}

#[test]
fn publication_fields_name_the_object_and_its_directories() {
    use crate::providers::cloudkit::record::Record;
    use objc2::AnyThread;
    use objc2_cloud_kit::CKRecordID;
    let first = ObjectPath::slot(store(), NonZeroU64::MIN);
    for path in [
        first.clone(),
        ObjectPath::snapshot(store(), Audience::Store, DeviceId(7), NonZeroU64::MIN),
        ObjectPath::floor(store()),
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
    }
}
