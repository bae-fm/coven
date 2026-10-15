use super::*;
use crate::test_utils::STORE_ID;

fn listing(prefix: ObjectPrefix) -> ObjectListing {
    ObjectListing::new(prefix, CloudProvider::S3)
}

#[test]
fn names_outside_the_layout_are_skipped_and_counted() {
    let mut listing = listing(ObjectPrefix::all(STORE_ID));
    let object = format!("{STORE_ID}/devices/31/1");
    assert_eq!(
        listing.object_path(&object).unwrap(),
        Some(ObjectPath::parse(&object).unwrap())
    );
    // Stray files, temporaries, probes, and names close to a coven path:
    // D10 gives each object one spelling, so none of these is one.
    for stray in [
        "Notes.txt",
        "store-log/31/1/extra",
        "store-log/31/01",
        "store-log/031/1",
        "store-log/31/0",
        "devices/ana/1",
        "positions/31/1",
        "created/1",
        ".coven-uploads/5a7a4c66-6f1c-4d58-9f5d-3c2a9b5f2e10",
        ".coven-check/run/probe",
        "files/31/not-a-uuid",
    ] {
        let name = format!("{STORE_ID}/{stray}");
        assert_eq!(listing.object_path(&name).unwrap(), None, "{name}");
    }
    assert_eq!(listing.skipped, 11);
    assert!(listing.finish().is_empty());
}

#[test]
fn folders_outside_the_layout_are_not_descended() {
    let mut listing = listing(ObjectPrefix::all(STORE_ID));
    for folder in ["", "/devices", "/devices/31", "/snapshots/store/31"] {
        assert!(listing
            .object_folder(&format!("{STORE_ID}{folder}"))
            .unwrap());
    }
    for stray in ["Photos", "devices/31/1", "devices/x", ".coven-uploads"] {
        assert!(!listing
            .object_folder(&format!("{STORE_ID}/{stray}"))
            .unwrap());
    }
    assert_eq!(listing.skipped, 4);
}

#[test]
fn an_entry_outside_the_requested_prefix_is_the_providers_error() {
    let mut listing = listing(ObjectPrefix::device_logs(STORE_ID));
    for name in [
        format!("{STORE_ID}/store-log/31/1"),
        format!("{STORE_ID}/Notes.txt"),
        "other/devices/31/1".to_owned(),
    ] {
        assert_eq!(
            listing.object_path(&name).unwrap_err().failure(),
            StorageFailure::Protocol
        );
        assert_eq!(
            listing.object_folder(&name).unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}

#[test]
fn writer_folders_name_canonical_devices_only() {
    assert_eq!(writer_folder(STORE_ID, "31"), Some(DeviceId(31)));
    for stray in ["031", "ana", "31/1", "", "New folder"] {
        assert_eq!(writer_folder(STORE_ID, stray), None, "{stray}");
    }
}
