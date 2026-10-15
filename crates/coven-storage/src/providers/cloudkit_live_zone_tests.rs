use super::*;
use crate::providers::cloudkit::record::{part_name, upload_name};
use crate::ObjectPath;
use coven_foundation::id_source::{DeviceId, FileId, UuidIds};
use objc2_cloud_kit::CKRecordID;
use objc2_foundation::NSString;
use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc};

fn record(kind: &str, name: &str) -> Record {
    let id =
        unsafe { CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(name)) };
    Record::new(kind, &id)
}

fn marked(test: &str) -> Record {
    let mut marker = record(MARKER_TYPE, MARKER_NAME);
    marker.set_text(TEST_FIELD, test);
    marker
}

/// A zone's records by name: the marker lookup is CloudKit's fetch by name.
fn verdict_of(zone: &BTreeMap<String, Record>) -> Verdict {
    verdict(zone.get(MARKER_NAME))
}

/// Every record a live fixture writes also appears in a real store, including
/// device 7's positions, which the earlier cleanup selected zones by.
#[test]
fn a_zone_holding_only_store_records_is_kept() {
    let store = StoreId(uuid::Uuid::from_u128(1));
    let file = ObjectPath::file(store, DeviceId(7), FileId(uuid::Uuid::from_u128(2)));
    let upload = upload_name(&file, &uuid::Uuid::from_u128(3).to_string()).unwrap();
    let mut zone: BTreeMap<String, Record> = [
        (
            "CovenObject",
            ObjectPath::positions(store, DeviceId(7))
                .as_str()
                .to_owned(),
        ),
        (
            "CovenObject",
            ObjectPath::clock(store, DeviceId(7)).as_str().to_owned(),
        ),
        (
            "CovenObject",
            ObjectPath::store_log(store, DeviceId(7), NonZeroU64::MIN)
                .as_str()
                .to_owned(),
        ),
        ("CovenObject", file.as_str().to_owned()),
        ("CovenUpload", upload.clone()),
        ("CovenAsset", part_name(&upload, 0)),
    ]
    .into_iter()
    .map(|(kind, name)| (name.clone(), record(kind, &name)))
    .collect();
    assert_eq!(
        verdict_of(&zone),
        Verdict::Keep("no live-test marker".to_owned())
    );
    zone.insert(MARKER_NAME.to_owned(), marked("cloudkit_live"));
    assert_eq!(
        verdict_of(&zone),
        Verdict::Delete {
            test: "cloudkit_live".to_owned()
        }
    );
}

#[test]
fn only_a_marker_typed_record_naming_its_test_selects_a_zone() {
    assert!(ObjectPath::parse(MARKER_NAME).is_err());
    assert!(uuid::Uuid::parse_str(&MARKER_NAME[..MARKER_NAME.len().min(36)]).is_err());
    assert!(matches!(
        verdict(Some(&record("CovenObject", MARKER_NAME))),
        Verdict::Keep(_)
    ));
    assert!(matches!(
        verdict(Some(&record(MARKER_TYPE, MARKER_NAME))),
        Verdict::Keep(_)
    ));
    assert!(matches!(verdict(Some(&marked(""))), Verdict::Keep(_)));
}

#[test]
fn only_canonical_coven_zone_names_belong_to_a_store() {
    let store = StoreId(uuid::Uuid::from_u128(
        0xfeed_0000_0000_0000_0000_0000_0000_beef,
    ));
    assert_eq!(store_zone(&format!("coven-{store}")), Some(store));
    for name in [
        "_defaultZone".to_owned(),
        format!("{store}"),
        format!("coven-{}", store.0.simple()),
        format!("coven-{}", store.0.to_string().to_uppercase()),
        format!("coven-{store}-other"),
    ] {
        assert_eq!(store_zone(&name), None, "{name}");
    }
}

/// Removes the zones failed live runs left in the container: only marked
/// `coven-<store>` zones. Prints each zone it found, deleted and kept.
#[tokio::test]
#[ignore = "requires a signed CloudKit host and an iCloud account; deletes marked live-test zones"]
async fn cloudkit_live_remove_leftover_test_zones() {
    let ids: IdSourceRef = Arc::new(UuidIds);
    let container = objc2::rc::autoreleasepool(|_| unsafe {
        CKContainer::containerWithIdentifier(&NSString::from_str(CONTAINER))
    });
    let owner = native::current_account(&container).wait().await.unwrap();
    let mut names = list(&container).wait().await.unwrap();
    names.sort();
    let mut deleted = Vec::new();
    let mut kept = Vec::new();
    for name in &names {
        println!("cloudkit_cleanup found zone={name}");
        let verdict = match store_zone(name) {
            None => Verdict::Keep("not a coven-<store> zone".to_owned()),
            Some(_) => {
                let config = crate::StorageConfig::CloudKit {
                    container: CONTAINER.to_owned(),
                    owner: owner.clone(),
                    zone: name.clone(),
                };
                let storage = CloudKitStorage::new(config, ids.clone()).await.unwrap();
                match verdict(marker(&storage).await.unwrap().as_ref()) {
                    Verdict::Delete { .. } => Verdict::Delete {
                        test: delete(&storage).await.unwrap(),
                    },
                    keep => keep,
                }
            }
        };
        match verdict {
            Verdict::Delete { test } => {
                println!("cloudkit_cleanup deleted zone={name} (marked by {test})");
                deleted.push(name.clone());
            }
            Verdict::Keep(reason) => {
                println!("cloudkit_cleanup kept zone={name} ({reason})");
                kept.push(name.clone());
            }
        }
    }
    let mut remaining = list(&container).wait().await.unwrap();
    remaining.sort();
    for name in &remaining {
        println!("cloudkit_cleanup remaining zone={name}");
    }
    assert!(deleted.iter().all(|name| !remaining.contains(name)));
    assert!(kept.iter().all(|name| remaining.contains(name)));
}
