#![cfg(test)]

//! Zones of the live CloudKit tests. They share `iCloud.fm.bae.desktop` and the
//! `coven-<store>` zone name with real bae stores, so each test zone carries a
//! marker record no store writes, saved before anything else in the zone. Only
//! a zone carrying it can be deleted here; a zone without it is never touched.

use crate::providers::cloudkit::native::{self, completion, error, CloudKitRequest};
use crate::providers::cloudkit::record::Record;
use crate::providers::cloudkit::{zone, CloudKitStorage};
use crate::{StorageError, StorageFailure};
use block2::RcBlock;
use coven_foundation::id_source::{IdSourceRef, StoreId};
use objc2::AnyThread;
use objc2_cloud_kit::{
    CKContainer, CKFetchRecordZonesOperation, CKModifyRecordZonesOperation, CKRecordSavePolicy,
    CKRecordZone, CKRecordZoneID,
};
use objc2_foundation::{NSArray, NSDictionary, NSError};

const CONTAINER: &str = "iCloud.fm.bae.desktop";
/// Stores write only `CovenObject`, `CovenUpload`, `CovenAsset` and the zone's
/// share, so a store zone never holds a record of this type.
const MARKER_TYPE: &str = "CovenLiveTestZone";
/// Store record names begin with the store id (object paths, upload sessions
/// and their parts), so no store record has this name.
const MARKER_NAME: &str = "coven-live-test-zone";
/// The marker's one field: the test that created the zone.
const TEST_FIELD: &str = "test";

/// Creates `store`'s zone for `test` and marks it before any other write. The
/// zone name is printed first, so a zone whose marking failed can be found
/// and removed through CloudKit Console.
pub(crate) async fn prepare(
    store: StoreId,
    test: &str,
    ids: IdSourceRef,
) -> Result<CloudKitStorage, StorageError> {
    println!("{test} zone=coven-{store}");
    let config = zone::prepare(CONTAINER, store).await?;
    let storage = CloudKitStorage::new(config, ids).await?;
    let mut marker = Record::new(MARKER_TYPE, &storage.record_id(MARKER_NAME));
    marker.set_text(TEST_FIELD, test);
    native::modify(
        &storage.database,
        vec![marker],
        &[],
        CKRecordSavePolicy::IfServerRecordUnchanged,
        None,
        vec![],
    )
    .wait()
    .await?;
    Ok(storage)
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// A live test marked the zone; `test` is the test that created it.
    Delete {
        test: String,
    },
    Keep(String),
}

/// The store a zone name belongs to, when it is the canonical `coven-<store>`.
fn store_zone(name: &str) -> Option<StoreId> {
    let store = StoreId(uuid::Uuid::parse_str(name.strip_prefix("coven-")?).ok()?);
    (format!("coven-{store}") == name).then_some(store)
}

/// Decides a `coven-<store>` zone from its record named [`MARKER_NAME`], or
/// None when it has no such record.
fn verdict(marker: Option<&Record>) -> Verdict {
    let Some(marker) = marker else {
        return Verdict::Keep("no live-test marker".to_owned());
    };
    let kind = unsafe { marker.raw().recordType() }.to_string();
    if kind != MARKER_TYPE {
        return Verdict::Keep(format!("the marker's name holds a {kind} record"));
    }
    match marker.optional_text(TEST_FIELD) {
        Ok(Some(test)) if !test.is_empty() => Verdict::Delete { test },
        _ => Verdict::Keep("the marker names no test".to_owned()),
    }
}

/// The zone's marker record, or None when the zone has none.
async fn marker(storage: &CloudKitStorage) -> Result<Option<Record>, StorageError> {
    let id = storage.record_id(MARKER_NAME);
    match native::fetch(&storage.database, &id, &[TEST_FIELD], Ok)
        .wait()
        .await
    {
        Ok(record) => Ok(Some(record)),
        Err(error) if error.failure() == StorageFailure::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Deletes a marked test zone and returns the test that marked it. An unmarked
/// zone is refused before any deletion is sent.
pub(crate) async fn delete(storage: &CloudKitStorage) -> Result<String, StorageError> {
    if store_zone(&storage.zone).is_none() {
        return Err(StorageFailure::Refused.with_source("not a coven-<store> zone"));
    }
    match verdict(marker(storage).await?.as_ref()) {
        Verdict::Delete { test } => {
            remove(storage).wait().await?;
            Ok(test)
        }
        Verdict::Keep(reason) => Err(StorageFailure::Refused.with_source(reason)),
    }
}

fn remove(storage: &CloudKitStorage) -> CloudKitRequest<()> {
    let (complete, result) = completion();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation =
            CKModifyRecordZonesOperation::initWithRecordZonesToSave_recordZoneIDsToDelete(
                CKModifyRecordZonesOperation::alloc(),
                None,
                Some(&NSArray::from_retained_slice(&[storage.zone_id()])),
            );
        let callback = RcBlock::new(
            move |_saved: *mut NSArray<CKRecordZone>,
                  _deleted: *mut NSArray<CKRecordZoneID>,
                  failure: *mut NSError| {
                complete(match error(failure) {
                    Some(error) => Err(error),
                    None => Ok(()),
                });
            },
        );
        operation.setModifyRecordZonesCompletionBlock(Some(&callback));
        storage.database.addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

/// Every zone name in the signed-in account's private database.
fn list(container: &CKContainer) -> CloudKitRequest<Vec<String>> {
    let (complete, result) = completion();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKFetchRecordZonesOperation::fetchAllRecordZonesOperation();
        let callback = RcBlock::new(
            move |zones: *mut NSDictionary<CKRecordZoneID, CKRecordZone>, failure: *mut NSError| {
                complete(match error(failure) {
                    Some(error) => Err(error),
                    None => match zones.as_ref() {
                        Some(zones) => Ok(zones
                            .allKeys()
                            .iter()
                            .map(|id| id.zoneName().to_string())
                            .collect()),
                        None => Err(StorageFailure::Protocol
                            .with_source("CloudKit zone listing omitted its zones")),
                    },
                });
            },
        );
        operation.setFetchRecordZonesCompletionBlock(Some(&callback));
        container.privateCloudDatabase().addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

#[cfg(test)]
#[path = "cloudkit_live_zone_tests.rs"]
mod tests;
