//! Provisioning the owner's custom zone; reconnecting never recreates a zone.

use super::native::{self as native, completion, error, CloudKitRequest};
use crate::{StorageConfig, StorageError, StorageFailure};
use block2::RcBlock;
use coven_foundation::id_source::StoreId;
use objc2::AnyThread;
use objc2_cloud_kit::{
    CKContainer, CKModifyRecordZonesOperation, CKRecordZone, CKRecordZoneCapabilities,
    CKRecordZoneID,
};
use objc2_foundation::{NSArray, NSError, NSString};

pub(crate) async fn prepare(
    container_id: &str,
    store: StoreId,
) -> Result<StorageConfig, StorageError> {
    native::check_available()?;
    if container_id.trim().is_empty() || container_id.chars().any(char::is_control) {
        return Err(StorageFailure::InvalidConfiguration.into());
    }
    let container = objc2::rc::autoreleasepool(|_| unsafe {
        CKContainer::containerWithIdentifier(&NSString::from_str(container_id))
    });
    let owner = native::current_account(&container).wait().await?;
    let zone = format!("coven-{store}");
    save(&container, &zone).wait().await?;
    Ok(StorageConfig::CloudKit {
        container: container_id.to_owned(),
        owner,
        zone,
    })
}

fn save(container: &CKContainer, name: &str) -> CloudKitRequest<()> {
    let (complete, result) = completion();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let zone = CKRecordZone::initWithZoneName(CKRecordZone::alloc(), &NSString::from_str(name));
        let operation =
            CKModifyRecordZonesOperation::initWithRecordZonesToSave_recordZoneIDsToDelete(
                CKModifyRecordZonesOperation::alloc(),
                Some(&NSArray::from_retained_slice(&[zone])),
                None,
            );
        let callback = RcBlock::new(
            move |zones: *mut NSArray<CKRecordZone>,
                  _deleted: *mut NSArray<CKRecordZoneID>,
                  failure: *mut NSError| {
                complete(match error(failure) {
                    Some(error) => Err(error),
                    None => match zones.as_ref() {
                        Some(zones)
                            if zones.count() == 1
                                && zones.objectAtIndex(0).capabilities().contains(
                                    CKRecordZoneCapabilities::Atomic
                                        | CKRecordZoneCapabilities::ZoneWideSharing,
                                ) =>
                        {
                            Ok(())
                        }
                        _ => Err(StorageFailure::Protocol
                            .with_source("CloudKit zone lacks atomic saves or zone sharing")),
                    },
                });
            },
        );
        operation.setModifyRecordZonesCompletionBlock(Some(&callback));
        container.privateCloudDatabase().addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

#[cfg(test)]
pub(super) fn delete(storage: &super::CloudKitStorage) -> CloudKitRequest<()> {
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
