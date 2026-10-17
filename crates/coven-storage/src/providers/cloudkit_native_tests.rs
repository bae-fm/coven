use super::*;
use objc2::runtime::AnyObject;
use objc2_cloud_kit::{CKErrorCode, CKErrorDomain, CKRecordChangedErrorClientRecordKey};

#[tokio::test]
async fn object_publications_use_atomic_change_tag_conditions_and_retain_the_fetched_record() {
    for floor in [false, true] {
        for bytes in [b"same", b"else"] {
            for failure in [
                None,
                Some(CKErrorCode::ServerRecordChanged),
                Some(CKErrorCode::ServerResponseLost),
            ] {
                let path = if floor {
                    crate::ObjectPath::floor(crate::test_utils::STORE_ID)
                } else {
                    crate::ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN)
                };
                let id = unsafe {
                    CKRecordID::initWithRecordName(
                        CKRecordID::alloc(),
                        &NSString::from_str(path.as_str()),
                    )
                };
                let mut record = Record::new("CovenObject", &id);
                record.set_object_fields(&path, bytes.len() as u64);
                let files = coven_foundation::files::AssetFiles::new().unwrap();
                let asset = record.set_asset(&files, bytes).unwrap();
                // Retaining the same record is what preserves CloudKit's opaque system fields.
                let pointer = record.raw() as *const CKRecord;
                let request = conditional_object(
                    |operation| unsafe {
                        assert!(operation.atomic());
                        assert_eq!(
                            operation.savePolicy(),
                            CKRecordSavePolicy::IfServerRecordUnchanged
                        );
                        let records = operation.recordsToSave().unwrap();
                        assert_eq!(records.count(), 1);
                        let saved = records.objectAtIndex(0);
                        assert_eq!(&*saved as *const CKRecord, pointer);
                        assert_eq!(saved.recordID().recordName().to_string(), path.as_str());
                        assert_eq!(
                            Record::from_callback(&saved).asset_bytes(&files).unwrap(),
                            bytes
                        );
                        let completion = operation.modifyRecordsCompletionBlock().as_ref().unwrap();
                        match failure {
                            None => completion.call((
                                &*records as *const _ as *mut _,
                                std::ptr::null_mut(),
                                std::ptr::null_mut(),
                            )),
                            Some(code) => {
                                let info = NSDictionary::<NSString, AnyObject>::from_slices(
                                    &[CKRecordChangedErrorClientRecordKey],
                                    &[&*saved],
                                );
                                let error = NSError::initWithDomain_code_userInfo(
                                    NSError::alloc(),
                                    CKErrorDomain,
                                    code.0,
                                    Some(&info),
                                );
                                completion.call((
                                    std::ptr::null_mut(),
                                    std::ptr::null_mut(),
                                    &*error as *const _ as *mut _,
                                ));
                            }
                        }
                    },
                    record,
                    &path,
                    vec![asset],
                );
                let result = request.wait().await;
                let result = if floor {
                    result.map_err(crate::StorageError::replacement)
                } else {
                    result
                };
                match failure {
                    None => assert_eq!(result.unwrap().len(), 1),
                    Some(CKErrorCode::ServerRecordChanged) => assert_eq!(
                        result.err().unwrap().failure(),
                        if floor {
                            StorageFailure::Conflict
                        } else {
                            StorageFailure::AlreadyExists
                        }
                    ),
                    Some(_) => assert_eq!(result.err().unwrap().failure(), StorageFailure::Network),
                }
            }
        }
    }
}
