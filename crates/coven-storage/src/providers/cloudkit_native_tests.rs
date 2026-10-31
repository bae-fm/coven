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
                    None,
                );
                let result = request.wait().await;
                let result =
                    if floor { result.map_err(crate::StorageError::replacement) } else { result };
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

#[tokio::test]
async fn multipart_floor_publication_replaces_its_asset_chain_in_one_conditional_transaction() {
    let path = crate::ObjectPath::floor(crate::test_utils::STORE_ID);
    let id = |name: &str| unsafe {
        CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(name))
    };
    let mut floor = Record::new("CovenObject", &id(path.as_str()));
    floor.set_object_fields(&path, 12);
    let files = coven_foundation::files::AssetFiles::new().unwrap();
    let asset = floor.set_asset(&files, b"former asset").unwrap();
    floor.remove_field("data");
    floor.set_text("upload", "new-upload");
    let pointer = floor.raw() as *const CKRecord;
    let mut upload = Record::new("CovenUpload", &id("new-upload"));
    upload.set_text("state", "published");
    upload.set_reference("owner", &id(path.as_str()));
    let obsolete = id("old-upload");
    let result = modify_with(
        |operation| unsafe {
            assert!(operation.atomic());
            assert_eq!(operation.savePolicy(), CKRecordSavePolicy::IfServerRecordUnchanged);
            let records = operation.recordsToSave().unwrap();
            let deleted = operation.recordIDsToDelete().unwrap();
            assert_eq!(records.count(), 2);
            assert_eq!(deleted.count(), 1);
            assert_eq!(deleted.objectAtIndex(0).recordName().to_string(), "old-upload");
            let saved = records.objectAtIndex(0);
            assert_eq!(&*saved as *const CKRecord, pointer);
            assert!(saved.objectForKey(&NSString::from_str("data")).is_none());
            assert_eq!(Record::from_callback(&saved).text("upload").unwrap(), "new-upload");
            operation.modifyRecordsCompletionBlock().as_ref().unwrap().call((
                &*records as *const _ as *mut _,
                &*deleted as *const _ as *mut _,
                std::ptr::null_mut(),
            ));
        },
        vec![floor, upload],
        &[obsolete],
        CKRecordSavePolicy::IfServerRecordUnchanged,
        Some(path.as_str()),
        vec![asset],
        None,
    )
    .wait()
    .await
    .unwrap();
    assert_eq!(result.len(), 2);
}

/// A save hears CloudKit's per-record progress for the record carrying the
/// asset, as bytes through the object; the other records of the save say
/// nothing.
#[tokio::test]
async fn a_save_hears_its_assets_progress_as_bytes() {
    let id = |name: &str| unsafe {
        CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(name))
    };
    let files = coven_foundation::files::AssetFiles::new().unwrap();
    let mut part = Record::new("CovenAsset", &id("upload/0"));
    let asset = part.set_asset(&files, &[1; 1000]).unwrap();
    let upload = Record::new("CovenUpload", &id("upload"));
    let heard = Arc::new(Mutex::new(Vec::new()));
    let sent = crate::ByteProgress::new({
        let heard = heard.clone();
        move |count| heard.lock().unwrap().push(count)
    })
    .starting_at(8000);
    let progress = AssetProgress::bytes("upload/0".into(), 0, 1000, 1000, &sent);
    let result = modify_with(
        |operation| unsafe {
            let records = operation.recordsToSave().unwrap();
            let block = operation.perRecordProgressBlock().as_ref().unwrap();
            for (record, fraction) in [(1, 0.5), (0, 0.25), (1, 1.0)] {
                let record = records.objectAtIndex(record);
                block.call((NonNull::from(&*record), fraction));
            }
            operation.modifyRecordsCompletionBlock().as_ref().unwrap().call((
                &*records as *const _ as *mut _,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ));
        },
        vec![upload, part],
        &[],
        CKRecordSavePolicy::IfServerRecordUnchanged,
        None,
        vec![asset],
        Some(progress),
    )
    .wait()
    .await
    .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(*heard.lock().unwrap(), [8500, 9000]);
}

/// A fetch hears CloudKit's per-record progress for its record as bytes
/// through the object, whatever its completion says.
#[tokio::test]
async fn a_fetch_hears_its_assets_progress_as_bytes() {
    let id = unsafe {
        CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str("upload/8"))
    };
    let heard = Arc::new(Mutex::new(Vec::new()));
    let received = crate::ByteProgress::new({
        let heard = heard.clone();
        move |count| heard.lock().unwrap().push(count)
    });
    let progress = AssetProgress::bytes("upload/8".into(), 8, 100, 50, &received);
    let result = fetch_with(
        |operation| unsafe {
            let block = operation.perRecordProgressBlock().as_ref().unwrap();
            block.call((NonNull::from(&*id), 0.1));
            block.call((NonNull::from(&*id), 2.0));
            let error = NSError::initWithDomain_code_userInfo(
                NSError::alloc(),
                CKErrorDomain,
                CKErrorCode::NetworkFailure.0,
                None,
            );
            operation
                .fetchRecordsCompletionBlock()
                .as_ref()
                .unwrap()
                .call((std::ptr::null_mut(), &*error as *const _ as *mut _));
        },
        &id,
        &["data"],
        Ok,
        Some(progress),
    )
    .wait()
    .await;
    assert_eq!(result.err().unwrap().failure(), StorageFailure::Network);
    // 8 + 10%, then clamped to the whole asset and to the range's end.
    assert_eq!(*heard.lock().unwrap(), [18, 50]);
}
