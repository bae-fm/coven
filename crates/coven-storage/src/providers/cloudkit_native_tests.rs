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
                let asset = files.stage(bytes).unwrap();
                record.set_asset(asset.path()).unwrap();
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
                            crate::providers::cloudkit::asset_bytes(
                                &files,
                                &Record::from_callback(&saved)
                            )
                            .unwrap(),
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
    let asset = files.stage(b"former asset").unwrap();
    floor.set_asset(asset.path()).unwrap();
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
    let asset = files.stage(&[1; 1000]).unwrap();
    part.set_asset(asset.path()).unwrap();
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

fn record_id(name: &str) -> Retained<CKRecordID> {
    unsafe { CKRecordID::initWithRecordName(CKRecordID::alloc(), &NSString::from_str(name)) }
}

/// A panic in an operation's last callback, which CloudKit calls on its
/// own queue, answers the request with the bug rather than unwinding into
/// CloudKit (api.md E15).
#[tokio::test]
async fn a_panic_in_a_completion_answers_the_request_with_its_bug() {
    let id = record_id("upload/8");
    let result = fetch_with(
        |operation| unsafe {
            let record = Record::new("CovenObject", &id);
            let records = NSDictionary::from_slices(&[&*id], &[record.raw()]);
            operation
                .fetchRecordsCompletionBlock()
                .as_ref()
                .unwrap()
                .call((&*records as *const _ as *mut _, std::ptr::null_mut()));
        },
        &id,
        &["data"],
        |_record| -> Result<(), StorageError> { panic!("the record has no data field") },
        None,
    )
    .wait()
    .await;
    let Err(StorageError::Bug(bug)) = result else { panic!("expected a bug, got {result:?}") };
    assert_eq!(bug.message, "the record has no data field");
}

/// A panic in a callback before the last, such as per-record progress,
/// answers the request with that bug once the operation completes, whatever
/// the completion says.
#[tokio::test]
async fn a_panic_in_progress_answers_the_request_with_its_bug() {
    let files = coven_foundation::files::AssetFiles::new().unwrap();
    let mut part = Record::new("CovenAsset", &record_id("upload/0"));
    let asset = files.stage(&[1; 10]).unwrap();
    let staged = asset.path().to_owned();
    part.set_asset(asset.path()).unwrap();
    let sent = crate::ByteProgress::new(|_count| panic!("the progress reader is gone"));
    let progress = AssetProgress::bytes("upload/0".into(), 0, 10, 10, &sent);
    let result = modify_with(
        |operation| unsafe {
            let records = operation.recordsToSave().unwrap();
            let block = operation.perRecordProgressBlock().as_ref().unwrap();
            block.call((NonNull::from(&*records.objectAtIndex(0)), 0.5));
            operation.modifyRecordsCompletionBlock().as_ref().unwrap().call((
                &*records as *const _ as *mut _,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ));
        },
        vec![part],
        &[],
        CKRecordSavePolicy::IfServerRecordUnchanged,
        None,
        vec![asset],
        Some(progress),
    )
    .wait()
    .await;
    let Err(StorageError::Bug(bug)) = result.map(|saved| saved.len()) else {
        panic!("expected a bug")
    };
    assert_eq!(bug.message, "the progress reader is gone");
    // The completion still closed the save's asset file.
    assert!(!staged.exists());
}
