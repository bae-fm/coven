//! CloudKit operation ownership and completion callbacks.

use super::query::Selection;
use super::record::{native_error, Record};
use crate::{StorageError, StorageFailure};
use block2::RcBlock;
use objc2::{rc::Retained, AnyThread};
use objc2_cloud_kit::{
    CKContainer, CKDatabase, CKFetchRecordsOperation, CKModifyRecordsOperation, CKQuery,
    CKQueryCursor, CKQueryOperation, CKRecord, CKRecordID, CKRecordSavePolicy, CKRecordZoneID,
};
use objc2_foundation::{NSArray, NSDictionary, NSError, NSOperation, NSString};
use std::{
    ptr::NonNull,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

pub(super) const QUERY_PAGE_SIZE: usize = 100;
pub(super) type CloudKitReply<T> = oneshot::Receiver<Result<T, StorageError>>;

/// An enqueued operation is only cancelled through NSOperation's thread-safe API.
/// Its CloudKit configuration and callbacks are never changed after enqueueing.
pub(super) struct CloudKitRequest<T> {
    pub(super) operation: Retained<NSOperation>,
    pub(super) result: CloudKitReply<T>,
}
impl<T> CloudKitRequest<T> {
    pub(super) async fn wait(mut self) -> Result<T, StorageError> {
        (&mut self.result)
            .await
            .map_err(|error| StorageFailure::Protocol.with_source(error))?
    }
}
impl<T> Drop for CloudKitRequest<T> {
    fn drop(&mut self) {
        self.operation.cancel();
    }
}

pub(super) fn completion<T: Send + 'static>() -> (
    impl Fn(Result<T, StorageError>) + Send + Sync,
    CloudKitReply<T>,
) {
    let (sender, receiver) = oneshot::channel();
    let sender = Mutex::new(Some(sender));
    (
        move |result| match sender
            .lock()
            .expect("CloudKit completion lock poisoned")
            .take()
        {
            Some(sender) => {
                if sender.send(result).is_err() {
                    tracing::debug!("CloudKit caller dropped its pending operation");
                }
            }
            None => tracing::error!("CloudKit invoked a completion more than once"),
        },
        receiver,
    )
}

pub(super) fn strings(values: &[&str]) -> Retained<NSArray<NSString>> {
    NSArray::from_retained_slice(
        &values
            .iter()
            .map(|s| NSString::from_str(s))
            .collect::<Vec<_>>(),
    )
}

/// SAFETY: Called only from a framework completion; nonnull error pointers are
/// borrowed for the callback's duration and retained before that duration ends.
pub(super) unsafe fn error(error: *mut NSError) -> Option<StorageError> {
    unsafe { error.as_ref() }.map(native_error)
}

pub(super) fn fetch<T: Send + 'static>(
    database: &CKDatabase,
    id: &CKRecordID,
    keys: &[&str],
    convert: impl Fn(Record) -> Result<T, StorageError> + Send + Sync + 'static,
) -> CloudKitRequest<T> {
    let (complete, result) = completion();
    // SAFETY: The operation is exclusively configured here. CloudKit copies its
    // blocks and input arrays. Callback pointers are retained before returning.
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKFetchRecordsOperation::initWithRecordIDs(
            CKFetchRecordsOperation::alloc(),
            &NSArray::from_slice(&[id]),
        );
        operation.setDesiredKeys(Some(&strings(keys)));
        let block = RcBlock::new(
            move |records: *mut NSDictionary<CKRecordID, CKRecord>, failure: *mut NSError| {
                let response = if let Some(error) = error(failure) {
                    Err(error)
                } else {
                    match records.as_ref() {
                        Some(records) if records.count() == 1 => {
                            let values = records.allValues();
                            convert(Record::from_callback(&values.objectAtIndex(0)))
                        }
                        _ => Err(StorageFailure::Protocol
                            .with_source("CloudKit fetch omitted its record")),
                    }
                };
                complete(response);
            },
        );
        operation.setFetchRecordsCompletionBlock(Some(&block));
        database.addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

pub(super) fn current_account(container: &CKContainer) -> CloudKitRequest<String> {
    let (complete, result) = completion();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKFetchRecordsOperation::fetchCurrentUserRecordOperation();
        operation.setDesiredKeys(Some(&strings(&[])));
        let block = RcBlock::new(
            move |records: *mut NSDictionary<CKRecordID, CKRecord>, failure: *mut NSError| {
                let response = if let Some(error) = error(failure) {
                    Err(error)
                } else {
                    match records.as_ref() {
                        Some(records) if records.count() == 1 => {
                            let account = records
                                .allValues()
                                .objectAtIndex(0)
                                .recordID()
                                .recordName()
                                .to_string();
                            if account.is_empty() {
                                Err(StorageFailure::Authentication.into())
                            } else {
                                Ok(account)
                            }
                        }
                        _ => Err(StorageFailure::Authentication
                            .with_source("CloudKit returned no account")),
                    }
                };
                complete(response);
            },
        );
        operation.setFetchRecordsCompletionBlock(Some(&block));
        container.privateCloudDatabase().addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

/// Keep asset files alive until CloudKit finishes, even when the awaiting future
/// is cancelled. Explicit close failures reach a live caller with the save error.
pub(super) fn modify(
    database: &CKDatabase,
    records: Vec<Record>,
    deletes: &[Retained<CKRecordID>],
    policy: CKRecordSavePolicy,
    create_name: Option<&str>,
    assets: Vec<coven_foundation::files::AssetFile>,
) -> CloudKitRequest<Vec<Record>> {
    let (complete, result) = completion();
    let expected_saves = records.len();
    let expected_deletes = deletes.len();
    let assets = Mutex::new(Some(assets));
    let create_name = create_name.map(str::to_owned);
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let saves = NSArray::from_slice(&records.iter().map(Record::raw).collect::<Vec<_>>());
        let operation = CKModifyRecordsOperation::initWithRecordsToSave_recordIDsToDelete(
            CKModifyRecordsOperation::alloc(),
            Some(&saves),
            Some(&NSArray::from_retained_slice(deletes)),
        );
        // UserInitiated fails on network errors instead of retrying internally,
        // and does not defer publication until discretionary background time.
        let configuration = operation.configuration();
        configuration.setQualityOfService(objc2_foundation::NSQualityOfService::UserInitiated);
        configuration.setTimeoutIntervalForResource(120.0);
        operation.setConfiguration(Some(&configuration));
        operation.setSavePolicy(policy);
        operation.setAtomic(true);
        let block = RcBlock::new(
            move |saved: *mut NSArray<CKRecord>,
                  deleted: *mut NSArray<CKRecordID>,
                  failure: *mut NSError| {
                let mut response = if let Some(error) = failure.as_ref() {
                    Err(super::record::save_error(error, create_name.as_deref()))
                } else if saved.as_ref().map_or(0, |records| records.count()) != expected_saves
                    || deleted.as_ref().map_or(0, |ids| ids.count()) != expected_deletes
                {
                    Err(StorageFailure::Protocol
                        .with_source("CloudKit modification omitted its saved or deleted records"))
                } else {
                    Ok(match saved.as_ref() {
                        Some(saved) => saved
                            .iter()
                            .map(|record| Record::from_callback(&record))
                            .collect(),
                        None => Vec::new(),
                    })
                };
                if let Some(files) = assets.lock().expect("CloudKit assets lock poisoned").take() {
                    for file in files {
                        if let Err(error) = file.close() {
                            let cleanup = StorageFailure::File.with_source(error);
                            response = Err(match response {
                                Ok(_) => cleanup,
                                Err(operation) => StorageError::Cleanup {
                                    operation: Box::new(operation),
                                    cleanup: Box::new(cleanup),
                                },
                            });
                        }
                    }
                }
                complete(response);
            },
        );
        operation.setModifyRecordsCompletionBlock(Some(&block));
        database.addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

pub(super) struct Cursor(Retained<CKQueryCursor>);
// SAFETY: Query cursors are immutable continuations. Each value is transferred
// from its completion to one subsequent query, with no concurrent mutation.
unsafe impl Send for Cursor {}

pub(super) fn query<T: Send + 'static>(
    database: &CKDatabase,
    zone: &CKRecordZoneID,
    selection: Selection,
    cursor: Option<Cursor>,
    convert: impl Fn(Record) -> Result<T, StorageError> + Send + Sync + 'static,
    #[cfg(test)] pages: super::tests::Pages,
) -> CloudKitRequest<(Vec<T>, Option<Cursor>)> {
    let (complete, result) = completion();
    let records = Arc::new(Mutex::new(Vec::new()));
    let matched = records.clone();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = match cursor {
            Some(cursor) => CKQueryOperation::initWithCursor(CKQueryOperation::alloc(), &cursor.0),
            None => {
                let predicate = selection.predicate();
                let query = CKQuery::initWithRecordType_predicate(
                    CKQuery::alloc(),
                    &NSString::from_str("CovenObject"),
                    &predicate,
                );
                CKQueryOperation::initWithQuery(CKQueryOperation::alloc(), &query)
            }
        };
        operation.setZoneID(Some(zone));
        operation.setResultsLimit(QUERY_PAGE_SIZE);
        operation.setDesiredKeys(Some(&strings(selection.keys())));
        #[cfg(test)]
        let page_index = super::tests::query_enqueued(&pages, &operation);
        let block = RcBlock::new(
            move |_id: NonNull<CKRecordID>, record: *mut CKRecord, failure: *mut NSError| {
                #[cfg(test)]
                super::tests::query_matched(&pages, page_index, record.as_ref());
                let response = if let Some(error) = error(failure) {
                    Err(error)
                } else {
                    match record.as_ref() {
                        Some(record) => convert(Record::from_callback(record)),
                        None => Err(StorageFailure::Protocol
                            .with_source("CloudKit query omitted matched record")),
                    }
                };
                matched
                    .lock()
                    .expect("CloudKit results lock poisoned")
                    .push(response);
            },
        );
        operation.setRecordMatchedBlock(Some(&block));
        let block = RcBlock::new(move |cursor: *mut CKQueryCursor, failure: *mut NSError| {
            let response = if let Some(error) = error(failure) {
                Err(error)
            } else {
                let entries =
                    std::mem::take(&mut *records.lock().expect("CloudKit results lock poisoned"));
                entries
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map(|entries| (entries, Retained::retain(cursor).map(Cursor)))
            };
            complete(response);
        });
        operation.setQueryCompletionBlock(Some(&block));
        database.addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

pub(super) fn check_available() -> Result<(), StorageError> {
    if objc2::available!(
        macos = 12.0,
        ios = 15.0,
        tvos = 15.0,
        watchos = 8.0,
        visionos = 1.0
    ) {
        Ok(())
    } else {
        Err(StorageFailure::ProviderUnavailable
            .with_source("CloudKit zone sharing is unavailable on this OS version"))
    }
}
