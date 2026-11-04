//! CloudKit operation ownership and completion callbacks.

use super::query::Selection;
use super::record::{native_error, Record};
use crate::{StorageError, StorageFailure};
use block2::RcBlock;
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::{caught_sync, Bug};
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
    operation: Retained<NSOperation>,
    result: CloudKitReply<T>,
}
impl<T> CloudKitRequest<T> {
    /// The enqueued `operation`, whose completion answers `result`.
    pub(super) fn new(operation: Retained<NSOperation>, result: CloudKitReply<T>) -> Self {
        Self { operation, result }
    }

    pub(super) async fn wait(mut self) -> Result<T, StorageError> {
        (&mut self.result).await.map_err(|error| StorageFailure::Protocol.with_source(error))?
    }
}
impl<T> Drop for CloudKitRequest<T> {
    fn drop(&mut self) {
        self.operation.cancel();
    }
}

/// Tells how far one record's asset has gone, as the fraction of it
/// CloudKit's per-record progress gives, from 0 to 1 (§6.7). Other records
/// of the operation carry no asset, so they say nothing.
pub(super) struct AssetProgress {
    /// The record whose asset this follows, by name.
    pub(super) record: String,
    pub(super) report: Box<dyn Fn(f64) + Send + Sync>,
}

impl AssetProgress {
    /// Tell `progress` how far through the object the asset of `record`,
    /// `length` bytes starting `start` bytes in, has gone, never past `end`.
    pub(super) fn bytes(
        record: String,
        start: u64,
        length: u64,
        end: u64,
        progress: &crate::ByteProgress,
    ) -> Self {
        let progress = progress.clone();
        Self {
            record,
            report: Box::new(move |fraction| {
                let moved = (fraction * length as f64) as u64;
                progress.moved((start + moved).min(end));
            }),
        }
    }

    /// CloudKit's per-record progress: `record` has gone `fraction` of the way.
    pub(super) fn heard(&self, record: &str, fraction: f64) {
        if record == self.record {
            (self.report)(fraction.clamp(0.0, 1.0));
        }
    }
}

/// One CloudKit operation's answer, which its callbacks give on CloudKit's
/// own queues. Each callback runs under `caught_sync`, since a panic
/// unwinding into CloudKit would end the process, and the first panic in any
/// of an operation's callbacks answers its request with `StorageError::Bug`
/// (api.md E15).
pub(super) struct CloudKitCompletion<T> {
    /// `None` once the operation's last callback has answered.
    unanswered: Mutex<Option<Unanswered<T>>>,
}

struct Unanswered<T> {
    reply: oneshot::Sender<Result<T, StorageError>>,
    /// A panic in a callback before the operation's last, which the last
    /// answers with.
    bug: Option<Bug>,
}

impl<T> CloudKitCompletion<T> {
    /// An operation's answer, and where its request hears it.
    pub(super) fn new() -> (Arc<Self>, CloudKitReply<T>) {
        let (reply, result) = oneshot::channel();
        (Arc::new(Self { unanswered: Mutex::new(Some(Unanswered { reply, bug: None })) }), result)
    }

    /// Run `callback`, one the operation calls before its last, such as a
    /// record's progress or a matched record. A panic in it is the
    /// operation's answer once its last callback runs.
    pub(super) fn during(&self, operation: &'static str, callback: impl FnOnce()) {
        let Err(bug) = caught_sync(operation, callback) else {
            return;
        };
        match &mut *lock_unpoisoned(&self.unanswered) {
            Some(Unanswered { bug: first @ None, .. }) => *first = Some(bug),
            Some(Unanswered { bug: Some(first), .. }) => {
                tracing::error!("{bug}, after {first} had failed the CloudKit operation");
            }
            None => tracing::error!("{bug}, after its CloudKit operation answered"),
        }
    }

    /// Run `callback`, the operation's last, and answer the request with
    /// what it gives, or with the first panic in any of the operation's
    /// callbacks.
    pub(super) fn finish(
        &self,
        operation: &'static str,
        callback: impl FnOnce() -> Result<T, StorageError>,
    ) {
        let finished = caught_sync(operation, callback).unwrap_or_else(|bug| Err(bug.into()));
        let Some(Unanswered { reply, bug }) = lock_unpoisoned(&self.unanswered).take() else {
            tracing::error!(operation, "CloudKit invoked a completion more than once");
            return;
        };
        let answer = match bug {
            Some(bug) => {
                if let Err(error) = finished {
                    tracing::error!(%error, "a CloudKit operation also failed after {bug}");
                }
                Err(bug.into())
            }
            None => finished,
        };
        if reply.send(answer).is_err() {
            tracing::debug!("CloudKit caller dropped its pending operation");
        }
    }
}

pub(super) fn strings(values: &[&str]) -> Retained<NSArray<NSString>> {
    NSArray::from_retained_slice(&values.iter().map(|s| NSString::from_str(s)).collect::<Vec<_>>())
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
    progress: Option<AssetProgress>,
) -> CloudKitRequest<T> {
    fetch_with(|operation| unsafe { database.addOperation(operation) }, id, keys, convert, progress)
}

fn fetch_with<T: Send + 'static>(
    enqueue: impl FnOnce(&CKFetchRecordsOperation),
    id: &CKRecordID,
    keys: &[&str],
    convert: impl Fn(Record) -> Result<T, StorageError> + Send + Sync + 'static,
    progress: Option<AssetProgress>,
) -> CloudKitRequest<T> {
    let (completion, result) = CloudKitCompletion::new();
    // SAFETY: The operation is exclusively configured here. CloudKit copies its
    // blocks and input arrays. Callback pointers are retained before returning.
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKFetchRecordsOperation::initWithRecordIDs(
            CKFetchRecordsOperation::alloc(),
            &NSArray::from_slice(&[id]),
        );
        operation.setDesiredKeys(Some(&strings(keys)));
        if let Some(progress) = progress {
            let completion = completion.clone();
            let block = RcBlock::new(move |id: NonNull<CKRecordID>, fraction: f64| {
                completion.during("hear a CloudKit fetch's progress", || {
                    progress.heard(&id.as_ref().recordName().to_string(), fraction);
                });
            });
            operation.setPerRecordProgressBlock(Some(&block));
        }
        let block = RcBlock::new(
            move |records: *mut NSDictionary<CKRecordID, CKRecord>, failure: *mut NSError| {
                completion.finish("finish a CloudKit fetch", || {
                    if let Some(error) = error(failure) {
                        return Err(error);
                    }
                    match records.as_ref() {
                        Some(records) if records.count() == 1 => {
                            let values = records.allValues();
                            convert(Record::from_callback(&values.objectAtIndex(0)))
                        }
                        _ => Err(StorageFailure::Protocol
                            .with_source("CloudKit fetch omitted its record")),
                    }
                });
            },
        );
        operation.setFetchRecordsCompletionBlock(Some(&block));
        enqueue(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

pub(super) fn current_account(container: &CKContainer) -> CloudKitRequest<String> {
    let (completion, result) = CloudKitCompletion::new();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = CKFetchRecordsOperation::fetchCurrentUserRecordOperation();
        operation.setDesiredKeys(Some(&strings(&[])));
        let block = RcBlock::new(
            move |records: *mut NSDictionary<CKRecordID, CKRecord>, failure: *mut NSError| {
                completion.finish("finish a CloudKit account fetch", || match error(failure) {
                    Some(error) => Err(error),
                    None => super::record::account_name(
                        records.as_ref().filter(|records| records.count() == 1).map(|records| {
                            records.allValues().objectAtIndex(0).recordID().recordName().to_string()
                        }),
                    ),
                });
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
    progress: Option<AssetProgress>,
) -> CloudKitRequest<Vec<Record>> {
    modify_with(
        |operation| unsafe { database.addOperation(operation) },
        records,
        deletes,
        policy,
        create_name,
        assets,
        progress,
    )
}

/// Build and submit one conditional object save. The fetched record remains
/// intact, including its native change tag, while its payload fields change.
pub(super) fn conditional_object(
    enqueue: impl FnOnce(&CKModifyRecordsOperation),
    record: Record,
    path: &crate::ObjectPath,
    assets: Vec<coven_foundation::files::AssetFile>,
    progress: Option<AssetProgress>,
) -> CloudKitRequest<Vec<Record>> {
    modify_with(
        enqueue,
        vec![record],
        &[],
        CKRecordSavePolicy::IfServerRecordUnchanged,
        Some(path.as_str()),
        assets,
        progress,
    )
}

fn modify_with(
    enqueue: impl FnOnce(&CKModifyRecordsOperation),
    records: Vec<Record>,
    deletes: &[Retained<CKRecordID>],
    policy: CKRecordSavePolicy,
    create_name: Option<&str>,
    assets: Vec<coven_foundation::files::AssetFile>,
    progress: Option<AssetProgress>,
) -> CloudKitRequest<Vec<Record>> {
    let (completion, result) = CloudKitCompletion::new();
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
        if let Some(progress) = progress {
            let completion = completion.clone();
            let block = RcBlock::new(move |record: NonNull<CKRecord>, fraction: f64| {
                completion.during("hear a CloudKit save's progress", || {
                    let name = record.as_ref().recordID().recordName().to_string();
                    progress.heard(&name, fraction);
                });
            });
            operation.setPerRecordProgressBlock(Some(&block));
        }
        let block = RcBlock::new(
            move |saved: *mut NSArray<CKRecord>,
                  deleted: *mut NSArray<CKRecordID>,
                  failure: *mut NSError| {
                completion.finish("finish a CloudKit save", || {
                    let mut response = if let Some(error) = failure.as_ref() {
                        Err(super::record::save_error(error, create_name.as_deref()))
                    } else if saved.as_ref().map_or(0, |records| records.count()) != expected_saves
                        || deleted.as_ref().map_or(0, |ids| ids.count()) != expected_deletes
                    {
                        Err(StorageFailure::Protocol.with_source(
                            "CloudKit modification omitted its saved or deleted records",
                        ))
                    } else {
                        Ok(match saved.as_ref() {
                            Some(saved) => {
                                saved.iter().map(|record| Record::from_callback(&record)).collect()
                            }
                            None => Vec::new(),
                        })
                    };
                    if let Some(files) = lock_unpoisoned(&assets).take() {
                        for file in files {
                            if let Err(error) = file.close() {
                                response = crate::transfer::with_cleanup(
                                    response,
                                    Err(StorageFailure::File.with_source(error)),
                                );
                            }
                        }
                    }
                    response
                });
            },
        );
        operation.setModifyRecordsCompletionBlock(Some(&block));
        enqueue(&operation);
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
    let (completion, result) = CloudKitCompletion::new();
    let records = Arc::new(Mutex::new(Vec::new()));
    let matched = records.clone();
    let operation = objc2::rc::autoreleasepool(|_| unsafe {
        let operation = match cursor {
            Some(cursor) => CKQueryOperation::initWithCursor(CKQueryOperation::alloc(), &cursor.0),
            None => {
                let predicate = selection.predicate();
                let query = CKQuery::initWithRecordType_predicate(
                    CKQuery::alloc(),
                    &NSString::from_str(selection.record_type()),
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
        let matching = completion.clone();
        let block = RcBlock::new(
            move |_id: NonNull<CKRecordID>, record: *mut CKRecord, failure: *mut NSError| {
                matching.during("hear a CloudKit query's matched record", || {
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
                    lock_unpoisoned(&matched).push(response);
                });
            },
        );
        operation.setRecordMatchedBlock(Some(&block));
        let block = RcBlock::new(move |cursor: *mut CKQueryCursor, failure: *mut NSError| {
            completion.finish("finish a CloudKit query", || {
                if let Some(error) = error(failure) {
                    return Err(error);
                }
                let entries = std::mem::take(&mut *lock_unpoisoned(&records));
                entries
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map(|entries| (entries, Retained::retain(cursor).map(Cursor)))
            });
        });
        operation.setQueryCompletionBlock(Some(&block));
        database.addOperation(&operation);
        operation.into_super().into_super().into_super()
    });
    CloudKitRequest { operation, result }
}

pub(super) fn check_available() -> Result<(), StorageError> {
    if objc2::available!(macos = 12.0, ios = 15.0, tvos = 15.0, watchos = 8.0, visionos = 1.0) {
        Ok(())
    } else {
        Err(StorageFailure::ProviderUnavailable
            .with_source("CloudKit zone sharing is unavailable on this OS version"))
    }
}

#[cfg(test)]
#[path = "cloudkit_native_tests.rs"]
mod tests;
